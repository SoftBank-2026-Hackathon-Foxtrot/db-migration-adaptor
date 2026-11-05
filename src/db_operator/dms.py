"""DMS SDK, 실행 기록과 CLI를 한곳에서 제어합니다."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

import google.auth
from botocore.exceptions import BotoCoreError, ClientError
from google.api_core import exceptions as cloud_errors
from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import GoogleAuthError
from google.cloud import clouddms_v1 as dms
from pydantic import Field, ValidationError, model_validator

from .config import StateSettings
from .models import OperatorError, StrictModel, require
from .store import Store, now, request_digest

# DMS SDK 연결


class JobBinding(StrictModel):
    job: str = Field(
        pattern=r"^projects/[a-z0-9.-]+/locations/[a-z0-9-]+/migrationJobs/[a-z0-9-]+$"
    )
    source: str
    destination: str
    owner: str = Field(min_length=1, max_length=63, pattern=r"^[a-z0-9_-]+$")

    @model_validator(mode="after")
    def same_region(self) -> JobBinding:
        parent = self.job.split("/migrationJobs/")[0]
        for name in [self.source, self.destination]:
            require(
                name.startswith(parent + "/connectionProfiles/")
                and len(name.split("/")) == 6
                and bool(name.split("/")[-1]),
                "DMS_BINDING_INVALID",
                "DMS 작업과 연결 프로필의 프로젝트·리전이 같아야 합니다.",
            )
        require(self.source != self.destination, "DMS_BINDING_INVALID", "원본과 대상이 같습니다.")
        return self

    @property
    def project(self) -> str:
        return self.job.split("/")[1]


class DmsClient:
    def __init__(self, binding: JobBinding, client=None):
        self.binding = binding
        if client is None:
            credentials, _ = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
                quota_project_id=binding.project,
            )
            client = dms.DataMigrationServiceClient(credentials=credentials)
        self.client = client

    def observe(self) -> dict:
        job = self.client.get_migration_job(
            request={"name": self.binding.job}, retry=None, timeout=15
        )
        require(
            job.name == self.binding.job
            and job.source == self.binding.source
            and job.destination == self.binding.destination
            and job.labels.get("foxtrot-owner") == self.binding.owner,
            "DMS_BINDING_MISMATCH",
            "DMS 작업의 원본·대상·소유 표시가 등록값과 다릅니다.",
        )
        require(
            job.type_ == dms.MigrationJob.Type.CONTINUOUS
            and job.source_database.engine == dms.DatabaseEngine.POSTGRESQL
            and job.source_database.provider == dms.DatabaseProvider.RDS
            and job.destination_database.engine == dms.DatabaseEngine.POSTGRESQL
            and job.destination_database.provider == dms.DatabaseProvider.CLOUDSQL,
            "DMS_PROFILE_UNSUPPORTED",
            "현재 DMS 연동은 RDS PostgreSQL → Cloud SQL 연속 복제만 지원합니다.",
        )
        profiles = [
            self.client.get_connection_profile(request={"name": name}, retry=None, timeout=15)
            for name in [self.binding.source, self.binding.destination]
        ]
        require(
            all(p.name == n for p, n in zip(profiles, [job.source, job.destination], strict=True)),
            "DMS_BINDING_MISMATCH",
            "연결 프로필의 실제 식별자가 다릅니다.",
        )
        config = dms.MigrationJob.to_dict(job)
        for key in ["update_time", "state", "phase", "duration", "error", "end_time"]:
            config.pop(key, None)
        # The digest covers configuration changes without storing credentials.
        fingerprint = request_digest(
            {
                "binding": self.binding.model_dump(),
                "job": config,
                "profiles": [dms.ConnectionProfile.to_dict(p) for p in profiles],
            }
        )
        return {
            "job": job.name,
            "state": job.state.name,
            "phase": job.phase.name,
            "errorCode": job.error.code,
            "fingerprint": fingerprint,
            "cutoverReady": "UNKNOWN",
            "reverseReplication": "NOT_CONFIGURED",
        }

    def submit(self, action: Literal["VERIFY", "START_SYNC"]) -> str:
        method = {
            "VERIFY": self.client.verify_migration_job,
            "START_SYNC": self.client.start_migration_job,
        }[action]
        operation = method(request={"name": self.binding.job}, retry=None, timeout=30)
        return operation.operation.name

    def operation(self, name: str, action: str, *, submitted_at: datetime | None = None) -> dict:
        parent = self.binding.job.split("/migrationJobs/")[0]
        require(
            name.startswith(parent + "/operations/") and len(name.split("/")) == 6,
            "DMS_OPERATION_MISMATCH",
            "DMS 실행 식별자가 등록한 프로젝트·리전과 다릅니다.",
        )
        op = self.client.get_operation(request={"name": name}, retry=None, timeout=15)
        metadata = dms.OperationMetadata()
        require(
            op.name == name and op.metadata.Unpack(dms.OperationMetadata.pb(metadata)),
            "DMS_OPERATION_MISMATCH",
            "DMS 실행 근거를 읽을 수 없습니다.",
        )
        require(
            metadata.target == self.binding.job
            and metadata.verb == {"VERIFY": "verify", "START_SYNC": "start"}[action],
            "DMS_OPERATION_MISMATCH",
            "DMS 실행의 대상 또는 명령이 다릅니다.",
        )
        if submitted_at is not None:
            require(
                metadata.create_time
                and -5 <= (metadata.create_time - submitted_at).total_seconds() <= 45,
                "DMS_OPERATION_MISMATCH",
                "DMS 실행 시간이 접수 의도 기록과 다릅니다. 감사 기록을 확인하세요.",
            )
        if not op.done:
            return {"status": "PENDING", "errorCode": None}
        if op.HasField("error"):
            return {"status": "FAILED", "errorCode": op.error.code}
        result = dms.MigrationJob()
        require(
            op.response.Unpack(dms.MigrationJob.pb(result)) and result.name == self.binding.job,
            "DMS_OPERATION_MISMATCH",
            "DMS 실행 결과의 대상이 다릅니다.",
        )
        if result.error.code or result.state == dms.MigrationJob.State.FAILED:
            return {"status": "FAILED", "errorCode": result.error.code or 2}
        return {"status": "SUCCEEDED", "errorCode": None}


def unknown_operation() -> OperatorError:
    return OperatorError(
        "DMS_SUBMISSION_UNKNOWN",
        "DMS 접수 여부가 불명입니다. 같은 요청을 다시 보내지 말고 클라우드 실행 내역을 확인하세요.",
        503,
    )


# DMS 실행 기록


class DmsJournal:
    def __init__(self, store: Store):
        self.store = store

    @staticmethod
    def job_key(job: str) -> str:
        return hashlib.sha256(job.encode()).hexdigest()

    def initialize(self) -> None:
        self.store.initialize()

    @contextmanager
    def lock(self, job: str):
        with self.store.exclusive("dms:" + job) as lease:
            active = self.store._read("DMS_ACTIVE#" + self.job_key(job))
            if active:
                row = self.row(lease, job, active["operation_id"])
                if row["status"] == "SUBMITTING":
                    self.update(lease, job, row["id"], "UNKNOWN")
            yield lease

    def row(self, lease, job: str, operation_id: str) -> dict:
        lease.assert_owned()
        row = self.store._read("DMS_OP#" + self.job_key(job) + "#" + operation_id)
        require(row and row["job"] == job, "NOT_FOUND", "등록한 DMS 실행을 찾을 수 없습니다.")
        return row

    def find(self, lease, job: str, key: str) -> dict | None:
        lease.assert_owned()
        saved = self.store._read(
            "DMS_KEY#" + self.job_key(job) + "#" + hashlib.sha256(key.encode()).hexdigest()
        )
        return self.row(lease, job, saved["operation_id"]) if saved else None

    def begin(self, lease, job: str, action: str, key: str, fingerprint: str) -> tuple[dict, bool]:
        prefix = self.job_key(job)

        def begin(tx):
            tx.require_lease(lease)
            key_id = prefix + "#" + hashlib.sha256(key.encode()).hexdigest()
            saved = tx.get("DMS_KEY", key_id)
            if saved:
                old = tx.get("DMS_OP", prefix + "#" + saved["operation_id"])
                require(
                    old["action"] == action and old["fingerprint"] == fingerprint,
                    "IDEMPOTENCY_CONFLICT",
                    "같은 멱등 키의 명령 또는 DMS 설정이 다릅니다.",
                )
                return old, False
            require(
                not tx.get("DMS_ACTIVE", prefix),
                "DMS_BUSY",
                "진행 중이거나 접수 여부가 불명인 DMS 실행을 먼저 확인하세요.",
            )
            if action == "START_SYNC":
                verified = tx.get("DMS_VERIFIED", prefix)
                require(
                    verified and verified["fingerprint"] == fingerprint,
                    "DMS_VERIFICATION_REQUIRED",
                    "현재 DMS 설정의 연결 검사를 먼저 완료하세요.",
                )
            oid = "dms-" + uuid4().hex
            row = {
                "id": oid,
                "job": job,
                "action": action,
                "idempotency_key": key,
                "fingerprint": fingerprint,
                "status": "SUBMITTING",
                "cloud_operation": None,
                "error_code": None,
                "created_at": now(),
            }
            tx.put("DMS_OP", prefix + "#" + oid, row)
            tx.put("DMS_KEY", key_id, {"operation_id": oid})
            tx.put("DMS_ACTIVE", prefix, {"operation_id": oid})
            return row, True

        return self.store.transact(begin)

    def update(
        self,
        lease,
        job: str,
        operation_id: str,
        status: str,
        cloud_operation: str | None = None,
        error_code: int | None = None,
    ) -> dict:
        prefix = self.job_key(job)

        def update(tx):
            tx.require_lease(lease)
            row = tx.get("DMS_OP", prefix + "#" + operation_id)
            require(row is not None, "NOT_FOUND", "DMS 실행을 찾을 수 없습니다.")
            row.update(status=status, error_code=error_code)
            if cloud_operation is not None:
                row["cloud_operation"] = cloud_operation
            tx.put("DMS_OP", prefix + "#" + operation_id, row)
            if status in {"SUCCEEDED", "FAILED"}:
                active = tx.get("DMS_ACTIVE", prefix)
                if active and active["operation_id"] == operation_id:
                    tx.delete("DMS_ACTIVE", prefix)
            if status == "SUCCEEDED" and row["action"] == "VERIFY":
                tx.put(
                    "DMS_VERIFIED",
                    prefix,
                    {"fingerprint": row["fingerprint"], "operation_id": operation_id},
                )
            return row

        return self.store.transact(update)


# DMS 작업 제어


class DmsService:
    def __init__(self, client: DmsClient, journal: DmsJournal):
        self.client, self.journal = client, journal

    def submit(self, action: Literal["VERIFY", "START_SYNC"], key: str) -> dict:
        require(
            action in {"VERIFY", "START_SYNC"},
            "DMS_ACTION_UNSUPPORTED",
            "지원하지 않는 명령입니다.",
        )
        require(0 < len(key) <= 128, "INVALID_KEY", "멱등 키는 1~128자여야 합니다.")
        job = self.client.binding.job
        with self.journal.lock(job) as lease:
            observed = self.client.observe()
            if action == "START_SYNC":
                # A duplicate request is handled before checking the changed cloud state.
                old = self.journal.find(lease, job, key)
                if old:
                    require(
                        old["action"] == action and old["fingerprint"] == observed["fingerprint"],
                        "IDEMPOTENCY_CONFLICT",
                        "같은 멱등 키의 명령 또는 DMS 설정이 다릅니다.",
                    )
                    return old
                require(
                    observed["state"] == "NOT_STARTED" and not observed["errorCode"],
                    "DMS_INVALID_STATE",
                    "아직 시작하지 않은 정상 DMS 작업만 실행합니다.",
                )
            row, fresh = self.journal.begin(lease, job, action, key, observed["fingerprint"])
            if not fresh:
                return row
            try:
                # 외부 RPC 전에 접수 의도를 DynamoDB에 저장합니다.
                lease.assert_owned()
                name = self.client.submit(action)
                require(bool(name), "DMS_OPERATION_MISSING", "DMS 실행 식별자가 없습니다.")
            except (
                cloud_errors.InvalidArgument,
                cloud_errors.PermissionDenied,
                cloud_errors.FailedPrecondition,
                cloud_errors.NotFound,
                cloud_errors.Unauthenticated,
                cloud_errors.ResourceExhausted,
            ) as exc:
                # An explicit RPC rejection differs from a lost acceptance response.
                code = exc.grpc_status_code.value[0]
                return self.journal.update(lease, job, row["id"], "FAILED", error_code=code)
            except Exception as exc:
                self.journal.update(lease, job, row["id"], "UNKNOWN")
                raise unknown_operation() from exc
            return self.journal.update(lease, job, row["id"], "PENDING", name)

    def poll(self, operation_id: str) -> dict:
        job = self.client.binding.job
        with self.journal.lock(job) as lease:
            row = self.journal.row(lease, job, operation_id)
            require(
                row["fingerprint"] == self.client.observe()["fingerprint"],
                "DMS_CONFIG_CHANGED",
                "DMS 설정이 바뀌었습니다. 실행 근거를 다시 확인하세요.",
            )
            if row["status"] in {"SUCCEEDED", "FAILED", "UNKNOWN"}:
                return row
            require(row["cloud_operation"], "DMS_SUBMISSION_UNKNOWN", "DMS 실행 식별자가 없습니다.")
            result = self.client.operation(row["cloud_operation"], row["action"])
            return self.journal.update(
                lease, job, row["id"], result["status"], error_code=result["errorCode"]
            )

    def reconcile(self, operation_id: str, cloud_operation: str) -> dict:
        """운영자가 확인한 실행 근거를 연결합니다. 변경 RPC는 보내지 않습니다."""
        job = self.client.binding.job
        with self.journal.lock(job) as lease:
            row = self.journal.row(lease, job, operation_id)
            require(
                row["status"] == "UNKNOWN" and not row["cloud_operation"],
                "DMS_INVALID_STATE",
                "접수 여부가 불명인 실행에만 근거를 연결합니다.",
            )
            require(
                row["fingerprint"] == self.client.observe()["fingerprint"],
                "DMS_CONFIG_CHANGED",
                "접수 후 DMS 설정이 바뀌었습니다.",
            )
            result = self.client.operation(
                cloud_operation, row["action"], submitted_at=row["created_at"]
            )
            return self.journal.update(
                lease, job, row["id"], result["status"], cloud_operation, result["errorCode"]
            )


# DMS 실행 명령


def main() -> None:
    parser = argparse.ArgumentParser(description="GCP DMS initial copy and CDC control")
    parser.add_argument("command", choices=["status", "verify", "start", "operation", "reconcile"])
    parser.add_argument("--config", required=True, type=Path, help="JobBinding JSON file")
    parser.add_argument(
        "--key", help="Required for verify/start; reuse the same key after a timeout"
    )
    parser.add_argument("--operation-id", help="Local dms-* ID, required for operation")
    parser.add_argument("--cloud-operation", help="Verified cloud operation name for reconcile")
    args = parser.parse_args()
    try:
        binding = JobBinding.model_validate_json(args.config.read_text())
        client = DmsClient(binding)
        if args.command == "status":
            result = client.observe()
        else:
            require(
                args.operation_id if args.command in {"operation", "reconcile"} else args.key,
                "ARGUMENT_REQUIRED",
                "명령에 필요한 멱등 키 또는 실행 ID가 없습니다.",
            )
            settings = StateSettings()
            journal = DmsJournal(
                Store(
                    settings.state_table,
                    namespace=settings.state_namespace,
                    region=settings.aws_region,
                    endpoint=settings.dynamodb_endpoint,
                )
            )
            journal.initialize()
            service = DmsService(client, journal)
            if args.command == "operation":
                result = service.poll(args.operation_id)
            elif args.command == "reconcile":
                require(args.cloud_operation, "ARGUMENT_REQUIRED", "클라우드 실행 ID가 필요합니다.")
                result = service.reconcile(args.operation_id, args.cloud_operation)
            else:
                result = service.submit(
                    "VERIFY" if args.command == "verify" else "START_SYNC", args.key
                )
        print(json.dumps(result, default=str, ensure_ascii=False))
    except OperatorError as exc:
        print(
            json.dumps({"error": exc.code, "message": exc.message}, ensure_ascii=False),
            file=sys.stderr,
        )
        raise SystemExit(1) from None
    except (
        GoogleAPICallError,
        GoogleAuthError,
        BotoCoreError,
        ClientError,
        ValidationError,
        OSError,
    ) as exc:
        print(
            json.dumps(
                {
                    "error": type(exc).__name__,
                    "message": "설정·인증·접속을 확인하세요. 변경 요청은 같은 키로 확인하세요.",
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
