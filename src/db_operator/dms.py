"""DMS SDK, 실행 기록과 CLI를 한곳에서 제어합니다."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from uuid import uuid4

import google.auth
import psycopg
from botocore.exceptions import BotoCoreError, ClientError
from google.api_core import exceptions as cloud_errors
from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import GoogleAuthError
from google.auth.transport.requests import AuthorizedSession
from google.cloud import clouddms_v1 as dms
from google.protobuf.empty_pb2 import Empty
from psycopg import sql
from psycopg.conninfo import make_conninfo
from pydantic import Field, ValidationError, model_validator

from .config import StateSettings
from .models import DMS_PROFILE, OperatorError, StrictModel, require
from .postgres import Postgres, identifier
from .store import Store, now, request_digest

DMS_ACTIONS = {
    "VERIFY": "verify",
    "START_SYNC": "start",
    "RESUME_SYNC": "resume",
    "PROMOTE": "promote",
    "STOP": "stop",
    "DELETE": "delete",
}

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
    def __init__(self, binding: JobBinding, client=None, instance_reader=None):
        self.binding = binding
        if client is None:
            client = sdk_client(binding.project)
        self.client = client
        self.instance_reader = instance_reader or read_cloudsql_instance

    def destination(self, profile):
        instance = profile.cloudsql.cloud_sql_id or profile.postgresql.cloud_sql_id
        hostnames = []
        addresses = [
            a
            for a in [
                profile.cloudsql.public_ip,
                profile.cloudsql.private_ip,
                profile.cloudsql.additional_public_ip,
            ]
            if a
        ]
        if instance and not addresses:
            require(
                re.fullmatch(r"[a-z][a-z0-9-]{0,97}", instance),
                "DMS_BINDING_INVALID",
                "Cloud SQL 인스턴스 ID가 올바르지 않습니다.",
            )
            info = self.instance_reader(self.binding.project, instance)
            require(
                info.get("name") == instance
                and info.get("project") == self.binding.project
                and str(info.get("connectionName", "")).startswith(self.binding.project + ":")
                and str(info.get("connectionName", "")).endswith(":" + instance),
                "DMS_BINDING_MISMATCH",
                "Cloud SQL 조회 결과의 인스턴스 식별자가 다릅니다.",
            )
            addresses = [
                ip["ipAddress"]
                for ip in info.get("ipAddresses", [])
                if ip.get("type") in {"PRIMARY", "PRIVATE"} and ip.get("ipAddress")
            ]
            names = [info.get("dnsName", "")] + [
                item.get("name", "")
                for item in info.get("dnsNames", [])
                if item.get("connectionType") == "PUBLIC"
            ]
            hostnames = sorted(
                {
                    name.lower()
                    for name in names
                    if isinstance(name, str)
                    and re.fullmatch(r"[a-z0-9.-]+\.sql\.goog\.?", name.lower())
                }
            )
        return {"instance": instance, "addresses": sorted(set(addresses)), "hostnames": hostnames}

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
        destination = self.destination(profiles[1])
        for key in ["update_time", "state", "phase", "duration", "error", "end_time"]:
            config.pop(key, None)
        # The digest covers configuration changes without storing credentials.
        fingerprint = request_digest(
            {
                "binding": self.binding.model_dump(),
                "job": config,
                "profiles": [dms.ConnectionProfile.to_dict(p) for p in profiles],
                "destinationEndpoint": destination,
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
            "sourceEndpoint": {
                "host": profiles[0].postgresql.host,
                "port": profiles[0].postgresql.port or 5432,
            },
            "destinationInstance": destination["instance"],
            "sourceReplicationRole": profiles[0].postgresql.username,
            "destinationAddresses": destination["addresses"],
            "destinationHostnames": destination["hostnames"],
        }

    def submit(self, action: str) -> str:
        method = {
            "VERIFY": self.client.verify_migration_job,
            "START_SYNC": self.client.start_migration_job,
            "RESUME_SYNC": self.client.resume_migration_job,
            "PROMOTE": self.client.promote_migration_job,
            "STOP": self.client.stop_migration_job,
            "DELETE": self.client.delete_migration_job,
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
            metadata.target == self.binding.job and metadata.verb == DMS_ACTIONS[action],
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
        if action == "DELETE":
            require(
                op.response.Unpack(Empty()), "DMS_OPERATION_MISMATCH", "DMS 삭제 결과가 다릅니다."
            )
            return {"status": "SUCCEEDED", "errorCode": None}
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


@lru_cache(maxsize=8)
def google_credentials(project: str):
    credentials, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"], quota_project_id=project
    )
    return credentials


@lru_cache(maxsize=8)
def sdk_client(project: str):
    return dms.DataMigrationServiceClient(credentials=google_credentials(project))


@lru_cache(maxsize=8)
def sql_session(project: str):
    return AuthorizedSession(google_credentials(project))


def read_cloudsql_instance(project: str, instance: str):
    """Cloud SQL 관리 API의 GET만 사용합니다. DB를 시작하거나 변경하지 않습니다."""
    try:
        response = sql_session(project).get(
            f"https://sqladmin.googleapis.com/sql/v1beta4/projects/{project}/instances/{instance}",
            timeout=15,
        )
        require(
            response.status_code == 200,
            "CLOUDSQL_METADATA_UNAVAILABLE",
            "Cloud SQL 인스턴스 조회 권한과 상태를 확인하십시오.",
            503,
        )
        return response.json()
    except (OSError, ValueError) as error:
        raise OperatorError(
            "CLOUDSQL_METADATA_UNAVAILABLE", "Cloud SQL 메타데이터를 읽을 수 없습니다.", 503
        ) from error


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
        self.migration_id = None

    def check_configuration(self, row):
        try:
            observed = self.client.observe()
        except cloud_errors.NotFound:
            require(row["action"] == "DELETE", "DMS_CONFIG_CHANGED", "DMS 작업이 없어졌습니다.")
            return
        require(
            row["fingerprint"] == observed["fingerprint"],
            "DMS_CONFIG_CHANGED",
            "DMS 설정이 바뀌었습니다. 실행 근거를 다시 확인하십시오.",
        )

    def submit(self, action: str, key: str) -> dict:
        require(
            action in DMS_ACTIONS,
            "DMS_ACTION_UNSUPPORTED",
            "지원하지 않는 명령입니다.",
        )
        require(0 < len(key) <= 128, "INVALID_KEY", "멱등 키는 1~128자여야 합니다.")
        job = self.client.binding.job
        with self.journal.lock(job) as lease:
            claim = self.journal.store._read(
                "CLAIM#" + hashlib.sha256(("dms-job:" + job).encode()).hexdigest()
            )
            require(
                not claim or claim["migration_id"] == self.migration_id,
                "DMS_BUSY",
                "공통 API가 관리 중인 DMS 작업은 별도 CLI로 변경하지 않습니다.",
            )
            observed = self.client.observe()
            old = self.journal.find(lease, job, key)
            if old:
                require(
                    old["action"] == action and old["fingerprint"] == observed["fingerprint"],
                    "IDEMPOTENCY_CONFLICT",
                    "같은 멱등 키의 명령 또는 DMS 설정이 다릅니다.",
                )
                return old
            if action == "PROMOTE":
                require(
                    observed["state"] == "RUNNING"
                    and observed["phase"] == "CDC"
                    and not observed["errorCode"],
                    "DMS_INVALID_STATE",
                    "정상 CDC 작업만 승격할 수 있습니다.",
                )
            if action == "STOP":
                require(
                    observed["state"] == "RUNNING",
                    "DMS_INVALID_STATE",
                    "실행 중인 DMS 작업만 정지할 수 있습니다.",
                )
            if action == "DELETE":
                require(
                    observed["state"] in {"NOT_STARTED", "STOPPED", "COMPLETED", "FAILED"},
                    "DMS_INVALID_STATE",
                    "종료 또는 정지한 DMS 작업만 삭제합니다.",
                )
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
            if action == "RESUME_SYNC":
                require(
                    observed["state"] == "STOPPED"
                    and observed["phase"] == "CDC"
                    and not observed["errorCode"],
                    "DMS_INVALID_STATE",
                    "CDC 중 정지한 정상 작업만 기존 복제를 재개합니다.",
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
            if row["status"] in {"SUCCEEDED", "FAILED", "UNKNOWN"}:
                return row
            require(row["cloud_operation"], "DMS_SUBMISSION_UNKNOWN", "DMS 실행 식별자가 없습니다.")
            result = self.client.operation(row["cloud_operation"], row["action"])
            if result["status"] == "PENDING":
                # 대기 중에는 저장한 실행 ID만 조회합니다. 다음 변경 전에 설정을 대조합니다.
                return row
            self.check_configuration(row)
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
            self.check_configuration(row)
            result = self.client.operation(
                cloud_operation, row["action"], submitted_at=row["created_at"]
            )
            return self.journal.update(
                lease, job, row["id"], result["status"], cloud_operation, result["errorCode"]
            )


# 공통 API의 GCP DMS 실행 경로


class GcpDmsPostgres(Postgres):
    """SQL·프록시는 기존 제어기, 복제 변경은 GCP DMS만 사용합니다."""

    profile = DMS_PROFILE

    def __init__(self, registry, plan, store):
        super().__init__(registry)
        self.plan, self.store = plan, store
        entry = registry.dms_jobs.get(plan.get("replicationRef"))
        require(
            entry
            and entry["sourceRef"] == plan["sourceRef"]
            and entry["targetRef"] == plan["targetRef"],
            "DMS_BINDING_INVALID",
            "승인된 DMS 작업과 원본·대상이 다릅니다.",
        )
        self.client = DmsClient(JobBinding.model_validate(entry["binding"]))
        self.dms = DmsService(self.client, DmsJournal(store))
        self._observation_job = ContextVar("dms_observation_job", default=None)
        self._business_databases = set()

    def job(self):
        internal = {}
        if getattr(self, "migration_id", None):
            internal = self.store.get(self.migration_id)["internal"]
            if internal.get("dmsJobDeleted"):
                return {
                    "state": "DELETED",
                    "phase": "PHASE_UNSPECIFIED",
                    "errorCode": 0,
                    "fingerprint": internal["dmsFingerprint"],
                }
        observed = self.client.observe()
        require(
            not internal.get("dmsFingerprint")
            or internal["dmsFingerprint"] == observed["fingerprint"],
            "DMS_CONFIG_CHANGED",
            "DMS 작업 또는 접속 프로필이 변경됐습니다.",
        )
        return observed

    @contextmanager
    def connect(self, database, tag="observe", timeout=10):
        # DMS Verify는 사용자 DB가 없는 대상을 요구합니다. 초기 복사 전에는
        # maintenance DB에서 인스턴스·역할만 관측하고 업무 DB를 만들지 않습니다.
        if (
            database.provider == "GCP_CLOUDSQL"
            and database.database != "postgres"
            and database.identity not in self._business_databases
        ):
            job = self._observation_job.get() or self.job()
            if job["state"] == "NOT_STARTED" or (
                job["state"] in {"STARTING", "RUNNING"}
                and job["phase"] != "CDC"
                and not job["errorCode"]
            ):
                with super().connect(database, tag, timeout, database_name="postgres") as conn:
                    present = conn.execute(
                        "SELECT 1 FROM pg_database WHERE datname=%s", (database.database,)
                    ).fetchone()
                    if not present:
                        yield conn
                        return
        with super().connect(database, tag, timeout) as conn:
            # 같은 실행 범위에서 업무 DB 접속이 성공한 뒤에는 존재 여부를 다시 묻지 않습니다.
            # 접속 실패 시 maintenance DB로 우회하지 않고 실패를 전달합니다.
            self._business_databases.add(database.identity)
            yield conn

    def check_job_binding(self, plan, job):
        from psycopg.conninfo import conninfo_to_dict

        source, target, _ = self.resources(plan)
        require(
            job["sourceEndpoint"] == {"host": source.route_host, "port": source.route_port}
            and target.provider_instance
            == f"projects/{self.client.binding.project}/instances/{job['destinationInstance']}"
            and target.route_host
            in [*job["destinationAddresses"], *job.get("destinationHostnames", [])]
            and job["sourceReplicationRole"]
            == conninfo_to_dict(source.secret("replication")).get("user")
            and job["state"] == "NOT_STARTED",
            "DMS_BINDING_MISMATCH",
            "DMS 접속·복제 계정·대상 인스턴스·시작 전 상태가 다릅니다.",
        )

    def grant_target_writer(self, target, tag, intent):
        schemas = sorted(
            {name.split(".")[0] for name in [*self.registry.tables, *self.registry.sequences]}
        )
        with self.connect(target, tag) as conn:
            for schema in schemas:
                intent("dms.grant_schema")
                conn.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                        sql.Identifier(schema), sql.Identifier(target.writer_role)
                    )
                )
            for name in self.registry.tables:
                intent("dms.grant_table")
                conn.execute(
                    sql.SQL("GRANT SELECT,INSERT,UPDATE,DELETE ON {} TO {}").format(
                        identifier(name), sql.Identifier(target.writer_role)
                    )
                )
            for name in self.registry.sequences:
                intent("dms.grant_sequence")
                conn.execute(
                    sql.SQL("GRANT USAGE,SELECT ON SEQUENCE {} TO {}").format(
                        identifier(name), sql.Identifier(target.writer_role)
                    )
                )

    def check_catalog(self, conn, database):
        actual = {
            r["name"]
            for r in conn.execute(
                "SELECT n.nspname||'.'||c.relname AS name FROM pg_class c "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN ('r','p') "
                "AND n.nspname NOT IN ('pg_catalog','information_schema','foxtrot_migration') "
                "AND n.nspname NOT LIKE 'pg_toast%%' AND NOT EXISTS (SELECT 1 FROM "
                "pg_depend d WHERE d.classid='pg_class'::regclass "
                "AND d.objid=c.oid AND d.deptype='e')"
            ).fetchall()
        }
        require(
            actual == set(self.registry.tables),
            "UNSUPPORTED_SCHEMA",
            "업무 table 전체를 등록하십시오.",
        )
        for name in self.registry.tables:
            other = conn.execute(
                "SELECT r.rolname FROM pg_roles r WHERE r.rolcanlogin AND NOT r.rolsuper "
                "AND r.rolname NOT IN (current_user,%s,'rdsadmin','cloudsqladmin','cloudsqlagent') "
                "AND EXISTS (SELECT 1 FROM pg_roles p WHERE "
                "has_table_privilege(p.oid,%s::regclass,'INSERT,UPDATE,DELETE,TRUNCATE') "
                "AND pg_has_role(r.oid,p.oid,'MEMBER'))",
                (database.writer_role, name),
            ).fetchone()
            require(not other, "UNREGISTERED_WRITER", "등록하지 않은 업무 writer가 있습니다.")

    def check_default_database(self, database):
        if database.database == "postgres":
            return
        # 인스턴스 단위 승격에 함께 포함되는 기본 DB에 업무 데이터가 있으면 거절합니다.
        with psycopg.connect(
            make_conninfo(database.secret("admin"), dbname="postgres"),
            connect_timeout=3,
            options="-c statement_timeout=3000",
            autocommit=True,
        ) as conn:
            extra = conn.execute(
                "SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE c.relkind IN ('r','p','S') AND n.nspname NOT IN "
                "('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%%' "
                "AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE "
                "d.classid='pg_class'::regclass AND d.objid=c.oid AND d.deptype='e') LIMIT 1"
            ).fetchone()
        require(
            not extra,
            "DMS_INSTANCE_SCOPE_UNSUPPORTED",
            "기본 postgres DB에 등록하지 않은 데이터가 있습니다.",
        )

    def observe(self, plan, *, source_authority=False):
        job = self.job()
        token = self._observation_job.set(job)
        try:
            observed = super().observe(plan, source_authority=source_authority)
        finally:
            self._observation_job.reset(token)
        observed["dms"] = job
        return observed

    def replication_ready(self, database, name):
        job = self.job()
        return job["state"] == "RUNNING" and job["phase"] == "CDC" and not job["errorCode"]

    def check_admin_endpoint(self, database):
        from psycopg.conninfo import conninfo_to_dict

        dsn = conninfo_to_dict(database.secret("admin"))
        require(
            dsn.get("host") == database.route_host
            and int(dsn.get("port", 5432)) == database.route_port
            and dsn.get("dbname") == database.database,
            "DMS_BINDING_MISMATCH",
            "관리 Secret과 등록한 실제 DB 주소가 다릅니다.",
        )

    def check_instance_databases(self, conn, database):
        databases = {
            r["datname"]
            for r in conn.execute(
                "SELECT datname FROM pg_database WHERE NOT datistemplate AND datallowconn"
            ).fetchall()
        }
        require(
            databases <= {database.database, "postgres", "rdsadmin", "cloudsqladmin"},
            "DMS_INSTANCE_SCOPE_UNSUPPORTED",
            "현재 DMS 프로필은 전용 원본·대상 인스턴스만 지원합니다.",
        )

    def preflight(self, plan):
        source, target, binding = self.resources(plan)
        require(
            source.provider == "AWS_RDS"
            and target.provider == "GCP_CLOUDSQL"
            and source.provider_instance
            and target.provider_instance
            and source.database == target.database,
            "DMS_PROFILE_UNSUPPORTED",
            "RDS → Cloud SQL의 같은 DB 이름이 필요합니다.",
        )
        for db in [source, target]:
            self.check_admin_endpoint(db)
        observed = self.observe(plan)
        self._schema._check_proxy(source, target, binding, observed)
        self._schema._check_config(source, binding)
        job = observed["dms"]
        self.check_job_binding(plan, job)
        for db, node in [(source, "source"), (target, "target")]:
            info = observed[node]
            writer = info["writer"]
            require(
                (
                    info["db"] == db.database
                    or (
                        node == "target"
                        and info["db"] == "postgres"
                        and job["state"] == "NOT_STARTED"
                    )
                )
                and 150000 <= info["version"] < 190000
                and info["prepared"] == 0
                and writer
                and not any(
                    writer[k]
                    for k in [
                        "rolsuper",
                        "rolcreaterole",
                        "rolcreatedb",
                        "rolreplication",
                        "rolbypassrls",
                    ]
                ),
                "DMS_PROFILE_UNSUPPORTED",
                "지원하는 DB·전용 업무 역할·SQL 권한이 필요합니다.",
            )
            with self.connect(db) as conn:
                self.check_instance_databases(conn, db)
                require(
                    not conn.execute(
                        "SELECT 1 FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles "
                        "WHERE rolname=%s)",
                        (db.writer_role,),
                    ).fetchone(),
                    "UNSAFE_WRITER",
                    "업무 역할의 상속은 허용하지 않습니다.",
                )
                if node == "source":
                    require(
                        not conn.execute(
                            "SELECT 1 FROM pg_largeobject_metadata LIMIT 1"
                        ).fetchone(),
                        "UNSUPPORTED_SCHEMA",
                        "large object는 지원하지 않습니다.",
                    )
                    self._schema._check_sequences(conn)
                    self.check_catalog(conn, db)
                    for name in self.registry.tables:
                        owner = conn.execute(
                            "SELECT pg_get_userbyid(relowner) AS owner FROM pg_class "
                            "WHERE oid=%s::regclass",
                            (name,),
                        ).fetchone()["owner"]
                        require(
                            owner != db.writer_role,
                            "UNSAFE_WRITER",
                            "업무 계정은 table 소유자일 수 없습니다.",
                        )
                else:
                    for name in self.registry.tables:
                        relation = conn.execute(
                            "SELECT to_regclass(%s) AS value", (name,)
                        ).fetchone()["value"]
                        if relation:
                            require(
                                not conn.execute(
                                    sql.SQL("SELECT 1 FROM {} LIMIT 1").format(identifier(name))
                                ).fetchone(),
                                "TARGET_NOT_EMPTY",
                                "대상 업무 데이터가 이미 있습니다.",
                            )
            self.check_default_database(db)
        self.schema_signature(source)
        return observed


class DmsExecutor:
    """공통 작업 큐의 명령을 DMS·SQL·PgBouncer 순서로 실행합니다."""

    def __init__(self, context):
        self.ctx = context
        self.driver = context.driver

    @property
    def row(self):
        return self.ctx.store.get(self.ctx.migration_id)

    def run(self):
        action = str(self.ctx.request.action)
        getattr(self, action.lower())()
        self.ctx.confirm_and_observe("GCP DMS 경로의 " + action + " 확인 완료")

    def cloud(self, action):
        ctx = self.ctx
        ctx.assert_lease()
        evidence = ctx.verify_gate()
        if action == "PROMOTE":
            require(evidence, "QUIESCE_REQUIRED", "승격에는 현재 업무 정지 근거가 필요합니다.")
        self.driver.dms.migration_id = ctx.migration_id
        if not self.row["internal"].get("dmsFingerprint"):
            ctx.save_internal(dmsFingerprint=self.driver.job()["fingerprint"])
        ctx.save_internal(dmsRequest={"action": action, "key": ctx.operation_id})
        ctx.record_intent("dms." + DMS_ACTIONS[action])
        result = self.driver.dms.submit(action, ctx.operation_id)
        ctx.save_internal(dmsOperationId=result["id"])
        wait_seconds = 30
        if action == "PROMOTE":
            remaining = (
                ctx.plan["policy"]["maxQuiesceSeconds"]
                - (now() - evidence["held_since"]).total_seconds()
            )
            wait_seconds = min(180, max(0, remaining))
        deadline = time.monotonic() + wait_seconds
        while result["status"] == "PENDING":
            ctx.assert_lease()
            ctx.verify_gate()
            require(
                time.monotonic() < deadline,
                "DMS_OPERATION_PENDING",
                "DMS 실행이 아직 진행 중입니다. RECONCILE로 확인하십시오.",
            )
            result = self.driver.dms.poll(result["id"])
            if result["status"] == "PENDING":
                time.sleep(1)
        require(
            result["status"] == "SUCCEEDED",
            "DMS_OPERATION_UNCONFIRMED",
            "DMS 실행 성공을 확인할 수 없습니다.",
        )
        ctx.verify_gate()
        return result

    def prepare(self):
        ctx = self.ctx
        observed = self.driver.preflight(ctx.plan)
        if ctx.source.secret_ref and ctx.target.secret_ref:
            from .config import read_database_secret

            require(
                read_database_secret(ctx.source.secret_ref)["writerPassword"]
                == read_database_secret(ctx.target.secret_ref)["writerPassword"],
                "WRITER_PASSWORD_MISMATCH",
                "원본·대상 업무 비밀번호가 다릅니다.",
            )
        # DMS 복사에 포함할 barrier를 시작 전에 만듭니다. publication은 만들지 않습니다.
        self.driver.prepare_barrier(ctx.source, ctx.operation_id, ctx.record_intent)
        ctx.save_internal(
            schemas=self.driver.schema_signature(ctx.source),
            dmsFingerprint=observed["dms"]["fingerprint"],
        )
        self.cloud("VERIFY")
        ctx.result("PREPARED", prepared=True, targetWritesIntent=False)

    def start_sync(self):
        ctx = self.ctx
        require(
            self.driver.schema_signature(ctx.source) == ctx.internal.get("schemas"),
            "SCHEMA_MISMATCH",
            "준비 후 원본 schema가 바뀌었습니다.",
        )
        resumed = ctx.state["lifecycle"] == "RESTORED" or ctx.internal.get("dmsResumePending")
        if ctx.state["lifecycle"] == "RESTORED":
            self.begin_sync_attempt()
        elif resumed:
            self.check_resume_safety()
        self.cloud("RESUME_SYNC" if resumed else "START_SYNC")
        ctx.result("SYNCING", dmsResumePending=False)

    def check_resume_safety(self):
        ctx = self.ctx
        observed = ctx.service.inspect_resources(self.row)
        job = observed["dms"]
        require(
            not self.row["internal"].get("targetWritesIntent")
            and not self.row["internal"].get("dmsPromoted")
            and job["state"] == "STOPPED"
            and job["phase"] == "CDC"
            and not job["errorCode"]
            and observed["route"] == "SOURCE"
            and observed["access"] == "SOURCE_ONLY"
            and not observed["paused"]
            and observed["target"]["sessions"] == 0
            and all(
                observed[node][field] == 0
                for node in ["source", "target"]
                for field in ["prepared", "oldExecutors"]
            ),
            "DMS_RESUME_UNSAFE",
            "원본 단독 운영·대상 차단·승격 전 CDC 정지를 확인해야 다시 전환할 수 있습니다.",
        )

    def begin_sync_attempt(self):
        """원본 운영을 확인한 뒤 이전 복귀 이력을 보존하고 새 시도를 엽니다."""
        ctx = self.ctx
        self.check_resume_safety()

        def begin(tx):
            tx.require_lease(ctx.worker._guard)
            row = ctx.store.row(tx, ctx.migration_id)
            require(
                row["state"]["lifecycle"] == "RESTORED"
                and row["state"]["transitionState"] == "CLOSED"
                and row["state"]["activeOperationId"] == ctx.operation_id,
                "INVALID_STATE",
                "종료한 원본 복귀 시도에서만 복제를 재개합니다.",
            )
            old = row["internal"]
            previous = list(old.get("previousTransitionIds", []))
            previous.append(row["state"]["transitionId"])
            tx.put(
                "DMS_ATTEMPT",
                ctx.migration_id + "#" + ctx.operation_id,
                {"state": dict(row["state"]), "internal": dict(old), "closedAt": now()},
            )
            row["internal"] = {
                "schemas": old["schemas"],
                "dmsFingerprint": old["dmsFingerprint"],
                "prepared": True,
                "dmsResumePending": True,
                "targetWritesIntent": False,
                "previousTransitionIds": previous,
            }
            row["state"].update(lifecycle="PREPARED", transitionId=None, transitionState="NONE")
            ctx.store.persist(tx, row)

        ctx.assert_lease()
        ctx.store.transact(begin)

    def completion_evidence(self, method, **facts):
        return {
            "method": method,
            "migrationId": self.ctx.migration_id,
            "transitionId": self.row["state"]["transitionId"],
            **facts,
        }

    def require_completion(self, *, source_only=False):
        """데이터를 전수 비교하지 않고 현재 전환의 복제·복귀 완료 근거를 확인합니다."""
        ctx = self.ctx
        row = self.row
        internal = row["internal"]
        method = "SOURCE_RECOVERY" if source_only else "DMS_BARRIER"
        marker = internal.get("forwardBarrier", {}).get("id")
        evidence = internal.get("completionEvidence")
        if not evidence:
            # 이전 버전 계획의 완료 근거도 실제 상태와 대조해 새 형식으로 보존합니다.
            evidence = self.completion_evidence(method, **({"barrierId": marker} if marker else {}))
        require(
            evidence.get("migrationId") == ctx.migration_id
            and evidence.get("transitionId") == row["state"]["transitionId"]
            and evidence.get("method") == method,
            "DMS_COMPLETION_MISMATCH",
            "복제 완료 근거의 마이그레이션·전환 ID가 다릅니다.",
        )
        job = self.driver.job()
        if source_only:
            require(
                internal.get("directRecovery")
                and internal.get("forwardDetached")
                and job["state"] in {"STOPPED", "NOT_STARTED", "COMPLETED", "FAILED", "DELETED"},
                "DMS_STOP_UNCONFIRMED",
                "원본 재개 전에 DMS 정지·종료가 필요합니다.",
            )
        else:
            require(
                marker
                and evidence.get("barrierId") == marker
                and not job["errorCode"]
                and (
                    job["state"] == "COMPLETED"
                    or (job["state"] == "RUNNING" and job["phase"] == "CDC")
                    # 기존 승격을 관측하는 RECONCILE은 계속할 수 있습니다.
                    # 대상 쓰기는 CUTOVER·RESUME의 COMPLETED 조건이 따로 차단합니다.
                    or (
                        internal.get("dmsPromoted")
                        and job["state"] == "RUNNING"
                        and job["phase"] == "PROMOTE_IN_PROGRESS"
                    )
                ),
                "REPLICATION_NOT_READY",
                "현재 전환의 barrier와 DMS 완료 상태가 필요합니다.",
            )
            with self.driver.connect(ctx.target) as conn:
                require(
                    conn.execute(
                        "SELECT 1 FROM foxtrot_migration.barriers WHERE id=%s AND migration_id=%s",
                        (marker, ctx.migration_id),
                    ).fetchone(),
                    "REPLICATION_NOT_READY",
                    "최종 변경의 대상 적용을 확인하지 못했습니다.",
                )
        if not internal.get("completionEvidence"):
            ctx.save_internal(completionEvidence=evidence)
        return evidence

    def quiesce(self):
        ctx = self.ctx
        observed = ctx.service.inspect_resources(self.row)
        require(
            observed["route"] == "SOURCE" and not self.row["internal"].get("targetWritesIntent"),
            "ROLLBACK_UNAVAILABLE",
            "이 DMS 프로필은 대상 쓰기 후 역복제를 지원하지 않습니다.",
        )
        ctx.record_intent("pgbouncer.pause")
        self.driver.admin(ctx.binding, "PAUSE {}", tag=ctx.operation_id, timeout=10)
        self.driver.fence(ctx.source, ctx.operation_id, ctx.record_intent)
        ctx.require_held()
        job = self.driver.job()
        require(
            job["state"] == "RUNNING" and job["phase"] == "CDC" and not job["errorCode"],
            "REPLICATION_NOT_READY",
            "DMS의 정상 CDC 상태가 필요합니다.",
        )
        marker = uuid4()
        ctx.record_intent("dms.barrier")
        with self.driver.connect(ctx.source, ctx.operation_id) as conn:
            conn.execute("SET synchronous_commit=on")
            conn.execute(
                "INSERT INTO foxtrot_migration.barriers VALUES(%s,%s)", (marker, ctx.migration_id)
            )

        def applied():
            ctx.assert_lease()
            ctx.verify_gate()
            job = self.driver.job()
            require(
                job["state"] == "RUNNING" and job["phase"] == "CDC" and not job["errorCode"],
                "REPLICATION_NOT_READY",
                "최종 동기화 중 DMS가 정상 CDC 상태를 벗어났습니다.",
            )
            with self.driver.connect(ctx.target) as conn:
                return conn.execute(
                    "SELECT 1 FROM foxtrot_migration.barriers WHERE id=%s", (marker,)
                ).fetchone()

        self.driver.wait(applied, 20)
        ctx.verify_gate()
        ctx.result(
            "QUIESCED",
            forwardBarrier={"id": str(marker)},
            completionEvidence=self.completion_evidence("DMS_BARRIER", barrierId=str(marker)),
        )

    def cutover(self):
        ctx = self.ctx
        observed = ctx.require_held()
        require(observed["route"] == "SOURCE", "ROUTE_MISMATCH", "원본 정지 경로가 필요합니다.")
        self.require_completion()
        job = self.driver.job()
        if job["state"] != "COMPLETED":
            require(
                not self.row["internal"].get("dmsPromoted"),
                "DMS_PROMOTION_UNCONFIRMED",
                "기존 승격의 완료를 먼저 확인하십시오.",
            )
            self.cloud("PROMOTE")
            job = self.driver.job()
        else:
            # 응답 유실 후 RECONCILE이 연결한 기존 승격만 이어갑니다.
            require(
                self.row["internal"].get("dmsPromoted"),
                "DMS_PROMOTION_UNCONFIRMED",
                "승격 실행 근거가 없습니다.",
            )
        require(
            job["state"] == "COMPLETED" and not job["errorCode"],
            "DMS_PROMOTION_UNCONFIRMED",
            "DMS 승격 완료를 확인하지 못했습니다.",
        )
        ctx.save_internal(dmsPromoted=True, forwardDetached=True)
        ctx.require_held()
        self.driver.sync_sequences(ctx.source, ctx.target, ctx.operation_id, ctx.record_intent)
        self.driver.grant_target_writer(ctx.target, ctx.operation_id, ctx.record_intent)
        self.driver.reroute(ctx.binding, ctx.target, ctx.record_intent, ctx.operation_id)
        ctx.result("SWITCHED")

    def resume(self):
        from .worker import resume

        if self.ctx.request.expected_location == "TARGET":
            require(
                self.row["internal"].get("dmsPromoted")
                and self.driver.job()["state"] == "COMPLETED",
                "DMS_PROMOTION_UNCONFIRMED",
                "DMS 승격 전에는 대상 쓰기를 열지 않습니다.",
            )
        self.require_completion(source_only=self.ctx.request.expected_location == "SOURCE")
        resume(self.ctx, verify_data=False)

    def stop_replication(self):
        job = self.driver.job()
        if job["state"] == "RUNNING":
            self.cloud("STOP")
            job = self.driver.job()
        require(
            job["state"] in {"STOPPED", "NOT_STARTED", "COMPLETED", "FAILED"},
            "DMS_STOP_UNCONFIRMED",
            "DMS 정지 또는 종료를 확인하지 못했습니다.",
        )

    def rollback(self):
        ctx = self.ctx
        ctx.require_held()
        require(
            not self.row["internal"].get("targetWritesIntent"),
            "ROLLBACK_UNAVAILABLE",
            "대상 쓰기 후에는 역복제 없이 원본으로 복귀할 수 없습니다.",
        )
        self.stop_replication()
        ctx.save_internal(
            directRecovery=True,
            forwardDetached=True,
            completionEvidence=self.completion_evidence("SOURCE_RECOVERY"),
        )
        self.driver.reroute(ctx.binding, ctx.source, ctx.record_intent, ctx.operation_id)
        ctx.result("ROLLED_BACK_HELD")

    def cancel(self):
        ctx = self.ctx
        observed = ctx.service.inspect_resources(self.row)
        require(
            observed["route"] == "SOURCE"
            and observed["access"] == "SOURCE_ONLY"
            and not observed["paused"]
            and ctx.state["transitionState"] == "NONE",
            "INVALID_STATE",
            "정지 전 원본 운영 상태에서만 취소할 수 있습니다.",
        )
        self.stop_replication()
        ctx.result("CANCELLED")

    def finalize(self):
        ctx = self.ctx
        ctx.store.begin_finalization(ctx.migration_id, ctx.operation_id, ctx.service.check_finalize)
        job = self.driver.job()
        if job["state"] in {"STOPPED", "NOT_STARTED", "FAILED"}:
            # 삭제 API는 작업만 삭제합니다. 대상 인스턴스 삭제 옵션은 보내지 않습니다.
            self.cloud("DELETE")
            ctx.save_internal(dmsJobDeleted=True)
        else:
            require(
                job["state"] in {"COMPLETED", "DELETED"},
                "DMS_STOP_UNCONFIRMED",
                "실행 중인 DMS 작업은 정리 완료로 기록하지 않습니다.",
            )
        ctx.result("COMPLETED")
        ctx.store.update(
            ctx.migration_id,
            lambda row: row["state"].update(
                transitionState="CLOSED" if row["state"]["transitionId"] else "NONE"
            ),
        )

    def reconcile_cloud(self):
        ctx = self.ctx
        internal = self.row["internal"]
        request = internal.get("dmsRequest")
        if not request:
            require(
                not ctx.request.provider_operation_ref,
                "DMS_OPERATION_MISMATCH",
                "연결할 DMS 접수 기록이 없습니다.",
            )
            return
        service = self.driver.dms
        job = self.driver.client.binding.job
        with service.journal.lock(job) as lease:
            saved = service.journal.find(lease, job, request["key"])
        if not saved:
            require(
                not ctx.request.provider_operation_ref,
                "DMS_OPERATION_MISMATCH",
                "DMS 실행 의도 기록이 없습니다.",
            )
            return  # RPC 전에 기록하는 DMS journal이 없으면 변경 요청도 보내지 않았습니다.
        if saved["status"] == "UNKNOWN":
            require(
                ctx.request.provider_operation_ref,
                "DMS_SUBMISSION_UNKNOWN",
                "감사 기록에서 확인한 providerOperationRef가 필요합니다.",
            )
            saved = service.reconcile(saved["id"], ctx.request.provider_operation_ref)
        elif ctx.request.provider_operation_ref:
            require(
                ctx.request.provider_operation_ref == saved["cloud_operation"],
                "DMS_OPERATION_MISMATCH",
                "저장한 DMS 실행 ID와 다릅니다.",
            )
        if saved["status"] == "PENDING":
            saved = service.poll(saved["id"])
        require(
            saved["status"] in {"SUCCEEDED", "FAILED"},
            "DMS_OPERATION_PENDING",
            "이전 DMS 실행이 아직 끝나지 않았습니다.",
        )
        if saved["status"] == "SUCCEEDED":
            if saved["action"] == "RESUME_SYNC":
                ctx.save_internal(dmsResumePending=False)
            if saved["action"] == "PROMOTE":
                ctx.save_internal(dmsPromoted=True, forwardDetached=True)
            if saved["action"] == "VERIFY":
                ctx.save_internal(prepared=True, targetWritesIntent=False)
            if saved["action"] == "DELETE":
                ctx.save_internal(dmsJobDeleted=True)

    def reconcile(self):
        from .worker import Reconciler

        ctx = self.ctx
        self.reconcile_cloud()
        row = self.row
        internal = row["internal"]
        observed = ctx.service.inspect_resources(row)
        Reconciler(ctx.worker, ctx.migration_id)._require_executors_stopped(observed)
        route, access, paused = observed["route"], observed["access"], observed["paused"]
        require(
            route != "UNKNOWN" and access not in {"UNKNOWN", "BOTH"},
            "UNKNOWN_STATE",
            "접속 경로·쓰기 차단을 확인할 수 없습니다.",
        )
        job = observed["dms"]
        life, resume_pending = row["state"]["lifecycle"], None
        if life == "COMPLETED":
            pass
        elif route == "TARGET" and access == "TARGET_ONLY" and internal.get("targetWritesIntent"):
            require(
                internal.get("dmsPromoted") and job["state"] == "COMPLETED",
                "DMS_PROMOTION_UNCONFIRMED",
                "대상 운영의 DMS 승격 근거가 없습니다.",
            )
            life = "SWITCHED" if paused else "ACTIVE"
            resume_pending = "TARGET" if paused else None
        elif route == "SOURCE" and access == "SOURCE_ONLY" and not paused:
            if internal.get("sourceResumeIntent"):
                life = "RESTORED"
            else:
                require(
                    not row["state"]["transitionId"] and not internal.get("targetWritesIntent"),
                    "UNKNOWN_STATE",
                    "전환 중 원본 업무가 예기치 않게 재개됐습니다.",
                )
                life = (
                    "PREPARED"
                    if internal.get("dmsResumePending") and job["state"] == "STOPPED"
                    else "CANCELLED"
                    if job["state"] in {"STOPPED", "DELETED"}
                    else "READY"
                    if job["state"] == "RUNNING" and job["phase"] == "CDC"
                    else "SYNCING"
                    if job["state"] in {"STARTING", "RUNNING"}
                    else "PREPARED"
                    if internal.get("prepared")
                    else "CREATED"
                )
        elif paused and route == "SOURCE" and access == "SOURCE_ONLY":
            if internal.get("sourceResumeIntent"):
                life, resume_pending = "ROLLED_BACK_HELD", "SOURCE"
            else:
                require(
                    not internal.get("targetWritesIntent") and internal.get("prepared"),
                    "UNKNOWN_STATE",
                    "원본 정지 근거가 없습니다.",
                )
                ctx.save_internal(partialSourcePause=True)
                life = "READY"
        elif paused and access == "BLOCKED":
            ctx.require_held()
            if internal.get("targetWritesIntent"):
                previous = ctx.store.recovery_command(row)
                candidate = dict(row, internal=dict(internal, resumePending="TARGET"))
                require(
                    previous
                    and previous["action"] == "RESUME"
                    and internal.get("dmsPromoted")
                    and job["state"] == "COMPLETED"
                    and ctx.service.resume_pending(candidate, observed),
                    "UNKNOWN_STATE",
                    "대상 재개 중단 근거를 확인할 수 없습니다.",
                )
                life, resume_pending = "SWITCHED", "TARGET"
            elif internal.get("directRecovery") and internal.get("forwardDetached"):
                require(route == "SOURCE", "ROUTE_MISMATCH", "원본 복귀 경로가 다릅니다.")
                self.require_completion(source_only=True)
                life = "ROLLED_BACK_HELD"
            elif not observed.get("targetReachable", True):
                ctx.save_internal(
                    directRecovery=True,
                    completionEvidence=self.completion_evidence("SOURCE_RECOVERY"),
                )
                life = "READY"
            elif internal.get("forwardBarrier"):
                self.require_completion()
                life = "QUIESCED" if route == "SOURCE" else "SWITCHED"
            else:
                require(route == "SOURCE", "UNKNOWN_STATE", "검증 없는 대상 경로입니다.")
                life = "READY"
        else:
            raise OperatorError("UNKNOWN_STATE", "실제 DMS·프록시 상태를 대조할 수 없습니다.")

        def apply(value):
            value["state"].update(lifecycle=life, health="OK")
            previous = value["internal"].pop("unresolvedOperationId", None)
            if previous:
                value["internal"]["reconciledOperationId"] = previous
            value["internal"]["resumePending"] = resume_pending
            if life in {"ACTIVE", "RESTORED"}:
                value["internal"].setdefault("resumeAt", now().isoformat())
            if life == "RESTORED":
                value["state"]["transitionState"] = "CLOSED"

        ctx.assert_lease()
        ctx.store.update(ctx.migration_id, apply)


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
