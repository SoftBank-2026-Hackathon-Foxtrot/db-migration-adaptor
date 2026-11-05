"""단일 실행기가 복사·전환·복귀를 실행하고 실패 상태를 대조합니다."""

from __future__ import annotations

import logging
import signal
import time
from contextlib import contextmanager
from typing import Any

from .control import verify_gate
from .models import HELD_ACTIONS, Action, ActionRequest, OperatorError, require
from .service import Service
from .store import now, stamp

# 단일 실행기

log = logging.getLogger(__name__)


class Worker:
    def __init__(self, service: Service):
        self.service, self.store, self.driver = service, service.store, service.driver
        self.stopping = False
        self._guard = None

    def assert_lease(self) -> None:
        require(self._guard is not None, "EXECUTOR_NOT_LOCKED", "단일 실행기 잠금이 필요합니다.")
        self._guard.assert_owned()

    def result(self, mid: str, lifecycle: str, **internal) -> None:
        self.assert_lease()
        self.service.bump_action_result(mid, lifecycle, **internal)

    @contextmanager
    def exclusive(self):
        with self.store.exclusive() as lease:
            self._guard = lease
            try:
                yield lease
            finally:
                self._guard = None

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stopping", True))
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stopping", True))
        with self.exclusive() as guard:
            self.store.recover_interrupted()
            while not self.stopping:
                # DynamoDB 잠금 소유권을 잃으면 다음 명령을 실행하지 않습니다.
                guard.assert_owned()
                if not self.run_once():
                    time.sleep(self.service.settings.poll_seconds)

    def run_once(self) -> bool:
        self.assert_lease()
        operation = self.store.claim_operation()
        if not operation:
            return False
        try:
            self.execute(operation)
        except Exception as error:
            self._record_failure(operation, error)
        else:
            self._record_success(operation)
        return True

    def _record_failure(self, op: dict, error: Exception) -> None:
        external_intent = self.store.external_change_started(op["id"])
        status = "UNKNOWN" if external_intent else "FAILED"
        # Never log raw database errors, DSNs or query text containing secrets.
        code = error.code if isinstance(error, OperatorError) else "REMOTE_OPERATION_UNCONFIRMED"
        message = (
            error.message
            if isinstance(error, OperatorError)
            else "외부 작업 결과를 확인할 수 없습니다."
        )
        if op["action"] == "RECONCILE":
            status = "UNKNOWN"
        self.store.finish(
            op,
            status,
            {
                "code": code,
                "message": message,
                "operationId": op["id"],
                "blockers": ["RECONCILE_REQUIRED"],
            },
        )
        log.warning("operation=%s status=%s code=%s", op["id"], status, code)

    def _record_success(self, op: dict) -> None:
        self.store.finish(op, "SUCCEEDED")
        try:
            self.service.get(op["migration_id"])
        except Exception:
            log.warning("operation=%s code=OBSERVATION_REFRESH_FAILED", op["id"])

    def execute(self, operation: dict) -> None:
        context = OperationContext(self, operation)
        context.verify_gate()
        if context.request.action == Action.RECONCILE:
            self.reconcile(context.migration_id)
            note = None
        else:
            note = HANDLERS[context.request.action](context)
        context.confirm_and_observe(note)

    def reconcile(self, migration_id: str) -> None:
        Reconciler(self, migration_id).run()


# 명령 실행 환경


class OperationContext:
    def __init__(self, worker: Worker, operation: dict):
        self.worker = worker
        self.service, self.store, self.driver = worker.service, worker.store, worker.driver
        self.migration_id = operation["migration_id"]
        self.operation_id = operation["id"]
        self.request = ActionRequest.model_validate(operation["request"])
        record = self.store.get(self.migration_id)
        self.plan, self.state, self.internal = record["plan"], record["state"], record["internal"]
        self.source, self.target, self.binding = self.driver.resources(self.plan)
        self.names = self.driver.names(self.migration_id)

    def assert_lease(self) -> None:
        self.worker.assert_lease()

    def save_internal(self, **values: Any) -> None:
        self.assert_lease()
        self.store.update(self.migration_id, lambda record: record["internal"].update(values))

    def result(self, lifecycle: str, **internal: Any) -> None:
        self.worker.result(self.migration_id, lifecycle, **internal)

    def verify_gate(self) -> dict | None:
        if self.request.action not in HELD_ACTIONS:
            return None
        return self.store.transact(
            lambda tx: verify_gate(
                tx, self.store.row(tx, self.migration_id), self.request, self.service.registry
            )
        )

    def record_intent(self, phase: str) -> None:
        self.assert_lease()
        self.verify_gate()
        self.store.intent(self.operation_id, phase)

    def require_held(self, *, allow_resume: bool = False) -> dict:
        self.verify_gate()
        record = self.store.get(self.migration_id)
        observed = self.service.inspect_resources(record)
        nodes = ["source", "target"] if observed.get("targetReachable", True) else ["source"]
        partial_resume = allow_resume and self.service.resume_pending(record, observed)
        require(
            observed["paused"]
            and (observed["access"] == "BLOCKED" or partial_resume)
            and all(
                observed[node][field] == 0 for node in nodes for field in ["sessions", "prepared"]
            ),
            "WRITERS_NOT_DRAINED",
            "프록시·업무 writer의 정지를 확인할 수 없습니다.",
        )
        return observed

    def validate(self, *, source_only: bool = False) -> dict:
        baseline = self.store.get(self.migration_id)["internal"].get("schemas")
        require(baseline, "PREPARE_REQUIRED", "사전 schema 검사가 필요합니다.")
        databases = [self.source] if source_only else [self.source, self.target]
        for database in databases:
            require(
                self.driver.schema_signature(database) == baseline,
                "SCHEMA_MISMATCH",
                "이전 중 schema가 변경되었습니다.",
            )
        if source_only:
            return self.driver.snapshot(self.source)
        return self.driver.equal(self.source, self.target)

    def confirm_and_observe(self, note: str | None = None) -> None:
        self.store.confirmed(
            self.operation_id, note or str(self.request.action) + " 실제 상태 확인 완료"
        )
        observed = self.service.inspect_resources(self.store.get(self.migration_id))
        self.assert_lease()
        self.service.record_observation(self.migration_id, observed)


# 명령별 실행 순서


def prepare(ctx: OperationContext) -> str | None:
    """지원 조건과 실제 자원 소유 범위를 확인하고 복제 준비를 기록합니다."""
    observed = ctx.driver.preflight(ctx.plan)

    def claim_resources(tx):
        for key in ["source", "target"]:
            info = observed[key]
            db = ctx.source if key == "source" else ctx.target
            ctx.store.claim(
                tx, "actual-db:" + info["system_id"] + ":" + db.database, ctx.migration_id
            )
            ctx.store.claim(
                tx, "actual-role:" + info["system_id"] + ":" + db.writer_role, ctx.migration_id
            )

    ctx.store.transact(claim_resources)
    schemas = ctx.driver.schema_signature(ctx.source)
    ctx.save_internal(schemas=schemas)
    for db in [ctx.source, ctx.target]:
        ctx.driver.prepare_barrier(db, ctx.operation_id, ctx.record_intent)
    ctx.result("PREPARED", prepared=True, targetWritesIntent=False)


def start_sync(ctx: OperationContext) -> str | None:
    """준비 당시 schema를 확인한 뒤 초기 복사와 변경분 복제를 시작합니다."""
    baseline = ctx.internal.get("schemas")
    require(
        baseline
        and all(ctx.driver.schema_signature(db) == baseline for db in [ctx.source, ctx.target]),
        "SCHEMA_MISMATCH",
        "준비 후 schema가 변경됐습니다.",
    )
    ctx.driver.create_replication(
        ctx.source, ctx.target, ctx.names["forward"], True, ctx.operation_id, ctx.record_intent
    )
    ctx.result("SYNCING")


def quiesce(ctx: OperationContext) -> str | None:
    """업무를 차단하고 최종 복제·sequence·데이터를 검증합니다."""
    observed = ctx.service.inspect_resources(ctx.store.get(ctx.migration_id))
    location = observed["route"]
    require(location in {"SOURCE", "TARGET"}, "ROUTE_MISMATCH", "현재 경로가 불명입니다.")
    if location == "TARGET":
        require(
            ctx.internal.get("reversePrepared"),
            "ROLLBACK_UNAVAILABLE",
            "검증한 역복제가 없습니다.",
        )
    ctx.record_intent("pgbouncer.pause")
    evidence = ctx.verify_gate()
    remaining = min(
        20,
        ctx.plan["policy"]["maxQuiesceSeconds"] - (now() - evidence["held_since"]).total_seconds(),
    )
    if remaining <= 0 and location == "TARGET":
        remaining = 20  # Explicit reverse recovery; forward progress remains blocked.
    recovery_only = (
        location == "SOURCE"
        and ctx.internal.get("partialSourcePause")
        and (
            remaining <= 0
            or not observed.get("targetReachable", True)
            or ctx.internal.get("directRecovery")
        )
    )
    if recovery_only:
        ctx.save_internal(directRecovery=True)
        remaining = 20
    ctx.driver.admin(ctx.binding, "PAUSE {}", timeout=max(0.1, remaining), tag=ctx.operation_id)
    active, replica = (ctx.source, ctx.target) if location == "SOURCE" else (ctx.target, ctx.source)
    stream = ctx.names["forward"] if location == "SOURCE" else ctx.names["reverse"]
    ctx.driver.fence(active, ctx.operation_id, ctx.record_intent)
    ctx.require_held()
    if recovery_only:
        ctx.result("QUIESCED", validation=ctx.validate(source_only=True))
        return "기한 초과: 원본 복귀 전용 정지 완료"
    checkpoint = "forwardBarrier" if location == "SOURCE" else "reverseBarrier"
    if ctx.driver.replication_ready(replica, stream):
        barrier = ctx.driver.barrier(
            active,
            replica,
            stream,
            ctx.migration_id,
            ctx.operation_id,
            ctx.record_intent,
            min(20, remaining),
        )
        ctx.save_internal(**{checkpoint: barrier})
        ctx.driver.disable(replica, stream, ctx.operation_id, ctx.record_intent, min(10, remaining))
    else:
        require(
            ctx.internal.get(checkpoint),
            "REPLICATION_NOT_READY",
            "최종 복제 적용 근거가 없습니다.",
        )
    ctx.driver.sync_sequences(active, replica, ctx.operation_id, ctx.record_intent)
    validation = ctx.validate()
    ctx.result("QUIESCED", validation=validation, reverseValidated=location == "TARGET")


def cutover(ctx: OperationContext) -> str | None:
    """쓰기를 막은 상태에서 역복제를 준비하고 프록시 경로를 바꿉니다."""
    observed = ctx.require_held()
    require(observed["route"] == "SOURCE", "ROUTE_MISMATCH", "원본 정지 경로가 필요합니다.")
    require(
        observed.get("targetReachable", True) and not ctx.internal.get("directRecovery"),
        "ROLLBACK_ONLY",
        "대상 상태를 확인할 수 없거나 원본 복귀를 시작했습니다.",
    )
    ctx.validate()
    if ctx.plan["policy"]["rollbackMode"] == "REVERSE_REQUIRED":
        ctx.driver.create_replication(
            ctx.target, ctx.source, ctx.names["reverse"], False, ctx.operation_id, ctx.record_intent
        )
        ctx.driver.wait(lambda: ctx.driver.replication_ready(ctx.source, ctx.names["reverse"]), 20)
        ctx.save_internal(reversePrepared=True)
    ctx.driver.retire_publisher(
        ctx.source, ctx.names["forward"], ctx.operation_id, ctx.record_intent
    )
    ctx.save_internal(forwardDetached=True)
    ctx.driver.reroute(ctx.binding, ctx.target, ctx.record_intent, ctx.operation_id)
    observed = ctx.driver.observe(ctx.plan)
    require(
        observed["route"] == "TARGET" and observed["paused"] and observed["access"] == "BLOCKED",
        "ROUTE_MISMATCH",
        "대상 경로 또는 업무 정지를 확인할 수 없습니다.",
    )
    ctx.result("SWITCHED")


def resume(ctx: OperationContext) -> str | None:
    """검증한 위치의 DB 접속을 엽니다. 앱 업무 재개는 오케스트레이터가 수행합니다."""
    observed = ctx.require_held(allow_resume=True)
    require(
        observed["route"] == ctx.request.expected_location,
        "ROUTE_MISMATCH",
        "재개 위치가 다릅니다.",
    )
    location = ctx.request.expected_location
    require(
        location == "SOURCE" or not ctx.internal.get("directRecovery"),
        "ROLLBACK_ONLY",
        "원본 복귀를 시작한 계획은 대상 쓰기를 열지 않습니다.",
    )
    if location == "SOURCE" and ctx.internal.get("directRecovery"):
        require(
            ctx.internal.get("forwardDetached"),
            "REPLICATION_NOT_STOPPED",
            "원본의 정방향 복제를 정리해야 합니다.",
        )
    ctx.validate(source_only=location == "SOURCE" and ctx.internal.get("directRecovery", False))
    if location == "TARGET":
        if ctx.plan["policy"]["rollbackMode"] == "REVERSE_REQUIRED":
            require(
                ctx.internal.get("reversePrepared"),
                "ROLLBACK_UNAVAILABLE",
                "역복제가 준비되지 않았습니다.",
            )
            if not ctx.driver.replication_ready(ctx.source, ctx.names["reverse"]):
                ctx.driver.enable_replication(
                    ctx.source, ctx.names["reverse"], ctx.operation_id, ctx.record_intent, 15
                )

        # Durable intent MUST precede permission change and proxy resume.
        def target_intent(value):
            value["internal"]["targetWritesIntent"] = True
            value["internal"]["reverseValidated"] = False
            value["internal"].pop("reverseBarrier", None)
            value["state"]["observed"]["targetWritesPossible"] = True

        ctx.store.update(ctx.migration_id, target_intent)
        ctx.driver.resume(ctx.target, ctx.binding, ctx.operation_id, ctx.record_intent)
        ctx.result("ACTIVE", resumeAt=stamp(), reverseValidated=False, resumePending=None)
    else:
        require(
            not ctx.internal.get("targetWritesIntent") or ctx.internal.get("reverseValidated"),
            "ROLLBACK_UNAVAILABLE",
            "원본의 최신성을 증명할 수 없습니다.",
        )
        ctx.save_internal(sourceResumeIntent=True)
        ctx.driver.resume(ctx.source, ctx.binding, ctx.operation_id, ctx.record_intent)
        ctx.result("RESTORED", resumeAt=stamp(), resumePending=None)
        ctx.store.update(ctx.migration_id, lambda r: r["state"].update(transitionState="CLOSED"))


def rollback(ctx: OperationContext) -> str | None:
    """원본의 최신성을 검증하고 원본 경로로 복귀합니다. 쓰기는 계속 막습니다."""
    ctx.require_held()
    require(
        not ctx.internal.get("targetWritesIntent") or ctx.internal.get("reverseValidated"),
        "ROLLBACK_UNAVAILABLE",
        "역복제 검증이 필요합니다.",
    )
    if not ctx.internal.get("targetWritesIntent"):
        # Source remains the authoritative DB; an incomplete/corrupt unused target
        # must not prevent returning service to the verified original source.
        ctx.save_internal(directRecovery=True)
        ctx.driver.retire_publisher(
            ctx.source, ctx.names["forward"], ctx.operation_id, ctx.record_intent
        )
        ctx.save_internal(forwardDetached=True, validation=ctx.validate(source_only=True))
    else:
        ctx.validate()
    if ctx.internal.get("reversePrepared"):
        ctx.driver.disable(
            ctx.source, ctx.names["reverse"], ctx.operation_id, ctx.record_intent, 10
        )
    ctx.driver.reroute(ctx.binding, ctx.source, ctx.record_intent, ctx.operation_id)
    observed = ctx.service.inspect_resources(ctx.store.get(ctx.migration_id))
    require(
        observed["route"] == "SOURCE" and observed["paused"] and observed["access"] == "BLOCKED",
        "ROUTE_MISMATCH",
        "원본 복귀 경로를 확인할 수 없습니다.",
    )
    ctx.result("ROLLED_BACK_HELD")


def cancel(ctx: OperationContext) -> str | None:
    """업무 정지 전의 복제를 멈추고 계획을 취소합니다."""
    observed = ctx.driver.observe(ctx.plan)
    require(
        observed["route"] == "SOURCE"
        and observed["access"] == "SOURCE_ONLY"
        and not observed["paused"]
        and ctx.state["transitionState"] == "NONE",
        "INVALID_STATE",
        "업무 정지 전 원본 운영 상태에서만 취소할 수 있습니다.",
    )
    ctx.driver.disable(ctx.target, ctx.names["forward"], ctx.operation_id, ctx.record_intent, 10)
    ctx.result("CANCELLED")


def finalize(ctx: OperationContext) -> str | None:
    """최신 근거를 승인한 뒤 자기 계획의 복제 객체만 정리합니다."""
    ctx.assert_lease()
    ctx.store.begin_finalization(ctx.migration_id, ctx.operation_id, ctx.service.check_finalize)
    ctx.driver.cleanup_replication(
        ctx.source, ctx.target, ctx.names["forward"], ctx.operation_id, ctx.record_intent
    )
    ctx.save_internal(finalizeForwardDone=True)
    ctx.driver.cleanup_replication(
        ctx.target, ctx.source, ctx.names["reverse"], ctx.operation_id, ctx.record_intent
    )
    ctx.save_internal(finalizeReverseDone=True)
    ctx.result("COMPLETED")
    ctx.store.update(
        ctx.migration_id,
        lambda r: r["state"].update(
            transitionState="CLOSED" if r["state"]["transitionId"] else "NONE"
        ),
    )
    ctx.store.release_claims(ctx.migration_id)


HANDLERS = {
    Action.PREPARE: prepare,
    Action.START_SYNC: start_sync,
    Action.QUIESCE: quiesce,
    Action.CUTOVER: cutover,
    Action.RESUME: resume,
    Action.ROLLBACK: rollback,
    Action.CANCEL: cancel,
    Action.FINALIZE: finalize,
}


# 실패 후 상태 대조


class Reconciler:
    def __init__(self, worker: Worker, migration_id: str):
        self.worker = worker
        self.service, self.store, self.driver = worker.service, worker.store, worker.driver
        self.migration_id = migration_id
        self.record = self.store.get(migration_id)
        self.plan, self.state, self.internal = (
            self.record["plan"],
            self.record["state"],
            self.record["internal"],
        )
        self.resume_location: str | None = None

    def run(self) -> None:
        observed = self.service.inspect_resources(self.record)
        self._require_executors_stopped(observed)
        require(
            observed["route"] != "UNKNOWN" and observed["access"] != "BOTH",
            "UNKNOWN_STATE",
            "접속 경로 또는 쓰기 권한이 충돌합니다.",
        )
        self.previous_command = self.store.recovery_command(self.record)
        lifecycle = self._match_state(observed)
        self.worker.assert_lease()
        self.store.update(self.migration_id, lambda record: self._apply(record, lifecycle))
        if lifecycle == "COMPLETED":
            self.store.release_claims(self.migration_id)

    def _require_executors_stopped(self, observed: dict) -> None:
        require(
            observed["source"]["oldExecutors"] == 0
            and (
                not observed.get("targetReachable", True) or observed["target"]["oldExecutors"] == 0
            ),
            "EXECUTOR_STILL_ACTIVE",
            "이전 원격 명령이 아직 실행 중입니다.",
        )
        _, _, binding = self.driver.resources(self.plan)
        clients = self.driver.admin(binding, "SHOW CLIENTS")
        require(
            not any(
                str(client.get("application_name", "")).startswith("foxtrot-db-admin:op-")
                for client in clients
            ),
            "EXECUTOR_STILL_ACTIVE",
            "이전 PgBouncer 관리 연결이 남아 있습니다.",
        )

    def _match_state(self, observed: dict) -> str:
        lifecycle = self.state["lifecycle"]
        if lifecycle in {"COMPLETED", "CANCELLED"}:
            return lifecycle
        route, access, paused = observed["route"], observed["access"], observed["paused"]
        if route == "TARGET" and access == "TARGET_ONLY" and not paused:
            require(
                self.internal.get("targetWritesIntent"),
                "UNKNOWN_STATE",
                "대상 쓰기의 실행 의도가 없습니다.",
            )
            return "ACTIVE"
        if route == "SOURCE" and access == "SOURCE_ONLY" and not paused:
            return self._source_running()
        if paused and route == "SOURCE" and access == "SOURCE_ONLY":
            return self._source_paused(observed)
        if paused and access == "BLOCKED":
            return self._writers_blocked(observed)
        if (
            paused
            and route == "TARGET"
            and access == "TARGET_ONLY"
            and self.internal.get("targetWritesIntent")
        ):
            return self._target_paused(observed)
        raise OperatorError("UNKNOWN_STATE", "실제 상태를 안전한 진행 상태로 대조할 수 없습니다.")

    def _source_running(self) -> str:
        if self.internal.get("targetWritesIntent") or self.internal.get("sourceResumeIntent"):
            require(
                self.internal.get("sourceResumeIntent"),
                "UNKNOWN_STATE",
                "원본 재개 의도가 없습니다.",
            )
            return "RESTORED"
        if self.state["transitionId"]:
            raise OperatorError("UNKNOWN_STATE", "전환 중 원본 업무가 예기치 않게 재개되었습니다.")
        if not self.internal.get("prepared"):
            return self.state["lifecycle"]
        _, target, _ = self.driver.resources(self.plan)
        name = self.driver.names(self.migration_id)["forward"]
        with self.driver.connect(target) as connection:
            subscription = connection.execute(
                "SELECT subenabled FROM pg_subscription WHERE subname=%s", (name,)
            ).fetchone()
        return "SYNCING" if subscription and subscription["subenabled"] else "PREPARED"

    def _source_paused(self, observed: dict) -> str:
        previous = self.previous_command
        require(
            previous and self.state["transitionState"] == "OPEN",
            "UNKNOWN_STATE",
            "중단된 명령의 실행 기록이 필요합니다.",
        )
        if self.internal.get("sourceResumeIntent") and previous["action"] == "RESUME":
            self._require_partial_resume("SOURCE", observed)
            return "ROLLED_BACK_HELD"
        require(
            previous["action"] == "QUIESCE"
            and previous["phase"] in {"pgbouncer.pause", "pg.fence_writer"}
            and self.internal.get("prepared")
            and not self.internal.get("targetWritesIntent")
            and not self.internal.get("sourceResumeIntent"),
            "UNKNOWN_STATE",
            "원본 정지 중단 근거가 없습니다.",
        )
        self.store.update(
            self.migration_id, lambda record: record["internal"].update(partialSourcePause=True)
        )
        return "READY"

    def _writers_blocked(self, observed: dict) -> str:
        if (
            observed["route"] == "TARGET"
            and self.internal.get("targetWritesIntent")
            and not self.internal.get("reverseValidated")
        ):
            if (
                self.previous_command
                and self.previous_command["action"] == "RESUME"
                and self.internal.get("validation")
            ):
                self._require_partial_resume("TARGET", observed)
            else:
                self._discard_validation()
            return "SWITCHED"
        if self.internal.get("directRecovery") and self.internal.get("validation"):
            return self._authoritative_source(observed)
        if not observed.get("targetReachable", True):
            self._discard_validation()
            return "READY"
        if self.internal.get("validation"):
            source, target, _ = self.driver.resources(self.plan)
            self.driver.equal(source, target)
            return "QUIESCED" if observed["route"] == "SOURCE" else "SWITCHED"
        return "READY"

    def _authoritative_source(self, observed: dict) -> str:
        source, _, _ = self.driver.resources(self.plan)
        require(
            self.driver.schema_signature(source) == self.internal.get("schemas"),
            "SCHEMA_MISMATCH",
            "원본 schema가 변경됐습니다.",
        )
        require(
            self.driver.snapshot(source) == self.internal["validation"],
            "DATA_MISMATCH",
            "정지 중 원본 데이터가 변경됐습니다.",
        )
        if observed["route"] != "SOURCE":
            return "SWITCHED"
        return "ROLLED_BACK_HELD" if self.internal.get("forwardDetached") else "READY"

    def _target_paused(self, observed: dict) -> str:
        previous = self.previous_command
        require(previous, "UNKNOWN_STATE", "대상 재개 또는 정지 중단 근거가 없습니다.")
        if (
            previous["action"] == "QUIESCE"
            and self.internal.get("reversePrepared")
            and previous["phase"] in {"pgbouncer.pause", "pg.fence_writer"}
        ):
            self._discard_validation()
        else:
            require(
                previous["action"] == "RESUME", "UNKNOWN_STATE", "대상 재개 중단 근거가 없습니다."
            )
            self._require_partial_resume("TARGET", observed)
        return "SWITCHED"

    def _require_partial_resume(self, location: str, observed: dict) -> None:
        candidate = dict(self.record, internal=dict(self.internal, resumePending=location))
        require(
            self.service.resume_pending(candidate, observed),
            "WRITERS_NOT_DRAINED",
            "남은 업무 연결을 먼저 정리하십시오.",
        )
        self.resume_location = location

    def _discard_validation(self) -> None:
        self.store.update(
            self.migration_id, lambda record: record["internal"].pop("validation", None)
        )

    def _apply(self, record: dict, lifecycle: str) -> None:
        record["state"].update(lifecycle=lifecycle, health="OK")
        record["internal"]["resumePending"] = self.resume_location
        if lifecycle == "RESTORED":
            record["state"]["transitionState"] = "CLOSED"
        previous = record["internal"].pop("unresolvedOperationId", None)
        if previous:
            record["internal"]["reconciledOperationId"] = previous
        record["internal"]["reconciledAt"] = stamp()
        if lifecycle in {"ACTIVE", "RESTORED"} and (
            lifecycle != self.state["lifecycle"] or not self.internal.get("resumeAt")
        ):
            record["internal"]["resumeAt"] = stamp()
