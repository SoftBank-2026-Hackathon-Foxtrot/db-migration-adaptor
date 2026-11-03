from __future__ import annotations

import logging
import signal
import time
from contextlib import contextmanager
from datetime import datetime

from psycopg import sql

from .control import verify_gate
from .models import HELD_ACTIONS, Action, ActionRequest, OperatorError, require
from .service import Service
from .store import now, stamp

log = logging.getLogger(__name__)


class Worker:
    def __init__(self, service: Service):
        self.service, self.store, self.driver = service, service.store, service.driver
        self.stopping = False
        self._guard = None

    def assert_lease(self) -> None:
        require(self._guard is not None, "EXECUTOR_NOT_LOCKED", "단일 실행기 잠금이 필요합니다.")
        row = self._guard.execute(
            "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND "
            "pid=pg_backend_pid() AND classid=7126432 AND objid=1 AND objsubid=2) AS held"
        ).fetchone()
        require(row["held"], "EXECUTOR_LOCK_LOST", "실행기 잠금을 잃었습니다.")

    def result(self, mid: str, lifecycle: str, **internal) -> None:
        self.assert_lease()
        self.service.bump_action_result(mid, lifecycle, **internal)

    @contextmanager
    def exclusive(self):
        # The session is held for the lifetime of the designated executor.
        with self.store.connect(autocommit=True) as conn:
            acquired = conn.execute(
                "SELECT pg_try_advisory_lock(7126432,1) AS acquired"
            ).fetchone()["acquired"]
            require(acquired, "EXECUTOR_BUSY", "다른 DB 실행기가 동작 중입니다.")
            self._guard = conn
            try:
                yield conn
            finally:
                self._guard = None
                if not conn.closed:
                    conn.execute("SELECT pg_advisory_unlock(7126432,1)")

    def run(self) -> None:
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "stopping", True))
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "stopping", True))
        with self.exclusive() as guard:
            self.store.recover_interrupted()
            while not self.stopping:
                # Do not accept another command after losing the management lock session.
                guard.execute("SELECT 1")
                if not self.run_once():
                    time.sleep(self.service.settings.poll_seconds)

    def run_once(self) -> bool:
        self.assert_lease()
        op = self.store.claim_operation()
        if not op:
            return False
        try:
            self.execute(op)
        except Exception as error:
            saved = self.store.operation(op["id"])
            with self.store.connect() as conn:
                record = conn.execute(
                    "SELECT external_intent FROM foxtrot_operator.operations WHERE id=%s",
                    (op["id"],),
                ).fetchone()
            status = "UNKNOWN" if record["external_intent"] else "FAILED"
            # Never log raw database errors, DSNs or query text containing secrets.
            code = (
                error.code if isinstance(error, OperatorError) else "REMOTE_OPERATION_UNCONFIRMED"
            )
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
                    "operationId": saved["operationId"],
                    "blockers": ["RECONCILE_REQUIRED"],
                },
            )
            log.warning("operation=%s status=%s code=%s", op["id"], status, code)
        else:
            self.store.finish(op, "SUCCEEDED")
            self.service.get(op["migration_id"])
        return True

    def execute(self, op: dict) -> None:
        mid = op["migration_id"]
        row = self.store.get(mid)
        request = ActionRequest.model_validate(op["request"])
        plan, state, internal = row["plan"], row["state"], row["internal"]
        source, target, binding = self.driver.resources(plan)
        names, tag = self.driver.names(mid), op["id"]

        def update(**values):
            self.assert_lease()
            self.store.update(mid, lambda r: r["internal"].update(values))

        def gate():
            if request.action in HELD_ACTIONS:
                with self.store.connect() as conn:
                    return verify_gate(
                        conn, self.store.row(conn, mid), request, self.service.registry
                    )
            return None

        def intent(phase: str):
            self.assert_lease()
            gate()
            self.store.intent(op["id"], phase)

        def held():
            gate()
            observed = self.driver.observe(plan)
            require(
                observed["paused"]
                and observed["access"] == "BLOCKED"
                and observed["source"]["sessions"] == observed["target"]["sessions"] == 0
                and observed["source"]["prepared"] == observed["target"]["prepared"] == 0,
                "WRITERS_NOT_DRAINED",
                "프록시·업무 writer의 정지를 확인할 수 없습니다.",
            )
            return observed

        def validate(*, source_only=False):
            baseline = self.store.get(mid)["internal"].get("schemas")
            require(baseline, "PREPARE_REQUIRED", "사전 schema 검사가 필요합니다.")
            for db in [source] if source_only else [source, target]:
                actual = {
                    t: self.driver.table_signature(db, t) for t in self.service.registry.tables
                }
                require(actual == baseline, "SCHEMA_MISMATCH", "이전 중 schema가 변경되었습니다.")
            return (
                self.driver.snapshot(source) if source_only else self.driver.equal(source, target)
            )

        gate()
        action = request.action
        if action == Action.PREPARE:
            observed = self.driver.preflight(plan)
            with self.store.connect() as conn:
                for key in ["source", "target"]:
                    info = observed[key]
                    db = source if key == "source" else target
                    self.store.claim(
                        conn, "actual-db:" + info["system_id"] + ":" + db.database, mid
                    )
                    self.store.claim(
                        conn, "actual-role:" + info["system_id"] + ":" + db.writer_role, mid
                    )
            schemas = {
                t: self.driver.table_signature(source, t) for t in self.service.registry.tables
            }
            update(schemas=schemas)
            for db in [source, target]:
                self.driver.prepare_barrier(db, tag, intent)
            self.result(mid, "PREPARED", prepared=True, targetWritesIntent=False)
        elif action == Action.START_SYNC:
            self.driver.create_replication(source, target, names["forward"], True, tag, intent)
            self.result(mid, "SYNCING")
        elif action == Action.QUIESCE:
            observed = self.driver.observe(plan)
            location = observed["route"]
            require(location in {"SOURCE", "TARGET"}, "ROUTE_MISMATCH", "현재 경로가 불명입니다.")
            if location == "TARGET":
                require(
                    internal.get("reversePrepared"),
                    "ROLLBACK_UNAVAILABLE",
                    "검증한 역복제가 없습니다.",
                )
            intent("pgbouncer.pause")
            evidence = gate()
            remaining = min(
                20,
                plan["policy"]["maxQuiesceSeconds"]
                - (now() - evidence["held_since"]).total_seconds(),
            )
            if remaining <= 0 and location == "TARGET":
                remaining = 20  # Explicit reverse recovery; forward progress remains blocked.
            self.driver.admin(binding, "PAUSE {}", timeout=max(0.1, remaining), tag=tag)
            active, replica = (source, target) if location == "SOURCE" else (target, source)
            stream = names["forward"] if location == "SOURCE" else names["reverse"]
            self.driver.fence(active, tag, intent)
            held()
            checkpoint = "forwardBarrier" if location == "SOURCE" else "reverseBarrier"
            if self.driver.replication_ready(replica, stream):
                barrier = self.driver.barrier(
                    active, replica, stream, mid, tag, intent, min(20, remaining)
                )
                update(**{checkpoint: barrier})
                self.driver.disable(replica, stream, tag, intent, min(10, remaining))
            else:
                require(
                    internal.get(checkpoint),
                    "REPLICATION_NOT_READY",
                    "최종 복제 적용 근거가 없습니다.",
                )
            self.driver.sync_sequences(active, replica, tag, intent)
            validation = validate()
            self.result(
                mid, "QUIESCED", validation=validation, reverseValidated=location == "TARGET"
            )
        elif action == Action.CUTOVER:
            observed = held()
            require(observed["route"] == "SOURCE", "ROUTE_MISMATCH", "원본 정지 경로가 필요합니다.")
            validate()
            if plan["policy"]["rollbackMode"] == "REVERSE_REQUIRED":
                self.driver.create_replication(target, source, names["reverse"], False, tag, intent)
                self.driver.wait(
                    lambda: self.driver.replication_ready(source, names["reverse"]), 20
                )
                update(reversePrepared=True)
            self.driver.reroute(binding, target, intent, tag)
            observed = self.driver.observe(plan)
            require(
                observed["route"] == "TARGET"
                and observed["paused"]
                and observed["access"] == "BLOCKED",
                "ROUTE_MISMATCH",
                "대상 경로 또는 업무 정지를 확인할 수 없습니다.",
            )
            self.result(mid, "SWITCHED")
        elif action == Action.RESUME:
            observed = held()
            require(
                observed["route"] == request.expected_location,
                "ROUTE_MISMATCH",
                "재개 위치가 다릅니다.",
            )
            location = request.expected_location
            validate(source_only=location == "SOURCE" and internal.get("directRecovery", False))
            if location == "TARGET":
                if plan["policy"]["rollbackMode"] == "REVERSE_REQUIRED":
                    require(
                        internal.get("reversePrepared"),
                        "ROLLBACK_UNAVAILABLE",
                        "역복제가 준비되지 않았습니다.",
                    )
                    if not self.driver.replication_ready(source, names["reverse"]):
                        intent("pg.enable_reverse")
                        with self.driver.connect(source, tag) as conn:
                            conn.execute(
                                sql.SQL("ALTER SUBSCRIPTION {} ENABLE").format(
                                    sql.Identifier(names["reverse"])
                                )
                            )
                        self.driver.wait(
                            lambda: self.driver.replication_ready(source, names["reverse"]), 15
                        )

                # Durable intent MUST precede permission change and proxy resume.
                def target_intent(value):
                    value["internal"]["targetWritesIntent"] = True
                    value["internal"]["reverseValidated"] = False
                    value["internal"].pop("reverseBarrier", None)
                    value["state"]["observed"]["targetWritesPossible"] = True

                self.store.update(mid, target_intent)
                self.driver.resume(target, binding, tag, intent)
                self.result(mid, "ACTIVE", resumeAt=stamp(), reverseValidated=False)
            else:
                require(
                    not internal.get("targetWritesIntent") or internal.get("reverseValidated"),
                    "ROLLBACK_UNAVAILABLE",
                    "원본의 최신성을 증명할 수 없습니다.",
                )
                update(sourceResumeIntent=True)
                self.driver.resume(source, binding, tag, intent)
                self.result(mid, "RESTORED", resumeAt=stamp())
                self.store.update(mid, lambda r: r["state"].update(transitionState="CLOSED"))
        elif action == Action.ROLLBACK:
            held()
            require(
                not internal.get("targetWritesIntent") or internal.get("reverseValidated"),
                "ROLLBACK_UNAVAILABLE",
                "역복제 검증이 필요합니다.",
            )
            if not internal.get("targetWritesIntent"):
                # Source remains the authoritative DB; an incomplete/corrupt unused target
                # must not prevent returning service to the verified original source.
                self.driver.disable(target, names["forward"], tag, intent, 10)
                update(validation=validate(source_only=True), directRecovery=True)
            else:
                validate()
            if internal.get("reversePrepared"):
                self.driver.disable(source, names["reverse"], tag, intent, 10)
            self.driver.reroute(binding, source, intent, tag)
            observed = self.driver.observe(plan)
            require(
                observed["route"] == "SOURCE"
                and observed["paused"]
                and observed["access"] == "BLOCKED",
                "ROUTE_MISMATCH",
                "원본 복귀 경로를 확인할 수 없습니다.",
            )
            self.result(mid, "ROLLED_BACK_HELD")
        elif action == Action.CANCEL:
            observed = self.driver.observe(plan)
            require(
                observed["route"] == "SOURCE"
                and observed["access"] == "SOURCE_ONLY"
                and not observed["paused"]
                and state["transitionState"] == "NONE",
                "INVALID_STATE",
                "업무 정지 전 원본 운영 상태에서만 취소할 수 있습니다.",
            )
            self.driver.disable(target, names["forward"], tag, intent, 10)
            self.result(mid, "CANCELLED")
        elif action == Action.FINALIZE:
            # Repeat admission-time retention/readiness checks at execution time.
            since = internal.get("resumeAt")
            require(
                state["lifecycle"] == "CANCELLED"
                or (
                    since
                    and (now() - datetime.fromisoformat(since)).total_seconds()
                    >= self.service.settings.retention_seconds
                ),
                "RETENTION_ACTIVE",
                "롤백 보존 기간이 남아 있습니다.",
            )
            observed = self.driver.observe(plan)
            expected = "TARGET" if state["lifecycle"] == "ACTIVE" else "SOURCE"
            require(
                observed["route"] == expected
                and not observed["paused"]
                and observed["access"] == expected + "_ONLY",
                "PRECONDITION_FAILED",
                "단독 운영 상태를 확인할 수 없습니다.",
            )
            update(finalizeStarted=True)
            self.driver.cleanup_replication(source, target, names["forward"], tag, intent)
            self.driver.cleanup_replication(target, source, names["reverse"], tag, intent)
            self.result(mid, "COMPLETED")
            self.store.update(
                mid,
                lambda r: r["state"].update(
                    transitionState="CLOSED" if r["state"]["transitionId"] else "NONE"
                ),
            )
            with self.store.connect() as conn:
                conn.execute("DELETE FROM foxtrot_operator.claims WHERE migration_id=%s", (mid,))
        elif action == Action.RECONCILE:
            self.reconcile(mid)
        self.store.confirmed(op["id"], str(action) + " 실제 상태 확인 완료")
        observed = self.driver.observe(plan)
        self.assert_lease()
        self.service.record_observation(mid, observed)

    def reconcile(self, mid: str) -> None:
        row = self.store.get(mid)
        plan, state, internal = row["plan"], row["state"], row["internal"]
        observed = self.driver.observe(plan)
        require(
            not observed["source"]["oldExecutors"] and not observed["target"]["oldExecutors"],
            "EXECUTOR_STILL_ACTIVE",
            "이전 원격 명령이 아직 실행 중입니다.",
        )
        _, _, binding = self.driver.resources(plan)
        clients = self.driver.admin(binding, "SHOW CLIENTS")
        require(
            not any(
                str(c.get("application_name", "")).startswith("foxtrot-db-admin:op-")
                for c in clients
            ),
            "EXECUTOR_STILL_ACTIVE",
            "이전 PgBouncer 관리 연결이 남아 있습니다.",
        )
        require(
            observed["route"] != "UNKNOWN" and observed["access"] != "BOTH",
            "UNKNOWN_STATE",
            "접속 경로 또는 쓰기 권한이 충돌합니다.",
        )
        life = state["lifecycle"]
        if life in {"COMPLETED", "CANCELLED"}:
            # Terminal plans must never regain control after releasing resource claims.
            pass
        elif (
            observed["route"] == "TARGET"
            and observed["access"] == "TARGET_ONLY"
            and not observed["paused"]
        ):
            require(
                internal.get("targetWritesIntent"),
                "UNKNOWN_STATE",
                "대상 쓰기의 실행 의도가 없습니다.",
            )
            life = "ACTIVE"
        elif (
            observed["route"] == "SOURCE"
            and observed["access"] == "SOURCE_ONLY"
            and not observed["paused"]
        ):
            if internal.get("targetWritesIntent") or internal.get("sourceResumeIntent"):
                require(
                    internal.get("sourceResumeIntent"),
                    "UNKNOWN_STATE",
                    "원본 재개 의도가 없습니다.",
                )
                life = "RESTORED"
            elif state["transitionId"]:
                raise OperatorError(
                    "UNKNOWN_STATE", "전환 중 원본 업무가 예기치 않게 재개되었습니다."
                )
            elif internal.get("prepared"):
                source, target, _ = self.driver.resources(plan)
                name = self.driver.names(mid)["forward"]
                with self.driver.connect(target) as conn:
                    sub = conn.execute(
                        "SELECT subenabled FROM pg_subscription WHERE subname=%s", (name,)
                    ).fetchone()
                life = "SYNCING" if sub and sub["subenabled"] else "PREPARED"
        elif observed["paused"] and observed["access"] == "BLOCKED":
            if (
                observed["route"] == "TARGET"
                and internal.get("targetWritesIntent")
                and not internal.get("reverseValidated")
            ):
                life = "SWITCHED"
                # Only a fresh QUIESCE may prove reverse progress, data and sequences.
                self.store.update(mid, lambda value: value["internal"].pop("validation", None))
            elif internal.get("validation"):
                source, target, _ = self.driver.resources(plan)
                self.driver.equal(source, target)
                life = "QUIESCED" if observed["route"] == "SOURCE" else "SWITCHED"
            else:
                # Resume the same QUIESCE attempt; never manufacture a validation PASS.
                life = "READY"
        elif (
            observed["paused"]
            and observed["route"] == "TARGET"
            and observed["access"] == "TARGET_ONLY"
            and internal.get("targetWritesIntent")
        ):
            life = "SWITCHED"
        else:
            raise OperatorError(
                "UNKNOWN_STATE", "실제 상태를 안전한 진행 상태로 대조할 수 없습니다."
            )

        def update(value):
            value["state"].update(lifecycle=life, health="OK")
            if life == "RESTORED":
                value["state"]["transitionState"] = "CLOSED"
            previous = value["internal"].pop("unresolvedOperationId", None)
            value["internal"]["reconciledOperationId"] = previous
            value["internal"]["reconciledAt"] = stamp()
            if life in {"ACTIVE", "RESTORED"} and (
                life != state["lifecycle"] or not internal.get("resumeAt")
            ):
                value["internal"]["resumeAt"] = stamp()

        self.assert_lease()
        self.store.update(mid, update)
        if life == "COMPLETED":
            # Only management bookkeeping; no external resource change in RECONCILE.
            with self.store.connect() as conn:
                conn.execute("DELETE FROM foxtrot_operator.claims WHERE migration_id=%s", (mid,))
