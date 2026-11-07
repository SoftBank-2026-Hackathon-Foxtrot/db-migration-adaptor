"""Redis와 같은 명령 의미로 DB 정지·전환·재개를 실행합니다."""

from .models import Action, require
from .store import stamp
from .worker import HANDLERS, Reconciler, cutover, quiesce, resume, rollback


def confirm_open(ctx, location, observed=None):
    row = ctx.store.get(ctx.migration_id)
    observed = observed or ctx.service.inspect_resources(row)
    internal = row["internal"]
    require(
        observed["route"] == location
        and observed["access"] == location + "_ONLY"
        and not observed["paused"],
        "ROUTE_MISMATCH",
        "요청한 DB 경로의 쓰기 재개를 확인할 수 없습니다.",
    )
    if location == "TARGET":
        require(
            internal.get("targetWritesIntent")
            and internal.get("forwardDetached")
            and internal.get("validation")
            and observed["source"]["sessions"] == 0
            and observed["source"]["prepared"] == 0,
            "UNKNOWN_STATE",
            "대상 재개 근거와 원본 차단을 확인할 수 없습니다.",
        )
        # 재개 후 대상 sequence와 데이터는 정상 쓰기로 달라질 수 있습니다.
        ctx.driver.validate_record(row, verify_sequences=False)
    else:
        require(
            internal.get("sourceResumeIntent") and not internal.get("targetWritesIntent"),
            "ROLLBACK_UNAVAILABLE",
            "원본 재개 근거가 없거나 대상 쓰기를 허용했습니다.",
        )
        ctx.driver.validate_record(row, source_only=True)


class AtomicExecutor:
    def __init__(self, ctx):
        self.ctx = ctx

    def refresh(self):
        row = self.ctx.store.get(self.ctx.migration_id)
        self.ctx.state, self.ctx.internal = row["state"], row["internal"]

    def run(self):
        action = self.ctx.request.action
        if action == Action.QUIESCE:
            return self.check_ready()
        if action == Action.CUTOVER:
            return self.switch()
        if action == Action.RESUME:
            return self.confirm()
        if action == Action.ROLLBACK:
            return self.restore()
        if action == Action.CANCEL:
            return self.cancel()
        if action == Action.RECONCILE:
            AtomicReconciler(self.ctx).run()
            return "실제 경로·쓰기 권한·최종 복제 근거 대조 완료"
        return HANDLERS[action](self.ctx)

    def check_ready(self):
        ctx = self.ctx
        observed = ctx.service.inspect_resources(ctx.store.get(ctx.migration_id))
        require(
            observed["route"] == "SOURCE"
            and observed["access"] == "SOURCE_ONLY"
            and not observed["paused"]
            and ctx.driver.replication_ready(ctx.target, ctx.names["forward"]),
            "REPLICATION_NOT_READY",
            "원본 운영과 초기 동기화 완료를 확인할 수 없습니다.",
        )
        baseline = ctx.internal.get("schemas")
        require(
            baseline
            and all(ctx.driver.schema_signature(db) == baseline for db in [ctx.source, ctx.target]),
            "SCHEMA_MISMATCH",
            "복제 중 schema가 변경됐습니다.",
        )
        ctx.result("QUIESCED", atomicReadyTransition=ctx.request.transition_id)
        return "준비 확인 완료. DB 쓰기는 계속 열려 있습니다."

    def switch(self):
        ctx = self.ctx
        row = ctx.store.get(ctx.migration_id)
        observed = ctx.service.inspect_resources(row)
        if observed["route"] == "SOURCE":
            require(
                observed["paused"]
                or (
                    not row["internal"].get("forwardDetached")
                    and not row["internal"].get("forwardBarrier")
                ),
                "UNKNOWN_STATE",
                "최종 복제 뒤 원본 쓰기가 다시 열렸습니다.",
            )
            quiesce(ctx)
            self.refresh()
            cutover(ctx)
            self.refresh()
        else:
            require(
                observed["route"] == "TARGET"
                and observed["paused"]
                and row["internal"].get("forwardDetached")
                and row["internal"].get("validation"),
                "UNKNOWN_STATE",
                "중단된 대상 재개 근거가 없습니다.",
            )
        resume(ctx, location="TARGET", close_transition=False)
        ctx.result("SWITCHED", atomicConfirmed=False, atomicCompletedLocation="TARGET")
        return "쓰기 정지·최종 복제·대상 경로 변경·DB 쓰기 재개 완료"

    def confirm(self):
        ctx = self.ctx
        location = ctx.request.expected_location
        confirm_open(ctx, location)
        ctx.result("ACTIVE" if location == "TARGET" else "RESTORED", atomicConfirmed=True)
        if location == "SOURCE":
            ctx.store.update(
                ctx.migration_id, lambda row: row["state"].update(transitionState="CLOSED")
            )
        return "이미 재개한 DB 경로 확인 완료. 외부 변경 없음."

    def restore(self):
        ctx = self.ctx
        row = ctx.store.get(ctx.migration_id)
        require(
            not row["internal"].get("targetWritesIntent"),
            "ROLLBACK_UNAVAILABLE",
            "대상 쓰기 후 원본 복귀는 지원하지 않습니다.",
        )
        observed = ctx.service.inspect_resources(row)
        if not row["internal"].get("sourceResumeIntent"):
            ctx.record_intent("pgbouncer.pause")
            ctx.driver.admin(ctx.binding, "PAUSE {}", tag=ctx.operation_id)
            if observed["source"]["writer"]["rolcanlogin"]:
                ctx.driver.fence(ctx.source, ctx.operation_id, ctx.record_intent)
            if observed.get("targetReachable", True):
                ctx.driver.disable(
                    ctx.target, ctx.names["forward"], ctx.operation_id, ctx.record_intent, 10
                )
            rollback(ctx)
            self.refresh()
        resume(ctx, location="SOURCE", close_transition=False)
        ctx.result("ROLLED_BACK_HELD", atomicConfirmed=False, atomicCompletedLocation="SOURCE")
        return "원본 경로 복귀·DB 쓰기 재개 완료"

    def cancel(self):
        ctx = self.ctx
        observed = ctx.service.inspect_resources(ctx.store.get(ctx.migration_id))
        require(
            observed["route"] == "SOURCE"
            and observed["access"] == "SOURCE_ONLY"
            and not observed["paused"]
            and not ctx.internal.get("targetWritesIntent"),
            "INVALID_STATE",
            "원본에서 쓰기 중인 전환 전 계획만 취소할 수 있습니다.",
        )
        ctx.driver.disable(
            ctx.target, ctx.names["forward"], ctx.operation_id, ctx.record_intent, 10
        )
        ctx.result("CANCELLED")
        ctx.store.update(
            ctx.migration_id,
            lambda row: row["state"].update(
                transitionState="CLOSED" if row["state"]["transitionId"] else "NONE"
            ),
        )
        return "복제 정지·계획 취소 완료. 원본 쓰기는 유지합니다."


class AtomicReconciler(Reconciler):
    def __init__(self, ctx):
        super().__init__(ctx.worker, ctx.migration_id)
        self.ctx = ctx

    def _match_state(self, observed):
        if self.state["lifecycle"] in {"COMPLETED", "CANCELLED"}:
            return self.state["lifecycle"]
        route, access, paused = observed["route"], observed["access"], observed["paused"]
        if not paused and (
            (route, access) == ("TARGET", "TARGET_ONLY")
            or (
                (route, access) == ("SOURCE", "SOURCE_ONLY")
                and self.internal.get("sourceResumeIntent")
            )
        ):
            confirm_open(self.ctx, route, observed)
            if self.internal.get("atomicConfirmed"):
                return "ACTIVE" if route == "TARGET" else "RESTORED"
            return "SWITCHED" if route == "TARGET" else "ROLLED_BACK_HELD"
        if (
            route == "SOURCE"
            and access == "SOURCE_ONLY"
            and not paused
            and self.previous_command
            and self.previous_command["action"] == "QUIESCE"
            and not self.internal.get("atomicReadyTransition")
            and not self.internal.get("targetWritesIntent")
            and not self.internal.get("forwardBarrier")
        ):
            _, target, _ = self.driver.resources(self.plan)
            return (
                "READY"
                if self.driver.replication_ready(
                    target, self.driver.names(self.migration_id)["forward"]
                )
                else "SYNCING"
            )
        if self.internal.get("atomicReadyTransition") and not self.internal.get(
            "targetWritesIntent"
        ):
            if route == "SOURCE" and access == "SOURCE_ONLY" and not paused:
                if self.previous_command and self.previous_command["action"] == "CANCEL":
                    _, target, _ = self.driver.resources(self.plan)
                    with self.driver.connect(target) as conn:
                        subscription = conn.execute(
                            "SELECT oid,subenabled FROM pg_subscription WHERE subname=%s",
                            (self.driver.names(self.migration_id)["forward"],),
                        ).fetchone()
                        require(
                            not subscription
                            or (
                                not subscription["subenabled"]
                                and not conn.execute(
                                    "SELECT 1 FROM pg_stat_subscription "
                                    "WHERE subid=%s AND pid IS NOT NULL",
                                    (subscription["oid"],),
                                ).fetchone()
                            ),
                            "REPLICATION_NOT_STOPPED",
                            "취소한 복제의 정지를 확인할 수 없습니다.",
                        )
                    return "CANCELLED"
                require(
                    not self.internal.get("forwardDetached")
                    and not self.internal.get("forwardBarrier")
                    and self.driver.replication_ready(
                        self.driver.resources(self.plan)[1],
                        self.driver.names(self.migration_id)["forward"],
                    ),
                    "UNKNOWN_STATE",
                    "준비 확인 뒤 원본 운영 상태를 확인할 수 없습니다.",
                )
                return "QUIESCED"
            if route == "SOURCE" and paused and access in {"SOURCE_ONLY", "BLOCKED"}:
                require(
                    self.previous_command
                    and self.previous_command["action"] in {"CUTOVER", "ROLLBACK"},
                    "UNKNOWN_STATE",
                    "원본 정지 실행 근거가 없습니다.",
                )
                if self.internal.get("sourceResumeIntent"):
                    self._require_partial_resume("SOURCE", observed)
                    return "ROLLED_BACK_HELD"
                self.store.update(
                    self.migration_id, lambda row: row["internal"].update(partialSourcePause=True)
                )
                return "QUIESCED"
        if route == "TARGET" and paused:
            require(
                self.internal.get("forwardDetached") and self.internal.get("validation"),
                "UNKNOWN_STATE",
                "대상 경로 변경 근거가 없습니다.",
            )
            if self.internal.get("targetWritesIntent"):
                self._require_partial_resume("TARGET", observed)
            else:
                self.driver.validate_record(self.record)
            return "SWITCHED"
        return super()._match_state(observed)

    def _apply(self, record, lifecycle):
        super()._apply(record, lifecycle)
        if lifecycle in {"SWITCHED", "ROLLED_BACK_HELD"}:
            record["state"]["transitionState"] = "OPEN"
            record["internal"].setdefault("resumeAt", stamp())
        elif lifecycle == "CANCELLED":
            record["state"]["transitionState"] = (
                "CLOSED" if record["state"]["transitionId"] else "NONE"
            )
