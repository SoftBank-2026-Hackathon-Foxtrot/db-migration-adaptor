from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

from .config import Registry, Settings
from .control import participants_valid, verify_gate
from .models import (
    HELD_ACTIONS,
    PROFILE,
    Action,
    ActionRequest,
    CreateMigration,
    OperatorError,
    require,
)
from .postgres import Postgres
from .store import Store, decision, now, stamp


class Service:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.registry = Registry.load(settings.registry)
        self.store = Store(settings.management_dsn.get_secret_value())
        self.driver = Postgres(self.registry)

    def capabilities(self) -> dict:
        return {
            "apiVersion": "v1",
            "profiles": [
                {
                    "kind": "DATABASE",
                    "profile": PROFILE,
                    "sourceConditions": [
                        "SELF_MANAGED PostgreSQL 18; dedicated DB; superuser control"
                    ],
                    "targetConditions": [
                        "SELF_MANAGED PostgreSQL 18; prepared identical empty schema"
                    ],
                    "actions": [str(a) for a in Action],
                    "rollbackModes": ["BEFORE_TARGET_WRITE", "REVERSE_REQUIRED"],
                    "constraints": [
                        "Managed AWS/GCP services are not verified",
                        "transaction pooling",
                        "DDL/large objects/2PC/RLS/partitioned tables/user triggers unsupported",
                        "All application tables and sequences must be registered",
                        "API does not stop applications; trusted quiesceRef required",
                        "maxQuiesceSeconds <= 300; single designated executor",
                    ],
                }
            ],
        }

    def create(self, owner: str, key: str, plan: CreateMigration) -> dict:
        def resolve_identities():
            require(
                plan.kind == "DATABASE" and plan.profile == PROFILE,
                "UNSUPPORTED_PROFILE",
                "이 DB 프로필은 지원하지 않습니다.",
            )
            require(
                plan.source_ref in self.registry.databases
                and plan.target_ref in self.registry.databases
                and plan.binding_ref in self.registry.bindings
                and plan.workload_scope_ref in self.registry.workloads,
                "UNKNOWN_REFERENCE",
                "등록한 리소스 참조가 필요합니다.",
            )
            source, target, binding = self.driver.resources(plan.model_dump(by_alias=True))
            require(
                source.identity != target.identity,
                "UNSUPPORTED_PROFILE",
                "원본과 대상은 달라야 합니다.",
            )
            return [
                "db:" + source.identity,
                "db:" + target.identity,
                "config:" + str(binding.config_path.resolve()),
            ]

        return self.store.create(owner, key, plan, resolve_identities)

    def guard(self, conn, row: dict, request: ActionRequest) -> None:
        state = row["state"]
        if state["health"] in {"UNKNOWN", "FAILED"} and request.action != Action.RECONCILE:
            raise OperatorError("UNKNOWN_STATE", "RECONCILE로 실제 상태를 먼저 확인하십시오.", 409)
        if request.action not in state["allowedActions"]:
            raise OperatorError("INVALID_STATE", "현재 상태에서 허용하지 않는 명령입니다.", 409)
        if request.action in HELD_ACTIONS:
            if state["transitionId"] is None:
                require(
                    request.action == Action.QUIESCE
                    and state["lifecycle"] == "READY"
                    and state["transitionState"] == "NONE",
                    "INVALID_TRANSITION",
                    "최초 QUIESCE가 필요합니다.",
                )
            else:
                require(
                    request.transition_id == state["transitionId"]
                    and state["transitionState"] == "OPEN",
                    "INVALID_TRANSITION",
                    "현재 열린 전환 ID를 사용하십시오.",
                )
            verify_gate(conn, row, request, self.registry)
        if request.action == Action.RESUME:
            require(
                request.expected_location == state["observed"]["route"],
                "ROUTE_MISMATCH",
                "요청한 재개 위치와 실제 경로가 다릅니다.",
            )
        for command, name in [
            (Action.CUTOVER, "cutover"),
            (Action.RESUME, "resume"),
            (Action.ROLLBACK, "rollback"),
        ]:
            if request.action == command:
                require(
                    state["readiness"][name]["result"] == "PASS",
                    "PRECONDITION_FAILED",
                    "필수 검사를 통과하지 못했습니다.",
                )
        if request.action == Action.FINALIZE:
            require(
                state["lifecycle"] in {"ACTIVE", "RESTORED", "CANCELLED"},
                "INVALID_STATE",
                "정리 가능한 상태가 아닙니다.",
            )
            since = row["internal"].get("resumeAt")
            require(
                state["lifecycle"] == "CANCELLED"
                or (
                    since
                    and (now() - datetime.fromisoformat(since)).total_seconds()
                    >= self.settings.retention_seconds
                ),
                "RETENTION_ACTIVE",
                "롤백 보존 기간이 남아 있습니다.",
            )
            if state["lifecycle"] == "ACTIVE":
                require(
                    state["readiness"]["advance"]["result"] == "PASS"
                    or row["internal"].get("finalizeStarted"),
                    "PRECONDITION_FAILED",
                    "업무·후속 검증이 필요합니다.",
                )

    def submit(self, owner: str, mid: str, key: str, request: ActionRequest) -> dict:
        return self.store.submit(owner, mid, key, request, self.guard)

    @staticmethod
    def permitted(row: dict) -> list[str]:
        state = row["state"]
        if state["activeOperationId"]:
            return []
        if state["health"] != "OK":
            return ["RECONCILE"]
        life = state["lifecycle"]
        actions = {
            "CREATED": ["PREPARE", "CANCEL"],
            "PREPARED": ["START_SYNC", "CANCEL"],
            "SYNCING": ["CANCEL"],
            "READY": ["QUIESCE"],
            "QUIESCED": ["CUTOVER", "ROLLBACK"],
            "SWITCHED": ["RESUME", "ROLLBACK"],
            "ACTIVE": ["QUIESCE", "FINALIZE"],
            "ROLLED_BACK_HELD": ["RESUME"],
            "RESTORED": ["FINALIZE"],
            "CANCELLED": ["FINALIZE"],
            "COMPLETED": [],
        }[life].copy()
        if life == "READY" and state["transitionState"] == "NONE":
            actions.append("CANCEL")
        if life == "READY" and state["transitionState"] == "OPEN":
            actions.append("ROLLBACK")
        if life == "QUIESCED" and state["observed"]["route"] == "TARGET":
            actions.remove("CUTOVER")
            actions.append("RESUME")
        if life == "SWITCHED" and row["internal"].get("targetWritesIntent"):
            actions.append("QUIESCE")
        if life == "ACTIVE" and row["plan"]["policy"]["rollbackMode"] != "REVERSE_REQUIRED":
            actions.remove("QUIESCE")
        if row["internal"].get("finalizeStarted"):
            actions = [a for a in actions if a == "FINALIZE"]
        for command, name in [
            ("CUTOVER", "cutover"),
            ("RESUME", "resume"),
            ("ROLLBACK", "rollback"),
        ]:
            if command in actions and state["readiness"][name]["result"] != "PASS":
                actions.remove(command)
        return [*actions, "RECONCILE"]

    def observe(self, mid: str, *, allow_unhealthy: bool = False) -> dict:
        row = self.store.get(mid)
        if row["state"]["health"] != "OK" and not allow_unhealthy:
            return row
        try:
            observed = self.driver.observe(row["plan"])
        except Exception:

            def unknown(value):
                value["state"]["health"] = "UNKNOWN"
                value["state"]["readiness"] = {
                    k: decision("UNKNOWN", "OBSERVATION_UNAVAILABLE")
                    for k in value["state"]["readiness"]
                }
                value["internal"].pop("stableSince", None)
                value["state"]["allowedActions"] = ["RECONCILE"]

            return self.store.update(mid, unknown)
        self.record_observation(mid, observed)
        return self.store.get(mid)

    def record_observation(self, mid: str, observed: dict) -> None:
        with self.store.connect() as conn:
            row = self.store.row(conn, mid)
            state, internal = row["state"], row["internal"]
            fingerprint = [observed["route"], observed["access"], observed["paused"]]
            previous_fingerprint = internal.get("fingerprint")
            previous_health = state["health"]
            previous_topology = state["topologyRevision"]
            if previous_fingerprint is not None and previous_fingerprint != fingerprint:
                conn.execute(
                    "UPDATE foxtrot_operator.workflows SET topology_revision=topology_revision+1 "
                    "WHERE id=%s",
                    (row["plan"]["workflowId"],),
                )
                internal.pop("stableSince", None)
            internal["fingerprint"] = fingerprint
            internal["observedDetails"] = observed
            topology = conn.execute(
                "SELECT topology_revision FROM foxtrot_operator.workflows WHERE id=%s",
                (row["plan"]["workflowId"],),
            ).fetchone()["topology_revision"]
            state["topologyRevision"] = topology
            state["observed"] = {
                "observedAt": stamp(),
                "route": observed["route"],
                "businessWriteAccess": observed["access"],
                "targetWritesPossible": bool(internal.get("targetWritesIntent", False)),
            }
            if observed["access"] == "BOTH" or observed["route"] == "UNKNOWN":
                state["health"] = "UNKNOWN"
            if (
                previous_fingerprint != fingerprint
                or previous_health != state["health"]
                or previous_topology != topology
            ):
                state["revision"] += 1
            self.store.persist(conn, row)

    def get(self, mid: str, owner: str | None = None) -> dict:
        row = self.store.get(mid, owner)
        if row["state"]["activeOperationId"]:
            return row["state"]
        # Read-only GET does not clear UNKNOWN or replay commands.
        if row["state"]["lifecycle"] != "CREATED" and row["state"]["health"] == "OK":
            row = self.observe(mid)
        self.refresh(mid)
        return self.store.get(mid, owner)["state"]

    def refresh(self, mid: str) -> None:
        row = self.store.get(mid)
        state, plan = row["state"], row["plan"]
        if state["activeOperationId"] or state["health"] != "OK":
            return
        if state["lifecycle"] == "CREATED":
            self.store.update(mid, lambda r: r["state"].update(allowedActions=self.permitted(r)))
            return
        source, target, _ = self.driver.resources(plan)
        names = self.driver.names(mid)
        if state["lifecycle"] in {"SYNCING", "READY"} and state["transitionState"] == "NONE":
            ready = self.driver.replication_ready(target, names["forward"])
            self.store.update(
                mid, lambda r: r["state"].update(lifecycle="READY" if ready else "SYNCING")
            )
        row = self.store.get(mid)
        state, internal = row["state"], row["internal"]
        obs = state["observed"]
        held = obs["businessWriteAccess"] == "BLOCKED" and internal.get("observedDetails", {}).get(
            "paused"
        )
        has_validation = bool(internal.get("validation"))
        reverse_ready = internal.get("reversePrepared", False)
        cutover = (
            state["lifecycle"] == "QUIESCED"
            and obs["route"] == "SOURCE"
            and held
            and has_validation
        )
        resume = (
            state["lifecycle"] in {"SWITCHED", "ROLLED_BACK_HELD"}
            and held
            and has_validation
            and (
                obs["route"] == "SOURCE"
                or plan["policy"]["rollbackMode"] == "BEFORE_TARGET_WRITE"
                or reverse_ready
            )
        )
        direct_recovery = bool(
            internal.get("prepared")
            and not internal.get("targetWritesIntent")
            and state["transitionState"] == "OPEN"
        )
        rollback = (
            state["lifecycle"] in {"READY", "QUIESCED", "SWITCHED"}
            and held
            and (direct_recovery or (has_validation and internal.get("reverseValidated", False)))
        )
        with self.store.connect() as conn:
            workload = conn.execute(
                "SELECT * FROM foxtrot_operator.workload_observations WHERE scope_ref=%s",
                (plan["workloadScopeRef"],),
            ).fetchone()
        workload_ok = bool(
            workload
            and workload["state"] == "RUNNING"
            and workload["workflow_id"] == plan["workflowId"]
            and workload["topology_revision"] == state["topologyRevision"]
            and (now() - workload["observed_at"]).total_seconds() < 30
            and participants_valid(
                workload["participants"],
                self.registry.workloads[plan["workloadScopeRef"]].participants,
                workload["generation"],
                "RUNNING",
            )
        )
        probe_ok = bool(
            workload_ok and workload["probe_result"] == "PASS" and workload["probe_ref"]
        )
        route_ok = (
            obs["route"] == "TARGET"
            and obs["businessWriteAccess"] == "TARGET_ONLY"
            and not internal.get("observedDetails", {}).get("paused", True)
        )
        healthy = (
            state["lifecycle"] == "ACTIVE"
            and state["health"] == "OK"
            and not internal.get("unresolvedOperationId")
        )
        followup = plan["policy"][
            "rollbackMode"
        ] != "REVERSE_REQUIRED" or self.driver.replication_ready(source, names["reverse"])
        basics = healthy and route_ok and workload_ok and probe_ok and followup
        stable_since = internal.get("stableSince")
        last = internal.get("advanceObservedAt")
        if not basics or (last and (now() - datetime.fromisoformat(last)).total_seconds() > 10):
            stable_since = None
        if basics and not stable_since:
            stable_since = stamp()
        stable = bool(
            stable_since and (now() - datetime.fromisoformat(stable_since)).total_seconds() >= 30
        )
        checks = []

        def add(code: str, stage: str, passed: bool, reason: str, evidence: str | None = None):
            checks.append(
                {
                    "code": code,
                    "stage": stage,
                    "required": True,
                    "decision": decision("PASS") if passed else decision("FAIL", reason),
                    "evidenceRef": evidence or ("evidence:" + mid + ":" + code),
                    "observedAt": stamp(),
                    "validUntil": stamp(now() + timedelta(seconds=30)),
                    "transitionId": state["transitionId"],
                    "topologyRevision": state["topologyRevision"],
                }
            )

        add("CUTOVER_SAFE", "CUTOVER", cutover, "CUTOVER_PRECONDITIONS_NOT_MET")
        add("RESUME_SAFE", "RESUME", resume, "RESUME_PRECONDITIONS_NOT_MET")
        add("ROLLBACK_SAFE", "ROLLBACK", rollback, "ROLLBACK_PRECONDITIONS_NOT_MET")
        add("QUIESCE_COMPLETE", "QUIESCE", bool(held and has_validation), "QUIESCE_NOT_VALIDATED")
        for code, passed in [
            ("RESOURCE_HEALTH", healthy),
            ("ROUTE_MATCH", route_ok),
            ("WORKLOAD_RUNNING", workload_ok),
            ("BUSINESS_PROBE", probe_ok),
            ("FOLLOWUP_COMPLETE", followup),
            ("STABILITY_WINDOW", stable),
        ]:
            add(
                code,
                "ADVANCE",
                passed,
                code + "_NOT_READY",
                workload["probe_ref"] if workload and code == "BUSINESS_PROBE" else None,
            )

        def update(value):
            value["state"]["checks"] = checks
            value["state"]["readiness"] = {
                "cutover": decision("PASS")
                if cutover
                else decision("FAIL", "CUTOVER_PRECONDITIONS_NOT_MET"),
                "resume": decision("PASS")
                if resume
                else decision("FAIL", "RESUME_PRECONDITIONS_NOT_MET"),
                "rollback": decision("PASS")
                if rollback
                else decision("FAIL", "ROLLBACK_PRECONDITIONS_NOT_MET"),
                "advance": decision("PASS")
                if basics and stable
                else decision("FAIL", "ADVANCE_CHECKS_PENDING"),
            }
            value["state"]["rollback"] = {
                "method": ("REVERSE_SYNC" if obs["targetWritesPossible"] else "DIRECT")
                if rollback
                else "UNAVAILABLE",
                "reasons": [] if rollback else ["ROLLBACK_PRECONDITIONS_NOT_MET"],
            }
            if stable_since:
                value["internal"]["stableSince"] = stable_since
            else:
                value["internal"].pop("stableSince", None)
            value["internal"]["advanceObservedAt"] = stamp()
            value["state"]["allowedActions"] = self.permitted(value)

        self.store.update(mid, update)

    def evidence(self, mid: str) -> str:
        row = self.store.get(mid)
        return hashlib.sha256(
            json.dumps(row["internal"].get("validation"), sort_keys=True).encode()
        ).hexdigest()

    def bump_action_result(self, mid: str, lifecycle: str, **internal: Any) -> None:
        def update(row):
            row["state"].update(lifecycle=lifecycle, health="OK")
            row["internal"].update(internal)

        self.store.update(mid, update)
