"""요청과 실제 상태를 대조해 허용 명령과 다음 진행 조건을 결정합니다."""

from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass
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

# 진행 조건과 허용 명령


def workload_identity(row: dict, workload: dict | None) -> list:
    plan = row["plan"]
    return [
        plan["workflowId"],
        plan["workloadScopeRef"],
        row["state"]["topologyRevision"],
        workload["generation"] if workload else None,
        row["internal"].get("controlEpoch", 0),
    ]


def resume_pending(row: dict, observed: dict) -> bool:
    internal = row["internal"]
    location = internal.get("resumePending")
    nodes = ["source", "target"] if observed.get("targetReachable", True) else ["source"]
    return bool(
        location in {"SOURCE", "TARGET"}
        and observed.get("route") == location
        and observed.get("access") in {location + "_ONLY", "BLOCKED"}
        and observed.get("paused")
        and internal.get("validation")
        and internal.get("sourceResumeIntent" if location == "SOURCE" else "targetWritesIntent")
        and (location == "SOURCE" or observed.get("targetReachable", True))
        and all(
            observed.get(node, {}).get(field) == 0
            for node in nodes
            for field in ["sessions", "prepared", "oldExecutors"]
        )
    )


def allowed_actions(row: dict) -> list[str]:
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
    if row["internal"].get("resumePending"):
        actions = [a for a in actions if a == "RESUME"]
    if row["internal"].get("directRecovery"):
        actions = [
            a
            for a in actions
            if a in {"ROLLBACK", "RESUME", "FINALIZE"}
            or (a == "QUIESCE" and row["internal"].get("partialSourcePause"))
        ]
    if row["internal"].get("observedDetails", {}).get("targetReachable") is False:
        actions = [a for a in actions if a != "FINALIZE"]
    for command, name in [
        ("CUTOVER", "cutover"),
        ("RESUME", "resume"),
        ("ROLLBACK", "rollback"),
    ]:
        if command in actions and state["readiness"][name]["result"] != "PASS":
            actions.remove(command)
    return [*actions, "RECONCILE"]


def source_authority(row: dict) -> bool:
    return bool(
        row["internal"].get("prepared")
        and not row["internal"].get("targetWritesIntent")
        and row["state"]["lifecycle"] not in {"COMPLETED", "CANCELLED"}
        and (row["state"]["transitionState"] == "OPEN" or row["internal"].get("directRecovery"))
    )


@dataclass(frozen=True)
class AdvanceFacts:
    workload_ok: bool
    probe_ok: bool
    route_ok: bool
    healthy: bool
    basics: bool
    stable: bool
    stable_since: str | None
    identity: list


@dataclass(frozen=True)
class TransitionFacts:
    cutover: bool
    resume: bool
    rollback: bool
    quiesced: bool


@dataclass(frozen=True)
class ReadinessEvaluation:
    checks: list[dict]
    decisions: dict
    rollback: dict
    advance: AdvanceFacts

    def apply(self, record: dict) -> None:
        record["state"].update(checks=self.checks, readiness=self.decisions, rollback=self.rollback)
        internal = record["internal"]
        if self.advance.stable_since:
            internal["stableSince"] = self.advance.stable_since
            internal["stableIdentity"] = self.advance.identity
        else:
            internal.pop("stableSince", None)
        internal["advanceObservedAt"] = stamp()
        record["state"]["allowedActions"] = allowed_actions(record)


class ReadinessEvaluator:
    def __init__(self, registry: Registry):
        self.registry = registry

    def advance_facts(
        self,
        row: dict,
        observed: dict,
        workload: dict | None,
        followup: bool,
        operation_id: str | None = None,
    ) -> AdvanceFacts:
        state, internal, plan = row["state"], row["internal"], row["plan"]
        workload_ok = bool(
            workload
            and workload["state"] == "RUNNING"
            and workload["workflow_id"] == plan["workflowId"]
            and workload["topology_revision"] == state["topologyRevision"]
            and -1 <= (now() - workload["observed_at"]).total_seconds() < 30
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
            observed["route"] == "TARGET"
            and observed["access"] == "TARGET_ONLY"
            and not observed["paused"]
        )
        healthy = bool(
            state["lifecycle"] == "ACTIVE"
            and state["health"] == "OK"
            and not internal.get("unresolvedOperationId")
            and state["activeOperationId"] in {None, operation_id}
        )
        basics = bool(healthy and route_ok and workload_ok and probe_ok and followup)
        identity = workload_identity(row, workload)
        stable_since = internal.get("stableSince")
        last = internal.get("advanceObservedAt")
        if (
            not basics
            or internal.get("stableIdentity") != identity
            or not last
            or not 0 <= (now() - datetime.fromisoformat(last)).total_seconds() <= 10
        ):
            stable_since = None
        if basics and not stable_since:
            stable_since = stamp()
        stable = bool(
            stable_since and (now() - datetime.fromisoformat(stable_since)).total_seconds() >= 30
        )
        return AdvanceFacts(
            workload_ok=workload_ok,
            probe_ok=probe_ok,
            route_ok=route_ok,
            healthy=healthy,
            basics=basics,
            stable=stable,
            stable_since=stable_since,
            identity=identity,
        )

    @staticmethod
    def _transition_facts(record: dict) -> TransitionFacts:
        state, internal, plan = record["state"], record["internal"], record["plan"]
        observed = state["observed"]
        details = internal.get("observedDetails", {})
        nodes = ["source", "target"] if details.get("targetReachable", True) else ["source"]
        held = (
            observed["businessWriteAccess"] == "BLOCKED"
            and details.get("paused")
            and all(
                details.get(node, {}).get(field) == 0
                for node in nodes
                for field in ["sessions", "prepared"]
            )
        )
        validated = bool(internal.get("validation"))
        cutover = (
            state["lifecycle"] == "QUIESCED"
            and observed["route"] == "SOURCE"
            and held
            and validated
            and not internal.get("directRecovery")
            and details.get("targetReachable", True)
        )
        resume = (
            state["lifecycle"] in {"SWITCHED", "ROLLED_BACK_HELD"}
            and (held or resume_pending(record, details))
            and validated
            and (
                observed["route"] == "SOURCE"
                or (
                    not internal.get("directRecovery")
                    and details.get("targetReachable", True)
                    and (
                        plan["policy"]["rollbackMode"] == "BEFORE_TARGET_WRITE"
                        or internal.get("reversePrepared", False)
                    )
                )
            )
        )
        direct_recovery = (
            internal.get("prepared")
            and not internal.get("targetWritesIntent")
            and state["transitionState"] == "OPEN"
        )
        rollback = (
            state["lifecycle"] in {"READY", "QUIESCED", "SWITCHED"}
            and held
            and (direct_recovery or (validated and internal.get("reverseValidated", False)))
        )
        return TransitionFacts(
            bool(cutover), bool(resume), bool(rollback), bool(held and validated)
        )

    def evaluate(self, record: dict, workload: dict | None, followup: bool) -> ReadinessEvaluation:
        observed = record["state"]["observed"]
        details = record["internal"].get("observedDetails", {})
        transitions = self._transition_facts(record)
        advance = self.advance_facts(record, details, workload, followup)
        checks = [
            make_check(
                record,
                "CUTOVER_SAFE",
                "CUTOVER",
                transitions.cutover,
                "CUTOVER_PRECONDITIONS_NOT_MET",
            ),
            make_check(
                record, "RESUME_SAFE", "RESUME", transitions.resume, "RESUME_PRECONDITIONS_NOT_MET"
            ),
            make_check(
                record,
                "ROLLBACK_SAFE",
                "ROLLBACK",
                transitions.rollback,
                "ROLLBACK_PRECONDITIONS_NOT_MET",
            ),
            make_check(
                record,
                "QUIESCE_COMPLETE",
                "QUIESCE",
                transitions.quiesced,
                "QUIESCE_NOT_VALIDATED",
            ),
        ]
        for code, passed in [
            ("RESOURCE_HEALTH", advance.healthy),
            ("ROUTE_MATCH", advance.route_ok),
            ("WORKLOAD_RUNNING", advance.workload_ok),
            ("BUSINESS_PROBE", advance.probe_ok),
            ("FOLLOWUP_COMPLETE", followup),
            ("STABILITY_WINDOW", advance.stable),
        ]:
            evidence = workload["probe_ref"] if workload and code == "BUSINESS_PROBE" else None
            checks.append(
                make_check(record, code, "ADVANCE", passed, code + "_NOT_READY", evidence)
            )
        decisions = {
            name: decision("PASS") if passed else decision("FAIL", reason)
            for name, passed, reason in [
                ("cutover", transitions.cutover, "CUTOVER_PRECONDITIONS_NOT_MET"),
                ("resume", transitions.resume, "RESUME_PRECONDITIONS_NOT_MET"),
                ("rollback", transitions.rollback, "ROLLBACK_PRECONDITIONS_NOT_MET"),
                ("advance", advance.basics and advance.stable, "ADVANCE_CHECKS_PENDING"),
            ]
        }
        rollback_status = {
            "method": ("REVERSE_SYNC" if observed["targetWritesPossible"] else "DIRECT")
            if transitions.rollback
            else "UNAVAILABLE",
            "reasons": [] if transitions.rollback else ["ROLLBACK_PRECONDITIONS_NOT_MET"],
        }
        return ReadinessEvaluation(checks, decisions, rollback_status, advance)


def make_check(
    record: dict, code: str, stage: str, passed: bool, reason: str, evidence: str | None = None
) -> dict:
    state = record["state"]
    return {
        "code": code,
        "stage": stage,
        "required": True,
        "decision": decision("PASS") if passed else decision("FAIL", reason),
        "evidenceRef": evidence or ("evidence:" + record["id"] + ":" + code),
        "observedAt": stamp(),
        "validUntil": stamp(now() + timedelta(seconds=30)),
        "transitionId": state["transitionId"],
        "topologyRevision": state["topologyRevision"],
    }


# API 요청 처리


class Service:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._observation_lock = threading.Lock()
        self.registry = Registry.load(settings.registry)
        self.store = Store(
            settings.state_table,
            namespace=settings.state_namespace,
            region=settings.aws_region,
            endpoint=settings.dynamodb_endpoint,
        )
        self.driver = Postgres(self.registry)
        self.readiness = ReadinessEvaluator(self.registry)

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

    def guard(self, tx, row: dict, request: ActionRequest) -> None:
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
            verify_gate(tx, row, request, self.registry)
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
            self.check_finalize(tx, row)

    def check_finalize(self, tx, row: dict, operation_id: str | None = None) -> dict:
        state, internal, plan = row["state"], row["internal"], row["plan"]
        require(
            state["lifecycle"] in {"ACTIVE", "RESTORED", "CANCELLED"},
            "INVALID_STATE",
            "정리 가능한 상태가 아닙니다.",
        )
        since = internal.get("resumeAt")
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
        observed = self.driver.observe(plan)
        expected = "TARGET" if state["lifecycle"] == "ACTIVE" else "SOURCE"
        require(
            observed["route"] == expected
            and observed["access"] == expected + "_ONLY"
            and not observed["paused"],
            "PRECONDITION_FAILED",
            "단독 운영 상태가 필요합니다.",
        )
        if internal.get("finalizeStarted"):
            require(
                internal.get("finalizeLocation") == expected,
                "PRECONDITION_FAILED",
                "승인한 정리 위치가 다릅니다.",
            )
            return observed
        if state["lifecycle"] == "ACTIVE":
            workload = self.store.workload_record(tx, plan["workloadScopeRef"])
            source, _, _ = self.driver.resources(plan)
            followup = plan["policy"][
                "rollbackMode"
            ] != "REVERSE_REQUIRED" or self.driver.replication_ready(
                source, self.driver.names(row["id"])["reverse"]
            )
            facts = self.advance_facts(row, observed, workload, followup, operation_id)
            checks = [c for c in state["checks"] if c["stage"] == "ADVANCE"]
            require(
                facts.basics
                and facts.stable
                and len(checks) == 6
                and all(
                    c["decision"]["result"] == "PASS"
                    and c["validUntil"]
                    and now() < datetime.fromisoformat(c["validUntil"])
                    and c["topologyRevision"] == state["topologyRevision"]
                    for c in checks
                ),
                "PRECONDITION_FAILED",
                "최신 업무·안정화·역복제 확인이 필요합니다.",
            )
        return observed

    def submit(self, owner: str, mid: str, key: str, request: ActionRequest) -> dict:
        return self.store.submit(owner, mid, key, request, self.guard)

    def inspect_resources(self, row: dict) -> dict:
        return self.driver.observe(row["plan"], source_authority=self.source_authority(row))

    def observation_unavailable(self, mid: str, revision: int) -> dict:
        def unavailable(value):
            # A failed read is distinct from an unconfirmed external mutation.
            value["internal"]["observationUnavailable"] = True
            value["internal"].pop("stableSince", None)
            value["internal"].pop("observationCache", None)
            value["state"]["readiness"] = {
                k: decision("UNKNOWN", "OBSERVATION_UNAVAILABLE")
                for k in value["state"]["readiness"]
            }
            for check in value["state"]["checks"]:
                check["decision"] = decision("UNKNOWN", "OBSERVATION_UNAVAILABLE")
            value["state"]["allowedActions"] = ["RECONCILE"]

        return self.store.update(mid, unavailable, expected_revision=revision, idle=True)

    def observe(self, mid: str, *, allow_unhealthy: bool = False) -> dict:
        row = self.store.get(mid)
        if row["state"]["health"] != "OK" and not allow_unhealthy:
            return row
        try:
            observed = self.inspect_resources(row)
        except Exception:
            return self.observation_unavailable(mid, row["state"]["revision"])
        self.record_observation(mid, observed, expected_revision=row["state"]["revision"])
        return self.store.get(mid)

    def record_observation(
        self, mid: str, observed: dict, *, expected_revision: int | None = None
    ) -> None:
        def save(tx):
            row = self.store.row(tx, mid)
            state, internal = row["state"], row["internal"]
            if expected_revision is not None and (
                state["activeOperationId"] or state["revision"] != expected_revision
            ):
                return
            internal.pop("observationUnavailable", None)
            fingerprint = [observed["route"], observed["access"], observed["paused"]]
            previous_fingerprint = internal.get("fingerprint")
            previous_health = state["health"]
            previous_topology = state["topologyRevision"]
            if previous_fingerprint is not None and previous_fingerprint != fingerprint:
                workflow = tx.get("WORKFLOW", row["plan"]["workflowId"])
                workflow["topology_revision"] += 1
                tx.put("WORKFLOW", row["plan"]["workflowId"], workflow)
                internal.pop("stableSince", None)
            internal["fingerprint"] = fingerprint
            internal["observedDetails"] = observed
            topology = tx.get("WORKFLOW", row["plan"]["workflowId"])["topology_revision"]
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
            self.store.persist(tx, row)

        self.store.transact(save)

    def cached_state(self, row: dict) -> bool:
        if row["state"]["activeOperationId"]:
            return True
        cached = row["internal"].get("observationCache", {})
        return bool(
            row["state"]["health"] == "OK"
            and cached.get("revision") == row["state"]["revision"]
            and (now() - datetime.fromisoformat(cached["at"])).total_seconds()
            < self.settings.observation_cache_seconds
        )

    def get(self, mid: str, owner: str | None = None) -> dict:
        row = self.store.get(mid, owner)
        if self.cached_state(row):
            return row["state"]
        # 캐시가 만료된 동시 조회는 한 번의 관측 결과를 기다립니다.
        with self._observation_lock:
            return self._observe_for_get(mid, owner)

    def _observe_for_get(self, mid: str, owner: str | None) -> dict:
        row = self.store.get(mid, owner)
        if self.cached_state(row):
            return row["state"]
        # Read-only GET does not clear UNKNOWN or replay commands.
        if row["state"]["lifecycle"] != "CREATED" and row["state"]["health"] == "OK":
            row = self.observe(mid)
        if not row["internal"].get("observationUnavailable"):
            refresh_revision = self.store.get(mid)["state"]["revision"]
            try:
                self.refresh(mid)
            except Exception:
                self.observation_unavailable(mid, refresh_revision)
        return self.store.get(mid, owner)["state"]

    def refresh(self, mid: str) -> None:
        record = self.store.get(mid)
        state, plan = record["state"], record["plan"]
        if state["activeOperationId"] or state["health"] != "OK":
            return
        if state["lifecycle"] == "CREATED":
            self.store.update(
                mid,
                lambda value: value["state"].update(allowedActions=self.permitted(value)),
                expected_revision=state["revision"],
                idle=True,
                cache_observation=True,
            )
            return
        source, target, _ = self.driver.resources(plan)
        names = self.driver.names(mid)
        if state["lifecycle"] in {"SYNCING", "READY"} and state["transitionState"] == "NONE":
            ready = self.driver.replication_ready(target, names["forward"])
            self.store.update(
                mid,
                lambda value: value["state"].update(lifecycle="READY" if ready else "SYNCING"),
                expected_revision=state["revision"],
                idle=True,
            )
        record = self.store.get(mid)
        state = record["state"]
        if (
            state["activeOperationId"]
            or state["health"] != "OK"
            or record["internal"].get("observationUnavailable")
        ):
            return
        workload = self.store.transact(
            lambda tx: self.store.workload_record(tx, plan["workloadScopeRef"])
        )
        followup = plan["policy"]["rollbackMode"] != "REVERSE_REQUIRED" or (
            state["lifecycle"] == "ACTIVE"
            and self.driver.replication_ready(source, names["reverse"])
        )
        evaluation = self.readiness.evaluate(record, workload, followup)
        self.store.update(
            mid,
            evaluation.apply,
            expected_revision=state["revision"],
            idle=True,
            cache_observation=True,
        )

    # Keep these query helpers available to the worker and existing callers.
    permitted = staticmethod(allowed_actions)
    resume_pending = staticmethod(resume_pending)
    source_authority = staticmethod(source_authority)
    workload_identity = staticmethod(workload_identity)

    def advance_facts(
        self,
        row: dict,
        observed: dict,
        workload: dict | None,
        followup: bool,
        operation_id: str | None = None,
    ) -> AdvanceFacts:
        return self.readiness.advance_facts(row, observed, workload, followup, operation_id)

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
