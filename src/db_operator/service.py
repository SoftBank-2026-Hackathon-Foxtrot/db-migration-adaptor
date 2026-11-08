"""요청과 실제 상태를 대조해 허용 명령과 다음 진행 조건을 결정합니다."""

from __future__ import annotations

import hashlib
import json
import os
import threading
from copy import copy
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .config import (
    Registry,
    ResourceCatalog,
    Settings,
    database_endpoint,
    endpoint_database,
)
from .control import participants_valid, verify_gate
from .models import (
    ATOMIC_LOGICAL_PROFILE,
    DMS_PROFILE,
    FORWARD_LOGICAL_PROFILES,
    HELD_ACTIONS,
    MANAGED_LOGICAL_PROFILE,
    MANAGED_LOGICAL_PROFILES,
    ONPREM_LOGICAL_PROFILE,
    PROFILE,
    Action,
    ActionRequest,
    CreateBinding,
    CreateMigration,
    DirectMigration,
    OperatorError,
    RegisterBinding,
    needs_held_evidence,
    require,
)
from .observability import public_state
from .postgres import Postgres
from .store import Store, decision, now, request_digest, stamp

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


def transition_completed(row: dict) -> bool:
    internal = row["internal"]
    if row["plan"]["profile"] == DMS_PROFILE and internal.get("completionEvidence"):
        evidence = internal["completionEvidence"]
        return bool(
            evidence.get("migrationId") == row["id"]
            and evidence.get("transitionId") == row["state"]["transitionId"]
            and evidence.get("method") in {"DMS_BARRIER", "SOURCE_RECOVERY"}
        )
    # 기존 전수 비교 결과를 가진 진행 중 계획도 조회·재개할 수 있습니다.
    return bool(internal.get("validation"))


def resume_pending(row: dict, observed: dict) -> bool:
    internal = row["internal"]
    location = internal.get("resumePending")
    nodes = ["source", "target"] if observed.get("targetReachable", True) else ["source"]
    return bool(
        location in {"SOURCE", "TARGET"}
        and observed.get("route") == location
        and observed.get("access") in {location + "_ONLY", "BLOCKED"}
        and observed.get("paused")
        and transition_completed(row)
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
    dms = row["internal"].get("observedDetails", {}).get("dms", {})
    resumable = (
        row["plan"]["profile"] == DMS_PROFILE
        and life == "RESTORED"
        and dms.get("state") == "STOPPED"
        and dms.get("phase") == "CDC"
        and not dms.get("errorCode")
        and not row["internal"].get("targetWritesIntent")
        and not row["internal"].get("dmsPromoted")
    )
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
    if resumable:
        actions.insert(0, "START_SYNC")
    if life == "READY" and state["transitionState"] == "NONE":
        actions.append("CANCEL")
    if life == "READY" and state["transitionState"] == "OPEN":
        actions.append("ROLLBACK")
    if life == "QUIESCED" and state["observed"]["route"] == "TARGET":
        actions.remove("CUTOVER")
        actions.append("RESUME")
    if life == "SWITCHED" and row["internal"].get("targetWritesIntent"):
        actions.append("QUIESCE")
    if row["plan"]["profile"] == ATOMIC_LOGICAL_PROFILE:
        if life == "QUIESCED":
            actions = ["CUTOVER", "ROLLBACK", "CANCEL"]
            if row["internal"].get("observedDetails", {}).get("paused"):
                actions.remove("CANCEL")
        elif life == "SWITCHED":
            actions = ["RESUME", "CUTOVER", "ROLLBACK"]
        elif life == "ROLLED_BACK_HELD":
            actions = (
                ["RESUME"] if state["readiness"]["resume"]["result"] == "PASS" else ["ROLLBACK"]
            )
    if life == "ACTIVE" and row["plan"]["policy"]["rollbackMode"] != "REVERSE_REQUIRED":
        actions.remove("QUIESCE")
    if row["internal"].get("finalizeStarted"):
        actions = [a for a in actions if a == "FINALIZE"]
    if row["internal"].get("resumePending") and row["plan"]["profile"] != ATOMIC_LOGICAL_PROFILE:
        actions = [a for a in actions if a == "RESUME"]
    if row["internal"].get("directRecovery"):
        actions = [
            a
            for a in actions
            if a in {"ROLLBACK", "RESUME", "FINALIZE"}
            or (a == "START_SYNC" and resumable)
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
        validated = transition_completed(record)
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
        quiesced = bool(held and validated)
        if plan["profile"] == ATOMIC_LOGICAL_PROFILE:
            checked = bool(
                internal.get("atomicReadyTransition") == state["transitionId"]
                and state["transitionState"] == "OPEN"
                and internal.get("prepared")
            )
            source_open = (
                observed["route"] == "SOURCE"
                and observed["businessWriteAccess"] == "SOURCE_ONLY"
                and not details.get("paused")
            )
            source_ready = (
                source_open
                and not internal.get("forwardDetached")
                and not internal.get("forwardBarrier")
            )
            cutover = bool(
                checked
                and not internal.get("directRecovery")
                and details.get("targetReachable", True)
                and (
                    state["lifecycle"] == "QUIESCED"
                    and observed["route"] == "SOURCE"
                    and (
                        source_ready
                        or held
                        or (details.get("paused") and internal.get("partialSourcePause"))
                    )
                    or state["lifecycle"] == "SWITCHED"
                    and observed["route"] == "TARGET"
                    and details.get("paused")
                    and validated
                    and internal.get("forwardDetached")
                )
            )
            resume = bool(
                not details.get("paused")
                and validated
                and (
                    state["lifecycle"] == "SWITCHED"
                    and observed["route"] == "TARGET"
                    and observed["businessWriteAccess"] == "TARGET_ONLY"
                    and internal.get("targetWritesIntent")
                    and internal.get("forwardDetached")
                    or state["lifecycle"] == "ROLLED_BACK_HELD"
                    and source_open
                    and internal.get("sourceResumeIntent")
                    and not internal.get("targetWritesIntent")
                )
            )
            rollback = bool(
                checked
                and not internal.get("targetWritesIntent")
                and state["lifecycle"] in {"READY", "QUIESCED", "SWITCHED", "ROLLED_BACK_HELD"}
            )
            quiesced = checked
        return TransitionFacts(bool(cutover), bool(resume), bool(rollback), quiesced)

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
        self.bindings = BindingService(self)

    def scoped(self, row: dict):
        if "catalog" not in row:
            if row["plan"]["profile"] in FORWARD_LOGICAL_PROFILES:
                value = copy(self)
                value.driver = value.driver_for(row["plan"])
                value.driver.migration_id = row["id"]
                return value
            return self
        value = copy(self)
        value.registry = Registry.model_validate(row["catalog"])
        binding = value.registry.bindings[row["plan"]["bindingRef"]]
        binding.probe_database = value.registry.databases[row["plan"]["sourceRef"]]
        value.driver = value.driver_for(row["plan"])
        value.driver.migration_id = row["id"]
        value.readiness = ReadinessEvaluator(value.registry)
        return value

    def driver_for(self, plan: dict):
        if plan["profile"] == ONPREM_LOGICAL_PROFILE:
            from .onprem import OnpremLogicalPostgres

            return OnpremLogicalPostgres(self.registry, self.store)
        if plan["profile"] in MANAGED_LOGICAL_PROFILES:
            from .logical import ManagedLogicalPostgres

            return ManagedLogicalPostgres(self.registry, self.store)
        if plan["profile"] == DMS_PROFILE:
            from .dms import GcpDmsPostgres

            return GcpDmsPostgres(self.registry, plan, self.store)
        return Postgres(self.registry)

    def capabilities(self) -> dict:
        return {
            "apiVersion": "v1",
            "profiles": [
                {
                    "kind": "DATABASE",
                    "profile": ATOMIC_LOGICAL_PROFILE,
                    "sourceConditions": [
                        "AWS RDS or GCP Cloud SQL PostgreSQL 17; logical WAL; approved admin"
                    ],
                    "targetConditions": [
                        "Standalone RDS or Cloud SQL PostgreSQL 17; logical WAL; identical schema, "
                        "empty or the retired source of this binding's latest completed migration"
                    ],
                    "actions": [str(a) for a in Action],
                    "rollbackModes": ["BEFORE_TARGET_WRITE"],
                    "constraints": [
                        "QUIESCE checks only; CUTOVER holds, switches and resumes DB writes",
                        "RESUME confirms the already-open location; ROLLBACK restores and resumes",
                        "CUTOVER and ROLLBACK require verified workload-held evidence",
                        "Native publication/subscription; no DMS or Promote",
                        "No reverse replication or source fallback after target write intent",
                    ],
                },
                {
                    "kind": "DATABASE",
                    "profile": MANAGED_LOGICAL_PROFILE,
                    "sourceConditions": [
                        "AWS RDS or GCP Cloud SQL PostgreSQL 17; logical WAL; approved admin"
                    ],
                    "targetConditions": [
                        "Standalone RDS or Cloud SQL PostgreSQL 17; logical WAL; identical schema, "
                        "empty or the retired source of this binding's latest completed migration"
                    ],
                    "actions": [str(a) for a in Action],
                    "rollbackModes": ["BEFORE_TARGET_WRITE"],
                    "constraints": [
                        "Native publication/subscription; no DMS or Promote",
                        "Single subscription; durable barrier; no full table comparison",
                        "DDL and unregistered writers prohibited during migration",
                        "No reverse replication or automatic source fallback after target writes",
                        "Application pause and heartbeat remain orchestrator responsibilities",
                    ],
                },
                {
                    "kind": "DATABASE",
                    "profile": DMS_PROFILE,
                    "sourceConditions": [
                        "AWS RDS PostgreSQL; dedicated instance; approved DMS job"
                    ],
                    "targetConditions": ["GCP Cloud SQL PostgreSQL; prepared dedicated writer"],
                    "actions": [str(a) for a in Action],
                    "rollbackModes": ["BEFORE_TARGET_WRITE"],
                    "constraints": [
                        "replicationRef references a pre-provisioned GCP DMS job",
                        "No native replication fallback; no reverse replication",
                        "Application pause and heartbeat remain orchestrator responsibilities",
                        "Live cloud cutover is not verified",
                    ],
                },
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
                },
                {
                    "kind": "DATABASE",
                    "profile": ONPREM_LOGICAL_PROFILE,
                    "sourceConditions": ["AWS RDS PostgreSQL 17; logical WAL"],
                    "targetConditions": [
                        "Writable self-managed PostgreSQL 17; logical WAL; verified TLS",
                        "Identical empty schema; same dedicated writer with LOGIN disabled",
                    ],
                    "actions": [str(a) for a in Action],
                    "rollbackModes": ["BEFORE_TARGET_WRITE"],
                    "constraints": [
                        "Explicit opt-in; native subscription from the VM to RDS; no DMS",
                        "Verified workload hold before switching; no reverse CDC or target reset",
                    ],
                },
            ],
        }

    def create(
        self,
        owner: str,
        key: str,
        plan: CreateMigration | DirectMigration,
        *,
        request_body: dict | None = None,
        extra_databases: dict | None = None,
    ) -> dict:
        if isinstance(plan, DirectMigration):
            return self.create_direct(owner, key, plan)
        catalog, guard, selected = None, None, self
        if self.settings.bindings_enabled:
            duplicate = self.store.transact(
                lambda tx: self.store.duplicate(
                    tx,
                    owner + ":POST:/v1/migrations",
                    key,
                    request_body if request_body is not None else plan.model_dump(by_alias=True),
                )
            )
            if duplicate:
                return duplicate
            registry, guard = self.bindings.migration_catalog(
                plan, owner, extra_databases=extra_databases
            )
            selected = copy(self)
            selected.registry, selected.driver = registry, Postgres(registry)
        if plan.profile == DMS_PROFILE:
            require(
                self.settings.bindings_enabled,
                "BINDINGS_REQUIRED",
                "DMS는 동적 binding을 사용합니다.",
            )
            entry = self.store._read("CATALOG_DMS#" + str(plan.replication_ref))
            require(entry, "UNKNOWN_REFERENCE", "승인된 DMS 작업 참조가 필요합니다.")
            selected = copy(selected)
            selected.registry = selected.registry.model_copy(
                update={"dms_jobs": {plan.replication_ref: entry}}
            )
            selected.driver = selected.driver_for(plan.model_dump(by_alias=True))
            previous_guard = guard

            def dms_guard(tx):
                if previous_guard:
                    previous_guard(tx)
                for database in selected.registry.databases.values():
                    prefix = database.identity.rsplit("/", 1)[0] + "/"
                    require(
                        all(
                            assigned["binding_id"] == plan.binding_ref
                            for assigned in tx.list("DB_BINDING", prefix)
                        ),
                        "DMS_INSTANCE_SCOPE_UNSUPPORTED",
                        "같은 인스턴스의 다른 binding이 있어 DMS 전체 승격을 차단합니다.",
                        409,
                    )

            guard = dms_guard
        if self.settings.bindings_enabled or plan.profile == DMS_PROFILE:
            catalog = selected.registry.model_dump(mode="json")

        if plan.profile in FORWARD_LOGICAL_PROFILES:
            selected = copy(selected)
            selected.driver = selected.driver_for(plan.model_dump(by_alias=True))

        def resolve_identities():
            require(selected.registry.tables, "INVALID_REGISTRY", "이전할 table 등록이 필요합니다.")
            require(
                plan.kind == "DATABASE"
                and plan.profile in {PROFILE, DMS_PROFILE, *FORWARD_LOGICAL_PROFILES},
                "UNSUPPORTED_PROFILE",
                "이 DB 프로필은 지원하지 않습니다.",
            )
            require(
                plan.source_ref in selected.registry.databases
                and plan.target_ref in selected.registry.databases
                and plan.binding_ref in selected.registry.bindings
                and plan.workload_scope_ref in selected.registry.workloads,
                "UNKNOWN_REFERENCE",
                "등록한 리소스 참조가 필요합니다.",
            )
            source, target, binding = selected.driver.resources(plan.model_dump(by_alias=True))
            require(
                plan.profile in {DMS_PROFILE, *FORWARD_LOGICAL_PROFILES}
                or source.provider == target.provider == "SELF_MANAGED",
                "UNSUPPORTED_PROFILE",
                "관리형 DB에는 DMS 또는 관리형 논리 복제 프로필을 사용하십시오.",
            )
            if plan.profile in FORWARD_LOGICAL_PROFILES:
                selected.driver.check_pair(source, target)
            require(
                source.identity != target.identity,
                "UNSUPPORTED_PROFILE",
                "원본과 대상은 달라야 합니다.",
            )
            identities = [
                "db:" + source.identity,
                "db:" + target.identity,
                "binding:" + plan.binding_ref
                if catalog
                else "config:" + str(binding.config_path.resolve()),
            ]
            if plan.profile == DMS_PROFILE:
                require(
                    source.provider == "AWS_RDS" and target.provider == "GCP_CLOUDSQL",
                    "DMS_PROFILE_UNSUPPORTED",
                    "RDS → Cloud SQL DB 등록이 필요합니다.",
                )
                identities.extend(
                    [
                        "dms-job:" + selected.driver.client.binding.job,
                        "dms-instance:" + source.identity.rsplit("/", 1)[0],
                        "dms-instance:" + target.identity.rsplit("/", 1)[0],
                    ]
                )
            return identities

        return self.store.create(
            owner,
            key,
            plan,
            resolve_identities,
            catalog=catalog,
            guard=guard,
            request_body=request_body,
        )

    def create_direct(self, owner: str, key: str, request: DirectMigration) -> dict:
        self.bindings.enabled()
        body = request.model_dump(by_alias=True)
        duplicate = self.store.transact(
            lambda tx: self.store.duplicate(tx, owner + ":POST:/v1/migrations", key, body)
        )
        if duplicate:
            return duplicate
        row = self.bindings.select_app_binding(owner, request.resource_id, request.binding_ref)

        def current_source(tx):
            current = self.bindings.records.row(tx, row["bindingId"], owner)
            self.bindings.records.idle(tx, current)
            route = tx.get("ROUTE", row["bindingId"])
            require(
                current["status"] == "ACTIVE" and route and route["access"] == "OPEN",
                "BINDING_NOT_READY",
                "운영 중인 앱의 접속점을 먼저 확인하십시오.",
                409,
            )
            database, _ = self.bindings.resource(tx, route["database_ref"])
            require(
                database_endpoint(database)
                == request.source.model_dump(by_alias=True, exclude_none=True),
                "SOURCE_MISMATCH",
                "source가 앱 접속점의 현재 DB와 다릅니다.",
                409,
            )
            return route["database_ref"]

        source_ref = self.store.transact(current_source)
        require(
            (request.source.host, request.source.port, request.source.database)
            != (request.target.host, request.target.port, request.target.database),
            "UNSUPPORTED_PROFILE",
            "원본과 대상은 달라야 합니다.",
        )
        target = endpoint_database(request.target)
        target_ref = "db:" + request_digest(target.model_dump(mode="json"))
        profile = request.profile or (
            PROFILE
            if request.source.provider == request.target.provider == "SELF_MANAGED"
            else ATOMIC_LOGICAL_PROFILE
        )
        # 내부 모델의 기존 provider·복제·롤백 정책 검사를 다시 적용합니다.
        try:
            plan = CreateMigration(
                workflow_id=request.workflow_id,
                resource_id=request.resource_id,
                kind=request.kind,
                profile=profile,
                source_ref=source_ref,
                target_ref=target_ref,
                binding_ref=row["bindingId"],
                workload_scope_ref=request.workload_scope_ref or row["workloadScopeRef"],
                policy=request.policy,
                replication_ref=request.replication_ref,
            )
        except ValueError as error:
            raise OperatorError(
                "UNSUPPORTED_PROFILE", "프로필의 복제·롤백 지원 범위를 확인하십시오."
            ) from error
        return self.create(
            owner,
            key,
            plan,
            request_body=body,
            extra_databases={target_ref: target},
        )

    def guard(self, tx, row: dict, request: ActionRequest) -> None:
        require(
            not (
                row.get("directInput")
                and row["state"]["lifecycle"] == "COMPLETED"
                and row["state"]["health"] == "OK"
            ),
            "MIGRATION_COMPLETED",
            "완료한 이전 기록은 변경할 수 없습니다. 새 이전을 등록하십시오.",
            409,
        )
        if "catalog" in row:
            binding = self.bindings.records.row(tx, row["plan"]["bindingRef"])
            if (
                binding["status"] == "UNREGISTERED"
                and row["state"]["lifecycle"] == "COMPLETED"
                and row["state"]["health"] == "OK"
                and request.action == Action.RECONCILE
            ):
                return
            require(
                binding["status"] == "ACTIVE" and not binding["activeOperationId"],
                "BINDING_NOT_READY",
                "binding 복구·설정 작업을 먼저 완료하십시오.",
                409,
            )
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
                require(
                    request.transition_id not in row["internal"].get("previousTransitionIds", []),
                    "INVALID_TRANSITION",
                    "이전 시도의 전환 ID를 다시 사용할 수 없습니다.",
                )
            else:
                require(
                    request.transition_id == state["transitionId"]
                    and state["transitionState"] == "OPEN",
                    "INVALID_TRANSITION",
                    "현재 열린 전환 ID를 사용하십시오.",
                )
            if needs_held_evidence(row["plan"]["profile"], request.action):
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
        # No waiting period here: the orchestrator decides when the way back is no longer needed
        # and sends FINALIZE then. The state below is still checked.
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
        row = self.store.get(mid, owner)
        duplicate = self.store.transact(
            lambda tx: self.store.duplicate(
                tx,
                owner + ":POST:/v1/migrations/" + mid + "/actions",
                key,
                request.model_dump(by_alias=True, exclude_none=True),
            )
        )
        if duplicate:
            return duplicate
        archived = (
            "catalog" in row
            and row["state"]["lifecycle"] == "COMPLETED"
            and row["state"]["health"] == "OK"
            and request.action == Action.RECONCILE
            and self.bindings.records.get(row["plan"]["bindingRef"])["status"] == "UNREGISTERED"
        )
        selected = self if archived else self.scoped(row)
        return self.store.submit(owner, mid, key, request, selected.guard)

    def inspect_resources(self, row: dict) -> dict:
        observed = self.driver.observe(row["plan"], source_authority=self.source_authority(row))
        if row["plan"]["profile"] == DMS_PROFILE and row["internal"].get("dmsFingerprint"):
            require(
                observed["dms"]["fingerprint"] == row["internal"]["dmsFingerprint"],
                "DMS_CONFIG_CHANGED",
                "DMS 작업 설정이 변경됐습니다.",
            )
        return observed

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
        if row["state"]["lifecycle"] == "COMPLETED" and row["state"]["health"] == "OK":
            return True
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
            return public_state(row)
        profile = row["plan"]["profile"]
        driver_profile = getattr(self.driver, "profile", None)
        if (
            profile in MANAGED_LOGICAL_PROFILES and driver_profile not in MANAGED_LOGICAL_PROFILES
        ) or (profile == ONPREM_LOGICAL_PROFILE and driver_profile != profile):
            return self.scoped(row).get(mid, owner)
        if "catalog" in row and (
            not self.registry.bindings.get(row["plan"]["bindingRef"])
            or row["plan"]["profile"] == DMS_PROFILE
            and getattr(self.driver, "profile", None) != DMS_PROFILE
        ):
            return self.scoped(row).get(mid, owner)
        # 캐시가 만료된 동시 조회는 한 번의 관측 결과를 기다립니다.
        with self._observation_lock:
            if row["plan"]["profile"] in FORWARD_LOGICAL_PROFILES:
                with self.driver.operation("observe"):
                    return self._observe_for_get(mid, owner)
            return self._observe_for_get(mid, owner)

    def _observe_for_get(self, mid: str, owner: str | None) -> dict:
        row = self.store.get(mid, owner)
        if self.cached_state(row):
            return public_state(row)
        # Read-only GET does not clear UNKNOWN or replay commands.
        if row["state"]["lifecycle"] != "CREATED" and row["state"]["health"] == "OK":
            row = self.observe(mid)
        if not row["internal"].get("observationUnavailable"):
            refresh_revision = self.store.get(mid)["state"]["revision"]
            try:
                self.refresh(mid)
            except Exception:
                self.observation_unavailable(mid, refresh_revision)
        return public_state(self.store.get(mid, owner))

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
        checking_atomic = plan["profile"] == ATOMIC_LOGICAL_PROFILE and not record["internal"].get(
            "atomicReadyTransition"
        )
        if state["lifecycle"] in {"SYNCING", "READY"} and (
            state["transitionState"] == "NONE" or checking_atomic
        ):
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
            json.dumps(
                row["internal"].get("completionEvidence", row["internal"].get("validation")),
                sort_keys=True,
            ).encode()
        ).hexdigest()

    def bump_action_result(self, mid: str, lifecycle: str, **internal: Any) -> None:
        def update(row):
            row["state"].update(lifecycle=lifecycle, health="OK")
            row["internal"].update(internal)

        self.store.update(mid, update)


class BindingService:
    """등록된 리소스 참조만 받아 앱 연결 수명을 관리합니다."""

    def __init__(self, service: Service):
        from .store import BindingStore

        self.service, self.store = service, service.store
        self.records = BindingStore(self.store)

    def enabled(self):
        require(
            self.service.settings.bindings_enabled,
            "BINDINGS_DISABLED",
            "동적 binding 등록이 비활성 상태입니다.",
            409,
        )
        require(
            not self.service.registry.bindings and not self.service.registry.databases,
            "DYNAMIC_REGISTRY_REQUIRED",
            "동적 모드는 빈 기본 Registry를 사용하십시오.",
            409,
        )

    def resource(self, tx, reference: str):
        from .config import Database

        entry = tx.get("CATALOG_DB", reference)
        require(entry, "UNKNOWN_REFERENCE", "플랫폼이 승인한 DB 참조가 필요합니다.")
        return Database.model_validate(entry["databases"][reference]), entry

    def select_app_binding(self, owner: str, resource_id: str, reference: str | None = None):
        configured = self.service.settings.app_resource_id
        require(
            configured is None or configured == resource_id,
            "RESOURCE_MISMATCH",
            "이 실행 단위에 등록된 앱 리소스를 사용하십시오.",
            409,
        )
        if reference:
            row = self.records.get(reference, owner)
        else:
            rows = self.store.transact(
                lambda tx: [r for r in tx.list("BINDING") if r["owner"] == owner]
            )
            require(
                len(rows) == 1,
                "BINDING_REQUIRED",
                "앱 접속점이 하나여야 합니다. bindingRef를 확인하십시오.",
                409,
            )
            row = rows[0]
        require(
            row.get("resourceId", resource_id) == resource_id,
            "RESOURCE_MISMATCH",
            "다른 앱의 접속점은 사용할 수 없습니다.",
            409,
        )
        return row

    def proxy(self, alias: str) -> dict:
        return {
            "host": self.service.settings.public_host,
            "port": self.service.settings.public_port,
            "database": alias,
            "protocol": "postgresql",
        }

    def register_direct(self, owner: str, key: str, request: CreateBinding) -> dict:
        self.enabled()
        configured = self.service.settings.app_resource_id
        require(
            configured is None or configured == request.resource_id,
            "RESOURCE_MISMATCH",
            "이 실행 단위에 등록된 앱 리소스를 사용하십시오.",
            409,
        )
        profile = request.profile or (
            PROFILE if request.source.provider == "SELF_MANAGED" else ATOMIC_LOGICAL_PROFILE
        )
        require(
            profile in {PROFILE, *MANAGED_LOGICAL_PROFILES}
            or (profile == ONPREM_LOGICAL_PROFILE and request.source.provider == "AWS_RDS"),
            "UNSUPPORTED_PROFILE",
            "직접 접속점 등록에는 지원하는 논리 복제 프로필을 사용하십시오.",
        )
        database = endpoint_database(request.source)
        ref = "db:" + request_digest(database.model_dump(mode="json"))
        app_key = request_digest({"owner": owner, "resourceId": request.resource_id})
        scope_ref = "scope:app:" + app_key
        alias = "app_" + app_key[:24]
        legacy = RegisterBinding(
            app_ref="app:" + app_key,
            database_ref=ref,
            alias=alias,
            workload_scope_ref=scope_ref,
            pool_size=request.pool_size,
            max_db_connections=request.max_db_connections,
        )
        catalog = ResourceCatalog.database_entry(
            ref,
            database,
            tables=request.schema_spec.tables,
            sequences=request.schema_spec.sequences,
        )
        participants = request.workload.participants
        require(
            len(participants) == len(set(participants)), "INVALID_SCOPE", "참여자가 중복됐습니다."
        )

        def validate(tx):
            require(
                not tx.list("BINDING"),
                "APP_ALREADY_REGISTERED",
                "앱별 실행 단위에는 접속점을 하나만 등록할 수 있습니다.",
                409,
            )
            ResourceCatalog.publish(tx, "CATALOG_DB", ref, catalog)
            ResourceCatalog.publish(tx, "CATALOG_SCOPE", scope_ref, {"participants": participants})
            return self.validate_registration(tx, legacy)

        fields = {
            "resourceId": request.resource_id,
            "kind": "DATABASE",
            "profile": profile,
            "source": request.source.model_dump(by_alias=True, exclude_none=True),
            "proxy": self.proxy(alias),
        }
        return self.records.register(
            owner,
            key,
            legacy,
            validate,
            request_body=request.model_dump(by_alias=True),
            metadata={"resourceId": request.resource_id, "kind": "DATABASE", "profile": profile},
            response_fields=fields,
        )

    def register(self, owner: str, key: str, request) -> dict:
        self.enabled()
        if isinstance(request, CreateBinding):
            return self.register_direct(owner, key, request)
        require(
            request.alias != "pgbouncer", "INVALID_ALIAS", "관리용 DB 이름은 사용할 수 없습니다."
        )

        def validate(tx):
            require(
                self.service.settings.app_resource_id is None
                and not any("resourceId" in row for row in tx.list("BINDING")),
                "APP_BINDING_FIXED",
                "앱별 고정 실행 단위에는 다른 앱 접속점을 추가할 수 없습니다.",
                409,
            )
            return self.validate_registration(tx, request)

        return self.records.register(owner, key, request, validate)

    def validate_registration(self, tx, request):
        database, _ = self.resource(tx, request.database_ref)
        instance_claim = tx.get(
            "CLAIM",
            hashlib.sha256(
                ("dms-instance:" + database.identity.rsplit("/", 1)[0]).encode()
            ).hexdigest(),
        )
        require(
            not instance_claim,
            "RESOURCE_BUSY",
            "DMS 이전 중인 인스턴스에는 앱을 추가하지 않습니다.",
            409,
        )
        require(
            database.writer_role
            not in {"pgbouncer", os.environ.get("DB_OPERATOR_RUNTIME_ADMIN_USER", "pooler_admin")},
            "UNSAFE_WRITER",
            "PgBouncer 관리 계정은 업무 writer로 사용할 수 없습니다.",
        )
        require(
            tx.get("CATALOG_SCOPE", request.workload_scope_ref),
            "UNKNOWN_REFERENCE",
            "플랫폼이 승인한 업무 범위가 필요합니다.",
        )
        claim = tx.get("CLAIM", hashlib.sha256(("db:" + database.identity).encode()).hexdigest())
        require(not claim, "RESOURCE_BUSY", "이전 중인 DB입니다.", 409)
        return database

    def get(self, bid: str, owner: str | None = None) -> dict:
        self.enabled()
        row = self.records.get(bid, owner)
        route = self.store._read("ROUTE#" + bid)
        from .store import scope_prefix

        migrations = self.store.transact(
            lambda tx: [
                self.store.row(tx, r["migration_id"])
                for r in tx.list("BINDING_MIG", scope_prefix(bid))
            ]
        )
        if row["activeOperationId"] or any(m["state"]["activeOperationId"] for m in migrations):
            actions = []
        elif row["status"] == "ACTIVE":
            actions = ["UNREGISTER", "RESTORE"]
        elif row["status"] == "FAILED":
            actions = ["RECONCILE", "RETRY"] + (
                ["CANCEL"] if row["pendingAction"] == "REGISTER" else []
            )
        else:
            actions = ["RECONCILE"] if row["status"] == "UNKNOWN" else []
        if any(
            m["state"]["lifecycle"] not in {"COMPLETED", "CANCELLED"}
            or m["state"]["health"] != "OK"
            for m in migrations
        ):
            actions = [
                a
                for a in actions
                if a == "RESTORE"
                or (row["pendingAction"] == "RESTORE" and a in {"RECONCILE", "RETRY"})
            ]
        result = {
            key: row[key]
            for key in [
                "bindingId",
                "alias",
                "revision",
                "status",
                "activeOperationId",
                "apps",
                "workloadScopeRef",
                "phase",
                "error",
            ]
        } | {
            "databaseRef": route["database_ref"] if route else row["databaseRef"],
            "endpoint": (
                {
                    "host": self.service.settings.public_host,
                    "port": self.service.settings.public_port,
                    "database": row["alias"],
                }
                if row["status"] == "ACTIVE"
                else None
            ),
            "allowedActions": actions,
            "lastMigrationId": row.get("lastMigrationId"),
        }
        if "resourceId" in row:
            database = self.store.transact(lambda tx: self.resource(tx, result["databaseRef"])[0])
            result.update(
                bindingRef=bid,
                resourceId=row["resourceId"],
                kind="DATABASE",
                profile=row["profile"],
                source=database_endpoint(database),
                proxy=self.proxy(row["alias"]),
            )
        return result

    def submit(self, owner: str, bid: str, key: str, request) -> dict:
        self.enabled()
        return self.records.submit(owner, bid, key, request, self.verify_action)

    def verify_action(self, tx, row: dict, request) -> None:
        if request.action in {"RECONCILE", "CANCEL"}:
            return
        if request.action == "RETRY":
            require(
                row["status"] == "FAILED",
                "RECONCILE_REQUIRED",
                "UNKNOWN은 RECONCILE로 확인한 뒤 재시도하십시오.",
                409,
            )
        if row["pendingAction"] == "UNREGISTER" and row.get("releaseMode") != "CANCEL_REGISTRATION":
            from .control import verify_binding_release

            verify_binding_release(tx, row, request.evidence_ref, request.expected_revision)
            row["releaseEvidence"] = request.evidence_ref
            row["releaseRevision"] = request.expected_revision

    def registry(
        self,
        row: dict,
        extra_refs: list[str] | None = None,
        *,
        desired=False,
        extra_databases=None,
    ) -> Registry:
        """binding 하나의 DB와 업무 범위만 포함한 고정 실행 설정입니다."""
        from .config import Binding, Scope

        def read(tx):
            route = tx.get("ROUTE", row["bindingId"])
            active = route["database_ref"] if route else row["databaseRef"]
            refs = list(dict.fromkeys([active, *(extra_refs or [])]))
            databases, schema = {}, None
            for ref in refs:
                if extra_databases and ref in extra_databases:
                    databases[ref], entry = extra_databases[ref], None
                else:
                    databases[ref], entry = self.resource(tx, ref)
                if schema is None:
                    schema = entry
            participants = []
            for scope in (row["desiredApps"] if desired else row["apps"]).values():
                workload = tx.get("CATALOG_SCOPE", scope)
                require(workload, "UNKNOWN_REFERENCE", "업무 범위가 없습니다.")
                participants.extend(workload["participants"])
            require(
                len(participants) == len(set(participants)),
                "INVALID_SCOPE",
                "공유 앱의 참여자 이름이 중복됐습니다.",
            )
            binding = Binding(
                admin_env="PGBOUNCER_ADMIN_DSN",
                probe_env="",
                config_path=self.service.settings.pooler_directory / "pgbouncer.ini",
                alias=row["alias"],
                generation=row["generation"],
                probe_database=databases[active],
            )
            return Registry(
                databases=databases,
                bindings={row["bindingId"]: binding},
                workloads={row["workloadScopeRef"]: Scope(participants=participants)}
                if participants
                else {},
                tables=schema["tables"],
                sequences=schema["sequences"],
            )

        return self.store.transact(read)

    def migration_catalog(self, plan: CreateMigration, owner: str, *, extra_databases=None):
        row = self.records.get(plan.binding_ref, owner)
        registry = self.registry(
            row, [plan.source_ref, plan.target_ref], extra_databases=extra_databases
        )
        require(
            plan.workload_scope_ref == row["workloadScopeRef"],
            "INVALID_SCOPE",
            "binding 전체 앱의 업무 범위를 사용하십시오.",
        )
        require(registry.tables, "INVALID_REGISTRY", "이전할 table 등록이 필요합니다.")

        def guard(tx):
            current = self.records.row(tx, plan.binding_ref, owner)
            self.records.idle(tx, current)
            route = tx.get("ROUTE", plan.binding_ref)
            require(
                current["status"] == "ACTIVE"
                and not current["activeOperationId"]
                and current["revision"] == row["revision"]
                and route
                and route["access"] == "OPEN"
                and route["database_ref"] == plan.source_ref,
                "BINDING_NOT_READY",
                "운영 중인 현재 DB에서만 이전할 수 있습니다.",
                409,
            )
            for database in registry.databases.values():
                assigned = tx.get("DB_BINDING", database.identity)
                require(
                    not assigned or assigned["binding_id"] == plan.binding_ref,
                    "DATABASE_ALREADY_BOUND",
                    "다른 binding의 DB는 이전 대상으로 쓸 수 없습니다.",
                    409,
                )
                require(
                    database.writer_role == registry.databases[plan.source_ref].writer_role,
                    "UNSAFE_PROXY",
                    "원본·대상의 writer 역할이 같아야 합니다.",
                )
                tx.put(
                    "DB_BINDING",
                    database.identity,
                    {"identity": database.identity, "binding_id": plan.binding_ref},
                )
            for reference, database in (extra_databases or {}).items():
                ResourceCatalog.publish(
                    tx,
                    "CATALOG_DB",
                    reference,
                    ResourceCatalog.database_entry(
                        reference,
                        database,
                        tables=registry.tables,
                        sequences=registry.sequences,
                    ),
                )

            current["revision"] += 1
            tx.put("BINDING", plan.binding_ref, current)

        return registry, guard
