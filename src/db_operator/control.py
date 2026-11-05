"""업무 정지 근거를 확인하고 오케스트레이터의 제어 기록을 저장합니다."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from uuid import uuid4

from .config import Registry
from .models import ActionRequest, require
from .store import Store, Transaction, now, scope_prefix, stamp

# 업무 정지 근거 확인


def participants_valid(
    participants: list[dict], expected: list[str], generation: int, state: str
) -> bool:
    if len(participants) != len(expected) or {p.get("id") for p in participants} != set(expected):
        return False
    for participant in participants:
        try:
            age = (
                now() - datetime.fromisoformat(participant["observedAt"].replace("Z", "+00:00"))
            ).total_seconds()
            if not (
                participant["generation"] == generation
                and participant["state"] == state
                and -1 <= age < 30
                and (state != "HELD" or participant["inFlight"] == 0)
            ):
                return False
        except (KeyError, ValueError, TypeError):
            return False
    return True


def verify_gate(tx: Transaction, row: dict, request: ActionRequest, registry: Registry) -> dict:
    plan = row["plan"]
    gate = tx.get("GATE", str(request.quiesce_ref))
    current = tx.get("WORKFLOW", plan["workflowId"])
    workload = tx.get("WORKLOAD", plan["workloadScopeRef"])
    require(gate and current and workload, "QUIESCE_EVIDENCE_INVALID", "업무 정지 기록이 없습니다.")
    expected = (plan["workflowId"], row["id"], request.transition_id, plan["workloadScopeRef"])
    actual = (gate["workflow_id"], gate["migration_id"], gate["transition_id"], gate["scope_ref"])
    require(
        actual == expected
        and gate["state"] == "HELD"
        and gate["topology_revision"] == current["topology_revision"]
        and workload["workflow_id"] == plan["workflowId"]
        and workload["topology_revision"] == current["topology_revision"]
        and workload["state"] == "HELD"
        and workload["generation"] == gate["generation"],
        "QUIESCE_EVIDENCE_INVALID",
        "정지 범위·시도·구성 버전이 다릅니다.",
    )
    require(
        gate["observed_at"] <= now() + timedelta(seconds=1)
        and now() < gate["valid_until"] <= gate["observed_at"] + timedelta(seconds=30)
        and participants_valid(
            gate["participants"],
            registry.workloads[plan["workloadScopeRef"]].participants,
            gate["generation"],
            "HELD",
        ),
        "QUIESCE_EVIDENCE_EXPIRED",
        "업무 정지 근거가 만료되었거나 확인 대상이 빠졌습니다.",
    )
    recovery = (
        request.action == "ROLLBACK"
        or (
            request.action == "QUIESCE"
            and row["internal"].get("partialSourcePause")
            and not row["internal"].get("targetWritesIntent")
        )
        or (request.action == "RESUME" and request.expected_location == "SOURCE")
        or (
            request.action == "QUIESCE"
            and row["state"]["observed"]["route"] == "TARGET"
            and row["internal"].get("targetWritesIntent")
            and row["internal"].get("reversePrepared")
        )
    )
    require(
        recovery
        or (now() - gate["held_since"]).total_seconds() < plan["policy"]["maxQuiesceSeconds"],
        "QUIESCE_DEADLINE_EXCEEDED",
        "업무 정지 시간 상한을 넘었습니다.",
    )
    return gate


def actor_ack(actor: str, generation: int, state: str, in_flight: int = 0) -> dict:
    return {
        "id": actor,
        "generation": generation,
        "state": state,
        "inFlight": in_flight,
        "observedAt": stamp(),
    }


# 오케스트레이터 확인 기록


class ControlPlaneRecords:
    """실제 앱의 응답을 확인한 오케스트레이터만 호출하는 내부 SDK입니다."""

    def __init__(self, store: Store):
        self.store = store

    def topology(self, workflow: str) -> int:
        return self.store.transact(lambda tx: tx.get("WORKFLOW", workflow)["topology_revision"])

    def held(
        self,
        *,
        migration_id: str,
        transition_id: str,
        generation: int,
        participants: list[dict[str, Any]],
        held_since=None,
    ) -> str:
        ref = "quiesce:" + uuid4().hex

        def save(tx):
            row = self.store.row(tx, migration_id)
            plan = row["plan"]
            self.store.control_idle(tx, plan["workloadScopeRef"])
            topology = tx.get("WORKFLOW", plan["workflowId"])["topology_revision"]
            previous = tx.get("WORKLOAD", plan["workloadScopeRef"])
            require(
                not previous
                or generation > previous["generation"]
                or (
                    previous["state"] == "HELD"
                    and generation == previous["generation"]
                    and previous["workflow_id"] == plan["workflowId"]
                ),
                "STALE_GENERATION",
                "다시 정지할 때는 generation을 증가시켜야 합니다.",
            )
            observed = now()
            first = held_since or observed
            if previous and previous["state"] == "HELD" and generation == previous["generation"]:
                first = min(first, previous["held_since"])
            require(
                first <= observed + timedelta(seconds=1),
                "INVALID_TIME",
                "정지 시작 시각이 미래입니다.",
            )
            gate = {
                "ref": ref,
                "workflow_id": plan["workflowId"],
                "migration_id": migration_id,
                "transition_id": transition_id,
                "scope_ref": plan["workloadScopeRef"],
                "topology_revision": topology,
                "generation": generation,
                "state": "HELD",
                "held_since": first,
                "observed_at": observed,
                "valid_until": observed + timedelta(seconds=30),
                "participants": participants,
            }
            tx.put("GATE", ref, gate)
            tx.put("SCOPE_GATE", scope_prefix(plan["workloadScopeRef"]) + ref, {"ref": ref})
            tx.put(
                "WORKLOAD",
                plan["workloadScopeRef"],
                {
                    "scope_ref": plan["workloadScopeRef"],
                    "workflow_id": plan["workflowId"],
                    "topology_revision": topology,
                    "generation": generation,
                    "state": "HELD",
                    "participants": participants,
                    "probe_result": "UNKNOWN",
                    "probe_ref": None,
                    "observed_at": observed,
                    "held_since": first,
                },
            )
            if not previous or any(
                previous[key] != value
                for key, value in {
                    "workflow_id": plan["workflowId"],
                    "topology_revision": topology,
                    "generation": generation,
                    "state": "HELD",
                    "probe_result": "UNKNOWN",
                }.items()
            ):
                self.store.invalidate_advance(tx, plan["workloadScopeRef"])
            return ref

        return self.store.transact(save)

    def renew(self, ref: str, participants: list[dict]) -> None:
        def renew(tx):
            gate = tx.get("GATE", ref)
            require(
                gate and gate["state"] == "HELD", "INVALID_STATE", "유효한 정지 기록이 아닙니다."
            )
            workload = tx.get("WORKLOAD", gate["scope_ref"])
            require(
                workload
                and workload["state"] == "HELD"
                and workload["generation"] == gate["generation"],
                "QUIESCE_EVIDENCE_INVALID",
                "현재 업무 정지 범위가 다릅니다.",
            )
            require(
                participants_valid(
                    participants,
                    [p["id"] for p in gate["participants"]],
                    gate["generation"],
                    "HELD",
                ),
                "QUIESCE_EVIDENCE_INVALID",
                "최신 업무 정지 확인이 필요합니다.",
            )
            observed = now()
            gate.update(
                observed_at=observed,
                valid_until=observed + timedelta(seconds=30),
                participants=participants,
            )
            tx.put("GATE", ref, gate)

        self.store.transact(renew)

    def running(
        self,
        migration_id: str,
        generation: int,
        participants: list[dict],
        probe_result: str,
        probe_ref: str | None,
    ) -> None:
        def running(tx):
            row = self.store.row(tx, migration_id)
            plan = row["plan"]
            self.store.control_idle(tx, plan["workloadScopeRef"])
            current = tx.get("WORKLOAD", plan["workloadScopeRef"])
            require(
                current and generation == current["generation"],
                "STALE_GENERATION",
                "현재 generation이 아닙니다.",
            )
            require(
                row["state"]["lifecycle"] in {"ACTIVE", "RESTORED"}
                and row["state"]["health"] == "OK"
                and not row["state"]["activeOperationId"],
                "INVALID_STATE",
                "리소스 재개를 먼저 확인하십시오.",
            )
            topology = tx.get("WORKFLOW", plan["workflowId"])["topology_revision"]
            changed = self._running_changed(
                current, plan, topology, generation, participants, probe_result, probe_ref
            )
            tx.put(
                "WORKLOAD",
                plan["workloadScopeRef"],
                {
                    **current,
                    "state": "RUNNING",
                    "topology_revision": topology,
                    "participants": participants,
                    "probe_result": probe_result,
                    "probe_ref": probe_ref,
                    "observed_at": now(),
                },
            )
            for index in tx.list("SCOPE_GATE", scope_prefix(plan["workloadScopeRef"])):
                gate = tx.get("GATE", index["ref"])
                if gate["generation"] == generation and gate["state"] == "HELD":
                    gate["state"] = "RELEASED"
                    tx.put("GATE", gate["ref"], gate)
            if changed:
                self.store.invalidate_advance(tx, plan["workloadScopeRef"])

        self.store.transact(running)

    @staticmethod
    def _running_changed(
        current: dict,
        plan: dict,
        topology: int,
        generation: int,
        participants: list[dict],
        probe_result: str,
        probe_ref: str | None,
    ) -> bool:
        expected = [p["id"] for p in current["participants"]]

        def participant_state(records):
            return sorted(
                json.dumps(
                    {key: p.get(key) for key in ["id", "generation", "state"]}, sort_keys=True
                )
                for p in records
            )

        return (
            not participants_valid(participants, expected, generation, "RUNNING")
            or participant_state(participants) != participant_state(current["participants"])
            or not probe_ref
            or any(
                current[key] != value
                for key, value in {
                    "workflow_id": plan["workflowId"],
                    "topology_revision": topology,
                    "generation": generation,
                    "state": "RUNNING",
                    "probe_result": probe_result,
                }.items()
            )
        )
