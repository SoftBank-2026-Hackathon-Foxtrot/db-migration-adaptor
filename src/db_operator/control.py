"""Control-plane records, not an application stop implementation."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from .config import Registry
from .models import ActionRequest, require
from .store import Store, now, stamp


def participants_valid(
    participants: list[dict], expected: list[str], generation: int, state: str
) -> bool:
    if len(participants) != len(expected) or {p.get("id") for p in participants} != set(expected):
        return False
    for participant in participants:
        try:
            from datetime import datetime

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


def verify_gate(
    conn: psycopg.Connection, row: dict, request: ActionRequest, registry: Registry
) -> dict:
    plan = row["plan"]
    gate = conn.execute(
        "SELECT * FROM foxtrot_operator.quiesce_gates WHERE ref=%s", (request.quiesce_ref,)
    ).fetchone()
    current = conn.execute(
        "SELECT topology_revision FROM foxtrot_operator.workflows WHERE id=%s",
        (plan["workflowId"],),
    ).fetchone()
    workload = conn.execute(
        "SELECT * FROM foxtrot_operator.workload_observations WHERE scope_ref=%s",
        (plan["workloadScopeRef"],),
    ).fetchone()
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


class ControlPlaneRecords:
    """Trusted orchestrator SDK. Call only after the actual actors acknowledged control.

    This interface is deliberately NOT exposed by the operator's HTTP API.
    Production must restrict these tables to the control-plane DB principal.
    """

    def __init__(self, store: Store):
        self.store = store

    def topology(self, workflow: str) -> int:
        with self.store.connect() as conn:
            return conn.execute(
                "SELECT topology_revision FROM foxtrot_operator.workflows WHERE id=%s", (workflow,)
            ).fetchone()["topology_revision"]

    def held(
        self,
        *,
        migration_id: str,
        transition_id: str,
        generation: int,
        participants: list[dict[str, Any]],
        held_since=None,
    ) -> str:
        row = self.store.get(migration_id)
        plan = row["plan"]
        from uuid import uuid4

        ref = "quiesce:" + uuid4().hex
        with self.store.connect() as conn:
            # All writers of these records must take this workflow lock.
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (plan["workflowId"],)
            )
            current_migration = self.store.row(conn, migration_id)
            require(
                not current_migration["state"]["activeOperationId"],
                "RESOURCE_BUSY",
                "명령 실행 중에는 정지 범위를 바꾸지 않습니다.",
            )
            topology = conn.execute(
                "SELECT topology_revision FROM foxtrot_operator.workflows WHERE id=%s",
                (plan["workflowId"],),
            ).fetchone()["topology_revision"]
            previous = conn.execute(
                "SELECT generation,state,workflow_id FROM foxtrot_operator.workload_observations "
                "WHERE scope_ref=%s FOR UPDATE",
                (plan["workloadScopeRef"],),
            ).fetchone()
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
            first_held = held_since or observed
            if previous and previous["state"] == "HELD" and generation == previous["generation"]:
                prior = conn.execute(
                    "SELECT min(held_since) AS first FROM foxtrot_operator.quiesce_gates "
                    "WHERE scope_ref=%s AND workflow_id=%s AND generation=%s",
                    (plan["workloadScopeRef"], plan["workflowId"], generation),
                ).fetchone()["first"]
                if prior:
                    first_held = min(first_held, prior)
            require(
                first_held <= observed + timedelta(seconds=1),
                "INVALID_TIME",
                "정지 시작 시각이 미래입니다.",
            )
            conn.execute(
                "INSERT INTO foxtrot_operator.quiesce_gates "
                "VALUES(%s,%s,%s,%s,%s,%s,%s,'HELD',%s,%s,%s,%s)",
                (
                    ref,
                    plan["workflowId"],
                    migration_id,
                    transition_id,
                    plan["workloadScopeRef"],
                    topology,
                    generation,
                    first_held,
                    observed,
                    observed + timedelta(seconds=30),
                    Jsonb(participants),
                ),
            )
            conn.execute(
                "INSERT INTO foxtrot_operator.workload_observations "
                "VALUES(%s,%s,%s,%s,'HELD',%s,'UNKNOWN',NULL,%s) ON CONFLICT(scope_ref) DO UPDATE "
                "SET "
                "workflow_id=EXCLUDED.workflow_id,topology_revision=EXCLUDED.topology_revision,"
                "generation=EXCLUDED.generation,state=EXCLUDED.state,"
                "participants=EXCLUDED.participants,probe_result='UNKNOWN',probe_ref=NULL,"
                "observed_at=EXCLUDED.observed_at",
                (
                    plan["workloadScopeRef"],
                    plan["workflowId"],
                    topology,
                    generation,
                    Jsonb(participants),
                    observed,
                ),
            )
        return ref

    def renew(self, ref: str, participants: list[dict]) -> None:
        with self.store.connect() as conn:
            gate = conn.execute(
                "SELECT * FROM foxtrot_operator.quiesce_gates WHERE ref=%s FOR UPDATE", (ref,)
            ).fetchone()
            require(
                gate and gate["state"] == "HELD", "INVALID_STATE", "유효한 정지 기록이 아닙니다."
            )
            conn.execute(
                "UPDATE foxtrot_operator.quiesce_gates SET "
                "observed_at=%s,valid_until=%s,participants=%s WHERE ref=%s",
                (now(), now() + timedelta(seconds=30), Jsonb(participants), ref),
            )

    def running(
        self,
        migration_id: str,
        generation: int,
        participants: list[dict],
        probe_result: str,
        probe_ref: str | None,
    ) -> None:
        row = self.store.get(migration_id)
        plan = row["plan"]
        with self.store.connect() as conn:
            conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (plan["workflowId"],)
            )
            row = self.store.row(conn, migration_id)
            current = conn.execute(
                "SELECT * FROM foxtrot_operator.workload_observations WHERE scope_ref=%s FOR "
                "UPDATE",
                (plan["workloadScopeRef"],),
            ).fetchone()
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
            topology = self.topology(plan["workflowId"])
            conn.execute(
                "UPDATE foxtrot_operator.workload_observations SET "
                "state='RUNNING',topology_revision=%s,participants=%s,probe_result=%s,"
                "probe_ref=%s,observed_at=%s WHERE scope_ref=%s",
                (
                    topology,
                    Jsonb(participants),
                    probe_result,
                    probe_ref,
                    now(),
                    plan["workloadScopeRef"],
                ),
            )
            conn.execute(
                "UPDATE foxtrot_operator.quiesce_gates SET state='RELEASED' WHERE scope_ref=%s "
                "AND generation=%s",
                (plan["workloadScopeRef"], generation),
            )


def actor_ack(actor: str, generation: int, state: str, in_flight: int = 0) -> dict:
    return {
        "id": actor,
        "generation": generation,
        "state": state,
        "inFlight": in_flight,
        "observedAt": stamp(),
    }
