from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from .models import ActionRequest, CreateMigration, OperatorError, require

DDL = """
CREATE SCHEMA IF NOT EXISTS foxtrot_operator;
CREATE TABLE IF NOT EXISTS foxtrot_operator.migrations (
 id text PRIMARY KEY, owner text NOT NULL, plan jsonb NOT NULL,
 state jsonb NOT NULL, internal jsonb NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS foxtrot_operator.operations (
 id text PRIMARY KEY, migration_id text NOT NULL REFERENCES foxtrot_operator.migrations,
 action text NOT NULL, status text NOT NULL, request jsonb NOT NULL,
 phase text NOT NULL DEFAULT 'queued', external_intent boolean NOT NULL DEFAULT false,
 confirmed jsonb NOT NULL DEFAULT '[]', error jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS foxtrot_operator.idempotency (
 scope text NOT NULL, key text NOT NULL, body_hash text NOT NULL,
 response jsonb NOT NULL, PRIMARY KEY (scope,key)
);
CREATE TABLE IF NOT EXISTS foxtrot_operator.claims (
 identity text PRIMARY KEY, migration_id text NOT NULL REFERENCES foxtrot_operator.migrations
);
CREATE TABLE IF NOT EXISTS foxtrot_operator.workflows (
 id text PRIMARY KEY, topology_revision bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS foxtrot_operator.quiesce_gates (
 ref text PRIMARY KEY, workflow_id text NOT NULL, migration_id text NOT NULL,
 transition_id text NOT NULL, scope_ref text NOT NULL, topology_revision bigint NOT NULL,
 generation bigint NOT NULL, state text NOT NULL CHECK(state IN ('HELD','RELEASED')),
 held_since timestamptz NOT NULL,
 observed_at timestamptz NOT NULL, valid_until timestamptz NOT NULL, participants jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS foxtrot_operator.workload_observations (
 scope_ref text PRIMARY KEY, workflow_id text NOT NULL, topology_revision bigint NOT NULL,
 generation bigint NOT NULL, state text NOT NULL CHECK(state IN ('HELD','RUNNING')),
 participants jsonb NOT NULL, probe_result text NOT NULL, probe_ref text,
 observed_at timestamptz NOT NULL
);
"""


def now() -> datetime:
    return datetime.now(UTC)


def stamp(value: datetime | None = None) -> str:
    return (value or now()).isoformat().replace("+00:00", "Z")


def decision(result: str, *reasons: str) -> dict[str, Any]:
    return {"result": result, "reasons": list(reasons)}


def initial_state(mid: str, plan: CreateMigration, topology: int) -> dict[str, Any]:
    return {
        "migrationId": mid,
        "workflowId": plan.workflow_id,
        "resourceId": plan.resource_id,
        "kind": "DATABASE",
        "revision": 0,
        "lifecycle": "CREATED",
        "health": "OK",
        "phase": "plan.created",
        "activeOperationId": None,
        "transitionId": None,
        "transitionState": "NONE",
        "topologyRevision": topology,
        "allowedActions": ["PREPARE", "CANCEL", "RECONCILE"],
        "observed": {
            "observedAt": stamp(),
            "route": "UNKNOWN",
            "businessWriteAccess": "UNKNOWN",
            "targetWritesPossible": True,
        },
        "readiness": {
            x: decision("UNKNOWN", "PREPARE_REQUIRED")
            for x in ["cutover", "resume", "rollback", "advance"]
        },
        "rollback": {"method": "UNAVAILABLE", "reasons": ["PREPARE_REQUIRED"]},
        "progress": {"completed": None, "total": None, "unit": "tables"},
        "checks": [
            {
                "code": "QUIESCE_COMPLETE",
                "stage": "QUIESCE",
                "required": True,
                "decision": decision("UNKNOWN", "PREPARE_REQUIRED"),
                "evidenceRef": None,
                "observedAt": None,
                "validUntil": None,
                "transitionId": None,
                "topologyRevision": topology,
            }
        ],
    }


class Store:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def connect(self, *, autocommit: bool = False) -> psycopg.Connection:
        return psycopg.connect(
            self.dsn,
            autocommit=autocommit,
            row_factory=dict_row,
            connect_timeout=5,
            application_name="foxtrot-management",
        )

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.execute(DDL)

    @staticmethod
    def row(conn: psycopg.Connection, mid: str, owner: str | None = None) -> dict[str, Any]:
        row = conn.execute(
            "SELECT * FROM foxtrot_operator.migrations WHERE id=%s FOR UPDATE", (mid,)
        ).fetchone()
        if not row or (owner is not None and row["owner"] != owner):
            raise OperatorError("NOT_FOUND", "이전 작업을 찾을 수 없습니다.", 404)
        return row

    def get(self, mid: str, owner: str | None = None) -> dict[str, Any]:
        with self.connect() as conn:
            return self.row(conn, mid, owner)

    @staticmethod
    def persist(conn: psycopg.Connection, row: dict[str, Any]) -> None:
        conn.execute(
            "UPDATE foxtrot_operator.migrations SET state=%s,internal=%s WHERE id=%s",
            (Jsonb(row["state"]), Jsonb(row["internal"]), row["id"]),
        )

    def update(self, mid: str, fn: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        with self.connect() as conn:
            row = self.row(conn, mid)
            previous = json.dumps(
                {
                    k: row["state"][k]
                    for k in [
                        "lifecycle",
                        "health",
                        "transitionId",
                        "transitionState",
                        "readiness",
                        "allowedActions",
                        "topologyRevision",
                    ]
                },
                sort_keys=True,
            )
            fn(row)
            current = json.dumps(
                {
                    k: row["state"][k]
                    for k in [
                        "lifecycle",
                        "health",
                        "transitionId",
                        "transitionState",
                        "readiness",
                        "allowedActions",
                        "topologyRevision",
                    ]
                },
                sort_keys=True,
            )
            if previous != current:
                row["state"]["revision"] += 1
            self.persist(conn, row)
            return row

    @staticmethod
    def duplicate(conn: psycopg.Connection, scope: str, key: str, body: dict) -> dict | None:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (scope + key,))
        digest = hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        saved = conn.execute(
            "SELECT * FROM foxtrot_operator.idempotency WHERE scope=%s AND key=%s", (scope, key)
        ).fetchone()
        if saved:
            if saved["body_hash"] != digest:
                raise OperatorError("IDEMPOTENCY_CONFLICT", "같은 키에 다른 본문입니다.", 409)
            return saved["response"]
        return None

    @staticmethod
    def remember(
        conn: psycopg.Connection, scope: str, key: str, body: dict, response: dict
    ) -> None:
        digest = hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        conn.execute(
            "INSERT INTO foxtrot_operator.idempotency VALUES (%s,%s,%s,%s)",
            (scope, key, digest, Jsonb(response)),
        )

    @staticmethod
    def claim(conn: psycopg.Connection, identity: str, mid: str) -> None:
        saved = conn.execute(
            "INSERT INTO foxtrot_operator.claims VALUES (%s,%s) ON CONFLICT DO NOTHING RETURNING "
            "migration_id",
            (identity, mid),
        ).fetchone()
        if not saved:
            existing = conn.execute(
                "SELECT migration_id FROM foxtrot_operator.claims WHERE identity=%s", (identity,)
            ).fetchone()
            require(
                existing and existing["migration_id"] == mid,
                "RESOURCE_BUSY",
                "다른 계획이 같은 실제 자원을 사용 중입니다.",
            )

    def create(
        self, owner: str, key: str, plan: CreateMigration, identities: Callable[[], list[str]]
    ) -> dict:
        body = plan.model_dump(by_alias=True)
        scope = owner + ":POST:/v1/migrations"
        with self.connect() as conn:
            duplicate = self.duplicate(conn, scope, key, body)
            if duplicate:
                return duplicate
            conn.execute(
                "INSERT INTO foxtrot_operator.workflows(id) VALUES (%s) ON CONFLICT DO NOTHING",
                (plan.workflow_id,),
            )
            topology = conn.execute(
                "SELECT topology_revision FROM foxtrot_operator.workflows WHERE id=%s",
                (plan.workflow_id,),
            ).fetchone()["topology_revision"]
            mid = "mig-" + uuid4().hex
            state = initial_state(mid, plan, topology)
            conn.execute(
                "INSERT INTO foxtrot_operator.migrations(id,owner,plan,state) VALUES(%s,%s,%s,%s)",
                (mid, owner, Jsonb(body), Jsonb(state)),
            )
            for identity in sorted(set(identities())):
                self.claim(conn, identity, mid)
            self.remember(conn, scope, key, body, state)
            return state

    def submit(
        self,
        owner: str,
        mid: str,
        key: str,
        request: ActionRequest,
        guard: Callable[[psycopg.Connection, dict, ActionRequest], None],
    ) -> dict:
        body = request.model_dump(by_alias=True, exclude_none=True)
        scope = owner + ":POST:/v1/migrations/" + mid + "/actions"
        with self.connect() as conn:
            duplicate = self.duplicate(conn, scope, key, body)
            if duplicate:
                return duplicate
            row = self.row(conn, mid, owner)
            state = row["state"]
            if state["revision"] != request.expected_revision:
                raise OperatorError("REVISION_CONFLICT", "최신 revision을 조회하십시오.", 409)
            if state["activeOperationId"]:
                raise OperatorError("RESOURCE_BUSY", "명령이 이미 실행 중입니다.", 409)
            guard(conn, row, request)
            oid = "op-" + uuid4().hex
            if request.transition_id and state["transitionId"] is None:
                state.update(transitionId=request.transition_id, transitionState="OPEN")
            state["activeOperationId"] = oid
            state["revision"] += 1
            state["allowedActions"] = []
            conn.execute(
                "INSERT INTO foxtrot_operator.operations(id,migration_id,action,status,request) "
                "VALUES(%s,%s,%s,'QUEUED',%s)",
                (oid, mid, request.action, Jsonb(body)),
            )
            self.persist(conn, row)
            response = {
                "operationId": oid,
                "migrationId": mid,
                "action": str(request.action),
                "status": "QUEUED",
                "statusUrl": "/v1/operations/" + oid,
            }
            self.remember(conn, scope, key, body, response)
            return response

    def operation(self, oid: str, owner: str | None = None) -> dict:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT o.*,m.owner FROM foxtrot_operator.operations o JOIN "
                "foxtrot_operator.migrations m ON m.id=o.migration_id WHERE o.id=%s",
                (oid,),
            ).fetchone()
            if not row or (owner and row["owner"] != owner):
                raise OperatorError("NOT_FOUND", "명령을 찾을 수 없습니다.", 404)
            return {
                "operationId": row["id"],
                "migrationId": row["migration_id"],
                "action": row["action"],
                "status": row["status"],
                "error": row["error"],
                "confirmedChanges": row["confirmed"],
            }

    def claim_operation(self) -> dict | None:
        with self.connect() as conn:
            op = conn.execute(
                "SELECT * FROM foxtrot_operator.operations WHERE status='QUEUED' ORDER BY "
                "created_at FOR UPDATE SKIP LOCKED LIMIT 1"
            ).fetchone()
            if op:
                conn.execute(
                    "UPDATE foxtrot_operator.operations SET status='RUNNING' WHERE id=%s",
                    (op["id"],),
                )
            return op

    def intent(self, oid: str, phase: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE foxtrot_operator.operations SET external_intent=true,phase=%s WHERE id=%s",
                (phase, oid),
            )
            conn.execute(
                "UPDATE foxtrot_operator.migrations SET state=jsonb_set(state,'{phase}',%s) WHERE "
                "id=(SELECT migration_id FROM foxtrot_operator.operations WHERE id=%s)",
                (Jsonb(phase), oid),
            )

    def confirmed(self, oid: str, change: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE foxtrot_operator.operations SET confirmed=confirmed || %s WHERE id=%s",
                (Jsonb([change]), oid),
            )

    def finish(self, op: dict, status: str, error: dict | None = None) -> None:
        with self.connect() as conn:
            row = self.row(conn, op["migration_id"])
            # A late old executor must not clear or overwrite a newer reconciliation.
            if row["state"]["activeOperationId"] != op["id"]:
                return
            conn.execute(
                "UPDATE foxtrot_operator.operations SET status=%s,error=%s WHERE id=%s",
                (status, Jsonb(error) if error else None, op["id"]),
            )
            row["state"]["activeOperationId"] = None
            if status != "SUCCEEDED":
                row["state"]["health"] = status
                row["internal"]["unresolvedOperationId"] = op["id"]
                row["internal"].pop("stableSince", None)
                for field in row["state"]["readiness"]:
                    row["state"]["readiness"][field] = decision("UNKNOWN", "RECONCILE_REQUIRED")
            row["state"]["allowedActions"] = ["RECONCILE"]
            row["state"]["revision"] += 1
            self.persist(conn, row)

    def recover_interrupted(self) -> None:
        with self.connect() as conn:
            interrupted = conn.execute(
                "SELECT * FROM foxtrot_operator.operations WHERE status='RUNNING'"
            ).fetchall()
        for op in interrupted:
            status = "UNKNOWN" if op["external_intent"] else "FAILED"
            self.finish(
                op,
                status,
                {
                    "code": "EXECUTOR_INTERRUPTED",
                    "message": "이전 실행 상태를 확인하십시오.",
                    "operationId": op["id"],
                    "blockers": ["RECONCILE_REQUIRED"],
                },
            )
