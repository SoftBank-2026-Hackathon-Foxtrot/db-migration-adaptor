"""DynamoDB에 작업, 실행 의도, 업무 정지 근거와 프록시 경로를 저장합니다."""

from __future__ import annotations

import hashlib
import json
import random
import time
from collections.abc import Callable
from contextlib import contextmanager, suppress
from copy import deepcopy
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from .config import Registry
from .models import ActionRequest, CreateMigration, OperatorError, require


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


REVISION_FIELDS = (
    "lifecycle",
    "health",
    "transitionId",
    "transitionState",
    "readiness",
    "allowedActions",
    "topologyRevision",
)


def revision_values(state: dict) -> str:
    return json.dumps({key: state[key] for key in REVISION_FIELDS}, sort_keys=True)


def scope_prefix(reference: str) -> str:
    """참조 안의 #이 다른 참조의 조회 범위로 해석되지 않게 합니다."""
    return f"{len(reference)}:{reference}#"


def catalog_hash(registry: Registry, binding_ref: str) -> str:
    binding = registry.bindings[binding_ref]
    catalog = {
        "alias": binding.alias,
        "databases": {
            ref: {
                "identity": db.identity,
                "host": db.route_host,
                "port": db.route_port,
                "database": db.database,
                "writer": db.writer_role,
            }
            for ref, db in registry.databases.items()
        },
    }
    return hashlib.sha256(json.dumps(catalog, sort_keys=True).encode()).hexdigest()


class StateConflict(OperatorError):
    def __init__(self):
        super().__init__("STATE_CONFLICT", "상태가 변경됐습니다. 같은 키로 다시 확인하십시오.", 409)


def encode(value: Any) -> str:
    def default(item):
        if isinstance(item, datetime):
            return {"__foxtrot_datetime__": item.isoformat()}
        raise TypeError(type(item).__name__)

    return json.dumps(value, default=default, sort_keys=True, separators=(",", ":"))


def decode(value: str) -> Any:
    def hook(item):
        if set(item) == {"__foxtrot_datetime__"}:
            return datetime.fromisoformat(item["__foxtrot_datetime__"])
        return item

    return json.loads(value, object_hook=hook)


class Transaction:
    """개별 항목을 읽고 namespace revision을 조건으로 함께 저장합니다.

    namespace는 단일 앱의 관리 영역입니다. epoch는 여러 항목의 변경과 Query의
    추가/삭제 경합을 감지합니다. 업무 SQL 요청은 이 저장소를 사용하지 않습니다.
    """

    def __init__(self, store: Store):
        self.store = store
        self.version = store._epoch()
        self.pending: dict[str, dict | None] = {}
        self.leases: list[Lease] = []

    def require_lease(self, lease: Lease) -> None:
        self.leases.append(lease)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        elif isinstance(exc, OperatorError) and self.version != self.store._epoch():
            # 여러 항목을 읽는 중 다른 요청이 반영되면 거절 근거도 다시 계산합니다.
            raise StateConflict() from exc

    def get(self, kind: str, identity: str) -> dict | None:
        key = kind + "#" + identity
        if key in self.pending:
            return deepcopy(self.pending[key])
        return self.store._read(key)

    def put(self, kind: str, identity: str, value: dict) -> None:
        self.pending[kind + "#" + identity] = deepcopy(value)

    def delete(self, kind: str, identity: str) -> None:
        self.pending[kind + "#" + identity] = None

    def list(self, kind: str, prefix: str = "") -> list[dict]:
        start = kind + "#" + prefix
        values = dict(self.store._query(start))
        for key, value in self.pending.items():
            if key.startswith(start):
                if value is None:
                    values.pop(key, None)
                else:
                    values[key] = value
        return [deepcopy(value) for _, value in sorted(values.items())]

    def commit(self) -> None:
        if not self.pending:
            if self.version != self.store._epoch():
                raise StateConflict()
            return
        changes = [
            {
                "Update": {
                    "TableName": self.store.table,
                    "Key": self.store._key("EPOCH"),
                    "UpdateExpression": "SET #v = :next",
                    "ConditionExpression": "#v = :old",
                    "ExpressionAttributeNames": {"#v": "version"},
                    "ExpressionAttributeValues": {":old": self.version, ":next": self.version + 1},
                }
            }
        ]
        leases = list(self.leases)
        if self.store._executor:
            leases.append(self.store._executor)
        for lease in {item.name: item for item in leases}.values():
            if "LOCK#" + lease.name in self.pending:
                continue
            changes.append(
                {
                    "ConditionCheck": {
                        "TableName": self.store.table,
                        "Key": self.store._key("LOCK#" + lease.name),
                        "ConditionExpression": "#o = :owner",
                        "ExpressionAttributeNames": {"#o": "lock_owner"},
                        "ExpressionAttributeValues": {":owner": lease.owner},
                    }
                }
            )
        for key, value in self.pending.items():
            if value is None:
                changes.append(
                    {"Delete": {"TableName": self.store.table, "Key": self.store._key(key)}}
                )
            else:
                payload = encode(value)
                require(
                    len(payload.encode()) + len(key.encode()) < 390_000,
                    "STATE_TOO_LARGE",
                    "상태 기록이 항목 크기 상한에 가깝습니다.",
                )
                item = {**self.store._key(key), "payload": payload}
                if key.startswith("LOCK#"):
                    item["lock_owner"] = value["owner"]
                changes.append({"Put": {"TableName": self.store.table, "Item": item}})
        require(
            len(changes) <= 100,
            "STATE_TRANSACTION_TOO_LARGE",
            "한 번에 변경할 상태가 너무 많습니다.",
        )
        try:
            self.store.client.transact_write_items(
                TransactItems=changes, ClientRequestToken=uuid4().hex
            )
        except self.store.client.exceptions.TransactionCanceledException as error:
            for lease in leases:
                lease.assert_owned()
            codes = {r.get("Code") for r in error.response.get("CancellationReasons", [])} - {
                "None",
                None,
            }
            if codes and codes <= {"ConditionalCheckFailed", "TransactionConflict"}:
                raise StateConflict() from error
            raise
        except self.store.client.exceptions.TransactionConflictException as error:
            raise StateConflict() from error


class Lease:
    """자동 만료하지 않는 실행 권한. 강제 복구 전에 이전 실행기를 정지해야 합니다."""

    def __init__(self, store: Store, name: str, owner: str):
        self.store, self.name, self.owner = store, name, owner

    def assert_owned(self) -> None:
        saved = self.store._read("LOCK#" + self.name)
        require(
            saved and saved["owner"] == self.owner,
            "EXECUTOR_LOCK_LOST",
            "실행기 잠금을 잃었습니다.",
        )


class RouteJournal:
    def __init__(self, store: Store, registry: Registry, binding_ref: str):
        self.store, self.registry, self.binding_ref = store, registry, binding_ref

    def register(self, initial_database_ref: str) -> dict:
        require(
            initial_database_ref in self.registry.databases,
            "UNKNOWN_REFERENCE",
            "초기 DB 참조를 확인하십시오.",
        )

        def register(tx):
            row = tx.get("ROUTE", self.binding_ref)
            if row is None:
                require(
                    not tx.list("BINDING_MIG", scope_prefix(self.binding_ref)),
                    "RUNTIME_ADOPTION_REQUIRED",
                    "기존 이전 기록이 있습니다. 초기 DB를 자동으로 선택하지 않습니다.",
                )
                row = {
                    "binding_ref": self.binding_ref,
                    "database_ref": initial_database_ref,
                    "catalog_hash": catalog_hash(self.registry, self.binding_ref),
                    "access": "OPEN",
                    "updated_at": now(),
                }
                tx.put("ROUTE", self.binding_ref, row)
            return self._validate(row)

        return self.store.transact(register)

    def read(self) -> dict:
        return self._validate(self.store._read("ROUTE#" + self.binding_ref))

    def _validate(self, row: dict | None) -> dict:
        require(row is not None, "RUNTIME_ROUTE_MISSING", "저장한 프록시 경로가 없습니다.")
        require(
            row["catalog_hash"] == catalog_hash(self.registry, self.binding_ref)
            and row["database_ref"] in self.registry.databases,
            "RUNTIME_CATALOG_CHANGED",
            "등록한 DB 경로가 변경됐습니다. 기존 경로를 먼저 확인하십시오.",
        )
        return row


class Store:
    """DynamoDB 관리 저장소. namespace마다 단일 앱의 상태를 분리합니다."""

    def __init__(
        self,
        table: str,
        *,
        namespace: str = "foxtrot-db-operator",
        region: str | None = None,
        endpoint: str | None = None,
        client=None,
    ):
        self.table, self.namespace = table, namespace
        self.client = (
            client
            if client is not None
            else boto3.resource(
                "dynamodb",
                region_name=region,
                endpoint_url=endpoint,
                config=Config(
                    connect_timeout=3,
                    read_timeout=5,
                    retries={"mode": "standard", "total_max_attempts": 3},
                ),
            ).meta.client
        )
        self._executor: Lease | None = None
        self._pending_release: dict[str, str] = {}

    def _key(self, record: str) -> dict:
        control = record.split("#", 1)[0] in {"GATE", "WORKLOAD", "SCOPE_GATE"}
        namespace = self.namespace + ":control" if control else self.namespace
        return {"namespace": namespace, "record": record}

    def _read(self, record: str) -> dict | None:
        item = self.client.get_item(
            TableName=self.table, Key=self._key(record), ConsistentRead=True
        ).get("Item")
        return decode(item["payload"]) if item else None

    def _epoch(self) -> int:
        item = self.client.get_item(
            TableName=self.table, Key=self._key("EPOCH"), ConsistentRead=True
        ).get("Item")
        require(
            item is not None, "STATE_NOT_INITIALIZED", "DynamoDB 상태 영역을 먼저 초기화하십시오."
        )
        return int(item["version"])

    def _query(self, prefix: str):
        paginator = self.client.get_paginator("query")
        for page in paginator.paginate(
            TableName=self.table,
            ConsistentRead=True,
            KeyConditionExpression="#n = :n AND begins_with(#r, :r)",
            ExpressionAttributeNames={"#n": "namespace", "#r": "record"},
            ExpressionAttributeValues={":n": self._key(prefix)["namespace"], ":r": prefix},
        ):
            for item in page["Items"]:
                yield item["record"], decode(item["payload"])

    def initialize(self, *, create: bool = False) -> None:
        try:
            table = self.client.describe_table(TableName=self.table)["Table"]
        except self.client.exceptions.ResourceNotFoundException:
            if not create:
                raise OperatorError(
                    "STATE_TABLE_MISSING", "DynamoDB 테이블을 먼저 생성하십시오.", 503
                ) from None
            self.client.create_table(
                TableName=self.table,
                BillingMode="PAY_PER_REQUEST",
                KeySchema=[
                    {"AttributeName": "namespace", "KeyType": "HASH"},
                    {"AttributeName": "record", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "namespace", "AttributeType": "S"},
                    {"AttributeName": "record", "AttributeType": "S"},
                ],
                DeletionProtectionEnabled=True,
            )
            self.client.get_waiter("table_exists").wait(TableName=self.table)
            table = self.client.describe_table(TableName=self.table)["Table"]
        require(
            {(k["AttributeName"], k["KeyType"]) for k in table["KeySchema"]}
            == {("namespace", "HASH"), ("record", "RANGE")},
            "STATE_TABLE_SCHEMA_MISMATCH",
            "DynamoDB 키 구성이 다릅니다.",
        )
        with suppress(self.client.exceptions.ConditionalCheckFailedException):
            self.client.put_item(
                TableName=self.table,
                Item={**self._key("EPOCH"), "version": 0},
                ConditionExpression="attribute_not_exists(#n)",
                ExpressionAttributeNames={"#n": "namespace"},
            )

    def health(self) -> None:
        self._epoch()

    def transaction(self) -> Transaction:
        return Transaction(self)

    def transact(self, fn: Callable[[Transaction], Any]):
        for attempt in range(8):
            try:
                with self.transaction() as tx:
                    result = fn(tx)
                return result
            except StateConflict:
                if attempt == 7:
                    raise
                time.sleep(random.uniform(0.002, 0.01) * (attempt + 1))

    @contextmanager
    def exclusive(self, name: str = "executor"):
        # 이 프로세스가 반납하지 못한 자기 잠금만 다시 확인할 수 있습니다.
        owner = self._pending_release.get(name, uuid4().hex)

        def acquire(tx):
            current = tx.get("LOCK", name)
            require(
                current is None
                or (self._pending_release.get(name) == owner and current["owner"] == owner),
                "EXECUTOR_BUSY",
                "다른 실행기의 잠금이 있습니다. 종료 근거를 확인하십시오.",
            )
            tx.put("LOCK", name, current or {"name": name, "owner": owner, "acquired_at": now()})

        try:
            self.transact(acquire)
        except (BotoCoreError, ClientError):
            self._pending_release[name] = owner
            raise
        lease = Lease(self, name, owner)
        previous = self._executor
        if name == "executor":
            self._executor = lease
        try:
            yield lease
        finally:
            self._executor = previous

            def release(tx):
                current = tx.get("LOCK", name)
                if current and current["owner"] == owner:
                    tx.delete("LOCK", name)

            try:
                self.transact(release)
            except (BotoCoreError, ClientError, StateConflict):
                self._pending_release[name] = owner
                raise
            else:
                self._pending_release.pop(name, None)

    def release_stopped_executor(self, name: str, owner: str, stopped_evidence: str) -> None:
        require(
            bool(stopped_evidence.strip()),
            "STOP_EVIDENCE_REQUIRED",
            "이전 실행기의 종료 근거가 필요합니다.",
        )

        def release(tx):
            current = tx.get("LOCK", name)
            require(
                current and current["owner"] == owner,
                "EXECUTOR_LOCK_MISMATCH",
                "현재 잠금 소유자가 다릅니다.",
            )
            tx.put(
                "LOCK_RECOVERY",
                uuid4().hex,
                {**current, "stopped_evidence": stopped_evidence, "released_at": now()},
            )
            tx.delete("LOCK", name)

        self.transact(release)

    @staticmethod
    def row(tx: Transaction, mid: str, owner: str | None = None) -> dict:
        row = tx.get("MIG", mid)
        if not row or (owner is not None and row["owner"] != owner):
            raise OperatorError("NOT_FOUND", "이전 작업을 찾을 수 없습니다.", 404)
        return row

    def get(self, mid: str, owner: str | None = None) -> dict:
        row = self._read("MIG#" + mid)
        if not row or (owner is not None and row["owner"] != owner):
            raise OperatorError("NOT_FOUND", "이전 작업을 찾을 수 없습니다.", 404)
        return row

    @staticmethod
    def persist(tx: Transaction, row: dict) -> None:
        tx.put("MIG", row["id"], row)

    def scope_migrations(self, tx: Transaction, scope: str) -> list[dict]:
        return [
            self.row(tx, ref["migration_id"]) for ref in tx.list("SCOPE_MIG", scope_prefix(scope))
        ]

    def control_idle(self, tx: Transaction, scope: str) -> None:
        require(
            not any(
                r["state"]["activeOperationId"]
                or (r["internal"].get("finalizeStarted") and r["state"]["lifecycle"] != "COMPLETED")
                for r in self.scope_migrations(tx, scope)
            ),
            "RESOURCE_BUSY",
            "명령 실행 중에는 같은 업무 범위의 제어 상태를 바꾸지 않습니다.",
        )

    def invalidate_advance(self, tx: Transaction, scope: str) -> None:
        for row in self.scope_migrations(tx, scope):
            state, internal = row["state"], row["internal"]
            if state["lifecycle"] in {"COMPLETED", "CANCELLED"}:
                continue
            internal["controlEpoch"] = internal.get("controlEpoch", 0) + 1
            for key in ["stableSince", "stableIdentity", "advanceObservedAt", "observationCache"]:
                internal.pop(key, None)
            state["readiness"]["advance"] = decision("UNKNOWN", "WORKLOAD_CHANGED")
            state["allowedActions"] = [a for a in state["allowedActions"] if a != "FINALIZE"]
            for check in state["checks"]:
                if check["stage"] == "ADVANCE":
                    check["decision"] = decision("UNKNOWN", "WORKLOAD_CHANGED")
            state["revision"] += 1
            self.persist(tx, row)

    def update(
        self,
        mid: str,
        fn: Callable[[dict], None],
        *,
        expected_revision: int | None = None,
        idle: bool = False,
        cache_observation: bool = False,
    ) -> dict:
        def update(tx):
            row = self.row(tx, mid)
            if (
                expected_revision is not None and row["state"]["revision"] != expected_revision
            ) or (idle and row["state"]["activeOperationId"]):
                return row
            previous = revision_values(row["state"])
            fn(row)
            if previous != revision_values(row["state"]):
                row["state"]["revision"] += 1
            if cache_observation:
                row["internal"]["observationCache"] = {
                    "revision": row["state"]["revision"],
                    "at": stamp(),
                }
            self.persist(tx, row)
            return row

        return self.transact(update)

    @staticmethod
    def duplicate(tx: Transaction, scope: str, key: str, body: dict) -> dict | None:
        saved = tx.get("IDEMP", request_digest({"scope": scope, "key": key}))
        if saved:
            if saved["body_hash"] != request_digest(body):
                raise OperatorError("IDEMPOTENCY_CONFLICT", "같은 키에 다른 본문입니다.", 409)
            return saved["response"]
        return None

    @staticmethod
    def remember(tx: Transaction, scope: str, key: str, body: dict, response: dict) -> None:
        tx.put(
            "IDEMP",
            request_digest({"scope": scope, "key": key}),
            {"body_hash": request_digest(body), "response": response},
        )

    @staticmethod
    def claim(tx: Transaction, identity: str, mid: str) -> None:
        key = hashlib.sha256(identity.encode()).hexdigest()
        saved = tx.get("CLAIM", key)
        require(
            not saved or saved["migration_id"] == mid,
            "RESOURCE_BUSY",
            "다른 계획이 같은 실제 자원을 사용 중입니다.",
        )
        tx.put("CLAIM", key, {"identity": identity, "migration_id": mid})
        tx.put("MIG_CLAIM", mid + "#" + key, {"claim_key": key, "migration_id": mid})

    def create(
        self, owner: str, key: str, plan: CreateMigration, identities: Callable[[], list[str]]
    ) -> dict:
        body = plan.model_dump(by_alias=True)
        scope = owner + ":POST:/v1/migrations"

        def create(tx):
            duplicate = self.duplicate(tx, scope, key, body)
            if duplicate:
                return duplicate
            workflow = tx.get("WORKFLOW", plan.workflow_id) or {"topology_revision": 0}
            tx.put("WORKFLOW", plan.workflow_id, workflow)
            mid = "mig-" + uuid4().hex
            state = initial_state(mid, plan, workflow["topology_revision"])
            tx.put(
                "MIG",
                mid,
                {"id": mid, "owner": owner, "plan": body, "state": state, "internal": {}},
            )
            for kind, ref in [
                ("SCOPE_MIG", plan.workload_scope_ref),
                ("BINDING_MIG", plan.binding_ref),
            ]:
                tx.put(kind, scope_prefix(ref) + mid, {"migration_id": mid})
            for identity in sorted(set(identities())):
                self.claim(tx, identity, mid)
            self.remember(tx, scope, key, body, state)
            return state

        return self.transact(create)

    def submit(
        self, owner: str, mid: str, key: str, request: ActionRequest, guard: Callable
    ) -> dict:
        body = request.model_dump(by_alias=True, exclude_none=True)
        scope = owner + ":POST:/v1/migrations/" + mid + "/actions"

        def submit(tx):
            duplicate = self.duplicate(tx, scope, key, body)
            if duplicate:
                return duplicate
            row = self.row(tx, mid, owner)
            state = row["state"]
            if state["revision"] != request.expected_revision:
                raise OperatorError("REVISION_CONFLICT", "최신 revision을 조회하십시오.", 409)
            if state["activeOperationId"]:
                raise OperatorError("RESOURCE_BUSY", "명령이 이미 실행 중입니다.", 409)
            guard(tx, row, request)
            oid = "op-" + uuid4().hex
            if request.transition_id and state["transitionId"] is None:
                state.update(transitionId=request.transition_id, transitionState="OPEN")
            state.update(activeOperationId=oid, revision=state["revision"] + 1, allowedActions=[])
            queue_key = stamp() + "#" + oid
            op = {
                "id": oid,
                "migration_id": mid,
                "action": str(request.action),
                "status": "QUEUED",
                "request": body,
                "phase": "queued",
                "external_intent": False,
                "confirmed": [],
                "error": None,
                "created_at": now(),
                "queue_key": queue_key,
            }
            tx.put("OP", oid, op)
            tx.put("QUEUE", queue_key, {"operation_id": oid})
            self.persist(tx, row)
            response = {
                "operationId": oid,
                "migrationId": mid,
                "action": str(request.action),
                "status": "QUEUED",
                "statusUrl": "/v1/operations/" + oid,
            }
            self.remember(tx, scope, key, body, response)
            return response

        return self.transact(submit)

    def operation(self, oid: str, owner: str | None = None) -> dict:
        def read(tx):
            row = tx.get("OP", oid)
            if not row:
                raise OperatorError("NOT_FOUND", "명령을 찾을 수 없습니다.", 404)
            self.row(tx, row["migration_id"], owner)
            return {
                "operationId": row["id"],
                "migrationId": row["migration_id"],
                "action": row["action"],
                "status": row["status"],
                "error": row["error"],
                "confirmedChanges": row["confirmed"],
            }

        return self.transact(read)

    def claim_operation(self) -> dict | None:
        require(self._executor is not None, "EXECUTOR_NOT_LOCKED", "단일 실행기 잠금이 필요합니다.")

        def claim(tx):
            queued = tx.list("QUEUE")
            if not queued:
                return None
            op = tx.get("OP", queued[0]["operation_id"])
            require(
                op and op["status"] == "QUEUED", "QUEUE_INCONSISTENT", "명령 대기 상태가 다릅니다."
            )
            op["status"] = "RUNNING"
            tx.put("OP", op["id"], op)
            tx.delete("QUEUE", op["queue_key"])
            tx.put("RUNNING", op["id"], {"operation_id": op["id"]})
            return op

        return self.transact(claim)

    def intent(self, oid: str, phase: str) -> None:
        def save(tx):
            op = tx.get("OP", oid)
            require(op is not None, "NOT_FOUND", "명령을 찾을 수 없습니다.")
            row = self.row(tx, op["migration_id"])
            require(
                row["state"]["activeOperationId"] == oid,
                "EXECUTOR_NOT_OWNER",
                "현재 실행 명령이 아닙니다.",
            )
            route = tx.get("ROUTE", row["plan"]["bindingRef"])
            if route and phase in {
                "pgbouncer.pause",
                "pg.fence_writer",
                "pgbouncer.atomic_config",
                "pgbouncer.reload",
                "pg.allow_writer",
                "pgbouncer.resume",
            }:
                route.update(access="HELD", updated_at=now())
                if phase == "pgbouncer.atomic_config" and op["action"] in {"CUTOVER", "ROLLBACK"}:
                    route["database_ref"] = row["plan"][
                        "targetRef" if op["action"] == "CUTOVER" else "sourceRef"
                    ]
                tx.put("ROUTE", row["plan"]["bindingRef"], route)
            op.update(external_intent=True, phase=phase)
            row["state"]["phase"] = phase
            tx.put("OP", oid, op)
            self.persist(tx, row)

        self.transact(save)

    def confirmed(self, oid: str, change: str) -> None:
        def save(tx):
            op = tx.get("OP", oid)
            require(op is not None, "NOT_FOUND", "명령을 찾을 수 없습니다.")
            op["confirmed"].append(change)
            tx.put("OP", oid, op)

        self.transact(save)

    def finish(self, op: dict, status: str, error: dict | None = None) -> None:
        def finish(tx):
            row = self.row(tx, op["migration_id"])
            if row["state"]["activeOperationId"] != op["id"]:
                return
            saved = tx.get("OP", op["id"])
            if status == "SUCCEEDED" and saved["action"] == "RESUME":
                self._open_after_resume(tx, saved, row)
            saved.update(status=status, error=error)
            tx.put("OP", op["id"], saved)
            tx.delete("RUNNING", op["id"])
            row["state"]["activeOperationId"] = None
            if status != "SUCCEEDED":
                row["state"]["health"] = status
                row["internal"]["unresolvedOperationId"] = op["id"]
                if saved["action"] != "RECONCILE" and (
                    saved["external_intent"] or not row["internal"].get("recoveryOperationId")
                ):
                    row["internal"]["recoveryOperationId"] = op["id"]
                row["internal"].pop("stableSince", None)
                for field in row["state"]["readiness"]:
                    row["state"]["readiness"][field] = decision("UNKNOWN", "RECONCILE_REQUIRED")
            row["state"]["allowedActions"] = ["RECONCILE"]
            row["state"]["revision"] += 1
            self.persist(tx, row)

        self.transact(finish)

    @staticmethod
    def _open_after_resume(tx: Transaction, op: dict, row: dict) -> None:
        binding_ref = row["plan"]["bindingRef"]
        route = tx.get("ROUTE", binding_ref)
        if not route:
            return
        location = op["request"].get("expectedLocation")
        observed = row["internal"].get("observedDetails", {})
        require(
            location in {"SOURCE", "TARGET"}
            and row["state"]["health"] == "OK"
            and observed.get("route") == location
            and observed.get("access") == location + "_ONLY"
            and observed.get("paused") is False,
            "RESUME_UNCONFIRMED",
            "재개한 DB의 경로와 단독 쓰기를 확인할 수 없습니다.",
        )
        route.update(
            database_ref=row["plan"][location.lower() + "Ref"], access="OPEN", updated_at=now()
        )
        tx.put("ROUTE", binding_ref, route)

    def recover_interrupted(self) -> None:
        require(
            self._executor is not None,
            "EXECUTOR_NOT_LOCKED",
            "이전 실행기의 종료 확인과 잠금이 필요합니다.",
        )
        interrupted = self.transact(
            lambda tx: [tx.get("OP", r["operation_id"]) for r in tx.list("RUNNING")]
        )
        for op in interrupted:
            self.finish(
                op,
                "UNKNOWN" if op["external_intent"] else "FAILED",
                {
                    "code": "EXECUTOR_INTERRUPTED",
                    "message": "이전 실행 상태를 확인하십시오.",
                    "operationId": op["id"],
                    "blockers": ["RECONCILE_REQUIRED"],
                },
            )

    def recovery_command(self, record: dict) -> dict | None:
        internal = record["internal"]
        oid = (
            internal.get("recoveryOperationId")
            or internal.get("unresolvedOperationId")
            or internal.get("reconciledOperationId")
        )
        op = self._read("OP#" + str(oid))
        return op if op and op["migration_id"] == record["id"] else None

    def release_claims(self, migration_id: str) -> None:
        def release(tx):
            for ref in tx.list("MIG_CLAIM", migration_id + "#"):
                saved = tx.get("CLAIM", ref["claim_key"])
                if saved and saved["migration_id"] == migration_id:
                    tx.delete("CLAIM", ref["claim_key"])
                tx.delete("MIG_CLAIM", migration_id + "#" + ref["claim_key"])

        self.transact(release)

    @staticmethod
    def workload_record(tx: Transaction, scope: str) -> dict | None:
        return tx.get("WORKLOAD", scope)

    def external_change_started(self, operation_id: str) -> bool:
        return self._read("OP#" + operation_id)["external_intent"]

    def begin_finalization(self, migration_id: str, operation_id: str, check: Callable) -> None:
        def begin(tx):
            row = self.row(tx, migration_id)
            require(
                row["state"]["activeOperationId"] == operation_id,
                "EXECUTOR_NOT_OWNER",
                "현재 명령의 실행 권한이 없습니다.",
            )
            observed = check(tx, row, operation_id)
            row["internal"].update(
                finalizeStarted=True, finalizeLocation=observed["route"], finalizeApprovedAt=stamp()
            )
            row["state"]["phase"] = "finalize.cleanup"
            op = tx.get("OP", operation_id)
            op.update(external_intent=True, phase="finalize.cleanup")
            tx.put("OP", operation_id, op)
            self.persist(tx, row)

        self.transact(begin)


def request_digest(body: dict) -> str:
    return hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
