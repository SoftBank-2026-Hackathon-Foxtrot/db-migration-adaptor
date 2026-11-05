"""DynamoDB Local에서 원자성, 경합, 실행 권한과 경로 보존을 검사합니다."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest

from db_operator.models import PROFILE, ActionRequest, CreateMigration, OperatorError
from db_operator.store import Lease, StateConflict, Store, now

pytestmark = pytest.mark.integration


def plan():
    return CreateMigration(
        workflow_id="workflow",
        resource_id="db",
        kind="DATABASE",
        profile=PROFILE,
        source_ref="source",
        target_ref="target",
        binding_ref="binding",
        workload_scope_ref="scope",
        policy={"rollbackMode": "REVERSE_REQUIRED", "maxQuiesceSeconds": 300},
    )


def test_conflicting_transaction_has_no_partial_writes(state_store):
    old = state_store.transaction()
    old.put("OP", "stale", {"status": "QUEUED"})
    old.put("ROUTE", "binding", {"access": "OPEN"})
    state_store.transact(lambda tx: tx.put("ROUTE", "binding", {"access": "HELD"}))
    with pytest.raises(StateConflict):
        old.commit()
    assert state_store._read("OP#stale") is None
    assert state_store._read("ROUTE#binding") == {"access": "HELD"}


def test_query_snapshot_detects_insertions(state_store):
    snapshot = state_store.transaction()
    assert snapshot.list("QUEUE") == []
    state_store.transact(lambda tx: tx.put("QUEUE", "new", {"operation_id": "op-new"}))
    with pytest.raises(StateConflict):
        snapshot.commit()


def test_concurrent_duplicate_creation_has_one_plan_and_claim(state_store):
    def create(_):
        return state_store.create("owner", "same-key", plan(), lambda: ["source", "target"])

    with ThreadPoolExecutor(max_workers=8) as pool:
        states = list(pool.map(create, range(12)))
    assert len({s["migrationId"] for s in states}) == 1
    assert state_store.transact(lambda tx: len(tx.list("MIG"))) == 1
    assert state_store.transact(lambda tx: len(tx.list("CLAIM"))) == 2
    with pytest.raises(OperatorError) as caught:
        state_store.create(
            "owner", "same-key", plan().model_copy(update={"resource_id": "other"}), lambda: []
        )
    assert caught.value.code == "IDEMPOTENCY_CONFLICT" and caught.value.status == 409


def test_revision_and_idempotency_guard_command_acceptance(state_store):
    mid = state_store.create("owner", "create", plan(), lambda: [])["migrationId"]
    request = ActionRequest(action="PREPARE", expected_revision=0)
    first = state_store.submit("owner", mid, "prepare", request, lambda *_: None)
    assert state_store.submit("owner", mid, "prepare", request, lambda *_: None) == first
    with pytest.raises(OperatorError) as caught:
        state_store.submit("owner", mid, "different-key", request, lambda *_: None)
    assert caught.value.code == "REVISION_CONFLICT" and caught.value.status == 409
    assert state_store.transact(lambda tx: len(tx.list("OP"))) == 1


def test_route_hold_and_intent_survive_executor_restart(state_store):
    mid = state_store.create("owner", "create", plan(), lambda: [])["migrationId"]
    state_store.transact(
        lambda tx: tx.put("ROUTE", "binding", {"access": "OPEN", "database_ref": "source"})
    )
    state_store.submit(
        "owner",
        mid,
        "cutover",
        ActionRequest(action="CUTOVER", expected_revision=0, transition_id="t", quiesce_ref="q"),
        lambda *_: None,
    )
    with state_store.exclusive():
        op = state_store.claim_operation()
        state_store.intent(op["id"], "pgbouncer.atomic_config")
    restarted = Store(state_store.table, namespace=state_store.namespace, client=state_store.client)
    with restarted.exclusive():
        restarted.recover_interrupted()
        assert restarted.claim_operation() is None
    assert restarted.operation(op["id"])["status"] == "UNKNOWN"
    route = restarted._read("ROUTE#binding")
    assert route["access"] == "HELD" and route["database_ref"] == "target"


def test_old_lock_requires_stop_evidence_and_exact_owner(state_store):
    state_store.transact(
        lambda tx: tx.put(
            "LOCK",
            "executor",
            {
                "owner": "previous",
                "name": "executor",
                "acquired_at": now() - timedelta(days=30),
            },
        )
    )
    with pytest.raises(OperatorError, match="다른 실행기"), state_store.exclusive():
        pass
    with pytest.raises(OperatorError, match="소유자"):
        state_store.release_stopped_executor("executor", "wrong", "test:stopped")
    state_store.release_stopped_executor("executor", "previous", "test:stopped")
    with state_store.exclusive() as current:
        current.assert_owned()
        with pytest.raises(OperatorError, match="실행기 잠금"):
            Lease(state_store, "executor", "previous").assert_owned()
    assert len(state_store.transact(lambda tx: tx.list("LOCK_RECOVERY"))) == 1


def test_control_evidence_uses_separate_iam_partition(state_store):
    state_store.transact(lambda tx: tx.put("WORKLOAD", "scope", {"state": "HELD"}))
    client = state_store.client
    for namespace, exists in [
        (state_store.namespace, False),
        (state_store.namespace + ":control", True),
    ]:
        result = client.get_item(
            TableName=state_store.table,
            Key={"namespace": namespace, "record": "WORKLOAD#scope"},
            ConsistentRead=True,
        )
        assert bool(result.get("Item")) is exists


@pytest.mark.parametrize("stage", ["acquire", "release"])
def test_same_process_recovers_its_own_unacknowledged_lock(state_store, monkeypatch, stage):
    from botocore.exceptions import EndpointConnectionError

    original = state_store.client.transact_write_items
    calls = 0

    def lost_ack(**kwargs):
        nonlocal calls
        calls += 1
        if calls == (1 if stage == "acquire" else 2):
            if stage == "acquire":
                original(**kwargs)
            raise EndpointConnectionError(endpoint_url="http://local.test")
        return original(**kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(state_store.client, "transact_write_items", lost_ack)
        with pytest.raises(EndpointConnectionError), state_store.exclusive():
            pass
    assert state_store._read("LOCK#executor") is not None
    with state_store.exclusive() as recovered:
        recovered.assert_owned()
    assert state_store._read("LOCK#executor") is None


def test_scope_prefix_does_not_include_nested_reference(state_store):
    first = state_store.create("owner", "a", plan(), lambda: [])
    second = state_store.create(
        "owner",
        "b",
        plan().model_copy(
            update={
                "workload_scope_ref": "scope#nested",
                "binding_ref": "binding#nested",
            }
        ),
        lambda: [],
    )
    for scope, expected in [("scope", first), ("scope#nested", second)]:
        rows = state_store.transact(lambda tx, scope=scope: state_store.scope_migrations(tx, scope))
        assert [row["id"] for row in rows] == [expected["migrationId"]]
