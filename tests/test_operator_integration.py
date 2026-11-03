from __future__ import annotations

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from importlib.resources import files
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker

from db_operator.api import create_app
from db_operator.control import ControlPlaneRecords, actor_ack
from db_operator.models import OperatorError
from db_operator.service import Service
from db_operator.store import now
from db_operator.worker import Worker

pytestmark = pytest.mark.integration


def validate(name, value):
    spec = json.loads(files("db_operator").joinpath("contract.json").read_text())
    Draft202012Validator(
        {"$ref": "#/components/schemas/" + name, "components": spec["components"]},
        format_checker=FormatChecker(),
    ).validate(value)


class Harness:
    def __init__(self, settings):
        self.settings = settings
        self.client = TestClient(create_app(settings))
        self.client.headers["Authorization"] = "Bearer " + settings.api_token.get_secret_value()
        self.service = self.client.app.state.service
        self.worker = Worker(self.service)
        self.records = ControlPlaneRecords(self.service.store)
        self.mid, self.transition, self.generation = None, "tx-" + uuid4().hex, 1
        self.held_since = now()

    def create(self, mode="REVERSE_REQUIRED"):
        body = {
            "workflowId": "wf-" + uuid4().hex,
            "resourceId": "db-main",
            "kind": "DATABASE",
            "profile": "postgres-logical-pgbouncer-v1",
            "sourceRef": "resource:source",
            "targetRef": "resource:target",
            "bindingRef": "binding:appdb",
            "workloadScopeRef": "workload:lab",
            "policy": {"rollbackMode": mode, "maxQuiesceSeconds": 300},
        }
        response = self.client.post(
            "/v1/migrations", headers={"Idempotency-Key": "create"}, json=body
        )
        assert response.status_code == 201, response.json()
        validate("Migration", response.json())
        self.mid = response.json()["migrationId"]
        return body, response

    def state(self):
        response = self.client.get("/v1/migrations/" + self.mid)
        assert response.status_code == 200, response.json()
        validate("Migration", response.json())
        return response.json()

    def held(self):
        # No background work exists in this fixture; every business transaction is closed.
        return self.records.held(
            migration_id=self.mid,
            transition_id=self.transition,
            generation=self.generation,
            held_since=self.held_since,
            participants=[actor_ack(p, self.generation, "HELD") for p in ["lab-app", "lab-worker"]],
        )

    def running(self):
        self.records.running(
            self.mid,
            self.generation,
            [actor_ack(p, self.generation, "RUNNING") for p in ["lab-app", "lab-worker"]],
            "PASS",
            "evidence:actual-lab-business-transaction",
        )

    def action(self, name, *, location=None, expected="SUCCEEDED", key=None):
        state = self.state()
        body = {"action": name, "expectedRevision": state["revision"]}
        if name in {"QUIESCE", "CUTOVER", "RESUME", "ROLLBACK"}:
            body.update(transitionId=self.transition, quiesceRef=self.held())
        if location:
            body["expectedLocation"] = location
        key = key or uuid4().hex
        response = self.client.post(
            "/v1/migrations/" + self.mid + "/actions", headers={"Idempotency-Key": key}, json=body
        )
        assert response.status_code == 202, response.json()
        validate("OperationAccepted", response.json())
        with self.worker.exclusive():
            assert self.worker.run_once()
        operation = self.client.get(response.headers["Location"]).json()
        validate("Operation", operation)
        assert operation["status"] == expected, operation
        return key, body, response.json()

    def ready(self):
        deadline = time.monotonic() + 25
        while self.state()["lifecycle"] != "READY":
            assert time.monotonic() < deadline
            time.sleep(0.1)


def write(probe):
    request_id = uuid4()
    with psycopg.connect(probe.get_secret_value()) as conn:
        result = conn.execute(
            "INSERT INTO orders(request_id,sku) VALUES (%s,'chiikawa') RETURNING id", (request_id,)
        ).fetchone()[0]
        conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku='chiikawa'")
    return request_id, result


def test_full_migration_reverse_rollback_and_lost_resume_response(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    body, created = h.create()
    # Duplicate creation returns the original representation, even after state changes.
    h.action("PREPARE")
    again = h.client.post("/v1/migrations", headers={"Idempotency-Key": "create"}, json=body)
    assert again.json() == created.json()
    h.action("START_SYNC")
    h.ready()
    before = [write(probe) for _ in range(5)]
    h.action("QUIESCE")
    assert h.state()["observed"]["businessWriteAccess"] == "BLOCKED"
    h.action("CUTOVER")
    assert h.state()["lifecycle"] == "SWITCHED"
    real_resume = h.service.driver.resume

    def lost_reply(*args, **kwargs):
        real_resume(*args, **kwargs)
        raise TimeoutError("synthetic response loss after actual proxy resume")

    monkeypatch.setattr(h.service.driver, "resume", lost_reply)
    key, request, accepted = h.action("RESUME", location="TARGET", expected="UNKNOWN")
    uncertain = h.state()
    assert uncertain["health"] == "UNKNOWN"
    assert uncertain["observed"]["targetWritesPossible"] is True
    duplicate = h.client.post(
        "/v1/migrations/" + h.mid + "/actions", headers={"Idempotency-Key": key}, json=request
    )
    assert duplicate.json()["operationId"] == accepted["operationId"]
    blocked = h.client.post(
        "/v1/migrations/" + h.mid + "/actions",
        headers={"Idempotency-Key": "forbidden"},
        json={
            "action": "ROLLBACK",
            "expectedRevision": uncertain["revision"],
            "transitionId": h.transition,
            "quiesceRef": "untrusted",
        },
    )
    assert blocked.status_code == 409
    # New API and Worker objects use only durable records and actual external state.
    restarted = Harness(settings)
    restarted.mid, restarted.transition = h.mid, h.transition
    restarted.generation, restarted.held_since = h.generation, h.held_since
    h = restarted
    h.action("RECONCILE")
    assert h.state()["lifecycle"] == "ACTIVE"
    after = [write(probe) for _ in range(5)]
    h.running()
    assert h.state()["readiness"]["advance"]["result"] == "FAIL"
    deadline = time.monotonic() + 40
    while True:
        h.running()
        state = h.state()
        if state["readiness"]["advance"]["result"] == "PASS":
            break
        assert time.monotonic() < deadline
        time.sleep(3)
    h.generation += 1
    h.held_since = now()
    h.action("QUIESCE")
    assert h.state()["readiness"]["rollback"]["result"] == "PASS"
    h.action("ROLLBACK")
    assert h.state()["lifecycle"] == "ROLLED_BACK_HELD"
    h.action("RESUME", location="SOURCE")
    assert h.state()["transitionState"] == "CLOSED"
    h.running()
    _, next_id = write(probe)
    assert next_id > max(i for _, i in before + after)
    source, target, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    with h.service.driver.connect(source) as conn:
        assert (
            conn.execute(
                "SELECT count(*) AS n FROM orders WHERE request_id=ANY(%s)",
                ([r for r, _ in before + after],),
            ).fetchone()["n"]
            == 10
        )
        assert conn.execute("SELECT stock AS n FROM inventory").fetchone()["n"] == 979
    assert h.service.driver.inspect(target)["writer"]["rolcanlogin"] is False
    h.action("FINALIZE")
    assert h.state()["lifecycle"] == "COMPLETED"
    h.action("RECONCILE")
    assert h.state()["lifecycle"] == "COMPLETED"
    assert h.state()["allowedActions"] == ["RECONCILE"]
    # Original UNKNOWN outcome remains in history; reconciliation is a separate operation.
    assert h.service.store.operation(accepted["operationId"])["status"] == "UNKNOWN"


def test_direct_rollback_and_reject_invalid_gate(lab_case, monkeypatch):
    settings, _, _ = lab_case
    h = Harness(settings)
    h.create("BEFORE_TARGET_WRITE")
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    state = h.state()
    bad = {
        "action": "QUIESCE",
        "expectedRevision": state["revision"],
        "transitionId": h.transition,
        "quiesceRef": "invented",
    }
    result = h.client.post(
        "/v1/migrations/" + h.mid + "/actions", headers={"Idempotency-Key": "bad"}, json=bad
    )
    assert result.status_code == 422
    assert h.service.driver.observe(h.service.store.get(h.mid)["plan"])["access"] == "SOURCE_ONLY"
    ref = h.held()
    second = h.records.held(
        migration_id=h.mid,
        transition_id=h.transition,
        generation=h.generation,
        held_since=now(),
        participants=[actor_ack(p, h.generation, "HELD") for p in ["lab-app", "lab-worker"]],
    )
    with h.service.store.connect() as conn:
        times = conn.execute(
            "SELECT held_since FROM foxtrot_operator.quiesce_gates WHERE ref=ANY(%s)",
            ([ref, second],),
        ).fetchall()
        assert all(r["held_since"] == h.held_since for r in times)
        conn.execute(
            "UPDATE foxtrot_operator.quiesce_gates SET valid_until=%s WHERE ref=%s",
            (now() - timedelta(seconds=1), ref),
        )
    bad["quiesceRef"] = ref
    result = h.client.post(
        "/v1/migrations/" + h.mid + "/actions", headers={"Idempotency-Key": "expired"}, json=bad
    )
    assert result.status_code == 422
    h.action("QUIESCE")
    original_admin = h.service.driver.admin
    rejected = []

    def explicit_rejection(binding, command, *args, **kwargs):
        if command == "RELOAD" and not rejected:
            rejected.append(True)
            raise psycopg.errors.ProtocolViolation("RELOAD failed, see logs for additional details")
        return original_admin(binding, command, *args, **kwargs)

    monkeypatch.setattr(h.service.driver, "admin", explicit_rejection)
    h.action("CUTOVER")
    assert rejected
    assert h.state()["observed"]["targetWritesPossible"] is False
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    h.running()
    h.action("FINALIZE")


def test_no_automatic_fallback_after_target_write_without_reverse(lab_case):
    settings, _, probe = lab_case
    h = Harness(settings)
    h.create("BEFORE_TARGET_WRITE")
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")
    write(probe)
    state = h.state()
    assert state["observed"]["targetWritesPossible"] is True
    assert state["rollback"]["method"] == "UNAVAILABLE"
    assert "ROLLBACK" not in state["allowedActions"]
    refused = h.client.post(
        "/v1/migrations/" + h.mid + "/actions",
        headers={"Idempotency-Key": "forbidden-fallback"},
        json={
            "action": "ROLLBACK",
            "expectedRevision": state["revision"],
            "transitionId": h.transition,
            "quiesceRef": "invented",
        },
    )
    assert refused.status_code == 409
    observed = h.service.driver.observe(h.service.store.get(h.mid)["plan"])
    assert observed["route"] == "TARGET" and observed["access"] == "TARGET_ONLY"
    # Fixture housekeeping: preserve data, release only this test's unused forward slot.
    source, target, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    with h.worker.exclusive():
        h.service.driver.cleanup_replication(
            source,
            target,
            h.service.driver.names(h.mid)["forward"],
            "lab-fixture-cleanup",
            lambda _: h.worker.assert_lease(),
        )


def test_old_idle_executor_and_lost_lease_block_changes(lab_case):
    settings, _, _ = lab_case
    h = Harness(settings)
    h.create()
    with h.worker.exclusive() as guard:
        guard.execute("SELECT pg_advisory_unlock(7126432,1)")
        with pytest.raises(OperatorError, match="실행기 잠금"):
            h.worker.run_once()
    source, _, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    with psycopg.connect(
        source.secret("admin"), autocommit=True, application_name="foxtrot-db:op-old-idle-executor"
    ) as old:
        old.execute("SELECT 1")
        _, _, prior = h.action("RECONCILE", expected="UNKNOWN")
    state = h.state()
    response = h.client.post(
        "/v1/migrations/" + h.mid + "/actions",
        headers={"Idempotency-Key": "new-reconcile"},
        json={"action": "RECONCILE", "expectedRevision": state["revision"]},
    )
    assert response.status_code == 202
    oid = response.json()["operationId"]
    h.service.store.finish({"id": prior["operationId"], "migration_id": h.mid}, "FAILED")
    assert h.service.store.get(h.mid)["state"]["activeOperationId"] == oid
    with h.worker.exclusive():
        assert h.worker.run_once()
    assert h.service.store.operation(oid)["status"] == "SUCCEEDED"
    assert h.state()["health"] == "OK"
    h.action("CANCEL")
    h.action("FINALIZE")


def test_expired_forward_attempt_can_restore_authoritative_source(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()

    def timeout(*args, **kwargs):
        raise OperatorError("TEST_DEADLINE", "최종 동기화 시간 초과 시험")

    monkeypatch.setattr(h.service.driver, "barrier", timeout)
    h.action("QUIESCE", expected="UNKNOWN")
    source, target, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    with h.service.driver.connect(target) as conn:
        conn.execute("DELETE FROM orders")  # Deliberately inconsistent unused target.
    h.held_since = now() - timedelta(seconds=301)
    h.action("RECONCILE")
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    assert h.state()["lifecycle"] == "RESTORED"
    with h.service.driver.connect(source) as conn:
        assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 10
    write(probe)
    h.running()
    h.action("FINALIZE")


def test_interrupted_reverse_validation_requires_fresh_recovery(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")
    request_id, _ = write(probe)
    h.running()
    h.generation += 1
    h.held_since = now()
    actual = h.service.driver.sync_sequences

    def timeout(*args, **kwargs):
        raise OperatorError("TEST_DEADLINE", "역방향 시퀀스 검증 중단 시험")

    monkeypatch.setattr(h.service.driver, "sync_sequences", timeout)
    h.action("QUIESCE", expected="UNKNOWN")
    monkeypatch.setattr(h.service.driver, "sync_sequences", actual)
    h.held_since = now() - timedelta(seconds=301)
    h.action("RECONCILE")
    assert h.state()["readiness"]["rollback"]["result"] != "PASS"
    h.action("QUIESCE")
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    source, _, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    with h.service.driver.connect(source) as conn:
        assert conn.execute("SELECT 1 FROM orders WHERE request_id=%s", (request_id,)).fetchone()
    h.running()
    h.action("FINALIZE")


def test_concurrent_admission_auth_and_worker_restart(lab_case):
    settings, service, _ = lab_case
    h = Harness(settings)
    _, _ = h.create()
    assert TestClient(create_app(settings)).get("/v1/capabilities").status_code == 401
    current = h.state()
    command = {"action": "PREPARE", "expectedRevision": current["revision"]}
    barrier = threading.Barrier(2)

    def send():
        barrier.wait()
        return h.client.post(
            "/v1/migrations/" + h.mid + "/actions",
            headers={"Idempotency-Key": "same"},
            json=command,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: send(), range(2)))
    assert [r.status_code for r in results] == [202, 202]
    assert results[0].json()["operationId"] == results[1].json()["operationId"]
    with service.store.connect() as conn:
        assert (
            conn.execute("SELECT count(*) AS n FROM foxtrot_operator.operations").fetchone()["n"]
            == 1
        )
    operation = service.store.claim_operation()
    service.store.intent(operation["id"], "synthetic.interruption_before_remote_effect")
    # Restarting an executor does not enqueue/replay the interrupted command.
    worker = Worker(Service(settings))
    with worker.exclusive():
        with pytest.raises(OperatorError), Worker(Service(settings)).exclusive():
            pass
        worker.store.recover_interrupted()
        assert worker.store.operation(operation["id"])["status"] == "UNKNOWN"
        assert worker.run_once() is False
    assert h.state()["health"] == "UNKNOWN"
    changed = {**command, "action": "CANCEL"}
    conflict = h.client.post(
        "/v1/migrations/" + h.mid + "/actions", headers={"Idempotency-Key": "same"}, json=changed
    )
    assert conflict.status_code == 409
    h.action("RECONCILE")
    h.action("CANCEL")
    h.action("FINALIZE")
