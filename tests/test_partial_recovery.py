from __future__ import annotations

import time
from contextlib import contextmanager
from datetime import timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from test_operator_integration import Harness, write

from db_operator.store import now

pytestmark = pytest.mark.integration


def ready(h, mode="REVERSE_REQUIRED"):
    h.create(mode)
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()


def restart(h):
    restarted = Harness(h.settings)
    restarted.mid, restarted.transition = h.mid, h.transition
    restarted.generation, restarted.held_since = h.generation, h.held_since
    return restarted


def reconcile_repeated(h):
    # A read-only reconciliation must retain the original interrupted-operation proof.
    for _ in range(3):
        h.action("RECONCILE")
        assert h.state()["health"] == "OK"


def deny_proxy_resume(driver, patch):
    actual = driver.admin

    def interrupted(binding, command, *args, **kwargs):
        if command == "RESUME {}":
            # ALTER ROLE LOGIN has committed, but PgBouncer is still paused.
            raise psycopg.OperationalError("injected interruption before proxy resume")
        return actual(binding, command, *args, **kwargs)

    patch.setattr(driver, "admin", interrupted)


def source_resume_interrupted(h, monkeypatch):
    ready(h)
    h.action("QUIESCE")
    h.action("ROLLBACK")
    with monkeypatch.context() as patch:
        deny_proxy_resume(h.service.driver, patch)
        h.action("RESUME", location="SOURCE", expected="UNKNOWN")
    assert_paused(h, "SOURCE")
    assert h.service.store.get(h.mid)["internal"]["sourceResumeIntent"]


def assert_paused(h, location):
    observed = h.service.inspect_resources(h.service.store.get(h.mid))
    assert observed["route"] == location
    assert observed["access"] == location + "_ONLY"
    assert observed["paused"]


def assert_write_at(h, probe, location):
    request_id, sequence_id = write(probe)
    assert sequence_id > 10
    row = h.service.store.get(h.mid)
    source, target, _ = h.service.driver.resources(row["plan"])
    db = source if location == "SOURCE" else target
    with h.service.driver.connect(db) as conn:
        assert conn.execute("SELECT 1 FROM orders WHERE request_id=%s", (request_id,)).fetchone()


def direct_writer_dsn(h, probe, location):
    source, target, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    db = source if location == "SOURCE" else target
    app_info = conninfo_to_dict(probe.get_secret_value())
    return make_conninfo(db.secret("admin"), user=db.writer_role, password=app_info["password"])


def finish_source(h):
    h.running()
    h.action("FINALIZE")
    assert h.state()["lifecycle"] == "COMPLETED"


def finish_target(h):
    deadline = time.monotonic() + 45
    while True:
        h.running()
        if h.state()["readiness"]["advance"]["result"] == "PASS":
            break
        assert time.monotonic() < deadline
        time.sleep(2)
    h.action("FINALIZE")
    assert h.state()["lifecycle"] == "COMPLETED"


def resume_body(h, location, ref=None):
    # Control evidence changes the effective state. Fetch its revision afterwards.
    ref = ref or h.held()
    state = h.state()
    return {
        "action": "RESUME",
        "expectedRevision": state["revision"],
        "transitionId": h.transition,
        "quiesceRef": ref,
        "expectedLocation": location,
    }


def test_interrupted_initial_pause_can_quiesce_then_restore(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    ready(h)

    def interrupted(*args, **kwargs):
        raise psycopg.OperationalError("injected interruption before writer fence")

    with monkeypatch.context() as patch:
        patch.setattr(h.service.driver, "fence", interrupted)
        h.action("QUIESCE", expected="UNKNOWN")
    assert_paused(h, "SOURCE")

    h = restart(h)
    reconcile_repeated(h)
    assert "QUIESCE" in h.state()["allowedActions"]
    h.action("QUIESCE")
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    assert_write_at(h, probe, "SOURCE")
    assert not h.state()["observed"]["targetWritesPossible"]
    finish_source(h)


def test_expired_initial_pause_can_only_recover_to_source(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    ready(h)

    def interrupted(*args, **kwargs):
        raise psycopg.OperationalError("injected interruption before writer fence")

    with monkeypatch.context() as patch:
        patch.setattr(h.service.driver, "fence", interrupted)
        h.action("QUIESCE", expected="UNKNOWN")
    assert_paused(h, "SOURCE")
    h.held_since = now() - timedelta(seconds=301)
    h = restart(h)
    reconcile_repeated(h)
    # The recovery-only attempt can itself stop before NOLOGIN commits.
    with monkeypatch.context() as patch:
        patch.setattr(h.service.driver, "fence", interrupted)
        h.action("QUIESCE", expected="UNKNOWN")
    assert h.service.store.get(h.mid)["internal"]["directRecovery"]
    reconcile_repeated(h)
    assert "QUIESCE" in h.state()["allowedActions"]
    h.action("QUIESCE")
    row = h.service.store.get(h.mid)
    assert row["internal"]["directRecovery"]
    assert not row["internal"].get("targetWritesIntent")
    assert "CUTOVER" not in h.state()["allowedActions"]
    assert h.state()["readiness"]["cutover"]["result"] != "PASS"
    ref = h.held()
    response = h.client.post(
        f"/v1/migrations/{h.mid}/actions",
        headers={"Idempotency-Key": uuid4().hex},
        json={
            "action": "CUTOVER",
            "expectedRevision": h.state()["revision"],
            "transitionId": h.transition,
            "quiesceRef": ref,
        },
    )
    assert response.status_code in {409, 422}
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    assert_write_at(h, probe, "SOURCE")
    finish_source(h)


def test_partial_initial_pause_recovers_when_unused_target_is_unreachable(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    ready(h)

    def interrupted(*args, **kwargs):
        raise psycopg.OperationalError("injected interruption before writer fence")

    with monkeypatch.context() as patch:
        patch.setattr(h.service.driver, "fence", interrupted)
        h.action("QUIESCE", expected="UNKNOWN")
    assert_paused(h, "SOURCE")
    h = restart(h)
    driver = h.service.driver
    _, target, _ = driver.resources(h.service.store.get(h.mid)["plan"])
    actual = driver.connect

    @contextmanager
    def target_unreachable(db, *args, **kwargs):
        if db.identity == target.identity:
            raise psycopg.OperationalError("injected unavailable unused target")
        with actual(db, *args, **kwargs) as conn:
            yield conn

    # Only target connections fail. Source SQL, fencing and PgBouncer commands are real.
    with monkeypatch.context() as patch:
        patch.setattr(driver, "connect", target_unreachable)
        reconcile_repeated(h)
        assert not h.service.store.get(h.mid)["internal"]["observedDetails"]["targetReachable"]
        h.action("QUIESCE")
        row = h.service.store.get(h.mid)
        assert row["internal"]["directRecovery"]
        assert not row["internal"].get("targetWritesIntent")
        assert "CUTOVER" not in h.state()["allowedActions"]
        h.action("ROLLBACK")
        h.action("RESUME", location="SOURCE")
        assert_write_at(h, probe, "SOURCE")
    # Restore target connectivity before deleting its remaining subscriber metadata.
    finish_source(h)


def test_interrupted_source_resume_can_continue_after_restart(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    source_resume_interrupted(h, monkeypatch)

    h = restart(h)
    reconcile_repeated(h)
    assert "RESUME" in h.state()["allowedActions"]
    h.action("RESUME", location="SOURCE")
    assert h.state()["lifecycle"] == "RESTORED"
    assert h.state()["transitionState"] == "CLOSED"
    assert_write_at(h, probe, "SOURCE")
    finish_source(h)


@pytest.mark.parametrize("interruption", ["before_login", "before_proxy_resume"])
def test_prewrite_target_resume_can_continue_without_enabling_rollback(
    lab_case, monkeypatch, interruption
):
    settings, _, probe = lab_case
    h = Harness(settings)
    ready(h, "BEFORE_TARGET_WRITE")
    h.action("QUIESCE")
    h.action("CUTOVER")
    with monkeypatch.context() as patch:
        if interruption == "before_login":
            driver = h.service.driver
            actual = driver.connect
            _, target, _ = driver.resources(h.service.store.get(h.mid)["plan"])

            @contextmanager
            def connection_lost(db, tag="observe", *args, **kwargs):
                if db.identity == target.identity and tag.startswith("op-"):
                    raise psycopg.OperationalError("injected failure before ALTER ROLE LOGIN")
                with actual(db, tag, *args, **kwargs) as conn:
                    yield conn

            patch.setattr(driver, "connect", connection_lost)
        else:
            deny_proxy_resume(h.service.driver, patch)
        h.action("RESUME", location="TARGET", expected="UNKNOWN")
    if interruption == "before_login":
        observed = h.service.inspect_resources(h.service.store.get(h.mid))
        assert observed["paused"] and observed["route"] == "TARGET"
        assert observed["access"] == "BLOCKED"
    else:
        assert_paused(h, "TARGET")

    h = restart(h)
    reconcile_repeated(h)
    state = h.state()
    assert state["observed"]["targetWritesPossible"]
    assert h.service.store.get(h.mid)["internal"]["targetWritesIntent"]
    assert not h.service.store.get(h.mid)["internal"].get("reversePrepared")
    assert "RESUME" in state["allowedActions"]
    assert "ROLLBACK" not in state["allowedActions"]
    assert state["readiness"]["rollback"]["result"] != "PASS"

    ref = h.held()
    state = h.state()
    rollback = h.client.post(
        f"/v1/migrations/{h.mid}/actions",
        headers={"Idempotency-Key": uuid4().hex},
        json={
            "action": "ROLLBACK",
            "expectedRevision": state["revision"],
            "transitionId": h.transition,
            "quiesceRef": ref,
        },
    )
    assert rollback.status_code in {409, 422}
    assert h.service.inspect_resources(h.service.store.get(h.mid))["paused"]
    h.action("RESUME", location="TARGET")
    assert h.state()["lifecycle"] == "ACTIVE"
    assert h.state()["observed"]["targetWritesPossible"]
    assert_write_at(h, probe, "TARGET")
    finish_target(h)


@pytest.mark.parametrize("evidence", ["forged", "expired"])
def test_partial_source_resume_requires_current_control_evidence(lab_case, monkeypatch, evidence):
    settings, _, probe = lab_case
    h = Harness(settings)
    source_resume_interrupted(h, monkeypatch)
    h = restart(h)
    h.action("RECONCILE")
    ref = h.held()
    if evidence == "forged":
        ref = "quiesce:does-not-exist"
    else:
        with h.service.store.transaction() as tx:
            gate = tx.get("GATE", ref)
            gate["valid_until"] = now() - timedelta(seconds=1)
            tx.put("GATE", ref, gate)
    response = h.client.post(
        f"/v1/migrations/{h.mid}/actions",
        headers={"Idempotency-Key": uuid4().hex},
        json=resume_body(h, "SOURCE", ref),
    )
    assert response.status_code in {409, 422}
    assert h.service.store.get(h.mid)["state"]["activeOperationId"] is None
    assert_paused(h, "SOURCE")
    h.action("RESUME", location="SOURCE")
    assert_write_at(h, probe, "SOURCE")
    finish_source(h)


def test_partial_source_resume_rechecks_live_writer_sessions(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    source_resume_interrupted(h, monkeypatch)
    h = restart(h)
    h.action("RECONCILE")
    body = resume_body(h, "SOURCE")
    source, _, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    direct_writer = direct_writer_dsn(h, probe, "SOURCE")
    with psycopg.connect(direct_writer, autocommit=True, connect_timeout=3):
        assert h.service.driver.inspect(source)["sessions"] >= 1
        response = h.client.post(
            f"/v1/migrations/{h.mid}/actions",
            headers={"Idempotency-Key": uuid4().hex},
            json=body,
        )
        if response.status_code == 202:
            with h.worker.exclusive():
                assert h.worker.run_once()
            operation = h.client.get(response.headers["Location"]).json()
            assert operation["status"] in {"FAILED", "UNKNOWN"}
        else:
            assert response.status_code in {409, 422}
        assert_paused(h, "SOURCE")

    if h.state()["health"] != "OK":
        h.action("RECONCILE")
    h.action("RESUME", location="SOURCE")
    assert_write_at(h, probe, "SOURCE")
    finish_source(h)


def test_failed_reconcile_keeps_original_resume_proof(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    source_resume_interrupted(h, monkeypatch)
    h = restart(h)
    with psycopg.connect(direct_writer_dsn(h, probe, "SOURCE"), autocommit=True, connect_timeout=3):
        h.action("RECONCILE", expected="UNKNOWN")
        assert_paused(h, "SOURCE")
    # The failed RECONCILE must not replace the original RESUME proof.
    reconcile_repeated(h)
    h.action("RESUME", location="SOURCE")
    assert_write_at(h, probe, "SOURCE")
    finish_source(h)


def test_finalize_can_retry_after_forward_cleanup_commits(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    ready(h)
    h.action("QUIESCE")
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    h.running()
    driver = h.service.driver
    names = driver.names(h.mid)
    actual = driver.cleanup_replication
    cleaned = []

    def interrupted(publisher, subscriber, name, tag, intent):
        actual(publisher, subscriber, name, tag, intent)
        if name == names["forward"]:
            cleaned.append(name)
            raise psycopg.OperationalError("injected reply loss after committed forward cleanup")

    with monkeypatch.context() as patch:
        patch.setattr(driver, "cleanup_replication", interrupted)
        _, _, accepted = h.action("FINALIZE", expected="UNKNOWN")
    assert cleaned == [names["forward"]]
    row = h.service.store.get(h.mid)
    assert row["internal"]["finalizeStarted"]
    assert row["internal"]["finalizeLocation"] == "SOURCE"
    assert h.service.store.transact(lambda tx: tx.get("OP", accepted["operationId"]))[
        "external_intent"
    ]
    _, target, _ = driver.resources(row["plan"])
    with driver.connect(target) as conn:
        assert not conn.execute(
            "SELECT 1 FROM pg_subscription WHERE subname=%s", (names["forward"],)
        ).fetchone()

    h = restart(h)
    reconcile_repeated(h)
    assert set(h.state()["allowedActions"]) == {"FINALIZE", "RECONCILE"}
    h.action("FINALIZE")
    assert h.state()["lifecycle"] == "COMPLETED"
    row = h.service.store.get(h.mid)
    assert row["internal"]["finalizeForwardDone"]
    assert row["internal"]["finalizeReverseDone"]
    assert h.service.store.transact(lambda tx: tx.list("MIG_CLAIM", h.mid + "#")) == []
    assert_write_at(h, probe, "SOURCE")


def test_interrupted_reverse_pause_can_continue_then_restore(lab_case, monkeypatch):
    settings, _, probe = lab_case
    h = Harness(settings)
    ready(h)
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")
    request_id, _ = write(probe)
    h.running()
    h.generation += 1
    h.held_since = now()

    def interrupted(*args, **kwargs):
        raise psycopg.OperationalError("injected interruption before target writer fence")

    with monkeypatch.context() as patch:
        patch.setattr(h.service.driver, "fence", interrupted)
        h.action("QUIESCE", expected="UNKNOWN")
    assert_paused(h, "TARGET")
    h = restart(h)
    reconcile_repeated(h)
    assert "QUIESCE" in h.state()["allowedActions"]
    assert "RESUME" not in h.state()["allowedActions"]
    h.action("QUIESCE")
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    source, _, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    with h.service.driver.connect(source) as conn:
        assert conn.execute("SELECT 1 FROM orders WHERE request_id=%s", (request_id,)).fetchone()
    assert_write_at(h, probe, "SOURCE")
    finish_source(h)
