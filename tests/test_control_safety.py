from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from uuid import uuid4

import pytest
from test_operator_integration import Harness, write

from db_operator.control import actor_ack
from db_operator.models import OperatorError
from db_operator.store import now, stamp

pytestmark = pytest.mark.integration


def ready(h):
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()


def active(h):
    ready(h)
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")


def advance_pass_with_elapsed_timestamps(h):
    """Only elapsed time is simulated; workload and replication checks are real."""
    h.running()
    h.state()

    def elapsed(row):
        row["internal"]["stableSince"] = stamp(now() - timedelta(seconds=31))
        row["internal"]["advanceObservedAt"] = stamp()

    h.service.store.update(h.mid, elapsed)
    state = h.state()
    assert state["readiness"]["advance"]["result"] == "PASS", state["readiness"]
    return state


def post_finalize(h, revision):
    return h.client.post(
        f"/v1/migrations/{h.mid}/actions",
        headers={"Idempotency-Key": uuid4().hex},
        json={"action": "FINALIZE", "expectedRevision": revision},
    )


def run_queued(h, response, expected="SUCCEEDED"):
    assert response.status_code == 202, response.json()
    with h.worker.exclusive():
        assert h.worker.run_once()
    operation = h.client.get(response.headers["Location"]).json()
    assert operation["status"] == expected, operation
    return operation


def finish_active(h):
    state = advance_pass_with_elapsed_timestamps(h)
    run_queued(h, post_finalize(h, state["revision"]))
    assert h.state()["lifecycle"] == "COMPLETED"


def reverse_subscription(h):
    row = h.service.store.get(h.mid)
    source, _, _ = h.service.driver.resources(row["plan"])
    name = h.service.driver.names(h.mid)["reverse"]
    with h.service.driver.connect(source) as conn:
        return conn.execute(
            "SELECT subenabled FROM pg_subscription WHERE subname=%s", (name,)
        ).fetchone()


def test_renew_keeps_gate_within_ttl_and_accepts_quiesce(lab_case):
    settings, _, _ = lab_case
    h = Harness(settings)
    ready(h)
    ref = h.held()
    previous_observed = None
    for _ in range(30):
        h.records.renew(
            ref,
            [actor_ack(p, h.generation, "HELD") for p in ["lab-app", "lab-worker"]],
        )
        gate = h.service.store.transact(lambda tx: tx.get("GATE", ref))
        assert timedelta(0) < gate["valid_until"] - gate["observed_at"] <= timedelta(seconds=30)
        if previous_observed:
            assert gate["observed_at"] >= previous_observed
        previous_observed = gate["observed_at"]
    state = h.state()
    response = h.client.post(
        f"/v1/migrations/{h.mid}/actions",
        headers={"Idempotency-Key": uuid4().hex},
        json={
            "action": "QUIESCE",
            "expectedRevision": state["revision"],
            "transitionId": h.transition,
            "quiesceRef": ref,
        },
    )
    run_queued(h, response)
    assert h.state()["lifecycle"] == "QUIESCED"
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    h.running()
    h.action("FINALIZE")


def test_new_held_generation_invalidates_advance_and_stale_finalize(lab_case):
    settings, _, _ = lab_case
    h = Harness(settings)
    active(h)
    passed = advance_pass_with_elapsed_timestamps(h)
    h.generation += 1
    h.held_since = now()
    h.held()
    # Inspect persisted state without GET: the SDK must invalidate it atomically.
    row = h.service.store.get(h.mid)
    assert row["state"]["revision"] > passed["revision"]
    assert row["state"]["readiness"]["advance"]["result"] != "PASS"
    assert not row["internal"].get("stableSince")
    stale = post_finalize(h, passed["revision"])
    assert stale.status_code == 409, stale.json()
    assert stale.json()["code"] == "REVISION_CONFLICT"
    current = post_finalize(h, row["state"]["revision"])
    assert current.status_code in {409, 422}, current.json()
    assert h.service.store.get(h.mid)["state"]["activeOperationId"] is None
    assert reverse_subscription(h) == {"subenabled": True}
    finish_active(h)


def test_finalize_rechecks_probe_before_external_intent(lab_case):
    settings, _, _ = lab_case
    h = Harness(settings)
    active(h)
    passed = advance_pass_with_elapsed_timestamps(h)
    response = post_finalize(h, passed["revision"])
    assert response.status_code == 202, response.json()
    row = h.service.store.get(h.mid)
    # Simulate a probe failure arriving while FINALIZE is queued. Do not change
    # migration state: execution must independently check the trusted record.
    with h.service.store.transaction() as tx:
        workload = tx.get("WORKLOAD", row["plan"]["workloadScopeRef"])
        workload["probe_result"] = "FAIL"
        tx.put("WORKLOAD", row["plan"]["workloadScopeRef"], workload)
    operation = run_queued(h, response, expected="FAILED")
    record = h.service.store.transact(lambda tx: tx.get("OP", operation["operationId"]))
    assert record["external_intent"] is False and record["confirmed"] == []
    failed = h.service.store.get(h.mid)
    assert failed["state"]["lifecycle"] == "ACTIVE"
    assert failed["state"]["health"] == "FAILED"
    assert not failed["internal"].get("finalizeStarted")
    assert reverse_subscription(h) == {"subenabled": True}
    h.action("RECONCILE")
    finish_active(h)


def test_new_running_generation_starts_a_new_stability_window(lab_case):
    settings, _, _ = lab_case
    h = Harness(settings)
    active(h)
    advance_pass_with_elapsed_timestamps(h)
    h.generation += 1
    h.held_since = now()
    h.held()
    resumed_at = now()
    h.running()
    state = h.state()
    row = h.service.store.get(h.mid)
    assert state["readiness"]["advance"]["result"] != "PASS"
    assert datetime.fromisoformat(row["internal"]["stableSince"]) >= resumed_at
    stability = next(check for check in state["checks"] if check["code"] == "STABILITY_WINDOW")
    assert stability["decision"]["result"] != "PASS"
    finish_active(h)


def test_real_stability_window_survives_heartbeats_and_allows_finalize(lab_case):
    settings, _, probe = lab_case
    h = Harness(settings)
    active(h)
    write(probe)
    started = time.monotonic()
    h.running()
    state = h.state()
    assert state["readiness"]["advance"]["result"] != "PASS"
    first_stable = h.service.store.get(h.mid)["internal"]["stableSince"]
    observations = 1
    while state["readiness"]["advance"]["result"] != "PASS":
        assert time.monotonic() - started < 45, state["readiness"]
        time.sleep(5)
        h.running()
        state = h.state()
        observations += 1
        saved = h.service.store.get(h.mid)["internal"]["stableSince"]
        assert saved == first_stable, "A healthy heartbeat must preserve continuous stability"
        if time.monotonic() - started < 30:
            assert state["readiness"]["advance"]["result"] != "PASS"
    assert time.monotonic() - started >= 30
    assert observations >= 7
    assert (now() - datetime.fromisoformat(first_stable)).total_seconds() >= 30
    run_queued(h, post_finalize(h, state["revision"]))
    assert h.state()["lifecycle"] == "COMPLETED"
    assert reverse_subscription(h) is None


def test_concurrent_held_and_finalize_cannot_both_be_accepted(lab_case):
    settings, _, _ = lab_case
    h = Harness(settings)
    active(h)
    passed = advance_pass_with_elapsed_timestamps(h)
    h.generation += 1
    h.held_since = now()
    start = threading.Barrier(2)

    def hold():
        start.wait(timeout=5)
        try:
            return h.held(), None
        except OperatorError as error:
            return None, error.code

    def finalize():
        start.wait(timeout=5)
        return post_finalize(h, passed["revision"])

    with ThreadPoolExecutor(max_workers=2) as pool:
        held_future = pool.submit(hold)
        final_future = pool.submit(finalize)
        ref, error = held_future.result(timeout=15)
        response = final_future.result(timeout=15)
    assert bool(ref) != (response.status_code == 202)
    if response.status_code == 202:
        assert error == "RESOURCE_BUSY"
        run_queued(h, response)
        assert h.state()["lifecycle"] == "COMPLETED"
    else:
        assert ref and error is None
        assert response.status_code == 409, response.json()
        assert response.json()["code"] == "REVISION_CONFLICT"
        assert h.service.store.get(h.mid)["state"]["activeOperationId"] is None
        finish_active(h)


@pytest.mark.parametrize("invalid_record", ["missing", "expired", "wrong_state"])
def test_invalid_participant_heartbeat_breaks_stability_until_recovered(lab_case, invalid_record):
    settings, _, _ = lab_case
    h = Harness(settings)
    active(h)
    passed = advance_pass_with_elapsed_timestamps(h)
    participants = [actor_ack(p, h.generation, "RUNNING") for p in ["lab-app", "lab-worker"]]
    if invalid_record == "missing":
        participants.pop()
    elif invalid_record == "expired":
        participants[0]["observedAt"] = stamp(now() - timedelta(seconds=31))
    else:
        participants[0]["state"] = "HELD"
    h.records.running(
        h.mid,
        h.generation,
        participants,
        "PASS",
        "evidence:actual-lab-business-transaction",
    )
    # Do not GET between the invalid heartbeat and its recovery: the control
    # record writer must retain evidence that continuous stability was broken.
    invalidated = h.service.store.get(h.mid)
    assert invalidated["state"]["revision"] > passed["revision"]
    assert invalidated["state"]["readiness"]["advance"]["result"] != "PASS"
    assert not invalidated["internal"].get("stableSince")
    recovered_at = now()
    h.running()
    state = h.state()
    assert state["readiness"]["advance"]["result"] != "PASS"
    stable = h.service.store.get(h.mid)["internal"]["stableSince"]
    assert datetime.fromisoformat(stable) >= recovered_at
    finish_active(h)


def test_running_inflight_changes_preserve_healthy_stability(lab_case):
    settings, _, _ = lab_case
    h = Harness(settings)
    active(h)
    passed = advance_pass_with_elapsed_timestamps(h)
    before = h.service.store.get(h.mid)
    h.records.running(
        h.mid,
        h.generation,
        [
            actor_ack("lab-app", h.generation, "RUNNING", 3),
            actor_ack("lab-worker", h.generation, "RUNNING", 1),
        ],
        "PASS",
        "evidence:actual-lab-business-transaction",
    )
    after = h.service.store.get(h.mid)
    assert after["state"]["revision"] == passed["revision"]
    assert after["internal"]["stableSince"] == before["internal"]["stableSince"]
    assert after["internal"].get("controlEpoch") == before["internal"].get("controlEpoch")
    assert h.state()["readiness"]["advance"]["result"] == "PASS"
    finish_active(h)
