"""Redis와 같은 DB 명령 의미와 각 중단 지점의 복구를 실제 랩에서 검사합니다."""

from datetime import timedelta
from uuid import uuid4

import psycopg
import pytest
from test_control_safety import finish_active
from test_managed_logical import ManagedHarness, swap_direction
from test_managed_logical import managed_case as managed_case
from test_operator_integration import write

from db_operator.logical import ManagedLogicalPostgres
from db_operator.models import ATOMIC_LOGICAL_PROFILE, CreateMigration
from db_operator.store import now


class AtomicHarness(ManagedHarness):
    profile = ATOMIC_LOGICAL_PROFILE

    def action(self, name, *, location=None, expected="SUCCEEDED", key=None):
        if name not in {"QUIESCE", "RESUME"}:
            return super().action(name, location=location, expected=expected, key=key)
        body = {
            "action": name,
            "expectedRevision": self.state()["revision"],
            "transitionId": self.transition,
            "quiesceRef": "check-only:" + self.transition,
        }
        if location:
            body["expectedLocation"] = location
        key = key or uuid4().hex
        response = self.client.post(
            "/v1/migrations/" + self.mid + "/actions",
            headers={"Idempotency-Key": key},
            json=body,
        )
        assert response.status_code == 202, response.json()
        with self.worker.exclusive():
            assert self.worker.run_once()
        operation = self.client.get(response.headers["Location"]).json()
        assert operation["status"] == expected, operation
        return key, body, response.json()


def ready(settings):
    h = AtomicHarness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    return h


def test_atomic_profile_rejects_reverse_required():
    with pytest.raises(ValueError, match="reverse replication"):
        CreateMigration.model_validate(
            {
                "workflowId": "w",
                "resourceId": "db",
                "kind": "DATABASE",
                "profile": ATOMIC_LOGICAL_PROFILE,
                "sourceRef": "s",
                "targetRef": "t",
                "bindingRef": "b",
                "workloadScopeRef": "scope",
                "policy": {"rollbackMode": "REVERSE_REQUIRED", "maxQuiesceSeconds": 30},
            }
        )


@pytest.mark.integration
def test_quiesce_checks_cutover_resumes_and_resume_only_confirms(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")
    checked = h.state()
    assert checked["lifecycle"] == "QUIESCED"
    assert checked["observed"]["businessWriteAccess"] == "SOURCE_ONLY"
    _, source_id = write(probe)
    h.action("CUTOVER")
    switched = h.state()
    assert switched["lifecycle"] == "SWITCHED"
    assert switched["observed"]["route"] == "TARGET"
    assert switched["observed"]["businessWriteAccess"] == "TARGET_ONLY"
    _, target_id = write(probe)
    assert target_id > source_id
    assert "ROLLBACK" not in switched["allowedActions"]
    monkeypatch.setattr(
        ManagedLogicalPostgres, "resume", lambda *_: pytest.fail("writes already open")
    )
    h.action("RESUME", location="TARGET")
    assert h.state()["lifecycle"] == "ACTIVE"


@pytest.mark.integration
def test_cutover_rejects_a_check_only_reference(managed_case):
    settings, _ = managed_case
    h = ready(settings)
    h.action("QUIESCE")
    response = h.client.post(
        f"/v1/migrations/{h.mid}/actions",
        headers={"Idempotency-Key": uuid4().hex},
        json={
            "action": "CUTOVER",
            "expectedRevision": h.state()["revision"],
            "transitionId": h.transition,
            "quiesceRef": "check-only:" + h.transition,
        },
    )
    assert response.status_code == 422
    assert response.json()["code"] == "QUIESCE_EVIDENCE_INVALID"
    assert h.state()["observed"]["businessWriteAccess"] == "SOURCE_ONLY"


@pytest.mark.integration
def test_lost_cutover_reply_is_confirmed_without_reopening_or_rollback(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")
    actual = ManagedLogicalPostgres.resume

    def lost(driver, *args, **kwargs):
        actual(driver, *args, **kwargs)
        raise TimeoutError("reply lost after target writes opened")

    monkeypatch.setattr(ManagedLogicalPostgres, "resume", lost)
    h.action("CUTOVER", expected="UNKNOWN")
    write(probe)
    h.action("RECONCILE")
    assert h.state()["lifecycle"] == "SWITCHED"
    assert "ROLLBACK" not in h.state()["allowedActions"]
    monkeypatch.setattr(
        ManagedLogicalPostgres, "resume", lambda *_: pytest.fail("confirmation must be read-only")
    )
    h.action("RESUME", location="TARGET")
    assert h.state()["lifecycle"] == "ACTIVE"


@pytest.mark.integration
def test_failed_route_change_can_restore_and_resume_source_in_one_command(
    managed_case, monkeypatch
):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")
    actual = ManagedLogicalPostgres.reroute

    def fail(*args, **kwargs):
        raise TimeoutError("route change did not start")

    monkeypatch.setattr(ManagedLogicalPostgres, "reroute", fail)
    h.action("CUTOVER", expected="UNKNOWN")
    h.action("RECONCILE")
    assert h.state()["observed"]["businessWriteAccess"] == "BLOCKED"
    monkeypatch.setattr(ManagedLogicalPostgres, "reroute", actual)
    h.action("ROLLBACK")
    restored = h.state()
    assert restored["lifecycle"] == "ROLLED_BACK_HELD"
    assert restored["observed"]["businessWriteAccess"] == "SOURCE_ONLY"
    write(probe)
    h.action("RESUME", location="SOURCE")
    assert h.state()["lifecycle"] == "RESTORED"


@pytest.mark.integration
def test_partial_target_resume_can_finish_after_reconcile_and_deadline(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")
    actual = ManagedLogicalPostgres.admin

    def fail(driver, binding, command, *args, **kwargs):
        if command.startswith("RESUME"):
            raise TimeoutError("proxy resume did not start")
        return actual(driver, binding, command, *args, **kwargs)

    monkeypatch.setattr(ManagedLogicalPostgres, "admin", fail)
    h.action("CUTOVER", expected="UNKNOWN")
    h.action("RECONCILE")
    state = h.state()
    assert "CUTOVER" in state["allowedActions"]
    assert "RESUME" not in state["allowedActions"]
    assert "ROLLBACK" not in state["allowedActions"]
    h.held_since = now() - timedelta(seconds=301)
    monkeypatch.setattr(ManagedLogicalPostgres, "admin", actual)
    h.action("CUTOVER")
    write(probe)
    h.action("RESUME", location="TARGET")
    assert h.state()["lifecycle"] == "ACTIVE"


@pytest.mark.integration
def test_check_only_quiesce_can_cancel_without_pausing_source(managed_case):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")
    h.action("CANCEL")
    assert h.state()["lifecycle"] == "CANCELLED"
    assert h.state()["transitionState"] == "CLOSED"
    write(probe)


@pytest.mark.integration
def test_barrier_failure_and_expired_budget_can_restore_source(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")

    def fail(*args, **kwargs):
        raise TimeoutError("final barrier did not finish")

    monkeypatch.setattr(ManagedLogicalPostgres, "barrier", fail)
    h.action("CUTOVER", expected="UNKNOWN")
    h.action("RECONCILE")
    h.held_since = now() - timedelta(seconds=301)
    h.action("ROLLBACK")
    write(probe)
    h.action("RESUME", location="SOURCE")
    assert h.state()["lifecycle"] == "RESTORED"


@pytest.mark.integration
def test_unreachable_unused_target_does_not_block_source_restore(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")
    actual = ManagedLogicalPostgres.inspect

    def inspect(driver, database):
        if database.provider == "GCP_CLOUDSQL":
            raise psycopg.OperationalError("unused target unreachable")
        return actual(driver, database)

    monkeypatch.setattr(ManagedLogicalPostgres, "inspect", inspect)
    h.action("ROLLBACK")
    write(probe)
    h.action("RESUME", location="SOURCE")
    assert h.state()["lifecycle"] == "RESTORED"


@pytest.mark.integration
def test_lost_restore_reply_is_confirmed_without_reopening_source(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")
    actual = ManagedLogicalPostgres.resume

    def lost(driver, *args, **kwargs):
        actual(driver, *args, **kwargs)
        raise TimeoutError("reply lost after source resumed")

    monkeypatch.setattr(ManagedLogicalPostgres, "resume", lost)
    h.action("ROLLBACK", expected="UNKNOWN")
    write(probe)
    h.action("RECONCILE")
    assert h.state()["lifecycle"] == "ROLLED_BACK_HELD"
    monkeypatch.setattr(ManagedLogicalPostgres, "resume", lambda *_: pytest.fail("already open"))
    h.action("RESUME", location="SOURCE")
    assert h.state()["lifecycle"] == "RESTORED"


@pytest.mark.integration
def test_lost_cancel_reply_is_reconciled_without_pausing_source(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")
    actual = ManagedLogicalPostgres.disable

    def lost(driver, *args, **kwargs):
        actual(driver, *args, **kwargs)
        raise TimeoutError("reply lost after subscription stopped")

    monkeypatch.setattr(ManagedLogicalPostgres, "disable", lost)
    h.action("CANCEL", expected="UNKNOWN")
    write(probe)
    h.action("RECONCILE")
    assert h.state()["lifecycle"] == "CANCELLED"
    assert h.state()["transitionState"] == "CLOSED"


@pytest.mark.integration
def test_failed_check_can_be_reconciled_and_rechecked(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ready(settings)
    actual = ManagedLogicalPostgres.schema_signature
    monkeypatch.setattr(ManagedLogicalPostgres, "schema_signature", lambda *_: {"changed": True})
    h.action("QUIESCE", expected="FAILED")
    write(probe)
    monkeypatch.setattr(ManagedLogicalPostgres, "schema_signature", actual)
    h.action("RECONCILE")
    h.action("QUIESCE")
    h.action("CUTOVER")
    write(probe)
    h.action("RESUME", location="TARGET")
    assert h.state()["lifecycle"] == "ACTIVE"


@pytest.mark.integration
def test_unexpected_source_reopen_cannot_reuse_a_final_barrier(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ready(settings)
    h.action("QUIESCE")

    def fail(*args, **kwargs):
        raise TimeoutError("route change did not start")

    monkeypatch.setattr(ManagedLogicalPostgres, "reroute", fail)
    h.action("CUTOVER", expected="UNKNOWN")
    h.action("RECONCILE")
    row = h.service.store.get(h.mid)
    driver = h.service.scoped(row).driver
    source, _, binding = driver.resources(row["plan"])
    # 외부 관리자가 잘못 재개한 상황입니다. 이 쓰기는 대상에 복제되지 않습니다.
    driver.resume(source, binding, "test-drift", lambda _: None)
    write(probe)
    state = h.state()
    assert "CUTOVER" not in state["allowedActions"]
    assert state["observed"]["businessWriteAccess"] == "SOURCE_ONLY"


def count_orders(database):
    with psycopg.connect(database.secret("admin")) as conn:
        return conn.execute("SELECT count(*) FROM orders").fetchone()[0]


@pytest.mark.integration
def test_cloudsql_to_rds_direction_switches_with_the_same_command(managed_case):
    settings, probe = managed_case
    swap_direction(settings)
    h = ready(settings)
    before = [write(probe) for _ in range(3)]
    h.action("QUIESCE")
    h.action("CUTOVER")
    switched = h.state()
    assert switched["lifecycle"] == "SWITCHED" and switched["observed"]["route"] == "TARGET"
    h.action("RESUME", location="TARGET")
    _, new_id = write(probe)
    assert new_id > before[-1][1]
    timeline = h.state()["details"]["database"]["lastCutover"]
    assert timeline["operationId"] and "steps" not in timeline
    assert 0 < timeline["writePauseMs"] <= timeline["totalMs"]
    assert all(timeline[k] is not None for k in ("pauseMs", "fenceMs", "barrierMs", "resumeMs"))
    finish_active(h)


@pytest.mark.integration
def test_return_trip_resets_the_retired_source_and_brings_the_data_back(managed_case):
    settings, probe = managed_case
    h = ready(settings)
    plan = h.service.store.get(h.mid)["plan"]
    source, target, _ = h.service.driver.resources(plan)
    seeded = count_orders(source)
    frozen = [write(probe) for _ in range(4)]
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")
    after_switch = [write(probe) for _ in range(2)]
    finish_active(h)
    # The retired source keeps the copy frozen at the cutover; the live database moved on.
    assert count_orders(source) == seeded + len(frozen)
    assert count_orders(target) == seeded + len(frozen) + len(after_switch)

    back = AtomicHarness(settings)
    back.generation = h.generation + 1
    back.create(source="resource:target", target="resource:source", key="return-trip")
    back.action("PREPARE")
    assert count_orders(source) == 0
    with psycopg.connect(source.secret("admin")) as conn:
        assert conn.execute("SELECT count(*) FROM foxtrot_migration.barriers").fetchone()[0] == 0
    row = back.service.store.get(back.mid)
    assert row["internal"]["prepared"]
    back.action("START_SYNC")
    back.ready()
    more = [write(probe) for _ in range(2)]
    back.action("QUIESCE")
    back.action("CUTOVER")
    returned = back.state()
    assert returned["lifecycle"] == "SWITCHED" and returned["observed"]["route"] == "TARGET"
    back.action("RESUME", location="TARGET")
    expected = seeded + len(frozen) + len(after_switch) + len(more)
    assert count_orders(source) == expected
    _, latest = write(probe)
    assert latest > more[-1][1]
    assert count_orders(source) == expected + 1
    assert count_orders(target) == expected
    finish_active(back)
    assert back.state()["lifecycle"] == "COMPLETED"
