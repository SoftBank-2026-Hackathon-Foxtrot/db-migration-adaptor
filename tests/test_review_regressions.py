from __future__ import annotations

import json
import os
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict
from test_operator_integration import Harness, write

from db_operator.models import OperatorError

pytestmark = pytest.mark.integration


def target_container() -> str:
    selected = os.environ.get(
        "DB_OPERATOR_TEST_TARGET_CONTAINER", "foxtrot-db-operator-lab-target-1"
    )
    result = subprocess.run(
        ["docker", "inspect", selected], capture_output=True, text=True, check=True
    )
    details = json.loads(result.stdout)[0]
    connection = conninfo_to_dict(os.environ["LAB_TARGET_ADMIN"])
    ports = details["NetworkSettings"]["Ports"].get("5432/tcp") or []
    assert (
        details["State"]["Running"]
        and details["Config"]["Labels"].get("com.docker.compose.service") == "target"
        and connection.get("host") in {"127.0.0.1", "localhost"}
        and any(
            port["HostIp"] == "127.0.0.1" and port["HostPort"] == connection.get("port")
            for port in ports
        )
    ), "현재 테스트 DB와 다른 컨테이너입니다. DB_OPERATOR_TEST_TARGET_CONTAINER를 확인하십시오."
    return details["Id"]


def ready(h):
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()


def finish_source(h):
    h.action("RESUME", location="SOURCE")
    h.running()
    h.action("FINALIZE")


def test_temporary_observation_failure_recovers_without_reconcile(lab_case, monkeypatch):
    settings, _, _ = lab_case
    h = Harness(settings)
    h.create()
    h.action("PREPARE")
    actual = h.service.driver.observe

    def disconnected(*args, **kwargs):
        raise psycopg.OperationalError("injected observation failure")

    monkeypatch.setattr(h.service.driver, "observe", disconnected)
    state = h.state()
    assert state["health"] == "OK"
    assert state["readiness"]["advance"]["result"] == "UNKNOWN"
    assert state["allowedActions"] == ["RECONCILE"]
    monkeypatch.setattr(h.service.driver, "observe", actual)
    state = h.state()
    assert state["health"] == "OK" and "START_SYNC" in state["allowedActions"]
    assert not h.service.store.get(h.mid)["internal"].get("unresolvedOperationId")
    with monkeypatch.context() as patch:
        patch.setattr(h.service.store, "get", disconnected)
        failed_get = h.client.get(f"/v1/migrations/{h.mid}")
        assert failed_get.status_code == 503
        assert failed_get.json()["blockers"] == ["MANAGEMENT_UNAVAILABLE"]
    with monkeypatch.context() as patch:
        patch.setattr(h.service.store, "submit", disconnected)
        failed_post = h.client.post(
            f"/v1/migrations/{h.mid}/actions",
            headers={"Idempotency-Key": "management-outage"},
            json={"action": "CANCEL", "expectedRevision": state["revision"]},
        )
        assert failed_post.status_code == 503
        assert failed_post.json()["blockers"] == ["RESULT_UNCONFIRMED"]
    h.action("CANCEL")
    h.action("FINALIZE")


def test_cached_get_avoids_remote_io_and_does_not_lock_row(lab_case, monkeypatch):
    settings, _, _ = lab_case
    h = Harness(settings)
    h.create()
    h.action("PREPARE")
    h.service.settings.observation_cache_seconds = 2
    h.state()

    def unexpected(*args, **kwargs):
        pytest.fail("A fresh cached GET must not contact remote databases")

    with monkeypatch.context() as patch:
        patch.setattr(h.service.driver, "observe", unexpected)
        with ThreadPoolExecutor(max_workers=1) as executor, h.service.store.transaction() as tx:
            tx.put("MIG", h.mid, tx.get("MIG", h.mid))
            future = executor.submit(h.service.get, h.mid)
            assert future.result(timeout=2)["health"] == "OK"
    h.service.settings.observation_cache_seconds = 0
    h.action("CANCEL")
    h.action("FINALIZE")


def test_concurrent_expired_gets_share_one_observation(lab_case, monkeypatch):
    settings, _, _ = lab_case
    h = Harness(settings)
    h.create()
    h.action("PREPARE")
    h.service.settings.observation_cache_seconds = 2
    h.service.store.update(h.mid, lambda row: row["internal"].pop("observationCache", None))
    entered, second_read, release = threading.Event(), threading.Event(), threading.Event()
    actual_observe, actual_get = h.service.inspect_resources, h.service.store.get
    observations = []

    def observe(*args, **kwargs):
        observations.append(True)
        entered.set()
        assert release.wait(timeout=10)
        return actual_observe(*args, **kwargs)

    def get(*args, **kwargs):
        row = actual_get(*args, **kwargs)
        if threading.current_thread().name.endswith("_1"):
            second_read.set()
        return row

    monkeypatch.setattr(h.service, "inspect_resources", observe)
    monkeypatch.setattr(h.service.store, "get", get)
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="poll") as executor:
        first = executor.submit(h.service.get, h.mid)
        try:
            assert entered.wait(timeout=5)
            second = executor.submit(h.service.get, h.mid)
            assert second_read.wait(timeout=5)
        finally:
            release.set()
        assert first.result(timeout=5) == second.result(timeout=5)
    assert len(observations) == 1
    h.action("CANCEL")
    h.action("FINALIZE")


def test_stale_readiness_poll_cannot_overwrite_completed_quiesce(lab_case, monkeypatch):
    settings, _, _ = lab_case
    h = Harness(settings)
    ready(h)
    started, release = threading.Event(), threading.Event()
    actual = h.service.driver.replication_ready

    def delayed(*args, **kwargs):
        if threading.current_thread().name == "stale-refresh":
            started.set()
            assert release.wait(timeout=20)
        return actual(*args, **kwargs)

    monkeypatch.setattr(h.service.driver, "replication_ready", delayed)
    errors = []

    def stale_get():
        try:
            h.service.refresh(h.mid)
        except Exception as error:
            errors.append(error)

    thread = threading.Thread(target=stale_get, name="stale-refresh")
    thread.start()
    try:
        assert started.wait(timeout=5)
        h.action("QUIESCE")
    finally:
        release.set()
        thread.join(timeout=10)
    assert not thread.is_alive() and not errors
    assert h.state()["lifecycle"] == "QUIESCED"
    h.action("ROLLBACK")
    finish_source(h)


def test_successful_command_survives_post_success_refresh_error(lab_case, monkeypatch):
    settings, _, _ = lab_case
    h = Harness(settings)
    h.create()
    response = h.client.post(
        f"/v1/migrations/{h.mid}/actions",
        headers={"Idempotency-Key": uuid4().hex},
        json={"action": "PREPARE", "expectedRevision": h.state()["revision"]},
    )
    assert response.status_code == 202

    def unavailable(*args, **kwargs):
        raise RuntimeError("injected post-success refresh failure")

    with monkeypatch.context() as patch:
        patch.setattr(h.service, "get", unavailable)
        with h.worker.exclusive():
            assert h.worker.run_once()
            assert not h.worker.run_once()
    assert h.client.get(response.headers["Location"]).json()["status"] == "SUCCEEDED"
    assert h.state()["lifecycle"] == "PREPARED"
    h.action("CANCEL")
    h.action("FINALIZE")


@pytest.mark.parametrize("after_reroute", [False, True])
def test_interrupted_direct_rollback_reconciles_without_target_equality(
    lab_case, monkeypatch, after_reroute
):
    settings, _, probe = lab_case
    h = Harness(settings)
    ready(h)

    def failed_barrier(*args, **kwargs):
        raise OperatorError("TEST_DEADLINE", "최종 동기화 실패 시험")

    with monkeypatch.context() as patch:
        patch.setattr(h.service.driver, "barrier", failed_barrier)
        h.action("QUIESCE", expected="UNKNOWN")
    _, target, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    with h.service.driver.connect(target) as conn:
        conn.execute("DELETE FROM orders")
    h.action("RECONCILE")
    actual = h.service.driver.reroute

    def interrupted(*args, **kwargs):
        if after_reroute:
            actual(*args, **kwargs)
        raise psycopg.OperationalError("injected rollback interruption")

    with monkeypatch.context() as patch:
        patch.setattr(h.service.driver, "reroute", interrupted)
        h.action("ROLLBACK", expected="UNKNOWN")
    h.action("RECONCILE")
    assert h.state()["lifecycle"] == "ROLLED_BACK_HELD"
    assert "CUTOVER" not in h.state()["allowedActions"]
    h.action("RESUME", location="SOURCE")
    write(probe)
    h.running()
    h.action("FINALIZE")


@pytest.mark.parametrize("outage_at", ["before_quiesce", "after_cutover"])
def test_offline_unused_target_can_return_to_authoritative_source(lab_case, monkeypatch, outage_at):
    settings, _, probe = lab_case
    h = Harness(settings)
    ready(h)
    container = target_container()
    stopped = []

    def stop_target(*args, **kwargs):
        subprocess.run(["docker", "stop", container], check=True, capture_output=True)
        stopped.append(True)
        raise psycopg.OperationalError("Target stopped after source writers were fenced")

    try:
        if outage_at == "before_quiesce":
            with monkeypatch.context() as patch:
                patch.setattr(h.service.driver, "barrier", stop_target)
                h.action("QUIESCE", expected="UNKNOWN")
            h.action("RECONCILE")
        else:
            h.action("QUIESCE")
            h.action("CUTOVER")
            subprocess.run(["docker", "stop", container], check=True, capture_output=True)
            stopped.append(True)
        assert not h.state()["observed"]["targetWritesPossible"]
        h.action("ROLLBACK")
        h.action("RESUME", location="SOURCE")
        write(probe)
        assert h.state()["lifecycle"] == "RESTORED"
        assert (
            h.service.store.get(h.mid)["internal"]["observedDetails"]["accessEvidence"]
            == "PERSISTED_TARGET_FENCE"
        )
        assert (
            h.service.store.get(h.mid)["internal"]["observedDetails"]["target"]["oldExecutors"]
            is None
        )
        h.running()
    finally:
        if stopped:
            subprocess.run(["docker", "start", container], check=True, capture_output=True)
            h.service.driver.wait(
                lambda: (
                    subprocess.run(
                        ["docker", "exec", container, "pg_isready", "-q", "-U", "postgres"],
                        capture_output=True,
                    ).returncode
                    == 0
                ),
                20,
            )
    h.action("FINALIZE")


def test_barrier_rejects_stale_non_null_origin_progress(lab_case, monkeypatch):
    settings, _, _ = lab_case
    h = Harness(settings)
    ready(h)
    actual = h.service.driver.connect
    stale = []

    class Connection:
        def __init__(self, connection):
            self.connection = connection

        def __getattr__(self, name):
            return getattr(self.connection, name)

        def execute(self, query, *args, **kwargs):
            if (
                isinstance(query, str)
                and "pg_replication_origin_progress(" in query
                and len(stale) < 2
            ):
                stale.append(True)
                values = ("0/1", *args[0][1:])
                return self.connection.execute(
                    query.replace("pg_replication_origin_progress(%s,true)", "%s::pg_lsn"),
                    values,
                    **kwargs,
                )
            return self.connection.execute(query, *args, **kwargs)

    @contextmanager
    def connect(*args, **kwargs):
        with actual(*args, **kwargs) as connection:
            yield Connection(connection)

    with monkeypatch.context() as patch:
        patch.setattr(h.service.driver, "connect", connect)
        h.action("QUIESCE")
    assert len(stale) == 2
    evidence = h.service.store.get(h.mid)["internal"]["forwardBarrier"]
    source, _, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    with actual(source) as conn:
        assert conn.execute(
            "SELECT %s::pg_lsn >= %s::pg_lsn AS ok",
            (
                evidence["durableOriginLsn"],
                evidence["committedThroughLsn"],
            ),
        ).fetchone()["ok"]
    h.action("ROLLBACK")
    finish_source(h)


def test_target_outage_after_writes_does_not_use_persisted_prewrite_fence(lab_case):
    settings, _, probe = lab_case
    h = Harness(settings)
    ready(h)
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")
    request_id, _ = write(probe)
    container = target_container()
    subprocess.run(["docker", "stop", container], check=True, capture_output=True)
    try:
        state = h.state()
        assert state["observed"]["targetWritesPossible"]
        assert state["readiness"]["rollback"]["result"] == "UNKNOWN"
        assert state["allowedActions"] == ["RECONCILE"]
        assert not h.service.source_authority(h.service.store.get(h.mid))
        h.action("RECONCILE", expected="UNKNOWN")
        assert h.state()["health"] == "UNKNOWN"
    finally:
        subprocess.run(["docker", "start", container], check=True, capture_output=True)
        h.service.driver.wait(
            lambda: (
                subprocess.run(
                    ["docker", "exec", container, "pg_isready", "-q", "-U", "postgres"],
                    capture_output=True,
                ).returncode
                == 0
            ),
            20,
        )
    h.action("RECONCILE")
    source, target, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
    with h.service.driver.connect(target) as conn:
        assert conn.execute("SELECT 1 FROM orders WHERE request_id=%s", (request_id,)).fetchone()
    h.service.driver.wait(
        lambda: h.service.driver.replication_ready(
            source, h.service.driver.names(h.mid)["reverse"]
        ),
        20,
    )
    h.action("QUIESCE")
    h.action("ROLLBACK")
    finish_source(h)


def test_real_reroute_preserves_alias_options_comments_and_include(lab_case):
    settings, _, _ = lab_case
    h = Harness(settings)
    h.create()
    plan = h.service.store.get(h.mid)["plan"]
    source, _, binding = h.service.driver.resources(plan)
    ini = Path(binding.config_path)
    old = f"{binding.alias} = host=source port=5432 dbname={source.database}"
    options = " pool_size=7 reserve_pool_size=2 auth_user=app_writer"
    include = ini.parent / f"include-{binding.alias}.ini"
    include.write_text(f"[users]\n{source.writer_role} = pool_mode=transaction\n")
    before = ini.read_text().replace(old, "; keep route notes\n" + old + options, 1)
    before += f"\n%include /etc/pgbouncer/{include.name}\n"
    ini.write_text(before)
    h.service.driver.admin(binding, "RELOAD")
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("QUIESCE")
    h.action("CUTOVER")
    assert ini.read_text() == before.replace(
        old + options, old.replace("host=source", "host=target") + options
    )
    with h.service.driver.connect(source) as conn:
        assert not conn.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s",
            (h.service.driver.names(h.mid)["forward"],),
        ).fetchone()
    h.action("ROLLBACK")
    assert ini.read_text() == before
    finish_source(h)
