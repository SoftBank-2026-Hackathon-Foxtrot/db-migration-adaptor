from __future__ import annotations

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from test_operator_integration import Harness, validate, write

from db_operator.store import now

pytestmark = pytest.mark.integration


def resources(h):
    return h.service.driver.resources(h.service.store.get(h.mid)["plan"])


def assert_no_publication(h, db):
    with h.service.driver.connect(db) as conn:
        assert not conn.execute(
            "SELECT 1 FROM pg_publication WHERE pubname=%s",
            (h.service.driver.names(h.mid)["forward"],),
        ).fetchone()


def assert_prepare_rejected(h, code):
    _, _, accepted = h.action("PREPARE", expected="FAILED")
    operation = h.service.store.operation(accepted["operationId"])
    assert operation["error"]["code"] == code, operation
    source, _, _ = resources(h)
    assert_no_publication(h, source)
    observed = h.service.driver.observe(h.service.store.get(h.mid)["plan"])
    assert observed["route"] == "SOURCE"
    assert observed["access"] == "SOURCE_ONLY"
    assert not observed["paused"]


def rejected_held_action(h, action):
    # Control evidence is recorded before obtaining the current command revision.
    reference = h.held()
    state = h.state()
    response = h.client.post(
        f"/v1/migrations/{h.mid}/actions",
        headers={"Idempotency-Key": uuid4().hex},
        json={
            "action": action,
            "expectedRevision": state["revision"],
            "transitionId": h.transition,
            "quiesceRef": reference,
        },
    )
    assert response.status_code == 202, response.json()
    validate("OperationAccepted", response.json())
    with h.worker.exclusive():
        assert h.worker.run_once()
    operation = h.client.get(response.headers["Location"]).json()
    validate("Operation", operation)
    assert operation["status"] in {"FAILED", "UNKNOWN"}, operation
    assert operation["error"]["code"] == "SCHEMA_MISMATCH", operation
    return operation


def test_prepare_rejects_replica_identity_nothing_before_source_writes_break(lab_case):
    settings, _, probe = lab_case
    h = Harness(settings)
    h.create()
    source, target, _ = resources(h)
    for db in [source, target]:
        with h.service.driver.connect(db) as conn:
            conn.execute("ALTER TABLE inventory REPLICA IDENTITY NOTHING")
    assert_prepare_rejected(h, "UNSUPPORTED_SCHEMA")
    # An unsuccessful PREPARE must not install a publication that rejects UPDATE.
    with psycopg.connect(probe.get_secret_value()) as conn:
        row = conn.execute(
            "UPDATE inventory SET stock=stock-1 WHERE sku='chiikawa' RETURNING stock"
        ).fetchone()
        assert row[0] == 989


def test_prepare_rejects_missing_partial_unique_index_on_target(lab_case):
    settings, _, probe = lab_case
    h = Harness(settings)
    h.create()
    source, _, _ = resources(h)
    with h.service.driver.connect(source) as conn:
        conn.execute(
            "CREATE UNIQUE INDEX inventory_active_sku_source "
            "ON inventory(lower(sku)) WHERE stock > 0"
        )
    assert_prepare_rejected(h, "SCHEMA_MISMATCH")
    # The original writer remains available after rejecting an incomplete schema.
    write(probe)


def test_equivalent_partial_unique_indexes_ignore_names_and_support_reverse_rollback(lab_case):
    settings, _, probe = lab_case
    h = Harness(settings)
    h.create("REVERSE_REQUIRED")
    source, target, _ = resources(h)
    for db, name in [
        (source, "source USING label"),
        (target, "target USING other label"),
    ]:
        with h.service.driver.connect(db) as conn:
            conn.execute(
                sql.SQL("CREATE UNIQUE INDEX {} ON inventory(lower(sku)) WHERE stock > 0").format(
                    sql.Identifier(name)
                )
            )
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")
    with (
        pytest.raises(psycopg.errors.UniqueViolation),
        psycopg.connect(probe.get_secret_value()) as conn,
    ):
        conn.execute("INSERT INTO inventory VALUES ('CHIIKAWA',1)")
    request_id, _ = write(probe)
    h.running()
    h.generation += 1
    h.held_since = now()
    h.action("QUIESCE")
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    with psycopg.connect(probe.get_secret_value()) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM orders WHERE request_id=%s", (request_id,)
            ).fetchone()[0]
            == 1
        )
    write(probe)
    h.running()
    h.action("FINALIZE")
    assert h.state()["lifecycle"] == "COMPLETED"


def test_prepare_rejects_smaller_target_sequence_range(lab_case):
    settings, _, probe = lab_case
    h = Harness(settings)
    h.create()
    source, target, _ = resources(h)
    with h.service.driver.connect(target) as conn:
        conn.execute("ALTER SEQUENCE orders_id_seq MAXVALUE 10")
    with h.service.driver.connect(source) as conn:
        assert conn.execute("SELECT last_value FROM orders_id_seq").fetchone()["last_value"] == 10
    assert_prepare_rejected(h, "SCHEMA_MISMATCH")
    _, order_id = write(probe)
    assert order_id == 11


@pytest.mark.parametrize("stage", ["QUIESCE", "CUTOVER"])
def test_sequence_configuration_drift_blocks_transition_after_prepare(lab_case, stage):
    settings, _, _ = lab_case
    h = Harness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    if stage == "CUTOVER":
        h.action("QUIESCE")
    source, target, _ = resources(h)
    for db in [source, target]:
        with h.service.driver.connect(db) as conn:
            # Both schemas still match each other. PREPARE's baseline must differ.
            conn.execute("ALTER SEQUENCE orders_id_seq MAXVALUE 1000")
    rejected_held_action(h, stage)
    row = h.service.store.get(h.mid)
    observed = h.service.driver.observe(row["plan"])
    assert observed["route"] == "SOURCE"
    assert observed["access"] in {"SOURCE_ONLY", "BLOCKED"}
    assert observed["target"]["writer"]["rolcanlogin"] is False
    assert not row["internal"].get("targetWritesIntent")
    assert row["state"]["lifecycle"] not in {"SWITCHED", "ACTIVE"}
