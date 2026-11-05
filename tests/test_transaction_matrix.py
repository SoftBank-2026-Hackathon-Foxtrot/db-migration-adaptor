"""Real PostgreSQL transaction failures and races while native CDC is active."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest
from test_operator_integration import Harness

from db_operator.control import actor_ack
from db_operator.store import now

pytestmark = pytest.mark.integration


def syncing(lab_case):
    settings, _, probe = lab_case
    h = Harness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    return h, probe.get_secret_value()


def reserve(dsn, request_id):
    try:
        with psycopg.connect(dsn) as conn:
            held = conn.execute(
                "UPDATE inventory SET stock=stock-1 WHERE sku='chiikawa' AND stock>0 RETURNING sku"
            ).fetchone()
            if not held:
                return False
            conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,%s)", (request_id, held[0]))
        return True
    except psycopg.errors.UniqueViolation:
        return False  # The entire reservation transaction, including stock, rolled back.


def totals(dsn):
    with psycopg.connect(dsn) as conn:
        return conn.execute(
            "SELECT (SELECT count(*) FROM orders),stock FROM inventory WHERE sku='chiikawa'"
        ).fetchone()


def equal_after_drain(h):
    h.held_since = now()
    h.action("QUIESCE")  # Real barrier, durable origin, sequences and full table digest.
    row = h.service.store.get(h.mid)
    source, target, _ = h.service.driver.resources(row["plan"])
    assert h.service.driver.equal(source, target)
    assert h.state()["observed"]["businessWriteAccess"] == "BLOCKED"


def test_last_seven_items_have_no_oversell(lab_case):
    h, dsn = syncing(lab_case)
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE inventory SET stock=7")
    with ThreadPoolExecutor(max_workers=16) as pool:
        successes = list(pool.map(lambda _: reserve(dsn, uuid4()), range(80)))
    assert sum(successes) == 7
    assert totals(dsn) == (17, 0)
    equal_after_drain(h)


def test_eighty_duplicate_reservations_charge_once(lab_case):
    h, dsn = syncing(lab_case)
    request_id = uuid4()
    with ThreadPoolExecutor(max_workers=16) as pool:
        successes = list(pool.map(lambda _: reserve(dsn, request_id), range(80)))
    assert sum(successes) == 1
    assert totals(dsn) == (11, 989)
    equal_after_drain(h)


def test_duplicate_cancellations_restore_stock_once(lab_case):
    h, dsn = syncing(lab_case)
    with psycopg.connect(dsn) as conn:
        order_id = conn.execute("SELECT min(id) FROM orders").fetchone()[0]

    def cancel(_):
        with psycopg.connect(dsn) as conn:
            row = conn.execute(
                "WITH cancelled AS (DELETE FROM orders WHERE id=%s RETURNING sku) "
                "UPDATE inventory SET stock=stock+1 FROM cancelled "
                "WHERE inventory.sku=cancelled.sku RETURNING inventory.sku",
                (order_id,),
            ).fetchone()
            return bool(row)

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(cancel, range(64)))
    assert sum(results) == 1
    assert totals(dsn) == (9, 991)
    equal_after_drain(h)


@pytest.mark.parametrize("failure", ["check", "foreign_key", "unique", "statement_timeout"])
def test_error_rolls_back_all_prior_changes(lab_case, failure):
    h, dsn = syncing(lab_case)
    statements = {
        "check": ("UPDATE inventory SET stock=-1", (), psycopg.errors.CheckViolation),
        "foreign_key": (
            "INSERT INTO orders(request_id,sku) VALUES(%s,'missing')",
            (uuid4(),),
            psycopg.errors.ForeignKeyViolation,
        ),
        "unique": (
            "INSERT INTO orders(request_id,sku) SELECT request_id,sku FROM orders LIMIT 1",
            (),
            psycopg.errors.UniqueViolation,
        ),
        "statement_timeout": ("SELECT pg_sleep(0.5)", (), psycopg.errors.QueryCanceled),
    }
    statement, params, error = statements[failure]
    with pytest.raises(error), psycopg.connect(dsn) as conn:
        conn.execute("SET LOCAL statement_timeout='50ms'")
        conn.execute("UPDATE inventory SET stock=stock-1")
        conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
        conn.execute(statement, params)
    assert totals(dsn) == (10, 990)
    equal_after_drain(h)


def test_savepoint_recovers_only_failed_subtransaction(lab_case):
    h, dsn = syncing(lab_case)
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE inventory SET stock=stock-2")
        conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
        with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
            conn.execute("UPDATE inventory SET stock=-1")
        conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
    assert totals(dsn) == (12, 988)
    equal_after_drain(h)


def test_explicit_abort_and_disconnected_transaction_are_not_replicated(lab_case):
    h, dsn = syncing(lab_case)
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE inventory SET stock=stock-10")
        conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
        conn.rollback()
    conn = psycopg.connect(dsn)
    conn.execute("UPDATE inventory SET stock=stock-10")
    conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
    conn.close()  # Socket closes without COMMIT; backend must abort it.
    assert totals(dsn) == (10, 990)
    equal_after_drain(h)


def test_row_lock_timeout_aborts_whole_transaction(lab_case):
    h, dsn = syncing(lab_case)
    with psycopg.connect(dsn) as holder:
        holder.execute("SELECT * FROM inventory FOR UPDATE")
        with pytest.raises(psycopg.errors.LockNotAvailable), psycopg.connect(dsn) as waiter:
            waiter.execute("SET LOCAL lock_timeout='50ms'")
            waiter.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
            waiter.execute("UPDATE inventory SET stock=stock-1")
        holder.rollback()
    assert totals(dsn) == (10, 990)
    equal_after_drain(h)


def test_deadlock_victim_is_retried_as_a_whole_transaction(lab_case):
    h, dsn = syncing(lab_case)
    with psycopg.connect(dsn) as conn:
        conn.execute("INSERT INTO inventory VALUES('usagi',100)")
    barrier = threading.Barrier(2)

    def update(order):
        try:
            with psycopg.connect(dsn) as conn:
                conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku=%s", (order[0],))
                barrier.wait(timeout=10)
                conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku=%s", (order[1],))
            return "committed"
        except psycopg.errors.DeadlockDetected:
            # No partial changes from the failed attempt may survive.
            with psycopg.connect(dsn) as conn:
                for sku in sorted(order):
                    conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku=%s", (sku,))
            return "retried_40P01"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(update, [("chiikawa", "usagi"), ("usagi", "chiikawa")]))
    assert sorted(results) == ["committed", "retried_40P01"]
    assert totals(dsn) == (10, 988)
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT stock FROM inventory WHERE sku='usagi'").fetchone() == (98,)
    equal_after_drain(h)


def test_serializable_conflict_is_retried_from_new_snapshot(lab_case):
    h, dsn = syncing(lab_case)
    barrier = threading.Barrier(2)

    def update(_):
        try:
            with psycopg.connect(dsn) as conn:
                conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                conn.execute("SELECT stock FROM inventory").fetchone()
                barrier.wait(timeout=10)
                conn.execute("UPDATE inventory SET stock=stock-1")
            return "committed"
        except psycopg.errors.SerializationFailure:
            with psycopg.connect(dsn) as conn:
                conn.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                conn.execute("UPDATE inventory SET stock=stock-1")
            return "retried_40001"

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(update, range(2)))
    assert sorted(results) == ["committed", "retried_40001"]
    assert totals(dsn) == (10, 988)
    equal_after_drain(h)


def test_unfinished_transaction_cannot_supply_quiesce_evidence(lab_case):
    h, dsn = syncing(lab_case)
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE inventory SET stock=stock-1")
        conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
        ref = h.records.held(
            migration_id=h.mid,
            transition_id=h.transition,
            generation=h.generation,
            held_since=now(),
            participants=[
                actor_ack("lab-app", h.generation, "HELD", in_flight=1),
                actor_ack("lab-worker", h.generation, "HELD"),
            ],
        )
        response = h.client.post(
            "/v1/migrations/" + h.mid + "/actions",
            headers={"Idempotency-Key": uuid4().hex},
            json={
                "action": "QUIESCE",
                "expectedRevision": h.state()["revision"],
                "transitionId": h.transition,
                "quiesceRef": ref,
            },
        )
        assert response.status_code == 422, response.json()
        assert h.state()["observed"]["businessWriteAccess"] == "SOURCE_ONLY"
    assert totals(dsn) == (11, 989)
    equal_after_drain(h)


def test_bulk_insert_update_delete_commit_as_one_unit(lab_case):
    h, dsn = syncing(lab_case)
    with psycopg.connect(dsn) as conn:
        conn.execute("UPDATE inventory SET stock=stock-200")
        ids = conn.execute(
            "INSERT INTO orders(request_id,sku) SELECT gen_random_uuid(),'chiikawa' "
            "FROM generate_series(1,200) RETURNING id"
        ).fetchall()
        conn.execute("DELETE FROM orders WHERE id=ANY(%s)", ([r[0] for r in ids[:50]],))
        conn.execute("UPDATE inventory SET stock=stock+50")
    assert totals(dsn) == (160, 840)
    equal_after_drain(h)


def test_persistent_prepared_clients_exceed_backend_pool_without_stalling(lab_case):
    h, dsn = syncing(lab_case)
    start = threading.Barrier(96)

    def customer(_):
        with psycopg.connect(dsn, prepare_threshold=0) as conn:
            start.wait(timeout=15)
            for _ in range(10):
                conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku='chiikawa'")
                conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
                conn.commit()
        return 10

    with ThreadPoolExecutor(max_workers=96) as pool:
        assert sum(pool.map(customer, range(96))) == 960
    assert totals(dsn) == (970, 30)
    equal_after_drain(h)
