"""Remove replication objects only from explicitly owned per-test databases.

Rows, databases, roles, PgBouncer aliases and previous review cases are preserved.
"""

import time

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo


def cleanup(env, database, migrations):
    assert database.startswith("case_") and migrations
    names = {
        f"fxt_{mid.removeprefix('mig-')}_{direction}"
        for mid in migrations
        for direction in ["f", "r"]
    }
    removed = {"database": database, "subscriptions": 0, "publications": 0, "slots": 0}
    for node in ["SOURCE", "TARGET"]:
        with psycopg.connect(
            make_conninfo(env["LAB_" + node + "_ADMIN"], dbname=database), autocommit=True
        ) as conn:
            existing = conn.execute(
                "SELECT subname FROM pg_subscription WHERE subdbid="
                "(SELECT oid FROM pg_database WHERE datname=current_database())"
            ).fetchall()
            for (name,) in existing:
                assert name in names, (database, name)
                conn.execute(sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(sql.Identifier(name)))
                conn.execute(
                    sql.SQL("ALTER SUBSCRIPTION {} SET (slot_name=NONE)").format(
                        sql.Identifier(name)
                    )
                )
                conn.execute(sql.SQL("DROP SUBSCRIPTION {}").format(sql.Identifier(name)))
                removed["subscriptions"] += 1
    for node in ["SOURCE", "TARGET"]:
        with psycopg.connect(
            make_conninfo(env["LAB_" + node + "_ADMIN"], dbname=database), autocommit=True
        ) as conn:
            for (name,) in conn.execute("SELECT pubname FROM pg_publication").fetchall():
                assert name in names, (database, name)
                conn.execute(sql.SQL("DROP PUBLICATION {}").format(sql.Identifier(name)))
                removed["publications"] += 1
            for (name,) in conn.execute(
                "SELECT slot_name FROM pg_replication_slots WHERE database=%s", (database,)
            ).fetchall():
                assert name in names or name.startswith("pg_"), (database, name)
                for _attempt in range(20):
                    current = conn.execute(
                        "SELECT active_pid FROM pg_replication_slots "
                        "WHERE slot_name=%s AND database=%s",
                        (name, database),
                    ).fetchone()
                    if current is None:
                        break
                    if current[0]:
                        conn.execute("SELECT pg_terminate_backend(%s)", (current[0],))
                    try:
                        conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
                        removed["slots"] += 1
                        break
                    except psycopg.errors.ObjectInUse:
                        time.sleep(0.1)
                else:
                    raise RuntimeError("Owned test slot did not stop")
    return removed
