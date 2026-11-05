"""Seed and verify the isolated cloud DMS demo. Not a throughput or cutover benchmark."""

from __future__ import annotations

import argparse
import json
import os
import secrets
from pathlib import Path
from uuid import uuid4

import google.auth
import psycopg
from google.auth.transport.requests import AuthorizedSession
from psycopg import sql
from psycopg.conninfo import make_conninfo

from db_operator.config import Database, Registry
from db_operator.postgres import Postgres

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "runtime/dms-cloud"


def connections() -> tuple[dict, dict, dict]:
    source = json.loads((RUNTIME / "aws-source.json").read_text())
    if source["DBInstanceIdentifier"] != "foxtrot-dms-demo-20261104":
        raise RuntimeError("Source identity mismatch")
    admin = json.loads((RUNTIME / "source-admin.json").read_text())
    passwords = json.loads((RUNTIME / "generated-secrets.json").read_text())
    if "writer_password" not in passwords:
        passwords["writer_password"] = secrets.token_urlsafe(30)
        (RUNTIME / "generated-secrets.json").write_text(json.dumps(passwords))
    source_conn = {
        "host": source["Endpoint"]["Address"],
        "port": 5432,
        "dbname": "foxtrot",
        "user": admin["username"],
        "password": admin["password"],
        "sslmode": "verify-full",
        "sslrootcert": str(RUNTIME / "rds-ca.pem"),
        "connect_timeout": 5,
        "options": "-c statement_timeout=20000",
    }
    credentials, _ = google.auth.default(
        quota_project_id="foxtrot-513413", scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    session = AuthorizedSession(credentials)
    response = session.get(
        "https://sqladmin.googleapis.com/sql/v1beta4/projects/foxtrot-513413/instances/foxtrot-dms-demo-20261104",
        timeout=20,
    )
    response.raise_for_status()
    target = response.json()
    if (
        target["name"] != "foxtrot-dms-demo-20261104"
        or target["settings"].get("userLabels", {}).get("purpose") != "dms-demo"
    ):
        raise RuntimeError("Destination identity mismatch")
    (RUNTIME / "cloudsql-instance.json").write_text(json.dumps(target, indent=2))
    (RUNTIME / "target-ca.pem").write_text(target["serverCaCert"]["cert"])
    address = next(x["ipAddress"] for x in target["ipAddresses"] if x["type"] == "PRIMARY")
    target_conn = {
        "host": address,
        "port": 5432,
        "dbname": "foxtrot",
        "user": "postgres",
        "password": passwords["target_password"],
        "connect_timeout": 5,
        "sslmode": "verify-ca",
        "sslrootcert": str(RUNTIME / "target-ca.pem"),
        "options": "-c statement_timeout=20000",
    }
    return source_conn, target_conn, passwords


def backend(source: dict, target: dict, passwords: dict) -> tuple[Postgres, Database, Database]:
    databases = {}
    for key, parameters in [("SOURCE", source), ("TARGET", target)]:
        os.environ["DMS_DEMO_" + key] = make_conninfo(**parameters)
        os.environ["DMS_DEMO_REPLICATION"] = make_conninfo(
            **{**source, "user": "foxtrot_migrate", "password": passwords["migration_password"]}
        )
        databases[key] = Database(
            identity="cloud-demo-" + key.lower(),
            admin_env="DMS_DEMO_" + key,
            replication_env="DMS_DEMO_REPLICATION",
            writer_role="foxtrot_writer",
            route_host=parameters["host"],
            database="foxtrot",
        )
    registry = Registry(
        databases=databases, bindings={}, workloads={}, tables=["public.inventory", "public.orders"]
    )
    return Postgres(registry), databases["SOURCE"], databases["TARGET"]


def seed(source: dict, passwords: dict, driver: Postgres, source_db: Database) -> dict:
    with psycopg.connect(**source, autocommit=True) as conn:
        for role, password in [
            ("foxtrot_migrate", passwords["migration_password"]),
            ("foxtrot_writer", passwords["writer_password"]),
        ]:
            if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)).fetchone():
                conn.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        sql.Identifier(role), sql.Literal(password)
                    )
                )
        conn.execute("GRANT rds_replication TO foxtrot_migrate")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS inventory(sku text PRIMARY KEY, "
            "stock integer NOT NULL CHECK(stock>=0))"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS orders(id bigserial PRIMARY KEY,"
            "request_id uuid NOT NULL UNIQUE,sku text NOT NULL REFERENCES inventory(sku))"
        )
        with conn.transaction():
            if conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 0:
                conn.execute(
                    "INSERT INTO inventory VALUES('chiikawa',1000000) ON CONFLICT DO NOTHING"
                )
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')",
                        [(uuid4(),) for _ in range(1000)],
                    )
                conn.execute("UPDATE inventory SET stock=stock-1000 WHERE sku='chiikawa'")
        conn.execute("GRANT USAGE ON SCHEMA public TO foxtrot_writer")
        conn.execute("GRANT SELECT,INSERT,UPDATE,DELETE ON inventory,orders TO foxtrot_writer")
        conn.execute("GRANT USAGE,SELECT ON SEQUENCE orders_id_seq TO foxtrot_writer")
    driver.prepare_barrier(source_db, "dms-demo-seed", lambda _: None)
    for database in ["postgres", "foxtrot"]:
        with psycopg.connect(**{**source, "dbname": database}, autocommit=True) as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS pglogical")
            schemas = conn.execute(
                "SELECT nspname FROM pg_namespace WHERE nspname !~ '^pg_' "
                "AND nspname <> 'information_schema'"
            ).fetchall()
            for (schema,) in schemas:
                conn.execute(
                    sql.SQL("GRANT USAGE ON SCHEMA {} TO foxtrot_migrate").format(
                        sql.Identifier(schema)
                    )
                )
                conn.execute(
                    sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO foxtrot_migrate").format(
                        sql.Identifier(schema)
                    )
                )
                conn.execute(
                    sql.SQL("GRANT SELECT ON ALL SEQUENCES IN SCHEMA {} TO foxtrot_migrate").format(
                        sql.Identifier(schema)
                    )
                )
            conn.execute("GRANT USAGE ON SCHEMA pglogical TO PUBLIC")
    return {"seedOrders": 1000, "source": driver.snapshot(source_db)}


def mutate(source: dict, passwords: dict) -> dict:
    path = RUNTIME / "cdc-workload.json"
    if path.exists():
        raise RuntimeError("Workload was already attempted; do not repeat it automatically")
    path.write_text(json.dumps({"status": "SUBMITTING"}))
    writer = {**source, "user": "foxtrot_writer", "password": passwords["writer_password"]}
    requests = [uuid4() for _ in range(200)]
    with psycopg.connect(**writer, autocommit=True) as conn:
        for request in requests:
            with conn.transaction():
                conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku='chiikawa'")
                conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (request,))
        for request in requests[:25]:
            with conn.transaction():
                conn.execute("DELETE FROM orders WHERE request_id=%s", (request,))
                conn.execute("UPDATE inventory SET stock=stock+1 WHERE sku='chiikawa'")
        for request in requests[25:50]:
            try:
                with conn.transaction():
                    conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku='chiikawa'")
                    conn.execute(
                        "INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (request,)
                    )
            except psycopg.errors.UniqueViolation:
                continue
            raise AssertionError("Duplicate reservation unexpectedly committed")
        for _ in range(25):
            with conn.transaction(force_rollback=True):
                conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku='chiikawa'")
                conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
    marker = uuid4()
    with psycopg.connect(**source, autocommit=True) as conn:
        conn.execute("SET synchronous_commit=on")
        conn.execute("INSERT INTO foxtrot_migration.barriers VALUES(%s,'dms-demo-cdc')", (marker,))
    result = {
        "status": "COMMITTED",
        "reservations": 200,
        "cancellations": 25,
        "duplicateRollbacks": 25,
        "explicitRollbacks": 25,
        "marker": str(marker),
    }
    path.write_text(json.dumps(result, indent=2))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["seed", "mutate", "check"])
    args = parser.parse_args()
    try:
        source, target, passwords = connections()
        driver, source_db, target_db = backend(source, target, passwords)
        if args.command == "seed":
            result = seed(source, passwords, driver, source_db)
        elif args.command == "mutate":
            result = mutate(source, passwords)
        else:
            first, second = driver.snapshot(source_db), driver.snapshot(target_db)
            if first != second:
                raise AssertionError("Full source/target table digests differ")
            with psycopg.connect(**target, autocommit=True) as conn:
                row = conn.execute(
                    "SELECT (SELECT count(*) FROM orders), "
                    "(SELECT stock FROM inventory WHERE sku='chiikawa')"
                ).fetchone()
            if row[0] + row[1] != 1000000:
                raise AssertionError("Inventory invariant failed")
            result = {
                "equal": True,
                "orders": row[0],
                "stock": row[1],
                "tables": second,
                "cutoverVerified": False,
                "reverseReplicationVerified": False,
                "sequenceValues": "checked only after promotion",
            }
        (RUNTIME / (args.command + "-result.json")).write_text(json.dumps(result, indent=2))
        print(json.dumps(result, indent=2))
    except (psycopg.Error, KeyError, RuntimeError, AssertionError) as exc:
        print(json.dumps({"error": type(exc).__name__, "sqlstate": getattr(exc, "sqlstate", None)}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
