"""Measure the existing small RDS -> Cloud SQL DMS demo without changing DB settings.

This measures DB transactions, not HTTP requests. A reservation changes two rows.
Observer queries and the client's internet round trips are included. Marker delay
measures commit-call start to target visibility, not replica disk-flush durability.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
from dms_demo_data import RUNTIME, connections
from load_migration import parse_pgbench
from psycopg import sql

from db_operator.dms import DmsClient, JobBinding

ROOT = Path(__file__).resolve().parents[1]


def save(path, value):
    path.write_text(json.dumps(value, indent=2, default=str))


def emit(**values):
    print(json.dumps(values, default=str), flush=True)


def percentiles(values):
    values = sorted(values)
    if not values:
        return {"samples": 0}
    return {
        "samples": len(values),
        **{
            name: values[max(0, math.ceil(p * len(values)) - 1)]
            for name, p in [("p50", 0.5), ("p95", 0.95), ("p99", 0.99)]
        },
        "max": values[-1],
    }


def log_latencies(paths, throttled):
    elapsed, execution = [], []
    for path in paths:
        for line in path.read_text().splitlines():
            fields = line.split()
            if len(fields) < 6 or not fields[2].isdigit():
                continue  # skipped/failed records are not completed-transaction latency
            total = int(fields[2]) / 1000
            lag = int(fields[6]) / 1000 if throttled else 0
            elapsed.append(total)
            execution.append(max(0, total - lag))
    return {
        "sampling_fraction": 0.1,
        "scheduled_latency_ms": percentiles(elapsed),
        "execution_latency_ms": percentiles(execution),
    }


def connect(parameters):
    return psycopg.connect(
        **{**parameters, "options": "-c statement_timeout=3000"},
        autocommit=True,
        application_name="foxtrot-dms-benchmark",
    )


def business(parameters):
    with connect(parameters) as conn:
        count, stock = conn.execute(
            "SELECT (SELECT count(*) FROM orders),(SELECT sum(stock) FROM inventory)"
        ).fetchone()
    return {"orders": count, "stock": stock, "at": time.monotonic()}


def conserved_by_sku(parameters):
    with connect(parameters) as conn:
        return dict(
            conn.execute(
                "SELECT i.sku,i.stock+count(o.id) FROM inventory i LEFT JOIN orders o "
                "ON o.sku=i.sku GROUP BY i.sku,i.stock ORDER BY i.sku"
            ).fetchall()
        )


def full_snapshot(parameters):
    """Read every benchmark row in PK order with bounded network batches."""
    result = {}
    tables = [
        ("public", "inventory", "sku"),
        ("public", "orders", "id"),
        ("foxtrot_migration", "barriers", "id"),
    ]
    for schema, table, pk in tables:
        digest, count = hashlib.sha256(), 0
        with psycopg.connect(
            **{**parameters, "options": "-c statement_timeout=60000"},
            application_name="foxtrot-dms-benchmark-validation",
        ) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            with conn.cursor(name="foxtrot_benchmark_snapshot") as cursor:
                cursor.itersize = 10000
                cursor.execute(
                    sql.SQL("SELECT row_to_json(t)::text FROM {}.{} t ORDER BY {}").format(
                        sql.Identifier(schema), sql.Identifier(table), sql.Identifier(pk)
                    )
                )
                for (row,) in cursor:
                    encoded = row.encode()
                    digest.update(len(encoded).to_bytes(8, "big"))
                    digest.update(encoded)
                    count += 1
        result[schema + "." + table] = {"rows": count, "sha256": digest.hexdigest()}
    return result


def catchup(source, target, label, timeout=120):
    marker = uuid4()
    started = time.monotonic()
    with connect(source) as conn:
        conn.execute("INSERT INTO foxtrot_migration.barriers VALUES(%s,%s)", (marker, label))
    with connect(target) as conn:
        while time.monotonic() - started < timeout:
            if conn.execute(
                "SELECT 1 FROM foxtrot_migration.barriers WHERE id=%s", (marker,)
            ).fetchone():
                return {
                    "seconds": time.monotonic() - started,
                    "marker": str(marker),
                    "proof": "target visibility; not a durable cutover proof",
                }
            time.sleep(0.1)
    raise RuntimeError("CDC did not catch up within the test time budget")


class Observer:
    def __init__(self, source, target, output):
        self.parameters = {"source": source, "target": target}
        self.output = output
        self.stop = threading.Event()
        self.abort = threading.Event()
        self.phase = "setup"
        self.samples, self.markers, self.errors = [], [], []
        self.threads = []

    def counts(self):
        handles = {}
        context = {"node": None, "query": "connect"}
        try:
            handles = {name: connect(p) for name, p in self.parameters.items()}
            while not self.stop.is_set():
                sample = {"phase": self.phase, "nodes": {}}
                for name, conn in handles.items():
                    context = {"node": name, "query": "order_count"}
                    orders = conn.execute("SELECT count(*) FROM orders").fetchone()[0]
                    node = {"orders": orders, "at": time.monotonic()}
                    if name == "source":
                        context["query"] = "replication_slots"
                        node["slots"] = [
                            {
                                "name": n,
                                "database": d,
                                "active": active,
                                "unconfirmed_bytes": gap,
                                "retained_bytes": kept,
                            }
                            for n, d, active, gap, kept in conn.execute(
                                "SELECT slot_name,database,active,"
                                "pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)::bigint,"
                                "pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint "
                                "FROM pg_replication_slots"
                            ).fetchall()
                        ]
                        if any(
                            (s["unconfirmed_bytes"] or 0) > 256 * 1024**2
                            or (s["retained_bytes"] or 0) > 512 * 1024**2
                            for s in node["slots"]
                        ):
                            self.abort.set()
                        query_filter = "usename='foxtrot_writer'"
                    else:
                        query_filter = (
                            "usename='cloudsqlexternalsync' OR backend_type<>'client backend'"
                        )
                    context["query"] = "wait_events"
                    node["waits"] = [
                        {"type": kind, "event": event, "connections": n}
                        for kind, event, n in conn.execute(
                            "SELECT wait_event_type,wait_event,count(*) FROM pg_stat_activity "
                            "WHERE " + query_filter + " GROUP BY 1,2"
                        ).fetchall()
                    ]
                    sample["nodes"][name] = node
                self.samples.append(sample)
                self.stop.wait(2)
        except Exception as exc:
            self.errors.append(
                {
                    "observer": "counts",
                    "type": type(exc).__name__,
                    "phase": self.phase,
                    **context,
                }
            )
            self.abort.set()
        finally:
            for conn in handles.values():
                conn.close()

    def propagation(self):
        try:
            with connect(self.parameters["source"]) as a, connect(self.parameters["target"]) as b:
                while not self.stop.is_set():
                    marker, phase, started = uuid4(), self.phase, time.monotonic()
                    a.execute(
                        "INSERT INTO foxtrot_migration.barriers VALUES(%s,%s)",
                        (marker, "benchmark:" + phase),
                    )
                    while not self.stop.is_set():
                        if b.execute(
                            "SELECT 1 FROM foxtrot_migration.barriers WHERE id=%s", (marker,)
                        ).fetchone():
                            self.markers.append(
                                {"phase": phase, "seconds": time.monotonic() - started}
                            )
                            break
                        if time.monotonic() - started > 30:
                            raise RuntimeError("Marker propagation exceeded 30 seconds")
                        self.stop.wait(0.1)
                    self.stop.wait(1)
        except Exception as exc:
            self.errors.append({"observer": "propagation", "type": type(exc).__name__})
            self.abort.set()

    def __enter__(self):
        self.threads = [
            threading.Thread(target=f, daemon=True) for f in [self.counts, self.propagation]
        ]
        for thread in self.threads:
            thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=8)
        save(self.output / "observations.json", self.samples)
        save(self.output / "marker-observations.json", self.markers)
        save(self.output / "observer-errors.json", self.errors)
        if any(thread.is_alive() for thread in self.threads):
            raise RuntimeError("An observer did not stop")

    def applied(self, phase, pure_reservations):
        result = {}
        for name in ["source", "target"]:
            points = [
                s["nodes"][name] for s in self.samples if s["phase"] == phase and name in s["nodes"]
            ]
            if len(points) >= 2:
                a, b = points[0], points[-1]
                delta, duration = b["orders"] - a["orders"], b["at"] - a["at"]
                result[name] = {
                    "observed_seconds": duration,
                    "net_order_delta": delta,
                    "observed_reservations_per_second": delta / duration
                    if pure_reservations
                    else None,
                }
        result["marker_visibility_seconds"] = percentiles(
            [x["seconds"] for x in self.markers if x["phase"] == phase]
        )
        return result


def prepare_files(output, source, passwords):
    pgpass = output / "pgpass"
    pgpass.write_text(
        f"{source['host']}:5432:foxtrot:foxtrot_writer:{passwords['writer_password']}\n"
    )
    pgpass.chmod(0o600)
    work = output / "sql"
    work.mkdir()
    for name in ["reserve", "read", "abort", "cancel"]:
        text = (ROOT / "scripts/workloads" / (name + ".sql")).read_text()
        if name == "cancel":
            text = text.replace("random(1, 1000000)", "random(1, :order_upper)")
        (work / (name + ".sql")).write_text(text)
    reserve = (work / "reserve.sql").read_text()
    (work / "atomic.sql").write_text(reserve.replace("BEGIN;\n", "").replace("COMMIT;\n", ""))
    (work / "hotspot.sql").write_text(
        reserve.replace("\\set sku random(1, 1024)\n", "").replace("'sku-' || :sku", "'chiikawa'")
    )


def phase(source, target, output, observer, label, script, clients, seconds, rate):
    a = business(source)
    b = business(target)
    assert a["orders"] == b["orders"] and a["stock"] == b["stock"]
    with connect(source) as conn:
        upper = conn.execute("SELECT last_value FROM orders_id_seq").fetchone()[0]
    name = "foxtrot-dms-bench-" + uuid4().hex[:10]
    command = [
        "docker",
        "run",
        "--rm",
        "--name",
        name,
        "--label",
        "foxtrot.task=dms-performance",
        "--cpus=2",
        "--memory=512m",
        "--entrypoint=pgbench",
        "-v",
        str(output) + ":/out",
        "-v",
        str(RUNTIME / "rds-ca.pem") + ":/run/rds-ca.pem:ro",
    ]
    for key, value in {
        "PGPASSFILE": "/out/pgpass",
        "PGSSLMODE": "verify-full",
        "PGSSLROOTCERT": "/run/rds-ca.pem",
        "PGCONNECT_TIMEOUT": "5",
        "PGOPTIONS": "-c statement_timeout=2000 -c lock_timeout=1500",
        "PGAPPNAME": "foxtrot-dms-pgbench-" + label,
    }.items():
        command += ["-e", key + "=" + value]
    command += [
        "postgres:18-alpine",
        "-h",
        source["host"],
        "-p",
        "5432",
        "-U",
        "foxtrot_writer",
        "-n",
        "-M",
        "prepared",
        "-c",
        str(clients),
        "-j",
        "4",
        "-T",
        str(seconds),
        "-L",
        "1000",
        "-P",
        "5",
        "-r",
        "-l",
        "--sampling-rate=0.1",
        "--log-prefix=/out/" + label,
        "-D",
        "order_upper=" + str(upper),
    ]
    scripts = ["reserve@40", "read@30", "cancel@20", "abort@10"] if script == "mixed" else [script]
    for item in scripts:
        filename, _, weight = item.partition("@")
        command += ["-f", "/out/sql/" + filename + ".sql" + ("@" + weight if weight else "")]
    if rate:
        command += ["-R", str(rate)]
    command += ["foxtrot"]
    observer.phase = label
    started, utc = time.monotonic(), datetime.now(UTC).isoformat()
    emit(phase=label, event="start", offered_tps=rate, clients=clients, seconds=seconds)
    stats = None
    with (output / (label + ".txt")).open("w") as log:
        process = subprocess.Popen(
            command, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT
        )
        try:
            while process.poll() is None:
                if observer.abort.is_set() or time.monotonic() - started > seconds + 20:
                    raise RuntimeError("Load stopped by the benchmark limit")
                if stats is None and time.monotonic() - started > seconds / 2:
                    measured = subprocess.run(
                        ["docker", "stats", "--no-stream", "--format", "{{json .}}", name],
                        text=True,
                        capture_output=True,
                        timeout=5,
                    )
                    stats = measured.stdout.strip()
                time.sleep(0.25)
        finally:
            if process.poll() is None:
                subprocess.run(["docker", "stop", "-t", "3", name], capture_output=True)
                process.wait(timeout=10)
    elapsed = time.monotonic() - started
    observer.phase = "catchup:" + label
    parsed = parse_pgbench((output / (label + ".txt")).read_text())
    source_end = business(source)
    proof = catchup(source, target, "benchmark-end:" + label)
    target_end = business(target)
    assert (
        source_end["orders"] == target_end["orders"] and source_end["stock"] == target_end["stock"]
    )
    pure = script != "mixed"
    if pure and process.returncode == 0:
        assert source_end["orders"] - a["orders"] == parsed["processed"], (
            "Client/server committed counts differ"
        )
    result = {
        "phase": label,
        "script": script,
        "clients": clients,
        "seconds": seconds,
        "offered_tps": rate,
        "started_at_utc": utc,
        "wall_seconds": elapsed,
        "exit_code": process.returncode,
        "pgbench": parsed,
        "source_before": a,
        "source_after": source_end,
        "target_after": target_end,
        "catchup": proof,
        "observed_apply": observer.applied(label, pure),
        "latency": log_latencies(output.glob(label + ".[0-9]*"), bool(rate)),
        "generator_docker_stats": stats,
    }
    save(output / (label + "-result.json"), result)
    emit(
        phase=label,
        event="end",
        pgbench=parsed,
        catchup_seconds=proof["seconds"],
        observed_apply=result["observed_apply"],
    )
    if process.returncode:
        raise RuntimeError(
            "pgbench failed; data catchup was checked but this load result is incomplete"
        )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    profile = parser.add_mutually_exclusive_group()
    profile.add_argument("--smoke", action="store_true", help="Run one short low-load phase")
    profile.add_argument(
        "--followup", action="store_true", help="Run bounded mixed-load and recovery checks"
    )
    args = parser.parse_args()
    source, target, passwords = connections()
    job = DmsClient(JobBinding.model_validate_json((RUNTIME / "job.json").read_text()))
    assert job.observe()["phase"] == "CDC", "DMS must already be running CDC"
    output = (
        ROOT
        / "runtime/dms-performance"
        / (datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6])
    )
    output.mkdir(parents=True, mode=0o700)
    emit(output=str(output))
    prepare_files(output, source, passwords)
    # New data only: no table/sequence/replication configuration changes or data reset.
    with connect(source) as conn, conn.transaction():
        conn.execute(
            "INSERT INTO inventory(sku,stock) SELECT 'sku-'||g,100000 "
            "FROM generate_series(1,1024) g ON CONFLICT DO NOTHING"
        )
    catchup(source, target, "benchmark-setup")
    baseline = conserved_by_sku(source)
    assert baseline == conserved_by_sku(target)
    before = full_snapshot(source)
    assert before == full_snapshot(target)
    if args.smoke:
        phases = [("smoke", "atomic", 4, 8, 20)]
    elif args.followup:
        phases = [
            ("mixed-bounded", "mixed", 16, 60, 1000),
            ("recovery", "atomic", 16, 30, 100),
        ]
    else:
        phases = [
            ("hotspot", "hotspot", 16, 30, None),
            ("distributed", "reserve", 32, 45, None),
            ("rate-100", "atomic", 32, 45, 100),
            ("rate-500", "atomic", 32, 45, 500),
            ("rate-1000", "atomic", 32, 45, 1000),
            ("rate-5000", "atomic", 32, 45, 5000),
            ("rate-100000", "atomic", 32, 15, 100000),
            ("unlimited", "atomic", 48, 60, None),
            ("mixed", "mixed", 32, 60, None),
            ("recovery", "atomic", 16, 30, 100),
        ]
    results = []
    try:
        with Observer(source, target, output) as observer:
            for spec in phases:
                results.append(phase(source, target, output, observer, *spec))
                save(output / "progress.json", results)
            if observer.errors:
                raise RuntimeError("Observer failed")
        final_catchup = catchup(source, target, "benchmark-final")
        first, second = full_snapshot(source), full_snapshot(target)
        assert first == second, "Final full table digests differ"
        assert baseline == conserved_by_sku(source) == conserved_by_sku(target), (
            "SKU conservation failed"
        )
        summary = {
            "result": "PASS",
            "completed_at_utc": datetime.now(UTC).isoformat(),
            "phases": results,
            "initial_data": before,
            "final_data": second,
            "final_catchup": final_catchup,
            "job": job.observe(),
            "counts_unit": "DB transactions per second; not HTTP RPS",
            "replication_unit": "committed reservation inserts/s in pure reservation phases",
            "notes": [
                "Short burst benchmark from a macOS Docker client over the internet",
                "Includes observer overhead; hardware and DB settings were not changed",
                "Mixed TPS includes reads, cancellations and explicit rollbacks",
                "Marker delay proves visibility, not replica disk durability",
                "Sequences, promotion and application cutover are outside this benchmark",
            ],
        }
        save(output / "summary.json", summary)
        emit(result="PASS", output=str(output), final_orders=second["public.orders"]["rows"])
    except BaseException as exc:
        save(
            output / "failure.json",
            {
                "error": type(exc).__name__,
                "phases_completed": len(results),
                "message": str(exc) if isinstance(exc, (RuntimeError, AssertionError)) else None,
            },
        )
        raise
    finally:
        (output / "pgpass").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
