"""Measure offered/attained DB transaction rates, full copy, cutover and reverse rollback.

Run only against scripts/load_lab.py's private, single-use appdb. No HTTP throughput
claim is made. Reservation, read, cancel and explicit-abort scripts have different
business meanings even though pgbench counts each script execution as one transaction.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import psycopg
from load_lab import RUNTIME

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
from test_operator_integration import Harness  # noqa: E402

from db_operator.config import Settings  # noqa: E402
from db_operator.control import actor_ack  # noqa: E402
from db_operator.store import now  # noqa: E402


def parse_pgbench(text: str) -> dict:
    patterns = {
        "processed": r"^number of transactions actually processed: (\d+)",
        "failed": r"^number of failed transactions: (\d+)",
        "skipped": r"^number of transactions skipped: (\d+)",
        "late": r"^number of transactions above the .*: (\d+)",
        "latency_average_ms": r"^latency average = ([\d.]+)",
        "latency_stddev_ms": r"^latency stddev = ([\d.]+)",
        "schedule_lag_average_ms": r"^(?:rate limit schedule )?lag: avg ([\d.]+)",
        "schedule_lag_max_ms": r"^(?:rate limit schedule )?lag: avg [\d.]+ .*max ([\d.]+)",
        "tps": r"^tps = ([\d.]+)",
    }
    result = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, text, re.MULTILINE)
        if match:
            result[key] = float(match[1]) if "ms" in key or key == "tps" else int(match[1])
    return result


class ReplicationMonitor:
    """Read publisher WAL backlog and actual table-copy states; never estimate HTTP RPS."""

    def __init__(self, environment, output):
        self.environment, self.output = environment, output
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.samples = []
        self.failure = None

    def run(self):
        connections = {}
        try:
            for node in ["SOURCE", "TARGET"]:
                connections[node] = psycopg.connect(
                    self.environment[f"LAB_{node}_ADMIN"],
                    autocommit=True,
                    connect_timeout=3,
                    options="-c statement_timeout=2000",
                )
            while not self.stop.is_set():
                sample = {"at": datetime.now(UTC).isoformat(), "nodes": {}}
                for node, conn in connections.items():
                    try:
                        slots = conn.execute(
                            "SELECT slot_name,active,"
                            "pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint,"
                            "pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)::bigint "
                            "FROM pg_replication_slots WHERE database=current_database()"
                        ).fetchall()
                        states = conn.execute(
                            "SELECT srsubstate,count(*) FROM pg_subscription_rel GROUP BY 1"
                        ).fetchall()
                        sample["nodes"][node] = {
                            "slots": [
                                {
                                    "name": s[0],
                                    "active": s[1],
                                    "retained_wal_bytes": s[2],
                                    "unconfirmed_wal_bytes": s[3],
                                }
                                for s in slots
                            ],
                            "table_states": dict(states),
                        }
                        if any((s[2] or 0) > 1024**3 for s in slots):
                            raise RuntimeError("Test WAL backlog exceeded 1 GiB")
                    except psycopg.Error as error:
                        sample["nodes"][node] = {"error_type": type(error).__name__}
                self.samples.append(sample)
                self.stop.wait(0.5)
        except Exception as error:
            self.failure = type(error).__name__
        finally:
            for conn in connections.values():
                conn.close()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=5)
        self.output.write_text(json.dumps(self.samples, indent=2))
        assert not self.thread.is_alive() and not self.failure, self.failure


class Benchmark:
    def __init__(self, output, environment, query_mode):
        self.output, self.environment = output, environment
        self.query_mode = query_mode
        cfg = json.loads((RUNTIME / "config.json").read_text())
        assert cfg["project"].startswith("foxtrot-db-load-")
        self.project = cfg["project"]
        self.command = [
            "docker",
            "compose",
            "-p",
            self.project,
            "-f",
            str(RUNTIME / "compose.json"),
        ]
        self.process = None
        self.writer_location = "SOURCE"
        self.results = {
            "started_at": datetime.now(UTC).isoformat(),
            "project": self.project,
            "rate_unit": "DB script transactions/second; HTTP not measured",
            "phases": [],
            "actions": [],
            "checkpoints": [],
        }

    def save(self):
        (self.output / "results.json").write_text(json.dumps(self.results, indent=2, default=str))

    def cpu_counters(self):
        result = {}
        for service in ["source", "target", "pooler", "generator"]:
            output = subprocess.check_output(
                [*self.command, "exec", "-T", service, "cat", "/sys/fs/cgroup/cpu.stat"], text=True
            )
            result[service] = {
                key: int(value) for key, value in (line.split() for line in output.splitlines())
            }
        return result

    def business_counts(self):
        with psycopg.connect(self.environment[f"LAB_{self.writer_location}_ADMIN"]) as conn:
            orders, stock = conn.execute(
                "SELECT (SELECT count(*) FROM orders),(SELECT sum(stock) FROM inventory)"
            ).fetchone()
        assert orders + stock == 1024 * 1000000
        return {"orders": orders, "stock": stock}

    def start(self, label, rate, seconds, mixed=False):
        assert self.process is None
        self.cpu_before = self.cpu_counters()
        args = [
            *self.command,
            "exec",
            "-T",
            "generator",
            "pgbench",
            "-h",
            "pooler",
            "-p",
            "6432",
            "-U",
            "app_writer",
            "-d",
            "appdb",
            "-n",
            "-c",
            "128",
            "-j",
            "4",
            "-M",
            self.query_mode,
            "-T",
            str(seconds),
            "-R",
            str(rate),
            "-L",
            "1000",
            "--max-tries=1",
            "--failures-detailed",
            "-P",
            "2",
            "--progress-timestamp",
        ]
        workloads = (
            {"reserve": 35, "read": 50, "cancel": 10, "abort": 5} if mixed else {"reserve": 1}
        )
        for script, weight in workloads.items():
            args.extend(["-f", f"/work/{script}.sql@{weight}"])
        phase = {
            "label": label,
            "offered_tps": rate,
            "duration_seconds": seconds,
            "workload_weights": workloads,
            "clients": 128,
            "threads": 4,
            "query_mode": self.query_mode,
            "latency_limit_ms": 1000,
            "started_at": datetime.now(UTC).isoformat(),
            "writer_location": self.writer_location,
            "business_counts_before": self.business_counts(),
        }
        self.stream = (self.output / f"{label}.log").open("w")
        self.started = time.monotonic()
        self.process = subprocess.Popen(args, stdout=self.stream, stderr=subprocess.STDOUT)
        self.phase = phase
        print(json.dumps({"phase_started": label, "offered_tps": rate}), flush=True)

    def finish(self):
        try:
            self.process.wait(timeout=self.phase["duration_seconds"] + 20)
        except subprocess.TimeoutExpired:
            # Stop only the generator process inside this uniquely owned container.
            subprocess.run(
                [*self.command, "exec", "-T", "generator", "pkill", "-TERM", "pgbench"],
                check=False,
                capture_output=True,
            )
            self.process.wait(timeout=10)
            raise
        finally:
            self.stream.close()
        phase = self.phase
        phase.update(
            exit_code=self.process.returncode, wall_seconds=time.monotonic() - self.started
        )
        text = (self.output / f"{phase['label']}.log").read_text()
        phase.update(parse_pgbench(text))
        phase["business_counts_after"] = self.business_counts()
        if len(phase["workload_weights"]) == 1:
            extra = int(phase["label"] == "copy-and-cdc-write-100000")
            actual = (
                phase["business_counts_after"]["orders"] - phase["business_counts_before"]["orders"]
            )
            assert actual == phase["processed"] + extra, (actual, phase)
            phase["committed_reservations_verified"] = phase["processed"]
        self.process = None
        self.results["phases"].append(phase)
        after = self.cpu_counters()
        phase["cpu_counters_delta"] = {
            service: {key: value - self.cpu_before[service][key] for key, value in values.items()}
            for service, values in after.items()
        }
        self.save()
        assert phase["exit_code"] == 0 and "tps" in phase, text[-3000:]
        assert phase.get("failed", 0) == 0, phase
        print(json.dumps(phase), flush=True)
        return phase

    def action(self, h, name, **kwargs):
        started = time.monotonic()
        stop = threading.Event()
        original_held = h.held
        renewal_errors = []
        renewer = None

        def held():
            nonlocal renewer
            assert self.process is None, "Drain all generators before recording HELD"
            ref = original_held()

            def renew():
                while not stop.wait(3):
                    try:
                        h.records.renew(
                            ref,
                            [actor_ack(p, h.generation, "HELD") for p in ["lab-app", "lab-worker"]],
                        )
                    except Exception as error:
                        renewal_errors.append(type(error).__name__)
                        return

            renewer = threading.Thread(target=renew, daemon=True)
            renewer.start()
            return ref

        h.held = held
        try:
            h.action(name, **kwargs)
        finally:
            stop.set()
            if renewer:
                renewer.join(timeout=5)
            h.held = original_held
        assert not renewal_errors, renewal_errors
        action = {
            "action": name,
            "seconds": time.monotonic() - started,
            "location": kwargs.get("location"),
            "lifecycle": h.state()["lifecycle"],
        }
        self.results["actions"].append(action)
        self.save()
        print(json.dumps(action), flush=True)

    def checkpoint(self, h, label):
        source, target, _ = h.service.driver.resources(h.service.store.get(h.mid)["plan"])
        checkpoint = {"label": label, "databases": {}}
        for node, db in [("SOURCE", source), ("TARGET", target)]:
            with h.service.driver.connect(db) as conn:
                data = conn.execute(
                    "SELECT (SELECT count(*) FROM orders) AS orders,"
                    "(SELECT count(DISTINCT request_id) FROM orders) AS unique_requests,"
                    "(SELECT min(stock) FROM inventory) AS minimum_stock,"
                    "(SELECT sum(stock) FROM inventory) AS stock,"
                    "(SELECT count(*) FROM inventory i LEFT JOIN "
                    "(SELECT sku,count(*) AS n FROM orders GROUP BY sku) o USING(sku) "
                    "WHERE i.stock+coalesce(o.n,0) != 1000000) AS bad_skus,"
                    "(SELECT count(*) FROM orders o LEFT JOIN inventory i USING(sku) "
                    "WHERE i.sku IS NULL) AS orphan_orders,"
                    "(SELECT last_value FROM orders_id_seq) AS sequence,"
                    "(SELECT max(id) FROM orders) AS max_order_id"
                ).fetchone()
                assert data["orders"] == data["unique_requests"]
                assert data["minimum_stock"] >= 0
                assert data["orders"] + data["stock"] == 1024 * 1000000
                assert data["bad_skus"] == 0
                assert data["orphan_orders"] == 0
                assert data["sequence"] >= data["max_order_id"]
                checkpoint["databases"][node] = data
        checkpoint["digest"] = h.service.driver.equal(source, target)
        assert checkpoint["digest"]
        self.results["checkpoints"].append(checkpoint)
        self.save()
        print(
            json.dumps(
                {
                    "checkpoint": label,
                    "orders": checkpoint["databases"]["SOURCE"]["orders"],
                    "bad_skus": 0,
                    "equal": True,
                }
            ),
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed-rows", type=int, default=1000000)
    parser.add_argument("--seconds", type=int, default=12)
    parser.add_argument(
        "--query-mode", choices=["simple", "extended", "prepared"], default="extended"
    )
    parser.add_argument("--prepared-migration", help="Resume a test interrupted before START_SYNC")
    args = parser.parse_args()
    assert 10000 <= args.seed_rows <= 2000000 and 5 <= args.seconds <= 60
    environment = json.loads((RUNTIME / "environment.json").read_text())
    os.environ.update(environment)
    output = RUNTIME / ("run-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    output.mkdir(mode=0o700)
    bench = Benchmark(output, environment, args.query_mode)
    bench.results["seed_rows"] = args.seed_rows
    resources = RUNTIME / "resources.json"
    if resources.exists():
        bench.results["containers"] = json.loads(resources.read_text())
    bench.results["pgbench_version"] = subprocess.check_output(
        [*bench.command, "exec", "-T", "generator", "pgbench", "--version"], text=True
    ).strip()
    bench.results["base_commit"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
    ).strip()
    bench.results["tested_worktree_dirty"] = bool(
        subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
    )
    h = Harness(Settings(observation_cache_seconds=0))
    if args.prepared_migration:
        h.mid = args.prepared_migration
        assert h.state()["lifecycle"] == "PREPARED" and h.state()["health"] == "OK"
    else:
        seed(h, bench, environment, args.seed_rows)
    bench.results["migration_id"] = h.mid
    bench.results["initial_business_counts"] = bench.business_counts()
    bench.save()
    run_scenarios(h, bench, environment, output, args.seconds)


def seed(h, bench, environment, seed_rows):
    with psycopg.connect(environment["LAB_SOURCE_ADMIN"]) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 0, "Single-use appdb"
        conn.execute("DELETE FROM inventory")
        conn.execute(
            "INSERT INTO inventory SELECT 'sku-'||i,1000000 FROM generate_series(1,1024) i"
        )
        conn.execute(
            "INSERT INTO orders(request_id,sku) SELECT gen_random_uuid(),'sku-'||"
            "(1+mod(i-1,1024)) FROM generate_series(1,%s) i",
            (seed_rows,),
        )
        conn.execute(
            "UPDATE inventory i SET stock=stock-o.n FROM "
            "(SELECT sku,count(*) AS n FROM orders GROUP BY sku) o WHERE i.sku=o.sku"
        )
    h.create()
    bench.action(h, "PREPARE")


def run_scenarios(h, bench, environment, output, seconds):
    with ReplicationMonitor(environment, output / "replication.json"):
        bench.start("warmup-write-1000", 1000, 5)
        bench.finish()
        # Baseline means no publication/subscription yet, with identical DB/pool limits.
        for rate in [1000, 10000, 100000]:
            bench.start(f"baseline-write-{rate}", rate, seconds)
            bench.finish()
        bench.start("copy-and-cdc-write-100000", 100000, seconds)
        time.sleep(1)
        started = time.monotonic()
        bench.action(h, "START_SYNC")
        long_id = uuid4()
        long_started = time.monotonic()
        with psycopg.connect(environment["LAB_POOLER_APP"]) as conn:
            conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku='sku-1'")
            conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'sku-1')", (long_id,))
            # Slot snapshot creation may wait for old open transactions. Do not wait
            # for READY while keeping that transaction open: drain it, then await copy.
            time.sleep(3)
            bench.results["lifecycle_during_long_transaction"] = h.state()["lifecycle"]
            with psycopg.connect(environment["LAB_TARGET_ADMIN"]) as target:
                assert (
                    target.execute(
                        "SELECT count(*) FROM orders WHERE request_id=%s", (long_id,)
                    ).fetchone()[0]
                    == 0
                )
            bench.results["uncommitted_long_transaction_hidden"] = True
        bench.results["long_transaction_seconds"] = time.monotonic() - long_started
        deadline = time.monotonic() + 120
        while h.state()["lifecycle"] != "READY":
            assert time.monotonic() < deadline, "Initial copy timeout"
            time.sleep(0.2)
        bench.results["copy_ready_seconds"] = time.monotonic() - started
        bench.finish()  # pgbench has exited: all generators are drained before HELD.
        for rate in [1000, 10000, 100000]:
            bench.start(f"cdc-write-{rate}", rate, seconds)
            bench.finish()
        bench.start("cdc-mixed-100000", 100000, seconds, mixed=True)
        bench.finish()
        h.held_since = now()
        paused = time.monotonic()
        bench.action(h, "QUIESCE")
        bench.checkpoint(h, "forward-drained")
        bench.action(h, "CUTOVER")
        bench.action(h, "RESUME", location="TARGET")
        bench.writer_location = "TARGET"
        bench.results["forward_write_pause_seconds_including_extra_test_checks"] = (
            time.monotonic() - paused
        )
        # Probe once through the actual application endpoint before releasing HELD evidence.
        with psycopg.connect(environment["LAB_POOLER_APP"]) as conn:
            conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku='sku-1'")
            target_probe_id = conn.execute(
                "INSERT INTO orders(request_id,sku) VALUES(%s,'sku-1') RETURNING id", (uuid4(),)
            ).fetchone()[0]
        bench.results["target_business_probe"] = {"passed": True, "order_id": target_probe_id}
        h.running()
        bench.start("reverse-cdc-write-100000", 100000, seconds)
        bench.finish()
        bench.start("reverse-cdc-mixed-100000", 100000, seconds, mixed=True)
        bench.finish()
        h.generation += 1
        h.held_since = now()
        paused = time.monotonic()
        bench.action(h, "QUIESCE")
        bench.checkpoint(h, "reverse-drained")
        bench.action(h, "ROLLBACK")
        bench.action(h, "RESUME", location="SOURCE")
        bench.writer_location = "SOURCE"
        bench.results["rollback_write_pause_seconds_including_extra_test_checks"] = (
            time.monotonic() - paused
        )
        # Final source write must use a sequence value beyond the target's previous range.
        with psycopg.connect(environment["LAB_POOLER_APP"]) as conn:
            conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku='sku-1'")
            new_id = conn.execute(
                "INSERT INTO orders(request_id,sku) VALUES(%s,'sku-1') RETURNING id", (uuid4(),)
            ).fetchone()[0]
        prior = bench.results["checkpoints"][-1]["databases"]["TARGET"]["max_order_id"]
        assert new_id > prior
        bench.results["source_post_rollback_write"] = {"passed": True, "new_id": new_id}
        h.running()
        bench.action(h, "FINALIZE")
        bench.results["finished_at"] = datetime.now(UTC).isoformat()
        bench.results["status"] = "PASS"
        bench.save()
    print(json.dumps({"results": str(output / "results.json"), "status": "PASS"}), flush=True)


if __name__ == "__main__":
    main()
