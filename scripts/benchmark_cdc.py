"""Compare native subscriber commit modes in disposable, resource-limited labs.

Each transaction inserts one order and updates one inventory row. Subscriber
order-count deltas therefore measure committed reservation transactions actually
applied, rather than publisher throughput or an inferred WAL-byte conversion.
This script changes subscriptions in the test lab only; operator defaults stay
unchanged. Results include count-query overhead and local Docker networking.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import load_lab
import psycopg
from load_migration import Benchmark, Harness, Settings, seed
from psycopg import sql

ROOT = Path(__file__).resolve().parents[1]


def emit(**values):
    print(json.dumps(values, default=str), flush=True)


def business_state(environment, node):
    with psycopg.connect(
        environment[f"LAB_{node}_ADMIN"],
        autocommit=True,
        connect_timeout=2,
        options="-c statement_timeout=3000",
    ) as conn:
        orders, stock = conn.execute(
            "SELECT (SELECT count(*) FROM orders),(SELECT sum(stock) FROM inventory)"
        ).fetchone()
    return {"orders": orders, "stock": stock, "at_monotonic": time.monotonic()}


class ApplyMonitor:
    """Observe committed rows, publisher backlog and target wait events once/second."""

    def __init__(self, environment, output):
        self.environment, self.output = environment, output
        self.stop = threading.Event()
        self.samples, self.errors = [], []
        self.thread = threading.Thread(target=self.run, daemon=True)

    def sample(self):
        sample = {"at_monotonic": time.monotonic(), "nodes": {}}
        for node in ["SOURCE", "TARGET"]:
            try:
                observed = business_state(self.environment, node)
                with psycopg.connect(
                    self.environment[f"LAB_{node}_ADMIN"],
                    autocommit=True,
                    connect_timeout=2,
                    options="-c statement_timeout=2000",
                ) as conn:
                    observed["slots"] = [
                        {"slot": name, "unconfirmed_bytes": backlog, "retained_bytes": retained}
                        for name, backlog, retained in conn.execute(
                            "SELECT slot_name,"
                            "pg_wal_lsn_diff(pg_current_wal_lsn(),confirmed_flush_lsn)::bigint,"
                            "pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn)::bigint "
                            "FROM pg_replication_slots WHERE database=current_database()"
                        ).fetchall()
                    ]
                    observed["waits"] = [
                        {"type": kind, "event": event, "workers": workers}
                        for kind, event, workers in conn.execute(
                            "SELECT a.wait_event_type,a.wait_event,count(*) "
                            "FROM pg_stat_activity a JOIN pg_stat_subscription s ON s.pid=a.pid "
                            "GROUP BY a.wait_event_type,a.wait_event"
                        ).fetchall()
                    ]
                sample["nodes"][node] = observed
            except psycopg.Error as error:
                sample["nodes"][node] = {"error": type(error).__name__}
        self.samples.append(sample)

    def run(self):
        try:
            while not self.stop.is_set():
                self.sample()
                self.stop.wait(1)
        except Exception as error:
            self.errors.append(type(error).__name__)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join(timeout=8)
        self.output.write_text(json.dumps(self.samples, indent=2))
        assert not self.thread.is_alive() and not self.errors, self.errors

    def interval(self, started, ended):
        values = {}
        for node in ["SOURCE", "TARGET"]:
            samples = [
                s["nodes"][node]
                for s in self.samples
                if "orders" in s["nodes"].get(node, {})
                and started <= s["nodes"][node]["at_monotonic"] <= ended
            ]
            if len(samples) < 2:
                values[node] = {"samples": len(samples)}
                continue
            first, last = samples[0], samples[-1]
            elapsed = last["at_monotonic"] - first["at_monotonic"]
            delta = last["orders"] - first["orders"]
            values[node] = {
                "samples": len(samples),
                "sampled_seconds": elapsed,
                "committed_reservations": delta,
                "committed_reservations_per_second": delta / elapsed,
                "business_row_changes_per_second": 2 * delta / elapsed,
            }
        return values


def set_commit_mode(harness, mode):
    """Use PostgreSQL's existing ALTER SUBSCRIPTION; do not replace the CDC engine."""
    original = harness.service.driver.create_replication
    applied = []

    def create(*args, **kwargs):
        original(*args, **kwargs)
        subscriber, name = args[1], args[2]
        with harness.service.driver.connect(subscriber) as conn:
            conn.execute(
                sql.SQL("ALTER SUBSCRIPTION {} SET (synchronous_commit={})").format(
                    sql.Identifier(name), sql.Literal(mode)
                )
            )
            row = conn.execute(
                "SELECT subsynccommit FROM pg_subscription WHERE subname=%s", (name,)
            ).fetchone()
            assert row["subsynccommit"] == mode, row
        applied.append({"subscription": name, "synchronous_commit": mode})

    harness.service.driver.create_replication = create
    return applied


def durable_catchup(harness, label, direction="forward", timeout=180):
    source, target, _ = harness.service.driver.resources(
        harness.service.store.get(harness.mid)["plan"]
    )
    publisher, subscriber = (source, target) if direction == "forward" else (target, source)
    started = time.monotonic()
    proof = harness.service.driver.barrier(
        publisher,
        subscriber,
        harness.service.driver.names(harness.mid)[direction],
        harness.mid,
        "benchmark",
        lambda *_: None,
        timeout,
    )
    return {"label": label, "seconds": time.monotonic() - started, "proof": proof}


def run_phase(bench, monitor, label, rate, seconds, *, crash=False):
    bench.start(label, rate, seconds)
    started = bench.started
    crash_result = None
    if crash:
        time.sleep(4)
        before = business_state(bench.environment, "SOURCE")
        # Compose resolves only the target inside this uniquely owned project.
        subprocess.run([*bench.command, "kill", "-s", "SIGKILL", "target"], check=True)
        time.sleep(2)
        during = business_state(bench.environment, "SOURCE")
        assert during["orders"] > before["orders"], "Source writes stopped during target crash"
        subprocess.run([*bench.command, "start", "target"], check=True)
        crash_result = {
            "fault": "target PostgreSQL process/container SIGKILL; not host power loss",
            "source_reservations_while_target_down": during["orders"] - before["orders"],
        }
    bench.process.wait(timeout=seconds + 20)
    ended = time.monotonic()
    phase = bench.finish()
    phase["observed_apply"] = monitor.interval(started, ended)
    if crash_result:
        phase["crash"] = crash_result
    bench.save()
    emit(phase=label, tps=phase["tps"], applied=phase["observed_apply"].get("TARGET"))
    return phase


def trial(args):
    assert load_lab.RUNTIME.is_relative_to(ROOT / "runtime/cdc-ab"), "Use the A/B lab root"
    try:
        load_lab.up()
        environment = json.loads((load_lab.RUNTIME / "environment.json").read_text())
        cfg = json.loads((load_lab.RUNTIME / "config.json").read_text())
        assert cfg["project"].startswith("foxtrot-db-load-")
        os.environ.update(load_lab.lab.environment(cfg))
        os.environ.update(environment)
        bench = Benchmark(load_lab.RUNTIME, environment, "extended")
        bench.results.update(
            commit_mode=args.trial_mode,
            seed_rows=args.seed_rows,
            note="2 CPU/1.5 GiB per database; count-query monitoring included; not HTTP RPS",
            base_commit=subprocess.check_output(
                ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
            ).strip(),
        )
        harness = Harness(Settings(observation_cache_seconds=0))
        bench.results["configured_subscriptions"] = set_commit_mode(harness, args.trial_mode)
        seed(harness, bench, environment, args.seed_rows)
        bench.results["migration_id"] = harness.mid
        bench.action(harness, "START_SYNC")
        deadline = time.monotonic() + 90
        while harness.state()["lifecycle"] != "READY":
            assert time.monotonic() < deadline, "Initial copy timeout"
            time.sleep(0.2)
        bench.results["catchup"] = [durable_catchup(harness, "initial-copy")]
        with ApplyMonitor(environment, load_lab.RUNTIME / "apply.json") as monitor:
            for label, rate, seconds, crash in [
                ("warmup", 1000, 5, False),
                ("offered-10000", 10000, args.seconds, False),
                ("offered-100000", 100000, args.seconds, False),
                ("target-crash-under-load", 10000, 15, True),
            ]:
                run_phase(bench, monitor, label, rate, seconds, crash=crash)
                catchup = durable_catchup(harness, label)
                bench.results["catchup"].append(catchup)
                bench.save()
                emit(catchup=label, seconds=catchup["seconds"])

        # Measurements explicitly include this post-generator catch-up above.
        # Do not present that wait as live-traffic or automatic cutover behavior.
        harness.held_since = datetime.now(UTC)
        held = time.monotonic()
        bench.action(harness, "QUIESCE")
        bench.checkpoint(harness, "forward-durable-full-equality")
        bench.action(harness, "CUTOVER")
        bench.action(harness, "RESUME", location="TARGET")
        bench.writer_location = "TARGET"
        bench.results["forward_held_seconds_including_test_validation"] = time.monotonic() - held
        with ApplyMonitor(environment, load_lab.RUNTIME / "reverse-apply.json") as monitor:
            run_phase(bench, monitor, "reverse-write", 1000, 5)
        bench.results["catchup"].append(durable_catchup(harness, "reverse", "reverse"))
        harness.generation += 1
        harness.held_since = datetime.now(UTC)
        held = time.monotonic()
        bench.action(harness, "QUIESCE")
        bench.checkpoint(harness, "reverse-durable-full-equality")
        bench.action(harness, "ROLLBACK")
        bench.action(harness, "RESUME", location="SOURCE")
        bench.writer_location = "SOURCE"
        bench.results["rollback_held_seconds_including_test_validation"] = time.monotonic() - held
        harness.running()
        bench.action(harness, "FINALIZE")
        assert harness.state()["lifecycle"] == "COMPLETED"
        bench.results["status"] = "PASS"
        bench.save()
        emit(trial=args.trial_mode, status="PASS", results=str(load_lab.RUNTIME / "results.json"))
    finally:
        if (load_lab.RUNTIME / "compose.json").exists():
            load_lab.compose("stop", timeout=60)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--seed-rows", type=int, default=1_000_000)
    parser.add_argument("--trial-mode", choices=["on", "off"])
    args = parser.parse_args()
    assert 10 <= args.seconds <= 120 and 10000 <= args.seed_rows <= 1000000
    if args.trial_mode:
        trial(args)
        return
    output = (
        ROOT
        / "runtime/cdc-ab"
        / (datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:6])
    )
    output.mkdir(parents=True, mode=0o700)
    summary = {"order": ["on", "off", "off", "on"], "trials": []}
    for index, mode in enumerate(summary["order"], 1):
        path = output / f"{index}-{mode}"
        emit(starting_trial=index, mode=mode, output=str(path))
        environment = dict(os.environ, DB_OPERATOR_LOAD_RUNTIME=str(path))
        log = output / f"{index}-{mode}.log"
        with log.open("w") as stream:
            result = subprocess.run(
                [
                    sys.executable,
                    __file__,
                    "--trial-mode",
                    mode,
                    "--seconds",
                    str(args.seconds),
                    "--seed-rows",
                    str(args.seed_rows),
                ],
                env=environment,
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
        summary["trials"].append({"mode": mode, "exit_code": result.returncode, "path": str(path)})
        (output / "summary.json").write_text(json.dumps(summary, indent=2))
        assert result.returncode == 0, f"Trial {index} failed; inspect {log}"
    emit(status="PASS", summary=str(output / "summary.json"))


if __name__ == "__main__":
    main()
