"""Create a resource-limited load lab without changing the normal integration lab."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path
from uuid import uuid4

import lab

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = Path(os.environ.get("DB_OPERATOR_LOAD_RUNTIME", ROOT / "runtime/load-test")).resolve()
if not RUNTIME.is_relative_to(ROOT / "runtime"):
    raise ValueError("Load lab files must stay inside this repository's ignored runtime directory")


def compose(*args: str, **kwargs):
    cfg = json.loads((RUNTIME / "config.json").read_text())
    return subprocess.run(
        ["docker", "compose", "-p", cfg["project"], "-f", str(RUNTIME / "compose.json"), *args],
        env=lab.environment(cfg),
        check=True,
        **kwargs,
    )


def up():
    lab.RUNTIME = RUNTIME
    cfg = lab.configure()
    if "project" not in cfg:
        cfg["project"] = "foxtrot-db-load-" + uuid4().hex[:8]
        (RUNTIME / "config.json").write_text(json.dumps(cfg))
    lab.setup(cfg)
    ini = RUNTIME / "pooler/pgbouncer.ini"
    ini.write_text(
        ini.read_text()
        .replace("max_client_conn=200", "max_client_conn=512")
        .replace("default_pool_size=20", "default_pool_size=64")
    )
    services = {}
    for node in ["source", "target"]:
        services[node] = {
            "image": "postgres:18-alpine",
            "environment": {
                "POSTGRES_DB": "appdb",
                "POSTGRES_PASSWORD": "${LAB_ADMIN_PASSWORD}",
            },
            "ports": [f"127.0.0.1:${{LAB_{node.upper()}_PORT}}:5432"],
            "volumes": [f"{node}:/var/lib/postgresql"],
            "cpus": 2,
            "mem_limit": "1536m",
            "healthcheck": {
                "test": ["CMD-SHELL", "pg_isready -U postgres"],
                "interval": "1s",
                "timeout": "3s",
                "retries": 30,
            },
        }
        services[node]["command"] = [
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=32",
            "-c",
            "max_wal_senders=32",
            "-c",
            "max_logical_replication_workers=8",
            "-c",
            "max_worker_processes=16",
            "-c",
            "max_connections=200",
            "-c",
            "max_slot_wal_keep_size=2GB",
            "-c",
            "shared_buffers=256MB",
            "-c",
            f"cluster_name=load-{node}",
        ]
    services["dynamodb"] = {
        "image": "amazon/dynamodb-local:latest",
        "user": "0:0",
        "command": [
            "-jar",
            "DynamoDBLocal.jar",
            "-sharedDb",
            "-dbPath",
            "/home/dynamodblocal/data",
        ],
        "ports": ["127.0.0.1:${LAB_DYNAMODB_PORT}:8000"],
        "volumes": ["dynamodb:/home/dynamodblocal/data"],
        "mem_limit": "512m",
    }
    services["pooler"] = {
        "image": "foxtrot-db-operator-lab-pooler:latest",
        "ports": ["127.0.0.1:${LAB_POOLER_PORT}:6432"],
        "volumes": [str(RUNTIME / "pooler") + ":/etc/pgbouncer:ro"],
        "cpus": 1,
        "mem_limit": "256m",
        "depends_on": {node: {"condition": "service_healthy"} for node in ["source", "target"]},
    }
    (RUNTIME / "pgpass").write_text(f"pooler:6432:appdb:app_writer:{cfg['writer']}\n")
    (RUNTIME / "pgpass").chmod(0o600)
    services["generator"] = {
        "image": "postgres:18-alpine",
        "command": ["sleep", "infinity"],
        "cpus": 2,
        "mem_limit": "384m",
        "volumes": [
            str(ROOT / "scripts/workloads") + ":/work:ro",
            str(RUNTIME / "pgpass") + ":/run/secrets/pgpass:ro",
        ],
        "environment": {"PGPASSFILE": "/run/secrets/pgpass"},
    }
    spec = {"services": services, "volumes": {n: {} for n in ["dynamodb", "source", "target"]}}
    (RUNTIME / "compose.json").write_text(json.dumps(spec, indent=2))
    compose("up", "-d", "--wait")
    env = lab.export_settings(cfg)
    env["DB_OPERATOR_TEST_TARGET_CONTAINER"] = cfg["project"] + "-target-1"
    (RUNTIME / "environment.json").write_text(json.dumps(env))
    lab.bootstrap(cfg, env)
    os.environ.update(env)
    from db_operator.config import Settings
    from db_operator.service import Service

    Service(Settings()).store.initialize(create=True)
    print(
        json.dumps(
            {"project": cfg["project"], "environment_file": str(RUNTIME / "environment.json")}
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["up", "stop", "status"])
    args = parser.parse_args()
    if args.command == "up":
        up()
    else:
        compose("stop" if args.command == "stop" else "ps")


if __name__ == "__main__":
    main()
