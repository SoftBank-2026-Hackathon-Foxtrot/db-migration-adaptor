"""Create a separate local fixture. Never reset existing data automatically."""

from __future__ import annotations

import argparse
import json
import os
import secrets
import socket
import subprocess
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

ROOT = Path(__file__).resolve().parents[1]
MANAGED = os.environ.get("DB_OPERATOR_LAB_PROFILE") == "managed"
RUNTIME = Path(
    os.environ.get(
        "DB_OPERATOR_LAB_DIRECTORY", ROOT / ("runtime/managed-lab" if MANAGED else "runtime/lab")
    )
)
PROJECT = os.environ.get(
    "COMPOSE_PROJECT_NAME",
    "foxtrot-db-operator-managed-lab" if MANAGED else "foxtrot-db-operator-lab",
)


def configure() -> dict:
    path = RUNTIME / "config.json"
    if path.exists():
        cfg = json.loads(path.read_text())
        if "dynamodb_port" not in cfg:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                cfg["dynamodb_port"] = sock.getsockname()[1]
            path.write_text(json.dumps(cfg))
        return cfg
    RUNTIME.mkdir(parents=True, mode=0o700, exist_ok=True)
    cfg = {
        k: secrets.token_urlsafe(30) for k in ["admin", "writer", "replication", "pooler", "token"]
    }
    for name in ["dynamodb", "source", "target", "pooler"]:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            cfg[name + "_port"] = sock.getsockname()[1]
    path.write_text(json.dumps(cfg))
    path.chmod(0o600)
    return cfg


def environment(cfg: dict) -> dict:
    env = dict(os.environ)
    env["COMPOSE_PROJECT_NAME"] = PROJECT
    env["LAB_ADMIN_PASSWORD"] = cfg["admin"]
    env["LAB_POOLER_DIRECTORY"] = str(RUNTIME / "pooler")
    for name in ["dynamodb", "source", "target", "pooler"]:
        env["LAB_" + name.upper() + "_PORT"] = str(cfg[name + "_port"])
    return env


def setup(cfg: dict) -> None:
    pool = RUNTIME / "pooler"
    pool.mkdir(exist_ok=True)
    ini = pool / "pgbouncer.ini"
    if not ini.exists():
        ini.write_text(
            "[databases]\nappdb = host=source port=5432 dbname=appdb\n\n"
            "[pgbouncer]\nlisten_addr=0.0.0.0\nlisten_port=6432\n"
            "auth_type=scram-sha-256\nauth_file=/etc/pgbouncer/userlist.txt\n"
            "admin_users=pooler_admin\npool_mode=transaction\n"
            "unix_socket_dir=/tmp\npidfile=/tmp/pgbouncer.pid\n"
            "max_client_conn=200\ndefault_pool_size=20\n"
            "query_wait_timeout=5\nignore_startup_parameters=extra_float_digits,options\n"
        )
        (pool / "userlist.txt").write_text(
            f'"app_writer" "{cfg["writer"]}"\n"pooler_admin" "{cfg["pooler"]}"\n'
        )
        ini.chmod(0o644)
        (pool / "userlist.txt").chmod(0o644)
    if MANAGED:
        setup_managed()


def setup_managed() -> None:
    """관리형 엔진의 로컬 대역은 PostgreSQL 17과 실제 TLS를 사용합니다."""
    tls = RUNTIME / "tls"
    tls.mkdir(exist_ok=True)
    certificate, key = tls / "server.crt", tls / "server.key"
    valid = certificate.exists() and key.exists()
    if valid:
        valid = (
            subprocess.run(
                ["openssl", "x509", "-checkend", "3600", "-noout", "-in", str(certificate)],
                capture_output=True,
                check=False,
            ).returncode
            == 0
        )
    if not valid:
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(certificate),
                "-days",
                "7",
                "-subj",
                "/CN=127.0.0.1",
                "-addext",
                "subjectAltName=IP:127.0.0.1,DNS:source,DNS:target",
            ],
            check=True,
            capture_output=True,
        )
        key.chmod(0o600)
    (RUNTIME / "Dockerfile.postgres17").write_text(
        "FROM postgres:17-alpine\n"
        "COPY tls/server.crt tls/server.key /etc/foxtrot-test/\n"
        "RUN chown postgres:postgres /etc/foxtrot-test/server.* "
        "&& chmod 0600 /etc/foxtrot-test/server.key\n"
    )
    services = {}
    for node in ["source", "target"]:
        services[node] = {
            "image": PROJECT + "-postgres17:local",
            "build": {"context": str(RUNTIME.resolve()), "dockerfile": "Dockerfile.postgres17"},
            "command": [
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=10",
                "-c",
                "max_wal_senders=10",
                "-c",
                "cluster_name=operator-" + node,
                "-c",
                "ssl=on",
                "-c",
                "ssl_cert_file=/etc/foxtrot-test/server.crt",
                "-c",
                "ssl_key_file=/etc/foxtrot-test/server.key",
            ],
        }
    (RUNTIME / "compose.override.json").write_text(json.dumps({"services": services}))


def export_settings(cfg: dict) -> dict:
    def dsn(node, user, password, database="appdb", host="127.0.0.1", port=None):
        return make_conninfo(
            host=host,
            port=port or cfg[node + "_port"],
            dbname=database,
            user=user,
            password=password,
            sslmode="disable",
        )

    env = {
        "DB_OPERATOR_STATE_TABLE": "foxtrot-db-operator-lab",
        "DB_OPERATOR_STATE_NAMESPACE": "lab",
        "DB_OPERATOR_DYNAMODB_ENDPOINT": f"http://127.0.0.1:{cfg['dynamodb_port']}",
        "AWS_DEFAULT_REGION": "ap-northeast-2",
        "AWS_ACCESS_KEY_ID": "local",
        "AWS_SECRET_ACCESS_KEY": "local",
        "DB_OPERATOR_API_TOKEN": cfg["token"],
        "DB_OPERATOR_REGISTRY": str(RUNTIME / "registry.json"),
        "DB_OPERATOR_RETENTION_SECONDS": "0",  # Isolated tests only; production default is 3600.
        "LAB_SOURCE_ADMIN": dsn("source", "postgres", cfg["admin"]),
        "LAB_TARGET_ADMIN": dsn("target", "postgres", cfg["admin"]),
        "LAB_SOURCE_REPL": dsn(
            "source", "replicator", cfg["replication"], host="source", port=5432
        ),
        "LAB_TARGET_REPL": dsn(
            "target", "replicator", cfg["replication"], host="target", port=5432
        ),
        "LAB_POOLER_ADMIN": dsn("pooler", "pooler_admin", cfg["pooler"], "pgbouncer"),
        "LAB_POOLER_APP": dsn("pooler", "app_writer", cfg["writer"]),
        "DB_OPERATOR_TEST_TARGET_CONTAINER": PROJECT + "-target-1",
    }
    registry = {
        "databases": {},
        "bindings": {
            "binding:appdb": {
                "admin_env": "LAB_POOLER_ADMIN",
                "probe_env": "LAB_POOLER_APP",
                "config_path": str(RUNTIME / "pooler/pgbouncer.ini"),
                "alias": "appdb",
            }
        },
        "workloads": {"workload:lab": {"participants": ["lab-app", "lab-worker"]}},
        "tables": ["public.inventory", "public.orders"],
        "sequences": ["public.orders_id_seq"],
    }
    for node in ["source", "target"]:
        registry["databases"]["resource:" + node] = {
            "identity": "operator-lab-" + node,
            "admin_env": "LAB_" + node.upper() + "_ADMIN",
            "replication_env": "LAB_" + node.upper() + "_REPL",
            "writer_role": "app_writer",
            "route_host": node,
            "route_port": 5432,
            "database": "appdb",
        }
    (RUNTIME / "registry.json").write_text(json.dumps(registry, indent=2))
    (RUNTIME / "environment.json").write_text(json.dumps(env))
    (RUNTIME / "environment.json").chmod(0o600)
    return env


def bootstrap(cfg: dict, env: dict) -> None:
    marker = RUNTIME / "bootstrapped"
    if marker.exists():
        return
    for node in ["source", "target"]:
        with psycopg.connect(env["LAB_" + node.upper() + "_ADMIN"], autocommit=True) as conn:
            conn.execute("CREATE ROLE app_owner NOLOGIN")
            conn.execute(
                sql.SQL("CREATE ROLE app_writer {} PASSWORD {}").format(
                    sql.SQL("LOGIN" if node == "source" else "NOLOGIN"), sql.Literal(cfg["writer"])
                )
            )
            conn.execute(
                sql.SQL("CREATE ROLE replicator LOGIN REPLICATION PASSWORD {}").format(
                    sql.Literal(cfg["replication"])
                )
            )
            conn.execute(
                "CREATE TABLE inventory(sku text PRIMARY KEY,stock integer NOT NULL "
                "CHECK(stock>=0));CREATE TABLE orders(id bigint GENERATED BY DEFAULT AS IDENTITY "
                "PRIMARY KEY,request_id uuid NOT NULL UNIQUE,sku text NOT NULL REFERENCES "
                "inventory(sku));ALTER TABLE inventory OWNER TO app_owner;ALTER TABLE orders "
                "OWNER TO app_owner;GRANT USAGE ON SCHEMA public TO app_writer,replicator;GRANT "
                "SELECT,INSERT,UPDATE,DELETE ON inventory,orders TO app_writer;GRANT SELECT ON "
                "inventory,orders TO replicator;GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA "
                "public TO app_writer"
            )
            if node == "source":
                conn.execute("INSERT INTO inventory VALUES('chiikawa',1000)")
    marker.write_text("owned isolated operator fixture\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["up", "stop", "status"])
    args = parser.parse_args()
    cfg = configure()
    setup(cfg)
    compose = ["docker", "compose", "-f", str(ROOT / "compose.yaml")]
    if MANAGED:
        compose.extend(["-f", str(RUNTIME / "compose.override.json")])
    if args.command == "up":
        subprocess.run(
            [*compose, "up", "-d", "--build", "--wait"], cwd=ROOT, env=environment(cfg), check=True
        )
        env = export_settings(cfg)
        bootstrap(cfg, env)
        os.environ.update(env)
        from db_operator.config import Settings
        from db_operator.service import Service

        Service(Settings()).store.initialize(create=True)
        print(f"Isolated operator lab is ready. Settings: {RUNTIME / 'environment.json'}")
    else:
        subprocess.run(
            [*compose, "stop" if args.command == "stop" else "ps"],
            cwd=ROOT,
            env=environment(cfg),
            check=True,
        )


if __name__ == "__main__":
    main()
