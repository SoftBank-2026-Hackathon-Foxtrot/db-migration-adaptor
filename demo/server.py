"""Docker 시연 서버. cloud는 실제 DB와 선택한 복제 방식, simulation은 모의 DMS입니다."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import Request as HttpRequest
from urllib.request import urlopen
from uuid import uuid4

import psycopg
import uvicorn
from fastapi import HTTPException, Request
from fastapi.responses import FileResponse
from google.api_core import exceptions as cloud_errors
from google.cloud import clouddms_v1 as google_dms
from google.longrunning.operations_pb2 import Operation
from google.protobuf.any_pb2 import Any
from google.protobuf.empty_pb2 import Empty
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from db_operator import config, dms
from db_operator.api import create_app
from db_operator.config import Database, ResourceCatalog, Settings
from db_operator.control import ControlPlaneRecords, actor_ack
from db_operator.models import ATOMIC_LOGICAL_PROFILE, DMS_PROFILE, MANAGED_LOGICAL_PROFILES
from db_operator.postgres import Postgres
from db_operator.store import stamp
from db_operator.worker import Worker

DIRECTORY = Path("/run/foxtrot")
DIRECTORY.mkdir(exist_ok=True)
STATE_FILE = DIRECTORY / "demo.json"
CLOUD_MODE = os.environ.get("DEMO_MODE") == "cloud"
CLOUD_CONFIG = json.loads(Path(os.environ["DEMO_CLOUD_CONFIG"]).read_text()) if CLOUD_MODE else {}
MIGRATION_PROFILE = CLOUD_CONFIG.get("profile", DMS_PROFILE)
NATIVE_MODE = CLOUD_MODE and MIGRATION_PROFILE in MANAGED_LOGICAL_PROFILES
ATOMIC_MODE = MIGRATION_PROFILE == ATOMIC_LOGICAL_PROFILE
WRITER_ROLE = CLOUD_CONFIG.get("writerRole", "app_writer")
WRITER_PASSWORD = "local-demo-writer"
ADMIN_PASSWORD = "local-demo-admin"
POOLER_PASSWORD = "local-demo-pooler"
ACTORS = ["demo-web"]
COMMANDS = {
    "REGISTER",
    "CREATE",
    "PREPARE",
    "START_SYNC",
    "QUIESCE",
    "CUTOVER",
    "RESUME",
    "ROLLBACK",
    "RECONCILE",
    "FINALIZE",
    "AUTO",
    "MIGRATE_ALL",
    "ROLLBACK_ALL",
    "ARCHIVE",
    "RESET",
    "INJECT",
    "WRITE",
}


def admin_dsn(node):
    if CLOUD_MODE:
        return cloud_secret(node)["adminDsn"]
    return make_conninfo(host=node, dbname="appdb", user="postgres", password=ADMIN_PASSWORD)


@lru_cache(maxsize=2)
def cloud_secret(node):
    # 실제 Secrets Manager에서 읽습니다. 이 시연의 Secret은 재시작 때 다시 읽습니다.
    return config.read_database_secret(CLOUD_CONFIG["databases"][node]["secret_ref"])


class LocalCloud:
    """조회 시 전체 스냅샷을 복사합니다. 실제 DMS CDC·성능을 재현하지 않습니다."""

    def __init__(self, saved=None):
        parent = "projects/local-demo/locations/asia-northeast3"
        self.binding = dms.JobBinding(
            job=parent + "/migrationJobs/demo",
            source=parent + "/connectionProfiles/source",
            destination=parent + "/connectionProfiles/target",
            owner="foxtrot",
        )
        self.job = google_dms.MigrationJob(
            name=self.binding.job,
            source=self.binding.source,
            destination=self.binding.destination,
            labels={"foxtrot-owner": "foxtrot"},
            type_="CONTINUOUS",
            state="NOT_STARTED",
            source_database={"provider": "RDS", "engine": "POSTGRESQL"},
            destination_database={"provider": "CLOUDSQL", "engine": "POSTGRESQL"},
        )
        self.operations = {}
        self.deleted = False
        self.lose_response = False
        self.last_operation = None
        if saved:
            self.job.state = saved["state"]
            self.job.phase = saved["phase"]
            self.deleted = saved["deleted"]
            self.lose_response = saved["loseResponse"]
            self.last_operation = saved["lastOperation"]
            self.operations = {
                name: Operation.FromString(base64.b64decode(data))
                for name, data in saved["operations"].items()
            }
        self.lock = threading.RLock()

    def snapshot(self):
        return {
            "state": self.job.state.name,
            "phase": self.job.phase.name,
            "deleted": self.deleted,
            "loseResponse": self.lose_response,
            "lastOperation": self.last_operation,
            "operations": {
                name: base64.b64encode(op.SerializeToString()).decode()
                for name, op in self.operations.items()
            },
        }

    def copy(self):
        with (
            psycopg.connect(admin_dsn("source")) as source,
            psycopg.connect(admin_dsn("target")) as target,
        ):
            source.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            target.execute("CREATE SCHEMA IF NOT EXISTS foxtrot_migration")
            target.execute(
                "CREATE TABLE IF NOT EXISTS foxtrot_migration.barriers("
                "id uuid PRIMARY KEY,migration_id text NOT NULL)"
            )
            target.execute("TRUNCATE orders,inventory,foxtrot_migration.barriers")
            for name, columns in [
                ("inventory", "sku,stock"),
                ("orders", "id,request_id,sku"),
                ("foxtrot_migration.barriers", "id,migration_id"),
            ]:
                rows = source.execute(f"SELECT {columns} FROM {name}").fetchall()
                for row in rows:
                    placeholders = ",".join(["%s"] * len(row))
                    target.execute(f"INSERT INTO {name} VALUES({placeholders})", row)

    def get_migration_job(self, **_):
        with self.lock:
            if self.deleted:
                raise cloud_errors.NotFound("local demo job deleted")
            if self.job.state.name == "RUNNING" and self.job.phase.name == "CDC":
                self.copy()
            return deepcopy(self.job)

    def get_connection_profile(self, *, request, **_):
        if request["name"] == self.binding.source:
            return google_dms.ConnectionProfile(
                name=self.binding.source,
                postgresql={"host": "source", "port": 5432, "username": "replicator"},
            )
        return google_dms.ConnectionProfile(
            name=self.binding.destination,
            cloudsql={"cloud_sql_id": "target", "public_ip": "target"},
        )

    def get_operation(self, *, request, **_):
        return self.operations[request["name"]]

    def __getattr__(self, name):
        actions = {
            "verify_migration_job": "VERIFY",
            "start_migration_job": "START_SYNC",
            "resume_migration_job": "RESUME_SYNC",
            "promote_migration_job": "PROMOTE",
            "stop_migration_job": "STOP",
            "delete_migration_job": "DELETE",
        }
        if name not in actions:
            raise AttributeError(name)
        return lambda **_: self.mutate(actions[name])

    def mutate(self, action):
        with self.lock:
            if action in {"START_SYNC", "RESUME_SYNC", "PROMOTE"}:
                self.copy()
                self.job.state = "COMPLETED" if action == "PROMOTE" else "RUNNING"
                self.job.phase = "CDC"
            elif action == "STOP":
                self.job.state = "STOPPED"
            elif action == "DELETE":
                self.deleted = True
            metadata, response = Any(), Any()
            metadata.Pack(
                google_dms.OperationMetadata.pb(
                    google_dms.OperationMetadata(
                        target=self.binding.job,
                        verb=dms.DMS_ACTIONS[action],
                        create_time=datetime.now(UTC),
                    )
                )
            )
            response.Pack(Empty() if action == "DELETE" else google_dms.MigrationJob.pb(self.job))
            name = self.binding.job.split("/migrationJobs/")[0] + "/operations/" + uuid4().hex
            operation = Operation(name=name, metadata=metadata, response=response, done=True)
            self.operations[name] = operation
            self.last_operation = name
            lose = action == "PROMOTE" and self.lose_response
            if lose:
                self.lose_response = False
            demo.save()
            if lose:
                raise cloud_errors.DeadlineExceeded("local demo promotion response lost")
            return SimpleNamespace(operation=operation)


class Demo:
    def __init__(self):
        self.meta = (
            json.loads(STATE_FILE.read_text())
            if STATE_FILE.exists()
            else {
                "namespace": "demo-" + uuid4().hex[:12],
                "token": secrets.token_urlsafe(32),
                "bindingId": None,
                "migrationId": None,
                "transitionId": "demo-" + uuid4().hex,
                "heldSince": None,
                "generation": 1,
            }
        )
        self.cloud = (
            None
            if NATIVE_MODE
            else dms.DmsClient(dms.JobBinding.model_validate(CLOUD_CONFIG["job"]))
            if CLOUD_MODE
            else LocalCloud(self.meta.pop("cloud", None))
        )
        self.events, self.traces = [], []
        self.busy, self.error = None, None
        self.observation_error = None
        self.held = threading.Event()
        self.held.set()
        self.writer_lock = threading.Lock()
        self.command_lock = threading.Lock()
        self.gate_ref = None
        self.command_started = None
        self.state_lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.stop = threading.Event()
        self.success, self.errors, self.last_write = 0, 0, None
        self.service, self.worker = None, None
        self.view = {}
        batch = self.meta.get("batch")
        if batch and batch["status"] == "RUNNING":
            batch["status"] = "INTERRUPTED"
            self.error = "서버가 재시작됐습니다. 상태 재확인 후 다시 실행하십시오."

    def save(self):
        content = json.dumps(
            self.meta if CLOUD_MODE else {**self.meta, "cloud": self.cloud.snapshot()}
        )
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(content)
        tmp.chmod(0o600)
        tmp.replace(STATE_FILE)

    def event(self, text):
        self.events.insert(0, {"time": stamp(), "text": text})
        self.events = self.events[:60]

    def install_local_adapters(self):
        if CLOUD_MODE:
            return
        # 이 서버 프로세스에서만 로컬 Secret과 평문 Docker 전송을 허용합니다.
        # 배포 이미지의 운영 진입점과 소스 코드는 변경하지 않습니다.
        self.local_secrets = {}
        config.read_database_secret = lambda ref: self.local_secrets[ref]
        original_connect = Postgres.connect

        @contextmanager
        def local_connect(driver, database, *args, **kwargs):
            with original_connect(
                driver, database.model_copy(update={"provider": "SELF_MANAGED"}), *args, **kwargs
            ) as conn:
                yield conn

        Postgres.connect = local_connect
        original_client = dms.DmsClient
        dms.DmsClient = lambda binding: original_client(binding, client=self.cloud)

    def configure(self):
        empty = DIRECTORY / "registry.json"
        empty.write_text(json.dumps({"databases": {}, "bindings": {}, "workloads": {}}))
        os.environ["PGBOUNCER_ADMIN_DSN"] = make_conninfo(
            host="pooler",
            port=6432,
            user="pooler_admin",
            dbname="pgbouncer",
            password=POOLER_PASSWORD,
            sslmode="disable",
        )
        self.settings = Settings(
            state_table="foxtrot-dashboard",
            state_namespace=self.meta["namespace"],
            aws_region="ap-northeast-2",
            dynamodb_endpoint="http://dynamodb:8000",
            api_token=self.meta["token"],
            registry=empty,
            bindings_enabled=True,
            pooler_directory=DIRECTORY,
            public_host="127.0.0.1",
            public_port=int(os.environ.get("DEMO_POOLER_PORT", "16432")),
            binding_drain_seconds=1,
            retention_seconds=0,
        )
        if CLOUD_MODE:
            if not (DIRECTORY / "pgbouncer.ini").exists():
                self.pooler_files()
            self.save()
            return
        for node in ["source", "target"]:
            with psycopg.connect(admin_dsn(node), autocommit=True) as conn:
                for role, attributes in [
                    ("app_owner", "NOLOGIN"),
                    ("app_writer", "LOGIN" if node == "source" else "NOLOGIN"),
                    ("replicator", "LOGIN REPLICATION"),
                ]:
                    if not conn.execute(
                        "SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)
                    ).fetchone():
                        conn.execute(
                            sql.SQL("CREATE ROLE {} {} PASSWORD {}").format(
                                sql.Identifier(role),
                                sql.SQL(attributes),
                                sql.Literal(WRITER_PASSWORD),
                            )
                        )
                conn.execute(
                    "CREATE TABLE IF NOT EXISTS inventory(sku text PRIMARY KEY,stock integer "
                    "NOT NULL CHECK(stock>=0));CREATE TABLE IF NOT EXISTS orders("
                    "id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,request_id uuid "
                    "NOT NULL UNIQUE,sku text NOT NULL REFERENCES inventory(sku));"
                    "ALTER TABLE inventory OWNER TO app_owner;"
                    "ALTER TABLE orders OWNER TO app_owner;"
                    "GRANT USAGE ON SCHEMA public TO app_writer,replicator;GRANT "
                    "SELECT,INSERT,UPDATE,DELETE ON inventory,orders TO app_writer;"
                    "GRANT SELECT ON inventory,orders TO replicator;"
                    "GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO app_writer"
                )
                if node == "source":
                    conn.execute(
                        "INSERT INTO inventory VALUES('chiikawa',100000) ON CONFLICT DO NOTHING"
                    )
        if not (DIRECTORY / "pgbouncer.ini").exists():
            self.pooler_files()
        self.save()

    def pooler_files(self):
        (DIRECTORY / "pgbouncer.ini").write_text(
            "[databases]\n[pgbouncer]\nlisten_addr=0.0.0.0\nlisten_port=6432\n"
            "auth_type=hba\nauth_file=/run/foxtrot/userlist.txt\n"
            "auth_hba_file=/run/foxtrot/pg_hba.conf\nadmin_users=pooler_admin\n"
            "pool_mode=transaction\nunix_socket_dir=/tmp\npidfile=/tmp/pgbouncer.pid\n"
            "max_client_conn=100\ndefault_pool_size=10\nquery_wait_timeout=5\n"
            "ignore_startup_parameters=extra_float_digits\n"
            + (
                "server_tls_sslmode=verify-full\nserver_tls_ca_file=/cloud/ca-bundle.pem\n"
                if CLOUD_MODE
                else ""
            )
        )
        (DIRECTORY / "userlist.txt").write_text(f'"pooler_admin" "{POOLER_PASSWORD}"\n')
        (DIRECTORY / "pg_hba.conf").write_text(
            "host all pooler_admin 0.0.0.0/0 scram-sha-256\n"
            "host all all 0.0.0.0/0 reject\nhost all all ::/0 reject\n"
        )

    def publish(self):
        catalog = ResourceCatalog(self.service.store)
        if CLOUD_MODE:
            for node, value in CLOUD_CONFIG["databases"].items():
                catalog.database(
                    "db:" + node,
                    Database.model_validate(value),
                    tables=["public.inventory", "public.orders"],
                    sequences=["public.orders_id_seq"],
                )
            catalog.workload("scope:demo", ACTORS)
            if not NATIVE_MODE:
                catalog.dms_job(
                    "dms:demo",
                    source_ref="db:source",
                    target_ref="db:target",
                    binding=self.cloud.binding,
                )
            return
        for node, provider, instance in [
            ("source", "AWS_RDS", "arn:aws:rds:ap-northeast-2:000000000000:db:local-demo"),
            ("target", "GCP_CLOUDSQL", "projects/local-demo/instances/target"),
        ]:
            ref = f"arn:aws:secretsmanager:ap-northeast-2:000000000000:secret:demo-{node}"
            self.local_secrets[ref] = {
                "adminDsn": admin_dsn(node),
                "replicationDsn": make_conninfo(
                    host=node, dbname="appdb", user="replicator", password=WRITER_PASSWORD
                ),
                "writerPassword": WRITER_PASSWORD,
            }
            db = Database(
                identity=f"{provider}:{instance}/appdb",
                provider=provider,
                provider_instance=instance,
                secret_ref=ref,
                writer_role="app_writer",
                route_host=node,
                database="appdb",
            )
            catalog.database(
                "db:" + node,
                db,
                tables=["public.inventory", "public.orders"],
                sequences=["public.orders_id_seq"],
            )
        catalog.workload("scope:demo", ACTORS)
        catalog.dms_job(
            "dms:demo",
            source_ref="db:source",
            target_ref="db:target",
            binding=self.cloud.binding,
        )

    def api(self, method, path, body=None):
        headers = {"Authorization": "Bearer " + self.meta["token"]}
        if body is not None:
            headers.update({"Content-Type": "application/json", "Idempotency-Key": uuid4().hex})
        request = HttpRequest(
            "http://127.0.0.1:8080" + path,
            method=method,
            headers=headers,
            data=json.dumps(body).encode() if body is not None else None,
        )
        try:
            with urlopen(request, timeout=90) as response:
                result, code = json.load(response), response.status
        except HTTPError as error:
            result, code = json.load(error), error.code
        self.traces.insert(
            0, {"method": method, "path": path, "code": code, "request": body, "response": result}
        )
        self.traces = self.traces[:30]
        if code >= 400:
            raise RuntimeError(result.get("code", "API_ERROR") + ": " + result.get("message", ""))
        return result

    def execute(self, accepted):
        with self.worker.exclusive():
            if not self.worker.run_once():
                raise RuntimeError("접수된 작업이 없습니다.")
        result = self.api("GET", accepted["statusUrl"])
        self.event(result["action"] + " → " + result["status"])
        if result["status"] != "SUCCEEDED":
            self.error = result.get("error") or {"message": "결과 확인이 필요합니다."}
        return result

    def state(self):
        binding, migration = None, None
        if self.meta["bindingId"]:
            binding = self.service.bindings.get(self.meta["bindingId"], "orchestrator")
        if self.meta["migrationId"]:
            migration = self.service.get(self.meta["migrationId"], "orchestrator")
        nodes = {}
        for node in ["source", "target"]:
            parameters = admin_dsn(node)
            if CLOUD_MODE and node == "target":
                parameters = make_conninfo(parameters, dbname="postgres")
            with psycopg.connect(
                parameters, connect_timeout=3, options="-c statement_timeout=5000"
            ) as maintenance:
                present = (
                    maintenance.execute(
                        "SELECT 1 FROM pg_database WHERE datname=%s", ("foxtrot",)
                    ).fetchone()
                    if CLOUD_MODE and node == "target"
                    else True
                )
            if CLOUD_MODE and node == "target" and not present:
                with psycopg.connect(
                    parameters, connect_timeout=3, options="-c statement_timeout=5000"
                ) as maintenance:
                    login = maintenance.execute(
                        "SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (WRITER_ROLE,)
                    ).fetchone()[0]
                nodes[node] = {
                    "orders": 0,
                    "stock": None,
                    "writerLogin": login,
                    "databaseReady": False,
                }
                continue
            with psycopg.connect(
                admin_dsn(node), connect_timeout=3, options="-c statement_timeout=5000"
            ) as conn:
                present = conn.execute("SELECT to_regclass('public.orders')").fetchone()[0]
                count = (
                    conn.execute("SELECT count(*) FROM orders").fetchone()[0] if present else None
                )
                stock = (
                    conn.execute("SELECT stock FROM inventory WHERE sku='chiikawa'").fetchone()
                    if present
                    else None
                )
                login = conn.execute(
                    "SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (WRITER_ROLE,)
                ).fetchone()[0]
            nodes[node] = {
                "orders": count,
                "stock": stock[0] if stock else None,
                "writerLogin": login,
            }
        proxy = None
        with psycopg.connect(
            os.environ["PGBOUNCER_ADMIN_DSN"], autocommit=True, row_factory=dict_row
        ) as conn:
            for row in conn.execute("SHOW DATABASES").fetchall():
                if row["name"] == "reservation":
                    route = row["host"]
                    if CLOUD_MODE:
                        route = next(
                            (
                                node
                                for node, db in CLOUD_CONFIG["databases"].items()
                                if db["route_host"] == row["host"]
                            ),
                            "UNKNOWN",
                        )
                    proxy = {
                        "alias": row["name"],
                        "host": route,
                        "actualHost": row["host"],
                        "port": row["port"],
                        "paused": bool(row["paused"]),
                        "disabled": bool(row["disabled"]),
                    }
        try:
            if NATIVE_MODE:
                life = migration["lifecycle"] if migration else "NOT_STARTED"
                cloud = {
                    "state": "COMPLETED"
                    if life in {"SWITCHED", "ACTIVE", "COMPLETED"}
                    else "RUNNING"
                    if life in {"SYNCING", "READY", "QUIESCED"}
                    else "NOT_STARTED",
                    "phase": "CDC"
                    if life in {"READY", "QUIESCED"}
                    else "INITIAL_COPY"
                    if life == "SYNCING"
                    else "NATIVE_LOGICAL",
                }
            else:
                cloud = (
                    self.cloud.observe()
                    if CLOUD_MODE
                    else {
                        "state": "DELETED" if self.cloud.deleted else self.cloud.job.state.name,
                        "phase": self.cloud.job.phase.name,
                        "injected": self.cloud.lose_response,
                    }
                )
        except cloud_errors.NotFound:
            cloud = {
                "state": "NOT_FOUND",
                "error": "등록된 DMS 작업이 없습니다. 새 작업을 준비해야 합니다.",
            }
        except (cloud_errors.GoogleAPICallError, dms.GoogleAuthError):
            cloud = {"state": "UNKNOWN", "error": "DMS 상태를 확인할 수 없습니다."}
        return {
            "binding": binding,
            "migration": migration,
            "nodes": nodes,
            "proxy": proxy,
            "cloud": cloud,
        }

    def refresh(self):
        with self.state_lock:
            self.view = self.state()

    def write_order(self):
        with self.writer_lock:
            if self.held.is_set() or not self.meta["bindingId"]:
                return
            try:
                with psycopg.connect(
                    host="pooler",
                    port=6432,
                    dbname="reservation",
                    user=WRITER_ROLE,
                    password=cloud_secret("source")["writerPassword"]
                    if CLOUD_MODE
                    else WRITER_PASSWORD,
                    connect_timeout=2,
                ) as conn:
                    conn.execute("SET LOCAL statement_timeout=3000")
                    conn.execute("UPDATE inventory SET stock=stock-1 WHERE sku='chiikawa'")
                    order = conn.execute(
                        "INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa') RETURNING id",
                        (uuid4(),),
                    ).fetchone()[0]
                    cluster = conn.execute("SHOW cluster_name").fetchone()[0]
                    if CLOUD_MODE:
                        address = conn.execute("SELECT inet_server_addr()::text").fetchone()[0]
                        cluster = (
                            "demo-target"
                            if address.split("/", 1)[0] == CLOUD_CONFIG["targetIp"].split("/", 1)[0]
                            else "demo-source"
                        )
                self.success += 1
                self.last_write = {"id": order, "node": cluster, "time": stamp()}
            except psycopg.Error:
                self.errors += 1
                self.held.set()
                self.event("주문 쓰기 실패. 자동 생성을 멈췄습니다.")

    def hold(self):
        self.held.set()
        with self.writer_lock:
            if not self.meta["heldSince"]:
                self.meta["heldSince"] = stamp()
                self.meta["generation"] += 1
                row = self.service.store.get(self.meta["migrationId"])
                if row["state"]["transitionState"] == "NONE":
                    self.meta["transitionId"] = "demo-" + uuid4().hex
        reference = ControlPlaneRecords(self.service.store).held(
            migration_id=self.meta["migrationId"],
            transition_id=self.meta["transitionId"],
            generation=self.meta["generation"],
            held_since=datetime.fromisoformat(self.meta["heldSince"].replace("Z", "+00:00")),
            participants=[actor_ack(actor, self.meta["generation"], "HELD") for actor in ACTORS],
        )
        self.gate_ref = reference
        return reference

    def renew_hold(self):
        while not self.stop.wait(5):
            with self.writer_lock:
                if not self.held.is_set() or not self.gate_ref:
                    continue
                try:
                    ControlPlaneRecords(self.service.store).renew(
                        self.gate_ref,
                        [actor_ack(actor, self.meta["generation"], "HELD") for actor in ACTORS],
                    )
                except Exception as error:
                    self.observation_error = "정지 확인 갱신 실패: " + type(error).__name__

    def run_command(self, command):
        self.error = None
        try:
            if command in {"MIGRATE_ALL", "ROLLBACK_ALL"}:
                self.meta["batch"] = {
                    "command": command,
                    "status": "RUNNING",
                    "step": command,
                    "startedAt": stamp(),
                    "completed": [],
                }
                self.save()
                self.run_sequence(command)
                self.meta["batch"].update(status="SUCCEEDED", finishedAt=stamp())
            elif command == "AUTO":
                steps = (
                    ["REGISTER", "CREATE", "PREPARE", "START_SYNC"]
                    if CLOUD_MODE
                    else [
                        "REGISTER",
                        "CREATE",
                        "PREPARE",
                        "START_SYNC",
                        "QUIESCE",
                        "CUTOVER",
                        "RESUME",
                    ]
                )
                for step in steps:
                    self.busy = step
                    self.run_step(step)
                    self.refresh()
                    if self.error:
                        break
                    if step in {"REGISTER", "START_SYNC"}:
                        time.sleep(1)
            else:
                self.run_step(command)
            self.refresh()
        except Exception as error:
            # SQL/DSN은 노출하지 않습니다. HTTP 오류는 API가 이미 정제한 본문입니다.
            self.error = str(error) if isinstance(error, RuntimeError) else type(error).__name__
            self.event("실행 중지: " + str(self.error))
            if command in {"MIGRATE_ALL", "ROLLBACK_ALL"}:
                self.meta["batch"].update(status="STOPPED", finishedAt=stamp(), error=self.error)
        finally:
            self.save()
            self.busy = None
            self.command_started = None
            self.command_lock.release()

    def run_checked(self, command):
        self.busy = command
        batch = self.meta["batch"]
        batch["step"] = command
        self.save()
        self.error = None
        result = self.run_step(command)
        if self.error and isinstance(result, dict):
            code = (result.get("error") or {}).get("code")
            if code == "DMS_OPERATION_PENDING":
                self.settle_pending()
                if command in {"CUTOVER", "ROLLBACK"}:
                    # RECONCILE가 기존 Promote 완료를 연결했습니다. 이 명령은
                    # 그 승격을 다시 요청하지 않고 SQL·프록시 전환만 이어갑니다.
                    self.error = None
                    self.busy = command
                    result = self.run_step(command)
        if self.error:
            message = (
                self.error.get("message", "상태 확인이 필요합니다.")
                if isinstance(self.error, dict)
                else self.error
            )
            raise RuntimeError(message)
        batch["completed"].append(command)
        self.save()

    def settle_pending(self):
        """저장된 실행 ID만 조회합니다. 접수 불명 RPC는 다시 보내지 않습니다."""
        row = self.service.store.get(self.meta["migrationId"])
        driver = self.service.scoped(row).driver
        journal = driver.dms.journal
        key = (
            "DMS_OP#"
            + journal.job_key(driver.client.binding.job)
            + "#"
            + str(row["internal"].get("dmsOperationId"))
        )
        operation = journal.store._read(key)
        if not operation or not operation.get("cloud_operation"):
            raise RuntimeError("클라우드 실행 ID가 없습니다. 상태를 직접 확인하십시오.")
        self.busy = "WAIT_CLOUD"
        self.meta["batch"]["step"] = "WAIT_CLOUD"
        self.save()
        deadline = time.monotonic() + 240
        while not self.stop.is_set() and time.monotonic() < deadline:
            result = driver.client.operation(operation["cloud_operation"], operation["action"])
            if result["status"] != "PENDING":
                if result["status"] != "SUCCEEDED":
                    raise RuntimeError("클라우드 작업이 실패했습니다. 상태 재확인이 필요합니다.")
                self.error = None
                result = self.run_step("RECONCILE")
                if self.error:
                    raise RuntimeError("클라우드 결과 대조에 실패했습니다. 자동 진행을 멈췄습니다.")
                return
            self.stop.wait(2)
        raise RuntimeError("클라우드 작업이 아직 진행 중입니다. 실행 ID를 보존하고 멈췄습니다.")

    def wait_ready(self):
        self.busy = "WAIT_CDC"
        self.meta["batch"]["step"] = "WAIT_CDC"
        self.save()
        deadline = time.monotonic() + 600
        latest = None
        while not self.stop.is_set() and time.monotonic() < deadline:
            state = self.api("GET", "/v1/migrations/" + self.meta["migrationId"])
            if state["health"] != "OK":
                raise RuntimeError("CDC 상태를 확인할 수 없습니다. 자동 진행을 멈췄습니다.")
            if state["lifecycle"] == "READY":
                if latest is None:
                    with psycopg.connect(admin_dsn("source")) as conn:
                        latest = conn.execute(
                            "SELECT id,request_id FROM orders ORDER BY id DESC LIMIT 1"
                        ).fetchone()
                if latest is None:
                    return
                with psycopg.connect(admin_dsn("target")) as conn:
                    if conn.execute(
                        "SELECT 1 FROM orders WHERE id=%s AND request_id=%s", latest
                    ).fetchone():
                        return
            elif state["lifecycle"] != "SYNCING":
                raise RuntimeError("CDC 대기 중 예상하지 않은 상태입니다.")
            self.stop.wait(2)
        raise RuntimeError("CDC 준비 대기 시간을 넘었습니다. 앱은 원본 운영을 유지합니다.")

    def rollback_and_resume(self):
        state = self.api("GET", "/v1/migrations/" + self.meta["migrationId"])
        if state["health"] != "OK":
            self.run_checked("RECONCILE")
            state = self.api("GET", "/v1/migrations/" + self.meta["migrationId"])
        if state["lifecycle"] == "RESTORED":
            return
        if "ROLLBACK" in state["allowedActions"]:
            self.run_checked("ROLLBACK")
            state = self.api("GET", "/v1/migrations/" + self.meta["migrationId"])
        if state["lifecycle"] == "ROLLED_BACK_HELD":
            self.run_checked("RESUME")
            return
        raise RuntimeError("대상 쓰기 후에는 역방향 CDC 없이 원본으로 복귀할 수 없습니다.")

    def run_sequence(self, command):
        if command == "ROLLBACK_ALL":
            self.rollback_and_resume()
            return
        if not self.meta["bindingId"]:
            self.run_checked("REGISTER")
        if not self.meta["migrationId"]:
            self.run_checked("CREATE")
        state = self.api("GET", "/v1/migrations/" + self.meta["migrationId"])
        row = self.service.store.get(self.meta["migrationId"])
        if row["internal"].get("targetWritesIntent"):
            raise RuntimeError(
                "이미 대상 쓰기를 시작했습니다. 원본 복귀·재전환을 실행하지 않습니다."
            )
        if row["internal"].get("dmsPromoted"):
            raise RuntimeError(
                "이미 승격한 DMS 작업은 재사용할 수 없습니다. 새 대상·작업이 필요합니다."
            )
        if state["health"] != "OK":
            self.run_checked("RECONCILE")
            state = self.api("GET", "/v1/migrations/" + self.meta["migrationId"])
        if state["transitionState"] == "OPEN":
            self.rollback_and_resume()
            state = self.api("GET", "/v1/migrations/" + self.meta["migrationId"])
        if state["lifecycle"] == "RESTORED":
            self.run_checked("START_SYNC")
        elif state["lifecycle"] == "CREATED":
            self.run_checked("PREPARE")
            self.run_checked("START_SYNC")
        elif state["lifecycle"] == "PREPARED":
            self.run_checked("START_SYNC")
        elif state["lifecycle"] not in {"SYNCING", "READY"}:
            raise RuntimeError("현재 상태에서 전체 전환을 시작할 수 없습니다.")
        self.wait_ready()
        self.run_checked("QUIESCE")
        self.run_checked("CUTOVER")
        self.run_checked("RESUME")
        state = self.api("GET", "/v1/migrations/" + self.meta["migrationId"])
        if state["lifecycle"] != "ACTIVE" or state["observed"]["route"] != "TARGET":
            raise RuntimeError("대상 운영을 확인하지 못했습니다.")
        self.event("전체 전환 완료. Cloud SQL에서 앱 쓰기를 재개했습니다.")

    def run_step(self, command):
        if CLOUD_MODE and command in {"RESET", "INJECT"}:
            raise RuntimeError("실제 클라우드에서는 초기화와 모의 장애 주입을 지원하지 않습니다.")
        if command == "ARCHIVE":
            row = self.service.store.get(self.meta["migrationId"])
            if row["state"]["lifecycle"] != "COMPLETED":
                raise RuntimeError("완료한 시연만 보관할 수 있습니다.")
            self.held.set()
            with self.writer_lock:
                binding = self.service.bindings.get(self.meta["bindingId"], "orchestrator")
                ref = ControlPlaneRecords(self.service.store).binding_release(
                    self.meta["bindingId"],
                    "app:reservation",
                    mode="STOP_APP",
                    participants=[
                        actor_ack(actor, self.meta["generation"], "HELD") for actor in ACTORS
                    ],
                    connections_closed=True,
                )
                accepted = self.api(
                    "POST",
                    "/v1/bindings/" + self.meta["bindingId"] + "/actions",
                    {
                        "action": "UNREGISTER",
                        "expectedRevision": binding["revision"],
                        "appRef": "app:reservation",
                        "mode": "STOP_APP",
                        "evidenceRef": ref,
                    },
                )
                result = self.execute(accepted)
                if result["status"] != "SUCCEEDED":
                    raise RuntimeError("이전 앱 접속점의 해제 결과를 확인해야 합니다.")
                self.event("이전 시연을 보관했습니다. DB 데이터는 유지합니다.")
            return result
        if command == "RESET":
            self.held.set()
            with self.writer_lock, self.state_lock, self.cloud.lock:
                if self.meta["bindingId"]:
                    with psycopg.connect(
                        os.environ["PGBOUNCER_ADMIN_DSN"], autocommit=True
                    ) as conn:
                        conn.execute("KILL reservation")
                self.pooler_files()
                with psycopg.connect(os.environ["PGBOUNCER_ADMIN_DSN"], autocommit=True) as conn:
                    conn.execute("RELOAD")
                for node in ["source", "target"]:
                    with psycopg.connect(admin_dsn(node), autocommit=True) as conn:
                        conn.execute("TRUNCATE orders,inventory RESTART IDENTITY")
                        conn.execute("DROP SCHEMA IF EXISTS foxtrot_migration CASCADE")
                        conn.execute(
                            "ALTER ROLE app_writer " + ("LOGIN" if node == "source" else "NOLOGIN")
                        )
                        if node == "source":
                            conn.execute("INSERT INTO inventory VALUES('chiikawa',100000)")
                self.cloud = LocalCloud()
                self.meta.update(
                    namespace="demo-" + uuid4().hex[:12],
                    bindingId=None,
                    migrationId=None,
                    transitionId="demo-" + uuid4().hex,
                    heldSince=None,
                    generation=1,
                    batch=None,
                )
                self.service.store.namespace = self.meta["namespace"]
                self.service.store.initialize(create=True)
                self.publish()
                self.success, self.errors, self.last_write = 0, 0, None
                self.events, self.traces = [], []
                self.event("이 Docker 시연의 데이터와 상태를 초기화했습니다.")
            return
        if command == "INJECT":
            self.cloud.lose_response = not self.cloud.lose_response
            self.event("다음 승격 응답 유실: " + ("켜짐" if self.cloud.lose_response else "꺼짐"))
            return
        if command == "WRITE":
            self.write_order()
            return
        if command == "REGISTER":
            if self.meta["bindingId"]:
                return
            accepted = self.api(
                "POST",
                "/v1/bindings",
                {
                    "appRef": "app:reservation",
                    "databaseRef": "db:source",
                    "alias": "reservation",
                    "workloadScopeRef": "scope:demo",
                    "poolSize": 10,
                    "maxDbConnections": 10,
                },
            )
            self.meta["bindingId"] = accepted["bindingId"]
            result = self.execute(accepted)
            if result["status"] == "SUCCEEDED":
                self.held.clear()
                self.event("주문 앱이 같은 PgBouncer 주소로 쓰기를 시작합니다.")
            return
        if command == "CREATE":
            if self.meta["migrationId"]:
                return
            result = self.api(
                "POST",
                "/v1/migrations",
                {
                    "workflowId": "demo-" + uuid4().hex,
                    "resourceId": "database",
                    "kind": "DATABASE",
                    "profile": MIGRATION_PROFILE,
                    "sourceRef": "db:source",
                    "targetRef": "db:target",
                    "bindingRef": self.meta["bindingId"],
                    "workloadScopeRef": self.service.bindings.get(
                        self.meta["bindingId"], "orchestrator"
                    )["workloadScopeRef"],
                    **({} if NATIVE_MODE else {"replicationRef": "dms:demo"}),
                    "policy": {"rollbackMode": "BEFORE_TARGET_WRITE", "maxQuiesceSeconds": 300},
                },
            )
            self.meta["migrationId"] = result["migrationId"]
            self.event(
                "기본 논리 복제 계획을 등록했습니다."
                if NATIVE_MODE
                else "DMS 프로필의 마이그레이션을 등록했습니다."
            )
            return
        mid = self.meta["migrationId"]
        if not mid:
            raise RuntimeError("마이그레이션 계획을 먼저 등록하십시오.")
        state = self.api("GET", "/v1/migrations/" + mid)
        if command not in state["allowedActions"]:
            raise RuntimeError("현재 상태에서는 이 명령을 실행할 수 없습니다.")
        check_only = ATOMIC_MODE and command in {"QUIESCE", "RESUME"}
        gate = (
            "check-only:" + self.meta["transitionId"]
            if check_only
            else self.hold()
            if command in {"QUIESCE", "CUTOVER", "RESUME", "ROLLBACK"}
            else None
        )
        if gate:
            state = self.api("GET", "/v1/migrations/" + mid)
        body = {"action": command, "expectedRevision": state["revision"]}
        if gate:
            body.update(transitionId=self.meta["transitionId"], quiesceRef=gate)
        if command == "RESUME":
            body["expectedLocation"] = state["observed"]["route"]
        if (
            not CLOUD_MODE
            and command == "RECONCILE"
            and state["health"] != "OK"
            and self.cloud.last_operation
        ):
            body["providerOperationRef"] = self.cloud.last_operation
        result = self.execute(self.api("POST", f"/v1/migrations/{mid}/actions", body))
        if result["status"] == "SUCCEEDED" and (
            command == "RESUME" or ATOMIC_MODE and command in {"CUTOVER", "ROLLBACK"}
        ):
            self.meta["heldSince"] = None
            self.gate_ref = None
            self.held.clear()
            self.event("앱 쓰기를 재개했습니다. DB 접속 주소는 같습니다.")
        return result

    def write_loop(self):
        while not self.stop.wait(0.2):
            self.write_order()

    def background(self):
        last_observe = 0
        while not self.stop.wait(0.2):
            if time.monotonic() - last_observe >= (3 if CLOUD_MODE else 1):
                try:
                    mid = self.meta["migrationId"]
                    if mid and not self.held.is_set() and self.busy is None:
                        row = self.service.store.get(mid)
                        if row["state"]["lifecycle"] in {"ACTIVE", "RESTORED"}:
                            ControlPlaneRecords(self.service.store).running(
                                mid,
                                self.meta["generation"],
                                [
                                    actor_ack(actor, self.meta["generation"], "RUNNING")
                                    for actor in ACTORS
                                ],
                                "PASS",
                                "demo-last-committed-order",
                            )
                    self.refresh()
                    self.observation_error = None
                except Exception as error:
                    self.observation_error = "상태 확인 실패: " + type(error).__name__
                last_observe = time.monotonic()


demo = Demo()
demo.configure()
demo.install_local_adapters()
app = create_app(demo.settings)
demo.service = app.state.service
demo.worker = Worker(demo.service)


def initialize():
    for attempt in range(60):
        try:
            demo.service.store.initialize(create=True)
            with psycopg.connect(os.environ["PGBOUNCER_ADMIN_DSN"], connect_timeout=2):
                pass
            break
        except Exception:
            if attempt == 59:
                raise
            time.sleep(1)
    demo.publish()
    if demo.meta["bindingId"]:
        state = demo.state()
        if state["proxy"] and not state["proxy"]["paused"]:
            demo.held.clear()
        elif demo.meta["heldSince"] and state["proxy"] and state["proxy"]["paused"]:
            demo.hold()
    demo.refresh()
    demo.event(
        "실제 RDS·Cloud SQL의 기본 논리 복제 시연입니다."
        if NATIVE_MODE
        else "실제 RDS·Cloud SQL·GCP DMS에 연결했습니다."
        if CLOUD_MODE
        else "로컬 시연이 준비됐습니다. GCP DMS는 모의 SDK입니다."
    )
    threading.Thread(target=demo.background, daemon=True).start()
    threading.Thread(target=demo.write_loop, daemon=True).start()
    threading.Thread(target=demo.renew_hold, daemon=True).start()


@app.on_event("startup")
async def startup():
    # PgBouncer의 동기 관리 API는 내부에서 asyncio.run()을 사용합니다.
    await asyncio.to_thread(initialize)


@app.on_event("shutdown")
def shutdown():
    demo.stop.set()
    demo.held.set()
    demo.executor.shutdown(wait=True, cancel_futures=True)
    demo.save()


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(Path(__file__).with_name("index.html"))


@app.get("/demo/health", include_in_schema=False)
def health():
    return {"status": "ok" if demo.view else "starting"}


@app.get("/demo/state", include_in_schema=False)
def state():
    view = dict(demo.view)
    expired = False
    error = demo.error or demo.observation_error
    if demo.meta["migrationId"]:
        row = demo.service.store.get(demo.meta["migrationId"])
        if demo.busy or row["state"]["health"] != "OK":
            view["migration"] = row["state"]
        if demo.meta["heldSince"]:
            since = datetime.fromisoformat(demo.meta["heldSince"].replace("Z", "+00:00"))
            expired = (datetime.now(UTC) - since).total_seconds() >= row["plan"]["policy"][
                "maxQuiesceSeconds"
            ]
        unresolved = row["internal"].get("unresolvedOperationId")
        if not error and unresolved:
            error = demo.service.store.operation(unresolved, "orchestrator").get("error")
    return {
        "mode": "cloud" if CLOUD_MODE else "simulation",
        "replicationMode": "native" if NATIVE_MODE else "dms",
        "atomicCommands": ATOMIC_MODE,
        "resources": CLOUD_CONFIG.get("resources"),
        **view,
        "busy": demo.busy,
        "error": error,
        "quiesceExpired": expired,
        "held": demo.held.is_set(),
        "writes": {"success": demo.success, "errors": demo.errors, "last": demo.last_write},
        "events": list(demo.events),
        "traces": list(demo.traces),
        "elapsed": round(time.monotonic() - demo.command_started) if demo.command_started else None,
        "heldSince": demo.meta["heldSince"],
        "batch": demo.meta.get("batch"),
    }


@app.post("/demo/commands/{command}", status_code=202, include_in_schema=False)
def command(command: str, request: Request):
    if request.headers.get("X-Demo-Control") != "local":
        raise HTTPException(403, "시연 화면에서 실행하십시오.")
    if command not in COMMANDS:
        raise HTTPException(400, "지원하지 않는 명령입니다.")
    if not demo.command_lock.acquire(blocking=False):
        raise HTTPException(409, "실행 중입니다.")
    demo.busy = command
    demo.command_started = time.monotonic()
    demo.executor.submit(demo.run_command, command)
    return {"accepted": command}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, access_log=False)
