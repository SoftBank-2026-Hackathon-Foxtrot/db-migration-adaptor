from __future__ import annotations

import asyncio
import configparser
import hashlib
import io
import os
import tempfile
import time
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import AsyncClientCursor, sql
from psycopg.rows import dict_row

from .config import Binding, Database, Registry
from .models import OperatorError, require

BARRIER = "foxtrot_migration.barriers"


def identifier(name: str) -> sql.Identifier:
    return sql.Identifier(*name.split("."))


class Postgres:
    """Only registered resources are accepted. No shell execution or user SQL."""

    def __init__(self, registry: Registry):
        self.registry = registry

    @contextmanager
    def connect(self, db: Database, tag: str = "observe", timeout: float = 10):
        with psycopg.connect(
            db.secret("admin"),
            autocommit=True,
            row_factory=dict_row,
            application_name="foxtrot-db:" + tag,
            connect_timeout=3,
            options=f"-c statement_timeout={max(1, int(timeout * 1000))} "
            "-c lock_timeout=3000 -c timezone=UTC -c datestyle=ISO,YMD",
        ) as conn:
            yield conn

    def admin(
        self, binding: Binding, command: str, timeout: float = 10, tag: str = "observe"
    ) -> list[dict]:
        dsn = os.environ.get(binding.admin_env)
        require(dsn, "SECRET_UNAVAILABLE", "PgBouncer 관리 Secret이 없습니다.")

        async def run():
            async with asyncio.timeout(timeout):
                async with await psycopg.AsyncConnection.connect(
                    dsn,
                    autocommit=True,
                    cursor_factory=AsyncClientCursor,
                    row_factory=dict_row,
                    connect_timeout=3,
                    application_name="foxtrot-db-admin:" + tag,
                ) as conn:
                    query = sql.SQL(command)
                    if "{}" in command:
                        query = query.format(sql.Identifier(binding.alias))
                    cursor = await conn.execute(query)
                    return await cursor.fetchall() if cursor.description else []

        return asyncio.run(run())

    def resources(self, plan: dict) -> tuple[Database, Database, Binding]:
        return (
            self.registry.databases[plan["sourceRef"]],
            self.registry.databases[plan["targetRef"]],
            self.registry.bindings[plan["bindingRef"]],
        )

    @staticmethod
    def names(mid: str) -> dict[str, str]:
        prefix = "fxt_" + mid.removeprefix("mig-")
        return {"forward": prefix + "_f", "reverse": prefix + "_r"}

    @staticmethod
    def wait(test: Callable[[], Any], timeout: float) -> Any:
        until = time.monotonic() + timeout
        while True:
            value = test()
            if value:
                return value
            if time.monotonic() >= until:
                raise OperatorError("DEADLINE_EXCEEDED", "검사 시간 상한을 넘었습니다.")
            time.sleep(0.05)

    def inspect(self, db: Database) -> dict:
        with self.connect(db) as conn:
            result = conn.execute(
                "SELECT current_database() AS db, (pg_control_system()).system_identifier::text "
                "AS system_id, current_setting('server_version_num')::int AS version, "
                "current_setting('wal_level') AS wal_level, inet_server_addr()::text AS address, "
                "inet_server_port() AS port, (SELECT rolsuper FROM pg_roles WHERE "
                "rolname=current_user) AS admin"
            ).fetchone()
            result["writer"] = conn.execute(
                "SELECT "
                "rolcanlogin,rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls FROM "
                "pg_roles WHERE rolname=%s",
                (db.writer_role,),
            ).fetchone()
            result["sessions"] = conn.execute(
                "SELECT count(*) AS n FROM pg_stat_activity WHERE usename=%s", (db.writer_role,)
            ).fetchone()["n"]
            result["prepared"] = conn.execute(
                "SELECT count(*) AS n FROM pg_prepared_xacts"
            ).fetchone()["n"]
            result["oldExecutors"] = conn.execute(
                "SELECT count(*) AS n FROM pg_stat_activity WHERE application_name LIKE "
                "'foxtrot-db:op-%%' AND pid<>pg_backend_pid()"
            ).fetchone()["n"]
            return result

    def observe(self, plan: dict) -> dict:
        source, target, binding = self.resources(plan)
        a, b = self.inspect(source), self.inspect(target)
        rows = [r for r in self.admin(binding, "SHOW DATABASES") if r["name"] == binding.alias]
        require(len(rows) == 1, "PROXY_ALIAS_MISSING", "PgBouncer alias를 확인할 수 없습니다.")
        proxy = rows[0]
        route = "UNKNOWN"
        for name, db in [("SOURCE", source), ("TARGET", target)]:
            if (proxy["host"], int(proxy["port"]), proxy["database"]) == (
                db.route_host,
                db.route_port,
                db.database,
            ):
                route = name
        access = "UNKNOWN"
        if a["writer"] and b["writer"]:
            flags = (a["writer"]["rolcanlogin"], b["writer"]["rolcanlogin"])
            access = {
                (True, False): "SOURCE_ONLY",
                (False, True): "TARGET_ONLY",
                (False, False): "BLOCKED",
                (True, True): "BOTH",
            }[flags]
        return {
            "source": a,
            "target": b,
            "route": route,
            "access": access,
            "paused": bool(proxy.get("paused")),
            "proxy": proxy,
        }

    def table_signature(self, db: Database, name: str) -> dict:
        with self.connect(db) as conn:
            columns = conn.execute(
                "SELECT a.attname,format_type(a.atttypid,a.atttypmod) AS "
                "type,a.attnotnull,a.attidentity,a.attgenerated,pg_get_expr(d.adbin,d.adrelid) AS "
                "def FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND "
                "d.adnum=a.attnum WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT "
                "a.attisdropped ORDER BY a.attnum",
                (name,),
            ).fetchall()
            relation = conn.execute(
                "SELECT relkind,relrowsecurity FROM pg_class WHERE oid=%s::regclass", (name,)
            ).fetchone()
            pk = conn.execute(
                "SELECT a.attname FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey) WITH "
                "ORDINALITY k(n,ord) JOIN pg_attribute a ON a.attrelid=i.indrelid AND "
                "a.attnum=k.n WHERE i.indrelid=%s::regclass AND i.indisprimary ORDER BY k.ord",
                (name,),
            ).fetchall()
            constraints = conn.execute(
                "SELECT pg_get_constraintdef(oid) AS def FROM pg_constraint WHERE "
                "conrelid=%s::regclass ORDER BY pg_get_constraintdef(oid)",
                (name,),
            ).fetchall()
            require(
                relation["relkind"] == "r" and not relation["relrowsecurity"] and pk,
                "UNSUPPORTED_SCHEMA",
                "일반 table과 PK가 필요하며 RLS는 지원하지 않습니다.",
            )
            require(
                not any(c["attgenerated"] for c in columns),
                "UNSUPPORTED_SCHEMA",
                "생성 열은 지원하지 않습니다.",
            )
            triggers = conn.execute(
                "SELECT count(*) AS n FROM pg_trigger WHERE tgrelid=%s::regclass AND NOT "
                "tgisinternal",
                (name,),
            ).fetchone()["n"]
            require(
                not triggers,
                "UNSUPPORTED_SCHEMA",
                "사용자 trigger는 이 프로필에서 지원하지 않습니다.",
            )
            return {
                "columns": columns,
                "pk": [r["attname"] for r in pk],
                "constraints": constraints,
            }

    def preflight(self, plan: dict) -> dict:
        source, target, binding = self.resources(plan)
        observed = self.observe(plan)
        require(
            source.writer_role == target.writer_role
            and observed["proxy"].get("force_user") in {None, "", source.writer_role},
            "UNSAFE_PROXY",
            "원본·대상 업무 역할이 같아야 하며 다른 역할로 강제 접속할 수 없습니다.",
        )
        require(
            observed["route"] == "SOURCE"
            and observed["access"] == "SOURCE_ONLY"
            and not observed["paused"],
            "PRECONDITION_FAILED",
            "원본 단독 운영 상태가 필요합니다.",
        )
        require(
            observed["source"]["system_id"] != observed["target"]["system_id"],
            "UNSUPPORTED_PROFILE",
            "같은 PostgreSQL 클러스터 간 이전은 지원하지 않습니다.",
        )
        pool = {r["key"]: r["value"] for r in self.admin(binding, "SHOW CONFIG")}
        require(
            pool.get("pool_mode") == "transaction",
            "UNSUPPORTED_PROFILE",
            "transaction pooling이 필요합니다.",
        )
        require(
            binding.config_path.is_file() and os.access(binding.config_path.parent, os.W_OK),
            "CONFIG_UNAVAILABLE",
            "등록한 PgBouncer 설정 경로를 쓸 수 없습니다.",
        )
        signatures = {}
        for db, node in [(source, "source"), (target, "target")]:
            info = observed[node]
            require(
                180000 <= info["version"] < 190000
                and info["admin"]
                and info["db"] == db.database
                and info["wal_level"] == "logical",
                "UNSUPPORTED_PROFILE",
                "PostgreSQL 18 자체 운영·관리 권한·logical WAL이 필요합니다.",
            )
            writer = info["writer"]
            require(
                writer
                and not any(
                    writer[k]
                    for k in [
                        "rolsuper",
                        "rolcreaterole",
                        "rolcreatedb",
                        "rolreplication",
                        "rolbypassrls",
                    ]
                ),
                "UNSAFE_WRITER",
                "업무 계정에 관리 권한이 있습니다.",
            )
            with self.connect(db) as conn:
                require(
                    conn.execute(
                        "SELECT to_regclass('foxtrot_operator.migrations') AS manager"
                    ).fetchone()["manager"]
                    is None,
                    "UNSAFE_MANAGEMENT_DB",
                    "관리 DB는 업무 DB와 분리해야 합니다.",
                )
                require(
                    not conn.execute(
                        "SELECT 1 FROM pg_auth_members WHERE member=(SELECT oid FROM pg_roles "
                        "WHERE rolname=%s)",
                        (db.writer_role,),
                    ).fetchone(),
                    "UNSAFE_WRITER",
                    "업무 계정의 역할 상속은 지원하지 않습니다.",
                )
                require(
                    not conn.execute(
                        "SELECT 1 FROM pg_subscription WHERE subdbid=(SELECT oid FROM pg_database "
                        "WHERE datname=current_database()) LIMIT 1"
                    ).fetchone(),
                    "UNSUPPORTED_PROFILE",
                    "기존 subscription이 있는 DB는 지원하지 않습니다.",
                )
                require(
                    not conn.execute("SELECT 1 FROM pg_largeobject_metadata LIMIT 1").fetchone(),
                    "UNSUPPORTED_SCHEMA",
                    "large object는 지원하지 않습니다.",
                )
                all_tables = {
                    r["name"]
                    for r in conn.execute(
                        "SELECT n.nspname||'.'||c.relname AS name FROM pg_class c JOIN "
                        "pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname NOT IN "
                        "('pg_catalog','information_schema','foxtrot_migration') AND n.nspname "
                        "NOT LIKE 'pg_toast%%' AND c.relkind IN ('r','p')"
                    ).fetchall()
                }
                require(
                    all_tables == set(self.registry.tables),
                    "UNSUPPORTED_SCHEMA",
                    "DB의 업무 table 전체를 등록해야 합니다.",
                )
                for name in self.registry.tables:
                    owner = conn.execute(
                        "SELECT pg_get_userbyid(relowner) AS owner FROM pg_class WHERE "
                        "oid=%s::regclass",
                        (name,),
                    ).fetchone()["owner"]
                    require(
                        owner != db.writer_role,
                        "UNSAFE_WRITER",
                        "업무 계정은 table 소유자일 수 없습니다.",
                    )
                    other_writers = conn.execute(
                        "SELECT r.rolname FROM pg_roles r WHERE r.rolcanlogin AND r.rolname NOT "
                        "IN (current_user,%s) AND EXISTS (SELECT 1 FROM pg_roles p WHERE "
                        "has_table_privilege(p.oid,%s::regclass,'INSERT,UPDATE,DELETE,TRUNCATE') "
                        "AND pg_has_role(r.oid,p.oid,'SET'))",
                        (db.writer_role, name),
                    ).fetchall()
                    require(
                        not other_writers,
                        "UNREGISTERED_WRITER",
                        "미등록 계정이 업무 table을 쓰거나 쓰기 역할로 전환할 수 있습니다.",
                    )
                    if node == "target":
                        require(
                            not conn.execute(
                                sql.SQL("SELECT 1 FROM {} LIMIT 1").format(identifier(name))
                            ).fetchone(),
                            "TARGET_NOT_EMPTY",
                            "대상 업무 table이 비어 있어야 합니다.",
                        )
                actual_sequences = {
                    r["name"]
                    for r in conn.execute(
                        "SELECT schemaname||'.'||sequencename AS name FROM pg_sequences WHERE "
                        "schemaname NOT IN ('pg_catalog','information_schema')"
                    ).fetchall()
                }
                require(
                    actual_sequences == set(self.registry.sequences),
                    "UNSUPPORTED_SCHEMA",
                    "sequence 전체를 등록해야 합니다.",
                )
                for seq in self.registry.sequences:
                    settings = conn.execute(
                        "SELECT seqincrement,seqcache,seqcycle FROM pg_sequence WHERE "
                        "seqrelid=%s::regclass",
                        (seq,),
                    ).fetchone()
                    require(
                        settings["seqincrement"] == 1
                        and settings["seqcache"] == 1
                        and not settings["seqcycle"],
                        "UNSUPPORTED_SCHEMA",
                        "증가량 1·cache 1·비순환 sequence만 지원합니다.",
                    )
            signatures[node] = {
                name: self.table_signature(db, name) for name in self.registry.tables
            }
        require(
            signatures["source"] == signatures["target"],
            "SCHEMA_MISMATCH",
            "원본·대상 schema가 다릅니다.",
        )
        return observed

    def prepare_barrier(self, db: Database, tag: str, intent: Callable) -> None:
        intent("pg.prepare_barrier")
        with self.connect(db, tag) as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS foxtrot_migration")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS foxtrot_migration.barriers (id uuid PRIMARY KEY, "
                "migration_id text NOT NULL)"
            )
            # The publisher role must be able to read the barrier for initial sync.
            repl = psycopg.conninfo.conninfo_to_dict(db.secret("replication")).get("user")
            require(repl, "INVALID_REGISTRY", "복제 계정이 필요합니다.")
            conn.execute(
                sql.SQL("GRANT USAGE ON SCHEMA foxtrot_migration TO {}").format(
                    sql.Identifier(repl)
                )
            )
            conn.execute(
                sql.SQL("GRANT SELECT ON foxtrot_migration.barriers TO {}").format(
                    sql.Identifier(repl)
                )
            )

    def create_replication(
        self,
        publisher: Database,
        subscriber: Database,
        name: str,
        copy: bool,
        tag: str,
        intent: Callable,
    ) -> None:
        tables = [*self.registry.tables, BARRIER]
        with self.connect(publisher, tag) as conn:
            exists = conn.execute(
                "SELECT 1 FROM pg_publication WHERE pubname=%s", (name,)
            ).fetchone()
            if not exists:
                intent("pg.create_publication")
                conn.execute(
                    sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                        sql.Identifier(name), sql.SQL(",").join(identifier(t) for t in tables)
                    )
                )
            else:
                actual = {
                    r["schemaname"] + "." + r["tablename"]
                    for r in conn.execute(
                        "SELECT schemaname,tablename FROM pg_publication_tables WHERE pubname=%s",
                        (name,),
                    ).fetchall()
                }
                require(
                    actual == set(tables),
                    "REPLICATION_CONFLICT",
                    "기존 publication 범위가 다릅니다.",
                )
        with self.connect(subscriber, tag, 30) as conn:
            existing = conn.execute(
                "SELECT subconninfo,subpublications,suborigin FROM pg_subscription WHERE "
                "subname=%s",
                (name,),
            ).fetchone()
            if existing:
                require(
                    existing["subpublications"] == [name]
                    and existing["suborigin"] == "none"
                    and existing["subconninfo"] == publisher.secret("replication"),
                    "REPLICATION_CONFLICT",
                    "기존 subscription 구성이 다릅니다.",
                )
                return
            intent("pg.create_subscription")
            conn.execute(
                sql.SQL(
                    "CREATE SUBSCRIPTION {} CONNECTION {} PUBLICATION {} "
                    "WITH(copy_data={},origin=none,streaming=off,synchronous_commit=on,"
                    "two_phase=false)"
                ).format(
                    sql.Identifier(name),
                    sql.Literal(publisher.secret("replication")),
                    sql.Identifier(name),
                    sql.Literal(copy),
                )
            )

    def replication_ready(self, db: Database, name: str) -> bool:
        with self.connect(db) as conn:
            rows = conn.execute(
                "SELECT r.srsubstate FROM pg_subscription_rel r JOIN pg_subscription s ON "
                "s.oid=r.srsubid WHERE s.subname=%s",
                (name,),
            ).fetchall()
            active = conn.execute(
                "SELECT 1 FROM pg_stat_subscription WHERE subname=%s AND pid IS NOT NULL AND "
                "relid IS NULL",
                (name,),
            ).fetchone()
            return bool(
                active
                and len(rows) == len(self.registry.tables) + 1
                and all(r["srsubstate"] == "r" for r in rows)
            )

    def disable(self, db: Database, name: str, tag: str, intent: Callable, timeout: float) -> None:
        with self.connect(db, tag) as conn:
            if conn.execute("SELECT 1 FROM pg_subscription WHERE subname=%s", (name,)).fetchone():
                intent("pg.disable_subscription")
                conn.execute(sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(sql.Identifier(name)))

        def stopped():
            with self.connect(db) as conn:
                return not conn.execute(
                    "SELECT 1 FROM pg_stat_subscription WHERE subname=%s AND pid IS NOT NULL",
                    (name,),
                ).fetchone()

        self.wait(stopped, timeout)

    def fence(self, db: Database, tag: str, intent: Callable) -> None:
        intent("pg.fence_writer")
        with self.connect(db, tag) as conn:
            conn.execute(sql.SQL("ALTER ROLE {} NOLOGIN").format(sql.Identifier(db.writer_role)))
        info = self.inspect(db)
        require(
            info["sessions"] == 0 and info["prepared"] == 0,
            "WRITERS_NOT_DRAINED",
            "기존 업무 연결 또는 prepared transaction이 남아 있습니다.",
        )

    def barrier(
        self,
        publisher: Database,
        subscriber: Database,
        name: str,
        mid: str,
        tag: str,
        intent: Callable,
        timeout: float,
    ) -> str:
        value = uuid4()
        require(
            self.replication_ready(subscriber, name),
            "REPLICATION_NOT_READY",
            "복제 흐름이 준비되지 않았습니다.",
        )
        intent("pg.commit_barrier")
        with self.connect(publisher, tag) as conn:
            conn.execute("INSERT INTO foxtrot_migration.barriers VALUES (%s,%s)", (value, mid))

        def applied():
            with self.connect(subscriber) as conn:
                if not conn.execute(
                    "SELECT 1 FROM foxtrot_migration.barriers WHERE id=%s", (value,)
                ).fetchone():
                    return False
                sub = conn.execute(
                    "SELECT oid FROM pg_subscription WHERE subname=%s", (name,)
                ).fetchone()
                progress = conn.execute(
                    "SELECT pg_replication_origin_progress(%s,true) AS lsn",
                    ("pg_" + str(sub["oid"]),),
                ).fetchone()
                return progress and progress["lsn"] is not None

        self.wait(applied, timeout)
        return str(value)

    def sync_sequences(
        self, source: Database, target: Database, tag: str, intent: Callable
    ) -> None:
        for seq in self.registry.sequences:
            with self.connect(source) as conn:
                value = conn.execute(
                    sql.SQL("SELECT last_value,is_called FROM {}").format(identifier(seq))
                ).fetchone()
            intent("pg.sync_sequence")
            with self.connect(target, tag) as conn:
                conn.execute(
                    "SELECT setval(%s::regclass,%s,%s)",
                    (seq, value["last_value"], value["is_called"]),
                )

    def snapshot(self, db: Database) -> dict:
        result = {}
        for table in [*self.registry.tables, BARRIER]:
            pk = self.table_signature(db, table)["pk"]
            digest, count = hashlib.sha256(), 0
            with self.connect(db, timeout=60) as conn, conn.transaction():
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                with conn.cursor(name="foxtrot_snapshot") as cur:
                    cur.execute(
                        sql.SQL("SELECT row_to_json(t)::text AS row FROM {} t ORDER BY {}").format(
                            identifier(table), sql.SQL(",").join(sql.Identifier(k) for k in pk)
                        )
                    )
                    for row in cur:
                        encoded = row["row"].encode()
                        digest.update(len(encoded).to_bytes(8, "big"))
                        digest.update(encoded)
                        count += 1
            result[table] = {"rows": count, "sha256": digest.hexdigest()}
        return result

    def equal(self, source: Database, target: Database) -> dict:
        a, b = self.snapshot(source), self.snapshot(target)
        require(a == b, "DATA_MISMATCH", "원본·대상 데이터가 다릅니다.")
        for seq in self.registry.sequences:
            with self.connect(source) as conn:
                first = conn.execute(
                    sql.SQL("SELECT last_value,is_called FROM {}").format(identifier(seq))
                ).fetchone()
            with self.connect(target) as conn:
                second = conn.execute(
                    sql.SQL("SELECT last_value,is_called FROM {}").format(identifier(seq))
                ).fetchone()
            require(first == second, "SEQUENCE_MISMATCH", "sequence가 다릅니다.")
        return a

    def reroute(
        self, binding: Binding, db: Database, intent: Callable, tag: str = "observe"
    ) -> None:
        config = configparser.ConfigParser(interpolation=None)
        config.optionxform = str
        config.read(binding.config_path)
        require(
            config.has_option("databases", binding.alias),
            "CONFIG_UNAVAILABLE",
            "alias 설정이 없습니다.",
        )
        config["databases"][binding.alias] = (
            f"host={db.route_host} port={db.route_port} dbname={db.database}"
        )
        output = io.StringIO()
        config.write(output)
        intent("pgbouncer.atomic_config")
        path = binding.config_path
        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".foxtrot-")
        try:
            with os.fdopen(fd, "w") as stream:
                os.fchmod(stream.fileno(), path.stat().st_mode & 0o777)
                stream.write(output.getvalue())
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            Path(temporary).unlink(missing_ok=True)
        intent("pgbouncer.reload")
        deadline = time.monotonic() + 2
        while True:
            try:
                self.admin(binding, "RELOAD", tag=tag)
                break
            except psycopg.Error as error:
                # A bind-mounted directory may briefly lag an atomic rename.
                # Retry ONLY an explicit server rejection, never a lost response.
                message = error.diag.message_primary or str(error)
                if (
                    message != "RELOAD failed, see logs for additional details"
                    or time.monotonic() >= deadline
                ):
                    raise
                rows = [
                    r for r in self.admin(binding, "SHOW DATABASES") if r["name"] == binding.alias
                ]
                require(
                    rows and rows[0].get("paused") and path.read_text() == output.getvalue(),
                    "CONFIG_UNCONFIRMED",
                    "설정 내용과 프록시 정지를 확인할 수 없습니다.",
                )
                time.sleep(0.05)
                intent("pgbouncer.reload_rejected_retry")

    def resume(self, db: Database, binding: Binding, tag: str, intent: Callable) -> None:
        intent("pg.allow_writer")
        with self.connect(db, tag) as conn:
            conn.execute(sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(db.writer_role)))
        intent("pgbouncer.resume")
        self.admin(binding, "RESUME {}", tag=tag)
        dsn = os.environ.get(binding.probe_env)
        require(dsn, "SECRET_UNAVAILABLE", "앱 연결 검사 Secret이 없습니다.")
        with psycopg.connect(dsn, connect_timeout=3, row_factory=dict_row) as conn:
            conn.execute("SET LOCAL statement_timeout=5000")
            probe = conn.execute(
                "SELECT current_database() AS db,inet_server_addr()::text AS "
                "address,inet_server_port() AS port,current_user AS writer"
            ).fetchone()
        expected = self.inspect(db)
        require(
            (probe["db"], probe["address"], probe["port"], probe["writer"])
            == (db.database, expected["address"], expected["port"], db.writer_role),
            "ROUTE_MISMATCH",
            "앱 연결의 실제 DB가 다릅니다.",
        )

    def cleanup_replication(
        self, publisher: Database, subscriber: Database, name: str, tag: str, intent: Callable
    ) -> None:
        self.disable(subscriber, name, tag, intent, 10)
        with self.connect(subscriber, tag) as conn:
            if conn.execute("SELECT 1 FROM pg_subscription WHERE subname=%s", (name,)).fetchone():
                intent("pg.drop_subscription")
                # Drop only this plan's subscription. PostgreSQL drops its remote slot too.
                conn.execute(sql.SQL("DROP SUBSCRIPTION {}").format(sql.Identifier(name)))
        with self.connect(publisher, tag) as conn:
            remaining = conn.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name=%s", (name,)
            ).fetchone()
            if remaining:
                require(not remaining["active"], "RESOURCE_BUSY", "복제 slot이 사용 중입니다.")
                intent("pg.drop_slot")
                conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
            intent("pg.drop_publication")
            conn.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(name)))
