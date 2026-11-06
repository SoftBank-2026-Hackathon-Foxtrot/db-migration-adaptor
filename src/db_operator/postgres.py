"""PostgreSQL 관측·복제·데이터 검사와 PgBouncer 제어를 담당합니다."""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import time
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any
from uuid import uuid4

import psycopg
from psycopg import AsyncClientCursor, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from . import config_file
from .config import Binding, Database, Registry
from .models import OperatorError, require

# SQL 식별자

BARRIER = "foxtrot_migration.barriers"


def identifier(name: str) -> sql.Identifier:
    return sql.Identifier(*name.split("."))


# 프록시 경로 편집

PAIR = re.compile(r"(?P<key>[^\s=]+)\s*=\s*(?P<value>'(?:[^']|'')*'|[^\s']+)")


def rewrite_route(text: str, alias: str, route: dict[str, str]) -> str:
    section, matches, offset = "", [], 0
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip()
        elif section == "databases" and not stripped.startswith((";", "#", "%")):
            assignment = re.match(r"[ \t]*" + re.escape(alias) + r"[ \t]*=[ \t]*", line)
            if assignment:
                matches.append((offset + assignment.end(), offset + len(line.rstrip("\r\n"))))
        offset += len(line)
    require(
        len(matches) == 1, "CONFIG_UNAVAILABLE", "등록한 파일에 alias가 정확히 하나 필요합니다."
    )
    start, end = matches[0]
    value = text[start:end]
    edits, seen, position = [], set(), 0
    for pair in PAIR.finditer(value):
        require(
            not value[position : pair.start()].strip(),
            "CONFIG_UNAVAILABLE",
            "alias 문법을 확인하십시오.",
        )
        key = pair["key"]
        if key in route:
            require(key not in seen, "CONFIG_UNAVAILABLE", "중복된 경로 옵션입니다.")
            seen.add(key)
            edits.append((pair.start("value"), pair.end("value"), route[key]))
        position = pair.end()
    require(not value[position:].strip(), "CONFIG_UNAVAILABLE", "alias 문법을 확인하십시오.")
    for left, right, replacement in reversed(edits):
        value = value[:left] + replacement + value[right:]
    missing = " ".join(
        f"{key}={replacement}" for key, replacement in route.items() if key not in seen
    )
    if missing:
        tail = len(value.rstrip())
        value = value[:tail] + (" " if tail else "") + missing + value[tail:]
    return text[:start] + value + text[end:]


# DB 제어 진입점


class Postgres:
    """공통 DB 제어 진입점. schema·복제·프록시 구현에 명령을 위임합니다."""

    def __init__(self, registry: Registry):
        self.registry = registry
        self._schema = SchemaInspector(self)
        self._replication = LogicalReplication(self)
        self._pooler = PgBouncer(self)

    @contextmanager
    def connect(
        self,
        db: Database,
        tag: str = "observe",
        timeout: float = 10,
        *,
        database_name: str | None = None,
    ):
        dsn = db.secret("admin")
        if database_name is not None:
            dsn = make_conninfo(dsn, dbname=database_name)
        require(
            db.provider == "SELF_MANAGED" or conninfo_to_dict(dsn).get("sslmode") == "verify-full",
            "TLS_REQUIRED",
            "관리형 DB 접속에는 sslmode=verify-full이 필요합니다.",
        )
        with psycopg.connect(
            dsn,
            autocommit=True,
            row_factory=dict_row,
            application_name="foxtrot-db:" + tag,
            connect_timeout=3,
            options=f"-c statement_timeout={max(1, int(timeout * 1000))} "
            "-c lock_timeout=3000 -c timezone=UTC -c datestyle=ISO,YMD",
        ) as conn:
            yield conn

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
            system_id = (
                sql.SQL("(pg_control_system()).system_identifier::text")
                if db.provider == "SELF_MANAGED"
                else sql.Literal(db.provider + ":" + str(db.provider_instance))
            )
            result = conn.execute(
                sql.SQL(
                    "SELECT current_database() AS db, {} "
                    "AS system_id, current_setting('server_version_num')::int AS version, "
                    "current_setting('wal_level') AS wal_level, "
                    "inet_server_addr()::text AS address, "
                    "inet_server_port() AS port, (SELECT rolsuper FROM pg_roles WHERE "
                    "rolname=current_user) AS admin, "
                    "(SELECT json_build_object('rolcanlogin',rolcanlogin,'rolsuper',rolsuper,"
                    "'rolcreaterole',rolcreaterole,'rolcreatedb',rolcreatedb,"
                    "'rolreplication',rolreplication,'rolbypassrls',rolbypassrls) "
                    "FROM pg_roles WHERE rolname=%s) AS writer, "
                    "(SELECT count(*) FROM pg_stat_activity WHERE usename=%s) AS sessions, "
                    "(SELECT count(*) FROM pg_prepared_xacts) AS prepared, "
                    "(SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE "
                    "'foxtrot-db:op-%%' AND pid<>pg_backend_pid()) AS \"oldExecutors\""
                ).format(system_id),
                (db.writer_role, db.writer_role),
            ).fetchone()
            return result

    def observe(self, plan: dict, *, source_authority: bool = False) -> dict:
        source, target, binding = self.resources(plan)
        a = self.inspect(source)
        target_reachable = True
        try:
            b = self.inspect(target)
        except psycopg.Error:
            if not source_authority:
                raise
            target_reachable = False
            # This is a persisted fencing proof, not a live target observation.
            b = {
                "writer": {"rolcanlogin": False},
                "sessions": None,
                "prepared": None,
                "oldExecutors": None,
            }
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
            "targetReachable": target_reachable,
            "accessEvidence": "LIVE" if target_reachable else "PERSISTED_TARGET_FENCE",
        }

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

    def table_signature(self, db: Database, name: str) -> dict:
        return self._schema.table_signature(db, name)

    def schema_signature(self, db: Database) -> dict:
        return self._schema.schema_signature(db)

    def preflight(self, plan: dict) -> dict:
        return self._schema.preflight(plan)

    def snapshot(self, db: Database) -> dict:
        return self._schema.snapshot(db)

    def equal(self, source: Database, target: Database) -> dict:
        return self._schema.equal(source, target)

    def prepare_barrier(self, db: Database, tag: str, intent: Callable) -> None:
        return self._replication.prepare_barrier(db, tag, intent)

    def create_replication(
        self,
        publisher: Database,
        subscriber: Database,
        name: str,
        copy: bool,
        tag: str,
        intent: Callable,
    ) -> None:
        return self._replication.create_replication(publisher, subscriber, name, copy, tag, intent)

    def replication_ready(self, db: Database, name: str) -> bool:
        return self._replication.replication_ready(db, name)

    def disable(self, db: Database, name: str, tag: str, intent: Callable, timeout: float) -> None:
        return self._replication.disable(db, name, tag, intent, timeout)

    def barrier(
        self,
        publisher: Database,
        subscriber: Database,
        name: str,
        mid: str,
        tag: str,
        intent: Callable,
        timeout: float,
    ) -> dict:
        return self._replication.barrier(publisher, subscriber, name, mid, tag, intent, timeout)

    def sync_sequences(
        self, source: Database, target: Database, tag: str, intent: Callable
    ) -> None:
        return self._replication.sync_sequences(source, target, tag, intent)

    def cleanup_replication(
        self, publisher: Database, subscriber: Database, name: str, tag: str, intent: Callable
    ) -> None:
        return self._replication.cleanup_replication(publisher, subscriber, name, tag, intent)

    def retire_publisher(self, db: Database, name: str, tag: str, intent: Callable) -> None:
        return self._replication.retire_publisher(db, name, tag, intent)

    def admin(
        self, binding: Binding, command: str, timeout: float = 10, tag: str = "observe"
    ) -> list[dict]:
        return self._pooler.admin(binding, command, timeout, tag)

    def reroute(
        self, binding: Binding, db: Database, intent: Callable, tag: str = "observe"
    ) -> None:
        return self._pooler.reroute(binding, db, intent, tag)

    def resume(self, db: Database, binding: Binding, tag: str, intent: Callable) -> None:
        return self._pooler.resume(db, binding, tag, intent)

    def enable_replication(
        self, db: Database, name: str, tag: str, intent: Callable, timeout: float
    ) -> None:
        self._replication.enable(db, name, tag, intent, timeout)


# 스키마와 데이터 확인


class SchemaInspector:
    def __init__(self, backend: Postgres):
        self.backend = backend

    def table_signature(self, db: Database, name: str) -> dict:
        with self.backend.connect(db) as conn:
            columns = conn.execute(
                "SELECT a.attname,format_type(a.atttypid,a.atttypmod) AS "
                "type,a.attnotnull,a.attidentity,a.attgenerated,pg_get_expr(d.adbin,d.adrelid) AS "
                "def FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND "
                "d.adnum=a.attnum WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT "
                "a.attisdropped ORDER BY a.attnum",
                (name,),
            ).fetchall()
            relation = conn.execute(
                "SELECT relkind,relrowsecurity,relreplident FROM pg_class WHERE oid=%s::regclass",
                (name,),
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
                relation["relkind"] == "r"
                and not relation["relrowsecurity"]
                and pk
                and relation["relreplident"] == "d",
                "UNSUPPORTED_SCHEMA",
                "일반 table·PK·DEFAULT replica identity가 필요합니다. RLS는 지원하지 않습니다.",
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
            indexes = conn.execute(
                "SELECT substring(pg_get_indexdef(indexrelid,0,true) from "
                "length('CREATE UNIQUE INDEX '||quote_ident(c.relname)||' ON ')+1) AS def,"
                "indisvalid,indisready,indislive,indimmediate,indnullsnotdistinct "
                "FROM pg_index JOIN pg_class c ON c.oid=indexrelid "
                "WHERE indrelid=%s::regclass AND indisunique "
                "ORDER BY 1,2,3,4,5,6",
                (name,),
            ).fetchall()
            require(
                all(i["indisvalid"] and i["indisready"] and i["indislive"] for i in indexes),
                "UNSUPPORTED_SCHEMA",
                "완성된 UNIQUE index가 필요합니다.",
            )
            return {
                "columns": columns,
                "pk": [r["attname"] for r in pk],
                "constraints": constraints,
                "replicaIdentity": relation["relreplident"],
                "uniqueIndexes": indexes,
            }

    def schema_signature(self, db: Database) -> dict:
        with self.backend.connect(db) as conn:
            sequences = {}
            for name in self.backend.registry.sequences:
                spec = conn.execute(
                    "SELECT format_type(seqtypid,NULL) AS type,seqstart,seqincrement,"
                    "seqmax,seqmin,seqcache,seqcycle FROM pg_sequence WHERE seqrelid=%s::regclass",
                    (name,),
                ).fetchone()
                require(
                    spec
                    and spec["seqincrement"] == 1
                    and spec["seqcache"] == 1
                    and not spec["seqcycle"],
                    "UNSUPPORTED_SCHEMA",
                    "증가량 1·cache 1·비순환 sequence만 지원합니다.",
                )
                sequences[name] = spec
        return {
            "version": 2,
            "tables": {
                name: self.backend.table_signature(db, name)
                for name in self.backend.registry.tables
            },
            "sequences": sequences,
        }

    def preflight(self, plan: dict) -> dict:
        source, target, binding = self.backend.resources(plan)
        observed = self.backend.observe(plan)
        self._check_proxy(source, target, binding, observed)
        self._check_config(source, binding)
        signatures = {}
        for database, node in [(source, "source"), (target, "target")]:
            self._check_database(database, observed[node])
            with self.backend.connect(database) as connection:
                self._check_isolation(connection, database)
                self._check_tables(connection, database, node)
                self._check_sequences(connection)
            signatures[node] = self.backend.schema_signature(database)
        require(
            signatures["source"] == signatures["target"],
            "SCHEMA_MISMATCH",
            "원본·대상 schema가 다릅니다.",
        )
        return observed

    def _check_proxy(
        self, source: Database, target: Database, binding: Binding, observed: dict
    ) -> None:
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
        pool = {r["key"]: r["value"] for r in self.backend.admin(binding, "SHOW CONFIG")}
        require(
            pool.get("pool_mode") == "transaction",
            "UNSUPPORTED_PROFILE",
            "transaction pooling이 필요합니다.",
        )

    def _check_config(self, source: Database, binding: Binding) -> None:
        require(
            binding.config_path.is_file() and os.access(binding.config_path.parent, os.W_OK),
            "CONFIG_UNAVAILABLE",
            "등록한 PgBouncer 설정 경로를 쓸 수 없습니다.",
        )
        with binding.config_path.open(newline="") as stream:
            rewrite_route(stream.read(), binding.alias, {"host": source.route_host})
        try:
            config_file.preflight(binding.config_path)
        except OSError as error:
            raise OperatorError(
                "CONFIG_METADATA_UNSUPPORTED", "설정 파일 소유자·그룹·권한을 보존할 수 없습니다."
            ) from error

    def _check_database(self, db: Database, info: dict) -> None:
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

    def _check_isolation(self, conn, db: Database) -> None:
        require(
            conn.execute("SELECT to_regclass('foxtrot_operator.migrations') AS manager").fetchone()[
                "manager"
            ]
            is None,
            "UNSAFE_MANAGEMENT_DB",
            "이전 관리 상태가 있는 DB는 업무 이전 대상으로 사용할 수 없습니다.",
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

    def _check_tables(self, conn, db: Database, node: str) -> None:
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
            all_tables == set(self.backend.registry.tables),
            "UNSUPPORTED_SCHEMA",
            "DB의 업무 table 전체를 등록해야 합니다.",
        )
        for name in self.backend.registry.tables:
            self._check_table_writer(conn, db, name)
            if node == "target":
                require(
                    not conn.execute(
                        sql.SQL("SELECT 1 FROM {} LIMIT 1").format(identifier(name))
                    ).fetchone(),
                    "TARGET_NOT_EMPTY",
                    "대상 업무 table이 비어 있어야 합니다.",
                )

    def _check_table_writer(self, conn, db: Database, name: str) -> None:
        owner = conn.execute(
            "SELECT pg_get_userbyid(relowner) AS owner FROM pg_class WHERE oid=%s::regclass",
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

    def _check_sequences(self, conn) -> None:
        actual_sequences = {
            r["name"]
            for r in conn.execute(
                "SELECT schemaname||'.'||sequencename AS name FROM pg_sequences WHERE "
                "schemaname NOT IN ('pg_catalog','information_schema')"
            ).fetchall()
        }
        require(
            actual_sequences == set(self.backend.registry.sequences),
            "UNSUPPORTED_SCHEMA",
            "sequence 전체를 등록해야 합니다.",
        )

    def snapshot(self, db: Database) -> dict:
        result = {}
        for table in [*self.backend.registry.tables, BARRIER]:
            pk = self.backend.table_signature(db, table)["pk"]
            digest, count = hashlib.sha256(), 0
            with self.backend.connect(db, timeout=60) as conn, conn.transaction():
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                with conn.cursor(name="foxtrot_snapshot") as cur:
                    # 기본 100행 단위 FETCH는 원격 DB에서 왕복 지연을 크게 만듭니다.
                    cur.itersize = 10_000
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
        a, b = self.backend.snapshot(source), self.backend.snapshot(target)
        require(a == b, "DATA_MISMATCH", "원본·대상 데이터가 다릅니다.")
        for seq in self.backend.registry.sequences:
            with self.backend.connect(source) as conn:
                first = conn.execute(
                    sql.SQL("SELECT last_value,is_called FROM {}").format(identifier(seq))
                ).fetchone()
            with self.backend.connect(target) as conn:
                second = conn.execute(
                    sql.SQL("SELECT last_value,is_called FROM {}").format(identifier(seq))
                ).fetchone()
            require(first == second, "SEQUENCE_MISMATCH", "sequence가 다릅니다.")
        return a


# 초기 복사와 변경분 복제


class LogicalReplication:
    def __init__(self, backend: Postgres):
        self.backend = backend

    def prepare_barrier(self, db: Database, tag: str, intent: Callable) -> None:
        intent("pg.prepare_barrier")
        with self.backend.connect(db, tag) as conn:
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
        tables = [*self.backend.registry.tables, BARRIER]
        with self.backend.connect(publisher, tag) as conn:
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
        with self.backend.connect(subscriber, tag, 30) as conn:
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
        with self.backend.connect(db) as conn:
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
                and len(rows) == len(self.backend.registry.tables) + 1
                and all(r["srsubstate"] == "r" for r in rows)
            )

    def disable(self, db: Database, name: str, tag: str, intent: Callable, timeout: float) -> None:
        with self.backend.connect(db, tag) as conn:
            if conn.execute("SELECT 1 FROM pg_subscription WHERE subname=%s", (name,)).fetchone():
                intent("pg.disable_subscription")
                conn.execute(sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(sql.Identifier(name)))

        def stopped():
            with self.backend.connect(db) as conn:
                return not conn.execute(
                    "SELECT 1 FROM pg_stat_subscription WHERE subname=%s AND pid IS NOT NULL",
                    (name,),
                ).fetchone()

        self.backend.wait(stopped, timeout)

    def barrier(
        self,
        publisher: Database,
        subscriber: Database,
        name: str,
        mid: str,
        tag: str,
        intent: Callable,
        timeout: float,
    ) -> dict:
        value, successor = uuid4(), uuid4()
        require(
            self.backend.replication_ready(subscriber, name),
            "REPLICATION_NOT_READY",
            "복제 흐름이 준비되지 않았습니다.",
        )
        intent("pg.commit_barrier")
        with self.backend.connect(publisher, tag) as conn:
            with conn.transaction():
                conn.execute("SET LOCAL synchronous_commit=on")
                conn.execute("INSERT INTO foxtrot_migration.barriers VALUES (%s,%s)", (value, mid))
            boundary = conn.execute("SELECT pg_current_wal_insert_lsn()::text AS lsn").fetchone()[
                "lsn"
            ]
            # The boundary is at or beyond the first COMMIT. A second replicated
            # transaction guarantees progress beyond it even on an otherwise idle DB.
            intent("pg.commit_barrier_successor")
            with conn.transaction():
                conn.execute("SET LOCAL synchronous_commit=on")
                conn.execute(
                    "INSERT INTO foxtrot_migration.barriers VALUES (%s,%s)", (successor, mid)
                )

        def applied():
            with self.backend.connect(subscriber) as conn:
                if not conn.execute(
                    "SELECT 1 FROM foxtrot_migration.barriers WHERE id=%s", (successor,)
                ).fetchone():
                    return False
                sub = conn.execute(
                    "SELECT oid FROM pg_subscription WHERE subname=%s", (name,)
                ).fetchone()
                progress = conn.execute(
                    "WITH progress AS MATERIALIZED (SELECT "
                    "pg_replication_origin_progress(%s,true) AS lsn) "
                    "SELECT lsn::text,lsn >= %s::pg_lsn AS durable FROM progress",
                    ("pg_" + str(sub["oid"]), boundary),
                ).fetchone()
                return progress if progress and progress["durable"] else False

        progress = self.backend.wait(applied, timeout)
        return {
            "id": str(value),
            "successorId": str(successor),
            "committedThroughLsn": boundary,
            "durableOriginLsn": progress["lsn"],
        }

    def sync_sequences(
        self, source: Database, target: Database, tag: str, intent: Callable
    ) -> None:
        for seq in self.backend.registry.sequences:
            with self.backend.connect(source) as conn:
                value = conn.execute(
                    sql.SQL("SELECT last_value,is_called FROM {}").format(identifier(seq))
                ).fetchone()
            intent("pg.sync_sequence")
            with self.backend.connect(target, tag) as conn:
                conn.execute(
                    "SELECT setval(%s::regclass,%s,%s)",
                    (seq, value["last_value"], value["is_called"]),
                )

    def cleanup_replication(
        self, publisher: Database, subscriber: Database, name: str, tag: str, intent: Callable
    ) -> None:
        self.backend.disable(subscriber, name, tag, intent, 10)
        with self.backend.connect(subscriber, tag) as conn:
            if conn.execute("SELECT 1 FROM pg_subscription WHERE subname=%s", (name,)).fetchone():
                intent("pg.drop_subscription")
                # The publisher slot may already have been retired during cutover.
                conn.execute(
                    sql.SQL("ALTER SUBSCRIPTION {} SET (slot_name=NONE)").format(
                        sql.Identifier(name)
                    )
                )
                conn.execute(sql.SQL("DROP SUBSCRIPTION {}").format(sql.Identifier(name)))
        with self.backend.connect(publisher, tag) as conn:
            remaining = conn.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name=%s", (name,)
            ).fetchone()
            if remaining:
                require(not remaining["active"], "RESOURCE_BUSY", "복제 slot이 사용 중입니다.")
                intent("pg.drop_slot")
                conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
            intent("pg.drop_publication")
            conn.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(name)))

    def retire_publisher(self, db: Database, name: str, tag: str, intent: Callable) -> None:
        """Retire only this plan's outgoing stream, including offline-target recovery."""
        with self.backend.connect(db, tag) as conn:
            intent("pg.retire_publication")
            conn.execute(sql.SQL("DROP PUBLICATION IF EXISTS {}").format(sql.Identifier(name)))
            deadline = time.monotonic() + 5
            while True:
                slot = conn.execute(
                    "SELECT active_pid FROM pg_replication_slots WHERE slot_name=%s", (name,)
                ).fetchone()
                if not slot:
                    return
                if slot["active_pid"]:
                    intent("pg.stop_owned_walsender")
                    conn.execute("SELECT pg_terminate_backend(%s,1000)", (slot["active_pid"],))
                intent("pg.retire_slot")
                try:
                    conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
                    return
                except psycopg.errors.ObjectInUse:
                    require(
                        time.monotonic() < deadline,
                        "RESOURCE_BUSY",
                        "계획의 복제 slot이 사용 중입니다.",
                    )

    def enable(self, db: Database, name: str, tag: str, intent: Callable, timeout: float) -> None:
        intent("pg.enable_reverse")
        with self.backend.connect(db, tag) as connection:
            connection.execute(sql.SQL("ALTER SUBSCRIPTION {} ENABLE").format(sql.Identifier(name)))
        self.backend.wait(lambda: self.backend.replication_ready(db, name), timeout)


# PgBouncer 제어


class PgBouncer:
    def __init__(self, backend: Postgres):
        self.backend = backend

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

    def reroute(
        self, binding: Binding, db: Database, intent: Callable, tag: str = "observe"
    ) -> None:
        path = binding.config_path
        original, metadata = config_file.snapshot(path)
        output = rewrite_route(
            original.decode(),
            binding.alias,
            {"host": db.route_host, "port": str(db.route_port), "dbname": db.database},
        )
        intent("pgbouncer.atomic_config")
        try:
            config_file.replace(path, original, metadata, output.encode())
        except FileExistsError as error:
            raise OperatorError("CONFIG_CONFLICT", "설정 파일이 외부에서 변경됐습니다.") from error
        intent("pgbouncer.reload")
        deadline = time.monotonic() + 2
        while True:
            try:
                self.backend.admin(binding, "RELOAD", tag=tag)
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
                    r
                    for r in self.backend.admin(binding, "SHOW DATABASES")
                    if r["name"] == binding.alias
                ]
                require(
                    rows and rows[0].get("paused") and path.read_bytes() == output.encode(),
                    "CONFIG_UNCONFIRMED",
                    "설정 내용과 프록시 정지를 확인할 수 없습니다.",
                )
                time.sleep(0.05)
                intent("pgbouncer.reload_rejected_retry")

    def resume(self, db: Database, binding: Binding, tag: str, intent: Callable) -> None:
        intent("pg.allow_writer")
        with self.backend.connect(db, tag) as conn:
            conn.execute(sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(db.writer_role)))
        intent("pgbouncer.resume")
        self.backend.admin(binding, "RESUME {}", tag=tag)
        dsn = binding.probe_dsn()
        with psycopg.connect(dsn, connect_timeout=3, row_factory=dict_row) as conn:
            conn.execute("SET LOCAL statement_timeout=5000")
            probe = conn.execute(
                "SELECT current_database() AS db,inet_server_addr()::text AS "
                "address,inet_server_port() AS port,current_user AS writer"
            ).fetchone()
        expected = self.backend.inspect(db)
        require(
            (probe["db"], probe["address"], probe["port"], probe["writer"])
            == (db.database, expected["address"], expected["port"], db.writer_role),
            "ROUTE_MISMATCH",
            "앱 연결의 실제 DB가 다릅니다.",
        )
