"""RDS 17 → 독립 Cloud SQL 17의 기본 논리 복제를 제어합니다. DMS는 호출하지 않습니다."""

import re
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar

from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from .dms import read_cloudsql_instance
from .models import MANAGED_LOGICAL_PROFILE, require
from .postgres import BARRIER, LogicalReplication, Postgres, SchemaInspector, identifier
from .store import request_digest


class ManagedSchemaInspector(SchemaInspector):
    def _check_database(self, db, info):
        require(
            170000 <= info["version"] < 180000
            and info["admin"]
            and info["db"] == db.database
            and (db.provider != "AWS_RDS" or info["wal_level"] == "logical"),
            "UNSUPPORTED_PROFILE",
            "PostgreSQL 17·승인된 관리 역할과 원본 logical WAL이 필요합니다.",
        )
        require(
            info["writer"]
            and not any(
                info["writer"][key]
                for key in [
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

    def _check_table_writer(self, conn, db, name):
        # 관리 계정과 명시한 복제 계정만 예외입니다. 다른 앱 writer는 허용하지 않습니다.
        trusted = [db.writer_role, conninfo_to_dict(db.secret("replication"))["user"]]
        trusted += (
            ["rdsadmin", "rdsrepladmin"]
            if db.provider == "AWS_RDS"
            else [
                "cloudsqladmin",
                "cloudsqlexternalsync",
                "cloudsqlreplica",
                "cloudsqlbackup",
                "cloudsqlimportexport",
            ]
        )
        others = conn.execute(
            "SELECT 1 FROM pg_roles r WHERE r.rolcanlogin AND r.rolname<>current_user "
            "AND r.rolname<>ALL(%s) AND EXISTS (SELECT 1 FROM pg_roles p WHERE "
            "has_table_privilege(p.oid,%s::regclass,'INSERT,UPDATE,DELETE,TRUNCATE') "
            "AND pg_has_role(r.oid,p.oid,'SET')) LIMIT 1",
            (trusted, name),
        ).fetchone()
        require(not others, "UNREGISTERED_WRITER", "미등록 계정이 업무 table을 쓸 수 있습니다.")


class ManagedLogicalReplication(LogicalReplication):
    def create_replication(self, publisher, subscriber, name, copy, tag, intent):
        require(copy, "UNSUPPORTED_PROFILE", "이 프로필은 정방향 초기 복제만 지원합니다.")
        backend = self.backend
        fingerprint = request_digest(
            {
                "connection": publisher.secret("replication"),
                "publisher": publisher.identity,
                "subscriber": subscriber.identity,
                "subscription": name,
            }
        )
        record = backend.store.get(backend.migration_id)
        saved = record["internal"].get("nativeConnectionFingerprint")
        require(
            not saved or saved == fingerprint,
            "REPLICATION_CONFLICT",
            "등록한 복제 연결이 변경됐습니다.",
        )
        backend.store.update(
            backend.migration_id,
            lambda row: row["internal"].update(nativeConnectionFingerprint=fingerprint),
        )
        tables = [*backend.registry.tables, BARRIER]
        with backend.connect(publisher, tag) as conn:
            publication = conn.execute(
                "SELECT pubowner=(SELECT oid FROM pg_roles WHERE rolname=current_user) AS owned "
                "FROM pg_publication WHERE pubname=%s",
                (name,),
            ).fetchone()
            if publication:
                actual = {
                    r["schemaname"] + "." + r["tablename"]
                    for r in conn.execute(
                        "SELECT schemaname,tablename FROM pg_publication_tables WHERE pubname=%s",
                        (name,),
                    ).fetchall()
                }
                require(
                    publication["owned"] and actual == set(tables),
                    "REPLICATION_CONFLICT",
                    "publication 소유자·범위가 다릅니다.",
                )
            else:
                intent("pg.create_publication")
                conn.execute(
                    sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                        sql.Identifier(name), sql.SQL(",").join(identifier(t) for t in tables)
                    )
                )
        with backend.connect(subscriber, tag, 30) as conn:
            existing = conn.execute(
                "SELECT subpublications,suborigin,subslotname,subowner="
                "(SELECT oid FROM pg_roles WHERE rolname=current_user) AS owned "
                "FROM pg_subscription WHERE subname=%s AND subdbid="
                "(SELECT oid FROM pg_database WHERE datname=current_database())",
                (name,),
            ).fetchone()
            if existing:
                # Cloud SQL은 subconninfo 조회를 허용하지 않습니다. 실행 기록의 해시와
                # 읽을 수 있는 소유자·publication·slot을 대조하고, 외부 관리 변경은 금지합니다.
                require(
                    saved == fingerprint
                    and existing["owned"]
                    and existing["suborigin"] == "none"
                    and existing["subslotname"] == name
                    and existing["subpublications"] == [name],
                    "REPLICATION_CONFLICT",
                    "기존 subscription의 실행 근거·소유·설정이 다릅니다.",
                )
                return
            intent("pg.create_subscription")
            conn.execute(
                sql.SQL(
                    "CREATE SUBSCRIPTION {} CONNECTION {} PUBLICATION {} WITH "
                    "(copy_data=true,origin=none,streaming=off,synchronous_commit=on,two_phase=false)"
                ).format(
                    sql.Identifier(name),
                    sql.Literal(publisher.secret("replication")),
                    sql.Identifier(name),
                )
            )


class ManagedLogicalPostgres(Postgres):
    profile = MANAGED_LOGICAL_PROFILE

    def __init__(self, registry, store):
        super().__init__(registry)
        self.store = store
        self._schema = ManagedSchemaInspector(self)
        self._replication = ManagedLogicalReplication(self)
        self._connections = ContextVar("native_operation_connections", default=None)

    @contextmanager
    def operation(self, tag):
        connections = {}
        with ExitStack() as stack:
            token = self._connections.set((connections, tag, stack))
            try:
                yield
            finally:
                self._connections.reset(token)

    @contextmanager
    def connect(self, db, tag="observe", timeout=10, *, database_name=None):
        scope = self._connections.get()
        if scope is None:
            with super().connect(db, tag, timeout, database_name=database_name) as conn:
                yield conn
            return
        connections, operation_tag, stack = scope
        key = (db.identity, database_name)
        if key not in connections:
            connection = stack.enter_context(
                super().connect(db, operation_tag, timeout, database_name=database_name)
            )
            connections[key] = (connection, timeout)
        connection, previous_timeout = connections[key]
        if previous_timeout != timeout:
            connection.execute(
                "SELECT set_config('statement_timeout',%s,false)",
                (str(max(1, int(timeout * 1000))),),
            )
            connections[key] = (connection, timeout)
        yield connection

    def inspect(self, db):
        info = super().inspect(db)
        role = "rds_superuser" if db.provider == "AWS_RDS" else "cloudsqlsuperuser"
        with self.connect(db) as conn:
            info["admin"] = bool(
                info["admin"]
                or conn.execute(
                    "SELECT 1 FROM pg_roles r WHERE r.rolname=%s AND pg_has_role("
                    "(SELECT oid FROM pg_roles WHERE rolname=current_user),r.oid,'MEMBER')",
                    (role,),
                ).fetchone()
            )
        return info

    def preflight(self, plan):
        source, target, _ = self.resources(plan)
        require(
            source.provider == "AWS_RDS" and target.provider == "GCP_CLOUDSQL",
            "UNSUPPORTED_PROFILE",
            "RDS → Cloud SQL만 지원합니다.",
        )
        require(
            plan["policy"]["rollbackMode"] == "BEFORE_TARGET_WRITE",
            "ROLLBACK_UNAVAILABLE",
            "관리형 역복제는 구성하지 않았습니다.",
        )
        self.check_target_metadata(target)
        connection = conninfo_to_dict(source.secret("replication"))
        require(
            connection.get("host") == source.route_host
            and int(connection.get("port", 5432)) == source.route_port
            and connection.get("dbname") == source.database
            and connection.get("sslmode") in {"require", "verify-ca", "verify-full"},
            "REPLICATION_CONNECTION_INVALID",
            "복제 연결의 원본 주소·DB·TLS를 확인하십시오.",
        )
        return super().preflight(plan)

    @staticmethod
    def check_target_metadata(target):
        match = re.fullmatch(
            r"projects/([a-z0-9.-]+)/instances/([a-z0-9-]+)", target.provider_instance or ""
        )
        require(match, "UNSUPPORTED_PROFILE", "Cloud SQL 인스턴스 참조가 올바르지 않습니다.")
        project, instance = match.groups()
        info = read_cloudsql_instance(project, instance)
        addresses = {
            ip["ipAddress"]
            for ip in info.get("ipAddresses", [])
            if ip.get("type") in {"PRIMARY", "PRIVATE"}
        }
        names = {info.get("dnsName", "").lower().rstrip(".")} | {
            item.get("name", "").lower().rstrip(".")
            for item in info.get("dnsNames", [])
            if item.get("connectionType") == "PUBLIC"
        }
        require(
            info.get("project") == project
            and info.get("name") == instance
            and info.get("state") == "RUNNABLE"
            and info.get("instanceType") == "CLOUD_SQL_INSTANCE"
            and not info.get("masterInstanceName")
            and not info.get("replicaConfiguration")
            and target.route_host.lower().rstrip(".") in addresses | names,
            "TARGET_NOT_STANDALONE",
            "등록한 주소의 독립 Cloud SQL이 필요합니다. DMS 복제 대상은 사용할 수 없습니다.",
        )

    def barrier(self, publisher, subscriber, name, mid, tag, intent, timeout):
        baseline = self.store.get(mid)["internal"].get("schemas")
        require(
            baseline
            and self.schema_signature(publisher) == baseline
            and self.schema_signature(subscriber) == baseline,
            "SCHEMA_MISMATCH",
            "복제 중 schema가 변경됐습니다.",
        )
        return super().barrier(publisher, subscriber, name, mid, tag, intent, timeout)

    def validate_record(self, row, *, source_only=False, verify_sequences=True):
        internal, state, plan = row["internal"], row["state"], row["plan"]
        require(
            internal.get("prepared") and internal.get("schemas"),
            "PREPARE_REQUIRED",
            "사전 검사가 필요합니다.",
        )
        source, target, _ = self.resources(plan)
        if source_only:
            require(
                internal.get("directRecovery") and not internal.get("targetWritesIntent"),
                "ROLLBACK_UNAVAILABLE",
                "원본 복귀 근거가 없습니다.",
            )
            name = self.names(row["id"])["forward"]
            with self.connect(source) as conn:
                require(
                    not conn.execute(
                        "SELECT 1 FROM pg_publication WHERE pubname=%s UNION ALL "
                        "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s",
                        (name, name),
                    ).fetchone(),
                    "REPLICATION_NOT_STOPPED",
                    "원본 복제 객체가 남아 있습니다.",
                )
            require(
                self.schema_signature(source) == internal["schemas"],
                "SCHEMA_MISMATCH",
                "원본 schema가 변경됐습니다.",
            )
            return {
                "method": "SOURCE_RECOVERY",
                "migrationId": row["id"],
                "transitionId": state["transitionId"],
            }
        proof = internal.get("forwardBarrier", {})
        require(
            proof.get("migrationId") == row["id"]
            and proof.get("transitionId") == state["transitionId"]
            and proof.get("successorId")
            and proof.get("committedThroughLsn"),
            "REPLICATION_NOT_READY",
            "현재 전환의 최종 복제 근거가 없습니다.",
        )
        with self.connect(target) as conn:
            subscription = conn.execute(
                "SELECT oid,subenabled FROM pg_subscription WHERE subname=%s AND subdbid="
                "(SELECT oid FROM pg_database WHERE datname=current_database())",
                (self.names(row["id"])["forward"],),
            ).fetchone()
            require(
                subscription and not subscription["subenabled"],
                "REPLICATION_NOT_STOPPED",
                "subscription 정지가 필요합니다.",
            )
            require(
                not conn.execute(
                    "SELECT 1 FROM pg_stat_subscription WHERE subid=%s AND pid IS NOT NULL",
                    (subscription["oid"],),
                ).fetchone(),
                "REPLICATION_NOT_STOPPED",
                "복제 작업이 남아 있습니다.",
            )
            require(
                conn.execute(
                    "SELECT pg_replication_origin_progress(%s,true)>=%s::pg_lsn AS durable",
                    ("pg_" + str(subscription["oid"]), proof["committedThroughLsn"]),
                ).fetchone()["durable"],
                "REPLICATION_NOT_READY",
                "최종 변경의 내구성 있는 적용이 필요합니다.",
            )
            require(
                conn.execute(
                    "SELECT count(*) AS n FROM foxtrot_migration.barriers WHERE "
                    "id=ANY(%s::uuid[]) AND migration_id=%s",
                    ([proof["id"], proof["successorId"]], row["id"]),
                ).fetchone()["n"]
                == 2,
                "REPLICATION_NOT_READY",
                "최종 barrier가 대상에 없습니다.",
            )
        # 순번은 PostgreSQL 논리 복제로 복사되지 않습니다. 동기화 후 실제 값을 대조합니다.
        for sequence in self.registry.sequences if verify_sequences else []:
            with self.connect(source) as conn:
                expected = conn.execute(
                    sql.SQL("SELECT last_value,is_called FROM {}").format(identifier(sequence))
                ).fetchone()
            with self.connect(target) as conn:
                actual = conn.execute(
                    sql.SQL("SELECT last_value,is_called FROM {}").format(identifier(sequence))
                ).fetchone()
            require(expected == actual, "SEQUENCE_MISMATCH", "원본·대상 sequence가 다릅니다.")
        return {
            "method": "NATIVE_DURABLE_BARRIER",
            "migrationId": row["id"],
            "transitionId": state["transitionId"],
            "barrierId": proof["id"],
            "committedThroughLsn": proof["committedThroughLsn"],
        }
