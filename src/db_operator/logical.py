"""관리형 PostgreSQL 17(RDS, Cloud SQL) 사이의 기본 논리 복제를 어느 방향으로든 제어합니다.

DMS는 호출하지 않습니다. 같은 binding의 완료된 이전을 되돌리는 계획은 PREPARE에서 이전 원본을
초기화합니다.
"""

import re
import socket
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from .dms import read_cloudsql_instance
from .models import MANAGED_LOGICAL_PROFILE, OperatorError, require
from .postgres import BARRIER, LogicalReplication, Postgres, SchemaInspector, identifier
from .store import request_digest

MANAGED_PROVIDERS = {"AWS_RDS", "GCP_CLOUDSQL"}


def read_rds_instance(region: str, instance: str) -> dict:
    """RDS 관리 API의 DescribeDBInstances만 사용합니다. 인스턴스를 변경하지 않습니다."""
    try:
        client = boto3.client(
            "rds",
            region_name=region,
            config=Config(connect_timeout=5, read_timeout=15, retries={"max_attempts": 2}),
        )
        items = client.describe_db_instances(DBInstanceIdentifier=instance)["DBInstances"]
    except (BotoCoreError, ClientError, KeyError) as error:
        raise OperatorError(
            "RDS_METADATA_UNAVAILABLE", "RDS 인스턴스 메타데이터를 읽을 수 없습니다.", 503
        ) from error
    require(len(items) == 1, "RDS_METADATA_UNAVAILABLE", "RDS 인스턴스를 특정할 수 없습니다.", 503)
    return items[0]


class ManagedSchemaInspector(SchemaInspector):
    def _check_database(self, db, info):
        # Both sides keep logical WAL so that the next migration can run in either direction.
        require(
            170000 <= info["version"] < 180000
            and info["admin"]
            and info["db"] == db.database
            and info["wal_level"] == "logical",
            "UNSUPPORTED_PROFILE",
            "PostgreSQL 17·승인된 관리 역할과 양쪽 logical WAL이 필요합니다.",
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
            source.provider in MANAGED_PROVIDERS and target.provider in MANAGED_PROVIDERS,
            "UNSUPPORTED_PROFILE",
            "관리형 PostgreSQL(RDS, Cloud SQL) 사이의 이전만 지원합니다.",
        )
        require(
            plan["policy"]["rollbackMode"] == "BEFORE_TARGET_WRITE",
            "ROLLBACK_UNAVAILABLE",
            "관리형 역복제는 구성하지 않았습니다.",
        )
        for database in (source, target):
            self.check_instance_metadata(database)
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

    @classmethod
    def check_instance_metadata(cls, database):
        """등록한 주소가 실행 중인 독립 인스턴스인지 제공자 API로 확인합니다."""
        if database.provider == "GCP_CLOUDSQL":
            cls.check_target_metadata(database)
        else:
            cls.check_rds_metadata(database)

    @staticmethod
    def check_rds_metadata(target):
        match = re.fullmatch(
            r"arn:aws[a-z-]*:rds:([a-z0-9-]+):[0-9]{12}:db:([A-Za-z0-9-]+)",
            target.provider_instance or "",
        )
        require(match, "UNSUPPORTED_PROFILE", "RDS 인스턴스 ARN이 올바르지 않습니다.")
        region, instance = match.groups()
        info = read_rds_instance(region, instance)
        endpoint = info.get("Endpoint") or {}
        require(
            info.get("DBInstanceIdentifier") == instance
            and info.get("DBInstanceStatus") == "available"
            and str(info.get("Engine", "")).startswith("postgres")
            and not info.get("ReadReplicaSourceDBInstanceIdentifier")
            and str(endpoint.get("Address", "")).lower().rstrip(".")
            == target.route_host.lower().rstrip(".")
            and int(endpoint.get("Port") or 0) == target.route_port,
            "TARGET_NOT_STANDALONE",
            "등록한 주소의 실행 중인 독립 RDS가 필요합니다. 읽기 복제본은 사용할 수 없습니다.",
        )

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
        names = {(info.get("dnsName") or "").lower().rstrip(".")} | {
            item.get("name", "").lower().rstrip(".")
            for item in info.get("dnsNames") or []
            if item.get("connectionType") == "PUBLIC"
        }
        host = target.route_host.lower().rstrip(".")
        address_matches = host in addresses | names
        if not address_matches:
            try:
                resolved = {
                    item[4][0]
                    for item in socket.getaddrinfo(host, target.route_port, type=socket.SOCK_STREAM)
                }
            except socket.gaierror:
                resolved = set()
            # Private-only instances may omit the certificate DNS name from the API.
            # Every resolved address must belong to this instance. verify-full and
            # the subsequent SQL endpoint checks still verify the server identity.
            address_matches = bool(resolved) and resolved <= addresses
        require(
            info.get("project") == project
            and info.get("name") == instance
            and info.get("state") == "RUNNABLE"
            and info.get("instanceType") == "CLOUD_SQL_INSTANCE"
            and not info.get("masterInstanceName")
            and not info.get("replicaConfiguration")
            and address_matches,
            "TARGET_NOT_STANDALONE",
            "등록한 주소의 독립 Cloud SQL이 필요합니다. DMS 복제 대상은 사용할 수 없습니다.",
        )

    # 되돌아가는 이전: 같은 binding의 완료된 이전이 남긴 것을 PREPARE에서 정리합니다.

    def completed_migrations(self, plan, mid):
        """이 binding의 COMPLETED 이전 기록. 가장 최근에 재개한 것이 앞에 옵니다."""
        rows = self.store.transact(lambda tx: tx.list("MIG"))
        rows = [
            row
            for row in rows
            if row["id"] != mid
            and row["plan"].get("bindingRef") == plan["bindingRef"]
            and row["state"]["lifecycle"] == "COMPLETED"
        ]
        rows.sort(
            key=lambda row: str(
                row["internal"].get("resumeAt") or row["internal"].get("reconciledAt") or ""
            ),
            reverse=True,
        )
        return rows

    def prepare_reuse(self, plan, mid, tag, intent):
        source, target, _ = self.resources(plan)
        completed = self.completed_migrations(plan, mid)
        if not completed:
            return
        names = set()
        for row in completed:
            names.update(self.names(row["id"]).values())
        for database in (source, target):
            self._drop_retired(database, sorted(names), tag, intent)
        latest = completed[0]
        if not self._returns_to(latest, plan, target):
            return
        observed = self.observe(plan)
        require(
            observed["route"] == "SOURCE"
            and observed["access"] == "SOURCE_ONLY"
            and not observed["paused"]
            and observed["target"]["sessions"] == 0
            and observed["target"]["prepared"] == 0,
            "PRECONDITION_FAILED",
            "되돌아갈 대상은 writer 차단·연결 0건이어야 하고 앱 경로는 원본이어야 합니다.",
        )
        tables = list(self.registry.tables)
        with self.connect(target, tag, 60) as conn:
            populated = conn.execute(
                sql.SQL(" UNION ALL ").join(
                    sql.SQL("(SELECT 1 FROM {} LIMIT 1)").format(identifier(t)) for t in tables
                )
            ).fetchone()
            barrier_table = conn.execute("SELECT to_regclass(%s) AS name", (BARRIER,)).fetchone()[
                "name"
            ]
            if not populated and not barrier_table:
                return
            # The retired source holds data frozen at the earlier cutover; it is never served
            # again, so the return trip starts from an empty copy of the live database.
            intent("pg.reset_retired_target")
            conn.execute(
                sql.SQL("TRUNCATE TABLE {}").format(
                    sql.SQL(",").join(identifier(t) for t in tables)
                )
            )
            if barrier_table:
                conn.execute(sql.SQL("DELETE FROM {}").format(identifier(BARRIER)))

    @staticmethod
    def _returns_to(latest, plan, target):
        previous = latest["plan"]
        if (previous["sourceRef"], previous["targetRef"]) != (plan["targetRef"], plan["sourceRef"]):
            return False
        catalog = (latest.get("catalog") or {}).get("databases") or {}
        if catalog:
            return catalog.get(previous["sourceRef"], {}).get("identity") == target.identity
        return True

    def _drop_retired(self, database, names, tag, intent):
        """완료된 이전의 subscription·slot·publication이 남아 있으면 그 이름만 정리합니다."""
        with self.connect(database, tag) as conn:
            subscriptions = conn.execute(
                "SELECT s.subname,s.subenabled,(SELECT 1 FROM pg_stat_subscription p WHERE "
                "p.subid=s.oid AND p.pid IS NOT NULL LIMIT 1) AS running FROM pg_subscription s "
                "WHERE s.subname=ANY(%s) AND s.subdbid=(SELECT oid FROM pg_database WHERE "
                "datname=current_database())",
                (names,),
            ).fetchall()
            for item in subscriptions:
                require(
                    not item["subenabled"] and not item["running"],
                    "REPLICATION_NOT_STOPPED",
                    "완료된 이전의 subscription이 아직 실행 중입니다.",
                )
                intent("pg.drop_retired_subscription")
                conn.execute(
                    sql.SQL("ALTER SUBSCRIPTION {} SET (slot_name=NONE)").format(
                        sql.Identifier(item["subname"])
                    )
                )
                conn.execute(
                    sql.SQL("DROP SUBSCRIPTION {}").format(sql.Identifier(item["subname"]))
                )
            for slot in conn.execute(
                "SELECT slot_name,active FROM pg_replication_slots WHERE slot_name=ANY(%s)",
                (names,),
            ).fetchall():
                require(
                    not slot["active"], "RESOURCE_BUSY", "완료된 이전의 복제 slot이 사용 중입니다."
                )
                intent("pg.drop_retired_slot")
                conn.execute("SELECT pg_drop_replication_slot(%s)", (slot["slot_name"],))
            for publication in conn.execute(
                "SELECT pubname FROM pg_publication WHERE pubname=ANY(%s)", (names,)
            ).fetchall():
                intent("pg.drop_retired_publication")
                conn.execute(
                    sql.SQL("DROP PUBLICATION {}").format(sql.Identifier(publication["pubname"]))
                )

    def barrier(self, publisher, subscriber, name, mid, tag, intent, timeout):
        record = self.store.get(mid)
        baseline, digests = (
            record["internal"].get("schemas"),
            record["internal"].get("schemaDigests"),
        )
        require(baseline, "SCHEMA_MISMATCH", "복제 중 schema가 변경됐습니다.")
        source, target, _ = self.resources(record["plan"])
        node = {source.identity: "source", target.identity: "target"}
        for database in (publisher, subscriber):
            # Inside the write pause one catalog digest per database replaces the full
            # signature sweep; a plan prepared before digests existed keeps the sweep.
            unchanged = (
                self.schema_digest(database) == digests[node[database.identity]]
                if digests
                else self.schema_signature(database) == baseline
            )
            require(unchanged, "SCHEMA_MISMATCH", "복제 중 schema가 변경됐습니다.")
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
