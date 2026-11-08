"""AWS RDS → self-managed PG17. No reverse direction or reuse/reset of a retired database."""

from psycopg.conninfo import conninfo_to_dict

from .logical import ManagedLogicalPostgres, ManagedSchemaInspector
from .models import ONPREM_LOGICAL_PROFILE, require
from .postgres import Postgres, SchemaInspector


class OnpremSchemaInspector(ManagedSchemaInspector):
    def _check_isolation(self, conn, db):
        super()._check_isolation(conn, db)
        if db.provider == "SELF_MANAGED":
            # NOLOGIN does not disconnect existing sessions, including the schema owner.
            active = conn.execute(
                "SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND "
                "backend_type='client backend' AND pid<>pg_backend_pid() UNION ALL "
                "SELECT 1 FROM pg_prepared_xacts WHERE database=current_database() LIMIT 1"
            ).fetchone()
            require(
                not active,
                "TARGET_IN_USE",
                "대상 DB의 다른 연결과 준비된 트랜잭션을 먼저 정리하십시오.",
            )

    def _check_table_writer(self, conn, db, name):
        if db.provider == "SELF_MANAGED":
            # No AWS/Google service accounts are trusted on a self-managed server.
            return SchemaInspector._check_table_writer(self, conn, db, name)
        return super()._check_table_writer(conn, db, name)


class OnpremLogicalPostgres(ManagedLogicalPostgres):
    profile = ONPREM_LOGICAL_PROFILE

    def __init__(self, registry, store):
        super().__init__(registry, store)
        self._schema = OnpremSchemaInspector(self)

    @staticmethod
    def check_pair(source, target):
        require(
            (source.provider, target.provider) == ("AWS_RDS", "SELF_MANAGED"),
            "UNSUPPORTED_PROFILE",
            "온프렘 프로필은 AWS RDS → 자체 운영 PostgreSQL만 지원합니다.",
        )

    @staticmethod
    def check_target_tls(db):
        for which in ("admin", "replication"):
            require(
                conninfo_to_dict(db.secret(which)).get("sslmode") == "verify-full",
                "TLS_REQUIRED",
                "온프렘 DB 연결은 서버 인증서와 호스트 이름을 검증해야 합니다.",
            )

    def inspect(self, db):
        if db.provider != "SELF_MANAGED":
            return super().inspect(db)
        self.check_target_tls(db)
        # Only real superuser privileges count here, not a role named cloudsqlsuperuser.
        return Postgres.inspect(self, db)

    def verify_endpoint(self, db):
        if db.provider == "AWS_RDS":
            self.check_replication_connection(db)
        else:
            self.check_target_tls(db)
        return super().verify_endpoint(db)

    def check_instance_metadata(self, db):
        if db.provider == "AWS_RDS":
            self.check_rds_metadata(db)
        # Static catalog references must prove endpoint identity too, just like direct inputs.
        self.verify_endpoint(db)
        if db.provider == "AWS_RDS":
            return
        with self.connect(db) as conn:
            primary = conn.execute(
                "SELECT NOT pg_is_in_recovery() AND "
                "current_setting('transaction_read_only')='off' AS writable"
            ).fetchone()
        require(
            primary["writable"], "TARGET_NOT_STANDALONE", "쓰기가 가능한 온프렘 주 DB가 필요합니다."
        )

    def prepare_reuse(self, plan, mid, tag, intent):
        # Do not drop or truncate anything, even if the same binding once ran a cloud migration.
        return None
