"""실제 SQL·PgBouncer·DynamoDB와 모의 GCP SDK로 공통 API 연결을 검사합니다."""

from contextlib import contextmanager
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import psycopg
import pytest
from google.api_core import exceptions as cloud_errors
from google.cloud import clouddms_v1 as google_dms
from google.longrunning.operations_pb2 import Operation
from google.protobuf.any_pb2 import Any
from google.protobuf.empty_pb2 import Empty
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from test_bindings import apps as apps
from test_operator_integration import validate

from db_operator import dms
from db_operator.config import ResourceCatalog
from db_operator.control import actor_ack
from db_operator.models import DMS_PROFILE, OperatorError
from db_operator.postgres import Postgres
from db_operator.store import now

pytestmark = pytest.mark.integration


class CloudFixture:
    """DMS의 복사·SDK 응답을 재현합니다. 실제 클라우드 성능을 재현하지 않습니다."""

    def __init__(self, binding, source, target):
        self.binding, self.source, self.target = binding, source, target
        self.job = google_dms.MigrationJob(
            name=binding.job,
            source=binding.source,
            destination=binding.destination,
            labels={"foxtrot-owner": "foxtrot"},
            type_="CONTINUOUS",
            state="NOT_STARTED",
            source_database={"provider": "RDS", "engine": "POSTGRESQL"},
            destination_database={"provider": "CLOUDSQL", "engine": "POSTGRESQL"},
        )
        self.profiles = {
            binding.source: google_dms.ConnectionProfile(
                name=binding.source,
                postgresql={
                    "host": source.route_host,
                    "port": source.route_port,
                    "username": conninfo_to_dict(source.secret("replication"))["user"],
                },
            ),
            binding.destination: google_dms.ConnectionProfile(
                name=binding.destination,
                cloudsql={"cloud_sql_id": "target", "public_ip": target.route_host},
            ),
        }
        self.operations = {}
        self.deleted = False
        self.promotion_pending = False
        self.lose = None
        self.sdk = Mock(spec=google_dms.DataMigrationServiceClient)
        self.sdk.get_migration_job.side_effect = self.observe
        self.sdk.get_connection_profile.side_effect = lambda **kw: deepcopy(
            self.profiles[kw["request"]["name"]]
        )
        self.sdk.get_operation.side_effect = lambda **kw: self.operations[kw["request"]["name"]]
        for action in dms.DMS_ACTIONS:
            name = {
                "VERIFY": "verify",
                "START_SYNC": "start",
                "PROMOTE": "promote",
                "STOP": "stop",
                "DELETE": "delete",
            }[action]
            getattr(self.sdk, name + "_migration_job").side_effect = lambda action=action, **kw: (
                self.mutate(action)
            )

    def copy(self):
        with (
            psycopg.connect(self.source.secret("admin"), autocommit=True) as first,
            psycopg.connect(self.target.secret("admin"), autocommit=True) as second,
        ):
            second.execute("CREATE SCHEMA IF NOT EXISTS foxtrot_migration")
            second.execute(
                "CREATE TABLE IF NOT EXISTS foxtrot_migration.barriers("
                "id uuid PRIMARY KEY,migration_id text NOT NULL)"
            )
            with second.transaction():
                second.execute("TRUNCATE orders,inventory,foxtrot_migration.barriers")
                for row in first.execute("SELECT sku,stock FROM inventory").fetchall():
                    second.execute("INSERT INTO inventory VALUES(%s,%s)", row)
                for row in first.execute("SELECT id,request_id,sku FROM orders").fetchall():
                    second.execute("INSERT INTO orders VALUES(%s,%s,%s)", row)
                for row in first.execute(
                    "SELECT id,migration_id FROM foxtrot_migration.barriers"
                ).fetchall():
                    second.execute("INSERT INTO foxtrot_migration.barriers VALUES(%s,%s)", row)

    def observe(self, **kwargs):
        if self.deleted:
            raise cloud_errors.NotFound("test job deleted")
        if self.job.state.name == "RUNNING" and self.job.phase.name == "CDC":
            self.copy()
        return deepcopy(self.job)

    def mutate(self, action):
        if action == "START_SYNC":
            self.copy()
            self.job.state, self.job.phase = "RUNNING", "CDC"
        elif action == "PROMOTE":
            self.copy()
            with psycopg.connect(self.target.secret("admin"), autocommit=True) as conn:
                conn.execute(
                    sql.SQL("REVOKE ALL ON inventory,orders FROM {}").format(
                        sql.Identifier(self.target.writer_role)
                    )
                )
                conn.execute(
                    sql.SQL("REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM {}").format(
                        sql.Identifier(self.target.writer_role)
                    )
                )
            self.job.state = "RUNNING" if self.promotion_pending else "COMPLETED"
            self.job.phase = "PROMOTE_IN_PROGRESS" if self.promotion_pending else "CDC"
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
        if action == "DELETE":
            response.Pack(Empty())
        else:
            response.Pack(google_dms.MigrationJob.pb(self.job))
        name = self.binding.job.split("/migrationJobs/")[0] + "/operations/" + uuid4().hex
        op = Operation(name=name, metadata=metadata, response=response, done=True)
        self.operations[name] = op
        if self.lose == action:
            raise cloud_errors.DeadlineExceeded("test acceptance response lost")
        return SimpleNamespace(operation=op)


class ApiFlow:
    def __init__(self, apps, cloud, bid):
        self.apps, self.cloud, self.bid = apps, cloud, bid
        self.client, self.service, self.worker = apps.client, apps.service, apps.worker
        self.records = apps.records
        self.mid, self.transition = None, "tx-" + uuid4().hex
        self.generation, self.held_since = 1, now()

    def create(self, mode="BEFORE_TARGET_WRITE", replication="dms:test"):
        self.body = dict(
            workflowId="wf-" + uuid4().hex,
            resourceId="database",
            kind="DATABASE",
            profile=DMS_PROFILE,
            sourceRef="db:dms-source",
            targetRef="db:dms-target",
            bindingRef=self.bid,
            workloadScopeRef=self.apps.state(self.bid)["workloadScopeRef"],
            replicationRef=replication,
            policy={"rollbackMode": mode, "maxQuiesceSeconds": 300},
        )
        response = self.client.post(
            "/v1/migrations", json=self.body, headers={"Idempotency-Key": uuid4().hex}
        )
        if response.status_code == 201:
            validate("Migration", response.json())
            self.mid = response.json()["migrationId"]
        return response

    def state(self):
        response = self.client.get("/v1/migrations/" + self.mid)
        assert response.status_code == 200, response.json()
        validate("Migration", response.json())
        return response.json()

    def held(self):
        return self.records.held(
            migration_id=self.mid,
            transition_id=self.transition,
            generation=self.generation,
            held_since=self.held_since,
            participants=[actor_ack(p, self.generation, "HELD") for p in ["a-web", "a-worker"]],
        )

    def action(self, action, expected="SUCCEEDED", provider=None, location=None):
        ref = self.held() if action in {"QUIESCE", "CUTOVER", "RESUME", "ROLLBACK"} else None
        body = {"action": action, "expectedRevision": self.state()["revision"]}
        if ref:
            body.update(transitionId=self.transition, quiesceRef=ref)
        if provider:
            body["providerOperationRef"] = provider
        if location:
            body["expectedLocation"] = location
        accepted = self.client.post(
            "/v1/migrations/" + self.mid + "/actions",
            json=body,
            headers={"Idempotency-Key": uuid4().hex},
        )
        assert accepted.status_code == 202, accepted.json()
        with self.worker.exclusive():
            assert self.worker.run_once()
        operation = self.client.get(accepted.json()["statusUrl"]).json()
        validate("Operation", operation)
        assert operation["status"] == expected, operation
        return operation

    def ready(self):
        assert self.create().status_code == 201
        self.action("PREPARE")
        self.action("START_SYNC")
        assert self.state()["lifecycle"] == "READY"


@pytest.fixture
def flow(apps, monkeypatch):
    catalog = ResourceCatalog(apps.service.store)

    def resource(ref):
        return apps.service.store.transact(lambda tx: apps.service.bindings.resource(tx, ref)[0])

    original_source, original_target = resource("db:a"), resource("db:target")
    source_instance, target_instance = (
        "arn:aws:rds:ap-northeast-2:123456789012:db:test-" + apps.suffix,
        "projects/foxtrot-test/instances/target",
    )
    source = original_source.model_copy(
        update={
            "provider": "AWS_RDS",
            "provider_instance": source_instance,
            "identity": f"AWS_RDS:{source_instance}/{original_source.database}",
        }
    )
    target = original_target.model_copy(
        update={
            "provider": "GCP_CLOUDSQL",
            "provider_instance": target_instance,
            "identity": f"GCP_CLOUDSQL:{target_instance}/{original_target.database}",
        }
    )
    for ref, value in [("db:dms-source", source), ("db:dms-target", target)]:
        catalog.database(
            ref,
            value,
            tables=["public.inventory", "public.orders"],
            sequences=["public.orders_id_seq"],
        )
    parent = "projects/foxtrot-test/locations/asia-northeast3"
    binding = dms.JobBinding(
        job=parent + "/migrationJobs/" + apps.suffix,
        source=parent + "/connectionProfiles/source",
        destination=parent + "/connectionProfiles/target",
        owner="foxtrot",
    )
    catalog.dms_job(
        "dms:test", source_ref="db:dms-source", target_ref="db:dms-target", binding=binding
    )
    cloud = CloudFixture(binding, source, target)
    client = dms.DmsClient(binding, cloud.sdk)
    monkeypatch.setattr(dms, "DmsClient", lambda binding: client)
    # 클라우드 endpoint·전용 인스턴스 검사는 별도 단위 검사에서 다룹니다.
    # 로컬 DB transport는 공유 fixture이고 공인 TLS가 없습니다.
    monkeypatch.setattr(dms.GcpDmsPostgres, "check_admin_endpoint", lambda driver, db: None)
    monkeypatch.setattr(dms.GcpDmsPostgres, "check_instance_databases", lambda *args: None)
    real_connect = Postgres.connect

    @contextmanager
    def local_transport(driver, db, *args, **kwargs):
        # 시험 전용 TLS 우회입니다. 운영 SQL 제어기는 verify-full을 강제합니다.
        with real_connect(
            driver, db.model_copy(update={"provider": "SELF_MANAGED"}), *args, **kwargs
        ) as conn:
            yield conn

    monkeypatch.setattr(Postgres, "connect", local_transport)
    monkeypatch.setattr(
        Postgres, "create_replication", lambda *a, **k: pytest.fail("native replication fallback")
    )
    bid = apps.register("a", db="dms-source")
    yield ApiFlow(apps, cloud, bid)


def test_dms_api_copies_promotes_and_keeps_app_endpoint(flow):
    flow.ready()
    endpoint = flow.apps.state(flow.bid)["endpoint"]
    with flow.apps.connect(flow.bid) as conn:
        conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
    flow.action("QUIESCE")
    flow.action("CUTOVER")
    assert flow.state()["lifecycle"] == "SWITCHED"
    flow.action("RESUME", location="TARGET")
    assert flow.state()["lifecycle"] == "ACTIVE"
    assert flow.apps.state(flow.bid)["databaseRef"] == "db:dms-target"
    assert flow.apps.state(flow.bid)["endpoint"] == endpoint
    with flow.apps.connect(flow.bid) as conn:
        row = conn.execute(
            "INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa') RETURNING id", (uuid4(),)
        ).fetchone()
        assert row[0] == 12
    flow.cloud.sdk.promote_migration_job.assert_called_once()
    assert "ROLLBACK" not in flow.state()["allowedActions"]


def test_dms_rolls_back_before_target_writes(flow):
    flow.ready()
    flow.action("QUIESCE")
    flow.action("ROLLBACK")
    flow.action("RESUME", location="SOURCE")
    assert flow.state()["lifecycle"] == "RESTORED"
    with flow.apps.connect(flow.bid) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 10
    flow.service.settings.retention_seconds = 0
    flow.client.app.state.service.settings.retention_seconds = 0
    flow.action("FINALIZE")
    assert flow.state()["lifecycle"] == "COMPLETED"
    assert flow.cloud.deleted
    flow.apps.action(flow.bid, "UNREGISTER", app="a")


def test_dms_unknown_promote_requires_exact_cloud_operation_without_replay(flow):
    flow.ready()
    flow.action("QUIESCE")
    flow.cloud.lose = "PROMOTE"
    flow.action("CUTOVER", expected="UNKNOWN")
    assert flow.state()["allowedActions"] == ["RECONCILE"]
    flow.action("RECONCILE", expected="UNKNOWN")
    name = list(flow.cloud.operations)[-1]
    flow.action("RECONCILE", provider=name)
    flow.action("CUTOVER")
    flow.action("RESUME", location="TARGET")
    flow.cloud.sdk.promote_migration_job.assert_called_once()


def test_dms_successful_rpc_does_not_open_target_before_completed_job(flow):
    flow.ready()
    flow.action("QUIESCE")
    flow.cloud.promotion_pending = True
    flow.action("CUTOVER", expected="UNKNOWN")
    flow.action("RECONCILE")
    flow.action("CUTOVER", expected="FAILED")
    assert flow.state()["observed"]["route"] == "SOURCE"
    flow.cloud.sdk.promote_migration_job.assert_called_once()


def test_dms_rejects_reverse_policy_and_unapproved_job(flow):
    assert flow.create(mode="REVERSE_REQUIRED").status_code == 400
    response = flow.create(replication="dms:unapproved")
    assert response.status_code == 422
    assert response.json()["code"] == "UNKNOWN_REFERENCE"


def test_cli_cannot_mutate_api_owned_dms_job(flow):
    flow.ready()
    service = dms.DmsService(
        flow.service.scoped(flow.service.store.get(flow.mid)).driver.client,
        dms.DmsJournal(flow.service.store),
    )
    with pytest.raises(OperatorError, match="공통 API"):
        service.submit("STOP", "standalone-cli")


def test_cancel_before_prepare_deletes_only_job_on_finalize(flow):
    assert flow.create().status_code == 201
    flow.action("CANCEL")
    flow.action("FINALIZE")
    assert flow.cloud.deleted
    with flow.apps.connect(flow.bid) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 10


def test_dms_create_is_idempotent_and_snapshot_is_immutable(flow):
    assert flow.create().status_code == 201
    assert (
        flow.service.store.get(flow.mid)["catalog"]["dms_jobs"]["dms:test"]["binding"]["job"]
        == flow.cloud.binding.job
    )
    assert not flow.cloud.sdk.start_migration_job.called
    assert not flow.cloud.sdk.promote_migration_job.called


def test_dms_configuration_change_blocks_execution(flow):
    flow.ready()
    flow.cloud.profiles[flow.cloud.binding.source].postgresql.host = "wrong-rds.example"
    state = flow.state()
    assert state["readiness"]["cutover"]["result"] != "PASS"
    assert "CUTOVER" not in state["allowedActions"]


@pytest.mark.parametrize("after_login", [False, True])
def test_interrupted_target_resume_continues_same_target_only(flow, monkeypatch, after_login):
    flow.ready()
    flow.action("QUIESCE")
    flow.action("CUTOVER")
    original = Postgres.resume

    def interrupted(driver, db, binding, tag, intent):
        intent("pg.allow_writer")
        if after_login:
            with driver.connect(db, tag) as conn:
                conn.execute(sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(db.writer_role)))
            intent("pgbouncer.resume")
        raise RuntimeError("test interrupted resume")

    monkeypatch.setattr(Postgres, "resume", interrupted)
    flow.action("RESUME", location="TARGET", expected="UNKNOWN")
    flow.action("RECONCILE")
    assert flow.state()["allowedActions"] == ["RESUME", "RECONCILE"]
    monkeypatch.setattr(Postgres, "resume", original)
    flow.action("RESUME", location="TARGET")
    assert flow.state()["lifecycle"] == "ACTIVE"
    assert "ROLLBACK" not in flow.state()["allowedActions"]
    flow.cloud.sdk.promote_migration_job.assert_called_once()


def test_unreachable_unused_target_does_not_block_source_recovery(flow, monkeypatch):
    flow.ready()
    flow.action("QUIESCE")
    original = Postgres.inspect

    def unreachable(driver, db):
        if db.provider == "GCP_CLOUDSQL":
            raise psycopg.OperationalError("test target connection failed")
        return original(driver, db)

    monkeypatch.setattr(Postgres, "inspect", unreachable)
    flow.action("ROLLBACK")
    flow.action("RESUME", location="SOURCE")
    assert flow.state()["lifecycle"] == "RESTORED"
    with flow.apps.connect(flow.bid) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 10


def test_other_binding_on_same_cloud_instance_blocks_whole_job(flow):
    catalog = ResourceCatalog(flow.service.store)
    other = flow.service.store.transact(lambda tx: flow.service.bindings.resource(tx, "db:b")[0])
    other = other.model_copy(
        update={
            "provider": "AWS_RDS",
            "provider_instance": flow.cloud.source.provider_instance,
            "identity": f"AWS_RDS:{flow.cloud.source.provider_instance}/{other.database}",
        }
    )
    catalog.database("db:dms-other", other, tables=["public.messages"])
    flow.apps.register("b", db="dms-other")
    response = flow.create()
    assert response.status_code == 409
    assert response.json()["code"] == "DMS_INSTANCE_SCOPE_UNSUPPORTED"
    flow.cloud.sdk.verify_migration_job.assert_not_called()


def test_final_confirmation_failure_keeps_claims_until_reconciled(flow, monkeypatch):
    assert flow.create().status_code == 201
    flow.action("CANCEL")
    original = dms.DmsExecutor.run

    def interrupted(executor):
        if str(executor.ctx.request.action) == "FINALIZE":
            executor.finalize()
            raise RuntimeError("test final confirmation lost")
        original(executor)

    monkeypatch.setattr(dms.DmsExecutor, "run", interrupted)
    flow.action("FINALIZE", expected="UNKNOWN")
    assert flow.service.store.transact(lambda tx: tx.list("MIG_CLAIM", flow.mid + "#"))
    monkeypatch.setattr(dms.DmsExecutor, "run", original)
    flow.action("RECONCILE")
    assert not flow.service.store.transact(lambda tx: tx.list("MIG_CLAIM", flow.mid + "#"))


def test_terminal_history_and_archived_reconcile_do_not_require_gcp_credentials(flow, monkeypatch):
    assert flow.create().status_code == 201
    flow.action("CANCEL")
    flow.action("FINALIZE")
    flow.apps.action(flow.bid, "UNREGISTER", app="a")

    def no_credentials(*args, **kwargs):
        pytest.fail("terminal history must not initialize a cloud client")

    monkeypatch.setattr(dms, "DmsClient", no_credentials)
    assert flow.state()["lifecycle"] == "COMPLETED"
    flow.action("RECONCILE")


def test_same_action_response_is_replayed_without_cloud_credentials(flow, monkeypatch):
    assert flow.create().status_code == 201
    body = {"action": "PREPARE", "expectedRevision": flow.state()["revision"]}
    url = "/v1/migrations/" + flow.mid + "/actions"
    headers = {"Idempotency-Key": "replay-without-gcp"}
    accepted = flow.client.post(url, json=body, headers=headers)
    assert accepted.status_code == 202
    with flow.worker.exclusive():
        assert flow.worker.run_once()
    assert flow.client.get(accepted.json()["statusUrl"]).json()["status"] == "SUCCEEDED"

    def no_credentials(*args, **kwargs):
        pytest.fail("idempotent replay must not initialize a cloud client")

    monkeypatch.setattr(dms, "DmsClient", no_credentials)
    replay = flow.client.post(url, json=body, headers=headers)
    assert replay.status_code == 202
    assert replay.json() == accepted.json()
    flow.cloud.sdk.verify_migration_job.assert_called_once()
