from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from google.cloud import clouddms_v1 as dms
from google.longrunning.operations_pb2 import Operation
from google.protobuf.any_pb2 import Any

from db_operator.dms import DmsClient, JobBinding
from db_operator.models import OperatorError

PARENT = "projects/foxtrot-test/locations/asia-northeast3"


def promotion_wait(monkeypatch, *, held_ago=0):
    from db_operator import dms as module

    elapsed = [0.0]
    origin = datetime(2026, 11, 5, tzinfo=UTC)
    evidence = {"held_since": origin - timedelta(seconds=held_ago)}

    def sleep(seconds):
        elapsed[0] += seconds

    def gate():
        if held_ago + elapsed[0] >= 300:
            raise OperatorError("QUIESCE_EXPIRED", "업무 정지 기한을 넘었습니다.")
        return evidence

    monkeypatch.setattr(module.time, "monotonic", lambda: elapsed[0])
    monkeypatch.setattr(module.time, "sleep", sleep)
    monkeypatch.setattr(module, "now", lambda: origin + timedelta(seconds=elapsed[0]))
    client = Mock()
    client.submit.return_value = {"id": "dms-test", "status": "PENDING"}
    client.poll.side_effect = lambda oid: {
        "id": oid,
        "status": "SUCCEEDED" if elapsed[0] >= 46 else "PENDING",
    }
    context = Mock()
    context.assert_lease = Mock()
    context.migration_id, context.operation_id = "migration-test", "operation-test"
    context.plan = {"policy": {"maxQuiesceSeconds": 300}}
    context.verify_gate.side_effect = gate
    context.store.get.return_value = {"internal": {"dmsFingerprint": "original"}}
    context.driver.dms = client
    return module.DmsExecutor(context), context, elapsed


def test_46_second_promotion_finishes_without_recovery_or_resubmission(monkeypatch):
    executor, context, elapsed = promotion_wait(monkeypatch)
    result = executor.cloud("PROMOTE")
    assert result["status"] == "SUCCEEDED" and elapsed[0] == 46
    context.driver.dms.submit.assert_called_once_with("PROMOTE", "operation-test")
    assert context.verify_gate.call_count > 46


def test_promotion_wait_does_not_extend_existing_write_pause_deadline(monkeypatch):
    executor, context, elapsed = promotion_wait(monkeypatch, held_ago=295)
    with pytest.raises(OperatorError, match="기한"):
        executor.cloud("PROMOTE")
    assert elapsed[0] == 5
    context.driver.dms.submit.assert_called_once_with("PROMOTE", "operation-test")


def test_promotion_wait_keeps_a_bounded_deadline(monkeypatch):
    executor, context, elapsed = promotion_wait(monkeypatch)
    context.driver.dms.poll.return_value = {"id": "dms-test", "status": "PENDING"}
    context.driver.dms.poll.side_effect = None
    with pytest.raises(OperatorError, match="진행 중"):
        executor.cloud("PROMOTE")
    assert elapsed[0] == 180
    context.driver.dms.submit.assert_called_once_with("PROMOTE", "operation-test")


def test_pending_poll_checks_saved_operation_then_rechecks_configuration_on_completion():
    from db_operator.dms import DmsService

    client, journal = Mock(), Mock()
    client.binding.job = PARENT + "/migrationJobs/test"
    journal.lock.return_value.__enter__ = Mock(return_value=Mock())
    journal.lock.return_value.__exit__ = Mock(return_value=False)
    row = {
        "id": "dms-test",
        "status": "PENDING",
        "action": "PROMOTE",
        "cloud_operation": PARENT + "/operations/test",
        "fingerprint": "original",
    }
    journal.row.return_value = row
    client.operation.return_value = {"status": "PENDING", "errorCode": None}
    service = DmsService(client, journal)
    assert service.poll("dms-test") == row
    client.observe.assert_not_called()
    journal.update.assert_not_called()
    client.operation.return_value = {"status": "SUCCEEDED", "errorCode": None}
    client.observe.return_value = {"fingerprint": "changed"}
    with pytest.raises(OperatorError, match="설정이 바뀌었습니다"):
        service.poll("dms-test")
    journal.update.assert_not_called()


def test_successful_business_database_connection_does_not_fallback_after_connection_failure(
    monkeypatch,
):
    from contextlib import contextmanager
    from contextvars import ContextVar

    import psycopg

    from db_operator.config import Database
    from db_operator.dms import GcpDmsPostgres
    from db_operator.postgres import Postgres

    database = Database(
        identity="GCP_CLOUDSQL:projects/foxtrot-test/instances/test/app",
        provider="GCP_CLOUDSQL",
        provider_instance="projects/foxtrot-test/instances/test",
        writer_role="writer",
        route_host="cloudsql.test",
        database="app",
    )
    driver = object.__new__(GcpDmsPostgres)
    driver._business_databases = set()
    driver._observation_job = ContextVar("test_observation_job", default=None)
    driver.job = Mock(return_value={"state": "RUNNING", "phase": "CDC", "errorCode": 0})
    calls = []

    @contextmanager
    def connect(backend, db, *args, **kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            raise psycopg.OperationalError("connection unavailable")
        yield Mock()

    monkeypatch.setattr(Postgres, "connect", connect)
    with driver.connect(database):
        pass
    with pytest.raises(psycopg.OperationalError), driver.connect(database):
        pass
    driver.job.assert_called_once()
    assert calls == [{}, {}]


def sdk_fixture(suffix="test"):
    binding = JobBinding(
        job=PARENT + "/migrationJobs/" + suffix,
        source=PARENT + "/connectionProfiles/source-" + suffix,
        destination=PARENT + "/connectionProfiles/target-" + suffix,
        owner="foxtrot",
    )
    job = dms.MigrationJob(
        name=binding.job,
        source=binding.source,
        destination=binding.destination,
        labels={"foxtrot-owner": "foxtrot"},
        type_=dms.MigrationJob.Type.CONTINUOUS,
        state=dms.MigrationJob.State.NOT_STARTED,
        create_time="2026-11-04T01:00:00Z",
        source_database={"provider": "RDS", "engine": "POSTGRESQL"},
        destination_database={"provider": "CLOUDSQL", "engine": "POSTGRESQL"},
    )
    profiles = {
        binding.source: dms.ConnectionProfile(name=binding.source, postgresql={"host": "rds.test"}),
        binding.destination: dms.ConnectionProfile(
            name=binding.destination, cloudsql={"cloud_sql_id": "test"}
        ),
    }
    sdk = Mock(spec=dms.DataMigrationServiceClient)
    sdk.get_migration_job.side_effect = lambda **kwargs: deepcopy(job)
    sdk.get_connection_profile.side_effect = lambda **kwargs: deepcopy(
        profiles[kwargs["request"]["name"]]
    )

    def instance_reader(project, instance):
        return {
            "name": instance,
            "project": project,
            "connectionName": f"{project}:asia-northeast3:{instance}",
            "ipAddresses": [{"type": "PRIMARY", "ipAddress": "203.0.113.10"}],
        }

    return binding, job, profiles, sdk, DmsClient(binding, sdk, instance_reader)


def cloud_operation(binding, action="VERIFY", *, done=True, code=None, target=None):
    metadata, response = Any(), Any()
    metadata.Pack(
        dms.OperationMetadata.pb(
            dms.OperationMetadata(
                target=target or binding.job,
                create_time=datetime.now(UTC),
                verb={"VERIFY": "verify", "START_SYNC": "start"}[action],
            )
        )
    )
    response.Pack(dms.MigrationJob.pb(dms.MigrationJob(name=binding.job)))
    op = Operation(name=PARENT + "/operations/op-test", metadata=metadata, done=done)
    if code is not None:
        op.error.code = code
        op.error.message = "sensitive remote details"
    elif done:
        op.response.CopyFrom(response)
    return op


def test_running_cdc_is_not_cutover_or_rollback_proof():
    _, job, _, _, client = sdk_fixture()
    job.state, job.phase = "RUNNING", "CDC"
    status = client.observe()
    assert status["state"] == "RUNNING" and status["phase"] == "CDC"
    assert status["cutoverReady"] == "UNKNOWN"
    assert status["reverseReplication"] == "NOT_CONFIGURED"


def test_fingerprint_ignores_job_progress_but_tracks_endpoint_changes():
    binding, job, profiles, _, client = sdk_fixture()
    before = client.observe()["fingerprint"]
    job.state, job.phase, job.update_time = "RUNNING", "CDC", "2026-11-04T02:00:00Z"
    assert client.observe()["fingerprint"] == before
    profiles[binding.source].postgresql.host = "another-rds.test"
    assert client.observe()["fingerprint"] != before


@pytest.mark.parametrize("change", ["owner", "source", "engine", "provider", "type"])
def test_binding_rejects_wrong_owner_route_or_database(change):
    _, job, _, _, client = sdk_fixture()
    if change == "owner":
        job.labels["foxtrot-owner"] = "another-team"
    elif change == "source":
        job.source += "-other"
    elif change == "engine":
        job.source_database.engine = "MYSQL"
    elif change == "provider":
        job.destination_database.provider = "RDS"
    else:
        job.type_ = "ONE_TIME"
    with pytest.raises(OperatorError):
        client.observe()


def test_binding_rejects_cross_project_profile():
    binding, *_ = sdk_fixture()
    with pytest.raises(OperatorError):
        JobBinding(
            **{
                **binding.model_dump(),
                "source": "projects/other/locations/asia-northeast3/connectionProfiles/a",
            }
        )


@pytest.mark.parametrize(
    "action,method", [("VERIFY", "verify_migration_job"), ("START_SYNC", "start_migration_job")]
)
def test_mutation_uses_official_sdk_without_automatic_retry(action, method):
    binding, _, _, sdk, client = sdk_fixture()
    getattr(sdk, method).return_value = SimpleNamespace(operation=cloud_operation(binding, action))
    assert client.submit(action).endswith("/operations/op-test")
    getattr(sdk, method).assert_called_once_with(
        request={"name": binding.job}, retry=None, timeout=30
    )


@pytest.mark.parametrize(
    "done,code,status", [(False, None, "PENDING"), (True, None, "SUCCEEDED"), (True, 7, "FAILED")]
)
def test_official_operation_response_is_checked(done, code, status):
    binding, _, _, sdk, client = sdk_fixture()
    op = cloud_operation(binding, done=done, code=code)
    sdk.get_operation.return_value = op
    result = client.operation(op.name, "VERIFY")
    assert result == {"status": status, "errorCode": code}
    assert "sensitive" not in str(result)


def test_operation_for_another_job_is_not_adopted():
    binding, _, _, sdk, client = sdk_fixture()
    op = cloud_operation(binding, target=binding.job + "-other")
    sdk.get_operation.return_value = op
    with pytest.raises(OperatorError, match="대상 또는 명령"):
        client.operation(op.name, "VERIFY")


def test_remote_credentials_are_not_exposed_in_status():
    binding, _, profiles, _, client = sdk_fixture()
    profiles[binding.source].postgresql.password = "not-for-status-output"
    assert "not-for-status-output" not in str(client.observe())


@pytest.mark.parametrize(
    "action,method",
    [
        ("RESUME_SYNC", "resume_migration_job"),
        ("PROMOTE", "promote_migration_job"),
        ("STOP", "stop_migration_job"),
        ("DELETE", "delete_migration_job"),
    ],
)
def test_common_api_mutations_use_sdk_without_retry(action, method):
    binding, _, _, sdk, client = sdk_fixture()
    getattr(sdk, method).return_value = SimpleNamespace(
        operation=SimpleNamespace(name=PARENT + "/operations/confirmed")
    )
    assert client.submit(action) == PARENT + "/operations/confirmed"
    getattr(sdk, method).assert_called_once_with(
        request={"name": binding.job}, retry=None, timeout=30
    )


def test_managed_sql_rejects_plaintext_before_opening_connection(monkeypatch):
    import psycopg

    from db_operator.config import Database, Registry
    from db_operator.postgres import Postgres

    database = Database(
        identity="AWS_RDS:instance/app",
        provider="AWS_RDS",
        provider_instance="instance",
        database="app",
        writer_role="writer",
        route_host="rds.test",
        admin_env="TLS_TEST_DSN",
    )
    monkeypatch.setenv("TLS_TEST_DSN", "host=rds.test dbname=app user=admin sslmode=disable")
    connect = Mock()
    monkeypatch.setattr(psycopg, "connect", connect)
    driver = Postgres(Registry(databases={"source": database}, bindings={}, workloads={}))
    with pytest.raises(OperatorError, match="verify-full"), driver.connect(database):
        pass
    connect.assert_not_called()


@pytest.mark.parametrize("change", ["source", "destination", "address", "role", "state"])
def test_dms_job_must_match_registered_database_and_replication_role(change, monkeypatch):
    from db_operator.config import Database
    from db_operator.dms import GcpDmsPostgres

    monkeypatch.setenv(
        "REPL_TEST_DSN", "host=rds.test dbname=app user=replicator sslmode=verify-full"
    )
    source = Database(
        identity="AWS_RDS:rds/app",
        provider="AWS_RDS",
        provider_instance="rds",
        database="app",
        writer_role="writer",
        route_host="rds.test",
        replication_env="REPL_TEST_DSN",
    )
    target = Database(
        identity="GCP_CLOUDSQL:projects/foxtrot-test/instances/target/app",
        provider="GCP_CLOUDSQL",
        provider_instance="projects/foxtrot-test/instances/target",
        database="app",
        writer_role="writer",
        route_host="203.0.113.10",
    )
    driver = object.__new__(GcpDmsPostgres)
    driver.client = SimpleNamespace(binding=SimpleNamespace(project="foxtrot-test"))
    driver.resources = lambda plan: (source, target, None)
    job = {
        "sourceEndpoint": {"host": "rds.test", "port": 5432},
        "destinationInstance": "target",
        "destinationAddresses": ["203.0.113.10"],
        "sourceReplicationRole": "replicator",
        "state": "NOT_STARTED",
        "errorCode": 0,
    }
    driver.check_job_binding({}, job)
    if change == "source":
        job["sourceEndpoint"]["host"] = "different-rds.test"
    if change == "destination":
        job["destinationInstance"] = "other-instance"
    if change == "address":
        job["destinationAddresses"] = ["203.0.113.20"]
    if change == "role":
        job["sourceReplicationRole"] = "unrelated-role"
    if change == "state":
        job["state"] = "COMPLETED"
    with pytest.raises(OperatorError, match="DMS 접속"):
        driver.check_job_binding({}, job)


@pytest.mark.parametrize("mismatch", ["host", "port", "dbname"])
def test_admin_secret_cannot_point_to_a_different_database(mismatch, monkeypatch):
    from psycopg.conninfo import make_conninfo

    from db_operator.config import Database
    from db_operator.dms import GcpDmsPostgres

    db = Database(
        identity="AWS_RDS:rds/app",
        provider="AWS_RDS",
        provider_instance="rds",
        database="app",
        writer_role="writer",
        route_host="rds.test",
        admin_env="ENDPOINT_TEST_DSN",
    )
    driver = object.__new__(GcpDmsPostgres)
    fields = {
        "host": "rds.test",
        "port": 5432,
        "dbname": "app",
        "user": "admin",
        "sslmode": "verify-full",
    }
    monkeypatch.setenv("ENDPOINT_TEST_DSN", make_conninfo(**fields))
    driver.check_admin_endpoint(db)
    fields[mismatch] = 5433 if mismatch == "port" else "other"
    monkeypatch.setenv("ENDPOINT_TEST_DSN", make_conninfo(**fields))
    with pytest.raises(OperatorError, match="관리 Secret"):
        driver.check_admin_endpoint(db)


def test_unregistered_database_blocks_whole_instance_promotion():
    from db_operator.dms import GcpDmsPostgres

    driver = object.__new__(GcpDmsPostgres)
    conn = Mock()
    conn.execute.return_value.fetchall.return_value = [{"datname": "app"}, {"datname": "postgres"}]
    driver.check_instance_databases(conn, SimpleNamespace(database="app"))
    conn.execute.return_value.fetchall.return_value.append({"datname": "unregistered_app"})
    with pytest.raises(OperatorError, match="전용 원본"):
        driver.check_instance_databases(conn, SimpleNamespace(database="app"))


def test_existing_cloudsql_postgresql_profile_resolves_instance_metadata():
    binding, _, profiles, _, client = sdk_fixture()
    profiles[binding.destination] = dms.ConnectionProfile(
        name=binding.destination, provider="CLOUDSQL", postgresql={"cloud_sql_id": "test"}
    )
    reader = Mock(
        return_value={
            "name": "test",
            "project": "foxtrot-test",
            "connectionName": "foxtrot-test:asia-northeast3:test",
            "ipAddresses": [
                {"type": "PRIMARY", "ipAddress": "203.0.113.10"},
                {"type": "OUTGOING", "ipAddress": "203.0.113.20"},
            ],
        }
    )
    client.instance_reader = reader
    observed = client.observe()
    assert observed["destinationInstance"] == "test"
    assert observed["destinationAddresses"] == ["203.0.113.10"]
    reader.assert_called_once_with("foxtrot-test", "test")


def test_cloudsql_address_change_invalidates_dms_configuration_fingerprint():
    _, _, _, _, client = sdk_fixture()
    first = client.observe()["fingerprint"]
    client.instance_reader = lambda project, instance: {
        "name": instance,
        "project": project,
        "connectionName": f"{project}:asia-northeast3:{instance}",
        "ipAddresses": [{"type": "PRIMARY", "ipAddress": "203.0.113.99"}],
    }
    assert client.observe()["fingerprint"] != first


def test_cloudsql_metadata_for_another_instance_is_rejected():
    _, _, _, _, client = sdk_fixture()
    client.instance_reader = lambda *args: {
        "name": "unrelated",
        "project": "foxtrot-test",
        "connectionName": "foxtrot-test:asia-northeast3:unrelated",
    }
    with pytest.raises(OperatorError, match="Cloud SQL 조회 결과"):
        client.observe()


def test_cloudsql_public_dns_is_bound_to_instance_and_configuration():
    _, _, _, _, client = sdk_fixture()
    info = {
        "name": "test",
        "project": "foxtrot-test",
        "connectionName": "foxtrot-test:asia-northeast3:test",
        "ipAddresses": [{"type": "PRIMARY", "ipAddress": "203.0.113.10"}],
        "dnsNames": [
            {"name": "instance.project.asia-northeast3.sql.goog.", "connectionType": "PUBLIC"},
            {"name": "unapproved.example", "connectionType": "PUBLIC"},
            {"name": "private.project.asia-northeast3.sql.goog.", "connectionType": "PRIVATE"},
        ],
    }
    client.instance_reader = lambda *_: info
    first = client.observe()
    assert first["destinationHostnames"] == ["instance.project.asia-northeast3.sql.goog."]
    info["dnsNames"][0]["name"] = "replacement.project.asia-northeast3.sql.goog."
    assert client.observe()["fingerprint"] != first["fingerprint"]
