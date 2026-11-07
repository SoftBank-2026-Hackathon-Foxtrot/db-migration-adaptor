"""관리형 프로필의 차단 조건과 실제 PostgreSQL 17 복제·전환을 검사합니다."""

import json
import os
import socket
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest
from psycopg.conninfo import make_conninfo
from test_operator_integration import Harness, write

from db_operator.logical import ManagedLogicalPostgres
from db_operator.models import MANAGED_LOGICAL_PROFILE, CreateMigration, OperatorError


class ManagedHarness(Harness):
    profile = MANAGED_LOGICAL_PROFILE

    def create(self, mode="BEFORE_TARGET_WRITE", **refs):
        return super().create(mode, **refs)


@pytest.fixture
def managed_case(lab_case, monkeypatch):
    settings, _, probe = lab_case
    tls = Path(os.environ["DB_OPERATOR_TEST_ENV"]).parent / "tls/server.crt"
    cfg = json.loads(settings.registry.read_text())
    for node, provider, instance in [
        ("source", "AWS_RDS", "arn:aws:rds:ap-northeast-2:000000000000:db:native-test"),
        ("target", "GCP_CLOUDSQL", "projects/native-test/instances/target"),
    ]:
        database = cfg["databases"]["resource:" + node]
        database.update(
            provider=provider,
            provider_instance=instance,
            identity=provider + ":" + instance + "/" + database["database"],
        )
        key = "LAB_" + node.upper() + "_ADMIN"
        monkeypatch.setenv(
            key, make_conninfo(os.environ[key], sslmode="verify-full", sslrootcert=str(tls))
        )
        key = "LAB_" + node.upper() + "_REPL"
        monkeypatch.setenv(key, make_conninfo(os.environ[key], sslmode="require"))
    settings.registry.write_text(json.dumps(cfg))
    stub_instance_metadata(monkeypatch, settings)
    return settings, probe


def stub_instance_metadata(monkeypatch, settings):
    """Answer the provider metadata checks for whichever lab database carries each instance."""

    def database_for(instance):
        cfg = json.loads(settings.registry.read_text())
        for database in cfg["databases"].values():
            if database.get("provider_instance") == instance:
                return database
        raise AssertionError("unknown instance " + instance)

    monkeypatch.setattr(
        "db_operator.logical.read_cloudsql_instance",
        lambda project, instance: {
            "project": project,
            "name": instance,
            "state": "RUNNABLE",
            "instanceType": "CLOUD_SQL_INSTANCE",
            "ipAddresses": [
                {
                    "type": "PRIMARY",
                    "ipAddress": database_for(f"projects/{project}/instances/{instance}")[
                        "route_host"
                    ],
                }
            ],
        },
    )

    def rds(region, instance):
        database = database_for(f"arn:aws:rds:{region}:000000000000:db:{instance}")
        return {
            "DBInstanceIdentifier": instance,
            "DBInstanceStatus": "available",
            "Engine": "postgres",
            "Endpoint": {
                "Address": database["route_host"],
                "Port": database.get("route_port", 5432),
            },
        }

    monkeypatch.setattr("db_operator.logical.read_rds_instance", rds)


def swap_direction(settings):
    """Register the lab source as Cloud SQL and the lab target as RDS: the return direction."""
    cfg = json.loads(settings.registry.read_text())
    for node, provider, instance in [
        ("source", "GCP_CLOUDSQL", "projects/native-test/instances/source"),
        ("target", "AWS_RDS", "arn:aws:rds:ap-northeast-2:000000000000:db:native-test-target"),
    ]:
        database = cfg["databases"]["resource:" + node]
        database.update(
            provider=provider,
            provider_instance=instance,
            identity=provider + ":" + instance + "/" + database["database"],
        )
    settings.registry.write_text(json.dumps(cfg))


def test_dms_replica_is_not_accepted_as_native_destination(monkeypatch):
    monkeypatch.setattr(
        "db_operator.logical.read_cloudsql_instance",
        lambda *_: {
            "project": "test",
            "name": "replica",
            "state": "RUNNABLE",
            "instanceType": "READ_REPLICA_INSTANCE",
            "masterInstanceName": "source",
            "ipAddresses": [{"type": "PRIMARY", "ipAddress": "target"}],
        },
    )
    with pytest.raises(OperatorError, match="독립 Cloud SQL"):
        ManagedLogicalPostgres.check_target_metadata(
            SimpleNamespace(
                provider_instance="projects/test/instances/replica", route_host="target"
            )
        )


@pytest.mark.parametrize(
    "resolved", [["10.51.0.3"], ["10.51.0.4"], ["10.51.0.3", "10.51.0.4"], [], None]
)
def test_private_certificate_dns_must_resolve_only_to_the_selected_instance(monkeypatch, resolved):
    monkeypatch.setattr(
        "db_operator.logical.read_cloudsql_instance",
        lambda *_: {
            "project": "test",
            "name": "target",
            "state": "RUNNABLE",
            "instanceType": "CLOUD_SQL_INSTANCE",
            "dnsName": None,
            "dnsNames": None,
            "ipAddresses": [{"type": "PRIVATE", "ipAddress": "10.51.0.3"}],
        },
    )

    def resolve(*_args, **_kwargs):
        if resolved is None:
            raise socket.gaierror("DNS resolution failed")
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", (ip, 5432)) for ip in resolved]

    monkeypatch.setattr("db_operator.logical.socket.getaddrinfo", resolve)
    target = SimpleNamespace(
        provider_instance="projects/test/instances/target",
        route_host="certificate.asia-northeast3.sql.goog",
        route_port=5432,
    )
    if resolved == ["10.51.0.3"]:
        ManagedLogicalPostgres.check_target_metadata(target)
    else:
        with pytest.raises(OperatorError, match="독립 Cloud SQL"):
            ManagedLogicalPostgres.check_target_metadata(target)


def test_stale_transition_proof_is_rejected_before_connecting():
    driver = object.__new__(ManagedLogicalPostgres)
    driver.resources = Mock(return_value=(Mock(), Mock(), Mock()))
    driver.connect = Mock()
    row = {
        "id": "mig-test",
        "plan": {},
        "state": {"transitionId": "new"},
        "internal": {
            "prepared": True,
            "schemas": {"checked": True},
            "forwardBarrier": {"migrationId": "mig-test", "transitionId": "old"},
        },
    }
    with pytest.raises(OperatorError, match="최종 복제 근거"):
        driver.validate_record(row)
    driver.connect.assert_not_called()


def test_managed_reverse_replication_is_rejected():
    with pytest.raises(ValueError, match="reverse replication"):
        CreateMigration.model_validate(
            {
                "workflowId": "w",
                "resourceId": "r",
                "kind": "DATABASE",
                "profile": MANAGED_LOGICAL_PROFILE,
                "sourceRef": "source",
                "targetRef": "target",
                "bindingRef": "binding",
                "workloadScopeRef": "scope",
                "policy": {"rollbackMode": "REVERSE_REQUIRED", "maxQuiesceSeconds": 60},
            }
        )


@pytest.mark.integration
def test_real_native_copy_mixed_transactions_and_cutover(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ManagedHarness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    committed = [write(probe) for _ in range(8)]
    with psycopg.connect(probe.get_secret_value()) as conn:
        conn.execute("DELETE FROM orders WHERE request_id=%s", (committed[-1][0],))
        conn.execute("UPDATE inventory SET stock=stock+1 WHERE sku='chiikawa'")
    with psycopg.connect(probe.get_secret_value()) as conn:
        conn.execute("UPDATE inventory SET stock=0 WHERE sku='chiikawa'")
        conn.rollback()
    # 전수 데이터 비교를 호출하면 이 검사는 실패합니다.
    monkeypatch.setattr(ManagedLogicalPostgres, "snapshot", lambda *_: pytest.fail("full scan"))
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")
    assert h.state()["lifecycle"] == "ACTIVE"
    _, new_id = write(probe)
    assert new_id > max(item[1] for item in committed)
    driver = h.service.scoped(h.service.store.get(h.mid)).driver
    source, target, _ = driver.resources(h.service.store.get(h.mid)["plan"])
    with driver.connect(target) as conn:
        assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 18
        assert (
            conn.execute("SELECT stock FROM inventory WHERE sku='chiikawa'").fetchone()["stock"]
            == 982
        )
    with driver.connect(source) as conn:
        assert (
            conn.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (source.writer_role,)
            ).fetchone()["rolcanlogin"]
            is False
        )
        assert not conn.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s",
            (driver.names(h.mid)["forward"],),
        ).fetchone()
    assert "ROLLBACK" not in h.state()["allowedActions"]


@pytest.mark.integration
def test_live_subscription_blocks_cutover_until_reconciled(managed_case, monkeypatch):
    settings, _ = managed_case
    h = ManagedHarness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    actual = ManagedLogicalPostgres.disable
    monkeypatch.setattr(ManagedLogicalPostgres, "disable", lambda *args: None)
    _, _, operation = h.action("QUIESCE", expected="UNKNOWN")
    result = h.service.store.operation(operation["operationId"], "orchestrator")
    assert result["error"]["code"] == "REPLICATION_NOT_STOPPED"
    assert h.state()["observed"]["route"] == "SOURCE"
    monkeypatch.setattr(ManagedLogicalPostgres, "disable", actual)
    h.action("RECONCILE")
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")
    assert h.state()["lifecycle"] == "ACTIVE"


@pytest.mark.integration
def test_source_recovery_does_not_compare_unused_target(managed_case):
    settings, probe = managed_case
    h = ManagedHarness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("QUIESCE")
    row = h.service.store.get(h.mid)
    driver = h.service.scoped(row).driver
    with driver.connect(driver.resources(row["plan"])[1]) as conn:
        conn.execute("DELETE FROM orders")
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    assert h.state()["lifecycle"] == "RESTORED"
    write(probe)


@pytest.mark.integration
def test_future_lsn_does_not_allow_cutover(managed_case):
    settings, _ = managed_case
    h = ManagedHarness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("QUIESCE")
    h.service.store.update(
        h.mid,
        lambda row: row["internal"]["forwardBarrier"].update(
            committedThroughLsn="FFFFFFFF/FFFFFFFF"
        ),
    )
    _, _, operation = h.action("CUTOVER", expected="FAILED")
    result = h.service.store.operation(operation["operationId"], "orchestrator")
    assert result["error"]["code"] == "REPLICATION_NOT_READY"
    assert h.state()["observed"]["route"] == "SOURCE"


@pytest.mark.integration
def test_lost_resume_reply_reconciles_without_repeating_writes(managed_case, monkeypatch):
    settings, probe = managed_case
    h = ManagedHarness(settings)
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("QUIESCE")
    h.action("CUTOVER")
    actual = ManagedLogicalPostgres.resume

    def lost_reply(driver, *args, **kwargs):
        actual(driver, *args, **kwargs)
        raise TimeoutError("reply lost after actual target resume")

    monkeypatch.setattr(ManagedLogicalPostgres, "resume", lost_reply)
    h.action("RESUME", location="TARGET", expected="UNKNOWN")
    write(probe)
    monkeypatch.setattr(ManagedLogicalPostgres, "resume", actual)
    h.action("RECONCILE")
    assert h.state()["lifecycle"] == "ACTIVE"
    assert "ROLLBACK" not in h.state()["allowedActions"]


def test_return_trip_is_only_the_exact_reverse_of_the_latest_completed_plan():
    target = SimpleNamespace(identity="AWS_RDS:arn:aws:rds:ap-northeast-2:0:db:a/app")
    plan = {"sourceRef": "db:b", "targetRef": "db:a"}
    latest = {"plan": {"sourceRef": "db:a", "targetRef": "db:b"}}
    assert ManagedLogicalPostgres._returns_to(latest, plan, target)
    assert not ManagedLogicalPostgres._returns_to(
        {"plan": {"sourceRef": "db:b", "targetRef": "db:a"}}, plan, target
    )
    catalogued = latest | {"catalog": {"databases": {"db:a": {"identity": "other"}}}}
    assert not ManagedLogicalPostgres._returns_to(catalogued, plan, target)
    catalogued["catalog"]["databases"]["db:a"]["identity"] = target.identity
    assert ManagedLogicalPostgres._returns_to(catalogued, plan, target)


def test_rds_metadata_requires_a_running_standalone_instance_at_the_registered_address(
    monkeypatch,
):
    target = SimpleNamespace(
        provider="AWS_RDS",
        provider_instance="arn:aws:rds:ap-northeast-2:000000000000:db:native",
        route_host="native.abc.ap-northeast-2.rds.amazonaws.com",
        route_port=5432,
    )
    info = {
        "DBInstanceIdentifier": "native",
        "DBInstanceStatus": "available",
        "Engine": "postgres",
        "Endpoint": {"Address": "NATIVE.abc.ap-northeast-2.rds.amazonaws.com", "Port": 5432},
    }
    monkeypatch.setattr("db_operator.logical.read_rds_instance", lambda region, name: info)
    ManagedLogicalPostgres.check_instance_metadata(target)
    for broken in [
        {"DBInstanceStatus": "modifying"},
        {"ReadReplicaSourceDBInstanceIdentifier": "primary"},
        {"Endpoint": {"Address": "other.rds.amazonaws.com", "Port": 5432}},
        {"Endpoint": {"Address": info["Endpoint"]["Address"], "Port": 5433}},
    ]:
        monkeypatch.setattr(
            "db_operator.logical.read_rds_instance", lambda region, name, b=broken: info | b
        )
        with pytest.raises(OperatorError) as error:
            ManagedLogicalPostgres.check_rds_metadata(target)
        assert error.value.code == "TARGET_NOT_STANDALONE"
    with pytest.raises(OperatorError) as error:
        ManagedLogicalPostgres.check_rds_metadata(
            SimpleNamespace(
                provider="AWS_RDS", provider_instance="db:native", route_host="x", route_port=1
            )
        )
    assert error.value.code == "UNSUPPORTED_PROFILE"
