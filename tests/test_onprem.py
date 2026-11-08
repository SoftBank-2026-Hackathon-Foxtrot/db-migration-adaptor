"""The dedicated PG17 profile must not broaden either managed-cloud profile."""

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import psycopg
import pytest
from jsonschema import ValidationError as ContractError
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from test_bindings import apps as apps
from test_control_safety import finish_active
from test_direct_inputs import create, reject, resources
from test_managed_logical import ManagedHarness
from test_managed_logical import managed_case as managed_case
from test_operator_integration import validate, write

from db_operator.config import database_endpoint
from db_operator.logical import ManagedLogicalPostgres
from db_operator.models import ONPREM_LOGICAL_PROFILE, CreateMigration, OperatorError
from db_operator.onprem import OnpremLogicalPostgres


@pytest.mark.parametrize("source", ["AWS_RDS", "GCP_CLOUDSQL", "SELF_MANAGED"])
@pytest.mark.parametrize("target", ["AWS_RDS", "GCP_CLOUDSQL", "SELF_MANAGED"])
def test_profile_boundaries(source, target):
    for driver, allowed in [
        (OnpremLogicalPostgres, (source, target) == ("AWS_RDS", "SELF_MANAGED")),
        (ManagedLogicalPostgres, "SELF_MANAGED" not in (source, target)),
    ]:
        args = (SimpleNamespace(provider=source), SimpleNamespace(provider=target))
        if allowed:
            driver.check_pair(*args)
        else:
            with pytest.raises(OperatorError) as error:
                driver.check_pair(*args)
            assert error.value.code == "UNSUPPORTED_PROFILE"


def test_contract_keeps_onprem_one_way_and_before_target_writes_only():
    arn = "arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:test"
    target = dict(
        host="vm", database="app", secretRef=arn, writerRole="app", provider="SELF_MANAGED"
    )
    source = target | dict(
        host="rds",
        provider="AWS_RDS",
        providerInstance="arn:aws:rds:ap-northeast-2:123456789012:db:demo",
    )
    base = dict(
        workflowId="wf",
        resourceId="database",
        kind="DATABASE",
        profile=ONPREM_LOGICAL_PROFILE,
        policy=dict(maxQuiesceSeconds=60, rollbackMode="BEFORE_TARGET_WRITE"),
    )
    direct = base | dict(source=source, target=target)
    validate("CreateMigration", direct)
    for extra in (
        dict(source=target, target=source),
        dict(replicationRef="dms:job"),
        dict(policy=base["policy"] | {"rollbackMode": "REVERSE_REQUIRED"}),
    ):
        with pytest.raises(ContractError):
            validate("CreateMigration", direct | extra)
    refs = base | dict(sourceRef="s", targetRef="t", bindingRef="b", workloadScopeRef="w")
    with pytest.raises(ValueError, match="reverse replication"):
        CreateMigration.model_validate(
            refs | {"policy": {"maxQuiesceSeconds": 60, "rollbackMode": "REVERSE_REQUIRED"}}
        )


def test_target_does_not_trust_a_cloud_role_or_weak_tls():
    db = Mock(provider="SELF_MANAGED")
    db.secret.side_effect = ["sslmode=verify-full", "sslmode=require"]
    driver = object.__new__(OnpremLogicalPostgres)
    driver.connect = Mock()
    with pytest.raises(OperatorError) as error:
        driver.inspect(db)
    assert error.value.code == "TLS_REQUIRED"
    driver.connect.assert_not_called()


def test_cloud_reuse_never_reaches_across_an_onprem_move():
    driver = object.__new__(ManagedLogicalPostgres)

    def row(i, profile):
        return dict(
            id=str(i),
            plan=dict(bindingRef="b", profile=profile),
            state=dict(lifecycle="COMPLETED"),
            internal=dict(resumeAt=str(i)),
        )

    rows = [row(1, "cloud"), row(2, ONPREM_LOGICAL_PROFILE), row(3, "cloud")]
    tx = Mock()
    tx.list.return_value = rows
    driver.store = Mock()
    driver.store.transact.side_effect = lambda f: f(tx)
    assert driver.completed_migrations({"bindingRef": "b"}, "next") == rows[2:]


class OnpremHarness(ManagedHarness):
    profile = ONPREM_LOGICAL_PROFILE


@pytest.mark.integration
def test_onprem_cannot_take_an_active_cloud_migrations_source(managed_case):
    settings, _ = managed_case
    cloud = ManagedHarness(settings)
    cloud_body, _ = cloud.create()
    original = cloud.service.store.get(cloud.mid)
    cfg = json.loads(settings.registry.read_text())
    cfg["databases"]["resource:vm"] = cfg["databases"]["resource:target"] | {
        "provider": "SELF_MANAGED",
        "provider_instance": None,
        "identity": "onprem:vm",
    }
    settings.registry.write_text(json.dumps(cfg))
    new_client = OnpremHarness(settings).client
    response = new_client.post(
        "/v1/migrations",
        headers={"Idempotency-Key": "new-onprem-plan"},
        json=cloud_body
        | {
            "workflowId": "wf-onprem-competing",
            "profile": ONPREM_LOGICAL_PROFILE,
            "targetRef": "resource:vm",
        },
    )
    assert response.status_code == 422 and response.json()["code"] == "RESOURCE_BUSY"
    assert cloud.service.store.get(cloud.mid) == original
    assert [row["id"] for row in cloud.service.store.transact(lambda tx: tx.list("MIG"))] == [
        cloud.mid
    ]


@pytest.fixture
def onprem_case(managed_case, monkeypatch):
    settings, probe = managed_case
    cfg = json.loads(settings.registry.read_text())
    target = cfg["databases"]["resource:target"]
    target.update(
        provider="SELF_MANAGED", provider_instance=None, identity="onprem:" + target["database"]
    )
    settings.registry.write_text(json.dumps(cfg))
    ca = Path(os.environ["DB_OPERATOR_TEST_ENV"]).parent / "tls/server.crt"
    monkeypatch.setenv(
        "LAB_TARGET_REPL",
        make_conninfo(os.environ["LAB_TARGET_REPL"], sslmode="verify-full", sslrootcert=str(ca)),
    )
    # Translate Compose DNS only for the test runner outside Docker. The real subscription
    # connects from the PG17 target to the source over the unmodified replication DSN.
    mapping = {
        key: conninfo_to_dict(os.environ[f"LAB_{key.upper()}_ADMIN"])
        for key in ("source", "target")
    }
    original = psycopg.connect

    def connect(conninfo="", *args, **kwargs):
        if (host := conninfo_to_dict(conninfo).get("host")) in mapping:
            addr = mapping[host]
            conninfo = make_conninfo(conninfo, host=addr["host"], port=addr["port"])
        return original(conninfo, *args, **kwargs)

    monkeypatch.setattr(psycopg, "connect", connect)
    return OnpremHarness(settings), probe


@pytest.mark.integration
def test_copy_cdc_fence_and_first_target_write(onprem_case):
    h, probe = onprem_case
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    request_id, last_id = write(probe)
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")
    assert h.state()["lifecycle"] == "ACTIVE"
    assert write(probe)[1] > last_id
    with psycopg.connect(os.environ["LAB_TARGET_ADMIN"]) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone() == (12,)
        assert conn.execute(
            "SELECT id FROM orders WHERE request_id=%s", (request_id,)
        ).fetchone() == (last_id,)
    with psycopg.connect(os.environ["LAB_SOURCE_ADMIN"]) as conn:
        role = h.service.registry.databases["resource:source"].writer_role
        assert conn.execute(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (role,)
        ).fetchone() == (False,)
    assert "ROLLBACK" not in h.state()["allowedActions"]
    finish_active(h)


@pytest.mark.integration
def test_rollback_before_target_writes_restores_only_the_source(onprem_case):
    h, probe = onprem_case
    h.create()
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("ROLLBACK")
    h.action("RESUME", location="SOURCE")
    state = h.state()
    assert state["lifecycle"] == "RESTORED" and state["observed"]["route"] == "SOURCE"
    request_id, _ = write(probe)
    with psycopg.connect(os.environ["LAB_SOURCE_ADMIN"]) as source:
        assert source.execute(
            "SELECT 1 FROM orders WHERE request_id=%s", (request_id,)
        ).fetchone() == (1,)
    with psycopg.connect(os.environ["LAB_TARGET_ADMIN"]) as target:
        role = h.service.registry.databases["resource:target"].writer_role
        assert target.execute(
            "SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (role,)
        ).fetchone() == (False,)


@pytest.mark.integration
def test_nonempty_target_is_preserved(onprem_case):
    h, _ = onprem_case
    with psycopg.connect(os.environ["LAB_TARGET_ADMIN"]) as conn:
        conn.execute("INSERT INTO inventory VALUES ('preexisting', 42)")
    h.create()
    _, _, accepted = h.action("PREPARE", expected="FAILED")
    operation = h.service.store.operation(accepted["operationId"], "orchestrator")
    assert operation["error"]["code"] == "TARGET_NOT_EMPTY"
    with psycopg.connect(os.environ["LAB_TARGET_ADMIN"]) as conn:
        assert conn.execute("SELECT stock FROM inventory WHERE sku='preexisting'").fetchone() == (
            42,
        )


@pytest.mark.integration
def test_connected_target_is_rejected_without_disconnecting_clients(onprem_case):
    h, _ = onprem_case
    with psycopg.connect(os.environ["LAB_TARGET_ADMIN"]) as other:
        h.create()
        _, _, accepted = h.action("PREPARE", expected="FAILED")
        operation = h.service.store.operation(accepted["operationId"], "orchestrator")
        assert operation["error"]["code"] == "TARGET_IN_USE"
        assert other.execute("SELECT 1").fetchone() == (1,)


@pytest.mark.integration
def test_existing_rds_binding_requires_explicit_onprem_profile(onprem_case, apps):
    # Register today's cloud source binding; do not replace it when adding the on-prem target.
    h, _ = onprem_case
    source_db, target_db = resources(apps)
    source = database_endpoint(source_db) | {
        "provider": "AWS_RDS",
        "providerInstance": h.service.registry.databases["resource:source"].provider_instance,
    }
    target = database_endpoint(target_db)
    binding = dict(
        resourceId="database",
        source=source,
        schema=dict(
            tables=["public.inventory", "public.orders"], sequences=["public.orders_id_seq"]
        ),
        workload=dict(participants=["lab-app", "lab-worker"]),
    )
    response = apps.client.post(
        "/v1/bindings", json=binding, headers={"Idempotency-Key": "current-rds"}
    )
    assert response.status_code == 202, response.text
    bid = response.json()["bindingId"]
    apps.execute(response.json())
    initial = apps.state(bid)
    body = dict(
        workflowId="wf-onprem-direct",
        resourceId="database",
        kind="DATABASE",
        source=source,
        target=target,
        policy=dict(rollbackMode="BEFORE_TARGET_WRITE", maxQuiesceSeconds=60),
    )
    reject(apps, body, "UNSUPPORTED_PROFILE")
    assert apps.service.store.transact(lambda tx: tx.list("MIG")) == []
    h, _ = create(apps, body | {"profile": ONPREM_LOGICAL_PROFILE})
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("CANCEL")
    assert h.state()["lifecycle"] == "CANCELLED"
    current = apps.state(bid)
    assert (current["source"], current["proxy"]) == (initial["source"], initial["proxy"])
