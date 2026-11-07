"""직접 입력과 고정 앱 접속점의 반복 이전 경계를 검사합니다."""

import json
from copy import deepcopy
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pydantic import ValidationError
from test_bindings import apps as apps
from test_control_safety import advance_pass_with_elapsed_timestamps
from test_operator_integration import Harness, validate

from db_operator import config
from db_operator.config import database_endpoint
from db_operator.models import (
    ATOMIC_LOGICAL_PROFILE,
    CreateBinding,
    DatabaseEndpoint,
    DirectMigration,
)
from db_operator.postgres import Postgres
from db_operator.runtime import restore_managed_bindings
from db_operator.service import Service
from db_operator.worker import Worker


def endpoint(host="source.example"):
    return {
        "host": host,
        "database": "appdb",
        "secretRef": "arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:test-endpoint",
        "writerRole": "app_writer",
    }


@pytest.mark.parametrize("field", ["password", "adminDsn", "replicationDsn", "sql"])
def test_direct_endpoint_rejects_secret_values_and_commands(field):
    with pytest.raises(ValidationError):
        DatabaseEndpoint.model_validate(endpoint() | {field: "not-accepted"})


def test_direct_contract_matches_endpoint_models_and_rejects_mixed_refs():
    binding = {
        "resourceId": "database",
        "kind": "DATABASE",
        "source": endpoint(),
        "schema": {"tables": ["public.orders"]},
        "workload": {"participants": ["web", "worker"]},
    }
    migration = {
        "workflowId": "wf-next",
        "resourceId": "database",
        "kind": "DATABASE",
        "source": endpoint(),
        "target": endpoint("target.example"),
        "policy": {"rollbackMode": "BEFORE_TARGET_WRITE", "maxQuiesceSeconds": 300},
    }
    CreateBinding.model_validate(binding)
    DirectMigration.model_validate(migration)
    validate("RegisterBinding", binding)
    validate("CreateMigration", migration)
    with pytest.raises(ValidationError):
        DirectMigration.model_validate(migration | {"sourceRef": "stale:source"})
    with pytest.raises(ValidationError):
        CreateBinding.model_validate(binding | {"alias": "caller_alias"})


def resources(apps):
    return [
        apps.service.store.transact(lambda tx, ref=ref: apps.service.bindings.resource(tx, ref)[0])
        for ref in ["db:a", "db:target"]
    ]


@pytest.fixture(autouse=True)
def docker_route_transport(request, monkeypatch):
    if "apps" not in request.fixturenames:
        return
    apps = request.getfixturevalue("apps")
    source, target = resources(apps)
    mapping = {
        database.route_host: conninfo_to_dict(database.secret("admin"))
        for database in [source, target]
    }
    connect = psycopg.connect

    def routed_connection(conninfo="", *args, **kwargs):
        values = conninfo_to_dict(conninfo)
        if values.get("host") in mapping:
            mapped = mapping[values["host"]]
            conninfo = make_conninfo(conninfo, host=mapped["host"], port=mapped["port"])
        return connect(conninfo, *args, **kwargs)

    # 호스트에서 보이지 않는 Compose DNS만 실제 컨테이너의 공개 포트로 바꿉니다.
    monkeypatch.setattr(psycopg, "connect", routed_connection)


def register(apps):
    source, target = resources(apps)
    body = {
        "resourceId": "database",
        "kind": "DATABASE",
        "source": database_endpoint(source),
        "schema": {
            "tables": ["public.inventory", "public.orders"],
            "sequences": ["public.orders_id_seq"],
        },
        "workload": {"participants": ["lab-app", "lab-worker"]},
    }
    response = apps.client.post(
        "/v1/bindings", json=body, headers={"Idempotency-Key": "first-binding"}
    )
    assert response.status_code == 202, response.json()
    accepted = response.json()
    validate("BindingOperationAccepted", accepted)
    apps.execute(accepted)
    state = apps.state(accepted["bindingId"])
    return body, accepted, state, source, target


def migration(source, target):
    return {
        "workflowId": "workflow-" + uuid4().hex,
        "resourceId": "database",
        "kind": "DATABASE",
        "source": database_endpoint(source),
        "target": database_endpoint(target),
        "policy": {"rollbackMode": "BEFORE_TARGET_WRITE", "maxQuiesceSeconds": 300},
    }


def create(apps, body, key=None):
    h = Harness(apps.settings)
    response = h.client.post(
        "/v1/migrations", json=body, headers={"Idempotency-Key": key or uuid4().hex}
    )
    assert response.status_code == 201, response.json()
    validate("Migration", response.json())
    h.mid = response.json()["migrationId"]
    return h, response.json()


def switch(h):
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    h.action("QUIESCE")
    h.action("CUTOVER")
    h.action("RESUME", location="TARGET")


def reject(apps, body, code):
    response = apps.client.post(
        "/v1/migrations", json=body, headers={"Idempotency-Key": uuid4().hex}
    )
    assert response.status_code in {409, 422}, response.json()
    assert response.json()["code"] == code, response.json()


@pytest.mark.integration
def test_initial_direct_binding_is_fixed_idempotent_and_secret_free(apps):
    body, accepted, state, source, _ = register(apps)
    assert state["source"] == body["source"]
    assert state["bindingRef"] == accepted["bindingId"] == accepted["bindingRef"]
    assert state["proxy"] == accepted["proxy"]
    assert state["proxy"]["protocol"] == "postgresql"
    assert state["lastMigrationId"] is None
    assert state["status"] == "ACTIVE"
    with apps.connect(state["bindingId"]) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 10
    again = apps.client.post(
        "/v1/bindings", json=body, headers={"Idempotency-Key": "first-binding"}
    )
    assert again.json() == accepted
    other = apps.client.post(
        "/v1/bindings",
        json=body | {"resourceId": "other-app"},
        headers={"Idempotency-Key": "other-app"},
    )
    assert other.status_code == 409 and other.json()["code"] == "APP_ALREADY_REGISTERED"
    conflict = apps.client.post(
        "/v1/bindings",
        json=body | {"resourceId": "other-app"},
        headers={"Idempotency-Key": "first-binding"},
    )
    assert conflict.status_code == 409 and conflict.json()["code"] == "IDEMPOTENCY_CONFLICT"
    rejected = apps.client.get(
        "/v1/bindings/" + state["bindingId"], headers={"Authorization": "Bearer another-app"}
    )
    assert rejected.status_code == 401
    stored = json.dumps(apps.service.store.transact(lambda tx: tx.list("CATALOG_DB")))
    for name in ["adminDsn", "replicationDsn", "writerPassword"]:
        assert name not in stored
    assert "identity" not in state["source"]
    isolated = Service(apps.settings.model_copy(update={"state_namespace": "other-" + apps.suffix}))
    isolated.store.initialize()
    assert isolated.store.transact(lambda tx: tx.list("BINDING")) == []
    assert source.secret_ref == state["source"]["secretRef"]


@pytest.mark.integration
def test_direct_migration_blocks_unfinished_unknown_source_and_unsupported_profile(apps):
    _, accepted, _, source, target = register(apps)
    body = migration(source, target)
    reject(apps, body | {"source": body["source"] | {"host": "stale.example"}}, "SOURCE_MISMATCH")
    reject(apps, body | {"resourceId": "different-app"}, "RESOURCE_MISMATCH")
    reject(apps, body | {"profile": ATOMIC_LOGICAL_PROFILE}, "UNSUPPORTED_PROFILE")
    assert apps.service.store.transact(lambda tx: tx.list("MIG")) == []
    h, _ = create(apps, body)
    assert apps.state(accepted["bindingId"])["lastMigrationId"] == h.mid
    reject(apps, migration(source, target), "MIGRATION_ACTIVE")
    h.service.store.update(h.mid, lambda row: row["state"].update(health="UNKNOWN"))
    reject(apps, migration(source, target), "MIGRATION_ACTIVE")


@pytest.mark.integration
@pytest.mark.parametrize("field", ["adminDsn", "replicationDsn"])
def test_direct_target_secret_mismatch_stops_before_any_external_change(apps, field):
    _, _, _, source, target = register(apps)
    h, _ = create(apps, migration(source, target))
    config.read_database_secret(target.secret_ref)[field] = config.read_database_secret(
        source.secret_ref
    )[field]
    _, _, accepted = h.action("PREPARE", expected="FAILED")
    operation = h.client.get(accepted["statusUrl"]).json()
    assert operation["error"]["code"] == "ENDPOINT_SECRET_MISMATCH"
    assert h.service.store._read("OP#" + accepted["operationId"])["external_intent"] is False
    assert not h.service.store.get(h.mid)["internal"].get("prepared")


@pytest.mark.integration
def test_finalize_keeps_proxy_restores_without_old_source_and_repeats(apps, monkeypatch):
    _, accepted, initial, source, target = register(apps)
    bid, fixed_proxy = accepted["bindingId"], initial["proxy"]
    body = migration(source, target)
    h, first_created = create(apps, body, "first-migration")
    switch(h)
    advance_pass_with_elapsed_timestamps(h)

    # 마지막 관측 응답 유실은 자원 점유를 풀지 않아야 합니다.
    actual_observe = Postgres.observe

    def lost_final_observation(driver, *args, **kwargs):
        if h.service.store.get(h.mid)["state"]["lifecycle"] == "COMPLETED":
            raise psycopg.OperationalError("owned test final observation unavailable")
        return actual_observe(driver, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(Postgres, "observe", lost_final_observation)
        h.action("FINALIZE", expected="UNKNOWN")
    assert h.service.store.transact(lambda tx: tx.list("MIG_CLAIM"))
    reject(apps, migration(target, source), "MIGRATION_ACTIVE")
    h.action("RECONCILE")
    assert not h.service.store.transact(lambda tx: tx.list("MIG_CLAIM"))
    completed = deepcopy(h.service.store.get(h.mid))
    assert h.state()["allowedActions"] == []
    state = apps.state(bid)
    assert state["proxy"] == fixed_proxy and state["source"] == database_endpoint(target)
    assert len(h.service.store.transact(lambda tx: tx.list("DB_BINDING"))) == 1
    assert len(h.service.store.transact(lambda tx: tx.list("BINDING_ACTUAL"))) == 2

    old_secret = config.read_database_secret
    maintenance = make_conninfo(source.secret("admin"), dbname="postgres")
    with psycopg.connect(maintenance, autocommit=True) as conn:
        conn.execute(
            sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
                sql.Identifier(source.database)
            )
        )
    try:

        def unavailable_old_secret(reference):
            assert reference != source.secret_ref, "retired source Secret was requested"
            return old_secret(reference)

        with monkeypatch.context() as patch:
            patch.setattr(config, "read_database_secret", unavailable_old_secret)
            restarted = Service(apps.settings)
            worker = Worker(restarted)
            with worker.exclusive():
                restore_managed_bindings(restarted, worker.assert_lease)
            assert restarted.bindings.get(bid)["status"] == "ACTIVE"
            with apps.connect(bid) as conn:
                conn.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
                assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 11
            assert h.state()["lifecycle"] == "COMPLETED"
    finally:
        with psycopg.connect(maintenance, autocommit=True) as conn:
            conn.execute(
                sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS true").format(
                    sql.Identifier(source.database)
                )
            )

    # 재사용 대상 준비는 시험이 명시적으로 수행합니다. 운영 API는 데이터를 지우지 않습니다.
    with psycopg.connect(source.secret("admin"), autocommit=True) as conn:
        conn.execute("TRUNCATE orders,inventory RESTART IDENTITY")
        assert conn.execute("SELECT count(*) FROM foxtrot_migration.barriers").fetchone()[0] == 0
    reject(apps, migration(source, target), "SOURCE_MISMATCH")
    h2, _ = create(apps, migration(target, source))
    assert h2.mid != h.mid
    h2.generation = h.generation + 1
    switch(h2)
    advance_pass_with_elapsed_timestamps(h2)
    h2.action("FINALIZE")
    assert apps.state(bid)["source"] == database_endpoint(source)
    assert apps.state(bid)["proxy"] == fixed_proxy
    assert h.service.store.get(h.mid) == completed
    assert (
        apps.client.post(
            "/v1/migrations", json=body, headers={"Idempotency-Key": "first-migration"}
        ).json()
        == first_created
    )
    frozen = apps.client.post(
        "/v1/migrations/" + h.mid + "/actions",
        json={"action": "RECONCILE", "expectedRevision": completed["state"]["revision"]},
        headers={"Idempotency-Key": "cannot-change-completed-history"},
    )
    assert frozen.status_code == 409 and frozen.json()["code"] == "MIGRATION_COMPLETED"
    with apps.connect(bid) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 11
