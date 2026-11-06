"""실제 PgBouncer에서 앱 등록·해제·이전의 경계를 검사합니다."""

import json
import os
import re
from datetime import timedelta
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo
from test_operator_integration import validate

from db_operator import config, config_file
from db_operator.api import create_app
from db_operator.config import ResourceCatalog
from db_operator.control import ControlPlaneRecords, actor_ack
from db_operator.models import OperatorError
from db_operator.runtime import ManagedPooler, restore_managed_bindings
from db_operator.service import Service
from db_operator.store import now
from db_operator.worker import Worker

pytestmark = pytest.mark.integration


@pytest.fixture
def apps(lab_case, monkeypatch, tmp_path):
    settings, legacy, _ = lab_case
    source = legacy.registry.databases["resource:source"]
    target = legacy.registry.databases["resource:target"]
    original_binding = legacy.registry.bindings["binding:appdb"]
    directory = original_binding.config_path.parent
    old_ini, old_meta = config_file.snapshot(original_binding.config_path)
    users, user_meta = config_file.snapshot(directory / "userlist.txt")
    hba_path = directory / "pg_hba.conf"
    hba_path.write_text(
        "host all pooler_admin 0.0.0.0/0 scram-sha-256\n"
        "host all all 0.0.0.0/0 reject\nhost all all ::/0 reject\n"
    )
    hba_path.chmod(0o644)  # The isolated fixture runs PgBouncer as nobody.
    hba_ini, replacements = re.subn(
        rb"(?m)^[ \t]*auth_type[ \t]*=[ \t]*scram-sha-256[ \t]*$",
        b"auth_type=hba\nauth_hba_file=/etc/pgbouncer/pg_hba.conf",
        old_ini,
    )
    assert replacements == 1, "랩의 SCRAM 인증 설정을 HBA로 바꾸지 못했습니다"
    config_file.replace(
        original_binding.config_path,
        old_ini,
        old_meta,
        hba_ini.replace(b"extra_float_digits,options", b"extra_float_digits"),
    )
    legacy.driver.admin(original_binding, "RELOAD")
    active_auth = {
        row["key"]: row["value"] for row in legacy.driver.admin(original_binding, "SHOW CONFIG")
    }
    assert active_auth["auth_type"] == "hba"
    assert active_auth["auth_hba_file"] == "/etc/pgbouncer/pg_hba.conf"
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"databases": {}, "bindings": {}, "workloads": {}, "tables": []}))
    settings = settings.model_copy(
        update={
            "registry": empty,
            "bindings_enabled": True,
            "pooler_directory": directory,
            "public_host": "127.0.0.1",
            "binding_drain_seconds": 1,
        }
    )
    monkeypatch.setenv(
        "PGBOUNCER_ADMIN_DSN",
        os.environ[original_binding.admin_env],
    )
    service = Service(settings)
    catalog = ResourceCatalog(service.store)
    suffix = uuid4().hex[:10]
    other_name, other_writer = "other_" + suffix, "other_writer_" + suffix
    password = "local-only-" + suffix
    with psycopg.connect(source.secret("admin"), autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(other_name)))
        conn.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                sql.Identifier(other_writer), sql.Literal(password)
            )
        )
    other_admin = make_conninfo(source.secret("admin"), dbname=other_name)
    with psycopg.connect(other_admin, autocommit=True) as conn:
        conn.execute("CREATE TABLE messages(id integer PRIMARY KEY, body text)")
        conn.execute(
            sql.SQL("GRANT SELECT,INSERT ON messages TO {}").format(sql.Identifier(other_writer))
        )
    initial_password = os.environ["LAB_POOLER_APP"]
    from psycopg.conninfo import conninfo_to_dict

    initial_password = conninfo_to_dict(initial_password)["password"]
    secrets = {}
    resources = {}
    for name, db, admin, replication, writer_password, tables, sequences in [
        (
            "a",
            source,
            source.secret("admin"),
            source.secret("replication"),
            initial_password,
            ["public.inventory", "public.orders"],
            ["public.orders_id_seq"],
        ),
        (
            "target",
            target,
            target.secret("admin"),
            target.secret("replication"),
            initial_password,
            ["public.inventory", "public.orders"],
            ["public.orders_id_seq"],
        ),
        (
            "b",
            source.model_copy(
                update={
                    "identity": "other/" + other_name,
                    "database": other_name,
                    "writer_role": other_writer,
                }
            ),
            other_admin,
            make_conninfo(source.secret("replication"), dbname=other_name),
            password,
            ["public.messages"],
            [],
        ),
    ]:
        arn = (
            "arn:aws:secretsmanager:ap-northeast-2:123456789012:secret:test-" + name + "-" + suffix
        )
        secrets[arn] = {
            "adminDsn": admin,
            "replicationDsn": replication,
            "writerPassword": writer_password,
        }
        value = db.model_copy(update={"secret_ref": arn, "admin_env": "", "replication_env": ""})
        resources[name] = value
        catalog.database("db:" + name, value, tables=tables, sequences=sequences)
    for app in ["a", "b", "shared"]:
        catalog.workload("scope:" + app, [app + "-web", app + "-worker"])
    monkeypatch.setattr(config, "read_database_secret", lambda ref: secrets[ref])
    harness = Apps(settings, service, suffix)
    try:
        yield harness
    finally:
        current, metadata = config_file.snapshot(original_binding.config_path)
        config_file.replace(original_binding.config_path, current, metadata, old_ini)
        current, metadata = config_file.snapshot(directory / "userlist.txt")
        config_file.replace(directory / "userlist.txt", current, metadata, users)
        legacy.driver.admin(original_binding, "RELOAD")
        hba_path.unlink(missing_ok=True)


class Apps:
    def __init__(self, settings, service, suffix):
        self.service, self.settings, self.suffix = service, settings, suffix
        self.client = TestClient(create_app(settings))
        self.client.headers["Authorization"] = "Bearer " + settings.api_token.get_secret_value()
        self.worker = Worker(service)
        self.records = ControlPlaneRecords(service.store)

    def execute(self, accepted, expected="SUCCEEDED"):
        with self.worker.exclusive():
            assert self.worker.run_once()
        response = self.client.get(accepted["statusUrl"])
        assert response.status_code == 200, response.json()
        assert response.json()["status"] == expected, response.json()
        validate("BindingOperation", response.json())
        return response.json()

    def register(self, app, db=None, alias=None, expected="SUCCEEDED", key=None):
        response = self.client.post(
            "/v1/bindings",
            json={
                "appRef": "app:" + app,
                "databaseRef": "db:" + (db or app),
                "alias": alias or app + "_" + self.suffix,
                "workloadScopeRef": "scope:" + app,
            },
            headers={"Idempotency-Key": key or uuid4().hex},
        )
        assert response.status_code == 202, response.json()
        validate("BindingOperationAccepted", response.json())
        self.execute(response.json(), expected)
        return response.json()["bindingId"]

    def state(self, bid):
        response = self.client.get("/v1/bindings/" + bid)
        assert response.status_code == 200, response.json()
        validate("Binding", response.json())
        return response.json()

    def connect(self, bid, *, database=None):
        row = self.service.bindings.records.get(bid)
        pooler = ManagedPooler(self.service, row)
        dsn = pooler.binding.probe_dsn()
        return psycopg.connect(
            make_conninfo(dsn, **({"dbname": database} if database else {})),
            autocommit=True,
        )

    def evidence(self, bid, app, mode="STOP_APP"):
        row = self.state(bid)
        return self.records.binding_release(
            bid,
            "app:" + app,
            mode=mode,
            participants=[
                actor_ack(app + suffix, 1, "HELD" if mode == "STOP_APP" else "RUNNING")
                for suffix in ["-web", "-worker"]
            ],
            connections_closed=True,
            direct_database_ref=row["databaseRef"] if mode == "KEEP_RUNNING" else None,
            probe_result="PASS" if mode == "KEEP_RUNNING" else None,
            probe_ref="probe:actual-direct-check" if mode == "KEEP_RUNNING" else None,
        )

    def action(
        self, bid, action, *, app=None, mode="STOP_APP", evidence=None, expected="SUCCEEDED"
    ):
        body = {"action": action, "expectedRevision": self.state(bid)["revision"]}
        if action == "UNREGISTER":
            body.update(
                appRef="app:" + app,
                mode=mode,
                evidenceRef=evidence or self.evidence(bid, app, mode),
            )
        elif evidence:
            body["evidenceRef"] = evidence
        response = self.client.post(
            "/v1/bindings/" + bid + "/actions", json=body, headers={"Idempotency-Key": uuid4().hex}
        )
        assert response.status_code == 202, response.json()
        return self.execute(response.json(), expected)


def test_two_apps_register_and_unregister_without_touching_other_connection(apps):
    a, b = apps.register("a"), apps.register("b")
    assert apps.state(a)["status"] == apps.state(b)["status"] == "ACTIVE"
    # 두 계정이 모두 유효할 때 alias 간 접근 차단을 검사합니다.
    with pytest.raises(psycopg.Error), apps.connect(a, database=apps.state(b)["alias"]):
        pass
    with apps.connect(b) as other:
        other.execute("INSERT INTO messages VALUES(1,'before')")
        apps.action(a, "UNREGISTER", app="a")
        other.execute("INSERT INTO messages VALUES(2,'after')")
        assert other.execute("SELECT count(*) FROM messages").fetchone()[0] == 2
    assert apps.state(a)["status"] == "UNREGISTERED"
    assert apps.state(b)["status"] == "ACTIVE"
    with pytest.raises(psycopg.Error), apps.connect(a):
        pass
    # A's valid credentials cannot be used with B's alias.
    with pytest.raises(psycopg.Error), apps.connect(a, database=apps.state(b)["alias"]):
        pass
    with apps.worker.exclusive() as lease:
        restore_managed_bindings(apps.service, lease.assert_owned)
    assert apps.state(a)["status"] == "UNREGISTERED"
    with apps.connect(b) as conn:
        assert conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 2
    assert not any(
        r["name"] == apps.state(a)["alias"]
        for r in ManagedPooler(apps.service, apps.service.bindings.records.get(b)).admin(
            "SHOW DATABASES"
        )
    )


def test_shared_database_keeps_alias_and_blocks_partial_direct_detach(apps):
    a = apps.register("a")
    shared = apps.register("shared", db="a", alias=apps.state(a)["alias"])
    assert shared == a and len(apps.state(a)["apps"]) == 2
    evidence = apps.evidence(a, "shared", "KEEP_RUNNING")
    response = apps.client.post(
        "/v1/bindings/" + a + "/actions",
        json={
            "action": "UNREGISTER",
            "expectedRevision": apps.state(a)["revision"],
            "appRef": "app:shared",
            "mode": "KEEP_RUNNING",
            "evidenceRef": evidence,
        },
        headers={"Idempotency-Key": uuid4().hex},
    )
    assert response.status_code == 409 and response.json()["code"] == "SHARED_DATABASE"
    with apps.connect(a) as conn:
        apps.action(a, "UNREGISTER", app="shared")
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 10
    assert apps.state(a)["apps"] == {"app:a": "scope:a"}


def test_remaining_client_requires_reconcile_and_fresh_release_evidence(apps):
    a, b = apps.register("a"), apps.register("b")
    client = apps.connect(a)
    try:
        apps.action(a, "UNREGISTER", app="a", expected="UNKNOWN")
        assert apps.state(a)["status"] == "UNKNOWN"
        apps.action(a, "RECONCILE", expected="UNKNOWN")
        with apps.connect(b) as conn:
            assert conn.execute("SELECT count(*) FROM messages").fetchone()[0] == 0
    finally:
        client.close()
    apps.action(a, "RECONCILE")
    assert apps.state(a)["status"] == "FAILED"
    fresh = apps.evidence(a, "a")
    apps.action(a, "RETRY", evidence=fresh)
    assert apps.state(a)["status"] == "UNREGISTERED"


def test_registration_idempotency_and_secret_values_are_not_persisted(apps):
    key = "register-once"
    a = apps.register("a", key=key)
    response = apps.client.post(
        "/v1/bindings",
        json={
            "appRef": "app:a",
            "databaseRef": "db:a",
            "alias": apps.state(a)["alias"],
            "workloadScopeRef": "scope:a",
        },
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 202 and response.json()["bindingId"] == a
    assert not apps.worker.store.transact(lambda tx: tx.list("QUEUE"))
    rows = apps.service.store.transact(lambda tx: tx.list("BINDING") + tx.list("OP"))
    assert "adminDsn" not in json.dumps(rows, default=str)
    assert "writerPassword" not in json.dumps(rows, default=str)
    assert "secret:" not in json.dumps(apps.state(a))


def test_missing_or_expired_release_evidence_is_rejected(apps):
    a = apps.register("a")
    ref = apps.evidence(a, "a")

    def expire(tx):
        gate = tx.get("BINDING_GATE", ref)
        gate["valid_until"] = now() - timedelta(seconds=1)
        tx.put("BINDING_GATE", ref, gate)

    apps.service.store.transact(expire)
    response = apps.client.post(
        "/v1/bindings/" + a + "/actions",
        json={
            "action": "UNREGISTER",
            "expectedRevision": apps.state(a)["revision"],
            "appRef": "app:a",
            "mode": "STOP_APP",
            "evidenceRef": ref,
        },
        headers={"Idempotency-Key": uuid4().hex},
    )
    assert response.status_code == 422 and response.json()["code"] == "RELEASE_EVIDENCE_EXPIRED"
    assert apps.state(a)["status"] == "ACTIVE"


def test_direct_detach_retains_database_and_data(apps):
    a = apps.register("a")
    db = ManagedPooler(apps.service, apps.service.bindings.records.get(a)).database
    direct = make_conninfo(
        db.secret("admin"),
        user=db.writer_role,
        password=config.read_database_secret(db.secret_ref)["writerPassword"],
    )
    with psycopg.connect(direct, autocommit=True) as app:
        app.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
        apps.action(a, "UNREGISTER", app="a", mode="KEEP_RUNNING")
        app.execute("INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa')", (uuid4(),))
    assert apps.state(a)["status"] == "UNREGISTERED"
    with psycopg.connect(db.secret("admin")) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 12
        assert (
            conn.execute(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (db.writer_role,)
            ).fetchone()[0]
            is True
        )


def test_restore_failure_isolated_and_retry_restores_only_selected_binding(apps, monkeypatch):
    a, b = apps.register("a"), apps.register("b")
    failing = ManagedPooler(apps.service, apps.service.bindings.records.get(a)).database.secret_ref
    original = config.read_database_secret

    def unavailable(reference):
        if reference == failing:
            raise RuntimeError("injected secret lookup failure")
        return original(reference)

    with monkeypatch.context() as patch:
        patch.setattr(config, "read_database_secret", unavailable)
        with apps.worker.exclusive() as lease:
            restore_managed_bindings(apps.service, lease.assert_owned)
        assert apps.state(a)["status"] == "FAILED"
        with apps.connect(b) as conn:
            conn.execute("INSERT INTO messages VALUES(1,'available')")
    apps.action(a, "RETRY")
    assert apps.state(a)["status"] == "ACTIVE"
    with apps.connect(a) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 10


def test_dynamic_binding_uses_existing_migration_and_reverse_rollback(apps, monkeypatch):
    from test_operator_integration import Harness

    from db_operator.models import PROFILE

    a, b = apps.register("a"), apps.register("b")
    apps.action(a, "RESTORE")
    old_release = apps.evidence(a, "a")
    h = Harness(apps.settings)
    plan = {
        "workflowId": "wf-" + uuid4().hex,
        "resourceId": "orders",
        "kind": "DATABASE",
        "profile": PROFILE,
        "sourceRef": "db:a",
        "targetRef": "db:target",
        "bindingRef": a,
        "workloadScopeRef": apps.state(a)["workloadScopeRef"],
        "policy": {"rollbackMode": "REVERSE_REQUIRED", "maxQuiesceSeconds": 300},
    }
    accepted = h.client.post("/v1/migrations", json=plan, headers={"Idempotency-Key": "migration"})
    assert accepted.status_code == 201, accepted.json()
    h.mid = accepted.json()["migrationId"]

    def held():
        return h.records.held(
            migration_id=h.mid,
            transition_id=h.transition,
            generation=h.generation,
            held_since=h.held_since,
            participants=[actor_ack(p, h.generation, "HELD") for p in ["a-web", "a-worker"]],
        )

    h.held = held
    h.action("PREPARE")
    h.action("START_SYNC")
    h.ready()
    rejected = apps.client.post(
        "/v1/bindings/" + a + "/actions",
        json={
            "action": "UNREGISTER",
            "expectedRevision": apps.state(a)["revision"],
            "appRef": "app:a",
            "mode": "STOP_APP",
            "evidenceRef": "binding-release:must-not-be-used-during-migration",
        },
        headers={"Idempotency-Key": uuid4().hex},
    )
    assert rejected.status_code == 409 and rejected.json()["code"] == "MIGRATION_ACTIVE"
    with apps.connect(b) as other:
        h.action("QUIESCE")
        h.action("CUTOVER")
        h.action("RESUME", location="TARGET")
        with apps.connect(a) as conn:
            assert (
                conn.execute(
                    "INSERT INTO orders(request_id,sku) VALUES(%s,'chiikawa') RETURNING id",
                    (uuid4(),),
                ).fetchone()[0]
                == 11
            )
        other.execute("INSERT INTO messages VALUES(1,'still-active')")
        h.generation += 1
        h.held_since = now()
        h.action("QUIESCE")
        h.action("ROLLBACK")
        h.action("RESUME", location="SOURCE")
        with apps.connect(a) as conn:
            assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 11
        other.execute("INSERT INTO messages VALUES(2,'after-rollback')")
    h.action("FINALIZE")
    stale = apps.client.post(
        "/v1/bindings/" + a + "/actions",
        json={
            "action": "UNREGISTER",
            "expectedRevision": apps.state(a)["revision"],
            "appRef": "app:a",
            "mode": "STOP_APP",
            "evidenceRef": old_release,
        },
        headers={"Idempotency-Key": uuid4().hex},
    )
    assert stale.status_code == 422 and stale.json()["code"] == "RELEASE_EVIDENCE_INVALID"
    # Replaying the original create remains valid after the active route changed twice.
    assert (
        h.client.post("/v1/migrations", json=plan, headers={"Idempotency-Key": "migration"}).json()[
            "migrationId"
        ]
        == h.mid
    )
    apps.action(a, "UNREGISTER", app="a")
    assert apps.state(a)["status"] == "UNREGISTERED"
    with monkeypatch.context() as patch:
        from db_operator.postgres import Postgres

        def no_remote_io(*args, **kwargs):
            raise AssertionError("완료 기록은 원격 DB에 접속하면 안 됩니다.")

        patch.setattr(Postgres, "observe", no_remote_io)
        assert h.state()["lifecycle"] == "COMPLETED"
        assert h.state()["allowedActions"] == ["RECONCILE"]
        h.action("RECONCILE")


def test_failed_initial_registration_can_be_cancelled_and_registered_again(apps, monkeypatch):
    with monkeypatch.context() as patch:

        def unavailable(_):
            raise RuntimeError("injected secret failure before external change")

        patch.setattr(config, "read_database_secret", unavailable)
        a = apps.register("a", expected="FAILED")
        assert apps.state(a)["endpoint"] is None
        apps.action(a, "CANCEL")
        assert apps.state(a)["status"] == "UNREGISTERED"
    replacement = apps.register("a")
    assert replacement != a and apps.state(replacement)["status"] == "ACTIVE"


def test_completed_registration_with_lost_confirmation_is_reconciled(apps, monkeypatch):
    original = ManagedPooler.verify
    with monkeypatch.context() as patch:

        def response_lost(pooler):
            original(pooler)
            raise RuntimeError("injected lost confirmation")

        patch.setattr(ManagedPooler, "verify", response_lost)
        a = apps.register("a", expected="UNKNOWN")
    assert apps.state(a)["endpoint"] is None
    apps.action(a, "RECONCILE")
    assert apps.state(a)["status"] == "ACTIVE"
    with apps.connect(a) as conn:
        assert conn.execute("SELECT count(*) FROM orders").fetchone()[0] == 10


def test_release_evidence_cannot_be_reused_after_revision_changes(apps):
    a = apps.register("a")
    old = apps.evidence(a, "a")
    apps.action(a, "RESTORE")
    response = apps.client.post(
        "/v1/bindings/" + a + "/actions",
        json={
            "action": "UNREGISTER",
            "expectedRevision": apps.state(a)["revision"],
            "appRef": "app:a",
            "mode": "STOP_APP",
            "evidenceRef": old,
        },
        headers={"Idempotency-Key": uuid4().hex},
    )
    assert response.status_code == 422 and response.json()["code"] == "RELEASE_EVIDENCE_INVALID"
    assert apps.state(a)["status"] == "ACTIVE"


def test_dynamic_runtime_refuses_legacy_state_adoption(apps):
    from db_operator.runtime import validate_managed_store

    apps.service.store.transact(
        lambda tx: tx.put(
            "ROUTE",
            "old-static-binding",
            {"binding_ref": "old-static-binding", "database_ref": "db:a", "access": "OPEN"},
        )
    )
    with pytest.raises(OperatorError) as error:
        validate_managed_store(apps.service)
    assert error.value.code == "RUNTIME_ADOPTION_REQUIRED"
