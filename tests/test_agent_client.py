"""오케스트레이터의 실제 HTTP 클라이언트로 DB 에이전트의 복사·전환을 검사합니다."""

import asyncio
import time
from uuid import uuid4

import httpx
import pytest
from test_managed_logical import managed_case as managed_case
from test_operator_integration import write

from db_operator.api import create_app
from db_operator.control import actor_ack
from db_operator.store import now
from db_operator.worker import Worker

pytestmark = pytest.mark.integration

adapter = pytest.importorskip("control_plane.adapters.client")
profiles = pytest.importorskip("control_plane.adapters.profiles")
domain = pytest.importorskip("control_plane.domain.models")
seed_module = pytest.importorskip("control_plane.seed")
gate_module = pytest.importorskip("control_plane.dbgate")


def test_orchestrator_client_drives_the_native_agent(managed_case, monkeypatch):
    settings, probe = managed_case
    app = create_app(settings)
    service = app.state.service
    worker = Worker(service)
    gate = gate_module.SdkDbGate(
        settings.state_table,
        namespace=settings.state_namespace,
        region="ap-northeast-2",
        endpoint=settings.dynamodb_endpoint,
    )

    def unexpected_dms(*_, **__):
        pytest.fail("native agent must not create a DMS client")

    monkeypatch.setattr("db_operator.dms.DmsClient", unexpected_dms)

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://db-agent"
        ) as http:
            client = adapter.AdapterClient(http, settings.api_token.get_secret_value())
            workflow = domain.Workflow(
                id="wf-" + uuid4().hex,
                application="reservation",
                source_env="aws",
                target_env="gcp",
                created_by="test",
                created_at=now(),
            )
            seed = seed_module.DatabaseSeed(
                sourceRef="resource:source",
                targetRef="resource:target",
                policy={"rollbackMode": "BEFORE_TARGET_WRITE", "maxQuiesceSeconds": 60},
                catalog={
                    "databases": {},
                    "tables": ["public.inventory", "public.orders"],
                    "alias": "appdb",
                    "appRef": "app:reservation",
                },
            )
            body = profiles.DatabaseProfile().plan_body(
                workflow,
                seed,
                domain.ResourceState(
                    kind=domain.ResourceKind.DATABASE,
                    binding_ref="binding:appdb",
                    workload_scope_ref="workload:lab",
                ),
                "workload:lab",
            )
            assert "replicationRef" not in body
            view = await client.create_migration("plan-" + workflow.id, body)
            mid = view.migration_id
            transition = "tx-" + uuid4().hex
            held_since = None

            async def action(name, location=None):
                nonlocal held_since
                request = {"action": name}
                if name == "CUTOVER":
                    held_since = held_since or now()
                    ref = await gate.issue_held(
                        migration_id=mid,
                        transition_id=transition,
                        generation=1,
                        held_since=held_since,
                        participants=[actor_ack(p, 1, "HELD") for p in ["lab-app", "lab-worker"]],
                    )
                    request.update(transitionId=transition, quiesceRef=ref)
                elif name in {"QUIESCE", "RESUME"}:
                    request.update(transitionId=transition, quiesceRef="check-only:" + transition)
                if location:
                    request["expectedLocation"] = location
                # 실제 CommandRunner도 정지 근거 저장 후 revision을 다시 읽습니다.
                current = await client.migration(mid)
                request["expectedRevision"] = current.revision
                key = uuid4().hex
                operation = await client.act(mid, key, request)
                assert await client.act(mid, key, request) == operation

                def execute():
                    with worker.exclusive():
                        assert worker.run_once()

                await asyncio.to_thread(execute)
                result = await client.operation(operation)
                assert result.status == "SUCCEEDED", result.error
                return await client.migration(mid)

            await action("PREPARE")
            await action("START_SYNC")
            committed = [write(probe) for _ in range(4)]
            deadline = time.monotonic() + 25
            while (await client.migration(mid)).lifecycle != "READY":
                assert time.monotonic() < deadline
                await asyncio.sleep(0.1)
            held = await action("QUIESCE")
            assert held.raw["observed"]["businessWriteAccess"] == "SOURCE_ONLY"
            switched = await action("CUTOVER")
            assert switched.route == "TARGET"
            assert switched.raw["observed"]["businessWriteAccess"] == "TARGET_ONLY"
            request_id, new_id = write(probe)
            assert new_id > max(item[1] for item in committed)
            active = await action("RESUME", "TARGET")
            assert active.lifecycle == "ACTIVE"
            assert "ROLLBACK" not in active.allowed_actions
            row = service.store.get(mid)
            driver = service.scoped(row).driver
            source, target, _ = driver.resources(row["plan"])
            with driver.connect(target) as conn:
                assert conn.execute(
                    "SELECT 1 FROM orders WHERE request_id=%s", (request_id,)
                ).fetchone()
                assert conn.execute("SELECT count(*) AS n FROM orders").fetchone()["n"] == 15
            with driver.connect(source) as conn:
                assert (
                    conn.execute(
                        "SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (source.writer_role,)
                    ).fetchone()["rolcanlogin"]
                    is False
                )

    asyncio.run(run())
