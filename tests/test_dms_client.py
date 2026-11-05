from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from google.cloud import clouddms_v1 as dms
from google.longrunning.operations_pb2 import Operation
from google.protobuf.any_pb2 import Any

from db_operator.dms import DmsClient, JobBinding
from db_operator.models import OperatorError

PARENT = "projects/foxtrot-test/locations/asia-northeast3"


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
    return binding, job, profiles, sdk, DmsClient(binding, sdk)


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
