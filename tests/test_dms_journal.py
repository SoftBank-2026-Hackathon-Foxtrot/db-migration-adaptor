from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from google.api_core.exceptions import DeadlineExceeded, PermissionDenied, ServiceUnavailable
from test_dms_client import cloud_operation, sdk_fixture

from db_operator.dms import DmsJournal, DmsService
from db_operator.models import OperatorError

pytestmark = pytest.mark.integration


@pytest.fixture
def dms_case(state_store):
    binding, job, profiles, sdk, client = sdk_fixture(uuid4().hex)
    journal = DmsJournal(state_store)
    journal.initialize()
    for action, method in [
        ("VERIFY", "verify_migration_job"),
        ("START_SYNC", "start_migration_job"),
    ]:
        getattr(sdk, method).return_value = SimpleNamespace(
            operation=cloud_operation(binding, action)
        )
    service = DmsService(client, journal)
    yield binding, job, profiles, sdk, journal, service


def verified(case):
    binding, _, _, sdk, _, service = case
    row = service.submit("VERIFY", "verify-1")
    sdk.get_operation.return_value = cloud_operation(binding)
    assert service.poll(row["id"])["status"] == "SUCCEEDED"


def test_verify_then_start_and_duplicate_after_cloud_state_changes(dms_case):
    binding, job, _, sdk, _, service = dms_case
    verified(dms_case)
    row = service.submit("START_SYNC", "start-1")
    job.state, job.phase = "RUNNING", "CDC"
    assert service.submit("START_SYNC", "start-1")["id"] == row["id"]
    sdk.start_migration_job.assert_called_once()
    sdk.get_operation.return_value = cloud_operation(binding, "START_SYNC")
    assert service.poll(row["id"])["status"] == "SUCCEEDED"
    assert service.client.observe()["cutoverReady"] == "UNKNOWN"


def test_start_without_successful_verification_has_no_external_effect(dms_case):
    *_, sdk, _, service = dms_case
    with pytest.raises(OperatorError, match="연결 검사"):
        service.submit("START_SYNC", "start-1")
    sdk.start_migration_job.assert_not_called()


def test_endpoint_change_invalidates_successful_verification(dms_case):
    binding, _, profiles, sdk, _, service = dms_case
    verified(dms_case)
    profiles[binding.source].postgresql.host = "different-rds.test"
    with pytest.raises(OperatorError, match="연결 검사"):
        service.submit("START_SYNC", "start-1")
    sdk.start_migration_job.assert_not_called()


def test_lost_submission_response_is_durable_and_never_resent(dms_case):
    binding, _, _, sdk, journal, service = dms_case
    sdk.verify_migration_job.side_effect = DeadlineExceeded("response lost after acceptance")
    with pytest.raises(OperatorError, match="접수 여부가 불명"):
        service.submit("VERIFY", "verify-1")
    new_process = DmsService(service.client, DmsJournal(journal.store))
    assert new_process.submit("VERIFY", "verify-1")["status"] == "UNKNOWN"
    with pytest.raises(OperatorError, match="불명인 DMS 실행"):
        new_process.submit("VERIFY", "another-key")
    sdk.verify_migration_job.assert_called_once()
    with journal.lock(binding.job) as lease:
        assert journal.find(lease, binding.job, "verify-1")["status"] == "UNKNOWN"


def test_abandoned_intent_after_process_exit_blocks_resubmission(dms_case):
    binding, _, _, sdk, journal, service = dms_case
    with journal.lock(binding.job) as conn:
        journal.begin(
            conn, binding.job, "VERIFY", "verify-1", service.client.observe()["fingerprint"]
        )
    assert service.submit("VERIFY", "verify-1")["status"] == "UNKNOWN"
    sdk.verify_migration_job.assert_not_called()


def test_temporary_operation_get_failure_does_not_reexecute_mutation(dms_case):
    binding, _, _, sdk, journal, service = dms_case
    row = service.submit("VERIFY", "verify-1")
    sdk.get_operation.side_effect = ServiceUnavailable("read failed")
    with pytest.raises(ServiceUnavailable):
        service.poll(row["id"])
    with journal.lock(binding.job) as conn:
        assert journal.row(conn, binding.job, row["id"])["status"] == "PENDING"
    sdk.get_operation.side_effect = None
    sdk.get_operation.return_value = cloud_operation(binding)
    assert service.poll(row["id"])["status"] == "SUCCEEDED"
    sdk.verify_migration_job.assert_called_once()


def test_cloud_failure_does_not_authorize_start(dms_case):
    binding, _, _, sdk, _, service = dms_case
    row = service.submit("VERIFY", "verify-1")
    sdk.get_operation.return_value = cloud_operation(binding, code=7)
    result = service.poll(row["id"])
    assert result["status"] == "FAILED" and result["error_code"] == 7
    with pytest.raises(OperatorError, match="연결 검사"):
        service.submit("START_SYNC", "start-1")


def test_two_executors_cannot_control_same_job(dms_case):
    binding, _, _, sdk, journal, service = dms_case
    with journal.lock(binding.job), pytest.raises(OperatorError, match="다른 실행기"):
        service.submit("VERIFY", "verify-1")
    sdk.verify_migration_job.assert_not_called()


def test_lost_management_connection_prevents_cloud_submission(dms_case, monkeypatch):
    binding, _, _, sdk, journal, service = dms_case
    begin = journal.begin

    def terminate_after_intent(lease, *args):
        row = begin(lease, *args)
        journal.store.release_stopped_executor(lease.name, lease.owner, "test:stopped-before-rpc")
        return row

    monkeypatch.setattr(journal, "begin", terminate_after_intent)
    with pytest.raises(OperatorError, match="실행기 잠금"):
        service.submit("VERIFY", "verify-1")
    sdk.verify_migration_job.assert_not_called()
    monkeypatch.setattr(journal, "begin", begin)
    assert service.submit("VERIFY", "verify-1")["status"] == "UNKNOWN"


def test_changed_binding_cannot_poll_old_operation(dms_case):
    binding, _, profiles, sdk, _, service = dms_case
    row = service.submit("VERIFY", "verify-1")
    profiles[binding.destination].cloudsql.cloud_sql_id = "other-cloud-sql"
    with pytest.raises(OperatorError, match="설정"):
        service.poll(row["id"])
    sdk.get_operation.assert_not_called()


def test_explicit_permission_rejection_is_failed_and_can_be_reverified(dms_case):
    binding, _, _, sdk, _, service = dms_case
    sdk.verify_migration_job.side_effect = PermissionDenied("not allowed")
    row = service.submit("VERIFY", "verify-1")
    assert row["status"] == "FAILED" and row["error_code"] == 7
    sdk.verify_migration_job.side_effect = None
    assert service.submit("VERIFY", "verify-2")["status"] == "PENDING"


@pytest.mark.parametrize("mismatch", [None, "old_time", "wrong_target", "wrong_action"])
def test_unknown_submission_reconciles_only_with_matching_cloud_evidence(dms_case, mismatch):
    binding, _, _, sdk, journal, service = dms_case
    sdk.verify_migration_job.side_effect = DeadlineExceeded("response lost")
    with pytest.raises(OperatorError):
        service.submit("VERIFY", "verify-1")
    with journal.lock(binding.job) as conn:
        row = journal.find(conn, binding.job, "verify-1")
    op = cloud_operation(
        binding,
        "START_SYNC" if mismatch == "wrong_action" else "VERIFY",
        target=binding.job + "-other" if mismatch == "wrong_target" else None,
    )
    if mismatch == "old_time":
        from google.cloud import clouddms_v1 as dms

        metadata = dms.OperationMetadata(
            target=binding.job, verb="verify", create_time=datetime.now(UTC) - timedelta(days=1)
        )
        op.metadata.Pack(dms.OperationMetadata.pb(metadata))
    sdk.get_operation.return_value = op
    if mismatch:
        with pytest.raises(OperatorError):
            service.reconcile(row["id"], op.name)
        with journal.lock(binding.job) as conn:
            assert journal.row(conn, binding.job, row["id"])["status"] == "UNKNOWN"
    else:
        assert service.reconcile(row["id"], op.name)["status"] == "SUCCEEDED"
    sdk.verify_migration_job.assert_called_once()
