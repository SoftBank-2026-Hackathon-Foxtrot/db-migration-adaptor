from copy import deepcopy

from db_operator.observability import public_state


def test_projection_is_read_only_and_excludes_internal_secrets():
    record = {
        "state": {"transitionId": "cutover-1", "observed": {"observedAt": "2026-11-07T10:00:00Z"}},
        "internal": {
            "observedDetails": {
                "source": {"sessions": 2, "dsn": "secret"},
                "target": {"sessions": None},
                "targetReachable": False,
                "accessEvidence": "PERSISTED_TARGET_FENCE",
                "paused": True,
            },
            "validationEvidence": {"transitionId": "cutover-0", "method": "NATIVE_DURABLE_BARRIER"},
        },
    }
    original = deepcopy(record)
    details = public_state(record)["details"]["database"]
    assert details["sourceWriterSessions"] == 2
    assert details["targetWriterSessions"] is None
    assert details["verification"] is None
    assert details["accessEvidence"] == "PERSISTED_TARGET_FENCE"
    assert "secret" not in str(details)
    assert record == original


def test_failed_observation_keeps_original_time_and_missing_counts():
    record = {
        "state": {"observed": {"observedAt": "2026-11-07T10:00:00Z"}},
        "internal": {"observationUnavailable": True},
    }
    details = public_state(record)["details"]["database"]
    assert details["observationAvailable"] is False
    assert details["sourceWriterSessions"] is None
    assert details["observedAt"] is None
