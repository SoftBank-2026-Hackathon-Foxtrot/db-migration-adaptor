import pytest
from pydantic import ValidationError

from db_operator.models import ActionRequest


def test_proxy_cannot_bypass_fenced_writer():
    import json
    from pathlib import Path

    from db_operator.config import Registry
    from db_operator.models import OperatorError
    from db_operator.postgres import Postgres

    registry = Registry.model_validate(
        json.loads((Path(__file__).parents[1] / "registry.example.json").read_text())
    )
    driver = Postgres(registry)
    driver.observe = lambda _: {"proxy": {"force_user": "postgres"}}
    with pytest.raises(OperatorError, match="강제 접속"):
        driver.preflight(
            {
                "sourceRef": "resource:source",
                "targetRef": "resource:target",
                "bindingRef": "binding:appdb",
            }
        )


@pytest.mark.parametrize("action", ["QUIESCE", "CUTOVER", "RESUME", "ROLLBACK"])
@pytest.mark.parametrize("missing", ["transitionId", "quiesceRef"])
def test_held_actions_require_evidence(action, missing):
    body = {"action": action, "expectedRevision": 1, "transitionId": "tx-1", "quiesceRef": "q-1"}
    if action == "RESUME":
        body["expectedLocation"] = "TARGET"
    body.pop(missing)
    with pytest.raises(ValidationError):
        ActionRequest.model_validate(body)


def test_resume_requires_location():
    with pytest.raises(ValidationError):
        ActionRequest.model_validate(
            {"action": "RESUME", "expectedRevision": 1, "transitionId": "tx", "quiesceRef": "q"}
        )


@pytest.mark.parametrize("extra", ["sql", "command", "password", "host"])
def test_arbitrary_commands_and_secrets_rejected(extra):
    with pytest.raises(ValidationError):
        ActionRequest.model_validate(
            {"action": "PREPARE", "expectedRevision": 0, extra: "untrusted"}
        )
