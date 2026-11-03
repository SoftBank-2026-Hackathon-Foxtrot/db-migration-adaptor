from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

PROFILE = "postgres-logical-pgbouncer-v1"


class Action(StrEnum):
    PREPARE = "PREPARE"
    START_SYNC = "START_SYNC"
    QUIESCE = "QUIESCE"
    CUTOVER = "CUTOVER"
    RESUME = "RESUME"
    ROLLBACK = "ROLLBACK"
    CANCEL = "CANCEL"
    RECONCILE = "RECONCILE"
    FINALIZE = "FINALIZE"


HELD_ACTIONS = {Action.QUIESCE, Action.CUTOVER, Action.RESUME, Action.ROLLBACK}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Policy(StrictModel):
    rollback_mode: Literal["BEFORE_TARGET_WRITE", "REVERSE_REQUIRED"] = Field(alias="rollbackMode")
    max_quiesce_seconds: int = Field(alias="maxQuiesceSeconds", ge=1, le=300)


class CreateMigration(StrictModel):
    workflow_id: str = Field(alias="workflowId", min_length=1, max_length=128)
    resource_id: str = Field(alias="resourceId", min_length=1, max_length=128)
    kind: Literal["DATABASE", "REDIS", "OBJECT_STORAGE", "COMPUTE"]
    profile: str = Field(min_length=1)
    source_ref: str = Field(alias="sourceRef", min_length=1)
    target_ref: str = Field(alias="targetRef", min_length=1)
    binding_ref: str = Field(alias="bindingRef", min_length=1)
    workload_scope_ref: str = Field(alias="workloadScopeRef", min_length=1)
    policy: Policy


class ActionRequest(StrictModel):
    action: Action
    expected_revision: int = Field(alias="expectedRevision", ge=0)
    transition_id: str | None = Field(
        default=None, alias="transitionId", min_length=1, max_length=128
    )
    quiesce_ref: str | None = Field(default=None, alias="quiesceRef", min_length=1, max_length=256)
    expected_location: Literal["SOURCE", "TARGET"] | None = Field(
        default=None, alias="expectedLocation"
    )

    @model_validator(mode="after")
    def required_fields(self) -> ActionRequest:
        if self.action in HELD_ACTIONS and (not self.transition_id or not self.quiesce_ref):
            raise ValueError("transitionId and quiesceRef are required")
        if self.action == Action.RESUME and not self.expected_location:
            raise ValueError("expectedLocation is required")
        if self.action != Action.RESUME and self.expected_location is not None:
            raise ValueError("expectedLocation is only accepted for RESUME")
        if self.action not in HELD_ACTIONS and (self.transition_id or self.quiesce_ref):
            raise ValueError("transition fields are not accepted for this action")
        return self


class OperatorError(Exception):
    def __init__(self, code: str, message: str, status: int = 422):
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


def require(condition: object, code: str, message: str) -> None:
    if not condition:
        raise OperatorError(code, message)
