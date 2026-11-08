"""공통 API 요청의 필드와 명령별 필수값을 Pydantic으로 검사합니다."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

PROFILE = "postgres-logical-pgbouncer-v1"
DMS_PROFILE = "gcp-dms-postgres-pgbouncer-v1"
MANAGED_LOGICAL_PROFILE = "postgres-managed-logical-pgbouncer-v1"
ATOMIC_LOGICAL_PROFILE = "postgres-managed-logical-pgbouncer-v2"
MANAGED_LOGICAL_PROFILES = {MANAGED_LOGICAL_PROFILE, ATOMIC_LOGICAL_PROFILE}
ONPREM_LOGICAL_PROFILE = "postgres-rds-onprem-logical-pgbouncer-v1"
# These profiles use PG17's forward subscription and durable barrier, not PG18 reverse CDC.
FORWARD_LOGICAL_PROFILES = MANAGED_LOGICAL_PROFILES | {ONPREM_LOGICAL_PROFILE}


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


def needs_held_evidence(profile: str, action: Action) -> bool:
    return action in HELD_ACTIONS and not (
        profile == ATOMIC_LOGICAL_PROFILE and action in {Action.QUIESCE, Action.RESUME}
    )


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Policy(StrictModel):
    rollback_mode: Literal["BEFORE_TARGET_WRITE", "REVERSE_REQUIRED"] = Field(alias="rollbackMode")
    max_quiesce_seconds: int = Field(alias="maxQuiesceSeconds", ge=1, le=300)


class DatabaseEndpoint(StrictModel):
    """접속 정보에는 비밀번호 대신 Secrets Manager 참조만 받습니다."""

    host: str = Field(pattern=r"^[a-zA-Z0-9_.:-]+$")
    port: int = Field(default=5432, ge=1, le=65535)
    database: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    secret_ref: str = Field(
        alias="secretRef",
        pattern=r"^arn:aws[a-z-]*:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+$",
    )
    provider: Literal["SELF_MANAGED", "AWS_RDS", "GCP_CLOUDSQL"] = "SELF_MANAGED"
    provider_instance: str | None = Field(default=None, alias="providerInstance", min_length=1)
    writer_role: str = Field(alias="writerRole", pattern=r"^[a-z_][a-z0-9_]{0,62}$")

    @model_validator(mode="after")
    def managed_instance(self):
        if self.provider != "SELF_MANAGED" and not self.provider_instance:
            raise ValueError("Managed databases require providerInstance")
        return self


class MigrationSchema(StrictModel):
    tables: list[str] = Field(min_length=1)
    sequences: list[str] = Field(default_factory=list)


class MigrationWorkload(StrictModel):
    participants: list[str] = Field(min_length=1)


class CreateBinding(StrictModel):
    resource_id: str = Field(alias="resourceId", min_length=1, max_length=128)
    kind: Literal["DATABASE"] = "DATABASE"
    profile: str | None = Field(default=None, min_length=1)
    source: DatabaseEndpoint
    schema_spec: MigrationSchema = Field(alias="schema")
    workload: MigrationWorkload
    pool_size: int = Field(default=20, alias="poolSize", ge=1, le=1000)
    max_db_connections: int = Field(default=20, alias="maxDbConnections", ge=1, le=1000)


class DirectMigration(StrictModel):
    workflow_id: str = Field(alias="workflowId", min_length=1, max_length=128)
    resource_id: str = Field(alias="resourceId", min_length=1, max_length=128)
    kind: Literal["DATABASE"] = "DATABASE"
    profile: str | None = Field(default=None, min_length=1)
    source: DatabaseEndpoint
    target: DatabaseEndpoint
    binding_ref: str | None = Field(default=None, alias="bindingRef", min_length=1)
    workload_scope_ref: str | None = Field(default=None, alias="workloadScopeRef", min_length=1)
    policy: Policy
    replication_ref: str | None = Field(default=None, alias="replicationRef", min_length=1)


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
    replication_ref: str | None = Field(default=None, alias="replicationRef", min_length=1)

    @model_validator(mode="after")
    def replication_profile(self):
        if (self.profile == DMS_PROFILE) != bool(self.replication_ref):
            raise ValueError("DMS profile requires replicationRef; other profiles reject it")
        if self.profile == DMS_PROFILE and self.policy.rollback_mode != "BEFORE_TARGET_WRITE":
            raise ValueError("GCP DMS reverse replication is not configured")
        if (
            self.profile in FORWARD_LOGICAL_PROFILES
            and self.policy.rollback_mode != "BEFORE_TARGET_WRITE"
        ):
            raise ValueError("Managed logical reverse replication is not configured")
        return self


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
    provider_operation_ref: str | None = Field(
        default=None, alias="providerOperationRef", min_length=1, max_length=512
    )

    @model_validator(mode="after")
    def required_fields(self) -> ActionRequest:
        if self.provider_operation_ref and self.action != Action.RECONCILE:
            raise ValueError("providerOperationRef is only accepted for RECONCILE")
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


class RegisterBinding(StrictModel):
    app_ref: str = Field(alias="appRef", min_length=1, max_length=128)
    database_ref: str = Field(alias="databaseRef", min_length=1, max_length=128)
    alias: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    workload_scope_ref: str = Field(alias="workloadScopeRef", min_length=1, max_length=128)
    pool_size: int = Field(default=20, alias="poolSize", ge=1, le=1000)
    max_db_connections: int = Field(default=20, alias="maxDbConnections", ge=1, le=1000)


class BindingActionRequest(StrictModel):
    action: Literal["UNREGISTER", "RETRY", "RECONCILE", "RESTORE", "CANCEL"]
    expected_revision: int = Field(alias="expectedRevision", ge=0)
    app_ref: str | None = Field(default=None, alias="appRef", min_length=1, max_length=128)
    mode: Literal["STOP_APP", "KEEP_RUNNING"] | None = None
    evidence_ref: str | None = Field(
        default=None, alias="evidenceRef", min_length=1, max_length=256
    )

    @model_validator(mode="after")
    def required_fields(self):
        if self.action == "UNREGISTER" and not (self.app_ref and self.mode and self.evidence_ref):
            raise ValueError("appRef, mode and evidenceRef are required")
        if self.action != "UNREGISTER" and (self.app_ref or self.mode):
            raise ValueError("appRef and mode are only accepted for UNREGISTER")
        if self.action in {"RECONCILE", "RESTORE", "CANCEL"} and self.evidence_ref:
            raise ValueError("RECONCILE/RESTORE does not accept evidenceRef")
        return self


def require(condition: object, code: str, message: str, status: int = 422) -> None:
    if not condition:
        raise OperatorError(code, message, status)
