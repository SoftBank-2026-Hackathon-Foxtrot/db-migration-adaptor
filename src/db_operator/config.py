"""실행 설정, 등록한 DB·프록시·업무 범위를 읽습니다. Secret 값은 환경에서 가져옵니다."""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

import boto3
from botocore.config import Config
from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import DatabaseEndpoint, StrictModel, require


class Database(StrictModel):
    identity: str
    provider: Literal["SELF_MANAGED", "AWS_RDS", "GCP_CLOUDSQL"] = "SELF_MANAGED"
    provider_instance: str | None = None
    admin_env: str = ""
    replication_env: str = ""
    secret_ref: str | None = Field(
        default=None,
        pattern=r"^arn:aws[a-z-]*:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+$",
    )
    writer_role: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    route_host: str = Field(pattern=r"^[a-zA-Z0-9_.:-]+$")
    route_port: int = Field(ge=1, le=65535, default=5432)
    database: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,62}$")

    @model_validator(mode="after")
    def managed_identity(self):
        if self.provider != "SELF_MANAGED" and (
            not self.provider_instance
            or self.identity != (f"{self.provider}:{self.provider_instance}/{self.database}")
        ):
            raise ValueError("managed identity must contain provider, instance and database")
        return self

    def secret(self, which: str) -> str:
        if self.secret_ref:
            value = read_database_secret(self.secret_ref).get(which + "Dsn")
            require(
                isinstance(value, str) and value,
                "SECRET_UNAVAILABLE",
                "DB Secret의 DSN이 없습니다.",
            )
            return value
        value = os.environ.get(getattr(self, which + "_env"))
        require(value, "SECRET_UNAVAILABLE", "등록한 Secret을 읽을 수 없습니다.")
        return str(value)


def endpoint_database(endpoint: DatabaseEndpoint) -> Database:
    instance = endpoint.provider_instance or f"{endpoint.host.lower()}:{endpoint.port}"
    return Database(
        identity=f"{endpoint.provider}:{instance}/{endpoint.database}",
        provider=endpoint.provider,
        provider_instance=endpoint.provider_instance,
        secret_ref=endpoint.secret_ref,
        writer_role=endpoint.writer_role,
        route_host=endpoint.host,
        route_port=endpoint.port,
        database=endpoint.database,
    )


def database_endpoint(database: Database) -> dict:
    return DatabaseEndpoint(
        host=database.route_host,
        port=database.route_port,
        database=database.database,
        secret_ref=database.secret_ref,
        provider=database.provider,
        provider_instance=database.provider_instance,
        writer_role=database.writer_role,
    ).model_dump(by_alias=True, exclude_none=True)


class Binding(StrictModel):
    admin_env: str
    probe_env: str
    config_path: Path
    alias: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    generation: str | None = None
    probe_database: Database | None = Field(default=None, exclude=True)

    def probe_dsn(self) -> str:
        if self.probe_database is None:
            value = os.environ.get(self.probe_env)
            require(value, "SECRET_UNAVAILABLE", "앱 연결 검사 Secret이 없습니다.")
            return value
        from psycopg.conninfo import conninfo_to_dict, make_conninfo

        admin = conninfo_to_dict(os.environ[self.admin_env])
        secret = read_database_secret(self.probe_database.secret_ref)
        return make_conninfo(
            **{
                **admin,
                "dbname": self.alias,
                "user": self.probe_database.writer_role,
                "password": secret["writerPassword"],
            }
        )


class Scope(StrictModel):
    participants: list[str] = Field(min_length=1)


class Registry(StrictModel):
    databases: dict[str, Database]
    bindings: dict[str, Binding]
    workloads: dict[str, Scope]
    tables: list[str] = Field(default_factory=list)
    sequences: list[str] = Field(default_factory=list)
    dms_jobs: dict[str, dict] = Field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> Registry:
        value = cls.model_validate(json.loads(path.read_text()))
        for name in [*value.tables, *value.sequences]:
            parts = name.split(".")
            require(
                len(parts) == 2 and all(p.isidentifier() for p in parts),
                "INVALID_REGISTRY",
                "table·sequence는 schema.name 형식이어야 합니다.",
            )
        require(
            len(set(value.tables)) == len(value.tables), "INVALID_REGISTRY", "중복 table입니다."
        )
        return value


class StateSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_OPERATOR_", populate_by_name=True)
    state_table: str = Field(min_length=3)
    state_namespace: str = Field(default="foxtrot-db-operator", min_length=1, max_length=200)
    aws_region: str | None = Field(
        default=None,
        validation_alias=AliasChoices("DB_OPERATOR_AWS_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"),
    )
    dynamodb_endpoint: str | None = None


class Settings(StateSettings):
    api_token: SecretStr = Field(min_length=24)
    registry: Path
    poll_seconds: float = Field(default=0.5, ge=0.05)
    observation_cache_seconds: float = Field(default=2, ge=0, le=10)
    bindings_enabled: bool = False
    app_resource_id: str | None = Field(default=None, min_length=1, max_length=128)
    pooler_directory: Path = Path("/run/foxtrot")
    public_host: str = "db.foxtrot.invalid"
    public_port: int = Field(default=6432, ge=1, le=65535)
    binding_drain_seconds: int = Field(default=10, ge=1, le=20)


@lru_cache(maxsize=8)
def secrets_client(region: str):
    return boto3.client(
        "secretsmanager",
        region_name=region,
        config=Config(
            connect_timeout=3, read_timeout=5, retries={"mode": "standard", "total_max_attempts": 2}
        ),
    )


def read_database_secret(reference: str) -> dict:
    """Secret 값은 상태 저장소·응답·로그에 기록하지 않습니다."""
    value = secrets_client(reference.split(":")[3]).get_secret_value(SecretId=reference)
    secret = json.loads(value["SecretString"])
    require(
        isinstance(secret, dict)
        and all(
            isinstance(secret.get(k), str) and secret[k]
            for k in ["adminDsn", "replicationDsn", "writerPassword"]
        ),
        "SECRET_INVALID",
        "DB Secret 필드를 확인하십시오.",
    )
    return secret


class ResourceCatalog:
    """플랫폼 백엔드가 승인한 DB·업무 범위를 게시하는 내부 SDK입니다."""

    def __init__(self, store):
        self.store = store

    def database(
        self,
        reference: str,
        database: Database,
        *,
        tables: list[str],
        sequences: list[str] | None = None,
    ) -> None:
        value = self.database_entry(reference, database, tables=tables, sequences=sequences)
        self._publish("CATALOG_DB", reference, value)

    @staticmethod
    def database_entry(reference, database, *, tables, sequences=None) -> dict:
        require(
            database.secret_ref, "SECRET_REFERENCE_REQUIRED", "Secrets Manager 참조가 필요합니다."
        )
        registry = Registry(
            databases={reference: database},
            bindings={},
            workloads={},
            tables=tables,
            sequences=sequences or [],
        )
        # Registry의 식별자 검사도 적용합니다.
        for name in [*tables, *(sequences or [])]:
            require(
                len(name.split(".")) == 2 and all(p.isidentifier() for p in name.split(".")),
                "INVALID_REGISTRY",
                "table·sequence는 schema.name 형식이어야 합니다.",
            )
        require(len(tables) == len(set(tables)), "INVALID_REGISTRY", "중복 table입니다.")
        return registry.model_dump(mode="json")

    def workload(self, reference: str, participants: list[str]) -> None:
        require(
            len(set(participants)) == len(participants), "INVALID_SCOPE", "참여자가 중복됐습니다."
        )
        self._publish("CATALOG_SCOPE", reference, Scope(participants=participants).model_dump())

    def dms_job(self, reference: str, *, source_ref: str, target_ref: str, binding) -> None:
        """미리 준비한 GCP DMS 작업을 게시합니다. 클라우드 자원을 만들지 않습니다."""
        from .dms import JobBinding

        value = binding if isinstance(binding, JobBinding) else JobBinding.model_validate(binding)
        require(source_ref != target_ref, "DMS_BINDING_INVALID", "원본과 대상은 달라야 합니다.")
        self._publish(
            "CATALOG_DMS",
            reference,
            {"sourceRef": source_ref, "targetRef": target_ref, "binding": value.model_dump()},
        )

    def _publish(self, kind: str, reference: str, value: dict) -> None:
        self.store.transact(lambda tx: self.publish(tx, kind, reference, value))

    @staticmethod
    def publish(tx, kind: str, reference: str, value: dict) -> None:
        previous = tx.get(kind, reference)
        require(
            previous is None or previous == value,
            "CATALOG_IMMUTABLE",
            "등록한 리소스 정의는 변경할 수 없습니다. 새 참조를 등록하십시오.",
        )
        tx.put(kind, reference, value)
