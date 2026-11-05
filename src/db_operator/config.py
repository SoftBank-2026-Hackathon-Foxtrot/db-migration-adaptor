"""실행 설정, 등록한 DB·프록시·업무 범위를 읽습니다. Secret 값은 환경에서 가져옵니다."""

from __future__ import annotations

import json
import os
from pathlib import Path

from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from .models import StrictModel, require


class Database(StrictModel):
    identity: str
    admin_env: str
    replication_env: str
    writer_role: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    route_host: str = Field(pattern=r"^[a-zA-Z0-9_.:-]+$")
    route_port: int = Field(ge=1, le=65535, default=5432)
    database: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,62}$")

    def secret(self, which: str) -> str:
        value = os.environ.get(getattr(self, which + "_env"))
        require(value, "SECRET_UNAVAILABLE", "등록한 Secret을 읽을 수 없습니다.")
        return str(value)


class Binding(StrictModel):
    admin_env: str
    probe_env: str
    config_path: Path
    alias: str = Field(pattern=r"^[a-z_][a-z0-9_]{0,62}$")


class Scope(StrictModel):
    participants: list[str] = Field(min_length=1)


class Registry(StrictModel):
    databases: dict[str, Database]
    bindings: dict[str, Binding]
    workloads: dict[str, Scope]
    tables: list[str] = Field(min_length=1)
    sequences: list[str] = Field(default_factory=list)

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
    retention_seconds: int = Field(default=3600, ge=0)
    poll_seconds: float = Field(default=0.5, ge=0.05)
    observation_cache_seconds: float = Field(default=2, ge=0, le=10)
