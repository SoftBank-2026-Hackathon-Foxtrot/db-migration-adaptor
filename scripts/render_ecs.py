"""AWS에 접속하지 않고 ECS Task·서비스 등록 JSON을 만듭니다."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class Deployment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    image: str
    cluster: str
    execution_role_arn: str
    task_role_arn: str
    secret_arn: str
    state_table: str
    state_namespace: str
    assign_public_ip: bool = True
    cloudmap_service_arn: str
    subnets: list[str] = Field(min_length=1)
    security_groups: list[str] = Field(min_length=1)
    region: str = "ap-northeast-2"
    family: str = "foxtrot-db-operator"
    log_group: str = "/ecs/foxtrot-db-operator"
    cpu_architecture: str = Field(default="ARM64", pattern="^(ARM64|X86_64)$")
    binding_ref: str = "binding:appdb"
    initial_database_ref: str = "resource:source"
    bindings_enabled: bool = False
    public_host: str = "db.foxtrot.invalid"
    gcp_credentials_secret_arn: str | None = None


SECRET_KEYS = [
    "DB_OPERATOR_API_TOKEN",
    "DB_OPERATOR_REGISTRY_JSON",
    "DB_OPERATOR_RUNTIME_USERS_JSON",
    "DB_OPERATOR_RUNTIME_CLIENT_CERTIFICATE_PEM",
    "DB_OPERATOR_RUNTIME_CLIENT_KEY_PEM",
    "DB_OPERATOR_RUNTIME_SERVER_CA_PEM",
    "SOURCE_ADMIN_DSN",
    "TARGET_ADMIN_DSN",
    "SOURCE_REPLICATION_DSN",
    "TARGET_REPLICATION_DSN",
    "PGBOUNCER_ADMIN_DSN",
    "PGBOUNCER_PROBE_DSN",
]


def manifests(config: Deployment) -> tuple[dict, dict]:
    containers = []
    for role, cpu, memory in [("pooler", 128, 256), ("backend", 384, 768)]:
        container = {
            "name": role,
            "image": config.image,
            "entryPoint": ["foxtrot-db-runtime"],
            "command": [role],
            "essential": True,
            "cpu": cpu,
            "memory": memory,
            "user": "10001:10001",
            "readonlyRootFilesystem": True,
            "stopTimeout": 120,
            "environment": [
                {"name": "DB_OPERATOR_STATE_TABLE", "value": config.state_table},
                {"name": "DB_OPERATOR_STATE_NAMESPACE", "value": config.state_namespace},
                {"name": "AWS_DEFAULT_REGION", "value": config.region},
                {"name": "DB_OPERATOR_REGISTRY", "value": "/run/foxtrot/registry.json"},
                {"name": "DB_OPERATOR_RUNTIME_BINDING_REF", "value": config.binding_ref},
                {
                    "name": "DB_OPERATOR_RUNTIME_INITIAL_DATABASE_REF",
                    "value": config.initial_database_ref,
                },
                {"name": "DB_OPERATOR_RUNTIME_CLIENT_TLS", "value": "require"},
                {"name": "DB_OPERATOR_RUNTIME_SERVER_TLS", "value": "verify-full"},
            ],
            "secrets": [
                {"name": key, "valueFrom": f"{config.secret_arn}:{key}::"}
                for key in SECRET_KEYS
                if not config.bindings_enabled
                or key
                not in {
                    "SOURCE_ADMIN_DSN",
                    "TARGET_ADMIN_DSN",
                    "SOURCE_REPLICATION_DSN",
                    "TARGET_REPLICATION_DSN",
                    "PGBOUNCER_PROBE_DSN",
                }
            ],
            "mountPoints": [
                {
                    "sourceVolume": "pooler-runtime",
                    "containerPath": "/run/foxtrot",
                    "readOnly": False,
                },
                {"sourceVolume": role + "-tmp", "containerPath": "/tmp", "readOnly": False},
            ],
            "linuxParameters": {"capabilities": {"drop": ["ALL"]}},
            "logConfiguration": {
                "logDriver": "awslogs",
                "options": {
                    "awslogs-group": config.log_group,
                    "awslogs-region": config.region,
                    "awslogs-stream-prefix": role,
                },
            },
        }
        container["environment"].extend(
            [
                {
                    "name": "DB_OPERATOR_BINDINGS_ENABLED",
                    "value": str(config.bindings_enabled).lower(),
                },
                {"name": "DB_OPERATOR_PUBLIC_HOST", "value": config.public_host},
            ]
        )
        if role != "pooler":
            container["dependsOn"] = [{"containerName": "pooler", "condition": "HEALTHY"}]
            if config.gcp_credentials_secret_arn:
                container["secrets"].append(
                    {
                        "name": "DB_OPERATOR_RUNTIME_GCP_CREDENTIALS_JSON",
                        "valueFrom": config.gcp_credentials_secret_arn,
                    }
                )
        health_command = (
            [
                "CMD-SHELL",
                "test -f /run/foxtrot/pooler.ready && pg_isready -q -h 127.0.0.1 -p 6432 "
                '-d pgbouncer -U "${DB_OPERATOR_RUNTIME_ADMIN_USER:-pooler_admin}" -t 3',
            ]
            if role == "pooler"
            else [
                "CMD",
                "curl",
                "--insecure",
                "--fail",
                "--silent",
                "--show-error",
                "--max-time",
                "3",
                "https://127.0.0.1:8080/live",
            ]
        )
        container["healthCheck"] = {
            "command": health_command,
            "interval": 10,
            "timeout": 8,
            "retries": 3,
            "startPeriod": 60,
        }
        container["portMappings"] = [
            {"containerPort": 6432 if role == "pooler" else 8080, "protocol": "tcp"}
        ]
        containers.append(container)
    task = {
        "family": config.family,
        "networkMode": "awsvpc",
        "requiresCompatibilities": ["FARGATE"],
        "cpu": "512",
        "memory": "1024",
        "runtimePlatform": {
            "cpuArchitecture": config.cpu_architecture,
            "operatingSystemFamily": "LINUX",
        },
        "executionRoleArn": config.execution_role_arn,
        "taskRoleArn": config.task_role_arn,
        "volumes": [{"name": name} for name in ["pooler-runtime", "pooler-tmp", "backend-tmp"]],
        "containerDefinitions": containers,
    }
    service = {
        "cluster": config.cluster,
        "serviceName": config.family,
        "taskDefinition": "REPLACE_WITH_REGISTERED_TASK_DEFINITION_ARN",
        "desiredCount": 1,
        "launchType": "FARGATE",
        "platformVersion": "1.4.0",
        "deploymentController": {"type": "ECS"},
        "deploymentConfiguration": {
            "minimumHealthyPercent": 0,
            "maximumPercent": 100,
            "deploymentCircuitBreaker": {"enable": True, "rollback": True},
        },
        "networkConfiguration": {
            "awsvpcConfiguration": {
                "subnets": config.subnets,
                "securityGroups": config.security_groups,
                "assignPublicIp": "ENABLED" if config.assign_public_ip else "DISABLED",
            }
        },
        "serviceRegistries": [{"registryArn": config.cloudmap_service_arn}],
    }
    return task, service


def state_template(config: Deployment) -> dict:
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "OperatorState": {
                "Type": "AWS::DynamoDB::Table",
                "DeletionPolicy": "Retain",
                "UpdateReplacePolicy": "Retain",
                "Properties": {
                    "TableName": config.state_table,
                    "BillingMode": "PAY_PER_REQUEST",
                    "DeletionProtectionEnabled": True,
                    "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": True},
                    "AttributeDefinitions": [
                        {"AttributeName": "namespace", "AttributeType": "S"},
                        {"AttributeName": "record", "AttributeType": "S"},
                    ],
                    "KeySchema": [
                        {"AttributeName": "namespace", "KeyType": "HASH"},
                        {"AttributeName": "record", "KeyType": "RANGE"},
                    ],
                },
            }
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config = Deployment.model_validate_json(args.config.read_text())
    task, service = manifests(config)
    args.output.mkdir(parents=True, exist_ok=True)
    for name, value in [
        ("task-definition.json", task),
        ("service.json", service),
        ("state-table.json", state_template(config)),
    ]:
        (args.output / name).write_text(json.dumps(value, indent=2) + "\n")
    print(f"ECS 등록 파일 생성 완료: {args.output}")


if __name__ == "__main__":
    main()
