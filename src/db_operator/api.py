"""인증, HTTP 요청 접수와 공통 응답 형식. 실제 작업은 Worker가 실행합니다."""

from __future__ import annotations

import json
import secrets
from importlib.resources import files
from typing import Annotated

import psycopg
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import Depends, FastAPI, Header, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from google.api_core.exceptions import GoogleAPICallError
from google.auth.exceptions import GoogleAuthError

from .config import Settings
from .models import (
    ActionRequest,
    BindingActionRequest,
    CreateMigration,
    OperatorError,
    RegisterBinding,
)
from .service import Service


def create_app(settings: Settings | None = None) -> FastAPI:
    service = Service(settings or Settings())
    app = FastAPI(title="Foxtrot DB Operator", version="0.2.0")
    app.state.service = service

    def authenticate_service(authorization: Annotated[str | None, Header()] = None) -> str:
        expected = "Bearer " + service.settings.api_token.get_secret_value()
        if not authorization or not secrets.compare_digest(
            authorization.encode(), expected.encode()
        ):
            raise OperatorError("UNAUTHORIZED", "서비스 인증이 필요합니다.", 401)
        return "orchestrator"

    def idempotency_key(
        value: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=256)],
    ) -> str:
        return value

    @app.exception_handler(OperatorError)
    async def operator_error(_: Request, error: OperatorError):
        return JSONResponse(
            status_code=error.status,
            content={
                "code": error.code,
                "message": error.message,
                "operationId": None,
                "blockers": [error.code],
            },
        )

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_: Request, __: RequestValidationError):
        return JSONResponse(
            status_code=400,
            content={
                "code": "INVALID_REQUEST",
                "message": "필수 필드와 요청 형식을 확인하십시오.",
                "operationId": None,
                "blockers": [],
            },
        )

    @app.exception_handler(BotoCoreError)
    @app.exception_handler(ClientError)
    @app.exception_handler(psycopg.Error)
    @app.exception_handler(GoogleAPICallError)
    @app.exception_handler(GoogleAuthError)
    async def store_unavailable(request: Request, __: Exception):
        return JSONResponse(
            status_code=503,
            content={
                "code": "SERVICE_UNAVAILABLE",
                "message": (
                    "접수 결과를 확인할 수 없습니다. 같은 키로 조회하십시오."
                    if request.method == "POST"
                    else "상태를 조회할 수 없습니다. 잠시 후 다시 조회하십시오."
                ),
                "operationId": None,
                "blockers": [
                    "RESULT_UNCONFIRMED" if request.method == "POST" else "MANAGEMENT_UNAVAILABLE"
                ],
            },
        )

    @app.get("/live", include_in_schema=False)
    def live():
        # DynamoDB 장애가 정상 프록시의 Task 교체로 번지지 않도록 실행 상태만 검사합니다.
        return {"status": "ok"}

    @app.get("/health", include_in_schema=False)
    def health():
        service.store.health()
        return {"status": "ok"}

    @app.get("/v1/capabilities")
    def capabilities(_: str = Depends(authenticate_service)):
        return service.capabilities()

    @app.post("/v1/migrations", status_code=201)
    def create_migration(
        plan: CreateMigration,
        response: Response,
        owner: str = Depends(authenticate_service),
        request_key: str = Depends(idempotency_key),
    ):
        result = service.create(owner, request_key, plan)
        response.headers["Location"] = "/v1/migrations/" + result["migrationId"]
        return result

    @app.get("/v1/migrations/{migration_id}")
    def get_migration(
        migration_id: str, response: Response, owner: str = Depends(authenticate_service)
    ):
        response.headers["Retry-After"] = "2"
        return service.get(migration_id, owner)

    @app.post("/v1/migrations/{migration_id}/actions", status_code=202)
    def submit_action(
        migration_id: str,
        body: ActionRequest,
        response: Response,
        owner: str = Depends(authenticate_service),
        request_key: str = Depends(idempotency_key),
    ):
        result = service.submit(owner, migration_id, request_key, body)
        response.headers["Location"] = result["statusUrl"]
        response.headers["Retry-After"] = "2"
        return result

    @app.get("/v1/operations/{operation_id}")
    def get_operation(
        operation_id: str, response: Response, owner: str = Depends(authenticate_service)
    ):
        response.headers["Retry-After"] = "2"
        return service.store.operation(operation_id, owner)

    @app.post("/v1/bindings", status_code=202)
    def register_binding(
        body: RegisterBinding,
        response: Response,
        owner: str = Depends(authenticate_service),
        request_key: str = Depends(idempotency_key),
    ):
        result = service.bindings.register(owner, request_key, body)
        response.headers["Location"] = result["statusUrl"]
        return result

    @app.get("/v1/bindings/{binding_id}")
    def get_binding(binding_id: str, owner: str = Depends(authenticate_service)):
        return service.bindings.get(binding_id, owner)

    @app.post("/v1/bindings/{binding_id}/actions", status_code=202)
    def binding_action(
        binding_id: str,
        body: BindingActionRequest,
        response: Response,
        owner: str = Depends(authenticate_service),
        request_key: str = Depends(idempotency_key),
    ):
        result = service.bindings.submit(owner, binding_id, request_key, body)
        response.headers["Location"] = result["statusUrl"]
        return result

    # One shared wire contract; tests validate actual responses against it.
    app.openapi = lambda: json.loads(files("db_operator").joinpath("contract.json").read_text())
    return app
