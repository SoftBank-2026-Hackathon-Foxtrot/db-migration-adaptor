"""ECS에서 PgBouncer를 복원하고 API·Worker를 실행합니다."""

from __future__ import annotations

import argparse
import http.client
import json
import logging
import os
import re
import signal
import ssl
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import Literal

import psycopg
import uvicorn
from botocore.exceptions import BotoCoreError, ClientError
from psycopg.conninfo import conninfo_to_dict
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from . import config_file
from .api import create_app
from .config import Registry, Settings
from .models import OperatorError, require
from .postgres import rewrite_route
from .service import Service
from .store import RouteJournal
from .worker import Worker

# ECS 설정과 Secret 파일


def write_private(path: Path, content: str) -> None:
    """같은 디렉터리에서 파일을 교체합니다. Secret 파일은 소유자만 읽습니다."""
    descriptor, name = tempfile.mkstemp(dir=path.parent, prefix=".runtime-")
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "w") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


class RuntimeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DB_OPERATOR_RUNTIME_")

    directory: Path = Path("/run/foxtrot")
    binding_ref: str
    initial_database_ref: str
    users_json: SecretStr
    registry_json: SecretStr = Field(validation_alias="DB_OPERATOR_REGISTRY_JSON")
    admin_user: str = Field(default="pooler_admin", pattern=r"^[a-z_][a-z0-9_]{0,62}$")
    client_tls: Literal["require", "disable"] = "require"
    server_tls: Literal["verify-full", "disable"] = "verify-full"
    client_certificate_pem: SecretStr | None = None
    client_key_pem: SecretStr | None = None
    server_ca_pem: SecretStr | None = None
    max_client_connections: int = Field(default=1000, ge=10, le=100000)
    pool_size: int = Field(default=20, ge=1, le=1000)
    query_wait_seconds: int = Field(default=330, ge=1, le=3600)

    def materialize_registry(self, settings: Settings, *, writable: bool) -> Registry:
        require(
            settings.registry.resolve() == (self.directory / "registry.json").resolve(),
            "RUNTIME_PATH_INVALID",
            "Registry는 공유 디렉터리의 registry.json을 사용하십시오.",
        )
        content = self.registry_json.get_secret_value()
        if writable:
            self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            write_private(settings.registry, content)
        else:
            require(
                settings.registry.read_text() == content,
                "RUNTIME_REGISTRY_MISMATCH",
                "컨테이너가 받은 Registry와 공유 파일이 다릅니다.",
            )
        registry = Registry.load(settings.registry)
        require(
            len(registry.bindings) == 1 and self.binding_ref in registry.bindings,
            "RUNTIME_BINDING_INVALID",
            "현재 ECS 실행 구성은 Task당 프록시 binding 하나를 지원합니다.",
        )
        binding = registry.bindings[self.binding_ref]
        require(
            binding.alias != "pgbouncer"
            and binding.config_path.resolve() == (self.directory / "pgbouncer.ini").resolve()
            and self.initial_database_ref in registry.databases,
            "RUNTIME_BINDING_INVALID",
            "프록시 alias, 공유 설정 경로, 초기 DB 참조를 확인하십시오.",
        )
        require(
            not re.search(r"[\s#;]", str(self.directory)),
            "RUNTIME_PATH_INVALID",
            "공유 디렉터리 경로에 공백이나 설정 구분 문자를 사용하지 마십시오.",
        )
        return registry


# PgBouncer 시작과 경로 복원

log = logging.getLogger(__name__)


def authentication_files(runtime: RuntimeSettings, service: Service) -> None:
    users = json.loads(runtime.users_json.get_secret_value())
    require(
        isinstance(users, dict)
        and runtime.admin_user in users
        and all(db.writer_role in users for db in service.registry.databases.values()),
        "RUNTIME_USERS_INVALID",
        "프록시 관리 계정과 업무 writer 비밀번호가 필요합니다.",
    )

    def quote(value: str) -> str:
        require(
            isinstance(value, str) and value and all(ord(char) >= 32 for char in value),
            "RUNTIME_USERS_INVALID",
            "프록시 인증 파일에 잘못된 값이 있습니다.",
        )
        return '"' + value.replace('"', '""') + '"'

    write_private(
        runtime.directory / "userlist.txt",
        "".join(f"{quote(user)} {quote(password)}\n" for user, password in users.items()),
    )
    # 관리 계정은 같은 Task의 loopback에서만 사용할 수 있습니다.
    write_private(
        runtime.directory / "pg_hba.conf",
        f"host all {runtime.admin_user} 127.0.0.1/32 scram-sha-256\n"
        f"host all {runtime.admin_user} ::1/128 scram-sha-256\n"
        f"host all {runtime.admin_user} 0.0.0.0/0 reject\n"
        f"host all {runtime.admin_user} ::/0 reject\n"
        "host all all 0.0.0.0/0 scram-sha-256\n"
        "host all all ::/0 scram-sha-256\n",
    )
    if runtime.client_tls == "require":
        require(
            runtime.client_certificate_pem and runtime.client_key_pem,
            "RUNTIME_TLS_MISSING",
            "앱 접속용 TLS 인증서와 개인 키가 필요합니다.",
        )
        write_private(
            runtime.directory / "client.crt", runtime.client_certificate_pem.get_secret_value()
        )
        write_private(runtime.directory / "client.key", runtime.client_key_pem.get_secret_value())
    if runtime.server_tls == "verify-full":
        require(runtime.server_ca_pem, "RUNTIME_TLS_MISSING", "DB 검증용 CA가 필요합니다.")
        write_private(runtime.directory / "server-ca.crt", runtime.server_ca_pem.get_secret_value())


def initial_config(runtime: RuntimeSettings, service: Service) -> str:
    binding = service.registry.bindings[runtime.binding_ref]
    tls = ""
    if runtime.client_tls == "require":
        tls += (
            f"client_tls_cert_file={runtime.directory}/client.crt\n"
            f"client_tls_key_file={runtime.directory}/client.key\n"
        )
    if runtime.server_tls == "verify-full":
        tls += f"server_tls_ca_file={runtime.directory}/server-ca.crt\n"
    # 첫 PAUSE 전에 접속이 와도 실제 DB에는 연결하지 않습니다.
    return (
        f"[databases]\n{binding.alias} = host=127.0.0.1 port=1 dbname=blocked\n\n"
        "[pgbouncer]\nlisten_addr=0.0.0.0\nlisten_port=6432\n"
        "unix_socket_dir=\n"
        f"pidfile={runtime.directory}/pgbouncer.pid\n"
        f"auth_type=hba\nauth_file={runtime.directory}/userlist.txt\n"
        f"auth_hba_file={runtime.directory}/pg_hba.conf\n"
        f"admin_users={runtime.admin_user}\npool_mode=transaction\n"
        f"max_client_conn={runtime.max_client_connections}\n"
        f"default_pool_size={runtime.pool_size}\n"
        f"query_wait_timeout={runtime.query_wait_seconds}\n"
        "server_connect_timeout=5\nserver_login_retry=1\n"
        "log_connections=0\nlog_disconnections=0\n"
        f"client_tls_sslmode={runtime.client_tls}\n"
        f"server_tls_sslmode={runtime.server_tls}\n{tls}"
    )


def require_local_admin(runtime: RuntimeSettings, service: Service) -> None:
    binding = service.registry.bindings[runtime.binding_ref]
    info = conninfo_to_dict(os.environ.get(binding.admin_env, ""))
    require(
        info.get("hostaddr", info.get("host")) in {"127.0.0.1", "::1"}
        and info.get("port", "5432") == "6432"
        and info.get("dbname") == "pgbouncer"
        and info.get("user") == runtime.admin_user,
        "RUNTIME_ADMIN_NOT_LOCAL",
        "PgBouncer 관리 DSN은 같은 Task의 loopback:6432를 사용하십시오.",
    )


def writers_match(service: Service, active_ref: str) -> bool:
    """초기화 시 writer 권한을 바꾸지 않습니다. 활성 DB 하나만 LOGIN이어야 합니다."""
    for ref, db in service.registry.databases.items():
        with service.driver.connect(db, tag="runtime-check") as conn:
            row = conn.execute(
                "SELECT current_database() AS db,rolcanlogin FROM pg_roles WHERE rolname=%s",
                (db.writer_role,),
            ).fetchone()
        if not row or row["db"] != db.database or row["rolcanlogin"] != (ref == active_ref):
            return False
    return True


def restore_route(
    runtime: RuntimeSettings, service: Service, assert_lease: Callable[[], None]
) -> str:
    binding = service.registry.bindings[runtime.binding_ref]
    driver = service.driver
    assert_lease()
    driver.admin(binding, "PAUSE {}", tag="runtime-start")
    route = RouteJournal(service.store, service.registry, runtime.binding_ref).read()
    database = service.registry.databases[route["database_ref"]]
    original, metadata = config_file.snapshot(binding.config_path)
    output = rewrite_route(
        original.decode(),
        binding.alias,
        {
            "host": database.route_host,
            "port": str(database.route_port),
            "dbname": database.database,
        },
    )
    assert_lease()
    config_file.replace(binding.config_path, original, metadata, output.encode())
    assert_lease()
    driver.admin(binding, "RELOAD", tag="runtime-start")
    rows = [row for row in driver.admin(binding, "SHOW DATABASES") if row["name"] == binding.alias]
    require(
        len(rows) == 1
        and rows[0]["paused"]
        and (rows[0]["host"], int(rows[0]["port"]), rows[0]["database"])
        == (database.route_host, database.route_port, database.database),
        "RUNTIME_RESTORE_UNCONFIRMED",
        "저장한 DB 경로의 복원을 확인할 수 없습니다.",
    )
    access = "HELD"
    if route["access"] == "OPEN":
        try:
            matched = writers_match(service, route["database_ref"])
        except psycopg.Error:
            matched = False
        if matched:
            assert_lease()
            driver.admin(binding, "RESUME {}", tag="runtime-start")
            access = "OPEN"
    log.info("runtime=pooler access=%s", access)
    return access


def run_pooler(runtime: RuntimeSettings, service: Service) -> None:
    require_local_admin(runtime, service)
    authentication_files(runtime, service)
    service.store.initialize()
    ready = runtime.directory / "pooler.ready"
    ready.unlink(missing_ok=True)
    child = None
    stopping = False

    def stop(signum, _):
        nonlocal stopping
        stopping = True
        if child is not None and child.poll() is None:
            child.send_signal(signum)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        # 새 Task 준비가 기존 Worker의 외부 변경과 겹치지 않도록 합니다.
        worker = Worker(service)
        with worker.exclusive():
            service.store.recover_interrupted()
            RouteJournal(service.store, service.registry, runtime.binding_ref).register(
                runtime.initial_database_ref
            )
            binding = service.registry.bindings[runtime.binding_ref]
            write_private(binding.config_path, initial_config(runtime, service))
            require(not stopping, "RUNTIME_STOPPING", "Task를 정지하고 있습니다.")
            child = subprocess.Popen(["pgbouncer", str(binding.config_path)])
            deadline = time.monotonic() + 20
            while True:
                require(
                    not stopping and child.poll() is None,
                    "RUNTIME_POOLER_STOPPED",
                    "PgBouncer를 시작할 수 없습니다.",
                )
                try:
                    service.driver.admin(binding, "SHOW VERSION", timeout=2, tag="runtime-start")
                    break
                except (psycopg.Error, TimeoutError):
                    require(
                        time.monotonic() < deadline,
                        "RUNTIME_POOLER_TIMEOUT",
                        "PgBouncer 시작 대기 시간을 넘었습니다.",
                    )
                    time.sleep(0.2)
            restore_route(runtime, service, worker.assert_lease)
        write_private(ready, "ready\n")
        return_code = child.wait()
        require(stopping and return_code == 0, "RUNTIME_POOLER_EXITED", "PgBouncer가 정지했습니다.")
    finally:
        ready.unlink(missing_ok=True)
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


# 역할별 실행 명령


def run_backend() -> None:
    """API와 Worker를 함께 실행하고, 하나가 종료되면 나머지도 정리합니다."""
    children: dict[str, subprocess.Popen] = {}
    stopping = False

    def stop(_signum, _frame):
        nonlocal stopping
        stopping = True

    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        for role in ("api", "worker"):
            if stopping:
                return
            child = subprocess.Popen([sys.executable, "-m", "db_operator.runtime", role])
            children[role] = child
            log.info("runtime=backend child=%s pid=%s", role, child.pid)
        while not stopping:
            for role, child in children.items():
                require(
                    child.poll() is None,
                    "RUNTIME_BACKEND_EXITED",
                    f"{role} 프로세스가 종료됐습니다.",
                )
            time.sleep(0.2)
    finally:
        for child in children.values():
            if child.poll() is None:
                child.terminate()
        # ECS stopTimeout 120초 안에 자식 프로세스를 종료하고 회수합니다.
        deadline = time.monotonic() + 105
        for child in children.values():
            try:
                child.wait(timeout=max(0, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.kill()
        for child in children.values():
            child.wait()
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def run_worker(service: Service) -> None:
    """DynamoDB 장애 시 실행을 중지합니다. 기존 프록시 연결은 계속 유지합니다."""
    worker = Worker(service)
    while not worker.stopping:
        try:
            worker.run()
        except (BotoCoreError, ClientError, psycopg.Error, OperatorError) as error:
            code = error.code if isinstance(error, OperatorError) else "MANAGEMENT_UNAVAILABLE"
            log.warning("runtime=worker code=%s retry_seconds=2", code)
            if not worker.stopping:
                time.sleep(2)


def health(role: str, runtime: RuntimeSettings, service: Service) -> None:
    if role == "pooler":
        require(
            (runtime.directory / "pooler.ready").is_file(),
            "RUNTIME_NOT_READY",
            "프록시 복원이 끝나지 않았습니다.",
        )
        service.driver.admin(service.registry.bindings[runtime.binding_ref], "SHOW VERSION")
        return
    if runtime.client_tls == "require":
        # 같은 Task의 loopback 상태 검사에만 사용합니다. 외부 API 클라이언트는 CA를 검증합니다.
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        connection = http.client.HTTPSConnection("127.0.0.1", 8080, timeout=3, context=context)
    else:
        connection = http.client.HTTPConnection("127.0.0.1", 8080, timeout=3)
    try:
        connection.request("GET", "/live")
        require(
            connection.getresponse().status == 200, "RUNTIME_NOT_READY", "API가 응답하지 않습니다."
        )
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Foxtrot ECS runtime")
    parser.add_argument("command", choices=["pooler", "backend", "api", "worker", "health"])
    parser.add_argument("role", nargs="?", choices=["pooler", "backend", "api"])
    args = parser.parse_args()
    if args.command == "health" and args.role is None:
        parser.error("health 명령에 pooler 또는 backend를 지정하십시오.")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        if args.command == "backend":
            run_backend()
            return
        runtime, settings = RuntimeSettings(), Settings()
        runtime.materialize_registry(settings, writable=args.command == "pooler")
        service = Service(settings)
        if args.command == "pooler":
            run_pooler(runtime, service)
        elif args.command == "worker":
            run_worker(service)
        elif args.command == "health":
            health(args.role, runtime, service)
        else:
            tls = {}
            if runtime.client_tls == "require":
                tls = {
                    "ssl_certfile": str(runtime.directory / "client.crt"),
                    "ssl_keyfile": str(runtime.directory / "client.key"),
                }
            uvicorn.run(create_app(settings), host="0.0.0.0", port=8080, access_log=False, **tls)
    except Exception as error:
        # 설정 값과 DB 오류 본문에 포함될 수 있는 Secret은 로그에 쓰지 않습니다.
        code = error.code if isinstance(error, OperatorError) else type(error).__name__
        log.error("runtime=%s code=%s", args.command, code)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
