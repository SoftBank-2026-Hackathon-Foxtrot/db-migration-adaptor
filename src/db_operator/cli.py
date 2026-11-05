"""DynamoDB 상태 초기화, API 서버, 단일 Worker 실행 명령입니다."""

from __future__ import annotations

import argparse
import logging

import uvicorn

from .api import create_app
from .config import Settings
from .service import Service
from .worker import Worker


def main() -> None:
    parser = argparse.ArgumentParser(description="Foxtrot PostgreSQL migration operator")
    parser.add_argument("command", choices=["init", "api", "worker", "locks", "recover-lock"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--once", action="store_true")
    parser.add_argument(
        "--create-table", action="store_true", help="init에서만 새 DynamoDB 테이블을 생성합니다"
    )
    parser.add_argument("--lock-name", default="executor")
    parser.add_argument("--lock-owner")
    parser.add_argument("--stopped-evidence")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    settings = Settings()
    if args.command == "api":
        uvicorn.run(create_app(settings), host=args.host, port=args.port, access_log=False)
        return
    service = Service(settings)
    if args.command == "init":
        service.store.initialize(create=args.create_table)
    elif args.command == "locks":
        import json

        print(json.dumps(service.store.transact(lambda tx: tx.list("LOCK")), default=str))
    elif args.command == "recover-lock":
        if not args.lock_owner or not args.stopped_evidence:
            parser.error("--lock-owner와 --stopped-evidence가 필요합니다")
        service.store.release_stopped_executor(
            args.lock_name, args.lock_owner, args.stopped_evidence
        )
    elif args.once:
        worker = Worker(service)
        with worker.exclusive():
            service.store.recover_interrupted()
            worker.run_once()
    else:
        Worker(service).run()


if __name__ == "__main__":
    main()
