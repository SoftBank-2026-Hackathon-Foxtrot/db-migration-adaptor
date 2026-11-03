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
    parser.add_argument("command", choices=["init", "api", "worker"])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    settings = Settings()
    if args.command == "api":
        uvicorn.run(create_app(settings), host=args.host, port=args.port, access_log=False)
        return
    service = Service(settings)
    if args.command == "init":
        service.store.initialize()
    elif args.once:
        worker = Worker(service)
        with worker.exclusive():
            service.store.recover_interrupted()
            worker.run_once()
    else:
        Worker(service).run()


if __name__ == "__main__":
    main()
