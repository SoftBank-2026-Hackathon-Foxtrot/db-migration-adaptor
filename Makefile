.PHONY: setup check test lab-up lab-down image-build
MANAGED_LAB_DIRECTORY ?= $(CURDIR)/runtime/managed-lab
MANAGED_LAB_PROJECT ?= foxtrot-db-operator-managed-lab
setup:
	uv sync --locked --dev
check:
	uv run --locked ruff check .
	uv run --locked ruff format --check .
	uv run --locked pytest
test:
	uv run --locked pytest
lab-up:
	uv run --locked python scripts/lab.py up
	DB_OPERATOR_LAB_PROFILE=managed DB_OPERATOR_LAB_DIRECTORY="$(MANAGED_LAB_DIRECTORY)" COMPOSE_PROJECT_NAME="$(MANAGED_LAB_PROJECT)" uv run --locked python scripts/lab.py up
lab-down:
	uv run --locked python scripts/lab.py stop
	DB_OPERATOR_LAB_PROFILE=managed DB_OPERATOR_LAB_DIRECTORY="$(MANAGED_LAB_DIRECTORY)" COMPOSE_PROJECT_NAME="$(MANAGED_LAB_PROJECT)" uv run --locked python scripts/lab.py stop
image-build:
	docker build -t foxtrot/db-operator:ecs-local .
