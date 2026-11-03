.PHONY: setup check test lab-up lab-down
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
lab-down:
	uv run --locked python scripts/lab.py stop
