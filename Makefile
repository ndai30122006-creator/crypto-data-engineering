.PHONY: sync lint test doctor migrate status alert

sync:
	uv sync --frozen

lint:
	uv run ruff check dagster_project ingestion streaming scripts tests

test:
	uv run pytest tests/ -q

doctor:
	uv run python scripts/doctor.py

migrate:
	uv run python scripts/migrate.py

status:
	uv run python scripts/status.py

alert:
	uv run python scripts/alert.py
