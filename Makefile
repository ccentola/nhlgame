.PHONY: up down logs dbt-debug dbt-run dbt-test dbt-docs

ENV_FILE := .env
ENV_VARS := $(shell grep -v '^\#' $(ENV_FILE) | grep -v '^\s*$$' | xargs)
DBT = cd nhlgame_transform && env $(ENV_VARS) uv run dbt

# --- Docker Compose ---

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# --- dbt ---

dbt-debug:
	$(DBT) debug --profiles-dir .

dbt-run:
	$(DBT) run --profiles-dir .

dbt-test:
	$(DBT) test --profiles-dir .

dbt-docs:
	$(DBT) docs generate --profiles-dir . && $(DBT) docs serve --profiles-dir .

dbt-query:
	uv run python scripts/query.py
