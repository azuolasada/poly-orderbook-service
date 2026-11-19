.DEFAULT_GOAL := help
.PHONY: help setup lint fmt test check start start-dev stop logs clean

help:  ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

setup:  ## Install deps, git hooks, and create .env
	uv sync --locked
	uv run prek install
	@test -f .env || (cp .env.example .env && echo "Created .env from .env.example")

lint:  ## Ruff + mypy
	uv run ruff check .
	uv run mypy src main.py

fmt:  ## Auto-fix lint issues
	uv run ruff check --fix .

test:  ## Run tests
	uv run pytest tests/ -q

check: lint test  ## Everything CI runs

start:  ## Run against real S3
	docker compose up --build

start-dev:  ## Run against local MinIO
	docker compose --profile dev up --build

stop:  ## Stop containers
	docker compose --profile dev down

logs:  ## Follow container logs
	docker compose logs -f

clean:  ## Remove tool caches
	rm -rf .mypy_cache .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} +
