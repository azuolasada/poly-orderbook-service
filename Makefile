.PHONY: setup check check-full

setup:
	uv sync
	uv run prek install

check:
	uv run ruff check .
	uv run mypy src main.py

check-full: check
	uv run pytest
