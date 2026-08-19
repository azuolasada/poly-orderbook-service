# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

An asyncio service that streams live orderbook/market data from Polymarket for a given event series, batches the messages in memory, and archives them as zstd-compressed `.jsonl.zst` files to S3-compatible storage (AWS S3 or MinIO).

## Commands

Dependency management is via `uv`. `pytest`/`ruff`/`mypy` live in the `dev` dependency group (excluded from the production Docker image via `--no-dev`).

```bash
uv sync                          # install all deps (prod + dev) into .venv

uv run pytest tests/ -q          # run the full test suite
uv run pytest tests/test_api_client.py -q          # single test file
uv run pytest tests/test_api_client.py::test_api_client_retries_on_429 -q   # single test

uv run ruff check .              # lint
uv run ruff check --fix .        # lint, auto-fixing what's fixable

uv run mypy src main.py          # type check
```

All three (ruff, mypy, pytest) run in GitHub Actions on every push/PR to `master` (`.github/workflows/ci.yml`) — run them locally before pushing.

Tests use `respx` to mock HTTP calls and mock the WebSocket connection object directly (see `tests/test_web_socket_client.py`); no live network calls or real S3/MinIO are needed to run the suite. `pytest-asyncio` is in `auto` mode (`pyproject.toml`), so async test functions don't need an explicit `@pytest.mark.asyncio` decorator to run correctly, though the existing tests include it anyway for clarity.

**Running the app itself is Docker-only** — there is no supported way to run `main.py` directly on the host (see README's "Running" section for `docker compose --profile dev up --build` against local MinIO, or plain `docker compose up --build` against real S3).

## Architecture

Data flow:
```
Polymarket Gamma API ──▶ MarketDataService ──▶ WebSocketClient ──▶ MessageBuffer ──▶ S3Storage ──▶ S3 / MinIO
                              │
                              └── watch_series() polls for event changes every 5 min
```

- **`MarketDataService`** (`src/market_data/market_data_service.py`) — orchestration. Resolves a Polymarket *series* → its *events* → the `moneyline`-market CLOB token IDs (via `ApiClient`), then hands a connected, subscribed `WebSocketClient` back to the caller. `watch_series()` runs as a background task polling for event-list changes and re-subscribing the same `WebSocketClient` when tokens change.
- **`WebSocketClient`** (`src/market_data/web_socket_client.py`) — owns the Polymarket CLOB WebSocket connection with auto-reconnect (`tenacity`-backed). `connect`/`subscribe`/`disconnect`/the reconnect path inside `listen()` all serialize through a single `asyncio.Lock` (`self._connection_lock`), because `listen()`'s reconnect logic and `watch_series()`'s resubscribe calls run concurrently against the same connection state — this lock is load-bearing, don't remove it. Internally, locked implementations are split from public wrappers (`_connect_locked`/`_subscribe_locked` vs `connect`/`subscribe`) since `asyncio.Lock` is not reentrant.
- **`MessageBuffer`** (`src/market_data/message_buffer.py`) — in-memory batching guarded by its own `asyncio.Lock`. Flushes (JSON-serialize + zstd-compress + upload) when either `flush_count_threshold` or `flush_interval_seconds` is hit, or via the periodic background task `start_periodic_flush()`. On a failed flush, the buffer is *not* cleared, so messages are retried on the next flush rather than dropped.
- **`S3Storage`** (`src/s3_storage.py`) — thin `boto3` wrapper; `upload_bytes` runs the blocking `boto3` call via `asyncio.to_thread` and retries transient errors (`ClientError`/`BotoCoreError`) with exponential backoff. Works against any S3-compatible endpoint via `S3_ENDPOINT_URL`.
- **`ApiClient`** (`src/market_data/api_client.py`) — Polymarket Gamma REST client. Retries are predicate-based (`_is_retryable`): connection errors, 5xx, and 429 are retried; other 4xx responses are not (they're treated as non-transient and `reraise=True` immediately).

**Shutdown**: `main.py` installs `SIGTERM`/`SIGINT` handlers (`loop.add_signal_handler`) that cancel the main task, so `docker stop`/Ctrl+C trigger the same graceful-shutdown path (`finally` block: cancel watcher → `asyncio.gather(..., return_exceptions=True)` to actually await the cancellation → final `buffer.flush()` → cancel the periodic flush task → disconnect). Background tasks (`watcher_task`, `flush_task`) are always explicitly awaited after `.cancel()`, never just cancelled and left — `.cancel()` alone only *schedules* cancellation, it doesn't wait for the task to actually stop.

**Config**: environment-variable driven, no config framework — `src/config.py` currently only reads `APP_ENV` (`dev` → `DEBUG` logging, else `INFO`). `S3_*` vars are read directly in `S3Storage.__init__` via `os.getenv`. See `.env.example` for the full list. The series to subscribe to is hardcoded as `series_id` in `main.py` (not yet an env var).

**Logging**: single shared logger from `src/utils/logger.py` (`from src.utils.logger import logger`), configured once at import time based on `APP_ENV`.

## Docstrings

All modules, classes, and functions/methods under `src/` must have Google-style docstrings — `src/s3_storage.py` is the reference example. Keep them short and concise (no restating the signature). `main.py` is excluded.

- Module: one-line summary at the top of the file.
- Class: short summary; `Attributes:` block for public attributes.
- Function/method: short summary; `Args:`, `Returns:`, `Raises:` sections only when applicable (skip `Returns:` for `None`, skip `Args:`/`Raises:` when there are none). Trivial one-line getters/dunder methods can stay a single summary line.
- Private helpers (`_leading_underscore`) still get a short docstring, especially noting non-obvious preconditions (e.g. "assumes the lock is held").
