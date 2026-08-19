# poly-orderbook-service

Streams live orderbook/market data from [Polymarket](https://polymarket.com) for a given event series, batches the messages, and archives them as compressed files in S3-compatible storage.

## How it works

1. `MarketDataService` looks up a Polymarket **series** (e.g. an ATP tennis series) via the Gamma REST API, resolves its events into markets, and extracts the CLOB token IDs for `moneyline` markets (configurable).
2. It connects to Polymarket's CLOB WebSocket feed and subscribes to those tokens.
3. A background watcher polls the series every 5 minutes for new/removed events and updates the WebSocket subscription accordingly.
4. Incoming messages are batched in memory by `MessageBuffer` and flushed to S3 as gzip-free, zstd-compressed `.jsonl.zst` files — either when a message-count threshold or a time interval is reached.
5. `S3Storage` handles the upload (and creates the bucket if it doesn't exist) via `boto3`, against any S3-compatible endpoint (AWS S3, MinIO, etc.).

```
Polymarket Gamma API ──▶ MarketDataService ──▶ WebSocketClient ──▶ MessageBuffer ──▶ S3Storage ──▶ S3 / MinIO
                              │
                              └── watch_series() polls for event changes every 5 min
```

## Requirements

- Docker + Docker Compose — the app is intended to run in Docker only (see [Running](#running))
- Python >= 3.14 and [uv](https://docs.astral.sh/uv/) — only needed for local development (running tests, linting, etc.), not for running the app itself

## Setup

```bash
cp .env.example .env   # fill in real values, see below
```

For local development (tests, etc.) also run `uv sync` — see [Testing](#testing).

## Configuration

All configuration is via environment variables (see `.env.example`):

| Variable | Description |
|---|---|
| `APP_ENV` | `dev` enables `DEBUG` logging; anything else defaults to `INFO`. |
| `POLYMARKET_SERIES_ID` | Polymarket series ID to subscribe to. Required, no default. |
| `S3_ENDPOINT_URL` | Endpoint of the S3-compatible service. Use `http://minio:9000` if the app runs inside docker-compose, or `http://localhost:9000` if running `main.py` directly on the host against the mapped MinIO port. |
| `S3_BUCKET` | Bucket to write flushed message batches to. Created automatically if missing. |
| `S3_ACCESS_KEY_ID` | Access key for the S3-compatible service. |
| `S3_SECRET_ACCESS_KEY` | Secret key for the S3-compatible service. |

## Running

The app is intended to be run **only via Docker** — there is no supported way to run `main.py` directly on the host.

### With local MinIO (dev)

```bash
docker compose --profile dev up --build
```

This starts the service alongside a MinIO instance (S3-compatible storage) with a web console at `http://localhost:9001` (default credentials: `admin` / `admin123`, see `docker-compose.yaml`). Use `S3_ENDPOINT_URL=http://minio:9000` in `.env` for this mode.

### Against external S3-compatible storage

```bash
docker compose up --build
```

Point `S3_ENDPOINT_URL` (and credentials) at your real S3-compatible endpoint in `.env`.

## Testing

Tests run locally (outside Docker), against a `uv`-managed environment:

```bash
uv sync
uv run pytest
```

## Project layout

```
main.py                              # entrypoint: wires up the service, buffer, and event loop
src/
  config.py                          # env-driven app config (APP_ENV)
  s3_storage.py                      # S3-compatible upload/bucket management
  utils/logger.py                    # shared logger
  market_data/
    api_client.py                    # Polymarket Gamma REST API client
    web_socket_client.py             # Polymarket CLOB WebSocket client (auto-reconnect)
    message_buffer.py                # in-memory batching + compression + S3 flush
    market_data_service.py           # orchestration: series -> tokens -> subscription -> watch
tests/                                # pytest test suite (respx/mock-based, no live network calls)
```
