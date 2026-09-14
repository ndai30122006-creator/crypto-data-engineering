# crypto-data-engineering

Mini real-time Crypto Data Platform (training Data Engineer):
ingest Binance market events, collect crypto news & market-cap snapshots,
process streaming data with Pathway, orchestrate batch pipelines with
Dagster, store queryable data in PostgreSQL.

## Status

- [x] News pipeline (RSS → Dagster → PostgreSQL) — live, schedule mỗi 5 phút
- [x] Market-cap snapshot job (hourly, CoinGecko top 50)
- [x] Resource practice P1–P4 (RSS/CoinGecko/Postgres resources, env-aware tables, mock tests)
- [x] Binance → Kafka ingestion (realtime, 5 cặp)
- [x] Kafka → Pathway → OHLCV 1m (stream processing → `market_1m`)
- [x] News ↔ market correlation queries (query 8, LAG-based)
- [x] Data quality: 6 asset checks + pure checks module
- [x] Reliability: retries, delivery handling, graceful shutdown, integration tests

## Architecture

```
Crypto News API / RSS (CoinDesk, CoinTelegraph, BitcoinMag, Google News)
        │  every 5 min (news_job_schedule)
        ▼
┌───────────────┐
│    Dagster    │  6 assets + 6 asset checks (freshness/dup/null/count/schema)
│  raw_news     │  RSSFeedResource fetch 4 RSS, validate with msgspec
│  cleaned_news │  strip HTML, extract symbols, sentiment, dedupe
│  loaded_news   │  PostgresResource.insert_news, skip existing URL
└───────┬───────┘
        ▼
┌───────────────┐
│  PostgreSQL   │
│  crypto_news  │
└───────────────┘

CoinGecko API ── hourly (market_job_schedule) ──▶ fetch_market
        ──▶ validate_market ──▶ loaded_snapshot ──▶ crypto_market_snapshot
                                                  └─▶ data_quality_errors (bad records)

Binance WS ──▶ binance-consumer ──▶ Kafka (crypto.trades)
        ──▶ Pathway (tumbling 1m OHLCV) ──▶ market_1m ──┐
                                                        ├─▶ correlation query (news ±10m, |Δ|>1%)
Crypto News ────────────────────────────────────────────┘
```

Tables are env-suffixed outside prod (e.g. `crypto_news_local`
when `DAGSTER_ENVIRONMENT=local`). `market_1m` is global (streaming).

## Tech stack

| Tech (locked in `uv.lock`) | Role |
|---|---|
| Python 3.12 | Pipeline code (`.python-version`) |
| Dagster 1.13.22 | Orchestration (6 assets + 6 checks, jobs + schedules) |
| PostgreSQL 16 | Storage (`crypto_db`) |
| Kafka 3.9.0 (KRaft, single broker) | Realtime trades buffer (`crypto.trades`) |
| Pathway 0.32.1 | Stream processing (tumbling 1m OHLCV, Linux-only) |
| kafka-python 3.0.11 + websocket-client 1.9.2 | Ingestion (producer + Binance WS) |
| httpx 0.28.1 + feedparser 6.0.14 | RSS fetch & parse (retry/backoff) |
| msgspec 0.21.1 / orjson 3.12.0 / ciso8601 2.3.3 | Validate + JSON + parse ngày tốc độ cao |
| Pydantic (transitive qua Dagster) | Code mình không import trực tiếp nữa |
| python-dateutil 2.9.0 + pyyaml 6.0.3 | Fallback parse RFC-2822 + đọc config YAML |
| psycopg2-binary 2.9.13 | Postgres driver |
| Docker Compose | 7 services: postgres, kafka, dagster-code (gRPC 4000), webserver, daemon, binance-consumer, pathway |
| uv | Package + project manager (`pyproject.toml` + `uv.lock`) |
| pytest 9.1.1 | 56 unit tests (offline) + integration tests (`INTEGRATION=1`) |

## Project structure

```
docker-compose.yml        7 services (postgres, kafka, dagster ×3, consumer, pathway)
Dockerfile.dagster + Dockerfile.consumer + Dockerfile.pathway
pyproject.toml + uv.lock (.python-version: 3.12)
workspace.yaml            grpc_server dagster-code:4000 (location: crypto-data-platform)
config/config.yaml        4 RSS feed URLs
database/schema.sql       crypto_news, crypto_market_snapshot, data_quality_errors, market_1m
database/migrations/      versioned schema changes (scripts/migrate.py)
database/queries.sql      8 analytical queries (incl. correlation)
dagster_project/
  definitions.py          2 jobs + 2 schedules + 6 checks + 3 resources
  resources/              RSSFeedResource, CoinGeckoResource, PostgresResource (env-aware, retry)
  news/ + market/         pure logic (parser, cleaner, schemas, validator)
  quality/                pure checks + 6 asset checks
  assets/                 news_assets (3) + market_assets (3)
ingestion/                Binance WS → Kafka (reconnect, heartbeat, graceful shutdown)
streaming/                Pathway OHLCV engine + upsert sink + correlation
scripts/                  status.py (health tổng), migrate.py (migrations)
tests/                    unit (offline) + integration (INTEGRATION=1)
plan/                     roadmap + phase 01–06 plans
docs/                     roadmap + learning guides
```

## Quickstart

Requirements: Docker Desktop with WSL2 backend (Windows).

```powershell
docker compose up --build -d
```

- Dagster UI: http://localhost:3000
  - Assets tab → **Materialize all** (run once now)
  - Automation tab → enable **news_job_schedule** (every 5 min)
    and **market_job_schedule** (hourly)
- Check data:
```powershell
docker exec crypto-postgres psql -U admin -d crypto_db -c "SELECT source, count(*) FROM crypto_news_local GROUP BY 1;"
```
- Run tests (local): `uv sync; uv run pytest tests/ -q`
  - Local Dagster CLI needs env vars first:
    `$env:DATABASE_URL="..."; $env:DAGSTER_ENVIRONMENT="local"`
- Integration tests (cần stack Docker đang lên):
  `$env:INTEGRATION="1"; uv run pytest tests/ -q`
- Health tổng: `uv run python scripts/status.py` (containers, API, Kafka, DB, freshness)
- Migrations: `uv run python scripts/migrate.py`
- Stop: `docker compose down` (data kept in `pgdata` volume)

## Secrets

- Không hard-code credentials: compose đọc `${POSTGRES_DB/USER/PASSWORD:-default}`,
  code đọc `DATABASE_URL`/`DAGSTER_ENVIRONMENT` qua `EnvVar` (Dagster UI chỉ hiện tên biến).
- Override local: `copy .env.example .env` rồi sửa — `.env` đã ignore khỏi git.
- Quy ước: secret chỉ đi qua env, không bao giờ vào code/image/docs.

## Notes

- CoinTelegraph links carry `?utm_*` params → stripped in `normalize_url()` for correct dedupe.
- CoinDesk ships empty `<content:encoded/>` which makes feedparser drop
  `<description>` → stripped in `sanitize_feed_xml()` (`news/parser.py`).
- Postgres here is OLTP/operational storage + light analytics, not a
  dedicated OLAP store.
- Dates: `ciso8601` fast path for ISO-8601, `dateutil` fallback for RFC-2822
  (RSS pubDates) — see `news/parser.py`.
- Schemas use msgspec Structs: validation happens on `convert`/decode,
  not on direct construction — assets always go through `convert`.
