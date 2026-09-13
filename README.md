# crypto-data-engineering

Mini real-time Crypto Data Platform (training Data Engineer):
ingest Binance market events, collect crypto news & market-cap snapshots,
process streaming data with Pathway, orchestrate batch pipelines with
Dagster, store queryable data in PostgreSQL.

## Status

- [x] News pipeline (RSS → Dagster → PostgreSQL) — live, schedule mỗi 5 phút
- [x] Market-cap snapshot job (hourly, CoinGecko top 50)
- [x] Resource practice P1–P4 (RSS/CoinGecko/Postgres resources, env-aware tables, mock tests)
- [ ] Binance → Kafka ingestion (realtime)
- [ ] Kafka → Pathway → OHLCV 1m (stream processing)
- [ ] News ↔ market correlation queries

## Architecture

```
Crypto News API / RSS (CoinDesk, CoinTelegraph, BitcoinMag, Google News)
        │  every 5 min (news_job_schedule)
        ▼
┌───────────────┐
│    Dagster    │
│  raw_news     │  RSSFeedResource fetch 4 RSS, validate with Pydantic
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
```

Tables are env-suffixed outside prod (e.g. `crypto_news_local`
when `DAGSTER_ENVIRONMENT=local`).

## Tech stack

| Tech | Role |
|---|---|
| Python 3.12 | Pipeline code |
| Dagster | Orchestration (assets + job + schedule) |
| PostgreSQL 16 | Storage (`crypto_db`) |
| httpx + feedparser | RSS fetch & parse |
| Pydantic | Data validation |
| psycopg2 | Postgres driver |
| Docker Compose | 4 services: postgres, dagster-code (gRPC 4000), webserver, daemon |
| uv | Package + project manager (`pyproject.toml` + `uv.lock`) |
| pytest | 20 unit tests (pure logic + resource mocks, offline) |

## Project structure

```
docker-compose.yml        postgres + dagster-code (gRPC 4000) + webserver + daemon + kafka + binance-consumer
Dockerfile.dagster + Dockerfile.consumer
pyproject.toml + uv.lock (.python-version: 3.12)
workspace.yaml            grpc_server dagster-code:4000 (location: crypto-data-platform)
config/config.yaml        4 RSS feed URLs
database/schema.sql       crypto_news, crypto_market_snapshot, data_quality_errors
database/queries.sql      8 analytical queries
dagster_project/
  definitions.py          2 jobs + 2 schedules + 3 resources
  resources/              RSSFeedResource, CoinGeckoResource, PostgresResource (env-aware)
  news/                   parser, cleaner, schemas (pure logic, no IO)
  market/                 schemas, validator (pure logic, no IO)
  assets/                 news_assets (3) + market_assets (3)
tests/                    test_cleaner, test_market, test_resources (20 tests)
plan/                     roadmap + phase 01–06 plans
docs/                     learning guides (news, market, correlation, resources, overview)
```

## Quickstart

Requirements: Docker Desktop with WSL2 backend (Windows).

```powershell
docker compose up --build
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
- Stop: `docker compose down` (data kept in `pgdata` volume)

## Notes

- CoinTelegraph links carry `?utm_*` params → stripped in `normalize_url()` for correct dedupe.
- CoinDesk ships empty `<content:encoded/>` which makes feedparser drop
  `<description>` → stripped in `sanitize_feed_xml()` (`news/parser.py`).
- Postgres here is OLTP/operational storage + light analytics, not a
  dedicated OLAP store.
