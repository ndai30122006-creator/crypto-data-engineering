# crypto-data-engineering

Mini real-time Crypto Data Platform (training Data Engineer):
ingest Binance market events, collect crypto news & market-cap snapshots,
process streaming data with Pathway, orchestrate batch pipelines with
Dagster, store queryable data in PostgreSQL.

## Status

- [x] News pipeline (RSS → Dagster → PostgreSQL) — live, 190+ rows
- [ ] Market-cap snapshot job (hourly)
- [ ] Binance → Kafka ingestion (realtime)
- [ ] Kafka → Pathway → OHLCV 1m (stream processing)
- [ ] News ↔ market correlation queries

## Architecture (news branch)

```
Crypto News API / RSS (CoinDesk, CoinTelegraph, BitcoinMag, Google News)
        │  every 5 min (news_job_schedule)
        ▼
┌───────────────┐
│    Dagster    │
│  raw_news     │  fetch 4 RSS concurrently, validate with Pydantic
│  cleaned_news │  strip HTML, extract symbols, sentiment, dedupe
│  loaded_news   │  INSERT, skip existing URL (ON CONFLICT DO NOTHING)
└───────┬───────┘
        ▼
┌───────────────┐
│  PostgreSQL   │
│  crypto_news  │
└───────────────┘
```

## Tech stack

| Tech | Role |
|---|---|
| Python 3.12 | Pipeline code |
| Dagster | Orchestration (assets + job + schedule) |
| PostgreSQL 16 | Storage (`crypto_db`) |
| httpx + feedparser | RSS fetch & parse |
| Pydantic | Data validation |
| psycopg2 | Postgres driver |
| Docker Compose | 3 services: postgres, dagster-webserver, dagster-daemon |
| pytest | Unit tests |

## Project structure

```
docker-compose.yml        postgres + dagster-webserver + dagster-daemon
Dockerfile.dagster
requirements.txt
workspace.yaml            code location: dagster_project.definitions
config/config.yaml        4 RSS feed URLs
database/schema.sql       CREATE TABLE crypto_news
dagster_project/
  definitions.py          assets + news_job + schedule */5 * * * *
  resources.py            PostgresResource (DATABASE_URL)
  news/
    collector.py          async fetch all feeds
    parser.py             normalize entries, sanitize CoinDesk XML quirk
    cleaner.py            clean text, symbols, sentiment, dedupe
    schemas.py            RawArticle / CleanArticle (Pydantic)
  assets/news_assets.py   raw_news -> cleaned_news -> loaded_news
tests/test_cleaner.py     8 unit tests
```

## Quickstart

Requirements: Docker Desktop with WSL2 backend (Windows).

```powershell
docker compose up --build
```

- Dagster UI: http://localhost:3000
  - Assets tab → **Materialize all** (run once now)
  - Automation tab → enable **news_job_schedule** (auto every 5 min)
- Check data:
```powershell
docker exec crypto-postgres psql -U admin -d crypto_db -c "SELECT source, count(*) FROM crypto_news GROUP BY 1;"
```
- Run tests (local): `pip install -r requirements.txt; pytest tests/ -q`
- Stop: `docker compose down` (data kept in `pgdata` volume)

## Notes

- CoinTelegraph links carry `?utm_*` params → stripped in `normalize_url()` for correct dedupe.
- CoinDesk ships empty `<content:encoded/>` which makes feedparser drop
  `<description>` → stripped in `sanitize_feed_xml()` (`news/parser.py`).
- Postgres here is OLTP/operational storage + light analytics, not a
  dedicated OLAP store.
