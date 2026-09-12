# Phase 2 — Market-cap snapshot job (mỗi 1 giờ)

## Mục tiêu

Định kỳ lấy giá + vốn hoá thị trường top coins, lưu snapshot để query
top gainers/losers, volume leaders.

## Luồng

```
CoinGecko API ── 0 * * * * ──▶ fetch_market ──▶ validate ──▶ loaded_snapshot ──▶ crypto_market_snapshot
```

## Schema

```sql
CREATE TABLE IF NOT EXISTS crypto_market_snapshot (
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    price NUMERIC(20,8),
    market_cap NUMERIC(30,2),
    circulating_supply NUMERIC(30,2),
    volume_24h NUMERIC(30,2),
    price_change_24h NUMERIC(10,4),
    PRIMARY KEY (collected_at, symbol)
);
```

## Files cần tạo

- `dagster_project/market/fetcher.py` — gọi CoinGecko
  `/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50`
  (free, không cần key; tôn trọng rate limit ~10-15 call/phút)
- `dagster_project/market/validator.py` — checks: price > 0,
  market_cap > 0, symbol NOT NULL; bad records → `data_quality_errors`
- `dagster_project/assets/market_assets.py` — 3 assets
- Thêm vào `definitions.py`: `market_job` + schedule `0 * * * *`

## Data quality

```sql
CREATE TABLE IF NOT EXISTS data_quality_errors (
    id BIGSERIAL PRIMARY KEY,
    pipeline VARCHAR(50),
    payload JSONB,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Verify

- Materialize 1 lần → `SELECT count(*) FROM crypto_market_snapshot;` > 0
- Query: top 10 by market_cap, top gainers 24h
