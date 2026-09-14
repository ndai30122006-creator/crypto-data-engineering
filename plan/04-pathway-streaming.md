# Phase 4 — Kafka → Pathway → OHLCV 1m (stream processing) ✅ DONE, live
> Đã triển khai: `streaming/` (windows pure + engine Pathway 0.32.1 + sink upsert),
> service `pathway`, bảng `market_1m` live. Chi tiết xem `docs/roadmap.md` §4.

## Mục tiêu

Đọc `crypto.trades` từ Kafka, gom theo tumbling window 1 phút / symbol,
tính OHLCV + metrics, ghi vào Postgres.

## Luồng

```
Kafka crypto.trades ──▶ Pathway ──▶ market_1m (OHLCV)
                              ──▶ signals (VOLUME_SPIKE, PRICE_SPIKE)
```

## Schema

```sql
CREATE TABLE IF NOT EXISTS market_1m (
    symbol VARCHAR(20),
    window_start TIMESTAMPTZ,
    open NUMERIC(20,8), high NUMERIC(20,8),
    low NUMERIC(20,8),  close NUMERIC(20,8),
    volume NUMERIC(30,12),
    trade_count INTEGER,
    price_change_1m NUMERIC(10,4),
    PRIMARY KEY (symbol, window_start)
);

CREATE TABLE IF NOT EXISTS signals (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    signal_type VARCHAR(30) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Metrics

- `price_change_1m/5m/15m` = `(close_now - close_N_ago) / close_N_ago * 100`
- Volume spike: `volume_5m / avg_volume_1h > 3` → insert `VOLUME_SPIKE`

## Files cần tạo

- `streaming/pathway_pipeline.py` — `pw.kafka.read(...)` → `windowby` 1 phút
  theo event_time → `reduce(open/ high/low/close/volume/count)` →
  `pw.postgres.write(...)`
- `Dockerfile.pathway`

## Data quality (OHLCV)

`high >= low`, `high >= open/close`, `low <= open/close`, `volume >= 0`.
Vi phạm → `data_quality_errors`.

## Verify

- Sau 2-3 phút: `SELECT * FROM market_1m ORDER BY window_start DESC LIMIT 5;`
- Query: giá BTC hiện tại, volume 5 phút gần nhất, coin tăng mạnh nhất 1h
