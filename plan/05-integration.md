# Phase 5 — Tích hợp: correlation, data quality, milestones ✅ DONE, live
> Query correlation + milestones L1–L4 đều xong (xem `docs/roadmap.md` §5).
> File này giữ thiết kế gốc.

## 1. News ↔ Market correlation

Tìm tin xuất hiện trong ±10 phút quanh biến động giá mạnh:

```sql
SELECT n.title, n.published_at, m.symbol, m.window_start,
       m.price_change_1m AS price_change
FROM crypto_news n
JOIN market_1m m
  ON m.window_start BETWEEN n.published_at - INTERVAL '10 minutes'
                        AND n.published_at + INTERVAL '10 minutes'
WHERE ABS(m.price_change_1m) > 1
ORDER BY m.window_start DESC;
```

Lưu vào `database/queries.sql`.

## 2. Data quality tổng

| Nguồn | Checks |
|---|---|
| Binance | price > 0, quantity > 0, symbol/event_time NOT NULL |
| News | url/title/published_at NOT NULL, url UNIQUE |
| OHLCV | high ≥ low/open/close, low ≤ open/close, volume ≥ 0 |

Bad records → `data_quality_errors` (đã tạo schema ở Phase 2).

## 3. Milestones (4 levels)

- **Level 1 — Batch**: RSS + market-cap → Postgres qua Dagster ✅/Phase 2
- **Level 2 — Streaming**: Binance → Kafka → Postgres (chưa Pathway)
- **Level 3 — Stream Processing**: + Pathway → OHLCV realtime
- **Level 4 — Integration**: News + Market chung 1 DB, query correlation

## 4. Câu hỏi sinh viên phải trả lời (15 câu)

Kafka (5): sao không ghi thẳng Binance→Postgres? partition để làm gì?
consumer chết 10 phút thì sao? tránh duplicate? at-least-once vs exactly-once?

Dagster (4): sao realtime không chạy bằng DAG? Dagster giải quyết gì?
retry có gây duplicate? backfill là gì?

Pathway (3): Kafka vs Pathway khác nhau? window 1 phút tính sao?
late-arriving event xử lý thế nào?

Postgres (3): index cho `WHERE symbol=.. AND event_time BETWEEN..`?
sao tách raw vs aggregated? 100M records còn hợp không?

## 5. Docker Compose cuối (mục tiêu `docker compose up` 1 lệnh)

kafka, postgres, dagster (webserver+daemon), binance-consumer, pathway (+ pgadmin optional).
