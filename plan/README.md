# Plan tổng — Crypto Data Platform

Training Data Engineer: batch + streaming + orchestration + storage.

## Roadmap

| Phase | Nội dung | Trạng thái | File plan |
|---|---|---|---|
| 1 | News pipeline (RSS → Dagster → Postgres) | ✅ DONE, live | `01-news-pipeline.md` |
| 2 | Market-cap snapshot job (mỗi 1 giờ) | ✅ DONE, live | `02-market-cap-snapshot.md` |
| 3 | Binance WebSocket → Kafka (realtime) | ✅ DONE, live | `03-binance-kafka.md` |
| 4 | Kafka → Pathway → OHLCV 1m (stream processing) | ⬜ chưa làm | `04-pathway-streaming.md` |
| 5 | Tích hợp: correlation query, data quality, milestones | ⬜ chưa làm (query 8 đã có khung, chờ bảng `market_1m`) | `05-integration.md` |
| 6 | Resource practice (blog Dagster Resources, P1–P4) | ✅ DONE (P5 check tay UI) | `06-resource-practice.md` |

Trạng thái thật của repo: xem `docs/roadmap.md` (tổng hợp, cập nhật theo code).

## Nguyên tắc phân biệt tool

- **Dagster**: orchestration (việc định kỳ) — KHÔNG xử lý realtime
- **Kafka**: message broker (đệm sự kiện) — KHÔNG tính toán
- **Pathway**: stream processing (tính toán realtime) — KHÔNG lưu trữ lâu
- **PostgreSQL**: persistent storage (OLTP + phân tích nhẹ) — KHÔNG phải OLAP
