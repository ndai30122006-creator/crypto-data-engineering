# Plan tổng — Crypto Data Platform

Kế hoạch hiện tại: [08-fix-upgrade.md](08-fix-upgrade.md), lập 02/10/2026
trước khi sửa code. Bao gồm 11 findings, thiết kế v2, acceptance tests,
rollout và nhật ký kiểm chứng. [Runbook](../docs/upgrade-runbook.md) hướng dẫn
migration/cutover/replay chi tiết.

## Các giai đoạn

| Phase | Nội dung | Trạng thái hiện tại | Plan |
|---|---|---|---|
| 1 | News pipeline | Có code/tests; các số live trong plan gốc là lịch sử | [01](01-news-pipeline.md) |
| 2 | Market snapshots | Có code/tests; schema rejects được sửa tại phase 8 | [02](02-market-cap-snapshot.md) |
| 3 | Binance → Kafka | v2 có trade ID, ack accounting, durable DLQ; chờ live v2 | [03](03-binance-kafka.md) |
| 4 | Pathway OHLCV | v2 dedupe, event-time + ID ordering; chờ Linux/live v2 | [04](04-pathway-streaming.md) |
| 5 | Correlation và quality | Có code; query live cũ không phải nghiệm thu v2 | [05](05-integration.md) |
| 6 | Resource practice | P1–P4 có code/tests; P5 kiểm tra UI thủ công | [06](06-resource-practice.md) |
| 7 | Correctness/observability | Review phát hiện các lỗ hổng, sửa trong phase 8 | [07](07-session-phases.md) |
| 8 | Fix và upgrade | Code + offline đã kiểm chứng; Linux CI/live còn chờ | [08](08-fix-upgrade.md) |

Các plans 01–07 giữ mục tiêu/ghi chép lịch sử. Nhãn DONE hoặc live trong
chúng không thay cho acceptance hiện tại; trạng thái bản mới ở phase 08.
PRICE_SPIKE, price change 5m/15m và REST backfill nằm ngoài scope hiện tại.

Dagster orchestration batch; Kafka đệm sự kiện; Pathway xử lý stream;
PostgreSQL lưu dữ liệu vận hành và phân tích nhẹ.
