# Migrations

Thay đổi schema SAU baseline (`database/schema.sql` lúc init volume mới)
đặt ở đây, tên `NNN_mo_ta.sql` (vd `002_index_news_symbols.sql`).

Chạy: `uv run python scripts/migrate.py` (đọc `DATABASE_URL`,
default localhost). Idempotent: file đã chạy ghi vào bảng
`schema_migrations`, chạy lại bỏ qua.

001 thêm phiên bản updated_at cho candle upsert/replay.
002 thêm view market_analytics_1m (price change 1m/5m/15m, contiguous closed candles).
Baseline mới đã gồm cả hai thay đổi; runner vẫn ghi versions khi chạy lần đầu.
