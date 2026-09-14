# Migrations

Thay đổi schema SAU baseline (`database/schema.sql` lúc init volume mới)
đặt ở đây, tên `NNN_mo_ta.sql` (vd `002_index_news_symbols.sql`).

Chạy: `uv run python scripts/migrate.py` (đọc `DATABASE_URL`,
default localhost). Idempotent: file đã chạy ghi vào bảng
`schema_migrations`, chạy lại bỏ qua.

Hiện chưa có migration nào đang chờ — thư mục giữ quy ước + runner.
