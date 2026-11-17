# State and volume durability

Các volume sau chứa trạng thái quan trọng; không dùng `docker compose down -v`
khi còn cần dữ liệu:

- `pgdata`: PostgreSQL data
- `kafka_data`: Kafka offsets/log
- `recovery_dlq`: durable DLQ cho trade và candle
- `pathway_state`: Pathway persistence/snapshot
- `ingestion_state`: raw trade gap checkpoints
- `dagster_home`: Dagster run history/metadata

## Backup trước cutover

```powershell
uv run python scripts/backup_db.py --directory .recovery
```

Nếu cần backup volume state, dừng service liên quan rồi copy volume bằng Docker
hoặc backup host mount tương ứng. Luôn ghi lại commit, ngày và kết quả kiểm tra
sau khi restore.

## Khôi phục

1. Giữ nguyên volume names.
2. Khôi phục DB dump nếu cần.
3. Khởi động lại services theo runbook cutover.
4. Chạy `scripts/status.py` và `scripts/alert.py` để xác nhận.
