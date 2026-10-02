# Runbook nâng cấp và phục hồi dữ liệu

Áp dụng kế hoạch [08-fix-upgrade](../plan/08-fix-upgrade.md), ngày 02/10/2026.
Các commands live bên dưới là checklist nghiệm thu; chưa được chạy trên stack
thật trong lần sửa này vì máy hiện chưa có Docker CLI dùng được.

## 1. Kiểm tra code trước rollout

```powershell
uv sync --frozen
uv run ruff check dagster_project ingestion streaming scripts tests
uv run pytest tests/ -q
$env:DAGSTER_ENVIRONMENT="local"
# Đặt DATABASE_URL cho đúng DB cần kiểm tra; validate không materialize assets.
uv run dagster definitions validate -m dagster_project.definitions
docker compose config --quiet
```

Windows skip graph Pathway vì không có wheel. Linux CI chạy
`tests/test_pathway_graph.py` với engine thực và streaming E2E trong job riêng.
Chỉ đánh dấu rollout đã nghiệm thu khi cả hai job thành công.

## 2. Cutover contract v1 → v2

1. Ghi lại thời điểm UTC cutover; backup PostgreSQL và giữ các named volumes
   hiện có. Topic `crypto.trades` cũ giữ để đối chiếu, không xóa/reset offsets.
2. Trong `.env`, dùng `KAFKA_TOPIC=crypto.trades.v2` và
   `KAFKA_GROUP_ID=pathway-ohlcv-1m-v2`. Topic v2 chỉ nhận contract đủ
   `symbol,trade_id,price,quantity,timestamp`. Không publish v1 sang v2.
3. Stop consumer/engine cũ trước khi chạy image mới để tránh hai writers
   cùng cập nhật `market_1m`:

   ```powershell
   docker compose stop binance-consumer pathway
   docker compose up -d --wait postgres kafka
   uv run python scripts/migrate.py
   docker compose build
   docker compose up -d --wait
   ```

   Runner migration dùng `DATABASE_URL` của shell, không tự đọc `.env`.
   Migration `001_market_1m_updated_at.sql` thêm cột phiên bản bằng
   `ADD COLUMN IF NOT EXISTS`; `ensure_tables()` của sink cũng tương thích
   DB cũ. Baseline `schema.sql` dành cho volume mới, không chạy lại toàn bộ
   baseline để thay migration. Nếu migration/build lỗi, dừng ở bước đó và
   giữ các services cũ đang stop cho đến khi nguyên nhân được xử lý.

4. Xác nhận consumer và engine cùng topic/group v2, volume `recovery_dlq`
   và `pathway_state` đã mount. `PATHWAY_PERSISTENCE_DIR` được bật trong Compose;
   filesystem backend giữ state qua restart. Không tái sử dụng snapshot
   của một graph/source khác. Không xóa state rồi resume từ offsets cuối:
   engine sẽ thiếu trades để tính lại các windows đang mở.
5. Nến của phút cutover có thể thiếu đầu phút. Chỉ nghiệm thu từ phút đầy
   đủ đầu tiên sau khi consumer mới nhận event. Lịch sử cũ không được sửa
   bằng migration; rebuild cần đầy đủ raw trades và phạm vi riêng.
6. Trong Dagster UI bật cả hai schedules; kiểm tra schema rejects xuất hiện
   trong bảng errors đúng environment. Detector cần ít nhất 65 nến liên
   tiếp cho một window và đọc 125 phút để xét cả giờ gần nhất.

## 3. Nghiệm thu live

```powershell
$env:INTEGRATION="1"
$env:KAFKA_TOPIC="crypto.trades.v2"
$env:KAFKA_GROUP_ID="pathway-ohlcv-1m-v2"
uv run pytest tests/integration/ tests/test_integration.py -q
uv run python scripts/status.py
uv run python scripts/status.py --json
uv run python scripts/metrics.py
uv run python scripts/alert.py
docker compose logs --tail 100 binance-consumer pathway
```

Các tests dùng symbol riêng cho mỗi run và cleanup dữ liệu test. Nghiệm thu
exact open/high/low/close/volume/count, duplicate replay, out-of-order/late,
multi-symbol, invalid contract, Postgres stale version. Không đổi assertion
volume/count sang `>=` khi có lỗi.

Status/alert phải phát hiện unhealthy, nguồn metrics unavailable, nến/news
cũ, lag, publish failures, DLQ failures và accounting lệch. Counters process
reset sau restart; lưu logs/metrics trước khi restart để đối chiếu incident.

Thử lỗi trên stack test: stop Kafka → trade delivery thất bại có bản ghi
`trades.jsonl`; stop Postgres → bounded retries + `candles.jsonl`; cấp quyền
không ghi được DLQ → log rõ “DLQ write failed”. Restore services, chạy replay,
đối chiếu dữ liệu và restart engine nếu đã fail. Không suy ra outage không
mất dữ liệu chỉ từ việc service trở lại healthy.

## 4. Replay durable DLQ

Hai file nằm trong volume `recovery_dlq`, tại `/var/lib/crypto/dlq/`.
Không xóa/rotate file đang replay. Chỉ chạy một replay process cho mỗi file
và checkpoint. JSONL record có `kind`, `payload`, `reason`, `recorded_at`.

Trade thất bại delivery chứa event đã validate; replay đợi Kafka ack. Candle
chứa `updated_at`; SQL chỉ cập nhật khi phiên bản replay không cũ hơn DB.
DLQ topic `crypto.dlq` của events invalid phục vụ điều tra, không đưa thẳng
vào command phục hồi trade.

Chạy trong container pathway (đã copy `scripts/`, có Kafka/DB env đúng):

```powershell
docker compose exec pathway python -m scripts.replay_dlq --file /var/lib/crypto/dlq/trades.jsonl --dry-run
docker compose exec pathway python -m scripts.replay_dlq --file /var/lib/crypto/dlq/trades.jsonl
docker compose exec pathway python -m scripts.replay_dlq --file /var/lib/crypto/dlq/candles.jsonl --dry-run
docker compose exec pathway python -m scripts.replay_dlq --file /var/lib/crypto/dlq/candles.jsonl
```

Nếu engine crash khiến container không chạy, dùng
`docker compose run --rm --no-deps pathway python -m scripts.replay_dlq ...`
sau khi Kafka/Postgres đã sẵn sàng. Container tạm vẫn mount recovery volume.
Sau candle replay, start lại engine và kiểm tra freshness/counters.

Chạy file local: `uv run python -m scripts.replay_dlq --file .dlq/trades.jsonl --dry-run`;
trade default bootstrap `localhost:29092`, topic v2. Candle replay cần
`DATABASE_URL` trong shell. Có thể override `--topic`, `--bootstrap`,
`--checkpoint`; phải xác nhận đích đúng contract trước khi chạy.

Checkpoint mặc định `<file>.checkpoint.json` lưu offset bytes và hash prefix;
chỉ ghi sau ack/DB commit. Lỗi → exit 1, giữ file nguồn và checkpoint cuối.
Chạy lại cùng command để resume. Dry-run chỉ validate các records sau
checkpoint, không gửi/ghi DB hoặc cập nhật checkpoint. Crash giữa ack và
checkpoint có thể gửi lại; dedupe trade ID và candle version xử lý replay.
Khi file nguồn thay đổi prefix, command dừng, không tự bỏ qua dữ liệu.

### Recovery drill local

`uv run python -m scripts.verify_recovery --allow-restarts` chủ động restart
Pathway, dừng consumer/Kafka ngắn hạn rồi restore trong finally. Chỉ chạy
trên local stack đã backup. Dùng symbol test riêng, không reset offsets/xóa
volumes; lưu DLQ/checkpoints/results trong `.recovery/<run-id>`.
DB fault là connection thật bị `pg_terminate_backend`, sau đó endpoint
reconnect bị từ chối; không dừng PostgreSQL của toàn hệ thống.
Drill kiểm tra exact OHLCV sau restart, broker outage → trade DLQ → ack replay,
DB failure → candle DLQ → commit replay, resume 0 records, DLQ volume tồn tại
sau recreate và trade mới sau recreate vẫn aggregate với lịch sử cũ.

## 5. Giới hạn và các bước tiếp theo

- DB version guard dùng thời gian UTC của writer: các máy writers cần
  đồng bộ clock; không dùng hai engine khác graph cùng viết một table.
- Kafka retention, Pathway snapshots và DLQ là một chuỗi phục hồi; backup
  cần giữ chúng nhất quán. Không reset group hoặc xóa volume trong incident
  khi chưa có phương án rebuild. Theo dõi RAM/disk vì dedupe giữ lịch sử.
- Chưa có REST backfill để lấp khoảng trống WebSocket outage. Muốn production
  cần quản lý gap theo trade ID, backfill có rate limit, retention/state bounds,
  backup/restore drill và cấu hình Kafka phù hợp.
- PRICE_SPIKE, price change 5m/15m, dashboards dài hạn là scope mở rộng sau
  khi nghiệm thu độ đúng v2; chưa gắn nhãn hoàn thành.
