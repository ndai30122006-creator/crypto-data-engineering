# Observability: LOG → METRIC → HEALTH → ALERT

Phiên bản v2 theo [kế hoạch 08](../plan/08-fix-upgrade.md).
Recovery và cutover: [runbook](upgrade-runbook.md).

## Logs và metrics

Consumer/producer/Pathway dùng JSON qua `ingestion/jlog.py`; Dagster assets
dùng `context.log`. Xem `docker compose logs binance-consumer pathway`
và Runs tab của Dagster. `uv run python scripts/metrics.py` xuất JSON:

| Section | Nội dung |
|---|---|
| `db` | News 1h, candles 10m, newest candle age, table totals, query latency; resolve tables theo `DAGSTER_ENVIRONMENT` |
| `kafka` | Group từ `KAFKA_GROUP_ID` (default `pathway-ohlcv-1m-v2`), lag, log-end, produce rate; không có committed offsets là unavailable |
| `binance` | Received, invalid, enqueued, acknowledged (`events_published_total`), pending, failed, DLQ saved/failed, reconnect và flush latency |
| `pathway` | Raw events observed, candle updates, processing latency và sink counters |
| `dagster_runs` | Success/failure indicators rút từ log trong khoảng đo; không thay thế run storage chính thức |

`events_lost = received - invalid - acknowledged - failed - pending` đo độ
lệch accounting trong process. Cần bằng 0; cả số âm và dương đều là lỗi.
`failed` có thể đã lưu DLQ, không đồng nghĩa mất vĩnh viễn; ngược lại,
`events_lost=0` không chứng minh không mất trades trước khi WS nhận được.
Enqueued không phải acknowledged. Heartbeat consumer chỉ touch sau Kafka ack.
Counters reset khi process restart; failure counters tích lũy cho cả process.
Metrics file là snapshot định kỳ, không phải dữ liệu thời gian thực từng event.

## Health và exit code

`uv run python scripts/status.py` giám sát 7 services, phân biệt chính xác
healthy/unhealthy/starting/missing, hiển thị flow/data và alert.
`--json` xuất cùng metrics cộng health/alerts.
Status dùng `alert.evaluate()` giống `scripts/alert.py`; exit 1 khi services
hoặc dữ liệu/metric source có lỗi. Docker command lỗi cũng không được báo xanh.
Healthcheck Pathway hiện kiểm tra process; freshness/lag giúp phát hiện
process sống nhưng không tiến triển.

## Rules và xử lý

| Metric | Ngưỡng default / env | Hành động |
|---|---|---|
| `newest_candle_age_min` | >15 / `ALERT_MAX_CANDLE_AGE_MIN` | Kiểm tra pathway, Kafka và consumer |
| `candles_10m` | <20 / `ALERT_MIN_CANDLES_10M` | Kiểm tra topic v2 và engine |
| `news_1h` | <1 / `ALERT_MIN_NEWS_1H` | Kiểm tra RSS và news schedule |
| `failed_runs` | >0 / `ALERT_MAX_FAILED_RUNS` | Kiểm tra Dagster Runs/daemon logs |
| `abs(events_lost)` | >0 / `ALERT_MAX_EVENTS_LOST` | Điều tra accounting, pending và callback settlement |
| `publish_failures_total` | >0 / `ALERT_MAX_PUBLISH_FAILURES` | Kiểm tra Kafka và recovery DLQ, replay sau recovery |
| `dlq_write_failures_total` | >0 / `ALERT_MAX_DLQ_FAILURES` | Kiểm tra disk/quyền ghi volume, xử lý incident trước restart |
| `lag_total` | >5000 / `ALERT_MAX_CONSUMER_LAG` | Kiểm tra engine CPU/logs, retention và throughput |
| Sources unavailable / invalid metric / invalid threshold | Luôn alert | Khôi phục nguồn đo hoặc sửa env; không coi thiếu metric là 0 |

Threshold phải hữu hạn và không âm. Có thể chạy alert command qua Task
Scheduler/cron; chưa cài scheduler bên ngoài trong scope lần sửa này.

## Failure handling

- Valid trade: Kafka producer bật idempotence cho transport retries; final
  failure và synchronous send failure ghi JSONL `trades.jsonl`, đếm failure.
  Application replay vẫn cần trade-ID dedupe.
- Invalid event: reject/log/counter, gửi `crypto.dlq` để điều tra (tắt bằng
  `KAFKA_DLQ_TOPIC=""`); counter DLQ topic chỉ tăng sau ack.
- WebSocket: backoff reconnect; shutdown đóng WS rồi flush/close producer.
- Postgres: retry có giới hạn (`SINK_RETRIES`), kết nối hỏng được thay bằng
  kết nối mới; hết retry ghi `candles.jsonl` rồi raise. File write lỗi được
  log rõ, không báo “saved”. Replay dùng `updated_at` để tránh ghi đè nến mới.
- Compose mount `recovery_dlq` tại `/var/lib/crypto/dlq`, và `pathway_state`
  cho engine snapshots. Không lưu recovery chỉ vào `/tmp`.
- Recovery DLQ giữ trades đã nhận. Chưa có Binance REST backfill cho outage.
