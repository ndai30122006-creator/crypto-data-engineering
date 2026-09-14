# Observability: LOG → METRIC → HEALTH → ALERT

Không thêm tech (Prometheus/Grafana để sau). Mọi tầng dùng thứ sẵn có:
log JSON, healthcheck Docker, DB, exit codes.

## LOG (ghi gì, ở đâu)

| Nguồn | Format | Xem bằng |
|---|---|---|
| consumer/producer/pathway | JSON qua `ingestion/jlog.py` (`logger`, `level`, `message` + fields) | `docker logs crypto-binance-consumer` |
| Dagster assets/resources | `context.log` / `get_dagster_logger` | Dagster UI (Runs tab) + `docker logs crypto-dagster-code` |
| Engine Pathway | jlogger + engine monitoring | `docker logs crypto-pathway` |

Quy ước: services dùng jlogger, Dagster dùng hệ log riêng — không lẫn.

## METRIC (rút từ log + DB)

`uv run python scripts/metrics.py` → 1 JSON:

- `dagster_runs.{success,failed}`: đếm `RUN_SUCCESS` vs `RUN_FAILURE`/`STEP_FAILURE`/`DagsterLaunchFailedError` trong log 60 phút
- `db.news_1h`, `db.candles_10m`, `db.trades_per_min_approx` (sum `trade_count` / 10), `db.newest_candle_age_min`
- `kafka.lag_total` (consumer lag group pathway-ohlcv-1m — metric số 1), `kafka.produce_per_sec` (delta log-end giữa 2 lần đo, null ở lần đầu), `kafka.log_end_total`
- `pathway.*`: `events_processed_total`, `windows_created_total`, `last/max_processing_latency_s`, `last_event`, `last_ohlcv` theo symbol (vd last candle BTC + latency), `db` (sink upsert rows/failures/latency)
- `binance.*`: counters consumer (`received/invalid/published/failures/reconnects`, `last/max_flush_latency_s`) + `events_lost = received - published - invalid - failures` (phải = 0)
- DB client (Dagster resource): `insert_success/failure_total`, `rows_inserted_total`, `last/max_query_latency_s` — xem qua asset metadata từng materialize; monitoring script tự đo `query_latency_s` khi quét DB
- `trades_per_min` là xấp xỉ (không phải đếm Kafka offsets) — đủ để thấy trend, không dùng tính tiền

## HEALTH (khỏe hay không)

- Từng container: healthcheck trong compose (postgres `pg_isready`, code socket 4000, webserver `/server_info`, daemon process `/proc`, kafka `kafka-topics.sh`, consumer heartbeat file).
- Tổng hợp cho người: `uv run python scripts/status.py` (dashboard HEALTH + FLOW + DATA, exit 0/1; `--json` ra metrics thô).

## ALERT (ngưỡng + hành động)

`uv run python scripts/alert.py` → in `ALERT ... — <hành động>`, exit 1 nếu có đỏ:

| Alert | Ngưỡng default (env override) | Hành động |
|---|---|---|
| `newest_candle_age_min` | > 15 (`ALERT_MAX_CANDLE_AGE_MIN`) | `docker logs crypto-pathway`, check kafka/consumer |
| `candles_10m` | < 20 (`ALERT_MIN_CANDLES_10M`) | topic `crypto.trades` còn event không |
| `news_1h` | < 1 (`ALERT_MIN_NEWS_1H`) | RSS/schedule `news_job` ở Automation tab |
| `failed_runs` | > 0 (`ALERT_MAX_FAILED_RUNS`) | Runs tab + daemon logs |
| `events_lost` | > 0 (`ALERT_MAX_EVENTS_LOST`) | consumer làm mất event — check bug code path |
| `lag_total` | > 5000 (`ALERT_MAX_CONSUMER_LAG`) | engine theo không kịp producer — check pathway CPU/log, cân nhắc tăng partition |
| `db unreachable` | — | `crypto-postgres` sống không |

Chạy định kỳ bằng Task Scheduler/cron gọi `alert.py` (exit code) — chưa cần server riêng.
