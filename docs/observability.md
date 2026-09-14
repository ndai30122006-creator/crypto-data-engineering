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
- `trades_per_min` là xấp xỉ (không phải đếm Kafka offsets) — đủ để thấy trend, không dùng tính tiền

## HEALTH (khỏe hay không)

- Từng container: healthcheck trong compose (postgres `pg_isready`, code socket 4000, webserver `/server_info`, daemon process `/proc`, kafka `kafka-topics.sh`, consumer heartbeat file).
- Tổng hợp cho người: `uv run python scripts/status.py` (11 checks, exit 0/1).

## ALERT (ngưỡng + hành động)

`uv run python scripts/alert.py` → in `ALERT ... — <hành động>`, exit 1 nếu có đỏ:

| Alert | Ngưỡng default (env override) | Hành động |
|---|---|---|
| `newest_candle_age_min` | > 15 (`ALERT_MAX_CANDLE_AGE_MIN`) | `docker logs crypto-pathway`, check kafka/consumer |
| `candles_10m` | < 20 (`ALERT_MIN_CANDLES_10M`) | topic `crypto.trades` còn event không |
| `news_1h` | < 1 (`ALERT_MIN_NEWS_1H`) | RSS/schedule `news_job` ở Automation tab |
| `failed_runs` | > 0 (`ALERT_MAX_FAILED_RUNS`) | Runs tab + daemon logs |
| `db unreachable` | — | `crypto-postgres` sống không |

Chạy định kỳ bằng Task Scheduler/cron gọi `alert.py` (exit code) — chưa cần server riêng.
