"""ALERT: đánh giá metrics theo ngưỡng, in ALERT + exit code cho automation.

Ngưỡng qua env (default cho local):
  ALERT_MAX_CANDLE_AGE_MIN=15  — nến mới nhất quá cũ (streaming kẹt)
  ALERT_MIN_CANDLES_10M=20     — dưới 20 nến/10 phút (5 symbols × 10)
  ALERT_MIN_NEWS_1H=1          — 1 giờ không có tin mới (RSS/schedule kẹt)
  ALERT_MAX_FAILED_RUNS=0      — bất kỳ run Dagster nào fail
  ALERT_MAX_EVENTS_LOST=0      — consumer làm mất event trong code path
  ALERT_MAX_CONSUMER_LAG=5000  — Kafka consumer lag quá cao
Exit: 0 xanh hết, 1 có đỏ. Mỗi ALERT ghi rõ metric + ngưỡng + hành động.
"""
import os
import sys

from metrics import collect

CHECKS = [
    ("newest_candle_age_min", "ALERT_MAX_CANDLE_AGE_MIN", 15, "gt",
     "market_1m cũ — check pathway/kafka/consumer (docker logs)"),
    ("candles_10m", "ALERT_MIN_CANDLES_10M", 20, "lt",
     "thiếu nến — check engine + topic crypto.trades có event không"),
    ("news_1h", "ALERT_MIN_NEWS_1H", 1, "lt",
     "không có tin mới — check RSS/schedule news_job (Automation tab)"),
    ("failed_runs", "ALERT_MAX_FAILED_RUNS", 0, "gt",
     "run Dagster fail — xem Runs tab + daemon logs"),
    ("events_lost", "ALERT_MAX_EVENTS_LOST", 0, "gt",
     "consumer làm mất event (received != published+invalid+failures) — check bug code path"),
    ("lag_total", "ALERT_MAX_CONSUMER_LAG", 5000, "gt",
     "consumer lag cao (engine theo không kịp producer) — check pathway CPU/log"),
]


# Metric thuộc section nào: section lỗi (error) thì evaluate bỏ qua rule
# (tránh alert giả trên cụm mới dựng; liveness đã có healthcheck lo).
_SECTION_OF = {"events_lost": "binance", "lag_total": "kafka"}


def _value(metrics: dict, key: str):
    if key == "failed_runs":
        return metrics.get("dagster_runs", {}).get("failed")
    if key == "events_lost":
        return metrics.get("binance", {}).get("events_lost")
    if key == "lag_total":
        return metrics.get("kafka", {}).get("lag_total")
    return metrics.get("db", {}).get(key)


def evaluate(metrics: dict, env: dict | None = None) -> list[str]:
    """Trả về list câu ALERT (rỗng = xanh). env inject để test được."""
    env = env if env is not None else os.environ
    alerts = []
    db = metrics.get("db", {})
    if isinstance(db, dict) and "error" in db:
        return [f"ALERT db unreachable: {db['error']} — check postgres container"]
    for key, env_name, default, op, action in CHECKS:
        # Section lỗi (infra mới/thiếu metrics) → bỏ qua rule, tránh alert giả.
        section_name = _SECTION_OF.get(key)
        if section_name:
            section = metrics.get(section_name)
            if not isinstance(section, dict) or "error" in section:
                continue
        threshold = float(env.get(env_name, default))
        value = _value(metrics, key)
        if value is None:
            alerts.append(f"ALERT {key} missing — không tính được metric")
            continue
        bad = value > threshold if op == "gt" else value < threshold
        if bad:
            alerts.append(f"ALERT {key}={value} (ngưỡng {op} {threshold}) — {action}")
    return alerts


def main() -> int:
    metrics = collect()
    alerts = evaluate(metrics)
    for line in alerts:
        print(line)
    if alerts:
        print(f"{len(alerts)} alert(s)")
        return 1
    print("ALL GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
