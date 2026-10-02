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

import math
import os
import sys

from metrics import collect

CHECKS = [
    (
        "newest_candle_age_min",
        "ALERT_MAX_CANDLE_AGE_MIN",
        15,
        "gt",
        "market_1m cũ — check pathway/kafka/consumer (docker logs)",
    ),
    (
        "candles_10m",
        "ALERT_MIN_CANDLES_10M",
        20,
        "lt",
        "thiếu nến — check engine + topic crypto.trades.v2 có event không",
    ),
    (
        "news_1h",
        "ALERT_MIN_NEWS_1H",
        1,
        "lt",
        "không có tin mới — check RSS/schedule news_job (Automation tab)",
    ),
    (
        "failed_runs",
        "ALERT_MAX_FAILED_RUNS",
        0,
        "gt",
        "run Dagster fail — xem Runs tab + daemon logs",
    ),
    (
        "events_lost",
        "ALERT_MAX_EVENTS_LOST",
        0,
        "gt",
        "delivery accounting lệch (received != acknowledged+invalid+failed+pending) — check consumer",
    ),
    (
        "publish_failures_total",
        "ALERT_MAX_PUBLISH_FAILURES",
        0,
        "gt",
        "Kafka delivery failed — inspect consumer logs and recovery DLQ",
    ),
    (
        "dlq_write_failures_total",
        "ALERT_MAX_DLQ_FAILURES",
        0,
        "gt",
        "recovery DLQ unwritable — check volume permissions and disk space",
    ),
    (
        "lag_total",
        "ALERT_MAX_CONSUMER_LAG",
        5000,
        "gt",
        "consumer lag cao (engine theo không kịp producer) — check pathway CPU/log",
    ),
]


# Section không truy cập được tạo alert unavailable, bỏ qua các số không có.
_SECTION_OF = {
    "events_lost": "binance",
    "publish_failures_total": "binance",
    "dlq_write_failures_total": "binance",
    "lag_total": "kafka",
}


def _value(metrics: dict, key: str):
    if key == "failed_runs":
        return metrics.get("dagster_runs", {}).get("failed")
    if _SECTION_OF.get(key) == "binance":
        return metrics.get("binance", {}).get(key)
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
    for name in ("binance", "kafka"):
        section = metrics.get(name)
        if not isinstance(section, dict) or "error" in section:
            alerts.append(
                f"ALERT {name} unavailable — inspect service and metrics source"
            )
    if "error" in metrics.get("pathway", {}):
        alerts.append("ALERT pathway unavailable — inspect engine and metrics source")
    for key, env_name, default, op, action in CHECKS:
        # Section lỗi (infra mới/thiếu metrics) → bỏ qua rule, tránh alert giả.
        section_name = _SECTION_OF.get(key)
        if section_name:
            section = metrics.get(section_name)
            if not isinstance(section, dict) or "error" in section:
                continue
        try:
            threshold = float(env.get(env_name, default))
            if not math.isfinite(threshold) or threshold < 0:
                raise ValueError("threshold must be finite and non-negative")
        except (TypeError, ValueError):
            alerts.append(f"ALERT invalid threshold {env_name}")
            continue
        value = _value(metrics, key)
        if value is None:
            alerts.append(f"ALERT {key} missing — không tính được metric")
            continue
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            alerts.append(f"ALERT {key} invalid — expected finite numeric metric")
            continue
        bad = (
            abs(value) > threshold
            if key == "events_lost"
            else (value > threshold if op == "gt" else value < threshold)
        )
        if bad:
            alerts.append(f"ALERT {key}={value} (ngưỡng {op} {threshold}) — {action}")
    return alerts


def _safe(text: str) -> str:
    """Console Windows (cp1252) không in được dấu tiếng Việt → thay ?."""
    enc = (sys.stdout.encoding or "utf-8").lower()
    if "utf" in enc:
        return text
    return text.encode(enc, errors="replace").decode(enc)


def main() -> int:
    metrics = collect()
    alerts = evaluate(metrics)
    for line in alerts:
        print(_safe(line))
    if alerts:
        print(f"{len(alerts)} alert(s)")
        return 1
    print("ALL GREEN")
    return 0


if __name__ == "__main__":
    sys.exit(main())
