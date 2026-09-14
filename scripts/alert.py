"""ALERT: đánh giá metrics theo ngưỡng, in ALERT + exit code cho automation.

Ngưỡng qua env (default cho local):
  ALERT_MAX_CANDLE_AGE_MIN=15  — nến mới nhất quá cũ (streaming kẹt)
  ALERT_MIN_CANDLES_10M=20     — dưới 20 nến/10 phút (5 symbols × 10)
  ALERT_MIN_NEWS_1H=1          — 1 giờ không có tin mới (RSS/schedule kẹt)
  ALERT_MAX_FAILED_RUNS=0      — bất kỳ run Dagster nào fail
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
]


def _value(metrics: dict, key: str):
    if key == "failed_runs":
        return metrics.get("dagster_runs", {}).get("failed")
    return metrics.get("db", {}).get(key)


def evaluate(metrics: dict, env: dict | None = None) -> list[str]:
    """Trả về list câu ALERT (rỗng = xanh). env inject để test được."""
    env = env if env is not None else os.environ
    alerts = []
    db = metrics.get("db", {})
    if isinstance(db, dict) and "error" in db:
        return [f"ALERT db unreachable: {db['error']} — check postgres container"]
    for key, env_name, default, op, action in CHECKS:
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
