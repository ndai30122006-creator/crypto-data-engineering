"""Crypto Data Platform Status: 1 lệnh thấy sức khỏe toàn hệ thống.

HEALTH (containers) + METRIC (collect) + FLOW (kafka) + DATA (db) + ALERT.
Chạy: uv run python scripts/status.py [--json]
Exit 0 xanh hết, 1 có đỏ (khớp alert.py).
"""
import subprocess
import sys

try:
    from metrics import collect
except ImportError:  # chạy từ repo root: scripts/ không phải package
    sys.path.insert(0, __import__("os").path.dirname(__file__))
    from metrics import collect

CONTAINERS = [
    ("PostgreSQL", "crypto-postgres"),
    ("Kafka", "crypto-kafka"),
    ("Dagster", "crypto-dagster-webserver"),
    ("Dagster Daemon", "crypto-dagster-daemon"),
    ("Binance Consumer", "crypto-binance-consumer"),
    ("Pathway", "crypto-pathway"),
]


def container_health() -> dict[str, str]:
    """Tên hiển thị → 'healthy'/'starting'/... (lỗi docker → 'unknown')."""
    try:
        out = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}|{{.Status}}"],
            capture_output=True, text=True, timeout=15, check=False,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return {}
    running = {}
    for line in out.splitlines():
        if "|" in line:
            name, status = line.split("|", 1)
            running[name.strip()] = status.strip()
    health = {}
    for label, name in CONTAINERS:
        status = running.get(name, "missing")
        if "healthy" in status:
            health[label] = "ok"
        elif status.startswith("Up"):
            health[label] = "starting"
        else:
            health[label] = "down"
    return health


def _fmt_int(value) -> str:
    return f"{value:,}" if isinstance(value, int) else "n/a"


def _fmt_age(age_min) -> str:
    if age_min is None:
        return "n/a"
    if age_min < 1:
        return f"{age_min * 60:.0f} sec"
    return f"{age_min:.1f} min"


def _safe(text: str) -> str:
    """Console Windows (cp1252) không in được ✓✗─ → thay ASCII."""
    enc = (sys.stdout.encoding or "utf-8").lower()
    if "utf" in enc:
        return text
    return text.replace("✓", "[OK]").replace("✗", "[FAIL]").replace("─", "-")


def render(metrics: dict, health: dict[str, str]) -> tuple[str, int]:
    """Render dashboard text + exit code (pure, test được)."""
    db = metrics.get("db", {})
    kafka = metrics.get("kafka", {})
    binance = metrics.get("binance", {})
    dagster = metrics.get("dagster_runs", {})
    lines = ["Crypto Data Platform Status", "─" * 28, ""]
    failed = 0

    for label, _ in CONTAINERS:
        state = health.get(label, "unknown")
        mark = "✓" if state == "ok" else "✗"
        if state != "ok":
            failed += 1
            lines.append(f"{label:16} {mark} ({state})")
        else:
            lines.append(f"{label:16} {mark}")
    lines.append("")

    def _flow(label: str, value: str) -> None:
        lines.append(f"{label:16} {value}")

    if isinstance(db, dict) and "error" in db:
        lines.append(f"DB                 ✗ ({db['error'][:60]})")
        failed += 1
    else:
        _flow("DB Rows", _fmt_int(db.get("rows_total")))
        _flow("OHLCV Freshness", _fmt_age(db.get("newest_candle_age_min")))
    if isinstance(kafka, dict) and "error" not in kafka:
        _flow("Kafka Events", _fmt_int(kafka.get("log_end_total")))
    else:
        lines.append("Kafka Events       n/a")
        failed += 1
    if isinstance(binance, dict) and "error" not in binance:
        _flow("Kafka Failures", _fmt_int(binance.get("publish_failures_total")))
        lag = kafka.get("lag_total") if isinstance(kafka, dict) else None
        _flow("Consumer Lag", _fmt_int(lag))
    else:
        lines.append("Kafka Failures     n/a")
        failed += 1
    if isinstance(dagster, dict):
        _flow("Dagster Failed", _fmt_int(dagster.get("failed")))
        if (dagster.get("failed") or 0) > 0:
            failed += 1
    return "\n".join(lines), (1 if failed else 0)


def main() -> int:
    if "--json" in sys.argv:
        import json

        print(json.dumps(collect(), indent=2, default=str))
        return 0
    metrics = collect()
    health = container_health()
    text, code = render(metrics, health)
    print(_safe(text))
    return code


if __name__ == "__main__":
    sys.exit(main())
