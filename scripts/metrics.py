"""METRIC: gom số liệu pipeline thành 1 JSON cho máy đọc.

Nguồn (không thêm tech): log containers (docker logs --since) + DB.
Chạy: uv run python scripts/metrics.py [--minutes 60]
"""
import datetime
import json
import subprocess
import sys

DEFAULT_MINUTES = 60


def _docker_logs(name: str, since: str) -> str:
    try:
        proc = subprocess.run(
            ["docker", "logs", f"--since={since}", name],
            capture_output=True, text=True, timeout=30, check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return (proc.stdout or "") + (proc.stderr or "")


def pathway_engine() -> dict:
    """Metrics engine (events/windows/latency/last OHLCV theo symbol)."""
    import subprocess

    try:
        proc = subprocess.run(
            ["docker", "exec", "crypto-pathway",
             "cat", "/tmp/pathway-metrics.json"],
            capture_output=True, text=True, timeout=15, check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"error": f"docker exec lỗi: {exc}"}
    if proc.returncode != 0:
        return {"error": "chưa có metrics file (engine mới start?)"}
    try:
        return json.loads(proc.stdout)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        return {"error": f"metrics file hỏng: {exc}"}


def binance_consumer() -> dict:
    """Counters consumer (received/invalid/published/failures/reconnects).

    Đọc file metrics consumer dump (HEARTBEAT chung nhịp 10s).
    """
    try:
        proc = subprocess.run(
            ["docker", "exec", "crypto-binance-consumer",
             "cat", "/tmp/binance-metrics.json"],
            capture_output=True, text=True, timeout=15, check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"error": f"docker exec lỗi: {exc}"}
    if proc.returncode != 0:
        return {"error": "chưa có metrics file (consumer mới start?)"}
    try:
        data = json.loads(proc.stdout)
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        return {"error": f"metrics file hỏng: {exc}"}
    accounted = (data.get("events_published_total", 0)
                 + data.get("events_invalid_total", 0)
                 + data.get("publish_failures_total", 0))
    data["events_lost"] = data.get("events_received_total", 0) - accounted
    return data


def kafka_group(group: str = "pathway-ohlcv-1m") -> dict:
    """Lag + log-end offsets của consumer group (lag = khoảng cách producer-consumer)."""
    import subprocess

    try:
        proc = subprocess.run(
            ["docker", "exec", "crypto-kafka",
             "/opt/kafka/bin/kafka-consumer-groups.sh",
             "--bootstrap-server", "localhost:9092",
             "--describe", "--group", group],
            capture_output=True, text=True, timeout=30, check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return {"error": f"docker exec lỗi: {exc}"}
    if proc.returncode != 0:
        return {"error": "describe group thất bại (group chưa tồn tại?)"}
    lag_total, end_total = 0, 0
    for line in proc.stdout.splitlines():
        parts = line.split()
        # Dòng dữ liệu: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        if len(parts) >= 6 and parts[0] == group and parts[3].lstrip("-").isdigit():
            try:
                end_total += int(parts[4])
                lag_total += int(parts[5])
            except ValueError:
                continue
    return {"group": group, "lag_total": lag_total, "log_end_total": end_total}


def kafka_rate(end_total: int) -> float | None:
    """Produce rate (msg/s) = delta log-end / delta thời gian giữa 2 lần đo."""
    import tempfile
    from pathlib import Path

    state_file = Path(tempfile.gettempdir()) / "crypto_kafka_rate.json"
    now = datetime.datetime.now(datetime.UTC).timestamp()
    prev = None
    try:
        prev = json.loads(state_file.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        prev = None
    try:
        state_file.write_text(json.dumps({"end": end_total, "ts": now}), encoding="utf-8")
    except OSError:
        pass
    if not prev or now - prev.get("ts", now) < 1:
        return None  # lần đo đầu hoặc quá nhanh: chưa có rate
    return round((end_total - prev.get("end", end_total)) / (now - prev["ts"]), 1)


def dagster_runs(since: str = "60m") -> dict:
    """Đếm kết quả runs từ log daemon+code (RUN_SUCCESS vs FAILURE/ERROR)."""
    logs = _docker_logs("crypto-dagster-daemon", since)
    logs += _docker_logs("crypto-dagster-code", since)
    return {
        "success": logs.count("RUN_SUCCESS"),
        "failed": logs.count("RUN_FAILURE") + logs.count("STEP_FAILURE")
                  + logs.count("DagsterLaunchFailedError"),
    }


def db_stats() -> dict:
    """Rows + throughput từ DB (candle 10 phút, tin 1 giờ)."""
    try:
        import psycopg2
    except ImportError:
        return {"error": "missing psycopg2"}
    import os

    url = os.getenv("DATABASE_URL", "postgresql://admin:secret@localhost:5432/crypto_db")
    try:
        conn = psycopg2.connect(url, connect_timeout=5)
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)[:120]}
    try:
        query_started = datetime.datetime.now(datetime.UTC).timestamp()
        with conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM crypto_news_local;")
            news_total = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM crypto_news_local "
                "WHERE collected_at > NOW() - INTERVAL '1 hour';"
            )
            news_1h = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*), COALESCE(sum(trade_count), 0) FROM market_1m "
                "WHERE window_start > NOW() - INTERVAL '10 minutes';"
            )
            candles_10m, trades_10m = cur.fetchone()
            cur.execute("SELECT max(window_start) FROM market_1m;")
            newest = cur.fetchone()[0]
        query_latency = round(
            datetime.datetime.now(datetime.UTC).timestamp() - query_started, 3
        )
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)[:120]}
    finally:
        conn.close()
    age_min = (
        (datetime.datetime.now(datetime.UTC) - newest).total_seconds() / 60
        if newest else None
    )
    return {
        "news_total": news_total,
        "news_1h": news_1h,
        "candles_10m": candles_10m,
        "trades_10m_approx": int(trades_10m or 0),
        "trades_per_min_approx": round(float(trades_10m or 0) / 10, 1),
        "newest_candle_at": newest.isoformat() if newest else None,
        "newest_candle_age_min": round(age_min, 1) if age_min is not None else None,
        "query_latency_s": query_latency,
    }


def collect(minutes: int = DEFAULT_MINUTES) -> dict:
    """Gom hết metrics. Key thiếu/infra chết → error thay vì crash."""
    group = kafka_group()
    rate = kafka_rate(group["log_end_total"]) if "log_end_total" in group else None
    group["produce_per_sec"] = rate
    return {
        "at": datetime.datetime.now(datetime.UTC).isoformat(),
        "window_minutes": minutes,
        "dagster_runs": dagster_runs(f"{minutes}m"),
        "kafka": group,
        "binance": binance_consumer(),
        "pathway": pathway_engine(),
        "db": db_stats(),
    }


def main() -> int:
    minutes = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_MINUTES
    print(json.dumps(collect(minutes), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
