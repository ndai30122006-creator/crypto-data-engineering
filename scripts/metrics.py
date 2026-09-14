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
        "newest_candle_age_min": round(age_min, 1) if age_min is not None else None,
    }


def collect(minutes: int = DEFAULT_MINUTES) -> dict:
    """Gom hết metrics. Key thiếu/infra chết → error thay vì crash."""
    return {
        "at": datetime.datetime.now(datetime.UTC).isoformat(),
        "window_minutes": minutes,
        "dagster_runs": dagster_runs(f"{minutes}m"),
        "binance": binance_consumer(),
        "db": db_stats(),
    }


def main() -> int:
    minutes = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_MINUTES
    print(json.dumps(collect(minutes), indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
