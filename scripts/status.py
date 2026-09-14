"""Tổng hợp sức khỏe pipeline: containers, API, Kafka, DB, freshness.

Chạy: uv run python scripts/status.py
Exit 0 khi mọi check critical xanh, 1 nếu có đỏ.
Chỉ dùng stdlib + psycopg2 + docker CLI (không thêm tech).
"""
import datetime
import json
import os
import socket
import subprocess
import sys
import urllib.request

OK, WARN, FAIL = "OK", "WARN", "FAIL"
results: list[tuple] = []


def _safe(text: str) -> str:
    """Console Windows (cp1252) không in được dấu tiếng Việt → thay ?."""
    enc = (sys.stdout.encoding or "utf-8").lower()
    if "utf" in enc:
        return text
    return text.encode(enc, errors="replace").decode(enc)


def report(name: str, status: str, detail: str = "") -> None:
    results.append((name, status, detail))
    print(_safe(f"[{status:4}] {name}" + (f" - {detail}" if detail else "")))


def sh(cmd: list[str], timeout: int = 15) -> str:
    # check=False cố ý: nonzero → stdout rỗng → báo missing thay vì crash.
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False).stdout


def check_containers() -> None:
    try:
        out = sh(["docker", "ps", "--format", "{{.Names}}|{{.Status}}"])
    except (OSError, subprocess.SubprocessError) as exc:
        report("docker", FAIL, f"docker CLI lỗi: {exc}")
        return
    running = {}
    for line in out.splitlines():
        if "|" in line:
            name, status = line.split("|", 1)
            running[name.strip()] = status.strip()
    expected = ["crypto-postgres", "crypto-kafka", "crypto-dagster-code",
                "crypto-dagster-webserver", "crypto-dagster-daemon",
                "crypto-binance-consumer", "crypto-pathway"]
    for name in expected:
        status = running.get(name, "missing")
        # "(unhealthy)" cũng startswith "Up" nên phải loại trước.
        if "healthy" in status or (status.startswith("Up") and "unhealthy" not in status):
            report(f"container {name}", OK, status)
        else:
            report(f"container {name}", FAIL, status)


def check_dagster_api() -> None:
    try:
        with urllib.request.urlopen("http://localhost:3000/server_info", timeout=10) as r:
            body = json.loads(r.read().decode())
        report("dagster API", OK, f"dagster {body.get('dagster_version')}")
    except OSError as exc:
        report("dagster API", FAIL, str(exc)[:100])


def check_kafka() -> None:
    try:
        socket.create_connection(("localhost", 29092), timeout=5).close()
        report("kafka :29092", OK, "port mở")
    except OSError as exc:
        report("kafka :29092", FAIL, str(exc)[:100])


def check_db() -> None:
    try:
        import psycopg2
    except ImportError:
        report("postgres", WARN, "thiếu psycopg2, bỏ qua check DB")
        return
    # Secret qua env, default local chỉ để chạy nhanh trên máy dev.
    url = os.getenv("DATABASE_URL", "postgresql://admin:secret@localhost:5432/crypto_db")
    try:
        conn = psycopg2.connect(url, connect_timeout=5)
    except Exception as exc:  # noqa: BLE001 - báo lỗi kết nối gọn
        report("postgres", FAIL, str(exc)[:120])
        return
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM crypto_news_local;")
            news = cur.fetchone()[0]
            cur.execute("SELECT count(*), max(window_start) FROM market_1m;")
            candles, newest = cur.fetchone()
        report("db rows", OK, f"news={news} candles={candles}")
        if newest is not None:
            age = (datetime.datetime.now(datetime.UTC) - newest).total_seconds() / 60
            report("market_1m freshness", OK if age < 15 else FAIL,
                   f"nến mới nhất {age:.1f} phút trước")
    except Exception as exc:  # noqa: BLE001
        report("db rows", FAIL, str(exc)[:120])
    finally:
        conn.close()


def main() -> int:
    print("== crypto-data-engineering status ==")
    check_containers()
    check_dagster_api()
    check_kafka()
    check_db()
    failed = sum(1 for _, s, _ in results if s == FAIL)
    print(f"== {len(results) - failed}/{len(results)} xanh ==")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
