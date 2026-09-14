"""PHASE 1 — E2E streaming thật: fake Binance events → Kafka → Pathway
(engine live) → market_1m → PostgreSQL → ASSERT.

Không import pathway (không có wheel Windows): test publish events vào
topic live, engine Pathway đang chạy gom nến ghi DB, test poll DB assert.

Symbol E2ETEST + timestamp 2025 (cũ) để không đè nến live; cleanup xóa
sau test. Chạy: $env:INTEGRATION="1"; uv run pytest tests/integration/ -q
"""
import os
import socket
import time

import orjson
import pytest

RUN = os.getenv("INTEGRATION") == "1"
KAFKA_BOOTSTRAP = os.getenv("TEST_KAFKA_BOOTSTRAP", "localhost:29092")
DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL", "postgresql://admin:secret@localhost:5432/crypto_db"
)
TOPIC = "crypto.trades"
SYMBOL = "E2ETEST"

# 6 events cùng 1 bucket phút (10:01:01 → 10:01:50), кількості 1.0 mỗi event.
# Expected: open=100 high=105 low=98 close=103 volume=6.0 count=6.
T0 = 1757578861000
FAKE_TRADES = [
    ("10:01:01", 100.0, 0),
    ("10:01:05", 100.0, 4),
    ("10:01:10", 105.0, 9),
    ("10:01:20", 98.0, 19),
    ("10:01:40", 103.0, 39),
    ("10:01:50", 103.0, 49),
]


def _tcp_ok(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True
    except OSError:
        return False


def _kafka_hostport() -> tuple[str, int]:
    host, _, port = KAFKA_BOOTSTRAP.partition(":")
    return host or "localhost", int(port or 29092)


def _pg_hostport() -> tuple[str, int]:
    from urllib.parse import urlsplit

    parts = urlsplit(DATABASE_URL)
    return parts.hostname or "localhost", parts.port or 5432


needs_stack = pytest.mark.skipif(
    not (RUN and _tcp_ok(*_kafka_hostport()) and _tcp_ok(*_pg_hostport())),
    reason="cần INTEGRATION=1 + Kafka/Postgres local",
)


def _pg():
    import psycopg2

    return psycopg2.connect(DATABASE_URL)


def _cleanup() -> None:
    conn = _pg()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market_1m WHERE symbol = %s;", (SYMBOL,))
    finally:
        conn.close()


@needs_stack
def test_streaming_e2e_fake_to_postgres():
    """Publish 6 fake trades → đợi engine ghi nến → ASSERT OHLCV."""
    from kafka import KafkaProducer

    _cleanup()
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            key_serializer=lambda k: k.encode(),
            value_serializer=lambda v: orjson.dumps(v),
        )
        try:
            for _, price, offset_s in FAKE_TRADES:
                event = {
                    "symbol": SYMBOL,
                    "price": price,
                    "quantity": 1.0,
                    "timestamp": T0 + offset_s * 1000,
                }
                producer.send(TOPIC, key=SYMBOL, value=event).get(timeout=15)
            producer.flush()
        finally:
            producer.close()

        row = None
        # Chờ engine xử lý có timeout 60s — không treo vô hạn nếu pipeline kẹt.
        deadline = time.time() + 60
        while time.time() < deadline:
            conn = _pg()
            try:
                with conn, conn.cursor() as cur:
                    cur.execute(
                        "SELECT open, high, low, close, volume, trade_count "
                        "FROM market_1m WHERE symbol = %s;",
                        (SYMBOL,),
                    )
                    row = cur.fetchone()
            finally:
                conn.close()
            if row and row[5] >= len(FAKE_TRADES):
                break
            time.sleep(2)

        assert row is not None, "engine không ghi nến E2ETEST trong 60s"
        o, h, low, c, vol, cnt = (float(row[0]), float(row[1]), float(row[2]),
                                  float(row[3]), float(row[4]), row[5])
        # OHLC phải chính xác; volume/count dùng >= vì producer retry
        # (kafka-python không có idempotence) có thể gửi trùng — upsert
        # giữ OHLC đúng, chỉ count/volume phình.
        assert (o, h, low, c) == (100.0, 105.0, 98.0, 103.0)
        assert vol >= 6.0 and cnt >= len(FAKE_TRADES)
    finally:
        _cleanup()
