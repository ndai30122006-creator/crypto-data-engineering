"""Helpers dùng chung cho integration tests (infra thật).

Symbol + timestamp 2025 (cũ) để không đè nến live; mỗi test tự cleanup
symbol của mình. Skip toàn bộ khi thiếu INTEGRATION=1 hoặc infra.
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

# 1 bucket phút 2025: mọi test symbol khác nhau nên không đụng nhau.
T0 = 1757578861000


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


def pg():
    import psycopg2

    return psycopg2.connect(DATABASE_URL)


def cleanup(symbol: str) -> None:
    conn = pg()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market_1m WHERE symbol = %s;", (symbol,))
    finally:
        conn.close()


def make_producer():
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: orjson.dumps(v),
    )


def publish_trades(producer, symbol: str, trades: list[tuple[float, int, float]]) -> None:
    """Publish list (price, offset_s, qty); mỗi send .get() giữ thứ tự arrival."""
    for price, offset_s, qty in trades:
        producer.send(
            TOPIC, key=symbol,
            value={"symbol": symbol, "price": price,
                   "quantity": qty, "timestamp": T0 + offset_s * 1000},
        ).get(timeout=15)
    producer.flush()


def wait_candle(symbol: str, min_count: int, timeout_s: float = 60):
    """Poll market_1m tới khi đủ count hoặc hết timeout (chống treo)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        conn = pg()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol, window_start, open, high, low, close, "
                    "volume, trade_count "
                    "FROM market_1m WHERE symbol = %s;",
                    (symbol,),
                )
                row = cur.fetchone()
        finally:
            conn.close()
        if row and row[7] >= min_count:
            return row
    return None


def as_floats(row) -> tuple:
    """Row DB → (symbol, window_start, o, h, low, c, vol, cnt)."""
    return (row[0], row[1], float(row[2]), float(row[3]),
            float(row[4]), float(row[5]), float(row[6]), row[7])
