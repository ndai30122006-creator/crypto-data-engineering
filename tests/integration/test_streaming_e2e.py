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


def _cleanup(symbol: str = SYMBOL) -> None:
    conn = _pg()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market_1m WHERE symbol = %s;", (symbol,))
    finally:
        conn.close()


def _wait_candle(symbol: str, min_count: int, timeout_s: float = 60):
    """Poll market_1m tới khi đủ count hoặc hết timeout (chống treo)."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        conn = _pg()
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
        time.sleep(2)
    return None


def _publish_all(producer) -> None:
    for _, price, offset_s in FAKE_TRADES:
        event = {
            "symbol": SYMBOL,
            "price": price,
            "quantity": 1.0,
            "timestamp": T0 + offset_s * 1000,
        }
        producer.send(TOPIC, key=SYMBOL, value=event).get(timeout=15)
    producer.flush()


@needs_stack
def test_streaming_e2e_fake_to_postgres():
    """Publish 6 fake trades → đợi engine ghi nến → ASSERT OHLCV."""
    from kafka import KafkaProducer

    _cleanup()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: orjson.dumps(v),
    )
    try:
        _publish_all(producer)

        # Chờ engine xử lý có timeout 60s — không treo vô hạn nếu pipeline kẹt.
        row = _wait_candle(SYMBOL, len(FAKE_TRADES))
        assert row is not None, "engine không ghi nến E2ETEST trong 60s"
        from datetime import UTC, datetime

        sym, w_start, o, h, low, c, vol, cnt = (
            row[0], row[1], float(row[2]), float(row[3]),
            float(row[4]), float(row[5]), float(row[6]), row[7],
        )
        expected_bucket = datetime.fromtimestamp(T0 // 1000 // 60 * 60, tz=UTC)
        assert sym == SYMBOL
        assert w_start == expected_bucket
        # OHLC phải chính xác; volume/count dùng >= vì producer retry
        # (kafka-python không có idempotence) có thể gửi trùng — upsert
        # giữ OHLC đúng, chỉ count/volume phình.
        assert (o, h, low, c) == (100.0, 105.0, 98.0, 103.0)
        assert vol >= 6.0 and cnt >= len(FAKE_TRADES)

        # Step 1.5 — Idempotency: publish lại y hệt 6 events, nến không đổi.
        baseline = (sym, w_start, o, h, low, c)
        _publish_all(producer)
        time.sleep(15)  # cho engine xử lý lại hết batch trùng
        conn = _pg()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol, window_start, open, high, low, close "
                    "FROM market_1m WHERE symbol = %s;",
                    (SYMBOL,),
                )
                row2 = cur.fetchone()
        finally:
            conn.close()
        assert row2 is not None
        again = (row2[0], row2[1],
                 float(row2[2]), float(row2[3]), float(row2[4]), float(row2[5]))
        assert again == baseline, f"idempotency vỡ: {baseline} != {again}"
    finally:
        producer.close()
        _cleanup()


BAD_SYMBOL = "E2EINVALID"


@needs_stack
def test_streaming_e2e_invalid_events_ignored():
    """Event rác (sai envelope/thiếu field) → engine bỏ qua, không có nến rác."""
    from kafka import KafkaProducer

    conn = _pg()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market_1m WHERE symbol = %s;", (BAD_SYMBOL,))
    finally:
        conn.close()
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            key_serializer=lambda k: k.encode(),
            value_serializer=lambda v: orjson.dumps(v),
        )
        try:
            # Envelope đúng JSON nhưng không phải trade, thiếu timestamp.
            producer.send(
                TOPIC, key=BAD_SYMBOL,
                value={"stream": "x", "data": {"e": "aggTrade", "s": BAD_SYMBOL}},
            ).get(timeout=15)
            # Thiếu price → sai schema TradeSchema.
            producer.send(
                TOPIC, key=BAD_SYMBOL,
                value={"symbol": BAD_SYMBOL, "quantity": 1.0, "timestamp": T0},
            ).get(timeout=15)
            producer.flush()
        finally:
            producer.close()
        time.sleep(20)  # đủ cho engine nuốt + ghi nếu nó ghi bậy
        conn = _pg()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM market_1m WHERE symbol = %s;", (BAD_SYMBOL,)
                )
                assert cur.fetchone()[0] == 0, "engine ghi nến từ event rác"
        finally:
            conn.close()
    finally:
        conn = _pg()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM market_1m WHERE symbol = %s;", (BAD_SYMBOL,))
        finally:
            conn.close()


OOO_SYMBOL = "E2EOOO"
# Thứ tự arrival: A → B → C. Thứ tự event-time: C → A → B.
# Nến đúng theo event-time: open=100 (C) high=105 low=98 close=98 (B).
OOO_TRADES = [
    ("A", 105.0, 10),
    ("B", 98.0, 20),
    ("C", 100.0, 5),
]


@needs_stack
def test_streaming_e2e_out_of_order():
    """Step 2.1 — events tới đảo thứ tự, nến vẫn đúng theo event-time."""
    from kafka import KafkaProducer

    _cleanup(OOO_SYMBOL)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: orjson.dumps(v),
    )
    try:
        for _, price, offset_s in OOO_TRADES:  # arrival A → B → C
            producer.send(
                TOPIC, key=OOO_SYMBOL,
                value={"symbol": OOO_SYMBOL, "price": price,
                       "quantity": 1.0, "timestamp": T0 + offset_s * 1000},
            ).get(timeout=15)
        producer.flush()
        row = _wait_candle(OOO_SYMBOL, len(OOO_TRADES))
        assert row is not None, "engine không ghi nến E2EOOO trong 60s"
        o, h, low, c = (float(row[2]), float(row[3]), float(row[4]), float(row[5]))
        assert (o, h, low, c) == (100.0, 105.0, 98.0, 98.0), (
            f"nến sai theo event-time (arrival A→B→C): {(o, h, low, c)}"
        )
    finally:
        producer.close()
        _cleanup(OOO_SYMBOL)
