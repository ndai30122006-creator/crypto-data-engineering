"""Integration tests với infra thật (Kafka/Postgres).

Chạy khi stack Docker đang lên:
    $env:INTEGRATION = "1"; uv run pytest tests/test_integration.py -q
Mặc định SKIP hết để `pytest tests/` offline vẫn xanh.
Host tới infra qua: Kafka localhost:29092, Postgres localhost:5432.
Helpers dùng chung ở integration/helpers.py (tránh duplicate).
"""
import orjson
import pytest
from integration.helpers import (
    DATABASE_URL,
    KAFKA_BOOTSTRAP,
    RUN,
    _kafka_hostport,
    _pg_hostport,
    _tcp_ok,
)

needs_kafka = pytest.mark.skipif(
    not (RUN and _tcp_ok(*_kafka_hostport())), reason="cần INTEGRATION=1 + Kafka local"
)
needs_pg = pytest.mark.skipif(
    not (RUN and _tcp_ok(*_pg_hostport())), reason="cần INTEGRATION=1 + Postgres local"
)


@needs_kafka
def test_kafka_produce_consume_roundtrip():
    """Producer thật → topic test → consume lại đúng event đã gửi."""
    from kafka import KafkaConsumer, KafkaProducer

    topic = "test.ingestion.roundtrip"
    event = {"symbol": "TEST", "price": 1.5, "quantity": 2.0, "timestamp": 1}
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: orjson.dumps(v),
    )
    try:
        producer.send(topic, key=event["symbol"], value=event).get(timeout=15)
        producer.flush()
    finally:
        producer.close()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=15000,
        value_deserializer=lambda b: orjson.loads(b),
    )
    try:
        received = [msg.value for msg in consumer if msg.value.get("symbol") == "TEST"]
    finally:
        consumer.close()
    assert event in received


@needs_kafka
def test_consumer_pipeline_to_real_kafka():
    """on_raw_message → Kafka thật → đọc lại được event parse từ WS raw."""
    from unittest.mock import MagicMock

    from kafka import KafkaConsumer

    from ingestion.binance_consumer import on_raw_message

    topic = "test.ingestion.pipeline"
    raw = orjson.dumps(
        {
            "stream": "btcusdt@trade",
            "data": {"e": "trade", "s": "BTCUSDT", "p": "10", "q": "1", "T": 5},
        }
    ).decode()
    producer = MagicMock()
    sent = []

    def _fake_send(t, key=None, value=None):
        sent.append(value)
        return MagicMock()  # giả Future (có add_errback)

    producer.send.side_effect = _fake_send
    producer.flush.return_value = None
    cfg = {"topic": topic, "heartbeat_file": "/tmp/itest-heartbeat", "flush_every": 10**9}
    on_raw_message(producer, cfg, raw)
    assert sent and sent[0]["symbol"] == "BTCUSDT"
    # Gửi thật 1 event rồi đọc lại để chứng minh topic nhận được.
    from kafka import KafkaProducer

    real = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: orjson.dumps(v),
    )
    try:
        real.send(topic, value=sent[0]).get(timeout=15)
        real.flush()
    finally:
        real.close()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=15000,
        value_deserializer=lambda b: orjson.loads(b),
    )
    try:
        got = [m.value for m in consumer if m.value.get("timestamp") == 5]
    finally:
        consumer.close()
    assert got and got[0]["symbol"] == "BTCUSDT"


@needs_pg
def test_postgres_news_roundtrip():
    """insert_news thật → đọc lại đúng title (bảng _local)."""
    import psycopg2

    from dagster_project.resources.postgres import PostgresResource

    pg = PostgresResource(
        conn_str=DATABASE_URL, env="test"
    )
    article = {
        "title": "ITEST",
        "url": "https://itest.local/1",
        "source": "itest",
        "published_at": None,
        "content": "bitcoin surges",
        "sentiment": "positive",
        "symbols": ["BTC"],
    }
    assert pg.insert_news([article]) >= 0
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT title FROM crypto_news_test WHERE url = %s",
                ("https://itest.local/1",),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    assert row and row[0] == "ITEST"
