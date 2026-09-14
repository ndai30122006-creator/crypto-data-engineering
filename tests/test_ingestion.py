"""Unit tests cho ingestion (offline: không mạng, không Kafka)."""
from unittest.mock import MagicMock

import ingestion.binance_consumer as consumer
from ingestion.binance_consumer import beat, on_raw_message
from ingestion.events import (
    combined_stream_url,
    parse_trade,
    serialize_event,
)
from ingestion.kafka_producer import publish

RAW_TRADE = {
    "stream": "btcusdt@trade",
    "data": {
        "e": "trade",
        "s": "BTCUSDT",
        "p": "112345.12",
        "q": "0.023",
        "T": 1757578923123,
    },
}


def test_combined_stream_url():
    url = combined_stream_url(["BTCUSDT", "ETHUSDT"])
    assert url.startswith("wss://stream.binance.com:9443/stream?streams=")
    assert "btcusdt@trade" in url and "ethusdt@trade" in url


def test_parse_trade_ok():
    event = parse_trade(RAW_TRADE)
    assert event == {
        "symbol": "BTCUSDT",
        "price": 112345.12,
        "quantity": 0.023,
        "timestamp": 1757578923123,
    }


def test_parse_trade_rejects_garbage():
    assert parse_trade({}) is None
    assert parse_trade({"data": {"e": "aggTrade"}}) is None
    assert parse_trade({"e": "trade", "s": "BTCUSDT", "p": "-1", "q": "1", "T": 1}) is None
    assert parse_trade({"e": "trade", "s": "", "p": "1", "q": "1", "T": 1}) is None


def test_serialize_event_roundtrip():
    import json

    event = parse_trade(RAW_TRADE)
    assert json.loads(serialize_event(event).decode("utf-8")) == event


def test_publish_uses_symbol_as_key():
    producer = MagicMock()
    event = parse_trade(RAW_TRADE)
    publish(producer, "crypto.trades", event)
    _, kwargs = producer.send.call_args
    assert kwargs["key"] == "BTCUSDT"  # cùng coin → cùng partition
    assert kwargs["value"] == event


def test_beat_throttles(tmp_path):
    consumer._last_beat = 0.0
    path = str(tmp_path / "heartbeat")
    assert beat(path, interval=10.0, now=100.0) is True
    assert beat(path, interval=10.0, now=105.0) is False  # chưa đủ 10s
    assert beat(path, interval=10.0, now=111.0) is True
    consumer._last_beat = 0.0


def test_on_raw_message_publishes_and_beats(tmp_path):
    consumer._last_beat = 0.0
    import json
    from pathlib import Path

    producer = MagicMock()
    path = str(tmp_path / "heartbeat")
    on_raw_message(producer, "crypto.trades", json.dumps(RAW_TRADE), path)
    assert producer.send.call_count == 1
    assert Path(path).exists()  # nhịp tim đã đập
    consumer._last_beat = 0.0


def test_on_raw_message_skips_garbage(tmp_path):
    producer = MagicMock()
    path = str(tmp_path / "heartbeat")
    on_raw_message(producer, "crypto.trades", "not-json", path)
    on_raw_message(producer, "crypto.trades", "{}", path)
    assert producer.send.call_count == 0
