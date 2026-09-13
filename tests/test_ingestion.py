"""Unit tests cho ingestion (offline: không mạng, không Kafka)."""
from unittest.mock import MagicMock

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
