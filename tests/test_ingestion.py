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


def _cfg(tmp_path, **over):
    cfg = {
        "topic": "crypto.trades",
        "heartbeat_file": str(tmp_path / "heartbeat"),
        "flush_every": 10_000,
    }
    cfg.update(over)
    return cfg


def test_on_raw_message_publishes_and_beats(tmp_path):
    consumer._last_beat = 0.0
    consumer._published = 0
    consumer._flushed_at = 0
    import json
    from pathlib import Path

    producer = MagicMock()
    on_raw_message(producer, _cfg(tmp_path), json.dumps(RAW_TRADE))
    assert producer.send.call_count == 1
    assert Path(_cfg(tmp_path)["heartbeat_file"]).exists()  # nhịp tim đã đập
    consumer._last_beat = 0.0


def test_on_raw_message_skips_garbage(tmp_path):
    producer = MagicMock()
    cfg = _cfg(tmp_path)
    on_raw_message(producer, cfg, "not-json")
    on_raw_message(producer, cfg, "{}")
    assert producer.send.call_count == 0


def test_handle_signal_closes_ws_and_flags_shutdown():
    ws = MagicMock()
    consumer._current_ws = ws
    consumer._shutdown.clear()
    consumer._handle_signal(15, None)
    assert consumer._shutdown.is_set()
    ws.close.assert_called_once_with()
    assert consumer._current_ws is None
    consumer._shutdown.clear()


def test_load_config_bad_flush_every_falls_back():
    import os
    from unittest.mock import patch

    with patch.dict(os.environ, {"FLUSH_EVERY": "abc"}):
        assert consumer.load_config()["flush_every"] == 500
    with patch.dict(os.environ, {"FLUSH_EVERY": "-3"}):
        assert consumer.load_config()["flush_every"] == 500


def test_on_raw_message_flushes_on_cadence(tmp_path):
    consumer._published = 0
    consumer._flushed_at = 0
    consumer._last_beat = 0.0
    import json

    producer = MagicMock()
    cfg = _cfg(tmp_path, flush_every=3)
    for _ in range(3):
        on_raw_message(producer, cfg, json.dumps(RAW_TRADE))
    assert producer.flush.call_count == 1
    consumer._published = 0
    consumer._flushed_at = 0
    consumer._last_beat = 0.0


def test_publish_errback_logs_and_hooks():
    from ingestion.kafka_producer import publish as real_publish

    producer = MagicMock()
    future = MagicMock()
    producer.send.return_value = future
    hooked = []
    event = parse_trade(RAW_TRADE)
    real_publish(producer, "crypto.trades", event, on_error=lambda e, ev: hooked.append(ev))
    errback = future.add_errback.call_args[0][0]
    errback(RuntimeError("boom"))
    assert hooked == [event]
