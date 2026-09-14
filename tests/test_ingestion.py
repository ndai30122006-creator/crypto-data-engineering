"""Unit tests cho ingestion (offline: không mạng, không Kafka)."""
from unittest.mock import MagicMock, patch

import pytest

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
    import orjson

    event = parse_trade(RAW_TRADE)
    assert orjson.loads(serialize_event(event)) == event


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
        "metrics_file": str(tmp_path / "metrics.json"),
        "flush_every": 10_000,
    }
    cfg.update(over)
    return cfg


def _reset_consumer_state() -> None:
    consumer._last_beat = 0.0
    consumer._flushed_at = 0
    for key in consumer._counters:
        consumer._counters[key] = 0


def test_on_raw_message_publishes_and_beats(tmp_path):
    _reset_consumer_state()
    from pathlib import Path

    import orjson

    producer = MagicMock()
    on_raw_message(producer, _cfg(tmp_path), orjson.dumps(RAW_TRADE).decode())
    assert producer.send.call_count == 1
    assert Path(_cfg(tmp_path)["heartbeat_file"]).exists()  # nhịp tim đã đập
    assert Path(_cfg(tmp_path)["metrics_file"]).exists()  # metrics đã dump
    assert consumer.snapshot_metrics()["events_published_total"] == 1
    _reset_consumer_state()


def test_on_raw_message_counts_invalid(tmp_path):
    _reset_consumer_state()
    producer = MagicMock()
    cfg = _cfg(tmp_path)
    on_raw_message(producer, cfg, "not-json")
    on_raw_message(producer, cfg, "{}")
    assert producer.send.call_count == 0
    assert consumer.snapshot_metrics()["events_invalid_total"] == 2
    assert consumer.snapshot_metrics()["events_received_total"] == 2
    _reset_consumer_state()


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
    _reset_consumer_state()
    import orjson

    producer = MagicMock()
    cfg = _cfg(tmp_path, flush_every=3)
    for _ in range(3):
        on_raw_message(producer, cfg, orjson.dumps(RAW_TRADE).decode())
    assert producer.flush.call_count == 1
    assert consumer.snapshot_metrics()["events_published_total"] == 3
    assert consumer.snapshot_metrics()["last_flush_latency_s"] >= 0.0
    _reset_consumer_state()


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


def test_kafka_down_flush_error_propagates(tmp_path):
    """Step 4.1 — Kafka chết: lỗi flush KHÔNG bị nuốt (vòng lặp ngoài retry)."""
    from kafka.errors import KafkaConnectionError

    _reset_consumer_state()
    import orjson

    producer = MagicMock()
    producer.flush.side_effect = KafkaConnectionError("no broker")
    cfg = _cfg(tmp_path, flush_every=1)
    with pytest.raises(KafkaConnectionError):
        on_raw_message(producer, cfg, orjson.dumps(RAW_TRADE).decode())
    _reset_consumer_state()


def test_ws_disconnect_reconnects():
    """Step 4.2 — WS rớt: tạo kết nối mới (reconnect), không chết vĩnh viễn."""
    _reset_consumer_state()
    consumer._shutdown.clear()
    created = []

    class FakeWS:
        def __init__(self, *a, **kw):
            created.append(self)

        def run_forever(self, **kw):
            if len(created) >= 2:
                consumer._shutdown.set()  # lần 2 xong thì dừng vòng lặp

        def close(self):
            pass

    with (
        patch("websocket.WebSocketApp", FakeWS),
        patch.object(consumer, "build_producer") as mock_build,
        patch.object(consumer._shutdown, "wait", return_value=None),
    ):
        mock_build.return_value = MagicMock()
        consumer.run_forever()
    assert len(created) == 2  # kết nối đầu + 1 lần reconnect
    assert consumer._shutdown.is_set()
    consumer._shutdown.clear()
    _reset_consumer_state()


def test_invalid_trade_rejected_with_metric(tmp_path):
    """Step 4.3 — trade invalid (price<0): reject + metric (sau này nâng DLQ)."""
    _reset_consumer_state()
    import orjson

    producer = MagicMock()
    cfg = _cfg(tmp_path)
    bad = orjson.dumps({"e": "trade", "s": "BTCUSDT", "p": "-100", "q": "1", "T": 1}).decode()
    on_raw_message(producer, cfg, bad)
    assert producer.send.call_count == 0
    assert consumer.snapshot_metrics()["events_invalid_total"] == 1
    _reset_consumer_state()
