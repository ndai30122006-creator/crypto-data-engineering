"""E2E test offline cả chuỗi (không Kafka/DB/Pathway thật).

Binance raw → parse → serialize → aggregate OHLCV → to_row →
upsert SQL (mock conn) → correlation. Rớt ở đâu là biết nấc đó sai.
"""
from unittest.mock import MagicMock

import orjson

from ingestion.events import parse_trade, serialize_event
from streaming.correlation import find_correlations, price_changes
from streaming.postgres_sink import UPSERT_1M, to_row
from streaming.windows import aggregate

RAW_WS = [
    {"stream": "btcusdt@trade",
     "data": {"e": "trade", "s": "BTCUSDT", "p": "100", "q": "1", "T": 1789363000000}},
    {"stream": "btcusdt@trade",
     "data": {"e": "trade", "s": "BTCUSDT", "p": "110", "q": "2", "T": 1789363030000}},
    {"stream": "btcusdt@trade",
     "data": {"e": "trade", "s": "BTCUSDT", "p": "105", "q": "1", "T": 1789363060000}},
]


def _mock_conn():
    conn, cur = MagicMock(), MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    return conn, cur


def test_e2e_parse_serialize_aggregate_sink():
    # 1. Parse + serialize (đi qua Kafka wire format rồi về).
    events = [parse_trade(m) for m in RAW_WS]
    assert all(e and e["symbol"] == "BTCUSDT" for e in events)
    assert [orjson.loads(serialize_event(e)) for e in events] == events
    # 2. Aggregate thành nến.
    candles = aggregate(events)
    assert len(candles) == 2
    assert (candles[1]["open"], candles[1]["close"]) == (110.0, 105.0)
    # 3. Sink: tuple INSERT + upsert idempotent.
    _, cur = _mock_conn()
    for candle in candles:
        cur.execute(UPSERT_1M, to_row({**candle, "price_change_1m": None}))
    assert cur.execute.call_count == 2
    assert "ON CONFLICT (symbol, window_start) DO UPDATE" in cur.execute.call_args[0][0]
    symbols = [call[0][1][0] for call in cur.execute.call_args_list]
    assert symbols == ["BTCUSDT", "BTCUSDT"]


def test_e2e_correlation_finds_big_move():
    candles = [
        {"symbol": "BTCUSDT", "window_start": 1000, "close": 100.0},
        {"symbol": "BTCUSDT", "window_start": 1060, "close": 103.0},  # +3%
        {"symbol": "ETHUSDT", "window_start": 1060, "close": 50.0},
    ]
    news = [
        {"title": "BTC surges", "published_at": 1065},  # epoch int, trong ±10p
        {"title": "Old news", "published_at": 1},
    ]
    hits = find_correlations(news, candles)
    assert len(hits) == 1
    assert hits[0]["title"] == "BTC surges" and hits[0]["symbol"] == "BTCUSDT"
    assert hits[0]["price_change_1m"] == 3.0


def test_e2e_correlation_empty_on_quiet_market():
    candles = [
        {"symbol": "BTCUSDT", "window_start": 1000, "close": 100.0},
        {"symbol": "BTCUSDT", "window_start": 1060, "close": 100.1},  # +0.1%
    ]
    assert find_correlations([{"title": "T", "published_at": 1065}], candles) == []


def test_price_changes_first_candle_none():
    out = price_changes([{"symbol": "B", "window_start": 1, "close": 10.0}])
    assert out[0]["price_change_1m"] is None
