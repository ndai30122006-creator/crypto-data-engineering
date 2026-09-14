"""Unit tests cho streaming windows + sink + engine metrics (offline)."""
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

from streaming import metrics as engine_metrics
from streaming.postgres_sink import ensure_tables, to_row, upsert_candles
from streaming.windows import WINDOW_SECONDS, aggregate, bucket_start


def test_bucket_start():
    assert bucket_start(1789363030123) == 1789363020
    assert WINDOW_SECONDS == 60


def test_aggregate_ohlcv():
    trades = [
        {"symbol": "BTCUSDT", "price": 100.0, "quantity": 1.0, "timestamp": 1789363000000},
        {"symbol": "BTCUSDT", "price": 110.0, "quantity": 2.0, "timestamp": 1789363030000},
        {"symbol": "BTCUSDT", "price": 105.0, "quantity": 1.0, "timestamp": 1789363060000},
    ]
    rows = aggregate(trades)
    assert len(rows) == 2
    first, second = rows
    assert (first["open"], first["close"], first["volume"], first["trade_count"]) == (100.0, 100.0, 1.0, 1)
    assert second["open"] == 110.0 and second["close"] == 105.0
    assert (second["high"], second["low"], second["volume"], second["trade_count"]) == (110.0, 105.0, 3.0, 2)


def test_to_row_converts_window():
    row = to_row(
        {"symbol": "BTC", "window_start": 1789363020, "open": 1.0, "high": 2.0,
         "low": 0.5, "close": 1.5, "volume": 3.0, "trade_count": 2,
         "price_change_1m": None}
    )
    assert row[0] == "BTC"
    assert row[1] == datetime.fromtimestamp(1789363020, tz=UTC)
    assert row[7] == 2 and row[8] is None


def test_upsert_candles_sql():
    conn, cur = MagicMock(), MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    n = upsert_candles(conn, [
        {"symbol": "B", "window_start": 1, "open": 1.0, "high": 1.0, "low": 1.0,
         "close": 1.0, "volume": 1.0, "trade_count": 1, "price_change_1m": None}
    ])
    assert n == 1
    sql = cur.execute.call_args[0][0]
    assert "ON CONFLICT (symbol, window_start) DO UPDATE" in sql
    assert upsert_candles(conn, []) == 0


def test_ensure_tables_runs_ddl():
    conn, cur = MagicMock(), MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    ensure_tables(conn)
    assert "market_1m" in cur.execute.call_args[0][0]


def test_engine_metrics_counters_and_latency():
    engine_metrics.reset()
    engine_metrics.note_event("BTCUSDT", 1_000)
    engine_metrics.note_event("BTCUSDT", 2_000)
    stats = engine_metrics.note_candle("BTCUSDT", 60, 100.0, now=130.0)
    assert stats["processing_latency_s"] == 10.0  # 130 - (60+60)
    snap = engine_metrics.snapshot()
    assert snap["events_processed_total"] == 2
    assert snap["windows_created_total"] == 1
    assert snap["last_event"] == {"symbol": "BTCUSDT", "timestamp_ms": 2000}
    assert snap["last_ohlcv"]["BTCUSDT"] == {"window_start": 60, "close": 100.0}
    assert snap["max_processing_latency_s"] == 10.0


def test_engine_metrics_dump(tmp_path):
    engine_metrics.reset()
    engine_metrics.note_event("ETHUSDT", 5_000)
    path = str(tmp_path / "m.json")
    assert engine_metrics.dump(path) is True
    import orjson

    assert orjson.loads(Path(path).read_bytes())["events_processed_total"] == 1
    engine_metrics.reset()
