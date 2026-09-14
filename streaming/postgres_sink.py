"""Sink Postgres cho market_1m: ensure schema + upsert idempotent.

Upsert theo PK (symbol, window_start): replay Kafka / restart engine
đều idempotent. Counters module cho metrics (thấy ở engine dump).
"""
import time
from datetime import UTC, datetime

_counters = {
    "sink_upserted_total": 0,
    "sink_failures_total": 0,
    "last_sink_latency_s": 0.0,
    "max_sink_latency_s": 0.0,
}


def snapshot_metrics() -> dict:
    return dict(_counters)


def reset_metrics() -> None:
    for key in _counters:
        _counters[key] = 0.0 if key.endswith("_s") else 0

DDL_MARKET_1M = """
CREATE TABLE IF NOT EXISTS market_1m (
    symbol VARCHAR(20),
    window_start TIMESTAMPTZ,
    open NUMERIC(20,8), high NUMERIC(20,8),
    low NUMERIC(20,8),  close NUMERIC(20,8),
    volume NUMERIC(30,12),
    trade_count INTEGER,
    price_change_1m NUMERIC(10,4),
    PRIMARY KEY (symbol, window_start)
);
"""

UPSERT_1M = """
INSERT INTO market_1m
    (symbol, window_start, open, high, low, close, volume, trade_count, price_change_1m)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, window_start) DO UPDATE SET
    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
    close = EXCLUDED.close, volume = EXCLUDED.volume,
    trade_count = EXCLUDED.trade_count, price_change_1m = EXCLUDED.price_change_1m;
"""


def ensure_tables(conn) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(DDL_MARKET_1M)


def to_row(candle: dict) -> tuple:
    """Chuẩn hoá 1 nến (window_start epoch seconds) → tuple INSERT."""
    return (
        candle["symbol"],
        datetime.fromtimestamp(candle["window_start"], tz=UTC),
        candle["open"],
        candle["high"],
        candle["low"],
        candle["close"],
        candle["volume"],
        candle["trade_count"],
        candle.get("price_change_1m"),
    )


def upsert_candles(conn, candles: list[dict]) -> int:
    """Upsert nhiều nến, trả về số dòng đã viết. Lỗi → đếm failures + raise."""
    if not candles:
        return 0
    started = time.time()
    try:
        with conn, conn.cursor() as cur:
            for candle in candles:
                cur.execute(UPSERT_1M, to_row(candle))
    except Exception:
        _counters["sink_failures_total"] += 1
        raise
    latency = time.time() - started
    _counters["sink_upserted_total"] += len(candles)
    _counters["last_sink_latency_s"] = round(latency, 3)
    _counters["max_sink_latency_s"] = round(max(_counters["max_sink_latency_s"], latency), 3)
    return len(candles)
