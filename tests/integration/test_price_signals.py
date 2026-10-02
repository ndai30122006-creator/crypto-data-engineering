import uuid
from datetime import UTC, datetime, timedelta

from dagster_project.resources.postgres import PostgresResource
from integration.helpers import DATABASE_URL, needs_stack, pg
from streaming.postgres_sink import upsert_candles
from streaming.signals import detect_price_spike


@needs_stack
def test_real_price_read_and_idempotent_signal_insert():
    symbol = "PRICE" + uuid.uuid4().hex[:8].upper()
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    conn = pg()
    resource = PostgresResource(conn_str=DATABASE_URL, env="test")
    rows = [{"symbol": symbol, "window_start": int((now - timedelta(minutes=6-i)).timestamp()),
             "open": 100, "high": 101, "low": 100, "close": 101 if i == 5 else 100,
             "volume": 10, "trade_count": 1} for i in range(6)]
    try:
        upsert_candles(conn, rows)
        fetched = [r for r in resource.fetch_candles(minutes=10) if r["symbol"] == symbol]
        signals = detect_price_spike(fetched, now=now)
        assert len(signals) == 1
        assert resource.insert_signals(signals) == 1
        assert resource.insert_signals(signals) == 0
    finally:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market_1m WHERE symbol=%s", (symbol,))
            cur.execute("DELETE FROM signals_test WHERE symbol=%s", (symbol,))
        conn.close()
