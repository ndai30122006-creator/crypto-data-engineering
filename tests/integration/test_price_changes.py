import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from integration.helpers import needs_stack, pg
from streaming.postgres_sink import upsert_candles


@needs_stack
def test_derived_prices_contiguity_late_updates_and_partial_candle():
    symbol = "CHANGES" + uuid.uuid4().hex[:8].upper()
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    base = now - timedelta(minutes=16)
    conn = pg()
    rows = [{"symbol": symbol, "window_start": int((base + timedelta(minutes=i)).timestamp()),
             "open": 100+i, "high": 100+i, "low": 100+i, "close": 100+i,
             "volume": 1, "trade_count": 1} for i in range(17)]

    def changes(minute=15):
        with conn, conn.cursor() as cur:
            cur.execute("SELECT price_change_1m,price_change_5m,price_change_15m "
                        "FROM market_analytics_1m WHERE symbol=%s AND window_start=%s",
                        (symbol, base + timedelta(minutes=minute)))
            return cur.fetchone()

    try:
        upsert_candles(conn, rows)
        first = changes()
        assert float(first[0]) == pytest.approx((115/114-1)*100)
        assert float(first[1]) == pytest.approx((115/110-1)*100)
        assert first[2] == Decimal(15)
        assert changes(16) == (None, None, None)  # current minute hasn't closed
        assert changes(0) == (None, None, None)  # insufficient history
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market_1m WHERE symbol=%s AND window_start=%s",
                        (symbol, base + timedelta(minutes=7)))
        assert changes()[2] is None
        assert changes()[1] == first[1]  # six recent contiguous rows remain
        upsert_candles(conn, [{**rows[10], "close": 200}])
        assert float(changes()[1]) == pytest.approx(-42.5)  # late correction reflected
        for invalid in [0, -1, Decimal("NaN")]:
            upsert_candles(conn, [{**rows[12], "close": invalid}])
            assert changes()[1] is None  # invalid intermediate price breaks window
        upsert_candles(conn, [{**rows[12], "close": 112}])
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE market_1m SET window_start=window_start+INTERVAL '1 second' "
                        "WHERE symbol=%s AND window_start=%s",
                        (symbol, base + timedelta(minutes=12)))
        assert changes()[1] is None  # misaligned minute cannot masquerade as contiguous
    finally:
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market_1m WHERE symbol=%s", (symbol,))
        conn.close()
