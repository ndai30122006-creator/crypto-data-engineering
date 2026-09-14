"""Step 2.4 — regression event-time: out-of-order, late, duplicate,
multiple symbols. Engine live + DB thật, skip khi thiếu infra."""
from integration.helpers import (
    as_floats,
    cleanup,
    make_producer,
    needs_stack,
    publish_trades,
    wait_candle,
)


@needs_stack
def test_out_of_order_events():
    """Arrival A→B→C nhưng event-time C→A→B: nến đúng theo event-time."""
    sym = "E2ETO"
    trades = [(105.0, 10, 1.0), (98.0, 20, 1.0), (100.0, 5, 1.0)]  # A, B, C
    cleanup(sym)
    producer = make_producer()
    try:
        publish_trades(producer, sym, trades)
        row = wait_candle(sym, len(trades))
        assert row is not None, "engine không ghi nến trong 60s"
        _, _, o, h, low, c, _, _ = as_floats(row)
        assert (o, h, low, c) == (100.0, 105.0, 98.0, 98.0)
    finally:
        producer.close()
        cleanup(sym)


@needs_stack
def test_late_event():
    """Nến đã có, event muộn tới sau: high/low/volume/count update, open giữ."""
    sym = "E2ETL"
    cleanup(sym)
    producer = make_producer()
    try:
        publish_trades(producer, sym, [(100.0, 5, 1.0), (105.0, 10, 1.0)])
        row = wait_candle(sym, 2)
        assert row is not None and float(row[2]) == 100.0
        publish_trades(producer, sym, [(90.0, 30, 2.0)])  # late
        row = wait_candle(sym, 3)
        assert row is not None, "nến không update sau late event"
        _, _, o, h, low, c, vol, cnt = as_floats(row)
        assert o == 100.0 and (h, low, c) == (105.0, 90.0, 90.0)
        assert vol >= 4.0 and cnt >= 3
    finally:
        producer.close()
        cleanup(sym)


@needs_stack
def test_duplicate_event():
    """Publish lại y hệt: OHLC không đổi (upsert idempotent)."""
    sym = "E2ETD"
    trades = [(100.0, 1, 1.0), (110.0, 9, 1.0)]
    cleanup(sym)
    producer = make_producer()
    try:
        publish_trades(producer, sym, trades)
        row = wait_candle(sym, len(trades))
        assert row is not None
        baseline = as_floats(row)[2:6]
        publish_trades(producer, sym, trades)  # trùng y hệt
        import time

        time.sleep(15)  # cho engine xử lý lại hết batch trùng
        row = wait_candle(sym, len(trades))
        assert row is not None
        assert as_floats(row)[2:6] == baseline
    finally:
        producer.close()
        cleanup(sym)


@needs_stack
def test_multiple_symbols():
    """2 symbols đan xen arrival: mỗi nến đúng riêng, không lẫn nhau."""
    s1, s2 = "E2ETM1", "E2ETM2"
    cleanup(s1)
    cleanup(s2)
    producer = make_producer()
    try:
        publish_trades(producer, s1, [(10.0, 1, 1.0)])
        publish_trades(producer, s2, [(5.0, 2, 1.0)])
        publish_trades(producer, s1, [(20.0, 3, 1.0)])
        publish_trades(producer, s2, [(7.0, 4, 1.0)])
        r1 = wait_candle(s1, 2)
        r2 = wait_candle(s2, 2)
        assert r1 is not None and r2 is not None
        assert as_floats(r1)[2:6] == (10.0, 20.0, 10.0, 20.0)
        assert as_floats(r2)[2:6] == (5.0, 7.0, 5.0, 7.0)
    finally:
        producer.close()
        cleanup(s1)
        cleanup(s2)
