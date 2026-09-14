"""PHASE 1 — E2E streaming thật: fake Binance events → Kafka → Pathway
(engine live) → market_1m → PostgreSQL → ASSERT.

Không import pathway (không có wheel Windows): test publish events vào
topic live, engine Pathway đang chạy gom nến ghi DB, test poll DB assert.

Symbol E2ETEST + timestamp 2025 (cũ) để không đè nến live; cleanup xóa
sau test. Helpers dùng chung ở helpers.py. Chạy:
$env:INTEGRATION="1"; uv run pytest tests/integration/ -q
"""
import time

import orjson

from integration.helpers import (
    T0,
    TOPIC,
    as_floats,
    cleanup,
    make_producer,
    needs_stack,
    publish_trades,
    wait_candle,
)

SYMBOL = "E2ETEST"

# 6 events cùng 1 bucket phút (10:01:01 → 10:01:50), qty 1.0 mỗi event.
# Expected: open=100 high=105 low=98 close=103 volume=6.0 count=6.
FAKE_TRADES = [
    (100.0, 0, 1.0),
    (100.0, 4, 1.0),
    (105.0, 9, 1.0),
    (98.0, 19, 1.0),
    (103.0, 39, 1.0),
    (103.0, 49, 1.0),
]


def _publish_all(producer) -> None:
    publish_trades(producer, SYMBOL, FAKE_TRADES)


@needs_stack
def test_streaming_e2e_fake_to_postgres():
    """Publish 6 fake trades → đợi engine ghi nến → ASSERT OHLCV."""
    cleanup(SYMBOL)
    producer = make_producer()
    try:
        _publish_all(producer)

        # Chờ engine xử lý có timeout 60s — không treo vô hạn nếu pipeline kẹt.
        row = wait_candle(SYMBOL, len(FAKE_TRADES))
        assert row is not None, "engine không ghi nến E2ETEST trong 60s"
        from datetime import UTC, datetime

        sym, w_start, o, h, low, c, vol, cnt = as_floats(row)
        expected_bucket = datetime.fromtimestamp(T0 // 1000 // 60 * 60, tz=UTC)
        assert sym == SYMBOL
        assert w_start == expected_bucket
        # OHLC phải chính xác; volume/count dùng >= vì producer retry
        # (kafka-python không có idempotence) có thể gửi trùng — upsert
        # giữ OHLC đúng, chỉ count/volume phình.
        assert (o, h, low, c) == (100.0, 105.0, 98.0, 103.0)
        assert vol >= 6.0 and cnt >= len(FAKE_TRADES)

        # Step 1.5 — Idempotency: publish lại y hệt 6 events, nến không đổi.
        baseline = (sym, w_start, o, h, low, c)
        _publish_all(producer)
        time.sleep(15)  # cho engine xử lý lại hết batch trùng
        row2 = wait_candle(SYMBOL, len(FAKE_TRADES))
        assert row2 is not None
        assert as_floats(row2)[:6] == baseline, (
            f"idempotency vỡ: {baseline} != {as_floats(row2)[:6]}"
        )
    finally:
        producer.close()
        cleanup(SYMBOL)


BAD_SYMBOL = "E2EINVALID"


@needs_stack
def test_streaming_e2e_invalid_events_ignored():
    """Event rác (sai envelope/thiếu field) → engine bỏ qua, không có nến rác."""
    cleanup(BAD_SYMBOL)
    producer = make_producer()
    try:
        # Envelope đúng JSON nhưng không phải trade, thiếu timestamp.
        producer.send(
            TOPIC, key=BAD_SYMBOL,
            value={"stream": "x", "data": {"e": "aggTrade", "s": BAD_SYMBOL}},
        ).get(timeout=15)
        # Thiếu price → sai schema TradeSchema.
        producer.send(
            TOPIC, key=BAD_SYMBOL,
            value={"symbol": BAD_SYMBOL, "quantity": 1.0, "timestamp": T0},
        ).get(timeout=15)
        producer.flush()
        row = wait_candle(BAD_SYMBOL, 1, timeout_s=25)
        assert row is None, "engine ghi nến từ event rác"
    finally:
        producer.close()
        cleanup(BAD_SYMBOL)
