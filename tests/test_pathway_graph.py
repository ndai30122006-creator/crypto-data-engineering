"""Real Pathway graph regression tests, executed by the Linux CI runner."""

import pytest

pw = pytest.importorskip("pathway", reason="Pathway requires Linux/macOS")

from streaming.pathway_pipeline import (  # noqa: E402
    TradeSchema,
    build_candles,
    decode_messages,
)


def test_real_graph_dedup_tie_and_multi_symbol():
    source = pw.debug.table_from_markdown(
        """
        symbol | trade_id | price | quantity | timestamp
        BTCUSDT | 2 | 10.0 | 2.0 | 1757578861000
        BTCUSDT | 1 | 9.0 | 1.0 | 1757578861000
        BTCUSDT | 2 | 10.0 | 2.0 | 1757578861000
        ETHUSDT | 1 | 50.0 | 1.0 | 1757578861000
    """,
        schema=TradeSchema,
    )
    result = pw.debug.table_to_pandas(build_candles(source)).to_dict("records")
    by_symbol = {row["symbol"]: row for row in result}
    btc = by_symbol["BTCUSDT"]
    assert (btc["open"], btc["close"], btc["volume"], btc["trade_count"]) == (
        9.0,
        10.0,
        3.0,
        2,
    )
    assert by_symbol["ETHUSDT"]["trade_count"] == 1


def test_real_graph_rejects_invalid_prices():
    source = pw.debug.table_from_markdown(
        """
        symbol | trade_id | price | quantity | timestamp
        BTCUSDT | 1 | -1.0 | 1.0 | 1757578861000
        BTCUSDT | 2 | 10.0 | 1.0 | 1757578861000
    """,
        schema=TradeSchema,
    )
    rows = pw.debug.table_to_pandas(build_candles(source)).to_dict("records")
    assert len(rows) == 1 and rows[0]["trade_count"] == 1 and rows[0]["open"] == 10.0


def test_raw_boundary_survives_invalid_json_and_schema():
    import orjson

    class RawSchema(pw.Schema):
        data: bytes

    payload = {
        "symbol": "BTCUSDT",
        "trade_id": 1,
        "price": 10.0,
        "quantity": 1.0,
        "timestamp": 1757578861000,
    }
    source = pw.debug.table_from_rows(
        RawSchema, [(b"not-json",), (b"{}",), (orjson.dumps(payload),)]
    )
    rows = pw.debug.table_to_pandas(build_candles(decode_messages(source))).to_dict(
        "records"
    )
    assert len(rows) == 1 and rows[0]["trade_count"] == 1
