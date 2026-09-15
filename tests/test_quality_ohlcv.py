"""Unit tests cho OHLCV quality + quarantine asset (mock, offline)."""
from unittest.mock import MagicMock

from dagster import build_op_context

from dagster_project.assets.quality_assets import quarantine_ohlcv
from streaming.quality_ohlcv import check_ohlcv, to_errors


def _candle(**over):
    c = {"symbol": "BTCUSDT", "window_start": 60, "open": 100.0, "high": 110.0,
         "low": 90.0, "close": 105.0, "volume": 5.0, "trade_count": 10}
    c.update(over)
    return c


def test_clean_candle_no_violations():
    assert check_ohlcv([_candle()]) == []


def test_bad_high_low():
    v = check_ohlcv([_candle(high=80.0)])
    rules = {x["rule"] for x in v}
    assert {"high_low", "high_open_close"} <= rules


def test_negative_volume_and_bad_count():
    v = check_ohlcv([_candle(volume=-1.0, trade_count=0)])
    assert {x["rule"] for x in v} == {"volume", "count"}


def test_missing_field():
    v = check_ohlcv([{"symbol": "B"}])
    assert v and v[0]["rule"] == "missing_field"


def test_to_errors_format():
    errs = to_errors(check_ohlcv([_candle(high=80.0)]))
    assert errs and all(e["pipeline"] == "ohlcv" for e in errs)
    assert all({"pipeline", "payload", "error"} <= set(e) for e in errs)


def test_quarantine_asset_writes_errors():
    """Asset quét mock candles → ghi errors qua resource mock."""
    mock_pg = MagicMock()
    mock_pg.fetch_ohlcv.return_value = [_candle(), _candle(high=80.0)]
    mock_pg.insert_errors.return_value = None
    result = quarantine_ohlcv(build_op_context(), mock_pg)
    assert len(result) == 2  # high_low + high_open_close
    mock_pg.insert_errors.assert_called_once()
    assert mock_pg.insert_errors.call_args[0][0][0]["pipeline"] == "ohlcv"
