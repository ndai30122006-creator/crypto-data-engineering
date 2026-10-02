from datetime import UTC, datetime

import pytest

from streaming.signals import detect_price_spike

NOW = datetime.fromtimestamp(120 * 60, UTC)


def candles(end=119, symbol="BTCUSDT", last=101):
    return [{"symbol": symbol, "window_start": i * 60,
             "close": last if i == end else 100} for i in range(end - 5, end + 1)]


@pytest.mark.parametrize("last,direction", [(101, "up"), (99, "down")])
def test_exact_threshold_rise_and_fall(last, direction):
    signals = detect_price_spike(candles(last=last), now=NOW)
    assert len(signals) == 1
    assert signals[0]["details"]["direction"] == direction
    assert abs(signals[0]["details"]["price_change_5m_pct"]) == 1


def test_middle_of_hour_multiple_symbols_and_repeated_rows():
    rows = candles(end=80) + candles(symbol="ETHUSDT", last=99)
    assert len(detect_price_spike(rows + rows, now=NOW)) == 2
    assert detect_price_spike(candles(end=59), now=NOW) == []


@pytest.mark.parametrize("last", [100, 100.99, 99.01])
def test_below_threshold(last):
    assert detect_price_spike(candles(last=last), now=NOW) == []


def test_missing_minute_and_partial_endpoint():
    rows = candles()
    assert detect_price_spike(rows[:2] + rows[3:], now=NOW) == []
    assert detect_price_spike(candles(end=120), now=NOW) == []
    assert detect_price_spike(candles(), now=NOW, threshold_pct=2) == []


@pytest.mark.parametrize("close", [None, 0, -1, "NaN", "Infinity", "bad"])
def test_invalid_intermediate_price_breaks_window(close):
    rows = candles()
    rows[3]["close"] = close
    assert detect_price_spike(rows, now=NOW) == []


@pytest.mark.parametrize("threshold", [0, -1, "NaN", "Infinity", "bad"])
def test_invalid_threshold_rejected(threshold):
    with pytest.raises((ValueError, ArithmeticError)):
        detect_price_spike(candles(), now=NOW, threshold_pct=threshold)


def test_conflicting_close_rejected():
    rows = candles()
    with pytest.raises(ValueError, match="conflicting"):
        detect_price_spike(rows + [{**rows[0], "close": 200}], now=NOW)


def test_asset_writes_price_signal(monkeypatch):
    from unittest.mock import MagicMock

    from dagster import build_op_context

    from dagster_project.assets.signals_assets import detected_signals

    current = int(datetime.now(UTC).timestamp()) // 60 - 1
    resource = MagicMock()
    resource.fetch_candles.return_value = [{**r, "volume": 10}
                                          for r in candles(end=current)]
    resource.insert_signals.return_value = 1
    monkeypatch.setenv("PRICE_SPIKE_THRESHOLD_PCT", "1")
    result = detected_signals(build_op_context(), resource)
    assert result[0]["signal_type"] == "PRICE_SPIKE"
    resource.insert_signals.assert_called_once_with(result)
