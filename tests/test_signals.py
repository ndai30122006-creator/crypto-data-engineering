"""Unit tests cho signals detector + asset (mock DB, offline)."""
from unittest.mock import MagicMock

from dagster import build_op_context

from dagster_project.assets.signals_assets import detected_signals
from streaming.signals import RECENT_N, detect_volume_spike


def _candles(symbol="BTCUSDT", n=65, spike_at_end=False):
    rows = [{"symbol": symbol, "window_start": i, "volume": 10.0} for i in range(n)]
    if spike_at_end:
        for c in rows[-RECENT_N:]:
            c["volume"] = 10.0 * 3 * RECENT_N + 1.0  # vượt ngưỡng 3x chắc chắn
    return rows


def test_no_signal_without_baseline():
    assert detect_volume_spike(_candles(n=10)) == []


def test_no_signal_flat_volume():
    assert detect_volume_spike(_candles(n=65)) == []


def test_volume_spike_detected():
    signals = detect_volume_spike(_candles(n=65, spike_at_end=True))
    assert len(signals) == 1
    sig = signals[0]
    assert sig["symbol"] == "BTCUSDT" and sig["signal_type"] == "VOLUME_SPIKE"
    assert sig["window_start"] == 64
    assert sig["details"]["ratio"] > 3.0


def test_multi_symbol_independent():
    rows = _candles("A", n=65, spike_at_end=True) + _candles("B", n=65)
    signals = detect_volume_spike(rows)
    assert [s["symbol"] for s in signals] == ["A"]


def test_detected_signals_asset_with_mock():
    """Asset gọi resource mock: trả signals, ghi DB mock."""
    mock_pg = MagicMock()
    mock_pg.fetch_candles.return_value = _candles(n=65, spike_at_end=True)
    mock_pg.insert_signals.return_value = 1
    result = detected_signals(build_op_context(), mock_pg)
    assert len(result) == 1
    mock_pg.insert_signals.assert_called_once()
    assert mock_pg.insert_signals.call_args[0][0][0]["signal_type"] == "VOLUME_SPIKE"
