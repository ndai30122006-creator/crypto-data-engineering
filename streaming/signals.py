"""Detect closed 5-minute volume spikes across an hourly lookback."""

import math
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

THRESHOLD = 3.0
RECENT_N = 5
BASELINE_N = 60
LOOKBACK_MINUTES = 60
HISTORY_MINUTES = BASELINE_N + RECENT_N + LOOKBACK_MINUTES


def _seconds(value) -> int:
    if isinstance(value, datetime):
        return int((value if value.tzinfo else value.replace(tzinfo=UTC)).timestamp())
    return int(value)


def detect_volume_spike(
    candles: list[dict], *, now: datetime | None = None
) -> list[dict]:
    """Require 65 consecutive closed candles; scan every endpoint in the last hour."""
    cutoff = int((now or datetime.now(UTC)).timestamp()) // 60 * 60
    by_symbol: dict[str, dict[int, dict]] = {}
    for c in candles:
        start = _seconds(c["window_start"])
        volume = float(c["volume"])
        if start % 60 or start >= cutoff or not math.isfinite(volume) or volume < 0:
            continue
        rows = by_symbol.setdefault(c["symbol"], {})
        if start in rows and rows[start] != c:
            raise ValueError("conflicting candles for one symbol/minute")
        rows[start] = c
    signals = []
    for symbol, rows in sorted(by_symbol.items()):
        for end in sorted(rows):
            if end < cutoff - LOOKBACK_MINUTES * 60:
                continue
            starts = range(end - (BASELINE_N + RECENT_N - 1) * 60, end + 1, 60)
            if any(start not in rows for start in starts):
                continue
            window = [rows[start] for start in starts]
            recent_vol = sum(float(c["volume"]) for c in window[-RECENT_N:])
            baseline_5m = (
                sum(float(c["volume"]) for c in window[:BASELINE_N])
                / BASELINE_N
                * RECENT_N
            )
            if baseline_5m <= 0 or recent_vol / baseline_5m <= THRESHOLD:
                continue
            signals.append(
                {
                    "symbol": symbol,
                    "signal_type": "VOLUME_SPIKE",
                    "window_start": rows[end]["window_start"],
                    "details": {
                        "recent_volume_5m": recent_vol,
                        "baseline_5m": round(baseline_5m, 8),
                        "ratio": round(recent_vol / baseline_5m, 2),
                    },
                }
            )
    return signals


def detect_price_spike(
    candles: list[dict], *, now: datetime | None = None, threshold_pct: float = 1.0
) -> list[dict]:
    """Absolute close change over five minutes, requiring six closed candles.

    Every completed endpoint in the last hour is examined. Decimal arithmetic
    includes an exact threshold (e.g. 100 → 101 is exactly +1%).
    """
    try:
        threshold = Decimal(str(threshold_pct))
    except InvalidOperation:
        raise ValueError("PRICE_SPIKE_THRESHOLD_PCT must be a number") from None
    if not threshold.is_finite() or threshold <= 0:
        raise ValueError("PRICE_SPIKE_THRESHOLD_PCT must be finite and positive")
    cutoff = int((now or datetime.now(UTC)).timestamp()) // 60 * 60
    by_symbol = {}
    for candle in candles:
        try:
            start = _seconds(candle["window_start"])
            close = Decimal(str(candle.get("close")))
            valid = close > 0 and math.isfinite(float(close))
        except (TypeError, ValueError, InvalidOperation, OverflowError):
            continue
        if not valid or start % 60 or start >= cutoff:
            continue
        rows = by_symbol.setdefault(candle["symbol"], {})
        if start in rows and rows[start][1] != close:
            raise ValueError("conflicting closes for one symbol/minute")
        rows[start] = (candle["window_start"], close)
    signals = []
    for symbol, rows in sorted(by_symbol.items()):
        for end in sorted(rows):
            if end < cutoff - LOOKBACK_MINUTES * 60:
                continue
            if any(start not in rows for start in range(end - 5 * 60, end + 1, 60)):
                continue
            base, latest = rows[end - 5 * 60][1], rows[end][1]
            change = (latest - base) / base * 100
            if not math.isfinite(float(change)) or abs(change) < threshold:
                continue
            signals.append({
                "symbol": symbol, "signal_type": "PRICE_SPIKE",
                "window_start": rows[end][0],
                "details": {"price_change_5m_pct": float(change),
                            "baseline_close": float(base), "close": float(latest),
                            "direction": "up" if change > 0 else "down",
                            "threshold_pct": float(threshold)},
            })
    return signals
