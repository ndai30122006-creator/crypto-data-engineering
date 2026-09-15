"""Phát hiện tín hiệu từ nến 1m (pure Python — test offline được).

Hiện tại: VOLUME_SPIKE khi volume 5 phút gần nhất vượt trung bình giờ
quá THRESHOLD lần. Chạy batch theo giờ trong Dagster (đủ nhanh, đơn giản
hơn stateful trong engine).
"""
THRESHOLD = 3.0
RECENT_N = 5
BASELINE_N = 60


def detect_volume_spike(candles: list[dict]) -> list[dict]:
    """candles: [{symbol, window_start, volume}] (mọi symbol lẫn nhau).

    Trả về signals [{symbol, signal_type, window_start, details}].
    Cần >= RECENT_N + BASELINE_N nến/symbol mới đủ baseline, thiếu thì bỏ qua.
    """
    by_symbol: dict[str, list[dict]] = {}
    for c in candles:
        by_symbol.setdefault(c["symbol"], []).append(c)
    signals = []
    for symbol, rows in by_symbol.items():
        ordered = sorted(rows, key=lambda c: c["window_start"])
        if len(ordered) < RECENT_N + BASELINE_N:
            continue
        recent = ordered[-RECENT_N:]
        baseline = ordered[-(RECENT_N + BASELINE_N):-RECENT_N]
        recent_vol = sum(float(c["volume"]) for c in recent)
        base_avg = sum(float(c["volume"]) for c in baseline) / len(baseline)
        baseline_5m = base_avg * RECENT_N
        if baseline_5m > 0 and recent_vol / baseline_5m > THRESHOLD:
            signals.append(
                {
                    "symbol": symbol,
                    "signal_type": "VOLUME_SPIKE",
                    "window_start": recent[-1]["window_start"],
                    "details": {
                        "recent_volume_5m": recent_vol,
                        "baseline_5m": round(baseline_5m, 8),
                        "ratio": round(recent_vol / baseline_5m, 2),
                    },
                }
            )
    return signals
