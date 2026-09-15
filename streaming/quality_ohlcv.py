"""Kiểm tra chất lượng nến OHLCV (pure Python — test offline được).

Luật: high >= low/open/close, low <= open/close, volume >= 0,
trade_count > 0. Vi phạm → bad records ghi vào data_quality_errors
(pipeline="ohlcv") thay vì drop lặng lẽ.
"""
OHLCV_RULES = ("high_low", "high_open_close", "low_open_close", "volume", "count")


def check_ohlcv(candles: list[dict]) -> list[dict]:
    """Trả về list violations [{symbol, window_start, rule, detail}].

    Candle cần keys: symbol, window_start, open, high, low, close,
    volume, trade_count. Thiếu key → violation `missing_field`.
    """
    violations = []
    for c in candles:
        missing = [k for k in ("symbol", "window_start", "open", "high", "low",
                               "close", "volume", "trade_count") if c.get(k) is None]
        if missing:
            violations.append(_v(c, "missing_field", f"thiếu {missing}"))
            continue
        o, h, low, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        vol, cnt = float(c["volume"]), c["trade_count"]
        if not h >= low:
            violations.append(_v(c, "high_low", f"high={h} < low={low}"))
        if not (h >= o and h >= cl):
            violations.append(_v(c, "high_open_close", f"high={h} ngoài [open={o}, close={cl}]"))
        if not (low <= o and low <= cl):
            violations.append(_v(c, "low_open_close", f"low={low} ngoài [open={o}, close={cl}]"))
        if vol < 0:
            violations.append(_v(c, "volume", f"volume={vol} < 0"))
        if not (isinstance(cnt, int) and cnt > 0):
            violations.append(_v(c, "count", f"trade_count={cnt!r} phải int > 0"))
    return violations


def _v(candle: dict, rule: str, detail: str) -> dict:
    return {
        "symbol": candle.get("symbol"),
        "window_start": str(candle.get("window_start")),
        "rule": rule,
        "detail": detail,
    }


def to_errors(violations: list[dict]) -> list[dict]:
    """Đổi violations sang format insert_errors (bad-record pattern chung)."""
    out = []
    for v in violations:
        out.append({
            "pipeline": "ohlcv",
            "payload": {k: v[k] for k in ("symbol", "window_start", "rule", "detail")},
            "error": f"ohlcv/{v['rule']}: {v['detail']}",
        })
    return out
