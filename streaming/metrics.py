"""Metrics engine Pathway (pure Python — test offline được, không import pathway).

- events_processed_total: số trades engine đã thấy (subscribe bảng nguồn).
- windows_created_total: số (symbol, window) distinct đã emit.
- processing latency: giờ ghi nến − cuối window (giây).
- last_event / last_ohlcv: trade và nến mới nhất theo từng symbol.
"""
import time
from pathlib import Path

import orjson

_started_at = time.time()
_events_processed_total = 0
_windows: set[tuple[str, int]] = set()
_last_event: dict = {}
_last_ohlcv: dict[str, dict] = {}
_last_latency_s = 0.0
_max_latency_s = 0.0


def reset() -> None:
    """Reset counters (cho tests)."""
    global _events_processed_total, _last_latency_s, _max_latency_s
    global _started_at
    _events_processed_total = 0
    _windows.clear()
    _last_event.clear()
    _last_ohlcv.clear()
    _last_latency_s = 0.0
    _max_latency_s = 0.0
    _started_at = time.time()


def note_event(symbol: str, ts_ms: int) -> None:
    """Ghi nhận 1 trade engine đã xử lý."""
    global _events_processed_total
    _events_processed_total += 1
    _last_event.clear()
    _last_event.update({"symbol": symbol, "timestamp_ms": int(ts_ms)})


def note_candle(symbol: str, window_start: int, close: float,
                now: float | None = None) -> dict:
    """Ghi nhận 1 nến emit. Trả về latency vừa đo (giờ ghi − cuối window)."""
    global _last_latency_s, _max_latency_s
    t = now if now is not None else time.time()
    _windows.add((symbol, int(window_start)))
    _last_ohlcv[symbol] = {"window_start": int(window_start), "close": close}
    latency = max(0.0, t - (int(window_start) + 60))
    _last_latency_s = round(latency, 3)
    _max_latency_s = round(max(_max_latency_s, latency), 3)
    return {"processing_latency_s": _last_latency_s}


def snapshot() -> dict:
    """Counters hiện tại (cho metrics file / status)."""
    return {
        "events_processed_total": _events_processed_total,
        "windows_created_total": len(_windows),
        "last_processing_latency_s": _last_latency_s,
        "max_processing_latency_s": _max_latency_s,
        "last_event": dict(_last_event),
        "last_ohlcv": {s: dict(v) for s, v in _last_ohlcv.items()},
        "uptime_seconds": round(time.time() - _started_at, 1),
    }


def dump(path: str) -> bool:
    """Ghi snapshot ra JSON file (metrics script đọc qua docker exec)."""
    try:
        Path(path).write_bytes(orjson.dumps(snapshot()))
        return True
    except OSError:
        return False
