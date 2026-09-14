"""Gom trades thành nến OHLCV theo bucket phút (thuần Python).

Cùng 1 spec với engine Pathway trong pathway_pipeline.py — dùng để
unit-test logic và đối chiếu kết quả live.
Bucket = epoch_seconds // 60 * 60 (UTC).
"""
WINDOW_SECONDS = 60


def bucket_start(ts_ms: int, window_seconds: int = WINDOW_SECONDS) -> int:
    """Mốc bắt đầu bucket (epoch seconds) chứa timestamp ms."""
    return (ts_ms // 1000 // window_seconds) * window_seconds


def aggregate(trades: list[dict], window_seconds: int = WINDOW_SECONDS) -> list[dict]:
    """Gom list trades {(symbol, price, quantity, timestamp ms)} → OHLCV/bucket.

    open/close theo timestamp (sớm nhất/muộn nhất), high/low theo giá,
    volume = tổng quantity, price_change_1m để engine/live tính sau
    (cần nến trước — ở đây để None).
    """
    buckets: dict[tuple[str, int], list[dict]] = {}
    for t in trades:
        key = (t["symbol"], bucket_start(t["timestamp"], window_seconds))
        buckets.setdefault(key, []).append(t)
    rows = []
    for (symbol, start), ts in sorted(buckets.items()):
        ordered = sorted(ts, key=lambda t: t["timestamp"])
        prices = [t["price"] for t in ordered]
        rows.append(
            {
                "symbol": symbol,
                "window_start": start,
                "open": ordered[0]["price"],
                "high": max(prices),
                "low": min(prices),
                "close": ordered[-1]["price"],
                "volume": round(sum(t["quantity"] for t in ordered), 12),
                "trade_count": len(ordered),
            }
        )
    return rows
