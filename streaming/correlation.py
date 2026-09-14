"""News ↔ Market correlation thuần Python (mirror query 8 trong queries.sql).

Dùng cho E2E test offline: cùng semantics với SQL (LAG theo symbol,
join ±window_minutes với published_at, lọc |change| > threshold).
"""
from datetime import UTC, datetime


def _as_utc(value) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=UTC)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except ValueError:
            return None
    return None


def price_changes(candles: list[dict]) -> list[dict]:
    """Thêm price_change_1m (LAG close theo symbol, nến đầu → None)."""
    out = []
    prev_close: dict[str, float] = {}
    for c in sorted(candles, key=lambda c: (c["symbol"], _as_utc(c["window_start"]))):
        prev = prev_close.get(c["symbol"])
        change = (c["close"] - prev) / prev * 100 if prev else None
        prev_close[c["symbol"]] = c["close"]
        out.append({**c, "price_change_1m": change})
    return out


def find_correlations(
    news: list[dict],
    candles: list[dict],
    window_minutes: int = 10,
    threshold: float = 1.0,
) -> list[dict]:
    """Tin trong ±window_minutes quanh nến có |change| > threshold."""
    moves = [c for c in price_changes(candles) if c["price_change_1m"] is not None]
    dated = [(n, _as_utc(n.get("published_at"))) for n in news]
    dated = [(n, ts) for n, ts in dated if ts is not None]
    hits = []
    for candle in moves:
        if abs(candle["price_change_1m"]) <= threshold:
            continue
        w_start = _as_utc(candle["window_start"])
        for article, pub in dated:
            if abs((w_start - pub).total_seconds()) <= window_minutes * 60:
                hits.append(
                    {
                        "title": article.get("title"),
                        "symbol": candle["symbol"],
                        "window_start": w_start,
                        "price_change_1m": round(candle["price_change_1m"], 4),
                    }
                )
    return sorted(hits, key=lambda h: h["window_start"], reverse=True)
