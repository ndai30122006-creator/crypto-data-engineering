"""Gọi CoinGecko API lấy top coins (free, không cần key)."""
import time

import httpx

BASE_URL = "https://api.coingecko.com/api/v3/coins/markets"


def fetch_markets(
    vs_currency: str = "usd", per_page: int = 50, retries: int = 3
) -> list[dict]:
    """Trả về raw JSON list. Tự retry với backoff khi 429/5xx/timeout."""
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
    }
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            resp = httpx.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise ValueError(f"unexpected response: {str(data)[:200]}")
            return data
        except (httpx.TimeoutException, httpx.HTTPStatusError, ValueError) as exc:
            last_error = exc
            time.sleep(2**attempt)
    raise RuntimeError(f"fetch_markets failed after {retries} attempts: {last_error}")
