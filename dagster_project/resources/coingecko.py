"""CoinGecko API bọc thành resource: retry/backoff gom 1 chỗ."""
import time

import httpx
from dagster import ConfigurableResource, get_dagster_logger


class CoinGeckoResource(ConfigurableResource):
    """Client CoinGecko. Asset chỉ gọi fetch_markets(), không biết retry."""

    base_url: str = "https://api.coingecko.com/api/v3/coins/markets"
    vs_currency: str = "usd"
    per_page: int = 50
    timeout: int = 30
    retries: int = 3

    def fetch_markets(self) -> list[dict]:
        """Trả về raw JSON list. Tự retry với backoff khi 429/5xx/timeout."""
        params = {
            "vs_currency": self.vs_currency,
            "order": "market_cap_desc",
            "per_page": self.per_page,
            "page": 1,
        }
        last_error: Exception | None = None
        for attempt in range(self.retries):
            try:
                resp = httpx.get(self.base_url, params=params, timeout=self.timeout)
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, list):
                    # Cố ý ValueError (không phải TypeError): retry handler
                    # bên dưới bắt nó để thử lại khi API trả payload lạ.
                    raise ValueError(  # noqa: TRY004
                        f"unexpected response: {str(data)[:200]}"
                    )
                return data
            except (
                httpx.TimeoutException,
                httpx.HTTPStatusError,
                ValueError,
            ) as exc:
                last_error = exc
                get_dagster_logger().warning(
                    f"CoinGecko attempt {attempt + 1}/{self.retries} failed: {exc}"
                )
                time.sleep(2**attempt)
        raise RuntimeError(
            f"fetch_markets failed after {self.retries} attempts: {last_error}"
        )
