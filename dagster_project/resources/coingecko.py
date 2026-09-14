"""CoinGecko API bọc thành resource: retry/backoff gom 1 chỗ."""
import random
import time

import httpx
from dagster import ConfigurableResource, get_dagster_logger

# Chỉ retry khi đáng: 429 + 5xx. 4xx khác là lỗi request → fail nhanh.
RETRYABLE_STATUS = {429, *range(500, 600)}


class CoinGeckoResource(ConfigurableResource):
    """Client CoinGecko. Asset chỉ gọi fetch_markets(), không biết retry."""

    base_url: str = "https://api.coingecko.com/api/v3/coins/markets"
    vs_currency: str = "usd"
    per_page: int = 50
    timeout: int = 30
    retries: int = 3

    def fetch_markets(self) -> list[dict]:
        """Trả về raw JSON list. Retry 429/5xx/timeout/mất mạng (có jitter,
        tôn trọng Retry-After). 4xx khác fail nhanh, không retry vô ích."""
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
            except httpx.TransportError as exc:
                # Timeout/mất mạng/reset kết nối: luôn retry được.
                last_error = exc
            else:
                if resp.status_code in RETRYABLE_STATUS:
                    last_error = httpx.HTTPStatusError(
                        f"{resp.status_code} {resp.reason_phrase}",
                        request=resp.request,
                        response=resp,
                    )
                    wait = self._retry_after(resp) or (
                        2**attempt + random.uniform(0, 1)
                    )
                    get_dagster_logger().warning(
                        f"CoinGecko attempt {attempt + 1}/{self.retries} "
                        f"failed ({resp.status_code}), retry in {wait:.1f}s"
                    )
                    time.sleep(wait)
                    continue
                try:
                    resp.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    raise RuntimeError(f"CoinGecko request failed: {exc}") from exc
                try:
                    data = resp.json()
                    if not isinstance(data, list):
                        # Cố ý ValueError (không phải TypeError): payload lạ
                        # có thể do lỗi tạm thời → retry như timeout.
                        raise ValueError(  # noqa: TRY004
                            f"unexpected response: {str(data)[:200]}"
                        )
                except ValueError as exc:
                    last_error = exc
                    get_dagster_logger().warning(
                        f"CoinGecko attempt {attempt + 1}/{self.retries} "
                        f"bad payload, retrying: {exc}"
                    )
                    time.sleep(2**attempt + random.uniform(0, 1))
                    continue
                return data
            get_dagster_logger().warning(
                f"CoinGecko attempt {attempt + 1}/{self.retries} failed: {last_error}"
            )
            time.sleep(2**attempt + random.uniform(0, 1))
        raise RuntimeError(
            f"fetch_markets failed after {self.retries} attempts: {last_error}"
        )

    @staticmethod
    def _retry_after(resp: httpx.Response) -> float | None:
        """Đọc header Retry-After (giây) khi bị 429, sai format → None."""
        try:
            return max(0.0, float(resp.headers.get("Retry-After", "")))
        except (TypeError, ValueError):
            return None
