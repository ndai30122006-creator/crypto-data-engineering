"""RSS client bọc thành resource: config + fetch gom 1 chỗ."""
import asyncio
import random
from pathlib import Path

import feedparser
import httpx
import yaml
from dagster import ConfigurableResource, get_dagster_logger

from dagster_project.news.parser import sanitize_feed_xml

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "config.yaml"

DEFAULT_FEEDS = {
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph": "https://cointelegraph.com/rss",
    "bitcoin_magazine": "https://bitcoinmagazine.com/feed",
    "google_news_crypto": (
        "https://news.google.com/rss/search"
        "?q=crypto+bitcoin&hl=en-US&gl=US&ceid=US%3Aen"
    ),
}


class RSSFeedResource(ConfigurableResource):
    """Client đọc RSS. Asset chỉ gọi fetch_raw(), không biết httpx/async."""

    feeds: dict[str, str] | None = None
    timeout: int = 30
    user_agent: str = "crypto-data-platform/1.0"
    retries: int = 3

    def _feeds(self) -> dict[str, str]:
        if self.feeds:
            return self.feeds
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, encoding="utf-8") as f:
                return yaml.safe_load(f).get("rss_feeds", {}) or dict(DEFAULT_FEEDS)
        return dict(DEFAULT_FEEDS)

    async def _fetch_one(
        self, client: httpx.AsyncClient, source: str, url: str
    ) -> list[tuple[str, dict]]:
        """Fetch 1 feed, retry/backoff khi timeout/mất mạng/5xx (fail sau N lần)."""
        last_error: Exception | None = None
        for attempt in range(self.retries):
            try:
                resp = await client.get(url)
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                # 4xx (trừ 429) là lỗi client, retry không khỏi → fail nhanh.
                if exc.response.status_code != 429 and exc.response.status_code < 500:
                    raise
                last_error = exc
            except httpx.HTTPError as exc:
                last_error = exc
            else:
                feed = feedparser.parse(sanitize_feed_xml(resp.content))
                if feed.bozo and not feed.entries:
                    raise ValueError(f"parse failed: {feed.bozo_exception}")
                return [(source, entry) for entry in feed.entries]
            get_dagster_logger().warning(
                f"RSS [{source}] attempt {attempt + 1}/{self.retries}: {last_error}"
            )
            await asyncio.sleep(2**attempt + random.uniform(0, 1))
        raise RuntimeError(f"RSS [{source}] failed after {self.retries}: {last_error}")

    def fetch_raw(self) -> tuple[list[tuple[str, dict]], list[str]]:
        """Trả về (entries, errors). Mỗi entry là (source, feedparser_entry)."""
        feeds = self._feeds()

        async def _run() -> list:
            headers = {"User-Agent": self.user_agent}
            async with httpx.AsyncClient(
                headers=headers, follow_redirects=True, timeout=self.timeout
            ) as client:
                return await asyncio.gather(
                    *[self._fetch_one(client, n, u) for n, u in feeds.items()],
                    return_exceptions=True,
                )

        items: list[tuple[str, dict]] = []
        errors: list[str] = []
        for (name, _), result in zip(feeds.items(), asyncio.run(_run())):
            if isinstance(result, Exception):
                errors.append(f"[{name}] {type(result).__name__}: {result}")
            else:
                items.extend(result)
        get_dagster_logger().info(
            f"RSS fetched {len(items)} entries from {len(feeds)} feeds "
            f"({len(errors)} feed errors)"
        )
        return items, errors
