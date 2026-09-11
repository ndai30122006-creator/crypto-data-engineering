"""Fetch RSS feeds đồng thời bằng httpx + feedparser."""
import asyncio
from pathlib import Path

import feedparser
import httpx
import yaml

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


def load_feeds() -> dict[str, str]:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f).get("rss_feeds", {}) or dict(DEFAULT_FEEDS)
    return dict(DEFAULT_FEEDS)


async def _fetch_one(
    client: httpx.AsyncClient, source: str, url: str
) -> list[tuple[str, dict]]:
    resp = await client.get(url)
    resp.raise_for_status()
    feed = feedparser.parse(sanitize_feed_xml(resp.content))
    if feed.bozo and not feed.entries:
        raise ValueError(f"parse failed: {feed.bozo_exception}")
    return [(source, entry) for entry in feed.entries]


async def fetch_all(timeout: int = 30) -> tuple[list[tuple[str, dict]], list[str]]:
    """Trả về (entries, errors). Mỗi entry là (source_name, feedparser_entry)."""
    feeds = load_feeds()
    headers = {"User-Agent": "crypto-data-platform/1.0"}
    async with httpx.AsyncClient(
        headers=headers, follow_redirects=True, timeout=timeout
    ) as client:
        results = await asyncio.gather(
            *[_fetch_one(client, name, url) for name, url in feeds.items()],
            return_exceptions=True,
        )
    items: list[tuple[str, dict]] = []
    errors: list[str] = []
    for (name, _), result in zip(feeds.items(), results):
        if isinstance(result, Exception):
            errors.append(f"[{name}] {type(result).__name__}: {result}")
        else:
            items.extend(result)
    return items, errors
