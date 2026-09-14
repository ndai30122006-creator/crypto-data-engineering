"""Unit tests cho retry/backoff (mock httpx, offline).

RSS: lỗi thoáng qua → thử lại thành công; 404 → fail nhanh không retry.
CoinGecko: 429 (tôn trọng Retry-After) → thành công; 403 → fail nhanh.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from dagster_project.resources.coingecko import CoinGeckoResource
from dagster_project.resources.rss import RSSFeedResource

RSS_XML = (
    b"<rss version='2.0'><channel><item>"
    b"<title>T</title><link>https://a.com/1</link>"
    b"<description>body</description>"
    b"</item></channel></rss>"
)


def _resp(status: int, url: str = "https://x.test/rss", **kw) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request("GET", url), **kw)


class FakeClient:
    """Giả httpx.AsyncClient theo kịch bản outcomes (response hoặc Exception)."""

    def __init__(self, outcomes: list):
        self.outcomes = list(outcomes)
        self.calls = 0

    async def get(self, url):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_rss_retries_transient_then_succeeds():
    res = RSSFeedResource(feeds={"t": "https://x.test/rss"}, retries=3)
    err = httpx.ConnectError("down", request=httpx.Request("GET", "https://x.test/rss"))
    client = FakeClient([err, _resp(200, content=RSS_XML)])
    with patch("asyncio.sleep", new_callable=AsyncMock):
        items = asyncio.run(res._fetch_one(client, "t", "https://x.test/rss"))
    assert len(items) == 1 and items[0][0] == "t"
    assert client.calls == 2


def test_rss_404_fails_fast_without_retry():
    res = RSSFeedResource(feeds={"t": "https://x.test/rss"}, retries=3)
    resp = _resp(404)
    client = FakeClient([resp, _resp(200, content=RSS_XML)])
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(res._fetch_one(client, "t", "https://x.test/rss"))
    assert client.calls == 1  # không retry 404


def test_coingecko_429_then_success_respects_retry_after():
    res = CoinGeckoResource(retries=3)
    good = {"symbol": "btc", "name": "Bitcoin", "current_price": 1.0}
    sleeps = []
    with (
        patch(
            "dagster_project.resources.coingecko.httpx.get",
            side_effect=[
                _resp(429, headers={"Retry-After": "7"}),
                _resp(200, json=[good]),
            ],
        ) as mock_get,
        patch(
            "dagster_project.resources.coingecko.time.sleep",
            side_effect=lambda s: sleeps.append(s),
        ),
    ):
        assert res.fetch_markets() == [good]
    assert mock_get.call_count == 2
    assert sleeps and sleeps[0] == 7.0  # tôn trọng Retry-After


def test_coingecko_403_fails_fast():
    res = CoinGeckoResource(retries=3)
    with (
        patch(
            "dagster_project.resources.coingecko.httpx.get",
            return_value=_resp(403),
        ) as mock_get,
        patch("dagster_project.resources.coingecko.time.sleep") as mock_sleep,
        pytest.raises(RuntimeError),
    ):
        res.fetch_markets()
    assert mock_get.call_count == 1
    mock_sleep.assert_not_called()


def test_coingecko_gives_up_after_retries():
    res = CoinGeckoResource(retries=2)
    with (
        patch(
            "dagster_project.resources.coingecko.httpx.get",
            side_effect=httpx.ConnectError("down", request=MagicMock()),
        ),
        patch("dagster_project.resources.coingecko.time.sleep"),
        pytest.raises(RuntimeError, match="after 2 attempts"),
    ):
        res.fetch_markets()


def test_coingecko_retries_transport_errors():
    """ReadError (không phải Timeout/Connect) trước đây văng thẳng ra ngoài."""
    res = CoinGeckoResource(retries=2)
    good = {"symbol": "btc", "name": "Bitcoin", "current_price": 1.0}
    with (
        patch(
            "dagster_project.resources.coingecko.httpx.get",
            side_effect=[
                httpx.ReadError("reset", request=MagicMock()),
                _resp(200, json=[good]),
            ],
        ),
        patch("dagster_project.resources.coingecko.time.sleep"),
    ):
        assert res.fetch_markets() == [good]
