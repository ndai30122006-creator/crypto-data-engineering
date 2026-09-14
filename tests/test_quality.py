"""Unit tests cho quality checks (pure, offline)."""
from datetime import UTC, datetime, timedelta

from dagster_project.market.schemas import RawMarket
from dagster_project.news.schemas import CleanArticle
from dagster_project.quality.checks import (
    check_duplicates,
    check_freshness,
    check_nulls,
    check_row_count,
    check_schema,
    summarize,
)

NOW = datetime(2026, 9, 14, 6, 0, tzinfo=UTC)


def _article(url="https://a.com/1", hours_ago=1, **over):
    a = {
        "title": "T",
        "url": url,
        "source": "test",
        "published_at": (NOW - timedelta(hours=hours_ago)).isoformat(),
        "content": "bitcoin surges",
        "symbols": ["BTC"],
        "sentiment": "positive",
    }
    a.update(over)
    return a


def test_freshness_ok_and_stale():
    assert check_freshness([_article(hours_ago=1)], now=NOW).passed
    assert not check_freshness([_article(hours_ago=30)], now=NOW).passed
    assert not check_freshness([{"url": "x"}], now=NOW).passed  # không có timestamp


def test_duplicates():
    ok = check_duplicates([_article("https://a.com/1"), _article("https://a.com/2")])
    assert ok.passed and ok.metrics["duplicates"] == 0
    bad = check_duplicates([_article("https://a.com/1"), _article("https://a.com/1")])
    assert not bad.passed and bad.metrics["duplicates"] == 1


def test_nulls():
    assert check_nulls([_article()], ["title", "url"]).passed
    assert not check_nulls([_article(title="")], ["title", "url"]).passed
    assert not check_nulls([{"title": "T"}], ["title", "url"]).passed


def test_row_count_bounds():
    assert check_row_count(50, 1, 1000).passed
    assert not check_row_count(0, 1, 1000).passed  # feed chết
    assert not check_row_count(5000, 1, 1000).passed  # parse lặp


def test_schema_news_and_market():
    assert check_schema([_article()], CleanArticle).passed
    assert not check_schema([{"url": "x"}], CleanArticle).passed
    coin = {
        "symbol": "BTC", "name": "Bitcoin", "price": 1.0,
        "market_cap": None, "circulating_supply": None,
        "volume_24h": None, "price_change_24h": None,
    }
    assert check_schema([coin], RawMarket).passed


def test_summarize_counts():
    from dagster_project.quality.checks import CheckResult

    out = summarize("news", {"a": CheckResult(True, "ok", {"n": 1}), "b": CheckResult(False, "x", {})})
    assert out["pipeline"] == "news" and out["passed"] == 1 and out["failed"] == 1
    assert out["a_n"] == 1
