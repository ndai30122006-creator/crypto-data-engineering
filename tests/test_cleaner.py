"""Unit tests cho cleaner + parser (không cần DB, không cần mạng)."""
from dagster_project.news.cleaner import (
    classify_sentiment,
    clean_text,
    dedupe_by_url,
    extract_symbols,
)
from dagster_project.news.parser import normalize_url, parse_entry, sanitize_feed_xml


def test_clean_text_strips_html():
    assert clean_text("<p>Bitcoin <b>surges</b>  &amp; gains</p>") == "Bitcoin surges & gains"


def test_extract_symbols():
    assert extract_symbols("Bitcoin ETF approval lifts Ethereum and Solana") == [
        "BTC",
        "ETF",
        "ETH",
        "SOL",
    ]


def test_classify_sentiment():
    assert classify_sentiment("Bitcoin surges to record high on ETF approval") == "positive"
    assert classify_sentiment("Exchange hack causes $300M loss, outflows accelerate") == "negative"
    assert classify_sentiment("Developers discuss protocol upgrade timeline") == "neutral"


def test_normalize_url_strips_tracking_params():
    assert (
        normalize_url(
            "https://cointelegraph.com/news/x?utm_source=rss_feed&utm_medium=rss#frag"
        )
        == "https://cointelegraph.com/news/x"
    )


def test_parse_entry_prefers_content_encoded():
    entry = {
        "title": "T",
        "link": "https://example.com/a?x=1",
        "published_parsed": None,
        "published": "Fri, 11 Sep 2026 13:44:27 +0000",
        "summary": "short",
        "content": [{"value": "<p>full</p>"}],
    }
    parsed = parse_entry("test", entry)
    assert parsed["url"] == "https://example.com/a"
    assert parsed["content"] == "<p>full</p>"
    assert parsed["published_at"] is not None


def test_dedupe_by_url():
    articles = [
        {"url": "https://a.com/1", "title": "x"},
        {"url": "https://a.com/1", "title": "x"},
        {"url": "https://a.com/2", "title": "y"},
    ]
    assert len(dedupe_by_url(articles)) == 2


def test_sanitize_removes_empty_content_encoded():
    import feedparser

    xml = (
        b"<rss version='2.0' "
        b"xmlns:dc='http://purl.org/dc/elements/1.1/' "
        b"xmlns:content='http://purl.org/rss/1.0/modules/content/'>"
        b"<channel><item>"
        b"<title>T</title><link>https://a.com/1</link>"
        b"<description>real content here</description>"
        b"<content:encoded/>"
        b"<dc:description/>"
        b"</item></channel></rss>"
    )
    raw = feedparser.parse(xml).entries[0].get("summary")
    fixed = feedparser.parse(sanitize_feed_xml(xml)).entries[0].get("summary")
    assert raw == ""
    assert fixed == "real content here"


def test_sanitize_keeps_nonempty_content_encoded():
    import feedparser

    xml = (
        b"<rss version='2.0' "
        b"xmlns:content='http://purl.org/rss/1.0/modules/content/'>"
        b"<channel><item>"
        b"<title>T</title><link>https://a.com/1</link>"
        b"<description>short</description>"
        b"<content:encoded><![CDATA[<p>full body</p>]]></content:encoded>"
        b"</item></channel></rss>"
    )
    fixed = feedparser.parse(sanitize_feed_xml(xml)).entries[0]
    assert fixed.get("content")[0]["value"] == "<p>full body</p>"
