"""Chuẩn hoá feedparser entries thành dict khớp RawArticle."""
import calendar
import re
from datetime import UTC, datetime
from urllib.parse import urlsplit, urlunsplit

from dateutil import parser as date_parser

# CoinDesk để <content:encoded/> RỖNG trong khi <description> có nội dung.
# feedparser ưu tiên content:encoded cho cả summary => mất content.
# Lọc tag rỗng này trước khi parse (chỉ tag rỗng, giữ nguyên tag có nội dung).
_EMPTY_ENCODED = re.compile(
    rb"<\w+:encoded\s*/>|<\w+:encoded\s*>\s*</\w+:encoded\s*>",
    re.IGNORECASE,
)


def sanitize_feed_xml(content: bytes) -> bytes:
    return _EMPTY_ENCODED.sub(b"", content)


def normalize_url(url: str) -> str:
    """Bỏ toàn bộ query + fragment để dedupe (kể cả utm_*)."""
    parts = urlsplit(url.strip())
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def parse_published(entry: dict) -> datetime | None:
    if entry.get("published_parsed"):
        return datetime.fromtimestamp(
            calendar.timegm(entry["published_parsed"]), tz=UTC
        )
    raw = entry.get("published") or entry.get("updated")
    if raw:
        try:
            dt = date_parser.parse(raw)
            return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
        except (ValueError, OverflowError, TypeError):
            return None
    return None


def extract_content(entry: dict) -> str:
    content = entry.get("content")
    if content and content[0].get("value"):
        return content[0]["value"]  # content:encoded (WordPress)
    return entry.get("summary") or entry.get("description") or ""


def parse_entry(source: str, entry: dict) -> dict | None:
    url = (entry.get("link") or "").strip()
    title = (entry.get("title") or "").strip()
    if not url or not title:
        return None
    return {
        "title": title,
        "url": normalize_url(url),
        "source": source,
        "published_at": parse_published(entry),
        "content": extract_content(entry),
    }


def parse_entries(items: list[tuple[str, dict]]) -> list[dict]:
    return [a for a in (parse_entry(s, e) for s, e in items) if a]
