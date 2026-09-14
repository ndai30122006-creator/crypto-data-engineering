"""msgspec Structs cho news articles."""
from datetime import datetime

import msgspec


class RawArticle(msgspec.Struct):
    title: str
    url: str
    source: str
    published_at: datetime | None = None
    content: str = ""


class CleanArticle(RawArticle):
    symbols: list[str] = msgspec.field(default_factory=list)
    sentiment: str = "neutral"
