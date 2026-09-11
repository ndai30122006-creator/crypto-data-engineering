"""Pydantic models cho news articles."""
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class RawArticle(BaseModel):
    title: str
    url: str
    source: str
    published_at: Optional[datetime] = None
    content: str = ""


class CleanArticle(RawArticle):
    symbols: list[str] = Field(default_factory=list)
    sentiment: str = "neutral"
