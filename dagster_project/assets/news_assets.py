"""Pipeline: fetch_news (raw) -> clean_news -> load_postgres."""
import asyncio

from dagster import AssetExecutionContext, asset
from pydantic import ValidationError

from dagster_project.news.cleaner import (
    classify_sentiment,
    clean_text,
    dedupe_by_url,
    extract_symbols,
)
from dagster_project.news.collector import fetch_all
from dagster_project.news.parser import parse_entries
from dagster_project.news.schemas import CleanArticle, RawArticle
from dagster_project.resources import PostgresResource


@asset
def raw_news(context: AssetExecutionContext) -> list[dict]:
    """Fetch tin tức từ tất cả RSS sources."""
    items, errors = asyncio.run(fetch_all())
    for err in errors:
        context.log.warning(err)
    parsed = parse_entries(items)
    valid: list[dict] = []
    for article in parsed:
        try:
            valid.append(RawArticle(**article).model_dump(mode="json"))
        except ValidationError as exc:
            context.log.warning(f"skip invalid article {article.get('url')}: {exc}")
    context.log.info(f"fetched {len(valid)} articles ({len(errors)} source errors)")
    return valid


@asset
def cleaned_news(context: AssetExecutionContext, raw_news: list[dict]) -> list[dict]:
    """Clean text, extract symbols, sentiment, dedupe."""
    deduped = dedupe_by_url([RawArticle(**a) for a in raw_news])
    cleaned: list[dict] = []
    for article in deduped:
        data = article.model_dump()
        text = f"{data['title']} {data['content']}"
        data["title"] = clean_text(data["title"])
        data["content"] = clean_text(data["content"])
        data["symbols"] = extract_symbols(text)
        data["sentiment"] = classify_sentiment(text)
        cleaned.append(CleanArticle(**data).model_dump(mode="json"))
    context.log.info(f"cleaned {len(cleaned)} articles (from {len(raw_news)} raw)")
    return cleaned


@asset
def loaded_news(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    cleaned_news: list[dict],
) -> int:
    """Load vào PostgreSQL, bỏ qua URL đã tồn tại."""
    conn = postgres.get_conn()
    inserted = 0
    try:
        with conn, conn.cursor() as cur:
            for article in cleaned_news:
                cur.execute(
                    """
                    INSERT INTO crypto_news
                        (title, url, source, published_at, content, sentiment, symbols)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING
                    """,
                    (
                        article["title"],
                        article["url"],
                        article["source"],
                        article["published_at"],
                        article["content"],
                        article["sentiment"],
                        article["symbols"],
                    ),
                )
                inserted += cur.rowcount
    finally:
        conn.close()
    context.log.info(f"inserted {inserted} new articles ({len(cleaned_news)} processed)")
    return inserted
