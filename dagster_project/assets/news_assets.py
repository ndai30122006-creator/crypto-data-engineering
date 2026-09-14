"""Pipeline: fetch_news (raw) -> clean_news -> load_postgres."""
import msgspec
from dagster import AssetExecutionContext, asset

from dagster_project.news.cleaner import (
    classify_sentiment,
    clean_text,
    dedupe_by_url,
    extract_symbols,
)
from dagster_project.news.parser import parse_entries
from dagster_project.news.schemas import CleanArticle, RawArticle
from dagster_project.resources import PostgresResource, RSSFeedResource


def _validate(model, data: dict):
    """msgspec.convert thay Model(**data): coerce + validate 1 bước."""
    return msgspec.convert(data, type=model)


@asset
def raw_news(context: AssetExecutionContext, rss: RSSFeedResource) -> list[dict]:
    """Fetch tin tức từ tất cả RSS sources."""
    items, errors = rss.fetch_raw()
    for err in errors:
        context.log.warning(err)
    parsed = parse_entries(items)
    valid: list[dict] = []
    for article in parsed:
        try:
            valid.append(msgspec.to_builtins(_validate(RawArticle, article)))
        except msgspec.ValidationError as exc:
            context.log.warning(f"skip invalid article {article.get('url')}: {exc}")
    context.log.info(f"fetched {len(valid)} articles ({len(errors)} source errors)")
    context.add_output_metadata(
        {"articles": len(valid), "source_errors": len(errors)}
    )
    return valid


@asset
def cleaned_news(context: AssetExecutionContext, raw_news: list[dict]) -> list[dict]:
    """Clean text, extract symbols, sentiment, dedupe."""
    deduped = dedupe_by_url([_validate(RawArticle, a) for a in raw_news])
    cleaned: list[dict] = []
    for article in deduped:
        data = msgspec.to_builtins(article)
        text = f"{data['title']} {data['content']}"
        data["title"] = clean_text(data["title"])
        data["content"] = clean_text(data["content"])
        data["symbols"] = extract_symbols(text)
        data["sentiment"] = classify_sentiment(text)
        cleaned.append(msgspec.to_builtins(_validate(CleanArticle, data)))
    context.log.info(f"cleaned {len(cleaned)} articles (from {len(raw_news)} raw)")
    context.add_output_metadata(
        {
            "raw": len(raw_news),
            "deduped": len(deduped),
            "cleaned": len(cleaned),
        }
    )
    return cleaned


@asset
def loaded_news(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    cleaned_news: list[dict],
) -> int:
    """Load vào PostgreSQL, bỏ qua URL đã tồn tại."""
    inserted = postgres.insert_news(cleaned_news)
    skipped = len(cleaned_news) - inserted
    context.log.info(f"inserted {inserted} new articles ({skipped} duplicates skipped)")
    context.add_output_metadata({"inserted": inserted, "skipped": skipped})
    return inserted
