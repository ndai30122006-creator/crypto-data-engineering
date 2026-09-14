"""Dagster asset checks cho news + market (mục 11–16).

Check chạy sau asset materialize, hiện kết quả trên UI (tab Checks).
Lỗi dữ liệu → check đỏ + log, nhưng không chặn pipeline (non-blocking);
muốn chặn thì thêm blocking=True.
"""
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    asset_check,
)

from dagster_project.assets.market_assets import fetch_market
from dagster_project.assets.news_assets import cleaned_news, raw_news
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


def _to_result(result, severity=AssetCheckSeverity.ERROR) -> AssetCheckResult:
    return AssetCheckResult(
        passed=result.passed,
        description=result.message,
        metadata=dict(result.metrics),
        severity=severity,
    )


def _combined(pipeline: str, parts: dict[str, object]) -> AssetCheckResult:
    """Gom nhiều CheckResult thành 1 check (khỏi lặp khối combine)."""
    passed = all(r.passed for r in parts.values())
    return AssetCheckResult(
        passed=passed,
        description=" | ".join(r.message for r in parts.values()),
        metadata=summarize(pipeline, parts),
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset=raw_news, description="11. Tin mới nhất trong 24h")
def news_freshness(raw_news: list[dict]) -> AssetCheckResult:
    return _to_result(
        check_freshness(raw_news, "published_at", 24 * 60),
        AssetCheckSeverity.WARN,  # RSS có thể toàn tin cũ lúc nguồn chậm
    )


@asset_check(asset=cleaned_news, description="12. Không trùng URL sau dedupe")
def news_no_duplicates(cleaned_news: list[dict]) -> AssetCheckResult:
    return _to_result(check_duplicates(cleaned_news, "url"))


@asset_check(asset=cleaned_news, description="13. Title/URL/source không rỗng")
def news_no_nulls(cleaned_news: list[dict]) -> AssetCheckResult:
    return _to_result(check_nulls(cleaned_news, ["title", "url", "source"]))


@asset_check(asset=cleaned_news, description="14+15. Đủ tin + đúng schema CleanArticle")
def news_count_and_schema(cleaned_news: list[dict]) -> AssetCheckResult:
    return _combined(
        "news",
        {
            "count": check_row_count(len(cleaned_news), min_count=1, max_count=1000),
            "schema": check_schema(cleaned_news, CleanArticle),
        },
    )


@asset_check(asset=fetch_market, description="14+15. Đủ coin + đúng schema RawMarket")
def market_count_and_schema(fetch_market: list[dict]) -> AssetCheckResult:
    # max 500 để dư chỗ nếu tăng per_page (hiện 50), min 10 bắt API rỗng/lỗi.
    return _combined(
        "market",
        {
            "count": check_row_count(len(fetch_market), min_count=10, max_count=500),
            "schema": check_schema(fetch_market, RawMarket),
            "nulls": check_nulls(fetch_market, ["symbol", "price"]),
        },
    )


@asset_check(asset=fetch_market, description="16. Metrics snapshot cho UI/log")
def market_metrics(fetch_market: list[dict]) -> AssetCheckResult:
    priced = sum(1 for c in fetch_market if isinstance(c.get("price"), (int, float)))
    return AssetCheckResult(
        passed=True,
        description="metrics only",
        metadata={
            "coins": len(fetch_market),
            "priced": priced,
            "missing_price": len(fetch_market) - priced,
        },
    )
