"""Dagster definitions: news (5 phút) + market snapshot (1 giờ)."""
from dagster import (
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
)

from dagster_project.assets.market_assets import (
    fetch_market,
    loaded_snapshot,
    validate_market,
)
from dagster_project.assets.news_assets import (
    cleaned_news,
    loaded_news,
    raw_news,
)
from dagster_project.assets.quality_assets import quarantine_ohlcv
from dagster_project.assets.signals_assets import detected_signals
from dagster_project.quality.asset_checks import (
    market_count_and_schema,
    market_metrics,
    news_count_and_schema,
    news_freshness,
    news_no_duplicates,
    news_no_nulls,
)
from dagster_project.resources import (
    CoinGeckoResource,
    PostgresResource,
    RSSFeedResource,
)

news_job = define_asset_job(
    name="news_job",
    selection=["raw_news", "cleaned_news", "loaded_news"],
)

news_schedule = ScheduleDefinition(
    job=news_job,
    cron_schedule="*/5 * * * *",
)

market_job = define_asset_job(
    name="market_job",
    selection=["fetch_market", "validate_market", "loaded_snapshot", "detected_signals", "quarantine_ohlcv"],
)

market_schedule = ScheduleDefinition(
    job=market_job,
    cron_schedule="0 * * * *",
)

defs = Definitions(
    assets=[
        raw_news,
        cleaned_news,
        loaded_news,
        fetch_market,
        validate_market,
        loaded_snapshot,
        detected_signals,
        quarantine_ohlcv,
    ],
    schedules=[news_schedule, market_schedule],
    asset_checks=[
        news_freshness,
        news_no_duplicates,
        news_no_nulls,
        news_count_and_schema,
        market_count_and_schema,
        market_metrics,
    ],
    resources={
        "postgres": PostgresResource(
            conn_str=EnvVar("DATABASE_URL"),
            env=EnvVar("DAGSTER_ENVIRONMENT"),
        ),
        "rss": RSSFeedResource(),
        "coingecko": CoinGeckoResource(),
    },
)
