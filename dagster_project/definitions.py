"""Dagster definitions: assets + schedule mỗi 5 phút."""
from dagster import (
    AssetSelection,
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
)

from dagster_project.assets.news_assets import (
    cleaned_news,
    loaded_news,
    raw_news,
)
from dagster_project.resources import PostgresResource

news_job = define_asset_job(name="news_job", selection=AssetSelection.all())

news_schedule = ScheduleDefinition(
    job=news_job,
    cron_schedule="*/5 * * * *",
)

defs = Definitions(
    assets=[raw_news, cleaned_news, loaded_news],
    schedules=[news_schedule],
    resources={"postgres": PostgresResource(conn_str=EnvVar("DATABASE_URL"))},
)
