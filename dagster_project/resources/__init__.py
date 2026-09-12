"""Resources package. Import từ đây để asset không dính module con."""
from dagster_project.resources.coingecko import CoinGeckoResource
from dagster_project.resources.postgres import PostgresResource
from dagster_project.resources.rss import RSSFeedResource

__all__ = ["CoinGeckoResource", "PostgresResource", "RSSFeedResource"]
