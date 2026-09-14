"""P4: test asset + resource bằng mock (không DB, không mạng).

Vì sao mock: test logic của mình, không test hạ tầng.
Chạy offline trong <1s thay vì dựng Postgres + gọi API thật.
"""
from datetime import UTC
from unittest.mock import MagicMock, patch

from dagster import build_op_context

from dagster_project.assets.market_assets import (
    fetch_market,
    loaded_snapshot,
)
from dagster_project.assets.news_assets import loaded_news
from dagster_project.resources import PostgresResource


def make_context():
    """Context thật của Dagster cho test (không cần chạy job)."""
    return build_op_context()


class MockPostgres:
    """Mock resource: ghi vào RAM thay vì DB."""

    def __init__(self):
        self.news = []
        self.snapshots = []
        self.errors = []

    def insert_news(self, articles):
        self.news.extend(articles)
        return len(articles)

    def insert_snapshot(self, collected_at, coins):
        self.snapshots.extend(coins)
        return len(coins)

    def insert_errors(self, errors):
        self.errors.extend(errors)


class MockCoinGecko:
    """Mock API: 1 coin tốt + 1 coin lỗi, không gọi mạng."""

    def fetch_markets(self):
        return [
            {
                "symbol": "btc",
                "name": "Bitcoin",
                "current_price": 77000,
                "market_cap": 1500000000000,
                "circulating_supply": 20000000,
                "total_volume": 30000000000,
                "price_change_percentage_24h": 1.5,
            },
            {"symbol": "xxx", "name": "Bad", "current_price": 0},
        ]


SAMPLE_ARTICLE = {
    "title": "T",
    "url": "https://a.com/1",
    "source": "test",
    "published_at": None,
    "content": "bitcoin surges",
    "symbols": ["BTC"],
    "sentiment": "positive",
}


def test_table_names_per_env():
    """P3: resource tự chọn bảng theo env, không cần DB."""
    local = PostgresResource(conn_str="dummy", env="local")
    prod = PostgresResource(conn_str="dummy", env="prod")
    assert local.table("crypto_news") == "crypto_news_local"
    assert prod.table("crypto_news") == "crypto_news"


def test_insert_news_sql_uses_env_table():
    """Mock psycopg2: assert SQL đúng bảng + có upsert, không cần DB thật."""
    pg = PostgresResource(conn_str="dummy", env="local")
    with patch(
        "dagster_project.resources.postgres.psycopg2.connect"
    ) as mock_connect:
        mock_cur = MagicMock()
        mock_conn = MagicMock()
        mock_conn.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur
        mock_cur.rowcount = 1
        mock_connect.return_value = mock_conn

        assert pg.insert_news([SAMPLE_ARTICLE]) == 1

        statements = " ".join(
            call.args[0] for call in mock_cur.execute.call_args_list
        )
        assert "crypto_news_local" in statements  # bảng theo env
        assert "CREATE TABLE IF NOT EXISTS" in statements  # ensure_tables
        assert "ON CONFLICT (url) DO NOTHING" in statements  # upsert


def test_loaded_news_with_mock():
    """Gọi asset trực tiếp với mock: test logic asset, không test DB."""
    mock_pg = MockPostgres()
    result = loaded_news(make_context(), mock_pg, [SAMPLE_ARTICLE])
    assert result == 1
    assert mock_pg.news[0]["url"] == "https://a.com/1"


def test_fetch_market_with_mock_api():
    """2 tầng validate: msgspec (kiểu dữ liệu) rồi rules (business).

    price=0 qua được msgspec (vẫn là số) nhưng rớt ở validate_market.
    """
    from dagster_project.assets.market_assets import validate_market

    fetched = fetch_market(make_context(), MockCoinGecko())
    assert len(fetched) == 2  # msgspec chỉ loại sai kiểu, không loại sai business

    split = validate_market(make_context(), fetched)
    assert len(split["valid"]) == 1
    assert split["valid"][0]["symbol"] == "BTC"
    assert len(split["errors"]) == 1
    assert "price" in split["errors"][0]["error"]


def test_loaded_snapshot_writes_errors_to_mock():
    """Bad records phải tới errors, không drop lặng lẽ."""
    from datetime import datetime

    mock_pg = MockPostgres()
    result = loaded_snapshot(
        make_context(),
        mock_pg,
        {
            "valid": [
                {
                    "symbol": "BTC",
                    "name": "Bitcoin",
                    "price": 1.0,
                    "market_cap": 1.0,
                    "circulating_supply": 1.0,
                    "volume_24h": 1.0,
                    "price_change_24h": 1.0,
                }
            ],
            "errors": [
                {"pipeline": "market", "payload": "{}", "error": "price <= 0"}
            ],
        },
    )
    assert result == 1
    assert len(mock_pg.errors) == 1
    assert mock_pg.errors[0]["error"] == "price <= 0"
    assert isinstance(mock_pg.snapshots[0], dict)
    assert datetime.now(UTC) is not None
