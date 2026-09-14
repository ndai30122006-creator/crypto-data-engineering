"""Postgres resource: mở connection + các thao tác INSERT gom 1 chỗ."""
import json
from contextlib import contextmanager
from datetime import datetime

import psycopg2
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    """Asset gọi insert_*(), không viết SQL tay, không lo đóng connection."""

    conn_str: str
    env: str = "local"

    def table(self, name: str) -> str:
        """Prod dùng tên gốc, môi trường khác thêm hậu tố.

        Vì sao: test local không bao giờ bẩn dữ liệu prod,
        mà asset không cần biết mình đang chạy ở đâu.
        """
        return name if self.env == "prod" else f"{name}_{self.env}"

    def get_conn(self):
        return psycopg2.connect(self.conn_str)

    @contextmanager
    def _session(self):
        """Mở conn + ensure tables, yield cursor, đóng an toàn.

        Gom 3 dòng lặp ở mọi insert_*() vào 1 chỗ; kể cả khi
        connect lỗi thì finally cũng không chạm biến chưa gán.
        """
        conn = self.get_conn()
        try:
            self.ensure_tables(conn)
            with conn, conn.cursor() as cur:
                yield cur
        finally:
            conn.close()

    def ensure_tables(self, conn) -> None:
        """Tự tạo bảng theo tên đã resolve (CREATE IF NOT EXISTS)."""
        ddls = {
            "crypto_news": """(
                id BIGSERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                url TEXT UNIQUE NOT NULL,
                source VARCHAR(100),
                published_at TIMESTAMPTZ,
                collected_at TIMESTAMPTZ DEFAULT NOW(),
                content TEXT,
                sentiment VARCHAR(20) DEFAULT 'neutral',
                symbols TEXT[] DEFAULT '{}'
            )""",
            "crypto_market_snapshot": """(
                collected_at TIMESTAMPTZ DEFAULT NOW(),
                symbol VARCHAR(20) NOT NULL,
                name VARCHAR(100),
                price NUMERIC(20,8),
                market_cap NUMERIC(30,2),
                circulating_supply NUMERIC(30,2),
                volume_24h NUMERIC(30,2),
                price_change_24h NUMERIC(10,4),
                PRIMARY KEY (collected_at, symbol)
            )""",
            "data_quality_errors": """(
                id BIGSERIAL PRIMARY KEY,
                pipeline VARCHAR(50),
                payload JSONB,
                error TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )""",
        }
        with conn, conn.cursor() as cur:
            for base, ddl in ddls.items():
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {self.table(base)} {ddl}"
                )

    def insert_news(self, articles: list[dict]) -> int:
        """INSERT tin, bỏ qua URL đã có. Trả về số dòng mới."""
        inserted = 0
        with self._session() as cur:
            for article in articles:
                cur.execute(
                    f"""
                    INSERT INTO {self.table("crypto_news")}
                        (title, url, source, published_at,
                         content, sentiment, symbols)
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
        return inserted

    def insert_snapshot(self, collected_at: datetime, coins: list[dict]) -> int:
        """INSERT 1 batch snapshot cùng mốc giờ. Trả về số dòng mới."""
        inserted = 0
        with self._session() as cur:
            for coin in coins:
                cur.execute(
                    f"""
                    INSERT INTO {self.table("crypto_market_snapshot")}
                        (collected_at, symbol, name, price, market_cap,
                         circulating_supply, volume_24h, price_change_24h)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (collected_at, symbol) DO NOTHING
                    """,
                    (
                        collected_at,
                        coin["symbol"],
                        coin["name"],
                        coin["price"],
                        coin["market_cap"],
                        coin["circulating_supply"],
                        coin["volume_24h"],
                        coin["price_change_24h"],
                    ),
                )
                inserted += cur.rowcount
        return inserted

    def insert_errors(self, errors: list[dict]) -> None:
        """Ghi bad records, không bao giờ drop lặng lẽ."""
        if not errors:
            return
        with self._session() as cur:
            for err in errors:
                payload = err["payload"]
                if not isinstance(payload, str):
                    payload = json.dumps(payload, default=str)
                cur.execute(
                    f"""
                    INSERT INTO {self.table("data_quality_errors")}
                        (pipeline, payload, error)
                    VALUES (%s, %s, %s)
                    """,
                    (err["pipeline"], payload, err["error"]),
                )
