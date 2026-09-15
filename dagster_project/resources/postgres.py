"""Postgres resource: mở connection + các thao tác INSERT gom 1 chỗ."""
import time
from contextlib import contextmanager
from datetime import datetime

import orjson
import psycopg2
from dagster import ConfigurableResource

_counters = {
    "insert_success_total": 0,  # số lần gọi insert_* thành công
    "insert_failure_total": 0,  # số lần gọi insert_* lỗi (exception)
    "rows_inserted_total": 0,  # tổng rows đã INSERT (news + snapshot)
    "last_query_latency_s": 0.0,
    "max_query_latency_s": 0.0,
}


def snapshot_metrics() -> dict:
    """Counters hiện tại (live xem qua asset metadata từng materialize)."""
    return dict(_counters)


def reset_metrics() -> None:
    for key in _counters:
        _counters[key] = 0.0 if key.endswith("_s") else 0


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
            "signals": """(
                id BIGSERIAL PRIMARY KEY,
                symbol VARCHAR(20) NOT NULL,
                signal_type VARCHAR(30) NOT NULL,
                window_start TIMESTAMPTZ NOT NULL,
                details JSONB,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (symbol, signal_type, window_start)
            )""",
        }
        with conn, conn.cursor() as cur:
            for base, ddl in ddls.items():
                cur.execute(
                    f"CREATE TABLE IF NOT EXISTS {self.table(base)} {ddl}"
                )

    @contextmanager
    def _timed(self):
        """Đo latency 1 lần insert + đếm success/failure (rows cộng ở caller)."""
        started = time.time()
        try:
            yield
        except Exception:
            _counters["insert_failure_total"] += 1
            raise
        latency = time.time() - started
        _counters["insert_success_total"] += 1
        _counters["last_query_latency_s"] = round(latency, 3)
        _counters["max_query_latency_s"] = round(
            max(_counters["max_query_latency_s"], latency), 3
        )

    def insert_news(self, articles: list[dict]) -> int:
        """INSERT tin, bỏ qua URL đã có. Trả về số dòng mới."""
        inserted = 0
        with self._timed(), self._session() as cur:
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
        _counters["rows_inserted_total"] += inserted
        return inserted

    def insert_snapshot(self, collected_at: datetime, coins: list[dict]) -> int:
        """INSERT 1 batch snapshot cùng mốc giờ. Trả về số dòng mới."""
        inserted = 0
        with self._timed(), self._session() as cur:
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
        _counters["rows_inserted_total"] += inserted
        return inserted

    def insert_errors(self, errors: list[dict]) -> None:
        """Ghi bad records, không bao giờ drop lặng lẽ."""
        if not errors:
            return
        with self._timed(), self._session() as cur:
            for err in errors:
                    payload = err["payload"]
                    if not isinstance(payload, str):
                        payload = orjson.dumps(payload, default=str).decode("utf-8")
                    cur.execute(
                        f"""
                        INSERT INTO {self.table("data_quality_errors")}
                            (pipeline, payload, error)
                        VALUES (%s, %s, %s)
                        """,
                        (err["pipeline"], payload, err["error"]),
                    )

    def _query_candles(self, columns: str, minutes: int) -> list[tuple]:
        """SELECT nến 1m (bảng global) — dùng chung cho detector và quality."""
        conn = self.get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT {columns} FROM market_1m
                    WHERE window_start > NOW() - (%s || ' minutes')::INTERVAL
                    ORDER BY symbol, window_start;
                    """,
                    (minutes,),
                )
                return cur.fetchall()
        finally:
            conn.close()

    def fetch_candles(self, minutes: int = 70) -> list[dict]:
        """Nến 1m gọn (symbol/window/volume) cho detector."""
        return [
            {"symbol": s, "window_start": w, "volume": float(v)}
            for s, w, v in self._query_candles("symbol, window_start, volume", minutes)
        ]

    def fetch_ohlcv(self, minutes: int = 70) -> list[dict]:
        """Nến 1m đầy đủ fields cho quality check."""
        return [
            {"symbol": s, "window_start": w, "open": o, "high": h,
             "low": low, "close": c, "volume": v, "trade_count": n}
            for s, w, o, h, low, c, v, n in self._query_candles(
                "symbol, window_start, open, high, low, close, volume, trade_count",
                minutes,
            )
        ]

    def insert_signals(self, signals: list[dict]) -> int:
        """INSERT signals, bỏ qua cái đã có. Trả về số dòng mới."""
        with self._session():
            pass  # ensure tables kể cả khi không có signal mới
        if not signals:
            return 0
        inserted = 0
        with self._timed(), self._session() as cur:
            for sig in signals:
                details = sig.get("details", {})
                if not isinstance(details, str):
                    details = orjson.dumps(details, default=str).decode("utf-8")
                cur.execute(
                    f"""
                    INSERT INTO {self.table("signals")}
                        (symbol, signal_type, window_start, details)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (symbol, signal_type, window_start) DO NOTHING
                    """,
                    (sig["symbol"], sig["signal_type"], sig["window_start"], details),
                )
                inserted += cur.rowcount
        _counters["rows_inserted_total"] += inserted
        return inserted
