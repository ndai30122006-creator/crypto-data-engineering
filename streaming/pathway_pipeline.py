"""Pathway engine: Kafka crypto.trades → tumbling 1m OHLCV → Postgres.

Chạy trong container Linux (pathway không có wheel Windows):
    python -m streaming.pathway_pipeline
Env: KAFKA_BOOTSTRAP_SERVERS (default kafka:9092), KAFKA_TOPIC,
     KAFKA_GROUP_ID, PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.

Ghi qua subscribe callback + upsert (symbol, window_start): replay Kafka
hay restart engine đều idempotent. open/close = giá trade sớm/muộn nhất
(trades cùng symbol đi 1 partition nên giữ thứ tự).
price_change_1m để NULL ở sink — query correlation tự tính bằng LAG()
trên close (stateless, đúng cả khi replay).
"""
import logging
import os
from datetime import UTC, datetime, timedelta

import pathway as pw

from streaming.postgres_sink import UPSERT_1M, ensure_tables, to_row
from streaming.windows import WINDOW_SECONDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("pathway-pipeline")


class TradeSchema(pw.Schema):
    symbol: str
    price: float
    quantity: float
    timestamp: int  # ms, event time từ Binance


def build_candles(trades: pw.Table) -> pw.Table:
    """Trades → nến 1m. Tách hàm để test static trong container."""
    timed = trades.with_columns(
        t=trades.timestamp.dt.utc_from_timestamp("ms"),
        # Bucket epoch-seconds: cùng window → cùng giá trị, lấy min() là xong.
        wb=(trades.timestamp // 1000 // WINDOW_SECONDS) * WINDOW_SECONDS,
    )
    return timed.windowby(
        timed.t,
        window=pw.temporal.tumbling(duration=timedelta(seconds=WINDOW_SECONDS)),
        instance=timed.symbol,
    ).reduce(
        # earliest/latest theo processing-time (có warning của engine):
        # OK vì trades cùng symbol đi 1 Kafka partition nên giữ thứ tự.
        symbol=pw.reducers.any(pw.this.symbol),
        window_start=pw.reducers.min(pw.this.wb),
        open=pw.reducers.earliest(pw.this.price),
        high=pw.reducers.max(pw.this.price),
        low=pw.reducers.min(pw.this.price),
        close=pw.reducers.latest(pw.this.price),
        volume=pw.reducers.sum(pw.this.quantity),
        trade_count=pw.reducers.count(pw.this.price),
    )


def make_sink():
    """Kết nối Postgres 1 lần, trả về callback upsert cho subscribe."""
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "crypto_db"),
        user=os.getenv("PGUSER", "admin"),
        password=os.getenv("PGPASSWORD", "secret"),
    )
    ensure_tables(conn)
    written = {"n": 0}

    def on_candle(key, row: dict, time, is_addition: bool) -> None:
        if not is_addition:
            return  # retraction của window cũ — upsert đã bao phủ ở bản mới
        candle = {
            "symbol": row["symbol"],
            "window_start": int(row["window_start"]),
            "open": row["open"],
            "high": row["high"],
            "low": row["low"],
            "close": row["close"],
            "volume": row["volume"],
            "trade_count": int(row["trade_count"]),
            "price_change_1m": None,
        }
        with conn, conn.cursor() as cur:
            cur.execute(UPSERT_1M, to_row(candle))
        written["n"] += 1
        if written["n"] == 1 or written["n"] % 50 == 0:
            log.info(
                "upserted %d candles, latest %s @ %s close=%s",
                written["n"],
                candle["symbol"],
                datetime.fromtimestamp(candle["window_start"], tz=UTC),
                candle["close"],
            )

    return on_candle


def main() -> None:
    topic = os.getenv("KAFKA_TOPIC", "crypto.trades")
    trades = pw.io.kafka.read(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": os.getenv("KAFKA_GROUP_ID", "pathway-ohlcv-1m"),
            "auto.offset.reset": "earliest",
        },
        topic=topic,
        schema=TradeSchema,
        format="json",
    )
    log.info("streaming topic=%s → market_1m (tumbling %ss)", topic, WINDOW_SECONDS)
    candles = build_candles(trades)
    pw.io.subscribe(candles, make_sink())
    pw.run()


if __name__ == "__main__":
    main()
