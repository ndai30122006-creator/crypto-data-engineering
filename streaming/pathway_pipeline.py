"""Pathway engine: Kafka crypto.trades → tumbling 1m OHLCV → Postgres.

Chạy trong container Linux (pathway không có wheel Windows):
    python -m streaming.pathway_pipeline
Env: KAFKA_BOOTSTRAP_SERVERS (default kafka:9092), KAFKA_TOPIC,
     KAFKA_GROUP_ID, PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.

Ghi qua subscribe callback + upsert (symbol, window_start): replay Kafka
hay restart engine đều idempotent. open/close tính định đoạt bằng
min/max composite key "ts|price" (đúng data-time; earliest/latest của
engine theo processing-time nên sai khi burst) — xem _oc_key.
price_change_1m để NULL ở sink — query correlation tự tính bằng LAG()
trên close (stateless, đúng cả khi replay).
"""
import os
from datetime import UTC, datetime, timedelta

import pathway as pw

from ingestion.jlog import get_logger
from streaming import metrics as engine_metrics
from streaming.postgres_sink import (
    ensure_tables,
    upsert_candles,
)
from streaming.postgres_sink import (
    snapshot_metrics as sink_metrics,
)
from streaming.windows import WINDOW_SECONDS

log = get_logger("pathway-pipeline")


class TradeSchema(pw.Schema):
    symbol: str
    price: float
    quantity: float
    timestamp: int  # ms, event time từ Binance


def _oc_key(ts_ms: int, price: float) -> str:
    """Composite key sortable: ts cố định 13 chữ số + giá cố định 8 thập phân.

    min(key) = giá ở trade sớm nhất (open), max(key) = giá trade muộn nhất
    (close) — đúng theo data-time bất kể thứ tự arrival (earliest/latest
    của engine theo processing-time nên sai khi burst/out-of-order).
    """
    return f"{int(ts_ms):013d}|{price:.8f}"


def build_candles(trades: pw.Table) -> pw.Table:
    """Trades → nến 1m. Tách hàm để test static trong container."""
    timed = trades.with_columns(
        t=trades.timestamp.dt.utc_from_timestamp("ms"),
        # Bucket epoch-seconds: cùng window → cùng giá trị, lấy min() là xong.
        wb=(trades.timestamp // 1000 // WINDOW_SECONDS) * WINDOW_SECONDS,
        oc=pw.apply(_oc_key, trades.timestamp, trades.price),
    )
    return timed.windowby(
        timed.t,
        window=pw.temporal.tumbling(duration=timedelta(seconds=WINDOW_SECONDS)),
        instance=timed.symbol,
    ).reduce(
        symbol=pw.reducers.any(pw.this.symbol),
        window_start=pw.reducers.min(pw.this.wb),
        open_key=pw.reducers.min(pw.this.oc),
        high=pw.reducers.max(pw.this.price),
        low=pw.reducers.min(pw.this.price),
        close_key=pw.reducers.max(pw.this.oc),
        volume=pw.reducers.sum(pw.this.quantity),
        trade_count=pw.reducers.count(pw.this.price),
    )


def _price_of_key(key: str) -> float:
    return float(key.split("|")[1])


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
            "open": _price_of_key(row["open_key"]),
            "high": row["high"],
            "low": row["low"],
            "close": _price_of_key(row["close_key"]),
            "volume": row["volume"],
            "trade_count": int(row["trade_count"]),
            "price_change_1m": None,
        }
        upsert_candles(conn, [candle])
        written["n"] += 1
        stats = engine_metrics.note_candle(
            candle["symbol"], candle["window_start"], candle["close"]
        )
        engine_metrics.note_db(sink_metrics())
        if written["n"] == 1 or written["n"] % 50 == 0:
            engine_metrics.dump(os.getenv("METRICS_FILE", "/tmp/pathway-metrics.json"))
            log.info(
                "upserted candles",
                n=written["n"],
                symbol=candle["symbol"],
                window=datetime.fromtimestamp(candle["window_start"], tz=UTC).isoformat(),
                close=candle["close"],
                **stats,
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
    log.info("streaming", topic=topic, table="market_1m", window_s=WINDOW_SECONDS)
    candles = build_candles(trades)

    def on_trade(key, row: dict, time, is_addition: bool) -> None:
        if is_addition:
            engine_metrics.note_event(row["symbol"], row["timestamp"])

    pw.io.subscribe(trades, on_trade)
    pw.io.subscribe(candles, make_sink())
    pw.run()


if __name__ == "__main__":
    main()
