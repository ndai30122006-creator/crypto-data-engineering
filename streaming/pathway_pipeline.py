"""Pathway engine: Kafka crypto.trades.v2 → tumbling 1m OHLCV → Postgres.

Chạy trong container Linux (pathway không có wheel Windows):
    python -m streaming.pathway_pipeline
Env: KAFKA_BOOTSTRAP_SERVERS (default kafka:9092), KAFKA_TOPIC,
     KAFKA_GROUP_ID, PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD.

Deduplicate (symbol, trade_id) trước khi aggregate. Open/close theo
(timestamp, trade_id); không dùng processing-time hay giá để phá tie.
Restart cần volume persistence và lịch sử Kafka còn đủ để replay.
price_change_1m để NULL ở sink — query correlation tự tính bằng LAG()
trên close (stateless, đúng cả khi replay).
"""

import os
from datetime import UTC, datetime, timedelta

import pathway as pw

from ingestion.events import decode_trade_fields, normalize_trade
from ingestion.jlog import get_logger
from streaming import metrics as engine_metrics
from streaming.postgres_sink import (
    ensure_tables,
    write_candle_with_retry,
)
from streaming.postgres_sink import (
    snapshot_metrics as sink_metrics,
)
from streaming.windows import WINDOW_SECONDS, order_key

log = get_logger("pathway-pipeline")


class TradeSchema(pw.Schema):
    symbol: str
    trade_id: int
    price: float
    quantity: float
    timestamp: int  # ms, event time từ Binance


def decode_messages(messages: pw.Table) -> pw.Table:
    """Parse raw payloads safely before strict trade schema/aggregation."""
    decoded = messages.select(fields=pw.apply(decode_trade_fields, messages.data))
    return decoded.select(
        symbol=decoded.fields[0],
        trade_id=decoded.fields[1],
        price=decoded.fields[2],
        quantity=decoded.fields[3],
        timestamp=decoded.fields[4],
    )


def _valid_fields(
    symbol: str, trade_id: int, price: float, quantity: float, timestamp: int
) -> bool:
    return (
        normalize_trade(
            {
                "symbol": symbol,
                "trade_id": trade_id,
                "price": price,
                "quantity": quantity,
                "timestamp": timestamp,
            }
        )
        is not None
    )


def _consistent(count: int) -> bool:
    if count != 1:
        raise ValueError("conflicting payloads for the same symbol/trade_id")
    return True


def build_candles(trades: pw.Table) -> pw.Table:
    """Trades → nến 1m. Tách hàm để test static trong container."""
    valid = trades.filter(
        pw.apply(
            _valid_fields,
            trades.symbol,
            trades.trade_id,
            trades.price,
            trades.quantity,
            trades.timestamp,
        )
    )
    # Remove exact repetitions, then reject a business ID with conflicting fields.
    distinct = valid.groupby(
        valid.symbol,
        valid.trade_id,
        valid.timestamp,
        valid.price,
        valid.quantity,
    ).reduce(
        pw.this.symbol,
        pw.this.trade_id,
        pw.this.timestamp,
        pw.this.price,
        pw.this.quantity,
    )
    unique = distinct.groupby(distinct.symbol, distinct.trade_id).reduce(
        pw.this.symbol,
        pw.this.trade_id,
        timestamp=pw.reducers.any(pw.this.timestamp),
        price=pw.reducers.any(pw.this.price),
        quantity=pw.reducers.any(pw.this.quantity),
        variants=pw.reducers.count(),
    )
    unique = unique.filter(pw.apply(_consistent, unique.variants))
    timed = unique.with_columns(
        t=unique.timestamp.dt.utc_from_timestamp("ms"),
        # Bucket epoch-seconds: cùng window → cùng giá trị, lấy min() là xong.
        wb=(unique.timestamp // 1000 // WINDOW_SECONDS) * WINDOW_SECONDS,
        oc=pw.apply(order_key, unique.timestamp, unique.trade_id),
    )
    return timed.windowby(
        timed.t,
        window=pw.temporal.tumbling(duration=timedelta(seconds=WINDOW_SECONDS)),
        instance=timed.symbol,
    ).reduce(
        symbol=pw.reducers.any(pw.this.symbol),
        window_start=pw.reducers.min(pw.this.wb),
        open=pw.reducers.argmin(pw.this.oc, pw.this.price),
        high=pw.reducers.max(pw.this.price),
        low=pw.reducers.min(pw.this.price),
        close=pw.reducers.argmax(pw.this.oc, pw.this.price),
        volume=pw.reducers.sum(pw.this.quantity),
        trade_count=pw.reducers.count(pw.this.price),
    )


def make_sink():
    """Kết nối Postgres 1 lần, trả về callback upsert cho subscribe."""
    import psycopg2

    def connect():
        return psycopg2.connect(
            host=os.getenv("PGHOST", "postgres"),
            port=int(os.getenv("PGPORT", "5432")),
            dbname=os.getenv("PGDATABASE", "crypto_db"),
            user=os.getenv("PGUSER", "admin"),
            password=os.getenv("PGPASSWORD", "secret"),
            connect_timeout=5,
        )

    conn = connect()
    ensure_tables(conn)
    written = {"n": 0}
    try:
        retries = max(1, int(os.getenv("SINK_RETRIES", "3")))
    except (TypeError, ValueError):
        retries = 3
    dlq_path = os.getenv("DLQ_FILE", "/var/lib/crypto/dlq/candles.jsonl")

    def on_candle(key, row: dict, time, is_addition: bool) -> None:
        nonlocal conn
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
            "updated_at": datetime.now(UTC).isoformat(),
        }
        try:
            conn = write_candle_with_retry(
                conn, candle, retries=retries, dlq_path=dlq_path, connect=connect
            )
        except RuntimeError as exc:
            log.error(
                "sink failed; inspect DLQ outcome", exc=exc, symbol=candle["symbol"]
            )
            raise
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
                window=datetime.fromtimestamp(
                    candle["window_start"], tz=UTC
                ).isoformat(),
                close=candle["close"],
                **stats,
            )

    return on_candle


def main() -> None:
    topic = os.getenv("KAFKA_TOPIC", "crypto.trades.v2")
    messages = pw.io.kafka.read(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": os.getenv("KAFKA_GROUP_ID", "pathway-ohlcv-1m-v2"),
            "auto.offset.reset": "earliest",
        },
        topic=topic,
        format="raw",
        autogenerate_key=True,  # Kafka key=symbol is not a unique Pathway row ID.
        name="trades-v2-raw",
    )
    trades = decode_messages(messages)
    log.info("streaming", topic=topic, table="market_1m", window_s=WINDOW_SECONDS)
    candles = build_candles(trades)

    def on_trade(key, row: dict, time, is_addition: bool) -> None:
        if is_addition and row["symbol"]:
            engine_metrics.note_event(row["symbol"], row["timestamp"])

    pw.io.subscribe(trades, on_trade)
    pw.io.subscribe(candles, make_sink())
    state_dir = os.getenv("PATHWAY_PERSISTENCE_DIR")
    persistence = (
        pw.persistence.Config(
            pw.persistence.Backend.filesystem(state_dir), snapshot_interval_ms=1000
        )
        if state_dir
        else None
    )
    pw.run(terminate_on_error=True, persistence_config=persistence)


if __name__ == "__main__":
    main()
