"""Replay durable recovery records, preserving source and checkpointing acknowledgements."""

import argparse
import hashlib
import os
from pathlib import Path

import orjson

from ingestion.events import normalize_trade


def replay_file(
    source: Path, handler, *, checkpoint: Path | None = None, dry_run: bool = False
) -> int:
    source = source.resolve()
    checkpoint = checkpoint or source.with_suffix(source.suffix + ".checkpoint.json")
    state = orjson.loads(checkpoint.read_bytes()) if checkpoint.exists() else None
    offset = state["offset"] if state else 0
    digest = hashlib.sha256()
    count = 0
    with source.open("rb") as records:
        prefix = records.read(offset)
        digest.update(prefix)
        if state and (
            state["source"] != str(source)
            or len(prefix) != offset
            or state["sha256"] != digest.hexdigest()
        ):
            raise ValueError("DLQ changed before checkpoint; use a new checkpoint")
        for line in records:
            if not line.endswith(b"\n"):
                raise ValueError("incomplete DLQ record; retry after writer finishes")
            record = orjson.loads(line)
            if record.get("kind") not in {"trade", "candle"} or not isinstance(
                record.get("payload"), dict
            ):
                raise ValueError("unsupported recovery record")
            if (
                record["kind"] == "trade"
                and normalize_trade(record["payload"].get("event")) is None
            ):
                raise ValueError("invalid v2 trade in DLQ")
            if record["kind"] == "candle" and not record["payload"].get("updated_at"):
                raise ValueError(
                    "candle recovery requires updated_at to prevent stale overwrite"
                )
            if not dry_run:
                handler(record)
            count += 1
            digest.update(line)
            offset += len(line)
            if not dry_run:
                new_state = {
                    "source": str(source),
                    "offset": offset,
                    "sha256": digest.hexdigest(),
                }
                checkpoint.parent.mkdir(parents=True, exist_ok=True)
                temporary = checkpoint.with_suffix(checkpoint.suffix + ".tmp")
                with temporary.open("wb") as output:
                    output.write(orjson.dumps(new_state))
                    output.flush()
                    os.fsync(output.fileno())
                temporary.replace(checkpoint)
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "crypto.trades.v2"))
    parser.add_argument(
        "--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    )
    args = parser.parse_args()
    producer = conn = None

    def handle(record):
        nonlocal producer, conn
        if record["kind"] == "trade":
            from ingestion.kafka_producer import build_producer, publish

            if producer is None:
                producer = build_producer(args.bootstrap)
            publish(producer, args.topic, record["payload"]["event"]).get(timeout=15)
        else:
            import psycopg2

            from streaming.postgres_sink import ensure_tables, upsert_candles

            if conn is None:
                url = os.getenv("DATABASE_URL")
                if not url:
                    raise ValueError("set DATABASE_URL before replaying candles")
                conn = psycopg2.connect(url, connect_timeout=5)
                ensure_tables(conn)
            upsert_candles(conn, [record["payload"]])

    try:
        count = replay_file(
            args.file, handle, checkpoint=args.checkpoint, dry_run=args.dry_run
        )
        print(f"{'validated' if args.dry_run else 'replayed'} {count} recovery records")
        return 0
    except Exception as exc:  # DB/Kafka errors also preserve the checkpoint.
        print(
            f"replay stopped: {type(exc).__name__}; source retained, inspect checkpoint"
        )
        return 1
    finally:
        if producer is not None:
            producer.close(timeout=15)
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
