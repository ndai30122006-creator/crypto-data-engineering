"""Opt-in local recovery drill; briefly stops consumer/Kafka and recreates Pathway.

Run: python -m scripts.verify_recovery --allow-restarts
Uses a unique test symbol, keeps recovery evidence, restores services in finally.
Never deletes volumes or resets Kafka offsets.
"""

import argparse
import os
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import orjson
import psycopg2
from kafka import KafkaProducer

from ingestion.binance_consumer import on_raw_message
from ingestion.kafka_producer import build_producer, publish
from scripts.docker_cli import docker_executable
from scripts.replay_dlq import replay_file
from streaming.postgres_sink import upsert_candles, write_candle_with_retry


def compose(*args):
    subprocess.run([docker_executable(), "compose", *args], check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-restarts", action="store_true", required=True)
    parser.parse_args()
    tag = uuid.uuid4().hex[:8].upper()
    symbol = "RECOVERY" + tag
    folder = Path(".recovery") / tag
    folder.mkdir(parents=True)
    url = os.getenv("DATABASE_URL", "postgresql://admin:secret@localhost:5432/crypto_db")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("KAFKA_TOPIC", "crypto.trades.v2")
    event = {"symbol": symbol, "trade_id": 1, "price": 10.0,
             "quantity": 1.0, "timestamp": 1757578861000}
    conn = psycopg2.connect(url)
    producer = build_producer(bootstrap)
    results = {}

    def row():
        with conn, conn.cursor() as cur:
            cur.execute("SELECT open, high, low, close, volume, trade_count "
                        "FROM market_1m WHERE symbol=%s", (symbol,))
            value = cur.fetchone()
        return tuple(float(v) for v in value) if value else None

    def wait(expected):
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if row() == expected:
                return
            time.sleep(0.5)
        raise AssertionError(f"expected {expected}, got {row()}")

    try:
        publish(producer, topic, event).get(timeout=15)
        wait((10, 10, 10, 10, 1, 1))
        compose("restart", "pathway")
        publish(producer, topic, event).get(timeout=15)
        publish(producer, topic, {**event, "trade_id": 2, "price": 11.0,
                                 "quantity": 2.0, "timestamp": 1757578862000}).get(timeout=15)
        wait((10, 11, 10, 11, 3, 2))
        results["restart_exact_ohlcv"] = True

        # Real terminated DB connection plus refused reconnect endpoint.
        victim = psycopg2.connect(url)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT pg_terminate_backend(%s)", (victim.get_backend_pid(),))
        candle = {"symbol": "DLQ" + tag, "window_start": 1757578860,
                  "open": 1, "high": 1, "low": 1, "close": 1, "volume": 5,
                  "trade_count": 1, "updated_at": datetime.now(UTC).isoformat()}
        source = folder / "candles.jsonl"
        try:
            write_candle_with_retry(victim, candle, retries=2, dlq_path=str(source),
                                    connect=lambda: psycopg2.connect(host="127.0.0.1", port=1,
                                                                    connect_timeout=1))
        except RuntimeError as exc:
            assert "saved to DLQ" in str(exc)
        else:
            raise AssertionError("DB fault did not fail")
        finally:
            victim.close()
        assert replay_file(source, lambda r: upsert_candles(conn, [r["payload"]])) == 1
        assert replay_file(source, lambda r: upsert_candles(conn, [r["payload"]])) == 0
        with conn, conn.cursor() as cur:
            cur.execute("SELECT volume FROM market_1m WHERE symbol=%s", (candle["symbol"],))
            assert float(cur.fetchone()[0]) == 5
        results["terminated_db_dlq_replay_resume"] = True

        # Stop live consumer so only one controlled delivery failure is generated.
        compose("stop", "binance-consumer")
        fault_producer = KafkaProducer(
            bootstrap_servers=bootstrap, acks="all", retries=0, linger_ms=0,
            request_timeout_ms=1000, delivery_timeout_ms=3000, max_block_ms=3000,
            key_serializer=lambda k: k.encode(), value_serializer=orjson.dumps)
        try:
            publish(fault_producer, topic, event).get(timeout=15)  # warm metadata
            compose("stop", "kafka")
            raw = orjson.dumps({"data": {"e": "trade", "s": symbol, "t": 3,
                                        "p": "12", "q": "4", "T": 1757578863000}}).decode()
            trades = folder / "trades.jsonl"
            on_raw_message(fault_producer, {
                "topic": topic, "heartbeat_file": str(folder / "heartbeat"),
                "metrics_file": str(folder / "metrics.json"),
                "delivery_dlq_file": str(trades), "flush_every": 10**9}, raw)
            deadline = time.monotonic() + 20
            while not trades.exists() and time.monotonic() < deadline:
                time.sleep(0.2)
            assert trades.exists(), "Kafka failure did not create recovery record"
        finally:
            fault_producer.close(timeout=5)
            compose("up", "-d", "--wait", "--wait-timeout", "90", "kafka")
        assert replay_file(trades, lambda r: publish(producer, topic, r["payload"]["event"]).get(timeout=15)) == 1
        assert replay_file(trades, lambda r: publish(producer, topic, r["payload"]["event"]).get(timeout=15)) == 0
        wait((10, 12, 10, 12, 7, 3))
        results["stopped_kafka_dlq_replay_resume"] = True

        marker = f"/var/lib/crypto/dlq/verify-{tag}"
        subprocess.run([docker_executable(), "exec", "crypto-pathway", "python", "-c",
                        f"from pathlib import Path; Path('{marker}').write_text('retained')"], check=True)
        compose("up", "-d", "--force-recreate", "--no-deps", "pathway")
        subprocess.run([docker_executable(), "exec", "crypto-pathway", "python", "-c",
                        f"from pathlib import Path; assert Path('{marker}').read_text() == 'retained'"], check=True)
        publish(producer, topic, event).get(timeout=15)
        publish(producer, topic, {**event, "trade_id": 4, "price": 13.0,
                                 "timestamp": 1757578864000}).get(timeout=15)
        wait((10, 13, 10, 13, 8, 4))
        results["recreated_container_state_and_dlq_volume"] = True
        (folder / "results.json").write_bytes(orjson.dumps(results))
        print(orjson.dumps({"evidence": str(folder), **results}).decode())
    finally:
        compose("up", "-d", "--wait", "--wait-timeout", "150", "kafka", "pathway", "binance-consumer")
        producer.close(timeout=15)
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM market_1m WHERE symbol IN (%s,%s)", (symbol, "DLQ" + tag))
        conn.close()


if __name__ == "__main__":
    main()
