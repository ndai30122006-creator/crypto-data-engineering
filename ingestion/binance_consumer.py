"""Service thường trực: Binance combined WebSocket → Kafka topic.

Chạy: python -m ingestion.binance_consumer (trong container consumer).
Env: KAFKA_BOOTSTRAP_SERVERS (default kafka:9092),
     KAFKA_TOPIC (default crypto.trades),
     SYMBOLS (comma, default BTC,ETH,BNB,SOL,XRP/USDT).

Không dùng Dagster ở đây: service giữ kết nối WebSocket lâu,
reconnect backoff, publish liên tục — Dagster chỉ orchestrate batch.
"""
import json
import logging
import os
import time

import websocket

from ingestion.events import (
    DEFAULT_SYMBOLS,
    combined_stream_url,
    parse_trade,
)
from ingestion.kafka_producer import TOPIC_TRADES, build_producer, publish

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("binance-consumer")


def load_config() -> dict:
    symbols = [
        s.strip().upper()
        for s in os.getenv("SYMBOLS", ",".join(DEFAULT_SYMBOLS)).split(",")
        if s.strip()
    ]
    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "topic": os.getenv("KAFKA_TOPIC", TOPIC_TRADES),
        "symbols": symbols or list(DEFAULT_SYMBOLS),
    }


def run_forever() -> None:
    cfg = load_config()
    url = combined_stream_url(cfg["symbols"])
    log.info(
        "connect kafka=%s topic=%s symbols=%s",
        cfg["bootstrap_servers"],
        cfg["topic"],
        cfg["symbols"],
    )
    producer = build_producer(cfg["bootstrap_servers"])
    backoff = 1

    while True:
        try:
            ws = websocket.WebSocketApp(
                url,
                on_message=lambda _ws, raw: on_raw_message(
                    producer, cfg["topic"], raw
                ),
                on_error=lambda _ws, err: log.warning("ws error: %s", err),
                on_close=lambda _ws, *a: log.warning("ws closed, reconnecting"),
            )
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except Exception as exc:  # noqa: BLE001 - vòng lặp service không được chết
            log.warning("consumer error: %s", exc)
        log.info("reconnect in %ss", backoff)
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)


def on_raw_message(producer, topic: str, raw: str) -> None:
    """Parse 1 raw WS message → publish nếu là trade hợp lệ."""
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        log.warning("skip non-JSON message")
        return
    event = parse_trade(msg)
    if event is None:
        return
    publish(producer, topic, event)


if __name__ == "__main__":
    run_forever()
