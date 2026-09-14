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
from pathlib import Path

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

_last_beat = 0.0


def beat(path: str, interval: float = 10.0, now: float | None = None) -> bool:
    """Touch heartbeat file tối đa mỗi `interval` giây.

    Healthcheck của Docker đọc độ tươi file này: process treo hoặc
    WS chết mà process còn sống → file cũ → unhealthy.
    Trả True nếu vừa touch (để test được mà không cần sleep).
    """
    global _last_beat
    t = now if now is not None else time.time()
    if t - _last_beat < interval and os.path.exists(path):
        return False
    try:
        Path(path).touch()
    except OSError:
        return False
    _last_beat = t
    return True


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
        "heartbeat_file": os.getenv(
            "HEARTBEAT_FILE", "/tmp/binance-consumer.heartbeat"
        ),
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
                    producer, cfg["topic"], raw, cfg["heartbeat_file"]
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


def on_raw_message(producer, topic: str, raw: str, heartbeat_path: str) -> None:
    """Parse 1 raw WS message → publish nếu là trade hợp lệ + đập nhịp tim."""
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        log.warning("skip non-JSON message")
        return
    event = parse_trade(msg)
    if event is None:
        return
    publish(producer, topic, event)
    beat(heartbeat_path)


if __name__ == "__main__":
    run_forever()
