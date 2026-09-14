"""Service thường trực: Binance combined WebSocket → Kafka topic.

Chạy: python -m ingestion.binance_consumer (trong container consumer).
Env: KAFKA_BOOTSTRAP_SERVERS (default kafka:9092),
     KAFKA_TOPIC (default crypto.trades),
     SYMBOLS (comma, default BTC,ETH,BNB,SOL,XRP/USDT),
     HEARTBEAT_FILE (default /tmp/binance-consumer.heartbeat),
     FLUSH_EVERY (số event giữa 2 lần flush, default 500).

Không dùng Dagster ở đây: service giữ kết nối WebSocket lâu,
reconnect backoff, publish liên tục — Dagster chỉ orchestrate batch.

Shutdown (SIGTERM/SIGINT từ docker stop): ngừng reconnect, đóng WS,
flush producer (đảm bảo event đã gửi tới broker) rồi mới thoát.
"""
import json
import logging
import os
import signal
import threading
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

_shutdown = threading.Event()
_current_ws: websocket.WebSocketApp | None = None
_last_beat = 0.0
_published = 0
_flushed_at = 0


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
        "flush_every": _positive_int(os.getenv("FLUSH_EVERY"), 500),
    }


def _positive_int(raw: str | None, default: int) -> int:
    """Parse env số nguyên dương, sai format → default (không crash service)."""
    try:
        value = int(raw) if raw is not None else default
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


def _handle_signal(signum, _frame) -> None:
    global _current_ws
    log.info("received signal %s, shutting down", signum)
    _shutdown.set()
    # Đánh thức run_forever đang block: không có dòng này, docker stop
    # phải chờ hết ping timeout rồi ăn SIGKILL → flush không kịp chạy.
    ws, _current_ws = _current_ws, None
    if ws is not None:
        try:
            ws.close()
        except Exception as exc:  # noqa: BLE001 - đang shutdown, cứ thoát
            log.warning("ws close on shutdown failed: %s", exc)


def run_forever() -> None:
    global _current_ws
    cfg = load_config()
    url = combined_stream_url(cfg["symbols"])
    log.info(
        "connect kafka=%s topic=%s symbols=%s",
        cfg["bootstrap_servers"],
        cfg["topic"],
        cfg["symbols"],
    )
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    producer = build_producer(cfg["bootstrap_servers"])
    backoff = 1

    try:
        while not _shutdown.is_set():
            ws: websocket.WebSocketApp | None = None
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_message=lambda _ws, raw: on_raw_message(
                        producer, cfg, raw
                    ),
                    on_error=lambda _ws, err: log.warning("ws error: %s", err),
                    on_close=lambda _ws, *a: log.warning("ws closed, reconnecting"),
                )
                if _shutdown.is_set():
                    break
                _current_ws = ws
                connected_at = time.time()
                ws.run_forever(ping_interval=60, ping_timeout=10)
                # Kết nối sống lâu rồi mới rớt → reset backoff về 1s
                # (lỗi chớp nhoáng không đáng bị chờ 60s).
                if time.time() - connected_at > 60:
                    backoff = 1
            except Exception as exc:  # noqa: BLE001 - vòng lặp service không được chết
                log.warning("consumer error: %s", exc)
            finally:
                _current_ws = None
            if _shutdown.is_set():
                break
            # Đóng WS trước khi reconnect để không rò rỉ kết nối cũ.
            if ws is not None:
                ws.close()
            log.info("reconnect in %ss", backoff)
            _shutdown.wait(backoff)
            backoff = min(backoff * 2, 60)
    finally:
        # Graceful shutdown: đẩy hết event còn kẹt rồi mới thoát.
        log.info("flushing producer (published=%d)", _published)
        try:
            producer.flush(timeout=15)
        finally:
            producer.close()
        log.info("shutdown complete")


def on_raw_message(producer, cfg: dict, raw: str) -> None:
    """Parse 1 raw WS message → publish nếu là trade hợp lệ + đập nhịp tim."""
    global _published, _flushed_at
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        log.warning("skip non-JSON message")
        return
    event = parse_trade(msg)
    if event is None:
        return
    publish(producer, cfg["topic"], event)
    _published += 1
    # Flush theo nhịp: đảm bảo event tới broker kể cả khi crash giữa chừng.
    if _published - _flushed_at >= cfg.get("flush_every", 500):
        producer.flush()
        _flushed_at = _published
    beat(cfg["heartbeat_file"])


if __name__ == "__main__":
    run_forever()
