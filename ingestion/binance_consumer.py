"""Service thường trực: Binance combined WebSocket → Kafka topic.

Chạy: python -m ingestion.binance_consumer (trong container consumer).
Env: KAFKA_BOOTSTRAP_SERVERS (default kafka:9092),
     KAFKA_TOPIC (default crypto.trades.v2),
     SYMBOLS (comma, default BTC,ETH,BNB,SOL,XRP/USDT),
     HEARTBEAT_FILE (default /tmp/binance-consumer.heartbeat),
     FLUSH_EVERY (số event giữa 2 lần flush, default 500).

Không dùng Dagster ở đây: service giữ kết nối WebSocket lâu,
reconnect backoff, publish liên tục — Dagster chỉ orchestrate batch.

Shutdown (SIGTERM/SIGINT từ docker stop): ngừng reconnect, đóng WS,
flush producer (đảm bảo event đã gửi tới broker) rồi mới thoát.
"""

import os
import signal
import threading
import time
from pathlib import Path

import orjson
import websocket

from ingestion.dlq import append_record
from ingestion.events import (
    DEFAULT_SYMBOLS,
    combined_stream_url,
    parse_trade,
)
from ingestion.jlog import get_logger
from ingestion.kafka_producer import TOPIC_TRADES, build_producer, publish

log = get_logger("binance-consumer")
_backfill = None

_shutdown = threading.Event()
_current_ws: websocket.WebSocketApp | None = None
_last_beat = 0.0
_flushed_at = 0
_started_at = time.time()
_counter_lock = threading.Lock()
_metrics_lock = threading.Lock()
_counters = {
    "events_received_total": 0,
    "events_invalid_total": 0,
    "events_dlq_total": 0,
    "events_published_total": 0,
    "events_enqueued_total": 0,
    "events_pending": 0,
    "events_delivery_dlq_total": 0,
    "dlq_write_failures_total": 0,
    "publish_failures_total": 0,
    "reconnect_total": 0,
    "last_flush_latency_s": 0.0,
    "max_flush_latency_s": 0.0,
}


def snapshot_metrics() -> dict:
    """Counters hiện tại + uptime (cho metrics file / status)."""
    with _counter_lock:
        stats = {**_counters, "uptime_seconds": round(time.time() - _started_at, 1)}
    if _backfill is not None:
        stats["backfill"] = {"enabled": True, **_backfill.snapshot()}
    return stats


def _count(**changes) -> None:
    with _counter_lock:
        for key, delta in changes.items():
            _counters[key] += delta


def dump_metrics(path: str) -> bool:
    """Ghi counters ra JSON file (metrics script đọc qua docker exec)."""
    destination = Path(path)
    temporary = destination.with_name(destination.name + ".tmp")
    with _metrics_lock:
        try:
            temporary.write_bytes(orjson.dumps(snapshot_metrics()))
            temporary.replace(destination)
            return True
        except OSError:
            return False


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
        "metrics_file": os.getenv("METRICS_FILE", "/tmp/binance-metrics.json"),
        # Topic chứa event invalid (rỗng = tắt DLQ, chỉ log + metric).
        "dlq_topic": os.getenv("KAFKA_DLQ_TOPIC", "crypto.dlq"),
        "delivery_dlq_file": os.getenv(
            "DELIVERY_DLQ_FILE", "/var/lib/crypto/dlq/trades.jsonl"
        ),
        "backfill_enabled": os.getenv("BACKFILL_ENABLED", "false").lower() == "true",
        "backfill_state_file": os.getenv("BACKFILL_STATE_FILE", "/var/lib/crypto/ingestion-state/gaps.json"),
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
    log.info("received signal, shutting down", signal=signum)
    _shutdown.set()
    # Đánh thức run_forever đang block: không có dòng này, docker stop
    # phải chờ hết ping timeout rồi ăn SIGKILL → flush không kịp chạy.
    ws, _current_ws = _current_ws, None
    if ws is not None:
        try:
            ws.close()
        except Exception as exc:  # noqa: BLE001 - đang shutdown, cứ thoát
            log.warning("ws close on shutdown failed", exc=exc)


def run_forever() -> None:
    global _current_ws, _backfill
    cfg = load_config()
    url = combined_stream_url(cfg["symbols"])
    log.info(
        "connect",
        kafka=cfg["bootstrap_servers"],
        topic=cfg["topic"],
        symbols=cfg["symbols"],
    )
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    producer = build_producer(cfg["bootstrap_servers"])
    backoff = 1

    try:
        if cfg["backfill_enabled"]:
            from ingestion.backfill import BackfillWorker, GapState

            state = GapState(cfg["backfill_state_file"], cfg["bootstrap_servers"] + "/" + cfg["topic"])
            _backfill = BackfillWorker(state, producer, cfg["topic"],
                                       fatal_callback=lambda: _handle_signal(0, None))
            cfg["backfill"] = _backfill
            _backfill.start()
        while not _shutdown.is_set():
            ws: websocket.WebSocketApp | None = None
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_message=lambda _ws, raw: on_raw_message(producer, cfg, raw),
                    on_error=lambda _ws, err: log.warning("ws error", err=str(err)),
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
                log.warning("consumer error", exc=exc)
            finally:
                _current_ws = None
            if _shutdown.is_set():
                break
            # Đóng WS trước khi reconnect để không rò rỉ kết nối cũ.
            if ws is not None:
                ws.close()
            log.info("reconnect", backoff_s=backoff)
            _count(reconnect_total=1)
            _shutdown.wait(backoff)
            backoff = min(backoff * 2, 60)
    finally:
        # Graceful shutdown: đẩy hết event còn kẹt rồi mới thoát.
        log.info("flushing producer", published=_counters["events_published_total"])
        try:
            if _backfill is not None:
                _backfill.close()
        finally:
            try:
                producer.flush(timeout=15)
            finally:
                producer.close()
                if _backfill is not None:
                    _backfill.state.flush()  # include final live delivery acknowledgements
                    _backfill = None
        log.info("shutdown complete")


def _to_dlq(producer, cfg: dict, raw: str, reason: str) -> None:
    """Gửi event invalid vào DLQ topic (rỗng KAFKA_DLQ_TOPIC = tắt)."""
    dlq_topic = cfg.get("dlq_topic", "crypto.dlq")
    if not dlq_topic:
        return
    try:
        payload = raw if isinstance(raw, str) else str(raw)
        future = producer.send(
            dlq_topic, key="invalid", value={"raw": payload[:2000], "reason": reason}
        )
        future.add_callback(lambda _metadata: _count(events_dlq_total=1))
        future.add_errback(
            lambda exc: log.warning("invalid DLQ delivery failed", exc=exc)
        )
    except Exception as exc:  # noqa: BLE001 - DLQ không được làm chết luồng chính
        log.warning("dlq send failed", exc=exc)


def on_raw_message(producer, cfg: dict, raw: str) -> None:
    """Parse 1 raw WS message → publish nếu là trade hợp lệ + đập nhịp tim."""
    global _flushed_at
    try:
        msg = orjson.loads(raw)
    except (orjson.JSONDecodeError, TypeError):
        log.warning("skip non-JSON message")
        _count(events_received_total=1, events_invalid_total=1)
        _to_dlq(producer, cfg, raw, "non-json")
        return
    event = parse_trade(msg)
    if event is None:
        # Invalid (giá/qty <= 0, thiếu field...): reject + log + metric + DLQ.
        log.warning("skip invalid trade", raw=str(raw)[:200])
        _count(events_received_total=1, events_invalid_total=1)
        _to_dlq(producer, cfg, raw, "invalid-trade")
        return

    backfill = cfg.get("backfill")
    if backfill is not None:
        try:
            backfill.state.observe(event)
        except OSError as exc:
            log.error("gap checkpoint failed; stopping consumer", exc=exc)
            backfill.fatal(exc)
            dump_metrics(cfg.get("metrics_file", "/tmp/binance-metrics.json"))
            return

    settled = False
    settle_lock = threading.Lock()

    def _settle(success: bool) -> bool:
        nonlocal settled
        with settle_lock:
            if settled:
                return False
            settled = True
        _count(
            events_pending=-1,
            **{
                "events_published_total" if success else "publish_failures_total": 1,
            },
        )
        return True

    def _acknowledged(_metadata, _event) -> None:
        if not _settle(True):
            return
        if backfill is not None:
            try:
                backfill.state.ack(_event)
            except OSError as exc:
                backfill.fatal(exc)
                log.error("ack checkpoint failed; stopping consumer", exc=exc)
        if beat(cfg["heartbeat_file"]):
            dump_metrics(cfg.get("metrics_file", "/tmp/binance-metrics.json"))

    def _failed(exc, failed_event) -> None:
        if not _settle(False):
            return
        try:
            append_record(
                cfg.get("delivery_dlq_file", "/var/lib/crypto/dlq/trades.jsonl"),
                "trade",
                {"topic": cfg["topic"], "event": failed_event},
                type(exc).__name__,
            )
        except OSError as dlq_error:
            _count(dlq_write_failures_total=1)
            log.error(
                "delivery failed and DLQ write failed",
                exc=dlq_error,
                symbol=event["symbol"],
            )
        else:
            _count(events_delivery_dlq_total=1)
            log.error("delivery failed, saved to recovery DLQ", symbol=event["symbol"])
        if backfill is not None:
            try:
                backfill.state.failed(failed_event)
            except OSError as exc:
                backfill.fatal(exc)
                log.error("failed delivery checkpoint failed; stopping consumer", exc=exc)
        dump_metrics(cfg.get("metrics_file", "/tmp/binance-metrics.json"))

    _count(events_received_total=1, events_pending=1)
    try:
        publish(
            producer, cfg["topic"], event, on_error=_failed, on_success=_acknowledged
        )
    except Exception as exc:  # send can fail before returning a Future
        _failed(exc, event)
        return
    _count(events_enqueued_total=1)
    # Flush theo nhịp: đảm bảo event tới broker kể cả khi crash giữa chừng.
    # Đo latency flush = proxy cho publish latency (send bất đồng bộ).
    enqueued = snapshot_metrics()["events_enqueued_total"]
    if enqueued - _flushed_at >= cfg.get("flush_every", 500):
        started = time.time()
        producer.flush()
        latency = time.time() - started
        with _counter_lock:
            _counters["last_flush_latency_s"] = round(latency, 3)
            _counters["max_flush_latency_s"] = round(
                max(_counters["max_flush_latency_s"], latency), 3
            )
        _flushed_at = enqueued


if __name__ == "__main__":
    run_forever()
