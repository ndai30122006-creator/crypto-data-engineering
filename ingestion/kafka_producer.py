"""Wrapper Kafka producer: JSON serialize, key = symbol.

Key = symbol để cùng coin vào cùng partition (giữ thứ tự / coin).
Delivery: errback log mọi lỗi gửi (không im lặng mất event),
caller flush theo nhịp để đảm bảo event tới broker trước khi thoát.
"""
import orjson
from kafka import KafkaProducer

from ingestion.jlog import get_logger

TOPIC_TRADES = "crypto.trades"

log = get_logger("kafka-producer")


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Tạo producer. Gọi 1 lần lúc service start, tái dùng cho mọi event."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: orjson.dumps(v),
        acks="all",
        retries=5,
        linger_ms=50,
    )


def publish(producer: KafkaProducer, topic: str, event: dict, on_error=None):
    """Publish 1 event, key = symbol. Gắn errback log lỗi delivery.

    on_error(err, event): hook tùy chọn (đếm metric / ghi dead-letter).
    Trả về Future để caller flush theo nhịp.
    """
    future = producer.send(topic, key=event["symbol"], value=event)

    def _failed(exc):
        log.error("delivery failed", exc=exc, topic=topic, symbol=event["symbol"])
        if on_error is not None:
            on_error(exc, event)

    future.add_errback(_failed)
    return future
