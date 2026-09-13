"""Wrapper Kafka producer: JSON serialize, key = symbol.

Key = symbol để cùng coin vào cùng partition (giữ thứ tự / coin).
"""
import json

from kafka import KafkaProducer

TOPIC_TRADES = "crypto.trades"


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    """Tạo producer. Gọi 1 lần lúc service start, tái dùng cho mọi event."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=50,
    )


def publish(producer: KafkaProducer, topic: str, event: dict):
    """Publish 1 event, key = symbol. Trả về Future (caller quyết định flush)."""
    return producer.send(topic, key=event["symbol"], value=event)
