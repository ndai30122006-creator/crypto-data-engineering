"""Binance → Kafka ingestion package.

- events.py: pure logic (URL, parse, serialize) — test offline được.
- kafka_producer.py: wrapper KafkaProducer.
- binance_consumer.py: service chạy thường trực (WebSocket + reconnect).
"""
