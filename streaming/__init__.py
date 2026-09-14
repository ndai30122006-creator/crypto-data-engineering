"""Stream processing: Kafka trades → OHLCV 1m → Postgres.

- windows.py: gom bucket + aggregate thuần Python (test offline được,
  dùng cho verify logic; engine Pathway chạy tương đương trong pipeline).
- postgres_sink.py: ensure + upsert market_1m (idempotent, chạy lại an toàn).
- pathway_pipeline.py: engine Pathway (chỉ chạy trong container Linux).
"""
