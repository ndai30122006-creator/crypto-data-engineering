# Phase 3 — Binance WebSocket → Kafka (realtime)

## Mục tiêu

Ingest giá/trade realtime 5 cặp (BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT)
vào Kafka topic `crypto.trades`. KHÔNG dùng Dagster cho việc này
(Dagster = batch, không giữ kết nối WebSocket lâu).

## Kiến trúc thêm vào docker-compose

```
Binance WebSocket ──▶ binance-consumer ──▶ Kafka (topic crypto.trades) ──▶ (Phase 4: Pathway)
                                          ▲
                                     postgres (market_trades, optional direct sink)
```

Services mới: `kafka` (Apache Kafka image, chế độ KRaft, không cần Zookeeper),
`binance-consumer` (Python service chạy thường trực).

## Event format

```json
{"symbol": "BTCUSDT", "price": 112345.12, "quantity": 0.023, "timestamp": 1757578923123}
```

- Key Kafka = `symbol` (đảm bảo cùng coin vào cùng partition, giữ thứ tự)
- Value = JSON trên

## Files cần tạo

- `ingestion/binance_consumer.py` — kết nối Binance combined WebSocket
  `wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/...`,
  vòng lặp reconnect (backoff), publish qua `kafka-python`/`confluent-kafka`
- `ingestion/kafka_producer.py` — wrapper producer (JSON serialize, key=symbol)
- `Dockerfile.consumer`
- Thêm service `kafka` + `binance-consumer` vào `docker-compose.yml`

## Schema (optional direct sink, Level 2)

```sql
CREATE TABLE IF NOT EXISTS market_trades (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC(20,8) NOT NULL,
    quantity NUMERIC(30,12) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_trades_symbol_time
    ON market_trades (symbol, event_time DESC);
```

## Sinh viên phải giải quyết

WebSocket reconnect, partition key, JSON serialization, timestamp
(event_time vs received_at), duplicate event, logging, restart an toàn.

## Verify

- `docker exec <kafka> kafka-console-consumer --topic crypto.trades --from-beginning`
  thấy event chảy liên tục
- Consumer chết 10 phút → restart vẫn đọc tiếp (offset commit)
