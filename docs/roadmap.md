# Roadmap — Crypto Data Platform

Training Data Engineer: batch + streaming + orchestration + storage.
Tổng hợp từ `plan/` (01–06 + README), cập nhật theo code thực tế.

## 0. Toàn cảnh

| Phase | Nội dung | Trạng thái | Chi tiết |
|---|---|---|---|
| 1 | News pipeline (RSS → Dagster → Postgres) | ✅ DONE, live | §1 |
| 2 | Market-cap snapshot job (mỗi 1 giờ) | ✅ DONE, live | §2 |
| 3 | Binance WebSocket → Kafka (realtime) | ✅ DONE, live | §3 |
| 4 | Kafka → Pathway → OHLCV 1m | ⬜ chưa làm | §4 |
| 5 | Tích hợp: correlation, data quality, milestones | ⬜ chưa làm (query 8 đã có khung) | §5 |
| 6 | Resource practice (theo blog Dagster Resources) | ✅ P1–P4 DONE, P5 check tay trên UI | §6 |

Hạ tầng hiện tại (đã vượt plan gốc):

- `uv` thay `pip`: `pyproject.toml` + `uv.lock` + `.python-version` (3.12). Chạy local: `uv sync; uv run pytest tests/ -q`.
- Deploy tách 4 services: `postgres` + `dagster-code` (gRPC 4000) + `dagster-webserver` + `dagster-daemon`. `workspace.yaml` load qua `grpc_server dagster-code:4000`.
- Logger: asset dùng `context.log` + `add_output_metadata`; resource dùng `get_dagster_logger()`. Chỉ dùng `@asset`, `@op` chỉ cần biết.
- Healthcheck cả 4 services trong `docker-compose.yml` (`pg_isready`, socket 4000, `/server_info`, `dagster instance info`).

Nguyên tắc phân biệt tool:

- **Dagster**: orchestration (việc định kỳ) — KHÔNG xử lý realtime
- **Kafka**: message broker (đệm sự kiện) — KHÔNG tính toán
- **Pathway**: stream processing (tính toán realtime) — KHÔNG lưu trữ lâu
- **PostgreSQL**: persistent storage (OLTP + phân tích nhẹ) — KHÔNG phải OLAP

## 1. Phase 1 — News pipeline ✅

Luồng:

```
RSS (4 nguồn) ── */5 min ──▶ raw_news ──▶ cleaned_news ──▶ loaded_news ──▶ crypto_news
```

- `resources/rss.py` (`RSSFeedResource.fetch_raw`) — tải đồng thời 4 feeds (httpx + feedparser). Config ở `config/config.yaml`, fallback `DEFAULT_FEEDS`.
- `news/parser.py` — chuẩn hoá entry: `normalize_url()` bỏ query tracking để dedupe, `sanitize_feed_xml()` lọc `<content:encoded/>` rỗng của CoinDesk (feedparser lấy nó đè mất `description`).
- `news/cleaner.py` — clean HTML, extract symbols, sentiment heuristic, dedupe theo URL.
- `news/schemas.py` — Pydantic `RawArticle` / `CleanArticle`.
- `assets/news_assets.py` — 3 assets nối nhau, log + metadata đầy đủ.
- `definitions.py` — `news_job` + schedule `*/5 * * * *`.
- `database/schema.sql` — bảng `crypto_news` (url UNIQUE).
- Test: `tests/test_cleaner.py` (8 tests).

Kết quả verify lúc triển khai: fetch live 165 articles / 0 lỗi nguồn, DB 190+ rows (132+ có symbols).

Còn lại (vận hành): bật `news_job_schedule` = ON trong Dagster UI (Automation tab).

## 2. Phase 2 — Market-cap snapshot ✅

Luồng:

```
CoinGecko API ── 0 * * * * ──▶ fetch_market ──▶ validate_market ──▶ loaded_snapshot ──▶ crypto_market_snapshot
                                                                                  └─▶ data_quality_errors
```

- `resources/coingecko.py` (`CoinGeckoResource.fetch_markets`) — top 50 coin, retry/backoff khi 429/5xx/timeout, log warning mỗi lần retry.
- `market/schemas.py` — Pydantic `RawMarket` + `from_coingecko(item)` cách ly mapping field (`current_price` → `price`...).
- `market/validator.py` — rules `price > 0`, `market_cap > 0`, `symbol` non-empty; `validate()` tách `(valid, errors)`, KHÔNG drop lặng lẽ.
- `assets/market_assets.py` — `fetch_market` → `validate_market` → `loaded_snapshot` (ghi snapshot + quarantine bad records vào `data_quality_errors`).
- `definitions.py` — `market_job` + schedule `0 * * * *`.
- Schema: `crypto_market_snapshot` PK `(collected_at, symbol)` + `data_quality_errors` (JSONB payload).
- Test: `tests/test_market.py` (mock JSON, offline).

Verify:

```sql
SELECT count(*) FROM crypto_market_snapshot;            -- ~50 / batch
SELECT symbol, price, price_change_24h                  -- top gainers
FROM crypto_market_snapshot ORDER BY price_change_24h DESC LIMIT 10;
SELECT symbol, volume_24h                               -- volume leaders
FROM crypto_market_snapshot ORDER BY volume_24h DESC LIMIT 10;
```

## 3. Phase 3 — Binance → Kafka ✅

Ingest giá/trade realtime 5 cặp (BTCUSDT, ETHUSDT, BNBUSDT, SOLUSDT, XRPUSDT) vào topic `crypto.trades`. KHÔNG dùng Dagster (Dagster = batch, không giữ WebSocket lâu).

```
Binance WebSocket ──▶ binance-consumer ──▶ Kafka (topic crypto.trades) ──▶ (Phase 4: Pathway)
```

- Services: `kafka` (apache/kafka 3.9, KRaft, healthcheck `kafka-topics.sh`), `binance-consumer` (image `Dockerfile.consumer`, uv, restart unless-stopped).
- Kafka single-broker + RF=1 là chủ ý cho local (nhẹ); lên prod mới cần 3 broker + RF=3 (xem comment trong compose).
- `ingestion/events.py` — pure logic (URL combined stream, `parse_trade` validate price/quantity > 0, serialize JSON). Test offline `tests/test_ingestion.py` (5 tests).
- `ingestion/kafka_producer.py` — wrapper producer, **key = symbol** (cùng coin → cùng partition, giữ thứ tự).
- `ingestion/binance_consumer.py` — service thường trực: `WebSocketApp` + reconnect backoff (1s → max 60s), log chuẩn. Config qua env `KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_TOPIC` / `SYMBOLS`.
- Verify live: `kafka-console-consumer --topic crypto.trades` thấy event chảy (`{"symbol": "ETHUSDT", "price": ..., ...}`).
- Còn lại: consumer chết 10 phút → restart đọc tiếp (offset commit) — tự kiểm chứng khi cần.

## 4. Phase 4 — Kafka → Pathway → OHLCV 1m ⬜

```
Kafka crypto.trades ──▶ Pathway ──▶ market_1m (OHLCV)
                                ──▶ signals (VOLUME_SPIKE, PRICE_SPIKE)
```

- `market_1m`: `(symbol, window_start)` PK + open/high/low/close/volume/trade_count/`price_change_1m`.
- `signals`: `signal_type` + `window_start` + `details` JSONB.
- Metrics: `price_change_1m/5m/15m`; volume spike `volume_5m / avg_volume_1h > 3`.
- Files: `streaming/pathway_pipeline.py` (`pw.kafka.read` → `windowby` 1 phút theo event_time → `reduce` → `pw.postgres.write`), `Dockerfile.pathway`.
- Quality OHLCV vi phạm → `data_quality_errors`.
- Verify sau 2–3 phút: `SELECT * FROM market_1m ORDER BY window_start DESC LIMIT 5;`

## 5. Phase 5 — Tích hợp ⬜

1. **News ↔ Market correlation** (±10 phút quanh biến động mạnh) — khung query đã có trong `database/queries.sql` (query 8, chờ bảng `market_1m`).
2. **Data quality tổng**: Binance (`price/quantity > 0`, NOT NULL) · News (url/title NOT NULL, url UNIQUE) · OHLCV (high ≥ low/open/close...) → `data_quality_errors`.
3. **Milestones**: L1 Batch ✅ (+ Phase 2 ✅) · L2 Streaming ⬜ · L3 Stream Processing ⬜ · L4 Integration ⬜.
4. **15 câu hỏi tự kiểm tra** (Kafka 5 / Dagster 4 / Pathway 3 / Postgres 3) — xem `plan/05-integration.md`.
5. **Compose cuối mục tiêu**: kafka, postgres, dagster (code+webserver+daemon), binance-consumer, pathway (+ pgadmin optional) — `docker compose up` 1 lệnh.

## 6. Phase 6 — Resource practice ✅ P1–P4

Theo https://dagster.io/blog/a-practical-guide-to-dagster-resources.

| Ý blog | Thực trạng lúc đó | Practice | Nay |
|---|---|---|---|
| What Are Resources | `PostgresResource` chỉ có conn + get_conn | P1 audit | ✅ `table()` env-aware + `insert_news/snapshot/errors` |
| API Encapsulation | asset gọi httpx trực tiếp | P2 bọc resource | ✅ `RSSFeedResource` + `CoinGeckoResource`, assets sạch httpx/feedparser/psycopg2 |
| Environment Management | Chưa phân biệt env | P3 `DAGSTER_ENVIRONMENT` | ✅ local ghi `*_local`, prod tên gốc |
| Dependency Injection | fetcher chưa sạch | P2 asset chỉ gọi `fetch_*()/insert_*()` | ✅ |
| Testing Without Pain | Chỉ test hàm thuần | P4 mock | ✅ `tests/test_resources.py`, offline < 2s |
| Configuration | `DATABASE_URL` qua EnvVar ✅ | P5 Deployment tab | ⏳ check tay trên UI (secret chỉ hiện tên biến) |

Thứ tự P1 → P2 → P3 → P4 → P5. Chi tiết từng practice xem `plan/06-resource-practice.md`, guide học xem `docs/resource-practice-guide.md`.

## Nguồn

- Plan gốc: `plan/01-news-pipeline.md` … `plan/06-resource-practice.md`, `plan/README.md`.
- Guide học: `docs/project-overview.md`, `docs/news-pipeline-tools.md`, `docs/market-cap-guide.md`, `docs/correlation-data-quality.md`, `docs/resource-practice-guide.md`.
