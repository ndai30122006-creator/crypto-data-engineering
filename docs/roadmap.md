# Roadmap — Crypto Data Platform

Training Data Engineer: batch + streaming + orchestration + storage.
Tổng hợp từ `plan/` (01–06 + README), cập nhật theo code thực tế.

## 0. Toàn cảnh

| Phase | Nội dung | Trạng thái | Chi tiết |
|---|---|---|---|
| 1 | News pipeline (RSS → Dagster → Postgres) | ✅ DONE, live | §1 |
| 2 | Market-cap snapshot job (mỗi 1 giờ) | ✅ DONE, live | §2 |
| 3 | Binance WebSocket → Kafka (realtime) | ✅ DONE, live | §3 |
| 4 | Kafka → Pathway → OHLCV 1m | ✅ DONE, live (145 nến, đủ 5 symbols) | §4 |
| 5 | Tích hợp: correlation query, data quality, milestones | ✅ query live (0 match — max 0.13% < ngưỡng 1%) | §5 |
| 6 | Resource practice (theo blog Dagster Resources) | ✅ P1–P4 DONE, P5 check tay trên UI | §6 |

Hạ tầng hiện tại (đã vượt plan gốc):
- `uv` thay `pip`: `pyproject.toml` + `uv.lock` + `.python-version` (3.12). Chạy local: `uv sync; uv run pytest tests/ -q`.
- Deploy tách 4 services: `postgres` + `dagster-code` (gRPC 4000) + `dagster-webserver` + `dagster-daemon`. `workspace.yaml` load qua `grpc_server dagster-code:4000`.
- Logger: asset dùng `context.log` + `add_output_metadata`; resource dùng `get_dagster_logger()`. Chỉ dùng `@asset`, `@op` chỉ cần biết.
- Healthcheck cả 6 services trong `docker-compose.yml` (postgres, code, webserver, daemon, kafka, consumer heartbeat).
- Data quality: `quality/checks.py` pure (freshness/dup/null/row-count/schema/metrics) + 6 `@asset_check` đăng ký trong `defs`, test `tests/test_quality.py`.
- Logger service: jlogger (JSON structured, Rust-backed) qua wrapper `ingestion/jlog.py` — consumer/producer/pathway log `info(msg, **fields)`; thiếu dep thì fallback stdlib cùng cú pháp. Dagster giữ `context.log` riêng.
- Observability LOG→METRIC→HEALTH→ALERT: `scripts/metrics.py` (JSON), `scripts/alert.py` (ngưỡng + exit code), `scripts/status.py` (11 checks) — chi tiết `docs/observability.md`.
- Libs tốc độ: msgspec (validate Struct), orjson (thay json), ciso8601 đọc ISO nhanh + dateutil fallback RFC-2822.

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
- `news/schemas.py` — msgspec Struct `RawArticle` / `CleanArticle`.
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
- `market/schemas.py` — msgspec Struct `RawMarket` + `from_coingecko(item)` cách ly mapping field (`current_price` → `price`...).
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
- Reliability: RSS retry/backoff (4xx fail nhanh, async sleep); CoinGecko retry theo status (429/5xx + Retry-After + jitter, 4xx fail nhanh); producer errback log + flush theo nhịp (500) + flush/close khi SIGTERM/SIGINT; consumer heartbeat file cho healthcheck.
- Tests: unit offline (`test_ingestion/reliability`, mock httpx) + integration (`test_integration.py`, chạy với `INTEGRATION=1` khi stack lên: Kafka roundtrip, pipeline → Kafka thật, Postgres roundtrip).
- Còn lại: consumer chết 10 phút → restart đọc tiếp (offset commit) — tự kiểm chứng khi cần.

## 4. Phase 4 — Kafka → Pathway → OHLCV 1m ✅

```
Kafka crypto.trades ──▶ Pathway ──▶ market_1m (OHLCV)
```

- `streaming/windows.py` — spec bucket + aggregate thuần Python (test offline, đối chiếu engine).
- `streaming/pathway_pipeline.py` — engine Pathway 0.32.1 (API đã verify trong image: `pw.io.kafka.read` + `windowby(tumbling 60s)` + `reduce` + `subscribe`): `TradeSchema` parse JSON, bucket epoch-seconds, OHLCV theo symbol.
- `streaming/postgres_sink.py` — upsert `(symbol, window_start)` idempotent (replay/restart an toàn). `price_change_1m` để NULL — query tự tính bằng `LAG()` (stateless).
- Service `pathway` (`Dockerfile.pathway`, uv, healthcheck process qua `/proc`).
- Bài học E2E test bắt được: `earliest/latest` của engine theo processing-time
  nên sai open/close khi burst — chuyển sang min/max composite key `ts|price`
  (đúng data-time mọi thứ tự arrival). Producer kafka-python không có
  idempotence nên test assert OHLC chính xác + volume/count `>=`.
- Verify live: 145 nến, đủ 5 symbols, OHLC hợp lệ (BTC 77522–77549).
- Chưa làm (giữ đúng scope): `price_change_5m/15m`, volume-spike detector trong engine.
- Verify sau 2–3 phút: `SELECT * FROM market_1m ORDER BY window_start DESC LIMIT 5;`

## 12. Signals batch (VOLUME_SPIKE) ✅

- `streaming/signals.py` pure: spike khi volume 5m > 3× baseline giờ (cần 65 nến/symbol).
- Asset `detected_signals` (theo giờ, trong `market_job`): quét 70 phút → ghi bảng `signals` (UNIQUE symbol/type/window). Verify live: scan 75 nến → 0 signals (thị trường yên, đúng hành vi).
- Test `tests/test_signals.py` (5 tests: thiếu baseline, phẳng, spike, đa symbol, asset mock).

## 13. OHLCV quality wiring ✅

- `streaming/quality_ohlcv.py` pure: high≥low/open/close, low≤open/close, volume≥0, count int>0 → violations + `to_errors()` (pipeline=`ohlcv`).
- Asset `quarantine_ohlcv` (theo giờ, trong `market_job`): quét 70 phút → vi phạm vào `data_quality_errors`. Verify live: 115 nến → 0 violations (engine sạch).
- Test `tests/test_quality_ohlcv.py` (6 tests: sạch, high/low, volume/count, thiếu field, format errors, asset mock).

## 5. Phase 5 — Tích hợp ✅ query live

1. **News ↔ Market correlation** (±10 phút quanh nến biến động > 1%): query 8 trong `database/queries.sql` đã chạy live — 0 match vì max biến động 1m hiện tại 0.13% < ngưỡng (đúng hành vi, không phải bug). `price_change` tính bằng `LAG(close)` vì sink để NULL cho stateless.
2. **Data quality tổng**: Binance (`price/quantity > 0`, NOT NULL) · News (url/title NOT NULL, url UNIQUE) · OHLCV (high ≥ low/open/close...) → `data_quality_errors`.
3. **Milestones**: L1 Batch ✅ · L2 Streaming ✅ (consumer → Kafka live) · L3 Stream Processing ✅ (Pathway → OHLCV live) · L4 Integration ✅ (chung 1 DB + query correlation live).
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

## 7. Reliability (6–10) ✅

- RSS retry/backoff (4xx fail nhanh, async sleep) · CoinGecko retry theo status (429/5xx + Retry-After + jitter, TransportError).
- Producer errback log + flush nhịp (500) + flush/close khi SIGTERM/SIGINT; consumer heartbeat + graceful shutdown (đóng WS, `stop_grace_period: 30s`).
- Tests: `tests/test_reliability.py` (mock httpx: retry, 429, 403 fail-nhanh) + `tests/test_integration.py` (`INTEGRATION=1`: Kafka roundtrip, pipeline → Kafka thật, Postgres roundtrip).

## 8. Data quality (11–16) ✅

- `quality/checks.py` pure: freshness, duplicates, nulls, row-count bounds, schema (msgspec), `summarize` metrics.
- 6 `@asset_check` trong `defs`: news freshness/dup/null/count+schema, market count+schema+nulls, market metrics. Non-blocking (đỏ nhưng không chặn pipeline).

## 9. Event-time correctness (Phase 2) ✅

- Window theo **event time** (`timestamp` Binance), không phải processing time.
- Out-of-order/late: đúng nhờ composite key `ts|price` + upsert (xem §4 bài học).
- Regression live `tests/integration/test_event_time.py`: out-of-order, late, duplicate, multiple-symbols.

## 10. E2E streaming (Phase 1) ✅

- `tests/integration/test_streaming_e2e.py`: fake events → Kafka thật → engine live → `market_1m` → assert đủ 8 fields + idempotency republish + invalid bỏ qua + wait timeout 60s.
- Helpers dùng chung `tests/integration/helpers.py` (env override, skip khi thiếu infra, cleanup theo symbol).

## 11. Operations ✅

- Observability LOG→METRIC→HEALTH→ALERT: `scripts/metrics.py` (JSON: runs, kafka lag/rate, binance counters, pathway engine, DB), `scripts/alert.py` (6 rules + exit code), `scripts/status.py` (dashboard) — xem `docs/observability.md`.
- Failure handling: Kafka chết (retry vô hạn, không crash) · WS rớt (reconnect) · invalid reject+log+metric (hướng DLQ) · Postgres chết (sink retry 3 + DLQ file + raise to).
- Migrations: `scripts/migrate.py` + `database/migrations/` (versioned, idempotent). Secrets: env-only (`.env` ignore khỏi git).
- Libs: msgspec (validate), orjson (JSON), ciso8601 + dateutil-fallback (ngày), jlogger (log JSON services; Dagster giữ `context.log` riêng).

## Nguồn

- Plan gốc: `plan/01-news-pipeline.md` … `plan/06-resource-practice.md`, `plan/README.md`.
- Guide học: `docs/project-overview.md`, `docs/news-pipeline-tools.md`, `docs/market-cap-guide.md`, `docs/correlation-data-quality.md`, `docs/resource-practice-guide.md`.
