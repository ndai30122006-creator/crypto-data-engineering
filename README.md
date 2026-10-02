# crypto-data-engineering

Crypto Data Platform để học Data Engineering: Binance → Kafka → Pathway → PostgreSQL,
RSS/CoinGecko → Dagster → PostgreSQL, quality checks và signals theo giờ.

## Trạng thái hiện tại

Kế hoạch sửa lỗi và nâng cấp: [plan/08-fix-upgrade.md](plan/08-fix-upgrade.md).
Hướng dẫn cutover, migration, replay và nghiệm thu: [docs/upgrade-runbook.md](docs/upgrade-runbook.md).
Review baseline: [docs/reviews/2026-10-02-code-review.md](docs/reviews/2026-10-02-code-review.md).

Đã triển khai contract trade v2, dedupe theo trade ID, OHLC theo event time,
acknowledged/pending/failure metrics, durable DLQ và replay có checkpoint,
quarantine lỗi schema CoinGecko, detector xét mọi cửa sổ 5 phút trong giờ.
Kiểm chứng offline và kiểm chứng live được ghi riêng trong plan; các xác nhận
live trong tài liệu cũ là lịch sử của phiên bản cũ.

## Kiến trúc

```text
Binance WebSocket
  → binance-consumer (validate trade_id, finite price/quantity)
  → Kafka crypto.trades.v2
  → Pathway (dedupe symbol/trade_id → OHLCV 1m)
  → PostgreSQL market_1m (version-aware upsert)

RSS → raw_news → cleaned_news → loaded_news → crypto_news
CoinGecko → fetch_market → validate_market → loaded_snapshot
                                                → crypto_market_snapshot
                     schema/business rejects → data_quality_errors
market_1m → detected_signals (VOLUME_SPIKE) → signals
          → quarantine_ohlcv → data_quality_errors

delivery failures → recovery_dlq volume → replay_dlq (ack/commit + checkpoint)
Pathway snapshots → pathway_state volume
```

Dagster có **8 assets, 6 asset checks, 2 jobs và 2 schedules**, dùng RSS,
CoinGecko và Postgres resources. News chạy mỗi 5 phút; market, signals và
OHLCV quality chạy theo giờ. Bảng batch có suffix `_local`/`_staging`,
production không suffix; `market_1m` và `signals` dùng chung.

## Stack và cấu trúc

Dependencies được khóa trong `uv.lock`; Python 3.12, Dagster, Pathway 0.32.1
(Linux/macOS), kafka-python, psycopg2, httpx, msgspec và Ruff.
Compose có 7 services: postgres, kafka, dagster-code, dagster-webserver,
dagster-daemon, binance-consumer và pathway. Kafka single broker phù hợp local learning.

| Thư mục | Nội dung |
|---|---|
| `dagster_project/` | Assets, quality checks, resources và definitions |
| `ingestion/` | Binance parser, Kafka producer, callbacks, durable DLQ |
| `streaming/` | Graph Pathway, Python spec, sink, signals và quality |
| `database/` | Baseline schema, migrations, analytical queries |
| `scripts/` | Status, metrics, alert, migrations, replay |
| `tests/` | Offline regressions, Linux graph, live integration |
| `plan/` | Các kế hoạch, acceptance criteria, nhật ký thực hiện |
| `docs/` | Review, runbook, observability, hướng dẫn học |

## Chạy local

Cần Docker Desktop với Linux containers và Docker CLI dùng được từ terminal.
Với DB/topic đã có dữ liệu, làm theo **runbook cutover** trước khi start phiên bản mới.

```powershell
Copy-Item .env.example .env
# Sửa .env theo môi trường của bạn.
docker compose config --quiet
docker compose up --build -d
```

Dagster UI: [localhost:3000](http://localhost:3000). Materialize jobs lần đầu,
sau đó bật `news_job_schedule` và `market_job_schedule` trong Automation.

```powershell
uv sync --frozen
uv run ruff check dagster_project ingestion streaming scripts tests
uv run pytest tests/ -q

# PowerShell không tự load .env: đặt DATABASE_URL/DAGSTER_ENVIRONMENT cho commands local.
$env:DAGSTER_ENVIRONMENT="local"
# $env:DATABASE_URL = <connection string cho DB đích>
uv run dagster definitions validate -m dagster_project.definitions
uv run python scripts/migrate.py
uv run python scripts/status.py
uv run python scripts/alert.py

# Khi Kafka/Postgres/Pathway đang chạy:
$env:INTEGRATION="1"
uv run pytest tests/integration/ tests/test_integration.py -q
```

CI bắt buộc import Pathway trên Linux và chạy graph thực; một job riêng
khởi động Kafka/Postgres/Pathway và chạy streaming E2E.

`status.py` dùng cùng alert rules với `alert.py`; exit 1 khi health/data/source
có lỗi. Metrics JSON: `uv run python scripts/metrics.py`.
Chi tiết: [observability](docs/observability.md).

`docker compose down` giữ named volumes. Không dùng `down -v` khi còn cần
Postgres, Kafka, DLQ hoặc Pathway state.

## Giới hạn vận hành

- Trade schema v1 thiếu ID không được trộn vào topic v2. Lịch sử sai trước đây
  chưa được sửa tự động; cần raw trades để backfill/rebuild.
- Binance WebSocket outage có thể bỏ lỡ trades chưa nhận. DLQ chỉ phục hồi
  các events đã nhận nhưng delivery thất bại; REST backfill chưa triển khai.
- Dedupe/state hiện giữ lịch sử, cần theo dõi RAM và dung lượng volume.
  Restart phải giữ state; thay graph/source cần kế hoạch replay riêng.
- Signals hiện là VOLUME_SPIKE batch. PRICE_SPIKE và price change 5m/15m
  là các bước mở rộng chưa triển khai.
- Credentials đi qua environment; `.env` được ignore. Giá trị mặc định
  trong Compose chỉ dành cho môi trường local.
