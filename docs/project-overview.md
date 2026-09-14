# Document toàn bộ dự án — crypto-data-engineering
*(Từ đầu đến hiện tại. File local only, không commit.)*

---

## 1. Nguồn gốc & mục tiêu

Anh bạn nhờ triển khai bài **training Data Engineer**: xây mini Crypto Data
Platform chạm đủ 5 mảng — batch, streaming, orchestration, database,
real-time processing — với stack: Airflow/Dagster, Kafka, PostgreSQL,
Pathway, dữ liệu giá Binance + news + market-cap.

Chọn **Dagster** (thay vì Airflow) cho orchestration. Project làm theo 5 phase
(`plan/README.md`), hiện xong **Phase 1 + 2**.

## 2. Kiến trúc hiện tại

```
RSS (4 nguồn) ── */5 min ──▶ raw_news ──▶ cleaned_news ──▶ loaded_news ──▶ crypto_news
                                                         (PostgreSQL)

CoinGecko API ── 0 * * * * ─▶ fetch_market ──▶ validate_market ──▶ loaded_snapshot ──▶ crypto_market_snapshot
                                                                               └────▶ data_quality_errors
```

Chưa làm: Binance → Kafka → Pathway → market_1m/signals (Phase 3+4),
correlation query (Phase 5).

## 3. Phase 1 — News pipeline ✅ live

- **Fetch** (`resources/rss.py` → `RSSFeedResource`): httpx async tải đồng thời 4 RSS
  (CoinDesk, CoinTelegraph, BitcoinMag `/feed`, Google News). Config ở
  `config/config.yaml`, fallback `DEFAULT_FEEDS` trong code.
- **Parse** (`news/parser.py`): chuẩn hoá về `{title, url, source,
  published_at, content}`. Hai fix thực tế:
  - `normalize_url()` bỏ `?utm_*` của CoinTelegraph để dedupe đúng;
  - `sanitize_feed_xml()` lọc `<content:encoded/>` rỗng của CoinDesk
    (feedparser lấy nó đè mất `description` — đã chứng minh bằng repro).
- **Validate** (`news/schemas.py`): Pydantic `RawArticle`/`CleanArticle`,
  thiếu title/url → loại + log.
- **Clean** (`news/cleaner.py`): strip HTML, extract symbols
  (`SYMBOL_KEYWORDS`), sentiment heuristic (2 word-list),
  `dedupe_by_url()`.
- **Load** (`news_assets.py:loaded_news`): `ON CONFLICT (url) DO NOTHING`
  → idempotent, chạy lại không trùng.
- **Schedule**: `news_job_schedule`, cron `*/5 * * * *`.
- **Verify**: fetch live 165 articles/0 lỗi; DB từng đạt 193 rows
  (hiện 0 do thay volumes mới — refill khi bật schedule); test 8/8.

## 4. Phase 2 — Market-cap snapshot ✅ chạy 1 lần thành công

- **API**: CoinGecko `/coins/markets` (free, verified status 200,
  fields đầy đủ). Rate limit ~5-15/phút → job giờ là dư dả.
- **Fetch** (`resources/coingecko.py` → `CoinGeckoResource`): httpx + retry 3 lần backoff (1s/2s/4s).
- **Mapping** (`market/schemas.py:from_coingecko`): lớp cách ly tên field
  (`current_price`→`price`...). Pydantic `RawMarket`, giá null → loại.
- **Validate** (`market/validator.py`): price>0, market_cap>0, symbol
  non-empty → tách `(valid, errors)`, errors sẵn format INSERT.
- **Assets** (`assets/market_assets.py`): cùng khuôn 3 bước Phase 1.
  `loaded_snapshot` dùng **1 `collected_at` chung cả batch** (50 rows cùng
  mốc giờ — đúng nghĩa snapshot), bad records → `data_quality_errors`.
- **Schedule**: `market_job_schedule`, cron `0 * * * *` (đầu mỗi giờ).
- **Schema** (`database/schema.sql`): `crypto_market_snapshot` PK composite
  `(collected_at, symbol)` + `data_quality_errors` (payload JSONB).
- **Verify**: launch `market_job` qua Dagster → **50 rows**, 0 errors;
  query top gainers thật: XMR +4.16%, OKB +4.08%...; test 7/7 (mock offline).
  Queries phân tích lưu ở `database/queries.sql` (8 câu, gồm correlation
  cho Phase 4).

## 5. Tech stack & vai trò

| Tech | Vai trò | Ghi nhớ |
|---|---|---|
| Python 3.12 | Toàn bộ pipeline | Chạy trong container |
| Dagster | Orchestration batch | Asset= dữ liệu, Job= gói chạy, Schedule= đồng hồ, Resource= kết nối chung |
| PostgreSQL 16 | OLTP storage + phân tích nhẹ | KHÔNG phải OLAP (OLAP = ClickHouse/BigQuery) |
| httpx/feedparser | Tải + parse RSS | Async tải song song |
| Pydantic | Validate tầng 1 | Rác bị loại trước khi vào DB |
| psycopg2 | Driver Postgres | `with conn` tự commit/rollback |
| Docker Compose | 4 services + volumes | postgres + dagster-code (gRPC 4000) + webserver + daemon |
| pytest | 15 unit tests | Mock để offline được |

Khái niệm cốt lõi đã học: cron, upsert/idempotency, composite key, JSONB,
retry/backoff, field mapping, bad-record pattern, volume/mount, code location
(`workspace.yaml` → `grpc_server` dagster-code:4000).

## 6. Repo & files

```
C:\crypto-data-engineering\  (GitHub: ndai30122006-creator/crypto-data-engineering)
├── docker-compose.yml, Dockerfile.dagster, pyproject.toml + uv.lock, workspace.yaml
├── config/config.yaml
├── database/schema.sql, database/queries.sql
├── dagster_project/
│   ├── definitions.py        (2 jobs + 2 schedules)
│   ├── resources/          (RSSFeedResource, CoinGeckoResource, PostgresResource)
│   ├── news/                 (collector, parser, cleaner, schemas)
│   ├── market/               (fetcher, schemas, validator)
│   └── assets/               (news_assets, market_assets)
├── tests/                    (test_cleaner 8 + test_market 7)
├── plan/                     (README + 5 phase, đã push GitHub)
├── docs/                     (4 tài liệu học, LOCAL ONLY)
└── README.md
```

Commits: Initial → News pipeline → Fix workspace → Full README →
plan/ folder → Detail Phase 2 (3f7c1fd). Phase 2 code chưa commit.

## 7. Sự cố đã gặp & cách fix (bài học vận hành)

1. **Docker Desktop chưa chạy** → pipe `dockerDesktopLinuxEngine` không tồn tại.
2. **Thiếu WSL** → `wsl --install` + restart (Docker bắt buộc WSL2 backend).
3. **API 500 sau cài** → engine chưa start xong / restart Docker.
4. **Dagster `No Definitions found`** → `workspace.yaml` dùng `python_package`
   (chỉ quét `__init__.py` rỗng) → đổi `python_module: dagster_project.definitions`.
5. **`news_job` ôm luôn market assets** → `AssetSelection.all()` nguy hiểm khi
   thêm asset → selection liệt kê tên rõ.
6. **Đổi thư mục → volumes mới, DB trống** → ghim `name:` trong compose;
   dữ liệu fetch lại được nên chấp nhận mất.
7. **Container name conflict** khi dựng lại → `docker rm -f` trước `up`.
8. **Lệnh timeout** khi `job launch` + sleep dài → launch xong kiểm tra riêng.

## 8. Vận hành hằng ngày

```powershell
cd C:\crypto-data-engineering
docker compose up -d          # chạy nền
docker compose ps             # xem trạng thái
docker logs crypto-dagster-daemon --tail 30
docker exec crypto-postgres psql -U admin -d crypto_db -c "SELECT count(*) FROM crypto_news;"
docker compose down           # tắt (data còn trong volumes)
```

UI http://localhost:3000: Assets → Materialize; Automation → bật 2 schedules.
Sửa code local → container tự load lại (volume mount), không cần build.

## 9. Việc còn lại

- [ ] Bật 2 schedules ON + re-materialize news (refill DB mới)
- [ ] Commit + push code Phase 2 (schema, market/, assets, tests, queries.sql, compose `name:`)
- [ ] Phase 3: Binance → Kafka (`plan/03`)
- [ ] Phase 4: Pathway OHLCV (`plan/04`)
- [ ] Phase 5: correlation + milestones (`plan/05`)
