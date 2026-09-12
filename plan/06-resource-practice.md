# Phase 6 — Resource practice (theo blog Dagster Resources)

Nguồn: https://dagster.io/blog/a-practical-guide-to-dagster-resources
Mục tiêu: refactor project theo đúng 6 ý của blog, vừa làm vừa học.

## Mapping: blog → project hiện tại

| Ý trong blog | Thực trạng project | Practice |
|---|---|---|
| What Are Resources | `PostgresResource` chỉ có conn_str + get_conn | P1: audit + chuẩn hoá |
| API Encapsulation | `fetcher.py` gọi httpx trực tiếp trong asset | P2: bọc thành `CoinGeckoResource` + `RSSFeedResource` |
| Environment Management | Chưa phân biệt local/staging/prod | P3: resource tự chọn schema theo `DAGSTER_ENVIRONMENT` |
| Dependency Injection | Asset đang sạch (đã tốt), fetcher chưa | P2: asset chỉ gọi `api.fetch_markets()`, `db.insert_*()` |
| Testing Without Pain | Tests chỉ test hàm thuần, chưa test asset | P4: `MockPostgresResource`, test asset không cần DB |
| Configuration | `DATABASE_URL` qua EnvVar (đã tốt) | P5: kiểm tra Deployment tab trong UI |

---

## P1 — Audit: tìm code setup lặp lại (30 phút)

- Soi `news_assets.py` + `market_assets.py`, liệt kê mọi chỗ chạm
  dịch vụ ngoài: `asyncio.run(fetch_all())`, `httpx.get`, `psycopg2.connect`,
  `cur.execute(INSERT...)` viết tay 2 lần.
- Quy tắc blog: setup lặp ≥ 2 lần = 1 resource đang chờ được sinh ra.
- Xong khi: có checklist các ứng viên resource (dự kiến: RSS client,
  CoinGecko client, Postgres writer).

## P2 — API Encapsulation: bọc 2 API thành resource (core)

Files:
- `dagster_project/resources/rss.py` → `RSSFeedResource(ConfigurableResource)`:
  fields `feeds: dict`, `timeout: int = 30`; methods `fetch_all()`
  (chuyển code từ `collector.py` vào, giữ `sanitize_feed_xml`).
- `dagster_project/resources/coingecko.py` → `CoinGeckoResource`:
  fields `base_url`, `per_page: int = 50`; method `fetch_markets()`
  (chuyển retry/backoff từ `fetcher.py` vào).
- `dagster_project/resources/postgres.py` → mở rộng `PostgresResource`:
  thêm `insert_snapshot(conn, rows)`, `insert_news(conn, rows)`,
  `insert_errors(conn, errors)` — assets không viết SQL tay nữa.
- Refactor assets: `raw_news(rss: RSSFeedResource)`,
  `fetch_market(coingecko: CoinGeckoResource)`,
  `loaded_*(postgres: PostgresResource)` chỉ còn business logic.
- Đăng ký 3 resources vào `defs` (giữ `EnvVar("DATABASE_URL")` cho secret).
- Xong khi: assets không còn import httpx/feedparser/psycopg2 trực tiếp;
  `docker exec ... dagster schedule list` vẫn thấy 2 schedules;
  materialize 1 lần ra dữ liệu như cũ.

## P3 — Environment Management: resource biết môi trường

- Thêm vào `PostgresResource`:
  ```python
  env: str = "local"  # đọc từ EnvVar("DAGSTER_ENVIRONMENT") khi đăng ký
  def table(self, name: str) -> str:
      return name if self.env == "prod" else f"{name}_{self.env}"
  ```
  → local ghi `crypto_news_local`, prod ghi `crypto_news`.
- Assets dùng `postgres.table("crypto_news")` thay vì tên cứng.
- Thêm `DAGSTER_ENVIRONMENT=local` vào `docker-compose.yml`.
- Xong khi: materialize ở local chỉ tạo bảng `*_local`, bảng prod
  không bị đụng; giải thích được vì sao asset không cần biết env.

## P4 — Testing with mocks: test asset không cần DB/mạng

- `tests/test_resources.py` (mới):
  - `MockPostgresResource`: `insert_*()` ghi vào list trong RAM,
    assert được số rows + nội dung.
  - `MockCoinGeckoResource.fetch_markets()` trả về 2 items JSON cứng.
  - Test `validate_market` + logic split valid/errors qua mock
    (không cần Postgres, không cần mạng, chạy < 1s).
- Giữ `test_market.py` cũ (test hàm thuần) — 2 tầng test bổ sung nhau.
- Xong khi: `pytest tests/ -q` pass khi **tắt Docker + rút mạng**
  (chứng minh không phụ thuộc hạ tầng).

## P5 — Configuration visibility: kiểm tra trên UI

- Vào Dagster UI → Deployment tab → xem 3 resources + config của chúng.
- Đảm bảo secret (conn_str) không lộ giá trị thật trên UI
  (EnvVar chỉ hiện tên biến).
- Xong khi: chụp/screenshot được Deployment tab có đủ resources;
  giải thích được vì sao secret dùng EnvVar thay vì string cứng.

## Thứ tự & verify cuối

P1 → P2 → P3 → P4 → P5. Sau P2 và P3 đều materialize 1 lần kiểm tra
dữ liệu ra đúng. Cuối cùng: `pytest` pass offline + 2 schedules load
sạch + commit/push (khi bạn đồng ý).
