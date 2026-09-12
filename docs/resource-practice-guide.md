# Tài liệu học: Resource practice P1–P5 (theo blog Dagster Resources)

Blog gốc: https://dagster.io/blog/a-practical-guide-to-dagster-resources
Áp vào project này: `dagster_project/resources/` (rss, coingecko, postgres).

---

## P1 — Audit: quy tắc "setup lặp 2 lần = 1 resource"

Trước refactor, assets chứa 3 loại setup lặp:
1. `asyncio.run(fetch_all())` / `fetch_markets()` — chi tiết HTTP trong asset
2. `get_conn → cursor → loop INSERT → close` lặp ở 2 assets (~25 dòng giống nhau)
3. SQL viết tay trong asset (đổi bảng = lùng từng asset)

**Vì sao xấu:** business logic lẫn hạ tầng → sửa hạ tầng phải đụng logic,
không test được khi thiếu DB/mạng.

## P2 — 3 resources đã tạo (và vì sao)

| Resource | Gom cái gì | Asset còn lại gì |
|---|---|---|
| `RSSFeedResource` | feeds config, timeout, httpx async, sanitize | `rss.fetch_raw()` 1 dòng |
| `CoinGeckoResource` | base_url, per_page, retry backoff | `coingecko.fetch_markets()` 1 dòng |
| `PostgresResource` | conn_str + `insert_news/snapshot/errors` | `postgres.insert_*(...)` 1-2 dòng |

**Vì sao `ConfigurableResource` mà không phải class thường:**
fields có type (`timeout: int = 30`) → Dagster validate lúc load +
hiện lên Deployment tab trong UI → đổi config không sửa code.

**Vì sao asset không import httpx/psycopg2 nữa:**
dependency injection — asset khai báo "tôi cần `rss`, `db`" thay vì tự tạo.
Lợi: đổi timeout sửa resource; test bằng mock; secret qua EnvVar.

**Vì sao xóa `collector.py`/`fetcher.py`:** logic đã chuyển vào resource,
giữ lại thành 2 nguồn sự thật (duplicate) — xóa để 1 nơi duy nhất.

## P3 — Environment: resource biết mình ở đâu

```python
def table(self, name): return name if self.env == "prod" else f"{name}_{self.env}"
```

- **Vì sao:** test local không bao giờ bẩn dữ liệu prod,
  mà asset không cần biết mình chạy ở đâu (blog: "assets don't need to know").
- **Vì sao EnvVar thay vì ghi `"local"` cứng:** cùng 1 image Docker chạy
  mọi môi trường, chỉ khác biến môi trường lúc deploy.
- **Vì sao `ensure_tables()`:** bảng `*_local` chưa tồn tại → resource tự
  CREATE IF NOT EXISTS bằng tên đã resolve. DDL 1 nguồn duy nhất.
- **Verify thật:** materialize → `crypto_news_local` tự sinh, 165 rows,
  bảng prod không đụng.
- **Gotcha đã gặp:** đổi `location_name` làm rớt run đang queue
  (`Location ... does not exist`) → launch lại qua `-w workspace.yaml`.

## P4 — Mock testing: test logic, không test hạ tầng

```python
class MockPostgres:  # ghi vào RAM thay vì DB
    def insert_news(self, articles): ...
```

- **Vì sao mock:** test câu hỏi "code tôi đúng không", không phải
  "Postgres có sống không". 20 tests chạy 1.8s offline.
- **Vì sao `build_op_context()`:** Dagster bắt context thật khi gọi asset
  trực tiếp — stub tự chế bị từ chối. Đây là chuẩn của Dagster University.
- **Vì sao mock cả `psycopg2.connect`:** assert được SQL chứa đúng bảng
  theo env + `ON CONFLICT`, không cần DB thật.
- **Bài học từ test fail:** `price=0` qua được Pydantic (vẫn là số!) nhưng
  rớt ở validator business rule → project có **2 tầng validate**:
  kiểu dữ liệu (Pydantic) rồi business rules (validator). Test sửa thành
  kiểm tra cả chuỗi fetch → validate.

## P5 — Config trên UI (bạn tự làm)

- Vào Dagster UI → Deployment tab → thấy 3 resources
  (`postgres`, `rss`, `coingecko`) + fields của chúng.
- Kiểm tra `conn_str` không lộ giá trị thật (EnvVar chỉ hiện tên biến).
- Bài tập: đổi `per_page` của coingecko xem cần deploy lại không, vì sao.

## Tổng kết 1 câu

> Resource = đặt tên cho mọi thứ ngoài code (DB, API, env) → asset sạch,
> test được offline, đổi config không sửa code, nhìn được trên UI.
