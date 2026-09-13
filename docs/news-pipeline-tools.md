# Tài liệu học tools theo pipeline News

Đi theo đúng diagram, mỗi bước học 1 tool + thực hành ngay trên project.

```
RSS ──▶ [every 5 min] ──▶ Dagster (fetch → clean → load) ──▶ PostgreSQL
```

---

## Bước 1 — RSS / public API: nguồn dữ liệu

### Khái niệm

**RSS** là định dạng XML mà các trang báo dùng để phát hành tin mới.
Thay vì mở web đọc, máy đọc file XML này để lấy tin tự động.
Ví dụ 1 item trong RSS:

```xml
<item>
  <title>Bitcoin rises above $110,000...</title>
  <link>https://www.coindesk.com/...</link>
  <pubDate>Fri, 11 Sep 2026 13:46:51 +0000</pubDate>
  <description>...</description>
</item>
```

### Trong project (`resources/rss.py`, `config/config.yaml`)

- 4 URLs RSS lưu ở `config/config.yaml`, đọc bằng `yaml.safe_load`
- `httpx.AsyncClient` tải cả 4 feeds **đồng thời** (asyncio.gather)
  thay vì chờ từng cái → nhanh gấp ~4 lần
- `feedparser.parse()` biến XML thành dict Python (`title`, `link`,
  `published_parsed`, `summary`, `content`...)

### Bài tập

1. Mở 1 URL trong `config.yaml` bằng trình duyệt, tìm thẻ `<item>` đầu tiên,
   đối chiếu với dict mà `parse_entry()` trả về.
2. Thử giải thích: vì sao CoinTelegraph cần `normalize_url()`?
   (gợi ý: xem `link` của nó có gì thừa)

---

## Bước 2 — every 5 min: lịch chạy (cron)

### Khái niệm

**Cron** là cú pháp đặt lịch gồm 5 trường:

```
* * * * *
│ │ │ │ └── thứ trong tuần (0-6)
│ │ │ └──── tháng (1-12)
│ │ └────── ngày trong tháng (1-31)
│ └──────── giờ (0-23)
└────────── phút (0-59)
```

`*/5 * * * *` = mỗi 5 phút. `0 * * * *` = đầu mỗi giờ.

### Trong project (`definitions.py:19-22`)

```python
news_schedule = ScheduleDefinition(job=news_job, cron_schedule="*/5 * * * *")
```

Daemon đọc schedule này và kích job đúng giờ. Không cần viết vòng lặp
`while True + sleep` thủ công.

### Bài tập

1. Viết cron cho: mỗi giờ lúc phút 30 / mỗi ngày lúc 2h sáng / mỗi thứ 2.
2. Vào Dagster UI → Automation → xem schedule, thử tắt/bật công tắc.

---

## Bước 3 — Dagster: đốc công

### Khái niệm (3 từ khóa)

| Từ | Nghĩa | Ví dụ trong project |
|---|---|---|
| **Asset** | Một cục *dữ liệu* + code tạo ra nó | `raw_news`, `cleaned_news`, `loaded_news` |
| **Job** | Gói nhiều asset thành 1 lần chạy | `news_job` (chạy cả 3 asset) |
| **Schedule** | Đồng hồ kích job theo cron | `news_job_schedule` mỗi 5 phút |

Điểm khác Airflow: Dagster nối asset bằng **tên tham số hàm**
(`def cleaned_news(..., raw_news: list)` → tự hiểu phụ thuộc `raw_news`),
không cần viết `A >> B` thủ công.

**Resource** (`resources.py`): kết nối dùng chung (Postgres) tiêm vào asset
qua tham số, lấy từ biến môi trường `DATABASE_URL` — code không hardcode
password.

### Trong project

- `assets/news_assets.py`: 3 asset nối nhau
- `definitions.py`: gom assets + job + schedule + resources thành `defs`
- `workspace.yaml`: chỉ cho Dagster biết code nằm ở module nào

### Bài tập

1. Vào UI → Assets → bấm vào `cleaned_news` → xem upstream/downstream.
2. Thử Materialize 1 asset đơn lẻ, quan sát asset phụ thuộc có chạy theo không.
3. Đọc log 1 run cũ trong UI, tìm dòng `inserted N new articles`.

---

## Bước 4 — fetch_news (raw): lấy thô + validate

### Tools: httpx, feedparser, Pydantic

- **httpx**: gọi HTTP hiện đại, hỗ trợ async (tải song song).
- **feedparser**: parse mọi biến thể RSS/Atom thành 1 format dict chung.
- **Pydantic** (`news/schemas.py`): khai báo `RawArticle` — bài nào thiếu
  `title`/`url` thì `ValidationError` → bỏ qua + log warning, không để
  dữ liệu rác lọt xuống DB.

### Bài tập

1. Chạy local (không cần Docker):
```powershell
uv sync
uv run pytest tests/ -q
```
2. Đọc `resources/rss.py`: nếu 1 trong 4 nguồn chết, 3 nguồn còn lại có sao
   không? (gợi ý: `return_exceptions=True`)

---

## Bước 5 — clean_news: làm sạch (vốn là todo, đã làm xong)

### Các kỹ thuật (`news/cleaner.py`)

| Việc | Cách làm |
|---|---|
| Xóa HTML | regex `<[^>]+>` + `html.unescape` + gộp khoảng trắng |
| Tìm coin | tách từ, tra bảng `SYMBOL_KEYWORDS` (bitcoin→BTC...) |
| Sentiment | đếm từ tích cực (`surge`, `rally`, `approve`...) vs tiêu cực (`hack`, `crash`, `outflow`...) — heuristic đơn giản, sau này thay bằng model ML |
| Dedupe | giữ 1 bài cho mỗi URL (trong batch + ở DB bằng UNIQUE) |

### Quirk thực tế đã gặp (nên nhớ)

CoinDesk để `<content:encoded/>` **rỗng** → feedparser lấy nó đè lên
`description` thật → mất content. Fix: `sanitize_feed_xml()` lọc tag rỗng
trước khi parse. Đây là ví dụ điển hình "dữ liệu thật luôn bẩn".

### Bài tập

1. Thêm 1 coin vào `SYMBOL_KEYWORDS`, chạy lại test.
2. Tìm 1 bài sentiment sai (vd bài trung tính bị đoán positive), nghĩ xem
   vì sao heuristic sai và sửa thế nào.

---

## Bước 6 — load_postgres: ghi DB

### Tools: psycopg2, SQL upsert

- **psycopg2**: driver cho Python nói chuyện với Postgres.
- **Upsert** (`ON CONFLICT (url) DO NOTHING`): link đã có thì bỏ qua,
  nên schedule chạy mỗi 5 phút không bao giờ tạo trùng.
- `with conn` → tự commit khi thành công, rollback khi lỗi.

### Bài tập

1. Chạy materialize 2 lần liên tiếp → lần 2 `inserted` phải ≈ 0. Giải thích
   vì sao (idempotency — chạy lại vẫn an toàn).
2. Thử xóa UNIQUE trên url (môi trường test) rồi chạy lại → quan sát
   duplicate, hiểu vì sao constraint quan trọng.

---

## Bước 7 — PostgreSQL: kho chứa

### Khái niệm

Postgres là database **OLTP** (lưu theo hàng): mạnh ở ghi từng record và
đọc theo key. Bảng `crypto_news`:

- `url TEXT UNIQUE` — chống trùng
- `symbols TEXT[]` — mảng, query được (`WHERE 'BTC' = ANY(symbols)`)
- index trên `published_at DESC` — lấy tin mới nhất nhanh

### Bài tập (chạy bằng psql)

```sql
-- 10 tin mới nhất
SELECT title, source, symbols FROM crypto_news
ORDER BY collected_at DESC LIMIT 10;

-- Tin nhắc tới BTC hôm nay
SELECT title, sentiment FROM crypto_news
WHERE 'BTC' = ANY(symbols)
  AND collected_at > NOW() - INTERVAL '1 day';

-- Thống kê theo nguồn + sentiment
SELECT source, sentiment, count(*)
FROM crypto_news GROUP BY 1, 2 ORDER BY 3 DESC;
```

---

## Bước 8 — Docker Compose: đóng gói

### Khái niệm

Mỗi **service** = 1 container: `postgres` (kho), `dagster-webserver`
(màn hình localhost:3000), `dagster-daemon` (đồng hồ schedule).
Chúng nói chuyện qua tên service (`postgres:5432`), dữ liệu DB giữ lại
nhờ **volume** `pgdata`, code sửa local tự vào container nhờ **volume**
mount `./dagster_project`.

### Bài tập

1. `docker compose ps` → kể tên 3 container + trạng thái.
2. `docker logs crypto-dagster-daemon --tail 20` → tìm dòng schedule.
3. `docker compose down` rồi `up` lại → kiểm tra dữ liệu DB còn không
   (phải còn — vì sao?).
