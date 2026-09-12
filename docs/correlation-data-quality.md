# Tài liệu học: Correlation & Data Quality

Hai kỹ năng phân biệt Data Engineer với người chỉ biết code pipeline:
**hỏi dữ liệu** (correlation) và **giữ dữ liệu sạch** (quality).

---

## Phần 1 — Correlation: tin tức ↔ thị trường

### Khái niệm

Correlation ở đây nghĩa là **nối 2 nguồn dữ liệu theo thời gian** để trả lời:
"tin này ra có trùng lúc giá biến động mạnh không?"

Ví dụ:

```
14:01  BTC +0.2%
14:02  BTC +0.3%
14:03  BTC +1.8%  ← spike
14:03  News: "Bitcoin ETF receives major inflows"
```

→ Tin và spike cùng cửa sổ ±10 phút → có thể liên quan.

### Query mẫu (chuẩn bị cho Phase 4, khi có bảng `market_1m`)

```sql
SELECT n.title, n.published_at, m.symbol, m.window_start,
       m.price_change_1m AS price_change
FROM crypto_news n
JOIN market_1m m
  ON m.window_start BETWEEN n.published_at - INTERVAL '10 minutes'
                        AND n.published_at + INTERVAL '10 minutes'
WHERE ABS(m.price_change_1m) > 1
ORDER BY m.window_start DESC;
```

Cách đọc: JOIN theo **khoảng thời gian** (không phải theo id),
lọc biến động > 1%. `ABS()` để bắt cả tăng mạnh lẫn giảm mạnh.

### Làm được NGAY với dữ liệu hiện có

Chưa có `market_1m`, nhưng练 được tư duy correlation trên bảng news:

```sql
-- Cùng 1 sự kiện, các báo đưa cách nhau bao lâu?
SELECT title, source, published_at FROM crypto_news
WHERE title ILIKE '%etf%'
ORDER BY published_at;

-- Giờ nào trong ngày nhiều tin nhất? (phân bố thời gian)
SELECT EXTRACT(HOUR FROM published_at) AS h, count(*)
FROM crypto_news GROUP BY 1 ORDER BY 1;

-- Coin nào được nhắc nhiều nhất tuần này?
SELECT unnest(symbols) AS coin, count(*)
FROM crypto_news
WHERE collected_at > NOW() - INTERVAL '7 days'
GROUP BY 1 ORDER BY 2 DESC;
```

### Bài tập

1. Chạy 3 query trên, giải thích kết quả.
2. Viết query: sentiment tiêu cực chiếm bao nhiêu % tin mỗi nguồn?
3. (Nâng cao, sau Phase 4) Lưu query JOIN ±10 phút vào
   `database/queries.sql`, chạy thử khi `market_1m` có dữ liệu.

---

## Phần 2 — Data Quality: giữ dữ liệu sạch

### Khái niệm

Pipeline chạy tự động mỗi 5 phút, không ai ngồi canh → phải có
**luật kiểm tra** để rác không lọt vào DB. 3 tầng phòng thủ:

```
Tầng 1: Validate khi parse (Pydantic)     → bỏ bài thiếu title/url
Tầng 2: Constraint ở DB (UNIQUE, NOT NULL) → chặn trùng/sai ở cửa cuối
Tầng 3: Bảng data_quality_errors           → ghi lại bad records để soi
```

### Luật hiện có trong project

| Tầng | Luật | File |
|---|---|---|
| Pydantic | title/url bắt buộc | `news/schemas.py` (`RawArticle`) |
| Parser | bỏ entry thiếu link/title | `news/parser.py` (`parse_entry`) |
| DB | `url UNIQUE`, `title NOT NULL` | `database/schema.sql` |
| Load | `ON CONFLICT DO NOTHING` | `news_assets.py` (`loaded_news`) |

### Luật sẽ thêm (Phase 2+)

```sql
CREATE TABLE IF NOT EXISTS data_quality_errors (
    id BIGSERIAL PRIMARY KEY,
    pipeline VARCHAR(50),   -- 'news' | 'market' | 'ohlcv'
    payload JSONB,          -- record lỗi nguyên bản
    error TEXT,             -- lý do: 'price <= 0', 'missing url'...
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

- Market data: `price > 0`, `market_cap > 0`, symbol NOT NULL
- OHLCV: `high >= low/open/close`, `low <= open/close`, `volume >= 0`
- Quy tắc: **không bao giờ drop lặng lẽ** — record lỗi phải vào
  `data_quality_errors` kèm lý do.

### Idempotency (chạy lại an toàn)

Pipeline schedule chạy đi chạy lại → chạy 10 lần cũng như 1 lần.
Trong project đạt được nhờ: dedupe URL trong batch + `ON CONFLICT`
ở DB. Đã verify: materialize lần 2 → `inserted ≈ 0`.

### Bài tập

1. Cố tình insert 1 bài trùng URL bằng tay → quan sát `ON CONFLICT`
   chặn thế nào (báo lỗi hay bỏ qua?).
2. Viết query đếm % bài không extract được symbols nào — đó có phải
   "bad data" không? Vì sao có/không?
3. Thêm 1 rule Pydantic mới (vd: `title` tối thiểu 10 ký tự), chạy test,
   xem bài nào bị loại.
