# Tài liệu học từng step — Phase 2 Market-cap snapshot

Học theo đúng 9 step trong `plan/02-market-cap-snapshot.md`.

---

## Step 0 — REST API là gì?

**API** = quán ăn: bạn gọi món (request), bếp nấu (server), bồi bàn mang ra
(response). **REST** là kiểu gọi qua URL HTTP + nhận JSON.

Mở URL CoinGecko trên trình duyệt chính là 1 request GET thật.
Các phần trong URL:

```
.../coins/markets?vs_currency=usd&order=market_cap_desc&per_page=5&page=1
                     └ đơn vị tiền ─┘ └ sắp xếp theo vốn hoá ┘ └ lấy 5 coin ┘
```

**Rate limit**: API free giới hạn số lần gọi (CoinGecko ~5-15/phút).
Gọi quá → lỗi `429 Too Many Requests`. Job mỗi giờ gọi 1 lần nên không lo,
nhưng code vẫn phải biết xử lý 429 (Step 2).

Bài tập: đổi `per_page=5` thành 10, đổi `vs_currency=usd` thành `vnd`,
quan sát JSON đổi gì.

---

## Step 1 — Composite PRIMARY KEY + JSONB

- **Composite key** `PRIMARY KEY (collected_at, symbol)`: mỗi giờ có 50 dòng,
  cặp (giờ + coin) là duy nhất. Khác Phase 1 (url UNIQUE đơn).
- **JSONB**: cột chứa JSON (`payload` record lỗi). Query được bên trong:
  `payload->>'symbol'`. Dùng cho dữ liệu "không cố định hình dạng".
- `IF NOT EXISTS`: chạy lại file schema không sợ mất bảng cũ.

Bài tập: sau khi apply, `\d crypto_market_snapshot` trong psql, đọc cấu trúc.

---

## Step 2 — Retry khi gọi API ngoài

Mạng/API ngoài không đáng tin: timeout, 500, 429. Pattern chuẩn:

```python
for attempt in range(3):
    try:
        return httpx.get(url, timeout=30)
    except (httpx.TimeoutException, httpx.HTTPStatusError):
        time.sleep(2 ** attempt)   # backoff: 1s, 2s, 4s
raise  # hết 3 lần thì báo lỗi cho Dagster retry ở tầng job
```

2 tầng retry: code tự thử 3 lần → vẫn lỗi thì Dagster retry cả asset.
Bài tập: giả lập API chết (sửa URL sai), xem log retry rồi job báo FAILED ra sao.

---

## Step 3 — Mapping field API (vì sao không dùng JSON thô?)

API ngoài có thể đổi tên field bất cứ lúc nào (`current_price` → `price`?).
Hàm `from_coingecko()` là **lớp cách ly**: bên trong pipeline chỉ dùng tên
của mình (`price`), API đổi thì sửa 1 chỗ.

Đây là cùng pattern với Phase 1 (`parse_entry` chuẩn hoá RSS các báo
khác nhau về 1 format). Nhớ pattern này — mọi nguồn ngoài đều cần nó.

Bài tập: 1 item thiếu `market_cap` (null) → msgspec xử lý sao?
(Field `float | None` cho qua hay ValidationError? Vì sao để Optional?)

---

## Step 4 — Tách valid/errors (bad-record pattern)

Thay vì `if lỗi: bỏ qua`, ta **thu gom lỗi có cấu trúc**:

```python
valid, errors = [], []
for r in records:
    if r.price <= 0:
        errors.append({"pipeline": "market", "payload": r, "error": "price <= 0"})
    else:
        valid.append(r)
```

Lợi ích: sau này mở `data_quality_errors` ra biết chính xác nguồn nào,
lỗi gì, record nào — thay vì đoán mò khi số liệu thiếu.

Bài tập: liệt kê 3 loại lỗi có thể xảy ra với dữ liệu CoinGecko thật.

---

## Step 5 — Tái dùng pattern Phase 1 + cron giờ

So sánh 2 pipeline:

```
Phase 1: raw_news      → cleaned_news  → loaded_news       (*/5 * * * *)
Phase 2: fetch_market  → validate_market → loaded_snapshot (0 * * * *)
```

Cùng 1 khuôn: **lấy thô → kiểm tra → cất**. Chỉ khác nguồn, schedule,
bảng đích. Làm Data Engineer giỏi = nhận ra khuôn lặp lại và tái dùng.

Cron `0 * * * *`: phút 0 mỗi giờ (01:00, 02:00...). Vì sao không chạy mỗi
5 phút như news? Vì market-cap biến động chậm, API free có rate limit.

Bài tập: trong UI, so sánh graph 2 jobs, chỉ ra asset nào tương ứng nhau.

---

## Step 6 — Mock test (test không cần mạng)

Test gọi API thật thì chậm + phụ thuộc mạng + tốn rate limit.
**Mock** = giả dữ liệu: copy 2-3 items JSON CoinGecko thật vào test,
feed cho validator, assert kết quả. Chạy offline trong 0.1s.

Bài tập: viết 1 test cho rule `market_cap > 0` theo mẫu test Phase 1.

---

## Step 7 — Verify bằng query phân tích

3 query trong plan tương ứng 3 câu hỏi kinh doanh: có bao nhiêu snapshot,
ai tăng mạnh nhất, ai giao dịch nhiều nhất. Đây là bước "ăn điểm" với
anh bạn: pipeline không chỉ chạy mà còn **trả lời được câu hỏi**.

Bài tập: viết thêm query "coin nào vốn hoá top 10 nhưng giá giảm > 5%?"
