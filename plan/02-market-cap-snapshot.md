# Phase 2 — Market-cap snapshot job (mỗi 1 giờ) ✅ DONE, live
> Trạng thái: đã triển khai xong (`resources/coingecko.py`, `assets/market_assets.py`).
> File này giữ lại các step gốc để đọc hiểu quá trình.

## Mục tiêu

Mỗi giờ lấy top 50 coin (giá, vốn hoá, volume 24h, % tăng giảm) từ CoinGecko,
lưu snapshot để query top gainers/losers, volume leaders.

## Luồng

```
CoinGecko API ── 0 * * * * ──▶ fetch_market ──▶ validate_market ──▶ loaded_snapshot ──▶ crypto_market_snapshot
```

---

## Step 0 — Tìm hiểu API (chưa viết code)

- Mở trình duyệt:
  `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=5&page=1`
- Ghi lại các field: `symbol`, `name`, `current_price`, `market_cap`,
  `total_volume`, `circulating_supply`, `price_change_percentage_24h`, `last_updated`
- Kiến thức: REST API, query params, JSON, rate limit (free ~5-15 call/phút
  → job mỗi giờ là dư dả)
- Xong khi: hiểu mỗi field nghĩa gì, biết rate limit bao nhiêu

## Step 1 — Schema DB

Files: sửa `database/schema.sql` (thêm 2 bảng), apply vào container đang chạy.

```sql
CREATE TABLE IF NOT EXISTS crypto_market_snapshot (
    collected_at TIMESTAMPTZ DEFAULT NOW(),
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    price NUMERIC(20,8),
    market_cap NUMERIC(30,2),
    circulating_supply NUMERIC(30,2),
    volume_24h NUMERIC(30,2),
    price_change_24h NUMERIC(10,4),
    PRIMARY KEY (collected_at, symbol)
);

CREATE TABLE IF NOT EXISTS data_quality_errors (
    id BIGSERIAL PRIMARY KEY,
    pipeline VARCHAR(50),
    payload JSONB,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

Apply: `Get-Content database/schema.sql | docker exec -i crypto-postgres psql -U admin -d crypto_db`
(CREATE TABLE IF NOT EXISTS nên chạy lại an toàn, không mất bảng news.)

- Kiến thức: composite PRIMARY KEY (cặp collected_at+symbol), JSONB, IF NOT EXISTS
- Xong khi: `\dt` thấy 3 bảng

## Step 2 — fetcher.py (gọi API)

File mới: `dagster_project/market/fetcher.py`

- Hàm `fetch_markets(vs_currency="usd", per_page=50) -> list[dict]`
- Dùng `httpx.get(..., timeout=30)`, `raise_for_status()`, retry 3 lần
  (vòng lặp + sleep, API free thỉnh thoảng 429)
- Trả về raw JSON list + log số coin lấy được
- Kiến thức: HTTP status (200/429/500), retry/backoff, timeout
- Xong khi: chạy thử local in ra 50 coins

## Step 3 — schemas.py (Pydantic)

File mới: `dagster_project/market/schemas.py`

```python
class RawMarket(BaseModel):
    symbol: str
    name: str
    price: float
    market_cap: Optional[float] = None
    circulating_supply: Optional[float] = None
    volume_24h: Optional[float] = None
    price_change_24h: Optional[float] = None
```

Map từ field CoinGecko (`current_price` → `price`...) trong 1 hàm
`from_coingecko(item)`. Field nào thiếu → ValidationError → loại.

- Kiến thức: mapping field API ngoài vào model nội bộ (chống API đổi tên field)
- Xong khi: parse được JSON thật từ Step 0

## Step 4 — validator.py (data quality)

File mới: `dagster_project/market/validator.py`

- Rules: `price > 0`, `market_cap > 0`, `symbol` non-empty
- Hàm `validate(records) -> (valid, errors)`; errors là list dict
  `{pipeline: "market", payload: {...}, error: "price <= 0"}`
- KHÔNG drop lặng lẽ — errors sẽ ghi vào `data_quality_errors` ở asset load
- Kiến thức: 3 tầng quality (Pydantic → rule → DB), bad-record pattern
- Xong khi: unit test với 1 record tốt + 3 record lỗi các loại

## Step 5 — assets + job + schedule

File mới: `dagster_project/assets/market_assets.py`

- `fetch_market` → gọi fetcher, validate Pydantic, return list JSON
- `validate_market(fetch_market)` → tách valid/errors
- `loaded_snapshot(validate_market, postgres)` → INSERT snapshot +
  INSERT errors vào `data_quality_errors`, return số rows
- Sửa `definitions.py`: thêm `market_job` + `ScheduleDefinition(cron="0 * * * *")`,
  đăng ký assets + job + schedule mới vào `defs`
- Kiến thức: tái dùng pattern 3 assets của Phase 1, cron giờ (`0 * * * *`)
- Xong khi: Dagster UI hiện thêm 3 assets + schedule mới

## Step 6 — tests

File mới: `tests/test_market.py` (mock JSON CoinGecko, không gọi mạng)

- Test map field, test validator (tốt/lỗi), test cron hiểu đúng giờ
- Chạy: `python -m pytest tests/ -q` → all pass
- Kiến thức: mock dữ liệu ngoài để test nhanh, không phụ thuộc mạng

## Step 7 — Chạy thật + verify queries

1. `docker compose up` (code mount tự load lại, không cần build)
2. UI → Materialize `loaded_snapshot`
3. Verify:
```sql
SELECT count(*) FROM crypto_market_snapshot;            -- ~50
SELECT symbol, price, price_change_24h                  -- top gainers
FROM crypto_market_snapshot ORDER BY price_change_24h DESC LIMIT 10;
SELECT symbol, volume_24h                               -- volume leaders
FROM crypto_market_snapshot ORDER BY volume_24h DESC LIMIT 10;
```
4. Bật schedule hourly trong Automation tab
5. Lưu queries vào `database/queries.sql` (file mới)

## Step 8 — Commit + push (khi bạn đồng ý)

`git add` các file mới → commit → push. Không commit `docs/`.

## Thứ tự làm

Step 0 → 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8. Mỗi step xong mới sang step tiếp.
Bạn muốn tôi triển khai step nào thì nói (vd "làm step 1+2").
