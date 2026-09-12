# Phase 1 — News pipeline ✅ DONE

## Đã triển khai

```
RSS (4 nguồn) ── */5 min ──▶ raw_news ──▶ cleaned_news ──▶ loaded_news ──▶ crypto_news
```

- `dagster_project/news/collector.py` — fetch đồng thời 4 feeds (httpx + feedparser)
- `dagster_project/news/parser.py` — chuẩn hoá entry, `normalize_url()` bỏ utm,
  `sanitize_feed_xml()` fix CoinDesk `<content:encoded/>` rỗng
- `dagster_project/news/cleaner.py` — clean HTML, extract symbols, sentiment
  heuristic, dedupe theo URL
- `dagster_project/news/schemas.py` — Pydantic `RawArticle` / `CleanArticle`
- `dagster_project/assets/news_assets.py` — 3 assets nối nhau
- `dagster_project/definitions.py` — `news_job` + schedule `*/5 * * * *`
- `database/schema.sql` — bảng `crypto_news` (url UNIQUE)
- `tests/test_cleaner.py` — 8 unit tests pass

## Kết quả verify

- Fetch live: 165 articles, 0 lỗi nguồn
- DB: 190+ rows, 132+ có symbols
- `docker compose config` OK, containers healthy

## Còn lại (vận hành)

- Bật `news_job_schedule` = ON trong Dagster UI (Automation tab)
