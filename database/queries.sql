-- Queries phân tích cho crypto-data-engineering
-- Lưu ý: bảng có hậu tố theo env (local → crypto_news_local,
-- crypto_market_snapshot_local). Thay tên bảng tương ứng khi chạy.

-- 1. Tin mới nhất
SELECT title, source, symbols, sentiment, collected_at
FROM crypto_news
ORDER BY collected_at DESC LIMIT 10;

-- 2. Tin nhắc tới BTC hôm nay
SELECT title, sentiment, published_at
FROM crypto_news
WHERE 'BTC' = ANY(symbols)
  AND collected_at > NOW() - INTERVAL '1 day'
ORDER BY published_at DESC;

-- 3. Thống kê theo nguồn + sentiment
SELECT source, sentiment, count(*)
FROM crypto_news
GROUP BY 1, 2 ORDER BY 3 DESC;

-- 4. Coin được nhắc nhiều nhất 7 ngày
SELECT unnest(symbols) AS coin, count(*)
FROM crypto_news
WHERE collected_at > NOW() - INTERVAL '7 days'
GROUP BY 1 ORDER BY 2 DESC;

-- 5. Top 10 vốn hoá (snapshot mới nhất)
SELECT symbol, name, price, market_cap
FROM crypto_market_snapshot
WHERE collected_at = (SELECT max(collected_at) FROM crypto_market_snapshot)
ORDER BY market_cap DESC LIMIT 10;

-- 6. Top gainers 24h
SELECT symbol, price, price_change_24h
FROM crypto_market_snapshot
WHERE collected_at = (SELECT max(collected_at) FROM crypto_market_snapshot)
ORDER BY price_change_24h DESC LIMIT 10;

-- 7. Volume leaders 24h
SELECT symbol, volume_24h
FROM crypto_market_snapshot
WHERE collected_at = (SELECT max(collected_at) FROM crypto_market_snapshot)
ORDER BY volume_24h DESC LIMIT 10;

-- 8. News ↔ Market correlation (cần bảng market_1m ở Phase 4)
-- SELECT n.title, n.published_at, m.symbol, m.window_start,
--        m.price_change_1m AS price_change
-- FROM crypto_news n
-- JOIN market_1m m
--   ON m.window_start BETWEEN n.published_at - INTERVAL '10 minutes'
--                         AND n.published_at + INTERVAL '10 minutes'
-- WHERE ABS(m.price_change_1m) > 1
-- ORDER BY m.window_start DESC;
