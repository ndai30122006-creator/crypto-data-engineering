-- Derived from the latest candle versions; closed, contiguous minutes only.
CREATE OR REPLACE VIEW market_analytics_1m AS
SELECT symbol, window_start, open, high, low, close, volume, trade_count, updated_at,
    CASE WHEN window_start < date_trunc('minute', CURRENT_TIMESTAMP)
               AND window_start - time_1 = INTERVAL '1 minutes'
               AND valid_1 = 2
         THEN (close / NULLIF(close_1, 0) - 1) * 100
    END AS price_change_1m,
    CASE WHEN window_start < date_trunc('minute', CURRENT_TIMESTAMP)
               AND window_start - time_5 = INTERVAL '5 minutes'
               AND valid_5 = 6
         THEN (close / NULLIF(close_5, 0) - 1) * 100
    END AS price_change_5m,
    CASE WHEN window_start < date_trunc('minute', CURRENT_TIMESTAMP)
               AND window_start - time_15 = INTERVAL '15 minutes'
               AND valid_15 = 16
         THEN (close / NULLIF(close_15, 0) - 1) * 100
    END AS price_change_15m
FROM (
    SELECT m.*,
        lag(close, 1) OVER ordered AS close_1,
        lag(window_start, 1) OVER ordered AS time_1,
        count(*) FILTER (WHERE close > 0 AND close < 'Infinity'::numeric
                            AND window_start = date_trunc('minute', window_start))
            OVER frame_1 AS valid_1,
        lag(close, 5) OVER ordered AS close_5,
        lag(window_start, 5) OVER ordered AS time_5,
        count(*) FILTER (WHERE close > 0 AND close < 'Infinity'::numeric
                            AND window_start = date_trunc('minute', window_start))
            OVER frame_5 AS valid_5,
        lag(close, 15) OVER ordered AS close_15,
        lag(window_start, 15) OVER ordered AS time_15,
        count(*) FILTER (WHERE close > 0 AND close < 'Infinity'::numeric
                            AND window_start = date_trunc('minute', window_start))
            OVER frame_15 AS valid_15
    FROM market_1m m
    WINDOW ordered AS (PARTITION BY symbol ORDER BY window_start),
           frame_1 AS (PARTITION BY symbol ORDER BY window_start
                       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
           frame_5 AS (PARTITION BY symbol ORDER BY window_start
                       ROWS BETWEEN 5 PRECEDING AND CURRENT ROW),
           frame_15 AS (PARTITION BY symbol ORDER BY window_start
                       ROWS BETWEEN 15 PRECEDING AND CURRENT ROW)
) candles;
