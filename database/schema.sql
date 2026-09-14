CREATE TABLE IF NOT EXISTS crypto_news (
    id BIGSERIAL PRIMARY KEY,

    title TEXT NOT NULL,
    url TEXT UNIQUE NOT NULL,
    source VARCHAR(100),

    published_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ DEFAULT NOW(),

    content TEXT,

    sentiment VARCHAR(20) DEFAULT 'neutral',
    symbols TEXT[] DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_crypto_news_published_at
    ON crypto_news (published_at DESC);

CREATE INDEX IF NOT EXISTS idx_crypto_news_source
    ON crypto_news (source);

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

CREATE TABLE IF NOT EXISTS market_1m (
    symbol VARCHAR(20),
    window_start TIMESTAMPTZ,
    open NUMERIC(20,8), high NUMERIC(20,8),
    low NUMERIC(20,8),  close NUMERIC(20,8),
    volume NUMERIC(30,12),
    trade_count INTEGER,
    price_change_1m NUMERIC(10,4),
    PRIMARY KEY (symbol, window_start)
);
