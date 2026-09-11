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
