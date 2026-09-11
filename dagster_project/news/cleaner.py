"""Clean text, extract coin symbols, sentiment heuristic, dedupe."""
import html
import re

SYMBOL_KEYWORDS = {
    "bitcoin": "BTC", "btc": "BTC",
    "ethereum": "ETH", "eth": "ETH",
    "solana": "SOL", "sol": "SOL",
    "bnb": "BNB",
    "xrp": "XRP", "ripple": "XRP",
    "dogecoin": "DOGE", "doge": "DOGE",
    "cardano": "ADA", "ada": "ADA",
    "tron": "TRX", "trx": "TRX",
    "avalanche": "AVAX", "avax": "AVAX",
    "chainlink": "LINK", "link": "LINK",
    "polkadot": "DOT",
    "polygon": "MATIC", "matic": "MATIC",
    "litecoin": "LTC", "ltc": "LTC",
    "stablecoin": "STABLE", "usdt": "USDT", "usdc": "USDC",
    "etf": "ETF",
}

POSITIVE_WORDS = {
    "surge", "surged", "rally", "rallied", "bullish", "gains", "gained",
    "record", "high", "adoption", "approve", "approved", "launch",
    "launched", "growth", "recover", "recovered", "rebound", "breakthrough",
}

NEGATIVE_WORDS = {
    "crash", "crashed", "hack", "hacked", "bearish", "plunge", "plunged",
    "lawsuit", "fraud", "scam", "breach", "exploit", "fell", "drop",
    "dropped", "decline", "loss", "losses", "outflow", "outflows",
    "sinks", "tumble", "warning", "risk",
}


def clean_text(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def extract_symbols(text: str) -> list[str]:
    words = set(re.findall(r"[a-z]+", text.lower()))
    return sorted({s for w, s in SYMBOL_KEYWORDS.items() if w in words})


def classify_sentiment(text: str) -> str:
    words = set(re.findall(r"[a-z]+", text.lower()))
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"


def dedupe_by_url(articles: list[dict]) -> list[dict]:
    seen: set[str] = set()
    unique: list[dict] = []
    for article in articles:
        url = article.get("url") if isinstance(article, dict) else article.url
        if url not in seen:
            seen.add(url)
            unique.append(article)
    return unique
