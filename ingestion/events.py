"""Pure logic cho ingestion: URL stream, parse trade, serialize event.

Không import kafka/websocket ở đây để test offline được.
"""
import json

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream"


def combined_stream_url(symbols: list[str]) -> str:
    """URL combined stream: 1 kết nối cho mọi cặp (...@trade/...@trade)."""
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)
    return f"{BINANCE_WS_BASE}?streams={streams}"


def parse_trade(msg: dict) -> dict | None:
    """Parse 1 message Binance trade → event nội bộ.

    Combined stream bọc payload trong {"stream", "data"}; direct stream
    thì payload nằm ngay top-level. Trả None nếu message lỗi/rác.
    Event: {"symbol", "price", "quantity", "timestamp"} (timestamp = ms).
    """
    data = msg.get("data", msg) if isinstance(msg, dict) else None
    if not isinstance(data, dict) or data.get("e") != "trade":
        return None
    try:
        price = float(data["p"])
        quantity = float(data["q"])
        symbol = str(data["s"]).upper()
        timestamp = int(data["T"])
    except (KeyError, TypeError, ValueError):
        return None
    if not symbol or price <= 0 or quantity <= 0 or timestamp <= 0:
        return None
    return {
        "symbol": symbol,
        "price": price,
        "quantity": quantity,
        "timestamp": timestamp,
    }


def serialize_event(event: dict) -> bytes:
    """Serialize event → JSON bytes cho Kafka value."""
    return json.dumps(event).encode("utf-8")
