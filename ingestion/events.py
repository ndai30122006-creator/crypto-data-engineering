"""Pure logic cho ingestion: URL stream, parse trade, serialize event.

Không import kafka/websocket ở đây để test offline được.
"""

import math
import re

import orjson

SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,20}$")
MAX_TIMESTAMP_MS = 253402300799999
MAX_TRADE_ID = 2**63 - 1


def _integer(value, maximum: int, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError("expected integer")
    if isinstance(value, str) and not value.isascii():
        raise ValueError("expected ASCII integer")
    result = int(value)
    if not minimum <= result <= maximum:
        raise ValueError("integer outside supported range")
    return result


def normalize_trade(data: dict) -> dict | None:
    """Validate the v2 trade contract; retain Binance's business identity."""
    if not isinstance(data, dict):
        return None
    try:
        symbol = data["symbol"]
        if not isinstance(symbol, str):
            return None
        symbol = symbol.upper()
        if not SYMBOL_RE.fullmatch(symbol):
            return None
        if isinstance(data["price"], bool) or isinstance(data["quantity"], bool):
            return None
        price, quantity = float(data["price"]), float(data["quantity"])
        if not (
            math.isfinite(price)
            and math.isfinite(quantity)
            and price > 0
            and quantity > 0
        ):
            return None
        return {
            "symbol": symbol,
            "trade_id": _integer(data["trade_id"], MAX_TRADE_ID, 0),
            "price": price,
            "quantity": quantity,
            "timestamp": _integer(data["timestamp"], MAX_TIMESTAMP_MS, 1),
        }
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def decode_trade_fields(raw: bytes) -> tuple[str, int, float, float, int]:
    """Safe Kafka boundary: invalid JSON/schema yields a rejectable sentinel."""
    try:
        event = normalize_trade(orjson.loads(raw))
    except (orjson.JSONDecodeError, TypeError):
        event = None
    if event is None:
        return ("", -1, 0.0, 0.0, 0)
    return (
        event["symbol"],
        event["trade_id"],
        event["price"],
        event["quantity"],
        event["timestamp"],
    )


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
    Event: {"symbol", "trade_id", "price", "quantity", "timestamp"} (ms).
    """
    data = msg.get("data", msg) if isinstance(msg, dict) else None
    if not isinstance(data, dict) or data.get("e") != "trade":
        return None
    return normalize_trade(
        {
            "symbol": data.get("s"),
            "trade_id": data.get("t"),
            "price": data.get("p"),
            "quantity": data.get("q"),
            "timestamp": data.get("T"),
        }
    )


def serialize_event(event: dict) -> bytes:
    """Serialize event → JSON bytes cho Kafka value (orjson, nhanh)."""
    return orjson.dumps(event)
