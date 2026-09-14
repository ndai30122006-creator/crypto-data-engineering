"""msgspec Structs + mapping field CoinGecko về tên nội bộ (thay Pydantic)."""
import msgspec


class RawMarket(msgspec.Struct):
    symbol: str
    name: str
    price: float
    market_cap: float | None = None
    circulating_supply: float | None = None
    volume_24h: float | None = None
    price_change_24h: float | None = None


def from_coingecko(item: dict) -> dict:
    """Lớp cách ly: API đổi tên field thì chỉ sửa ở đây."""
    return {
        "symbol": str(item.get("symbol", "")).upper(),
        "name": item.get("name", ""),
        "price": item.get("current_price"),
        "market_cap": item.get("market_cap"),
        "circulating_supply": item.get("circulating_supply"),
        "volume_24h": item.get("total_volume"),
        "price_change_24h": item.get("price_change_percentage_24h"),
    }
