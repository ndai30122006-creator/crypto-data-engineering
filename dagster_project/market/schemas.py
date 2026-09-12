"""Pydantic models + mapping field CoinGecko về tên nội bộ."""
from typing import Optional

from pydantic import BaseModel


class RawMarket(BaseModel):
    symbol: str
    name: str
    price: float
    market_cap: Optional[float] = None
    circulating_supply: Optional[float] = None
    volume_24h: Optional[float] = None
    price_change_24h: Optional[float] = None


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
