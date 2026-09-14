"""Unit tests cho market fetcher/schemas/validator (mock, offline)."""
import pytest
from pydantic import ValidationError

from dagster_project.market.schemas import RawMarket, from_coingecko
from dagster_project.market.validator import validate, validate_record

MOCK_ITEM = {
    "id": "bitcoin",
    "symbol": "btc",
    "name": "Bitcoin",
    "current_price": 77225,
    "market_cap": 1550962185512,
    "total_volume": 33640570957,
    "circulating_supply": 20083143.0,
    "price_change_percentage_24h": 0.09525,
}


def test_from_coingecko_maps_fields():
    mapped = RawMarket(**from_coingecko(MOCK_ITEM))
    assert mapped.symbol == "BTC"
    assert mapped.price == 77225
    assert mapped.volume_24h == 33640570957


def test_from_coingecko_missing_price_rejected():
    with pytest.raises(ValidationError):
        RawMarket(**from_coingecko({**MOCK_ITEM, "current_price": None}))


def test_validate_record_ok():
    assert validate_record(RawMarket(**from_coingecko(MOCK_ITEM)).model_dump()) is None


def test_validate_record_bad_price():
    assert "price" in validate_record({"symbol": "BTC", "price": -1}).lower()


def test_validate_record_missing_symbol():
    assert validate_record({"symbol": "", "price": 10}) == "missing symbol"


def test_validate_record_bad_market_cap():
    rec = {"symbol": "BTC", "price": 10, "market_cap": 0}
    assert "market_cap" in validate_record(rec)


def test_validate_splits_valid_and_errors():
    good = RawMarket(**from_coingecko(MOCK_ITEM)).model_dump()
    bad = {"symbol": "XXX", "price": 0}
    valid, errors = validate([good, bad])
    assert len(valid) == 1
    assert len(errors) == 1
    assert errors[0]["pipeline"] == "market"
    assert errors[0]["error"] == "price <= 0 (0)"
