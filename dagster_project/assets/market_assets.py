"""Pipeline: fetch_market -> validate_market -> loaded_snapshot (mỗi giờ)."""
from datetime import datetime, timezone

from dagster import AssetExecutionContext, asset
from pydantic import ValidationError

from dagster_project.market.schemas import RawMarket, from_coingecko
from dagster_project.market.validator import validate
from dagster_project.resources import CoinGeckoResource, PostgresResource


@asset
def fetch_market(
    context: AssetExecutionContext, coingecko: CoinGeckoResource
) -> list[dict]:
    """Lấy top coins từ CoinGecko, validate Pydantic."""
    raw_items = coingecko.fetch_markets()
    valid: list[dict] = []
    for item in raw_items:
        try:
            valid.append(RawMarket(**from_coingecko(item)).model_dump(mode="json"))
        except ValidationError as exc:
            context.log.warning(f"skip invalid coin {item.get('id')}: {exc}")
    context.log.info(f"fetched {len(valid)} coins")
    return valid


@asset
def validate_market(
    context: AssetExecutionContext, fetch_market: list[dict]
) -> dict:
    """Tách valid/errors theo rules."""
    valid, errors = validate([RawMarket(**a).model_dump() for a in fetch_market])
    for err in errors:
        context.log.warning(f"bad record: {err['error']} | {err['payload'].get('symbol')}")
    context.log.info(f"valid={len(valid)} errors={len(errors)}")
    return {
        "valid": [RawMarket(**v).model_dump(mode="json") for v in valid],
        "errors": errors,
    }


@asset
def loaded_snapshot(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    validate_market: dict,
) -> int:
    """INSERT snapshot + ghi bad records vào data_quality_errors."""
    collected_at = datetime.now(timezone.utc)
    inserted = postgres.insert_snapshot(collected_at, validate_market["valid"])
    postgres.insert_errors(validate_market["errors"])
    context.log.info(f"inserted {inserted} snapshots")
    return inserted
