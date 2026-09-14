"""Pipeline: fetch_market -> validate_market -> loaded_snapshot (mỗi giờ)."""
from datetime import UTC, datetime

import msgspec
from dagster import AssetExecutionContext, asset

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
            valid.append(msgspec.to_builtins(msgspec.convert(from_coingecko(item), type=RawMarket)))
        except msgspec.ValidationError as exc:
            context.log.warning(f"skip invalid coin {item.get('id')}: {exc}")
    context.log.info(f"fetched {len(valid)} coins")
    context.add_output_metadata({"coins": len(valid), "raw_items": len(raw_items)})
    return valid


@asset
def validate_market(
    context: AssetExecutionContext, fetch_market: list[dict]
) -> dict:
    """Tách valid/errors theo rules (validate 1 lần duy nhất)."""
    records = [msgspec.to_builtins(msgspec.convert(a, type=RawMarket)) for a in fetch_market]
    valid, errors = validate(records)
    for err in errors:
        context.log.warning(f"bad record: {err['error']} | {err['payload'].get('symbol')}")
    context.log.info(f"valid={len(valid)} errors={len(errors)}")
    context.add_output_metadata({"valid": len(valid), "errors": len(errors)})
    return {"valid": valid, "errors": errors}


@asset
def loaded_snapshot(
    context: AssetExecutionContext,
    postgres: PostgresResource,
    validate_market: dict,
) -> int:
    """INSERT snapshot + ghi bad records vào data_quality_errors."""
    collected_at = datetime.now(UTC)
    inserted = postgres.insert_snapshot(collected_at, validate_market["valid"])
    postgres.insert_errors(validate_market["errors"])
    context.log.info(
        f"inserted {inserted} snapshots "
        f"({len(validate_market['errors'])} bad records quarantined)"
    )
    context.add_output_metadata(
        {"inserted": inserted, "quarantined": len(validate_market["errors"])}
    )
    return inserted
