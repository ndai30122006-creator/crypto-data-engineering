"""Pipeline: fetch_market -> validate_market -> loaded_snapshot (mỗi giờ)."""

from datetime import UTC, datetime

import msgspec
from dagster import AssetExecutionContext, asset

from dagster_project.market.schemas import RawMarket, from_coingecko
from dagster_project.market.validator import validate
from dagster_project.resources import CoinGeckoResource, PostgresResource


@asset
def fetch_market(context: AssetExecutionContext, coingecko: CoinGeckoResource) -> dict:
    """Lấy top coins từ CoinGecko, validate msgspec."""
    raw_items = coingecko.fetch_markets()
    valid: list[dict] = []
    errors: list[dict] = []
    for item in raw_items:
        try:
            valid.append(
                msgspec.to_builtins(
                    msgspec.convert(from_coingecko(item), type=RawMarket)
                )
            )
        except (msgspec.ValidationError, AttributeError, TypeError, ValueError) as exc:
            errors.append(
                {"pipeline": "market", "payload": item, "error": f"schema: {exc}"}
            )
            context.log.warning(f"quarantine invalid coin schema: {exc}")
    context.log.info(f"fetched {len(valid)} coins")
    context.add_output_metadata(
        {"coins": len(valid), "raw_items": len(raw_items), "schema_errors": len(errors)}
    )
    return {"valid": valid, "errors": errors}


@asset
def validate_market(context: AssetExecutionContext, fetch_market: dict) -> dict:
    """Tách valid/errors theo rules (validate 1 lần duy nhất)."""
    records = fetch_market["valid"]
    valid, errors = validate(records)
    errors = [*fetch_market["errors"], *errors]
    for err in errors:
        context.log.warning(f"bad record: {err['error']}")
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
