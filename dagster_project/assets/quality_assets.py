"""Asset kiểm định chất lượng nến OHLCV (chạy theo giờ cùng market_job)."""
from dagster import AssetExecutionContext, asset

from dagster_project.resources import PostgresResource
from streaming.quality_ohlcv import check_ohlcv, to_errors


@asset
def quarantine_ohlcv(
    context: AssetExecutionContext, postgres: PostgresResource
) -> list[dict]:
    """Quét nến 1m 70 phút gần nhất, vi phạm → data_quality_errors (pipeline=ohlcv)."""
    candles = postgres.fetch_ohlcv(minutes=70)
    violations = check_ohlcv(candles)
    errors = to_errors(violations)
    postgres.insert_errors(errors)
    context.log.info(f"scanned {len(candles)} candles → {len(violations)} violations")
    context.add_output_metadata(
        {"candles": len(candles), "violations": len(violations)}
    )
    return errors
