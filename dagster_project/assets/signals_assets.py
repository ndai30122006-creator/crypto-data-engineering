"""Asset phát hiện tín hiệu từ nến 1m (chạy theo giờ cùng market_job)."""
from dagster import AssetExecutionContext, asset

from dagster_project.resources import PostgresResource
from streaming.signals import detect_volume_spike


@asset
def detected_signals(
    context: AssetExecutionContext, postgres: PostgresResource
) -> list[dict]:
    """Quét nến 1m 70 phút gần nhất, ghi VOLUME_SPIKE vào bảng signals."""
    candles = postgres.fetch_candles(minutes=70)
    signals = detect_volume_spike(candles)
    inserted = postgres.insert_signals(signals)
    context.log.info(
        f"scanned {len(candles)} candles → {len(signals)} signals ({inserted} new)"
    )
    context.add_output_metadata(
        {"candles": len(candles), "signals": len(signals), "inserted": inserted}
    )
    return signals
