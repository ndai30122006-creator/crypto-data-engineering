"""Asset phát hiện tín hiệu từ nến 1m (chạy theo giờ cùng market_job)."""

from datetime import UTC, datetime

from dagster import AssetExecutionContext, asset

from dagster_project.resources import PostgresResource
from streaming.signals import HISTORY_MINUTES, detect_volume_spike


@asset
def detected_signals(
    context: AssetExecutionContext, postgres: PostgresResource
) -> list[dict]:
    """Đọc 125 phút, xét mọi cửa sổ 5m đã đóng trong giờ gần nhất."""
    now = datetime.now(UTC)
    candles = postgres.fetch_candles(minutes=HISTORY_MINUTES)
    signals = detect_volume_spike(candles, now=now)
    inserted = postgres.insert_signals(signals)
    context.log.info(
        f"scanned {len(candles)} candles → {len(signals)} signals ({inserted} new)"
    )
    context.add_output_metadata(
        {"candles": len(candles), "signals": len(signals), "inserted": inserted}
    )
    return signals
