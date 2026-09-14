"""Unit tests cho metrics/alert (mock infra, offline)."""
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from alert import CHECKS, evaluate
from metrics import collect


def _metrics(**over):
    base = {
        "dagster_runs": {"success": 5, "failed": 0},
        "binance": {"events_received_total": 100, "events_published_total": 100,
                    "events_invalid_total": 0, "publish_failures_total": 0,
                    "events_lost": 0},
        "kafka": {"group": "pathway-ohlcv-1m", "lag_total": 6, "log_end_total": 100,
                  "produce_per_sec": 40.0},
        "db": {"news_1h": 10, "candles_10m": 50, "newest_candle_age_min": 2.0},
    }
    for section, values in over.items():
        base.setdefault(section, {}).update(values)
    return base


def test_evaluate_green():
    assert evaluate(_metrics(), env={}) == []


def test_evaluate_fires_each_rule():
    alerts = evaluate(
        _metrics(
            dagster_runs={"success": 0, "failed": 2},
            binance={"events_lost": 3},
            kafka={"lag_total": 99999},
            db={"news_1h": 0, "candles_10m": 5, "newest_candle_age_min": 99.0},
        ),
        env={},
    )
    assert len(alerts) == 6
    assert any("failed_runs=2" in a for a in alerts)
    assert any("candles_10m=5" in a for a in alerts)
    assert any("events_lost=3" in a for a in alerts)
    assert any("lag_total=99999" in a for a in alerts)


def test_evaluate_skips_rule_when_section_errored():
    alerts = evaluate(
        _metrics(binance={"error": "x"}, kafka={"error": "y"}), env={}
    )
    assert not any("events_lost" in a or "lag_total" in a for a in alerts)


def test_evaluate_db_unreachable():
    alerts = evaluate({"db": {"error": "boom"}, "dagster_runs": {}}, env={})
    assert len(alerts) == 1 and "unreachable" in alerts[0]


def test_evaluate_env_override():
    assert evaluate(_metrics(db={"candles_10m": 5}), env={"ALERT_MIN_CANDLES_10M": "1"}) == []


def test_checks_have_actions():
    assert all(len(c) == 5 and c[4] for c in CHECKS)


def test_collect_shape_with_mocked_infra():
    with (
        patch("metrics._docker_logs", side_effect=["RUN_SUCCESS", ""]),
        patch("metrics.db_stats", return_value={"news_1h": 1}),
    ):
        out = collect(minutes=60)
    assert out["dagster_runs"] == {"success": 1, "failed": 0}
    assert out["window_minutes"] == 60
    json.dumps(out)  # serialize được cho máy đọc


def test_kafka_group_parses_describe():
    from metrics import kafka_group

    fake = (
        "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID\n"
        "pathway-ohlcv-1m crypto.trades 0 100 110 10 host/id\n"
    )
    proc = MagicMock()
    proc.returncode = 0
    proc.stdout = fake
    with patch("metrics.subprocess.run", return_value=proc):
        out = kafka_group()
    assert out["lag_total"] == 10 and out["log_end_total"] == 110


def test_kafka_group_describe_failure():
    from metrics import kafka_group

    proc = MagicMock()
    proc.returncode = 1
    proc.stdout = ""
    with patch("metrics.subprocess.run", return_value=proc):
        assert "error" in kafka_group()


def test_kafka_rate_needs_two_samples(tmp_path):
    import datetime as real_dt
    import tempfile

    from metrics import kafka_rate

    with patch.object(tempfile, "gettempdir", return_value=str(tmp_path)):
        assert kafka_rate(1000) is None  # lần đầu chưa có baseline
        now = real_dt.datetime.now(real_dt.UTC).timestamp()
        (tmp_path / "crypto_kafka_rate.json").write_text(
            json.dumps({"end": 900, "ts": now - 10})
        )
        rate = kafka_rate(1000)  # (1000-900)/10s
        assert rate is not None and abs(rate - 10.0) < 1.0
