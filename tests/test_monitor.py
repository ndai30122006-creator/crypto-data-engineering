"""Unit tests cho metrics/alert (mock infra, offline)."""
import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from alert import CHECKS, evaluate
from metrics import collect


def _metrics(**over):
    base = {
        "dagster_runs": {"success": 5, "failed": 0},
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
            db={"news_1h": 0, "candles_10m": 5, "newest_candle_age_min": 99.0},
        ),
        env={},
    )
    assert len(alerts) == 4
    assert any("failed_runs=2" in a for a in alerts)
    assert any("candles_10m=5" in a for a in alerts)


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
