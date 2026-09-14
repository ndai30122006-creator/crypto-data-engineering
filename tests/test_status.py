"""Unit tests cho status dashboard (pure render, offline)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from status import CONTAINERS, container_health, render


def _metrics(**over):
    base = {
        "db": {"rows_total": 1254320, "newest_candle_age_min": 0.13},
        "kafka": {"log_end_total": 152421, "lag_total": 0},
        "binance": {"publish_failures_total": 4},
        "dagster_runs": {"failed": 0},
    }
    for section, values in over.items():
        base.setdefault(section, {}).update(values)
    return base


def _health(ok=True):
    return {label: ("ok" if ok else "down") for label, _ in CONTAINERS}


def test_render_green_dashboard():
    text, code = render(_metrics(), _health(ok=True))
    assert code == 0
    assert "Crypto Data Platform Status" in text
    assert "1,254,320" in text  # DB Rows format nghìn
    assert "8 sec" in text  # 0.13 min → sec
    assert "152,421" in text and "Consumer Lag" in text
    assert text.count("✓") == len(CONTAINERS)


def test_render_red_counts_failures():
    text, code = render(_metrics(db={"error": "boom"}), _health(ok=False))
    assert code == 1
    assert "✗" in text and "boom" in text


def test_render_failed_runs_flagged():
    _, code = render(_metrics(dagster_runs={"failed": 2}), _health(ok=True))
    assert code == 1


def test_render_missing_sections_degraded():
    text, code = render({}, {})
    assert code == 1
    assert "n/a" in text


def test_container_health_parses_ps():
    from unittest.mock import MagicMock, patch

    proc = MagicMock()
    proc.stdout = "crypto-postgres|Up 5 minutes (healthy)\nfoo|Up 1 minute\n"
    with patch("status.subprocess.run", return_value=proc):
        health = container_health()
    assert health["PostgreSQL"] == "ok"
    assert health["Kafka"] == "down"
