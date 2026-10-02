"""Regression coverage for the correctness and recovery review findings."""

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import orjson
import pytest
from dagster import build_op_context
from kafka.future import Future

from dagster_project.assets.market_assets import (
    fetch_market,
    loaded_snapshot,
    validate_market,
)
from ingestion import binance_consumer as consumer
from ingestion.dlq import append_record
from ingestion.events import decode_trade_fields, normalize_trade, parse_trade
from scripts.replay_dlq import replay_file
from streaming.postgres_sink import UPSERT_1M, write_candle_with_retry
from streaming.signals import detect_volume_spike
from streaming.windows import aggregate

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from alert import evaluate
from metrics import binance_consumer, db_stats
from status import CONTAINERS, container_health, render


def trade(**over):
    return {
        "symbol": "BTCUSDT",
        "trade_id": 1,
        "price": 9.0,
        "quantity": 1.0,
        "timestamp": 1757578861000,
        **over,
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("price", "NaN"),
        ("price", "Infinity"),
        ("quantity", "-Infinity"),
        ("symbol", None),
        ("symbol", "BTC USDT"),
        ("trade_id", True),
        ("trade_id", None),
        ("trade_id", -1),
        ("trade_id", 1.2),
        ("timestamp", False),
        ("timestamp", 0),
        ("timestamp", 2**63),
    ],
)
def test_invalid_normalized_contract(field, value):
    assert normalize_trade(trade(**{field: value})) is None


def test_binance_identity_retained_and_legacy_rejected():
    raw = {
        "e": "trade",
        "s": "BTCUSDT",
        "t": 55,
        "p": "9",
        "q": "1",
        "T": 1757578861000,
    }
    assert parse_trade(raw)["trade_id"] == 55
    del raw["t"]
    assert parse_trade(raw) is None


@pytest.mark.parametrize(
    "raw", [b"not-json", b"{}", b"null", b"[]", b'{"price":"NaN"}']
)
def test_raw_kafka_boundary_rejects_without_crashing(raw):
    assert decode_trade_fields(raw) == ("", -1, 0.0, 0.0, 0)


def test_duplicates_and_same_timestamp_keep_exact_ohlcv():
    first, last = trade(), trade(trade_id=2, price=10.0, quantity=2.0)
    candle = aggregate([last, first, last, first])[0]
    assert (
        candle["open"],
        candle["close"],
        candle["volume"],
        candle["trade_count"],
    ) == (9.0, 10.0, 3.0, 2)
    assert len(aggregate([first, trade(symbol="ETHUSDT")])) == 2
    with pytest.raises(ValueError, match="conflicting"):
        aggregate([first, trade(price=100.0)])


@pytest.fixture
def state():
    for key in consumer._counters:
        consumer._counters[key] = 0
    consumer._last_beat = consumer._flushed_at = 0
    yield
    for key in consumer._counters:
        consumer._counters[key] = 0
    consumer._last_beat = consumer._flushed_at = 0


def send_one(tmp_path, producer):
    cfg = {
        "topic": "crypto.trades.v2",
        "heartbeat_file": str(tmp_path / "heartbeat"),
        "metrics_file": str(tmp_path / "metrics.json"),
        "delivery_dlq_file": str(tmp_path / "trade.jsonl"),
        "flush_every": 100,
    }
    raw = {"e": "trade", "t": 1, "s": "BTCUSDT", "p": "9", "q": "1", "T": 1757578861000}
    consumer.on_raw_message(producer, cfg, orjson.dumps(raw))
    return cfg


def test_publish_ack_accounting_and_heartbeat(tmp_path, state):
    producer, future = MagicMock(), Future()
    producer.send.return_value = future
    cfg = send_one(tmp_path, producer)
    assert consumer.snapshot_metrics()["events_pending"] == 1
    assert consumer.snapshot_metrics()["events_published_total"] == 0
    assert not Path(cfg["heartbeat_file"]).exists()
    future.success(MagicMock())
    assert consumer.snapshot_metrics()["events_pending"] == 0
    assert consumer.snapshot_metrics()["events_published_total"] == 1
    assert Path(cfg["heartbeat_file"]).exists()


@pytest.mark.parametrize("synchronous", [False, True])
def test_failed_publish_preserves_event_and_alerts(tmp_path, state, synchronous):
    producer, future = MagicMock(), Future()
    if synchronous:
        producer.send.side_effect = RuntimeError("broker down")
    else:
        producer.send.return_value = future
    cfg = send_one(tmp_path, producer)
    if not synchronous:
        future.failure(RuntimeError("broker down"))
    stats = consumer.snapshot_metrics()
    assert stats["events_published_total"] == 0
    assert stats["publish_failures_total"] == 1
    assert stats["events_pending"] == 0
    saved = orjson.loads(Path(cfg["delivery_dlq_file"]).read_bytes())
    assert saved["payload"]["event"]["trade_id"] == 1
    proc = MagicMock(returncode=0, stdout=json.dumps(stats))
    with patch("metrics.subprocess.run", return_value=proc):
        measured = binance_consumer()
    assert measured["events_lost"] == 0
    assert any(
        "publish_failures_total=1" in message
        for message in evaluate(green(binance=measured), env={})
    )


def test_failed_dlq_is_visible(tmp_path, state):
    producer, future = MagicMock(), Future()
    producer.send.return_value = future
    send_one(tmp_path, producer)
    with patch("ingestion.binance_consumer.append_record", side_effect=PermissionError):
        future.failure(RuntimeError("down"))
    assert consumer.snapshot_metrics()["dlq_write_failures_total"] == 1


def green(**over):
    value = {
        "db": {"news_1h": 1, "candles_10m": 50, "newest_candle_age_min": 1},
        "kafka": {"lag_total": 0},
        "dagster_runs": {"failed": 0},
        "binance": {
            "events_lost": 0,
            "publish_failures_total": 0,
            "dlq_write_failures_total": 0,
        },
    }
    value.update(over)
    return value


def test_unhealthy_and_stale_status_are_red():
    proc = MagicMock(
        returncode=0,
        stdout="\n".join(f"{name}|Up 2 minutes (unhealthy)" for _, name in CONTAINERS),
    )
    with patch("status.subprocess.run", return_value=proc):
        assert set(container_health().values()) == {"unhealthy"}
    bad = green(db={"news_1h": 1, "candles_10m": 1, "newest_candle_age_min": 999})
    _, code = render(bad, {name: "ok" for name, _ in CONTAINERS})
    assert code == 1


def test_invalid_threshold_and_negative_accounting_alert():
    assert any(
        "invalid threshold" in a
        for a in evaluate(green(), {"ALERT_MAX_CONSUMER_LAG": "NaN"})
    )
    assert any(
        "events_lost=-1" in a
        for a in evaluate(
            green(
                binance={
                    "events_lost": -1,
                    "publish_failures_total": 0,
                    "dlq_write_failures_total": 0,
                }
            ),
            {},
        )
    )


@pytest.mark.parametrize("env", ["local", "staging", "prod"])
def test_db_metrics_queries_only_requested_environment(env):
    conn, cur = MagicMock(), MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchone.side_effect = [
        (10,),
        (1,),
        (50, 1000),
        (datetime.now(UTC),),
        (100,),
        (50,),
        (0,),
    ]
    with (
        patch("psycopg2.connect", return_value=conn),
        patch.dict("os.environ", {"DAGSTER_ENVIRONMENT": env}),
    ):
        assert db_stats()["snapshots_total"] == 50
    queries = " ".join(call.args[0] for call in cur.execute.call_args_list)
    suffix = "" if env == "prod" else f"_{env}"
    assert f"FROM crypto_market_snapshot{suffix};" in queries
    if env != "local":
        assert "_local" not in queries


def test_schema_errors_reach_quarantine():
    api, pg = MagicMock(), MagicMock()
    api.fetch_markets.return_value = [
        None,
        {"symbol": "bad", "current_price": None},
        {"symbol": "btc", "name": "Bitcoin", "current_price": 10},
    ]
    pg.insert_snapshot.return_value = 1
    with build_op_context() as context:
        batch = fetch_market(context, api)
        split = validate_market(context, batch)
        loaded_snapshot(context, pg, split)
    assert len(split["valid"]) == 1 and len(split["errors"]) == 2
    assert len(pg.insert_errors.call_args.args[0]) == 2


def test_mid_hour_spike_detected_but_missing_baseline_and_partial_skipped():
    rows = [
        {
            "symbol": "BTCUSDT",
            "window_start": i * 60,
            "volume": 100.0 if 80 <= i < 85 else 10.0,
        }
        for i in range(125)
    ]
    now = datetime.fromtimestamp(125 * 60, tz=UTC)
    result = detect_volume_spike(rows, now=now)
    assert any(sig["window_start"] == 84 * 60 for sig in result)
    partial = rows + [{"symbol": "BTCUSDT", "window_start": 125 * 60, "volume": 100000}]
    assert detect_volume_spike(partial, now=now) == result
    assert (
        detect_volume_spike([c for c in rows if c["window_start"] != 60 * 60], now=now)
        == []
    )


def test_sink_reports_unwritable_dlq(tmp_path):
    with (
        patch("streaming.postgres_sink.upsert_candles", side_effect=RuntimeError),
        patch("streaming.postgres_sink.append_record", side_effect=PermissionError),
    ):
        with pytest.raises(RuntimeError, match="DLQ write failed"):
            write_candle_with_retry(None, {}, retries=1, dlq_path=str(tmp_path / "dlq"))
    assert "WHERE market_1m.updated_at <= EXCLUDED.updated_at" in UPSERT_1M


def test_replay_resumes_only_after_ack_and_preserves_source(tmp_path):
    source = tmp_path / "trade.jsonl"
    for identity in [1, 2]:
        append_record(str(source), "trade", {"event": trade(trade_id=identity)}, "test")
    original = source.read_bytes()
    handled = []

    def fail_second(record):
        identity = record["payload"]["event"]["trade_id"]
        if identity == 2:
            raise RuntimeError("no ack")
        handled.append(identity)

    with pytest.raises(RuntimeError):
        replay_file(source, fail_second)
    assert handled == [1]
    assert (
        replay_file(
            source, lambda rec: handled.append(rec["payload"]["event"]["trade_id"])
        )
        == 1
    )
    assert handled == [1, 2] and source.read_bytes() == original
    assert replay_file(source, lambda _: pytest.fail("already acknowledged")) == 0


def test_replay_dry_run_never_sends_or_checkpoints(tmp_path):
    source = tmp_path / "trade.jsonl"
    append_record(str(source), "trade", {"event": trade()}, "test")
    assert (
        replay_file(source, lambda _: pytest.fail("dry run sent data"), dry_run=True)
        == 1
    )
    assert not source.with_suffix(".jsonl.checkpoint.json").exists()


def test_replay_rejects_replaced_file_and_candle_without_version(tmp_path):
    source = tmp_path / "records.jsonl"
    append_record(str(source), "trade", {"event": trade()}, "test")
    replay_file(source, lambda _: None)
    source.write_bytes(b"replaced\n")
    with pytest.raises(ValueError, match="DLQ changed"):
        replay_file(source, lambda _: None)
    fresh = tmp_path / "candles.jsonl"
    append_record(str(fresh), "candle", {"symbol": "BTCUSDT"}, "test")
    with pytest.raises(ValueError, match="updated_at"):
        replay_file(fresh, lambda _: None)


def test_retry_reconnects_dead_connection(tmp_path):
    import psycopg2

    dead, replacement = MagicMock(closed=False), MagicMock(closed=False)
    connector = MagicMock(return_value=replacement)
    with (
        patch("streaming.postgres_sink.time.sleep"),
        patch(
            "streaming.postgres_sink.upsert_candles",
            side_effect=[psycopg2.OperationalError(), 1],
        ) as write,
    ):
        returned = write_candle_with_retry(
            dead, {"symbol": "BTCUSDT"}, retries=2, connect=connector
        )
    assert returned is replacement
    dead.close.assert_called_once()
    connector.assert_called_once()
    assert write.call_args_list[1].args[0] is replacement


@pytest.mark.parametrize(
    "output",
    [
        "",
        "GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG\n",
        "custom crypto.trades.v2 0 - 100 - host\n",
    ],
)
def test_missing_kafka_offsets_are_unavailable_not_zero(output, monkeypatch):
    from metrics import kafka_group

    monkeypatch.setenv("KAFKA_GROUP_ID", "custom")
    with patch(
        "metrics.subprocess.run", return_value=MagicMock(returncode=0, stdout=output)
    ) as run:
        data = kafka_group()
    assert "error" in data and "lag_total" not in data
    assert run.call_args.args[0][-1] == "custom"


def test_status_respects_failed_runs_threshold_override(monkeypatch):
    monkeypatch.setenv("ALERT_MAX_FAILED_RUNS", "10")
    metrics = green(dagster_runs={"failed": 2})
    health = {label: "ok" for label, _ in CONTAINERS}
    assert evaluate(metrics) == []
    assert render(metrics, health)[1] == 0


def test_status_invalid_age_alerts_without_crashing():
    health = {label: "ok" for label, _ in CONTAINERS}
    text, code = render(green(db={"newest_candle_age_min": "invalid"}), health)
    assert code == 1 and "newest_candle_age_min invalid" in text
