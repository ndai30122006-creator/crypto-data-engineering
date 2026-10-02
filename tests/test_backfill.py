from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
from kafka.future import Future
from kafka.producer.future import FutureRecordMetadata

from ingestion.backfill import BackfillWorker, GapState, HistoricalTrades, RateLimited


def event(value=10, symbol="BTCUSDT"):
    return {"symbol": symbol, "trade_id": value, "price": 100.0,
            "quantity": 1.0, "timestamp": 1757578861000}


def rest_row(value):
    return {"id": value, "price": "100", "qty": "1", "time": 1757578861000}


def rest(handler):
    return HistoricalTrades(httpx.Client(base_url="https://api.binance.com",
                                         transport=httpx.MockTransport(handler)), wait=lambda _: None)


def store(tmp_path):
    return GapState(tmp_path / "gaps.json", "kafka:9092/crypto.trades.v2")


def producer(fail_id=None):
    mock = MagicMock()

    def send(_topic, key, value):
        future = FutureRecordMetadata(Future(), 0, None, None, 1, 1, 0)
        if value["trade_id"] == fail_id:
            future.failure(RuntimeError("delivery failed"))
        else:
            future.success(None)
        return future

    mock.send.side_effect = send
    return mock


def test_gap_is_durable_before_later_ack_and_restart_detects_outage(tmp_path):
    state = store(tmp_path)
    state.observe(event(10))
    state.ack(event(10))
    state.observe(event(14))
    disk = store(tmp_path).snapshot()
    assert disk["ack"] == {"BTCUSDT": 10}
    assert disk["gaps"] == {"BTCUSDT": [[11, 13]]}
    state.ack(event(14))
    state.flush()
    restarted = store(tmp_path)
    restarted.observe(event(20))
    assert restarted.snapshot()["gaps"]["BTCUSDT"] == [[11, 13], [15, 19]]
    restarted.observe(event(18))
    restarted.observe(event(20))
    assert restarted.snapshot()["gaps"]["BTCUSDT"] == [[11, 13], [15, 19]]


def test_failure_ranges_merge_and_complete_only_prefix(tmp_path):
    state = store(tmp_path)
    for value in [11, 13, 12, 11, 20]:
        state.failed(event(value))
    assert state.snapshot()["gaps"]["BTCUSDT"] == [[11, 13], [20, 20]]
    state.complete("BTCUSDT", 12, 12)
    assert store(tmp_path).snapshot()["gaps"]["BTCUSDT"] == [[11, 11], [13, 13], [20, 20]]


def test_ack_failure_preserves_suffix_and_restart_resumes(tmp_path):
    state = store(tmp_path)
    state.observe(event(10))
    state.ack(event(10))
    state.observe(event(14))
    api = rest(lambda request: httpx.Response(200, json=[rest_row(i) for i in range(
        int(request.url.params["fromId"]), int(request.url.params["fromId"])+int(request.url.params["limit"]))]))
    worker = BackfillWorker(state, producer(fail_id=12), "crypto.trades.v2", api)
    with pytest.raises(RuntimeError):
        worker.recover_one()
    assert store(tmp_path).snapshot()["gaps"]["BTCUSDT"] == [[12, 13]]
    restarted = BackfillWorker(store(tmp_path), producer(), "crypto.trades.v2", api)
    assert restarted.recover_one() == 2
    assert restarted.recover_one() == 0
    assert restarted.snapshot()["pending_trades"] == 0


def test_pages_are_bounded_and_fair_across_symbols(tmp_path):
    state = store(tmp_path)
    for symbol in ["BTCUSDT", "ETHUSDT"]:
        state.observe(event(0, symbol))
        state.observe(event(2002, symbol))
    requested = []

    def handle(request):
        params = request.url.params
        requested.append((params["symbol"], int(params["limit"])))
        return httpx.Response(200, json=[rest_row(i) for i in range(
            int(params["fromId"]), int(params["fromId"])+int(params["limit"]))])

    worker = BackfillWorker(state, producer(), "crypto.trades.v2", rest(handle))
    assert worker.recover_one() == 1000
    assert worker.recover_one() == 1000
    assert requested == [("BTCUSDT", 1000), ("ETHUSDT", 1000)]
    assert worker.snapshot()["pending_trades"] == 2002


@pytest.mark.parametrize("payload", [[], {}, [rest_row(12)], [rest_row(10), rest_row(12)],
                                    [{**rest_row(10), "qty": "NaN"}], ["invalid"]])
def test_invalid_raw_page_does_not_advance_checkpoint(tmp_path, payload):
    state = store(tmp_path)
    state.failed(event(10))
    worker = BackfillWorker(state, producer(), "crypto.trades.v2",
                            rest(lambda _: httpx.Response(200, json=payload)))
    with pytest.raises(ValueError):
        worker.recover_one()
    assert state.snapshot()["gaps"]["BTCUSDT"] == [[10, 10]]
    worker.producer.send.assert_not_called()


@pytest.mark.parametrize("status", [418, 429])
def test_rate_limit_keeps_gap_and_honors_retry_after(tmp_path, status):
    state = store(tmp_path)
    state.failed(event(10))
    worker = BackfillWorker(state, producer(), "crypto.trades.v2", rest(
        lambda _: httpx.Response(status, headers={"Retry-After": "120"})))
    with pytest.raises(RateLimited) as caught:
        worker.recover_one()
    assert caught.value.seconds == 120
    assert store(tmp_path).snapshot()["gaps"]["BTCUSDT"] == [[10, 10]]


def test_rest_retries_bounded_for_server_errors_and_transport():
    requests = []

    def fail(request):
        requests.append(request)
        return httpx.Response(503)

    with pytest.raises(RuntimeError, match="HTTP 503"):
        rest(fail).fetch("BTCUSDT", 10, 1)
    assert len(requests) == 3
    requests.clear()

    def transport(request):
        requests.append(request)
        raise httpx.ConnectError("offline", request=request)

    with pytest.raises(RuntimeError, match="transport"):
        rest(transport).fetch("BTCUSDT", 10, 1)
    assert len(requests) == 3


def test_checkpoint_write_failure_cannot_skip_gap_on_disk(tmp_path, monkeypatch):
    state = store(tmp_path)
    state.failed(event(10))
    worker = BackfillWorker(state, producer(), "crypto.trades.v2",
                            rest(lambda _: httpx.Response(200, json=[rest_row(10)])))
    monkeypatch.setattr(state, "_save", MagicMock(side_effect=OSError("disk full")))
    with pytest.raises(OSError):
        worker.recover_one()
    assert store(tmp_path).snapshot()["gaps"]["BTCUSDT"] == [[10, 10]]


def test_destination_mismatch_and_corrupted_checkpoint_fail_closed(tmp_path):
    state = store(tmp_path)
    state.flush()
    with pytest.raises(ValueError, match="destination"):
        GapState(state.path, "other/topic")
    state.path.write_text("not-json")
    with pytest.raises(ValueError):
        store(tmp_path)


def test_fatal_checkpoint_failure_stops_worker_and_is_visible(tmp_path):
    stopped = MagicMock()
    worker = BackfillWorker(store(tmp_path), producer(), "crypto.trades.v2", fatal_callback=stopped)
    worker.fatal(PermissionError("unwritable"))
    assert worker.stop.is_set()
    assert worker.snapshot()["checkpoint_failures_total"] == 1
    stopped.assert_called_once()
    worker.rest.close()


def test_backfill_pending_gap_is_an_alert():
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    from alert import evaluate

    alerts = evaluate({"binance": {"backfill": {"enabled": True, "pending_trades": 4,
                                               "checkpoint_failures_total": 1}}})
    assert any("backfill pending_trades=4" in alert for alert in alerts)
    assert any("backfill checkpoint_failures_total=1" in alert for alert in alerts)


def test_live_delivery_hooks_track_failure_and_ack(tmp_path):
    import orjson

    from ingestion.binance_consumer import on_raw_message

    state = store(tmp_path)
    state.observe(event(10))
    state.ack(event(10))
    worker = BackfillWorker(state, producer(fail_id=14), "crypto.trades.v2", rest(lambda _: None))
    raw = orjson.dumps({"data": {"e": "trade", "s": "BTCUSDT", "t": 14,
                                "p": "100", "q": "1", "T": 1757578861000}}).decode()
    cfg = {"topic": "crypto.trades.v2", "backfill": worker,
           "heartbeat_file": str(tmp_path / "heartbeat"), "flush_every": 10**9,
           "metrics_file": str(tmp_path / "metrics.json"),
           "delivery_dlq_file": str(tmp_path / "trades.jsonl")}
    on_raw_message(worker.producer, cfg, raw)
    assert store(tmp_path).snapshot()["gaps"]["BTCUSDT"] == [[11, 14]]
    assert store(tmp_path).snapshot()["ack"]["BTCUSDT"] == 10
    on_raw_message(producer(), cfg, raw)
    state.flush()
    assert store(tmp_path).snapshot()["ack"]["BTCUSDT"] == 14
    assert (tmp_path / "trades.jsonl").exists()


def test_checkpoint_failure_prevents_publishing_later_trade(tmp_path, monkeypatch):
    import orjson

    from ingestion import binance_consumer as consumer

    state = store(tmp_path)
    state.observe(event(10))
    state.ack(event(10))
    worker = BackfillWorker(state, producer(), "crypto.trades.v2", rest(lambda _: None))
    monkeypatch.setattr(consumer, "_backfill", worker)
    monkeypatch.setattr(state, "_save", MagicMock(side_effect=PermissionError("unwritable")))
    raw = orjson.dumps({"data": {"e": "trade", "s": "BTCUSDT", "t": 14,
                                "p": "100", "q": "1", "T": 1757578861000}}).decode()
    consumer.on_raw_message(worker.producer, {"backfill": worker, "topic": "crypto.trades.v2",
        "metrics_file": str(tmp_path / "metrics.json"), "heartbeat_file": str(tmp_path / "heartbeat")}, raw)
    worker.producer.send.assert_not_called()
    assert worker.stop.is_set()
    assert consumer.snapshot_metrics()["backfill"]["checkpoint_failures_total"] == 1


@pytest.mark.parametrize("limited", [False, True])
def test_worker_enforces_request_spacing_and_rate_cooldown(tmp_path, limited):
    worker = BackfillWorker(store(tmp_path), producer(), "crypto.trades.v2", rest(lambda _: None))
    worker.recover_one = MagicMock(side_effect=RateLimited(120) if limited else None, return_value=1)
    delay = []
    worker.stop.wait = lambda seconds: (delay.append(seconds), worker.stop.set())
    worker._run()
    assert delay == [120 if limited else 1.0]


def test_raw_quantity_and_timestamp_are_preserved():
    api = rest(lambda _: httpx.Response(200, json=[{
        "id": 100, "price": "12345.67", "qty": "0.00001234", "time": 1757578861234}]))
    assert api.fetch("BTCUSDT", 100, 1) == [{
        "symbol": "BTCUSDT", "trade_id": 100, "price": 12345.67,
        "quantity": 0.00001234, "timestamp": 1757578861234}]


@pytest.mark.parametrize("header", ["NaN", "bad"])
def test_invalid_retry_after_does_not_retry_immediately(header):
    api = rest(lambda _: httpx.Response(429, headers={"Retry-After": header}))
    with pytest.raises(RateLimited) as caught:
        api.fetch("BTCUSDT", 10, 1)
    assert caught.value.seconds >= 60
