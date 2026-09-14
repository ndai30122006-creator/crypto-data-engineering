"""Unit tests cho jlog wrapper (jlogger thật nếu cài, fallback stdlib)."""
import io
import logging
import sys

import pytest

from ingestion.jlog import JLog, get_logger

jlogger = pytest.importorskip("jlogger", reason="cần jlogger để test backend thật")


def _capture():
    """Hứng output jlogger qua set_writer (không đấu với capture của pytest)."""
    buf = io.StringIO()
    jlogger.set_output(buf)
    return buf


def _restore():
    jlogger.set_output(sys.stderr)


def _last_json(buf):
    import orjson

    lines = [ln for ln in buf.getvalue().splitlines() if ln.strip().startswith("{")]
    assert lines, "không bắt được dòng JSON nào"
    return orjson.loads(lines[-1])


def test_jlogger_structured_fields():
    buf = _capture()
    try:
        log = JLog("test", backend="jlogger")
        log.info("tick", symbol="BTCUSDT", price=65000)
        rec = _last_json(buf)
    finally:
        _restore()
    assert rec["message"] == "tick" and rec["symbol"] == "BTCUSDT"
    assert rec["price"] == 65000 and rec["level"] == "info"


def test_jlogger_child_inherits_context():
    buf = _capture()
    try:
        log = JLog("test", backend="jlogger").with_fields(symbol="ETHUSDT")
        log.warning("slow")
        rec = _last_json(buf)
    finally:
        _restore()
    assert rec["symbol"] == "ETHUSDT" and rec["level"] == "warning"


def test_jlogger_error_carries_exception():
    buf = _capture()
    try:
        log = JLog("test", backend="jlogger")
        log.error("delivery failed", exc=RuntimeError("boom"), topic="t")
        rec = _last_json(buf)
    finally:
        _restore()
    assert rec["error"] == "boom" and rec["error_type"] == "RuntimeError"


def test_stdlib_fallback_never_crashes(caplog):
    log = JLog("test", backend="stdlib").with_fields(symbol="BTCUSDT")
    with caplog.at_level(logging.INFO):
        log.info("tick", price=1.5)
        log.error("bad", exc=ValueError("x"))
    assert any("tick" in r.message for r in caplog.records)


def test_get_logger_defaults_to_jlogger_when_available():
    assert get_logger("test")._backend == "jlogger"
