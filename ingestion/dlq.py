"""Durable JSONL recovery records shared by ingestion and streaming."""

import os
import threading
from datetime import UTC, datetime
from pathlib import Path

import orjson

_lock = threading.Lock()


def append_record(path: str, kind: str, payload: dict, reason: str) -> None:
    """Append and fsync before reporting success; propagate filesystem failures."""
    record = {
        "kind": kind,
        "payload": payload,
        "reason": reason,
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    encoded = orjson.dumps(record) + b"\n"
    target = Path(path)
    with _lock:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("ab") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
