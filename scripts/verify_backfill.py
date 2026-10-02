"""Opt-in live Binance outage/recreate drill; keeps checkpoints and evidence.

Run: python -m scripts.verify_backfill --allow-restarts
Stops the local consumer for 10 seconds. Does not erase any volumes/state.
"""

import argparse
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import orjson

from scripts.docker_cli import docker_executable


def compose(*args):
    subprocess.run([docker_executable(), "compose", *args], check=True)


def read(path):
    result = subprocess.run([docker_executable(), "exec", "crypto-binance-consumer",
                             "cat", path], capture_output=True, check=True)
    return orjson.loads(result.stdout)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allow-restarts", action="store_true", required=True)
    parser.parse_args()
    before = read("/var/lib/crypto/ingestion-state/gaps.json")
    assert len(before["ack"]) == 5 and not any(before["gaps"].values()), "need five initialized symbols"
    try:
        compose("stop", "binance-consumer")
        time.sleep(10)
    finally:
        compose("up", "-d", "--force-recreate", "--no-deps", "binance-consumer")
    deadline = time.monotonic() + 180
    last = None
    while time.monotonic() < deadline:
        try:
            last = read("/tmp/binance-metrics.json")
        except subprocess.CalledProcessError:
            time.sleep(1)
            continue
        recovery = last.get("backfill", {})
        if recovery.get("recovered_total", 0) > 0 and recovery.get("pending_trades") == 0:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"unresolved backfill after timeout: {last}")
    after = read("/var/lib/crypto/ingestion-state/gaps.json")
    assert all(after["ack"][symbol] > value for symbol, value in before["ack"].items())
    assert not any(after["gaps"].values())
    assert last["backfill"]["failures_total"] == 0
    assert last["backfill"]["checkpoint_failures_total"] == 0
    folder = Path(".recovery") / ("backfill-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))
    folder.mkdir(parents=True)
    (folder / "results.json").write_bytes(orjson.dumps({"before": before, "after": after, "metrics": last}))
    print(orjson.dumps({"evidence": str(folder), "recovered": last["backfill"]["recovered_total"],
                       "pending": 0, "checkpoint_survived_recreate": True}).decode())


if __name__ == "__main__":
    main()
