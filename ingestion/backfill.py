"""Durable raw-trade gap recovery. One worker/state file per Kafka destination.

Never synthesize quantities/IDs from aggregate trades. Progress advances only
after Kafka acknowledgement; crash before checkpoint safely replays IDs.
"""

import copy
import math
import os
import threading
import time
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
import orjson

from ingestion.events import normalize_trade
from ingestion.kafka_producer import publish


class RateLimited(RuntimeError):
    def __init__(self, seconds):
        super().__init__("Binance rate limited; gap retained")
        self.seconds = seconds


class HistoricalTrades:
    """Bounded HTTP retries; 429/418 cooldown is enforced by the worker."""

    def __init__(self, client=None, wait=time.sleep):
        self.client = client or httpx.Client(base_url="https://api.binance.com", timeout=10)
        self.wait = wait

    def fetch(self, symbol, start, limit):
        limit = min(1000, max(1, limit))
        for attempt in range(3):
            try:
                response = self.client.get("/api/v3/historicalTrades",
                                           params={"symbol": symbol, "fromId": start, "limit": limit})
            except httpx.TransportError:
                if attempt == 2:
                    raise RuntimeError("Binance transport failure; gap retained") from None
                if self.wait(2**attempt):
                    raise RuntimeError("backfill stopped") from None
                continue
            if response.status_code in (418, 429):
                raw = response.headers.get("Retry-After", "60")
                try:
                    seconds = float(raw)
                    if not math.isfinite(seconds):
                        seconds = 60
                    seconds = max(1, seconds)
                    if seconds > 86400:
                        seconds = 86400
                except ValueError:
                    try:
                        seconds = max(1, (parsedate_to_datetime(raw) - datetime.now(UTC)).total_seconds())
                    except (ValueError, TypeError, OverflowError):
                        seconds = 60
                raise RateLimited(seconds)
            if response.status_code >= 500 and attempt < 2:
                if self.wait(2**attempt):
                    raise RuntimeError("backfill stopped")
                continue
            if response.status_code != 200:
                raise RuntimeError(f"Binance HTTP {response.status_code}; gap retained")
            data = response.json()
            if not isinstance(data, list) or not data or len(data) > limit:
                raise ValueError("empty/invalid historical page; gap retained")
            events = []
            for index, row in enumerate(data):
                event = normalize_trade({"symbol": symbol, "trade_id": row.get("id"),
                                         "price": row.get("price"), "quantity": row.get("qty"),
                                         "timestamp": row.get("time")}) if isinstance(row, dict) else None
                if event is None or event["trade_id"] != start + index:
                    raise ValueError("non-contiguous/invalid raw trade page; gap retained")
                events.append(event)
            return events
        raise RuntimeError("Binance retries exhausted")

    def close(self):
        self.client.close()


class GapState:
    def __init__(self, path, destination):
        self.path = Path(path)
        self.lock = threading.RLock()
        self.state = {"version": 1, "destination": destination, "ack": {}, "gaps": {}}
        if self.path.exists():
            state = orjson.loads(self.path.read_bytes())
            if state.get("version") != 1 or state.get("destination") != destination:
                raise ValueError("backfill state destination/version mismatch; use separate state")
            for symbol, ranges in state["gaps"].items():
                for start, end in ranges:
                    if (type(start) is not int or type(end) is not int
                            or not 0 <= start <= end < 2**63):
                        raise ValueError("invalid backfill gap checkpoint")
            for symbol, value in state["ack"].items():
                if type(value) is not int or not 0 <= value < 2**63:
                    raise ValueError("invalid backfill acknowledgement checkpoint")
            self.state = state
        self.seen = dict(self.state["ack"])
        self.last_save = 0.0

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        with temporary.open("wb") as output:
            output.write(orjson.dumps(self.state))
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(self.path)
        # Persist directory entry on Linux; filesystems must support fsync.
        if os.name != "nt":
            descriptor = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        self.last_save = time.monotonic()

    def _add(self, symbol, start, end):
        merged = []
        for left, right in sorted(self.state["gaps"].get(symbol, []) + [[start, end]]):
            if merged and left <= merged[-1][1] + 1:
                merged[-1][1] = max(merged[-1][1], right)
            else:
                merged.append([left, right])
        self.state["gaps"][symbol] = merged

    def observe(self, event):
        symbol, value = event["symbol"], event["trade_id"]
        with self.lock:
            previous = self.seen.get(symbol)
            if previous is not None and value > previous + 1:
                self._add(symbol, previous + 1, value - 1)
                self._save()  # gap durable BEFORE a later live trade can be acked
            self.seen[symbol] = max(value, previous if previous is not None else value)

    def ack(self, event):
        with self.lock:
            symbol = event["symbol"]
            self.state["ack"][symbol] = max(event["trade_id"], self.state["ack"].get(symbol, -1))
            if time.monotonic() - self.last_save >= 1:
                self._save()  # at most 1 fsync/s on the producer callback thread

    def failed(self, event):
        with self.lock:
            self._add(event["symbol"], event["trade_id"], event["trade_id"])
            self._save()

    def complete(self, symbol, start, end):
        with self.lock:
            remaining = []
            for left, right in self.state["gaps"].get(symbol, []):
                if right < start or left > end:
                    remaining.append([left, right])
                else:
                    if left < start:
                        remaining.append([left, start - 1])
                    if right > end:
                        remaining.append([end + 1, right])
            self.state["gaps"][symbol] = remaining
            self._save()

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.state)

    def flush(self):
        with self.lock:
            self._save()


class BackfillWorker:
    def __init__(self, state, producer, topic, rest=None, fatal_callback=None):
        self.state, self.producer, self.topic = state, producer, topic
        self.stop = threading.Event()
        self.rest = rest or HistoricalTrades(wait=self.stop.wait)
        self.fatal_callback = fatal_callback
        self.thread = None
        self.cursor = 0
        self.metrics_lock = threading.Lock()
        self.stats = {"recovered_total": 0, "failures_total": 0,
                      "checkpoint_failures_total": 0, "last_error": None}

    def snapshot(self):
        data = self.state.snapshot()
        ranges = [r for ranges in data["gaps"].values() for r in ranges]
        with self.metrics_lock:
            return {**self.stats, "pending_gaps": len(ranges),
                    "pending_trades": sum(end-start+1 for start, end in ranges),
                    "last_ack": data["ack"], "gaps": data["gaps"]}

    def fatal(self, exc):
        with self.metrics_lock:
            self.stats["checkpoint_failures_total"] += 1
            self.stats["last_error"] = type(exc).__name__
        self.stop.set()
        if self.fatal_callback:
            self.fatal_callback()

    def recover_one(self):
        data = self.state.snapshot()
        symbols = sorted(s for s, gaps in data["gaps"].items() if gaps)
        if not symbols:
            return 0
        symbol = symbols[self.cursor % len(symbols)]
        self.cursor += 1
        start, end = data["gaps"][symbol][0]
        events = self.rest.fetch(symbol, start, min(end-start+1, 1000))
        # Enqueue one bounded page before awaiting acks. Waiting after each send
        # would pay producer linger_ms once per trade and recover only ~20/s.
        futures = []
        for event in events:
            if self.stop.is_set():
                break
            futures.append((event, publish(self.producer, self.topic, event)))
        last = start - 1
        deadline = time.monotonic() + 15
        try:
            for event, future in futures:
                future.get(timeout=max(0, deadline-time.monotonic()))
                last = event["trade_id"]
                with self.metrics_lock:
                    self.stats["recovered_total"] += 1
        finally:
            if last >= start:
                self.state.complete(symbol, start, last)  # acknowledged prefix only
        return last - start + 1

    def _run(self):
        while not self.stop.is_set():
            delay = 1.0  # <=60 pages/min * weight 25 = 1500 weight/min per worker
            try:
                recovered = self.recover_one()
                if recovered:
                    with self.metrics_lock:
                        self.stats["last_error"] = None
            except OSError as exc:
                self.fatal(exc)
                break
            except Exception as exc:
                with self.metrics_lock:
                    self.stats["failures_total"] += 1
                    self.stats["last_error"] = type(exc).__name__
                delay = exc.seconds if isinstance(exc, RateLimited) else 10
            self.stop.wait(delay)

    def start(self):
        self.thread = threading.Thread(target=self._run, name="raw-trade-backfill", daemon=True)
        self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread:
            self.thread.join(timeout=45)  # network phases + bounded Kafka ack wait
            if self.thread.is_alive():
                raise RuntimeError("backfill worker did not stop; checkpoint retained")
        self.state.flush()
        self.rest.close()
