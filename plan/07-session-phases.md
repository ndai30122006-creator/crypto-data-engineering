# Session build: Phase 1–4 (streaming E2E → event-time → observability → failure handling)

Chuỗi 4 phase làm trong session này, sau khi batch (Phase 1–2 cũ) và
streaming (Phase 3–4 cũ) đã live. Mỗi phase: mục tiêu → steps → file →
verify. Chi tiết trạng thái tổng xem `docs/roadmap.md`.

---

## Phase 1 — E2E streaming test ✅ DONE

Mục tiêu: chứng minh chuỗi Binance → Kafka → Pathway → `market_1m` →
PostgreSQL hoạt động thật, không chỉ unit test.

Không import pathway trên Windows (không wheel): test publish vào topic
live, engine thật gom nến, test poll DB assert. Symbol `E2E*` + timestamp
2025 để không đè nến live; cleanup xóa sau mỗi test.

| Step | Nội dung | File | Verify |
|---|---|---|---|
| 1.1 | 6 fake trades + expected OHLC (open=100 high=105 low=98 close=103) | `tests/integration/test_streaming_e2e.py` | — |
| 1.2 | Publish thật qua Kafka (cấm gọi `aggregate()` trực tiếp) | idem | grep `aggregate` = 0 trong `tests/integration/` |
| 1.3 | Chờ nến timeout 60s (không treo vô hạn) | idem (`_wait_candle` dùng chung `helpers.py`) | pass 2.59s |
| 1.4 | Assert đủ 8 fields (symbol, window_start, OHLC, volume, count) | idem | pass live |
| 1.5 | Idempotency: publish lại → nến không đổi | idem | pass 17.68s |

Bài học lòi ra khi làm:
- `earliest/latest` của Pathway theo processing-time → open/close sai khi
  burst. Fix: min/max composite key `ts|price` (đúng data-time mọi thứ tự).
- `trade_count == N` không bao giờ khớp khi duplicate vượt qua N → dùng `>=`
  (at-least-once). Assert OHLC chính xác + volume/count `>=`.

Helpers dùng chung: `tests/integration/helpers.py` (env override
`TEST_KAFKA_BOOTSTRAP`/`TEST_DATABASE_URL`, skip khi thiếu infra, cleanup
theo symbol). Chạy: `$env:INTEGRATION="1"; uv run pytest tests/integration/ -q`.

## Phase 2 — Event-time correctness ✅ DONE

Mục tiêu: hiểu và khóa hành vi event-time (không sửa bừa trước khi hiểu).

- 2.1 Out-of-order: arrival `A→B→C`, event-time `C→A→B` → nến đúng `(100, 105, 98, 98)`.
- 2.2 Late event: nến đã có + trade muộn → high/low/volume/count update, open giữ.
- 2.3 Semantics (không code): window theo **event time**; `earliest/latest`
  theo processing time; late → update qua upsert; replay → OHLC giữ, count phình.
- 2.4 Regression `tests/integration/test_event_time.py`: out-of-order, late,
  duplicate, multiple-symbols (symbols riêng, không lẫn nến).

## Phase 3 — Observability (LOG→METRIC→HEALTH→ALERT) ✅ DONE

Không thêm tech (Prometheus/Grafana để sau) — rút từ log + DB sẵn có.

- 3.1 Binance metrics: `events_received/invalid/published/failures/reconnects`
  + flush latency, dump `/tmp/binance-metrics.json` cùng nhịp heartbeat.
  Bất biến: `received == published + invalid + failures` (live đo 1686 == 1686).
- 3.2 Kafka metrics: **lag là số 1** (`consumer-groups --describe` group
  `pathway-ohlcv-1m`), produce rate (delta log-end, null ở lần đo đầu),
  flush latency (proxy publish latency vì send bất đồng bộ).
- 3.3 Pathway metrics: `events_processed`, `windows_created`, processing
  latency, `last_event`, `last_ohlcv` theo symbol (`streaming/metrics.py`
  pure, test offline được).
- 3.4 DB metrics: `insert_success/failure_total`, `rows_inserted_total`,
  `last/max_query_latency_s` (resource + sink), `newest_candle_at` trong script.
- 3.5 `scripts/status.py` thành dashboard: HEALTH (6 containers) + FLOW
  (events/failures/lag) + DATA (rows/freshness) + `--json`. 1 lệnh thấy cả hệ thống.

Files: `scripts/metrics.py`, `scripts/alert.py` (6 rules + exit code),
`scripts/status.py`, `tests/test_monitor.py` + `tests/test_status.py`,
`docs/observability.md` (runbook từng alert).

## Phase 4 — Failure handling ✅ DONE

Production-like: mọi điểm chết phải có behavior rõ ràng, không mất data lặng lẽ.

- 4.1 Kafka DOWN: lỗi flush propagate (không nuốt) → vòng lặp log + retry
  vô hạn, không crash. Test mock `KafkaConnectionError`.
- 4.2 WS disconnect: đóng kết nối cũ, backoff reconnect, resume. Test mock
  2 vòng kết nối.
- 4.3 Invalid (`price: -100`): reject trước Kafka + log warning + counter
  (hướng nâng DLQ topic riêng).
- 4.4 Postgres DOWN: sink retry 3 backoff → ghi `/tmp/pathway-dlq.jsonl`
  (giữ data + lý do) → raise to, fail visible. Env `SINK_RETRIES`, `DLQ_FILE`.
  Test: retry-rồi-thành công, bỏ-cuộc-ghi-DLQ-raise.

Code: `ingestion/binance_consumer.py` (signal đóng WS, backoff reset),
`streaming/postgres_sink.py` (`write_candle_with_retry`), `Dockerfile.*`
(cài git cho uv clone dep), compose `stop_grace_period: 30s` cho flush kịp.
