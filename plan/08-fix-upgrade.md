# Kế hoạch sửa lỗi và nâng cấp Crypto Data Platform

Ngày lập: 02/10/2026. Baseline: `main@251163a`; review: `docs/reviews/2026-10-02-code-review.md`.

Mục tiêu là xử lý 11 phát hiện trong review và nâng cấp độ đúng dữ liệu, khả năng phục hồi, giám sát và kiểm thử. Tài liệu này là checklist triển khai; mỗi mục chỉ được ghi hoàn thành theo bằng chứng thật. “Code + offline tests” và “live verified” là hai trạng thái riêng.

## 1. Phạm vi và quyết định thiết kế

- Giữ kiến trúc Python, Dagster, Kafka, Pathway và PostgreSQL hiện có.
- Contract trade mới gồm `symbol`, `trade_id`, `price`, `quantity`, `timestamp`. Trade ID lấy từ field `t` của Binance, bắt buộc là số nguyên không âm; timestamp là epoch milliseconds dương; symbol là chuỗi hợp lệ; price/quantity phải hữu hạn và dương.
- Dùng topic `crypto.trades.v2` và group `pathway-ohlcv-1m-v2` khi cutover để không trộn dữ liệu Kafka cũ thiếu trade ID. Không giả lập trade ID từ giá/timestamp vì có thể gộp các giao dịch hợp lệ. Dữ liệu lịch sử hiện tại giữ nguyên; muốn sửa lịch sử cần nguồn raw trades đầy đủ.
- Dedupe theo `(symbol, trade_id)` trong Pathway trước khi window aggregation; open/close theo `(timestamp, trade_id)`. Spec Python dùng cùng contract. Payload khác nhau nhưng cùng ID là lỗi contract, không cộng thành hai trade.
- Metric publish thành công chỉ tăng khi Future được broker acknowledge. Theo dõi pending và failures riêng, bảo toàn `received = invalid + acknowledged + failed + pending` trong process; failed có thể đã được lưu DLQ và không đồng nghĩa đã mất vĩnh viễn.
- Final publish failure lưu event vào DLQ file bền vững; nếu lưu thất bại phải log/metric rõ ràng. DLQ không thay thế Binance backfill trong thời gian service WebSocket dừng. Không tuyên bố exactly-once hay không mất dữ liệu khi ingest chưa nhận được trade.
- Giữ detector batch theo giờ, nhưng xét mọi cửa sổ 5 phút hoàn tất trong giờ gần nhất và lấy đủ 125 phút lịch sử để có baseline 60 phút. Các cửa sổ có thiếu phút phải bị bỏ qua; chỉ dùng nến hoàn tất.
- Dùng chung `alert.evaluate()` cho alert command và status exit code. Giám sát đủ bảy services; metric source không truy cập được phải thể hiện trạng thái degraded.
- File DLQ mount volume; replay bằng command riêng, giữ nguyên file nguồn và chỉ ghi checkpoint sau khi mục tương ứng được xử lý thành công. Replay nến cũ không được ghi đè bản nến đã cập nhật mới hơn.
- CI quản lý Ruff trong dev dependencies/lockfile, chạy lint và offline tests;
  bắt buộc import/test graph Pathway trên Linux. Job streaming-e2e riêng
  khởi động Kafka/Postgres/Pathway, kiểm tra cả recovery sau invalid input.

## 2. Các giai đoạn triển khai

| Giai đoạn | Công việc | File chính | Tiêu chí nghiệm thu | Trạng thái |
|---|---|---|---|---|
| A | Sửa health, status/alert, metrics theo env và delivery accounting | `scripts/status.py`, `scripts/alert.py`, `scripts/metrics.py`, `ingestion/kafka_producer.py`, `ingestion/binance_consumer.py` | Unhealthy đỏ; stale/lag/failure làm exit=1; prod không query local; Future failure không tăng acknowledged | Code + offline đạt; live pending |
| B | Contract trade ID, finite validation, topic v2, dedupe, open/close | `ingestion/events.py`, `streaming/windows.py`, `streaming/pathway_pipeline.py`, `docker-compose.yml` | Exact duplicate/tie/multi-symbol/invalid; valid event vẫn đi qua sau invalid | Spec + parser offline đạt; graph Linux/E2E pending |
| C | Quarantine schema và detector quét toàn khoảng | `dagster_project/assets/market_assets.py`, `dagster_project/quality/asset_checks.py`, `streaming/signals.py`, `dagster_project/assets/signals_assets.py` | Schema errors được giữ; spike giữa giờ; thiếu phút/partial không tạo false signal | Code + offline đạt; materialize live pending |
| D | Durable DLQ, lỗi ghi rõ, reconnect/replay an toàn | `ingestion/dlq.py`, `streaming/postgres_sink.py`, `scripts/replay_dlq.py`, `docker-compose.yml`, `.env.example` | File durable; lỗi visible; checkpoint resume; version-aware upsert | Offline recovery đạt; DB/version/recreate live pending |
| E | Regression tests, CI, runbook và đồng bộ plans | `tests/`, `.github/workflows/ci.yml`, `pyproject.toml`, `uv.lock`, `README.md`, `docs/roadmap.md`, `docs/observability.md`, `plan/` | Lint/offline/definitions đạt; Linux graph + E2E jobs; runbook cụ thể | Code/docs đạt; CI remote và rollout pending |

Thứ tự: tạo tài liệu → A → B → C → D → E. Có thể sửa tests cùng giai đoạn tương ứng, nhưng không hạ assertion để che lỗi.

## 3. Chi tiết acceptance và test

### A — Delivery và monitoring

1. Producer hỗ trợ callback success/error. Counter acknowledged không đổi trước success; Future failed và synchronous send error đều kết thúc pending một lần.
2. Counters cập nhật thread-safe vì Kafka callbacks có thể chạy ở thread khác; dump snapshot nhất quán. Flush cadence dựa vào enqueue attempts, không dựa acknowledged.
3. Các failure được lưu qua DLQ hoặc phát hiện rõ. Heartbeat không được coi enqueue là bằng chứng broker healthy.
4. Health parsing phân biệt healthy/unhealthy/starting/missing, xử lý Docker command returncode khác 0, đưa dagster-code vào dashboard.
5. Alert có rule publish failure và DLQ failure; status áp dụng cùng rules. Env threshold không hợp lệ và metric không hữu hạn tạo lỗi cấu hình/metric rõ ràng.
6. `db_stats()` chỉ đọc bảng resolve theo environment. Test mock SQL trong local/staging/prod.

### B — Độ đúng OHLCV

1. Test Binance `t`, timestamp và type validation; NaN/Infinity, symbol null, bool ID và ID thiếu đều invalid.
2. Test duplicate chính xác mọi OHLCV field; trade ID giống nhau ở hai symbols không bị gộp.
3. Test hai trades cùng millisecond có ID tăng nhưng arrival đảo; open/close theo ID, không theo giá.
4. Test graph Pathway bằng data static trên Linux để kiểm tra API thực, không mock Pathway hay thay graph bằng aggregate Python.
5. E2E sử dụng symbol/bucket riêng cho mỗi run để state engine của test trước không làm count phình. Republish vẫn giữ exact volume/count.
   Kafka boundary đọc raw bytes với row ID tự sinh (Kafka key là symbol),
   parse/normalize bằng UDF an toàn rồi loại sentinel invalid. Nhờ đó JSON
   sai schema không làm engine fail-fast, trong khi ID conflict vẫn fail rõ.
6. Source topic v2 còn lịch sử cũ ở v1. Cutover phút đang chạy có thể thiếu đầu phút; xác nhận nến từ phút hoàn tất đầu tiên sau cutover. Không đưa nến cutover vào kết luận lịch sử đã sửa.

### C — Quality và signals

1. Output `fetch_market` đổi sang `{valid, errors}`; `validate_market` gộp schema errors với business errors; asset checks đọc phần valid và đếm schema rejects.
2. Test null/sai type/non-object payload cùng coin hợp lệ; errors được lưu đủ và pipeline không mất payload lỗi.
3. Detector quét các endpoints hoàn tất trong lookback 60 phút; mỗi endpoint cần 65 nến cách nhau đúng 60 giây. Dùng now inject để test quyết định thời gian.
4. Test spike ở giữa giờ, baseline thiếu, missing minute, nhiều symbols, nến hiện tại chưa đóng, replay signal không tạo bản trùng nhờ unique key hiện có.

### D — DLQ và replay

1. File format JSONL có type `trade`/`candle`, payload, reason và thời điểm ghi; ghi có flush/fsync. Không chứa DATABASE_URL hoặc password trong metadata.
2. Compose mount DLQ cho consumer và pathway. Env override đường dẫn, metrics có saved/failed counters.
3. Sink retry connection đã đóng bằng connection mới; lỗi cấu hình/schema không retry vô hạn. Khi hết retry, báo rõ DLQ saved hay failed và raise.
4. Nến lưu kèm thời điểm phiên bản cập nhật; upsert chỉ chấp nhận phiên bản mới hơn. Migration áp dụng an toàn cho DB đã có market_1m.
5. Replay hỗ trợ dry-run, trade → Kafka ack, candle → Postgres version-aware upsert; checkpoint riêng, không xóa file nguồn; dừng và giữ checkpoint khi gặp lỗi.
6. Test replay gián đoạn chạy lại, duplicate, DLQ unwritable và candle stale.

### E — CI và vận hành

1. Thêm Ruff vào dev group, cập nhật lockfile bằng uv; lint phạm vi mã ứng dụng/tests.
2. Chạy offline suite trên Windows, Linux graph tests trong CI; chạy E2E khi có Kafka/Postgres/Pathway.
3. Đọc Dagster definitions, kiểm tra jobs/schedules/assets và phản ánh số thực trong README.
4. Plan gốc được giữ làm lịch sử, thêm trạng thái hiện tại và link tài liệu này; không giữ nhãn DONE vô điều kiện cho các mục chưa live verify.
5. Runbook có cutover v1→v2, migrations, bật schedules, chạy status/alerts, replay DLQ, và giới hạn WebSocket outage/backfill.

## 4. Kiểm chứng cuối và rollout

```powershell
uv sync --frozen
uv run ruff check dagster_project ingestion streaming scripts tests
uv run pytest tests/ -q
docker compose config
# Sau khi đặt DATABASE_URL đúng đích và môi trường Docker sẵn sàng:
uv run python scripts/migrate.py
docker compose up --build -d
$env:INTEGRATION="1"
uv run pytest tests/integration/ tests/test_integration.py -q
uv run python scripts/status.py
uv run python scripts/alert.py
```

Chỉ thao tác migrations/replay trên DB thật khi endpoint và environment đã xác định. Với lần triển khai này, hoàn thiện code và test trước; nếu môi trường live thiếu thì ghi rõ check chưa chạy và cung cấp command. Không ghi live verified dựa trên test mock.

Nguồn API dùng để triển khai graph: [Pathway groupby/reduce và dedupe](https://pathway.com/developers/user-guide/data-transformation/groupby-reduce-manual/), [reducers](https://pathway.com/developers/api-docs/reducers), [Kafka connector](https://pathway.com/developers/api-docs/pathway-io/kafka).

## 5. Nhật ký kết quả

02/10/2026: đã lập kế hoạch trước khi sửa code. Baseline review chạy 97 passed,
10 skipped. Đã triển khai A–E trong working tree, chưa commit/push hay rollout.

| Kiểm chứng | Kết quả |
|---|---|
| `uv lock`, `uv sync --frozen` | Đạt; Ruff 0.16.10 đã có trong lock/dev env |
| `ruff check dagster_project ingestion streaming scripts tests` | Đạt |
| `python -m pytest tests/ -q` | **137 passed, 12 skipped** (7.25s) |
| `dagster definitions validate -m dagster_project.definitions` | Đạt; 8 assets, 6 checks, 2 jobs/schedules |
| Graph Pathway trên Linux | Test + CI step đã viết; chưa chạy trên máy Windows |
| Kafka → Pathway → PostgreSQL / stale version SQL | E2E tests + CI job đã viết; chưa chạy live |
| Docker build/config, recreate volume, fault injection | Chưa chạy: chưa có Docker CLI dùng được |
| Migration/cutover/replay trên DB thật | Chưa thực hiện; làm theo `docs/upgrade-runbook.md` |

Các regression offline gồm contract invalid, Future ack/error và synchronous
send failure, writable/unwritable recovery, unhealthy/stale/threshold, SQL
environment, schema quarantine, spike giữa giờ, missing/partial candles,
dead DB connection reconnect, replay ack checkpoint/resume và file integrity.
Graph tests thực không được thay bằng mock; CI bắt buộc import Pathway.
WS outage backfill, PRICE_SPIKE, price change 5m/15m vẫn chưa triển khai.

Môi trường: Docker executable chưa tìm thấy, WSL chỉ có distro docker-desktop;
Docker CLI trong distro này báo không hỗ trợ. Do đó chưa ghi “live verified”.
Ghi chú cập nhật 02/10/2026: phần trên là snapshot trước rollout. Code đã
commit/push `97d99d8`; rollout local mới có **3 graph tests Linux + 10 E2E
tests thật passed**, 7 containers healthy, migration 001 và hai schedules
đã bật. Theo dõi recovery và các features tiếp theo tại
[plan 09](09-live-rollout-and-features.md). CI GitHub chưa được xác minh tại đây.
