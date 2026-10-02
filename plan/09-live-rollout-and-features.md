# Triển khai từng hạng mục, commit và push sau nghiệm thu

Yêu cầu ngày 02/10/2026: hoàn tất từng ý còn thiếu trong plan 08 và thêm
PRICE_SPIKE, biến động 5m/15m, backfill Binance. Giữ dữ liệu/volumes/topic cũ.
Mỗi hàng có commit riêng; không ghi hoàn thành dựa trên tests bị skip.

| Hạng mục | Thiết kế và acceptance | Trạng thái / commit |
|---|---|---|
| 1. Rollout v2 | Backup DB, migration 001, build images, 7 services healthy, topic/group v2, Linux graph + Kafka→Pathway→Postgres tests, schedules hoạt động; tự tìm Docker CLI trên Windows | Đạt, commit riêng |
| 2. Recovery | Test restart engine giữ exact OHLCV; trade/candle DLQ qua lỗi thật, replay/checkpoint/version guard và volume còn sau recreate; ghi bằng chứng | Đạt, commit riêng |
| 3. PRICE_SPIKE | Batch theo giờ, mọi window 5 phút đã đóng trong giờ; >=1% tăng/giảm so với close 5 phút trước, threshold env; yêu cầu 6 nến liên tục, unique signal replay; tests boundaries/gaps/partial/multi-symbol | Đạt, commit riêng |
| 4. Price change 1m/5m/15m | SQL view trên candles: tính % từ close N phút trước, NULL nếu thiếu khoảng đầy đủ hoặc giá không hợp lệ; không đóng băng giá trị khi có late trade; migration và test SQL thật | Đạt, commit riêng |
| 5. Binance backfill | REST historicalTrades trả raw trade IDs/payloads; checkpoint chỉ sau Kafka ack; detect gap/restart per symbol, bounded pages/rate retries, durable progress và báo unresolved gap | Đạt, commit riêng |

PRICE_SPIKE là nhãn phân tích dữ liệu, không đặt lệnh. Ngưỡng 1%/5m là
mặc định có thể cấu hình, không là khuyến nghị giao dịch.

Backfill dùng historicalTrades raw payload đã xác minh public HTTP 200;
không đẩy aggregate giả thành raw trades. Giữ gap nếu endpoint bị từ chối,
IDs/payload không hợp lệ hoặc delivery/checkpoint thất bại. Coverage chỉ từ
checkpoint ack đầu tiên; không tự sửa lịch sử v1 trước đó.

Kiểm tra mỗi commit: Ruff + test liên quan, integration thật khi cần,
`git diff --check`, commit và push `origin/main`, xác nhận remote SHA.
Backup/runtime artifacts nằm trong thư mục ignore; secrets không commit.

## Nhật ký

- Trước rollout: main `97d99d8`; Kafka/daemon stopped; containers tạo
  18/09 vẫn dùng topic/group v1. Docker CLI thực nằm tại
  `%LOCALAPPDATA%/Programs/DockerDesktop/resources/bin/docker.exe`.
- Rollout: backup `.recovery/crypto_db-20261002T113356Z.dump` (514559 bytes),
  migration 001 applied, 5 images built, 7 services healthy. Topic
  `crypto.trades.v2`, group `pathway-ohlcv-1m-v2` có traffic thật.
  Linux graph **3 passed**; E2E/SQL thật **10 passed, 0 skipped** (96.85s).
  Hai schedules đã bật qua workspace gRPC; daemon đang chạy. Status lúc
  11:41 UTC có 4944 Kafka acks, 0 publish failures/lost, lag 40. Alert
  candles_10m=14 còn tồn tại trong giai đoạn warmup, chưa đủ 10 phút.
- Đã xác minh Binance `/api/v3/historicalTrades` trên raw IDs gần nhất:
  HTTP 200 không cần API key, raw `id/price/qty/time`. Contract chính thức:
  https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#old-trade-lookup.
- Recovery drill `.recovery/03C71B98/results.json`: 4 checks true. Restart
  giữ OHLCV `(10,11,10,11,3,2)`; Kafka bị stop gây delivery timeout và DLQ,
  replay nhận ack đưa volume/count lên `(7,3)`, resume không replay lại.
  Connection DB bị terminate và reconnect refused ghi candle DLQ; replay
  SQL lưu volume 5, resume 0. Recreate engine giữ marker trong DLQ volume;
  duplicate + trade mới sau recreate cho OHLCV `(10,13,10,13,8,4)`.
  Version guard SQL thật đã đạt ở suite rollout. Services được restore healthy.
- PRICE_SPIKE: 25 unit/asset tests + 1 test DB thật đọc close và insert signal
  hai lần (lần 2 insert 0) đều đạt. Dagster run
  `9e3438b9-e70d-42dc-9a4c-126b20b27431` materialize detected_signals thành
  công trên DB local (55 candles, 0 spikes; không tạo tín hiệu giả).
  Rollout commit `6b25937`, recovery `c246084`, sửa PATH permissions `0b2e656`.
- PRICE_SPIKE commit `26cb00a`. Migration 002 đã applied trên DB local;
  chạy lại runner báo up to date. Test SQL view thật đạt: 1m/5m/15m đúng %, thiếu
  phút/partial/zero/negative/NaN/misaligned → NULL đúng window; late update
  baseline đổi 5m thành -42.5% tức thì. Test migration live chuyển sang
  opt-in INTEGRATION=1, không tự sửa DB khi chạy offline.
- Price changes commit `2465f12`. Backfill worker + ingestion_state volume
  đã chạy thật. Drill ban đầu giữ gap đúng nhưng timeout do đợi ack từng
  trade (~20/s, linger 50ms); đã đổi enqueue page <=1000 rồi đợi ack theo
  thứ tự, checkpoint chỉ prefix đã ack. Drill sau sửa tại
  `.recovery/backfill-20261002T120919Z/results.json`: **1521 recovered,
  pending_trades/gaps=0, failures/checkpoint_failures=0**, high-water của 5
  symbols tiến lên và volume còn sau recreate.
- Ruff + offline suite cuối **184 passed, 14 skipped** (Windows không có
  Pathway và live tests cần opt-in). 24 tests backfill gồm raw quantity/time,
  startup/restart gaps, failure hooks, partial ack/resume, malformed page,
  bounded pages/fairness/rate spacing, 418/429/Retry-After, 5xx/transport
  retries, checkpoint unwritable/corruption/destination và alert pending.
- Status 12:13 UTC: 7 services healthy, 50 candles/10m, lag 6, 0 lost,
  0 publish/DB/DLQ failures, 0 backfill gap, alerts=[]; dữ liệu BTC thật
  có price_change_15m. Suite integration/migration khi build song song:
  14 passed, 1 lỗi DB connection thoáng qua; ca lỗi chạy lại đạt.
  Nghiệm thu toàn suite sau build/recreate được ghi tiếp bên dưới.
- Nghiệm thu cuối: 5 images build thành công; recreate đủ 7 services healthy.
  Suite `tests/integration tests/test_integration.py tests/test_migrate.py`
  **15 passed, 0 skipped** (99.19s; gồm 12 E2E/SQL + 3 migration tests).
  Graph Linux **3 passed** đã chạy trước (graph không thay đổi).
  Status 12:27 UTC alerts=[], lag 27, backfill pending=0/failures=0, HTTP 200.
  Consumer tự phục hồi thêm **1535 trades** khi rollout cuối recreate.
- Schedules: thêm script GraphQL `scripts/start_schedules.py`, commit
  `f84a6a7`, vì CLI start/list không đồng nhất với webserver/storage.
  Query webserver xác nhận hai RUNNING sau restart webserver/daemon;
  daemon launch news schedule tick 12:25 UTC, run
  `9e51cd5e-ad45-4946-95ac-e2b0b94bfda4`. Chi tiết tại docs/schedules.md.
  Tất cả năm hạng mục đã đạt local; kết quả CI GitHub chưa được xác minh.
