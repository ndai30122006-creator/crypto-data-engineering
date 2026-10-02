# Triển khai từng hạng mục, commit và push sau nghiệm thu

Yêu cầu ngày 02/10/2026: hoàn tất từng ý còn thiếu trong plan 08 và thêm
PRICE_SPIKE, biến động 5m/15m, backfill Binance. Giữ dữ liệu/volumes/topic cũ.
Mỗi hàng có commit riêng; không ghi hoàn thành dựa trên tests bị skip.

| Hạng mục | Thiết kế và acceptance | Trạng thái / commit |
|---|---|---|
| 1. Rollout v2 | Backup DB, migration 001, build images, 7 services healthy, topic/group v2, Linux graph + Kafka→Pathway→Postgres tests, schedules hoạt động; tự tìm Docker CLI trên Windows | Đạt, commit riêng |
| 2. Recovery | Test restart engine giữ exact OHLCV; trade/candle DLQ qua lỗi thật, replay/checkpoint/version guard và volume còn sau recreate; ghi bằng chứng | Chờ |
| 3. PRICE_SPIKE | Batch theo giờ, mọi window 5 phút đã đóng trong giờ; >=1% tăng/giảm so với close 5 phút trước, threshold env; yêu cầu 6 nến liên tục, unique signal replay; tests boundaries/gaps/partial/multi-symbol | Chờ |
| 4. Price change 1m/5m/15m | SQL view trên candles: tính % từ close N phút trước, NULL nếu thiếu khoảng đầy đủ hoặc giá không hợp lệ; không đóng băng giá trị khi có late trade; migration và test SQL thật | Chờ |
| 5. Binance backfill | REST historicalTrades trả raw trade IDs/payloads; checkpoint chỉ sau Kafka ack; detect gap/restart per symbol, bounded pages/rate retries, durable progress và báo unresolved gap | Chờ |

PRICE_SPIKE là nhãn phân tích dữ liệu, không đặt lệnh. Ngưỡng 1%/5m là
mặc định có thể cấu hình, không là khuyến nghị giao dịch.

Backfill cần xác minh contract Binance trước khi triển khai. Nếu endpoint
chỉ có aggregate trade với quantity gộp và không đủ raw payload để replay
trade v2, dùng lịch sử raw trades có quyền API hoặc contract backfill riêng;
không đẩy aggregate giả thành raw trades để làm sai count/volume. Thiết kế
cuối sẽ được cập nhật sau xác minh API.

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
