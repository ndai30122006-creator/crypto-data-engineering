# Review plan và code — 02/10/2026

Review bản local trên nhánh `main`, commit `251163a`. Working tree sạch trước review. Đã đọc các plan 01–07, roadmap, ingestion, streaming, Dagster assets/resources/checks, schema, scripts vận hành, cấu hình CI và các test liên quan. Không đối chiếu với remote HEAD trong lần review này.

**Đánh giá:** hướng xây dựng phù hợp dự án học Data Engineering: phân tách batch/streaming, logic thuần/resource, validation và vận hành. Tuy nhiên, chưa nên coi toàn bộ plan là hoàn thành: correctness của OHLCV, delivery accounting và monitoring còn lỗi; một số kết luận “đã verify” trong tài liệu chỉ là bằng chứng của lần chạy trước.

Đã chạy `.venv/Scripts/python.exe -m pytest tests/ -q --tb=short`: **97 passed, 10 skipped**. Đã chạy thêm các probe offline để tái hiện những lỗi ghi bên dưới. Docker CLI không có trong PATH của phiên này; chưa chạy lại Pathway trên Linux, Kafka/Postgres E2E, outage recovery hay kiểm tra UI Dagster. Mô phỏng dùng mocks chỉ xác minh các nhánh logic tương ứng, không thay thế kiểm thử hạ tầng thật.

P1: ưu tiên sửa vì ảnh hưởng tính đúng dữ liệu hoặc che giấu lỗi vận hành. P2: sửa trước khi xác nhận phase liên quan hoàn thành.

1. **[P1] Gửi Kafka thất bại vẫn được đếm là published, alert bỏ sót.**

   Vị trí: [binance_consumer.py:220](C:/crypto-data-engineering/ingestion/binance_consumer.py:220), [metrics.py:64](C:/crypto-data-engineering/scripts/metrics.py:64), [alert.py:31](C:/crypto-data-engineering/scripts/alert.py:31).

   `events_published_total` tăng ngay sau khi enqueue; errback tăng thêm `publish_failures_total` nếu delivery thất bại. Cùng một event được tính vào cả published và failures. Probe một event với Future thất bại cho kết quả `received=1, published=1, failures=1, events_lost=-1`; `evaluate()` trả về `[]` khi các metric khác bình thường. Errback chỉ log và tăng counter, chưa lưu/requeue event thất bại sau khi producer hết retry. Lệnh `flush()` cũng không phải bằng chứng mọi Future đã thành công.

   Hướng sửa: phân biệt attempted/enqueued/pending/acknowledged/failed; tăng acknowledged trong callback thành công; đếm lỗi send đồng bộ; có hành vi retry hoặc lưu bền vững cho lỗi delivery cuối cùng và alert riêng cho failures. Test cần mô phỏng Future thành công, Future thất bại và send ném exception, kiểm tra cả counter lẫn alert.

2. **[P1] Container unhealthy được dashboard báo ok.**

   Vị trí: [status.py:43](C:/crypto-data-engineering/scripts/status.py:43).

   `if "healthy" in status` cũng đúng với chuỗi `unhealthy`. Probe đưa cả sáu container về `Up 10 minutes (unhealthy)` nhưng `container_health()` trả về `ok` cho tất cả. Người vận hành có thể thấy dashboard xanh khi healthcheck đã báo lỗi.

   Hướng sửa: đọc chính xác trạng thái health, ưu tiên Docker inspect với `.State.Health.Status`, hoặc phân biệt chính xác `(healthy)`/`(unhealthy)`/`health: starting`. Bổ sung case unhealthy, starting, exited và thiếu container.

3. **[P1] Open/close sai khi giao dịch cùng timestamp.**

   Vị trí: [pathway_pipeline.py:41](C:/crypto-data-engineering/streaming/pathway_pipeline.py:41), [events.py:36](C:/crypto-data-engineering/ingestion/events.py:36).

   `_oc_key` dùng `timestamp|price` để lấy min/max, trong khi giá không quyết định thứ tự giao dịch. Với cùng timestamp, giao dịch theo thứ tự trade ID có giá 9 rồi 10 cần open=9, close=10; khóa hiện tại chọn open=10, close=9 do so sánh chuỗi `10.00000000` và `9.00000000`. Đây là probe trực tiếp của hàm khóa lấy từ AST, chưa phải chạy engine Linux. Ingestion đang bỏ field trade ID nên engine không có dữ liệu để xử lý tie chính xác.

   Hướng sửa: giữ trade ID và dùng thứ tự số `(timestamp, trade_id)` để chọn bản ghi open/close; lấy giá từ bản ghi được chọn. Bổ sung regression cùng millisecond với trade ID khác nhau và arrival đảo thứ tự.

4. **[P1] Idempotency trong plan chưa bao phủ volume và trade_count.**

   Vị trí: [pathway_pipeline.py:70](C:/crypto-data-engineering/streaming/pathway_pipeline.py:70), [test_streaming_e2e.py:59](C:/crypto-data-engineering/tests/integration/test_streaming_e2e.py:59).

   Engine cộng quantity và count trực tiếp, không loại trùng theo trade ID. Upsert chỉ ngăn tạo hai dòng nến, không ngăn cộng hai lần cùng giao dịch trước khi ghi nến. Test dùng `volume/count >=` và chỉ so sánh OHLC khi publish lại. Spec Python xác nhận một trade quantity=1 cho `(volume,count)=(1,1)`, lặp hai lần cho `(2,2)`; engine dùng cùng phép sum/count nhưng chưa được chạy lại trong review này.

   Tài liệu session đã thừa nhận count phình. Vì vậy đây là giới hạn đã được chấp nhận trong bản demo, nhưng nhãn “idempotency DONE” và dữ liệu OHLCV dùng cho volume spike vẫn cần điều chỉnh. Hướng sửa: dedupe `(symbol, trade_id)` trước aggregation, xác định phạm vi lưu trạng thái dedupe/replay, và assert chính xác toàn bộ OHLCV sau duplicate. Khi chưa sửa, ghi rõ volume/count chưa chính xác trước sự kiện trùng.

5. **[P2] Detector theo giờ bỏ sót spike xảy ra giữa giờ.**

   Vị trí: [signals.py:26](C:/crypto-data-engineering/streaming/signals.py:26), [signals_assets.py:13](C:/crypto-data-engineering/dagster_project/assets/signals_assets.py:13), [definitions.py:50](C:/crypto-data-engineering/dagster_project/definitions.py:50).

   Job chạy mỗi giờ, fetch 70 phút, nhưng detector chỉ xét năm nến cuối. Spike ở phút 20–24 trở về bình thường trước lần chạy phút 60 sẽ không được ghi. Probe phát hiện một signal khi xét tới phút 25, nhưng cùng dữ liệu quét ở phút 60 cho zero signals. Do đó “scan 70 phút” chưa đồng nghĩa kiểm tra mọi spike trong khoảng ấy.

   Hướng sửa: chạy detector thường xuyên theo yêu cầu độ trễ, hoặc giữ batch hourly và duyệt mọi cửa sổ 5 phút chưa xử lý với đủ lịch sử baseline. Cách hourly cần lấy nhiều hơn 70 phút để các cửa sổ đầu giờ có đủ 60 phút baseline. Test spike giữa giờ, khoảng thiếu nến và nến chưa hoàn tất.

6. **[P2] Status exit code không áp dụng các ngưỡng alert.**

   Vị trí: [status.py:99](C:/crypto-data-engineering/scripts/status.py:99), [status.py:119](C:/crypto-data-engineering/scripts/status.py:119).

   Dashboard in freshness, lag và failures nhưng không đánh giá chúng; `main()` không gọi `alert.evaluate()`. Probe với nến cũ 999 phút, chỉ một nến/10 phút, lag=99999 và containers healthy vẫn cho status exit=0, trong khi `alert.evaluate()` có ba cảnh báo. Điều này không khớp mô tả exit code của status.

   Hướng sửa: dùng chung kết quả evaluate cho dashboard và exit code, hoặc công bố rõ status chỉ kiểm tra một tập điều kiện riêng. Test cùng metrics phải cho kết quả lỗi nhất quán giữa hai command nếu giữ cam kết hiện tại.

7. **[P2] Metrics dùng bảng local dù đang chạy prod/staging.**

   Vị trí: [metrics.py:174](C:/crypto-data-engineering/scripts/metrics.py:174).

   `db_stats()` đã resolve bảng theo env, nhưng cuối hàm lại query cứng `crypto_market_snapshot_local` và `data_quality_errors_local`, ghi đè kết quả trước đó. Probe với `DAGSTER_ENVIRONMENT=prod` và DB giả chỉ có bảng prod trả về lỗi thiếu bảng local. Nếu DB có cả hai môi trường, số đếm bị lấy từ local.

   Hướng sửa: bỏ các query lặp hard-code và giữ kết quả từ `t_snap`/`t_err`. Test SQL/count cho local, staging và prod.

8. **[P2] Record CoinGecko sai schema bị loại trước quarantine.**

   Vị trí: [market_assets.py:22](C:/crypto-data-engineering/dagster_project/assets/market_assets.py:22).

   `fetch_market` chỉ log và bỏ record khi `msgspec.convert` lỗi; downstream không nhận được payload lỗi để ghi `data_quality_errors`. Probe đầu vào gồm một coin hợp lệ và một coin `current_price=null` cho `fetched=1, errors=0`. Khi API thay kiểu field, mất record khỏi kho quarantine dù plan yêu cầu bad-record pattern có traceability.

   Hướng sửa: đưa lỗi schema vào cùng luồng errors với lỗi business rules, lưu payload gốc và lý do. Test lỗi schema tới asset load phải được ghi errors, đồng thời coin hợp lệ vẫn được xử lý.

9. **[P2] NaN/Infinity được parser chấp nhận thành trade hợp lệ.**

   Vị trí: [events.py:28](C:/crypto-data-engineering/ingestion/events.py:28).

   So sánh `price <= 0` và `quantity <= 0` không loại các số không hữu hạn. Probe `p="NaN"` hoặc `p="Infinity"` trả về event; `orjson.dumps` biến price thành JSON `null`, không phù hợp `TradeSchema.price: float`. Luồng invalid/DLQ bị bypass và published metric vẫn có thể tăng dù không có nến hợp lệ.

   Hướng sửa: kiểm tra `math.isfinite` cho price/quantity và validate các trường nhận diện/timestamp. Test NaN, ±Infinity và trường bắt buộc null.

10. **[P2] CI gọi Ruff nhưng không quản lý dependency Ruff.**

    Vị trí: [ci.yml:17](C:/crypto-data-engineering/.github/workflows/ci.yml:17), [pyproject.toml:33](C:/crypto-data-engineering/pyproject.toml:33).

    Workflow chạy `uv sync --frozen` rồi `uv run ruff check`, nhưng dev dependencies chỉ có pytest; lockfile không có Ruff và môi trường `.venv` hiện tại cũng không có package này. CI phụ thuộc vào executable ngoài project và không tái lập được trên môi trường sạch. Chưa chạy GitHub Actions trong lần review này.

    Hướng sửa: thêm Ruff vào dev dependencies, cập nhật lockfile, chạy lint với version được quản lý và xử lý các lỗi lint thực tế. Kiểm tra CI trên môi trường sạch sau thay đổi.

11. **[P2] Lỗi ghi DLQ bị nuốt nhưng log vẫn nói candle đã vào DLQ.**

    Vị trí: [postgres_sink.py:84](C:/crypto-data-engineering/streaming/postgres_sink.py:84), [pathway_pipeline.py:115](C:/crypto-data-engineering/streaming/pathway_pipeline.py:115).

    `except OSError: pass` che lỗi ghi file. Probe DB down cộng DLQ PermissionError chỉ trả `sink failed ... DB down`; không có thông tin DLQ unwritable. Caller lại log `candle in DLQ`, nên người vận hành có thể tin có bản phục hồi khi không ghi được bản nào. Ngoài ra đường dẫn mặc định `/tmp/pathway-dlq.jsonl` chưa được mount volume; recreate container không giữ file này.

    Hướng sửa: báo rõ kết quả lưu DLQ, không log thành công nếu chưa lưu; dùng thư mục được mount bền vững và bổ sung đường replay. Test đồng thời DB lỗi và DLQ không ghi được.

Đối chiếu plan:

| Plan | Đánh giá hiện tại | Điều kiện để xác nhận hoàn thành |
|---|---|---|
| 01 — News | Luồng chính đã có, unit tests đạt | Xác nhận schedule đang bật và materialization hiện tại; sửa đường dẫn `collector.py` cũ thành resource RSS |
| 02 — Market snapshot | Luồng chính đã có; quarantine schema còn thiếu | Record sai schema phải được lưu lỗi; verify schedule và snapshot thực tế |
| 03 — Binance/Kafka | Đã có ingestion/reconnect; delivery accounting còn sai | Phân biệt enqueue/ack/fail, hành vi khi final delivery failure rõ ràng |
| 04 — Pathway/OHLCV | Aggregation đã có; correctness còn thiếu | Cùng timestamp và duplicate đúng toàn bộ OHLCV; xác định scope PRICE_SPIKE và change 5m/15m còn trong plan gốc |
| 05 — Integration | SQL và checks đã có; live acceptance chưa xác minh lại | Fixture tạo biến động thật và news đúng thời gian để query có positive match; bad records qua đủ các tầng |
| 06 — Resources | P1–P4 phần chính đã có; P5 còn mở | Kiểm tra Deployment UI và configuration visibility như plan yêu cầu |
| 07 — Session phases | Có tests/metrics/retry; chưa đủ để coi cả bốn phase DONE | E2E exact OHLCV, tie/duplicate, health/alert chính xác, failure recovery và DLQ replay có bằng chứng |

Một số điều cần sửa trong nội dung plan:

- Phân biệt **code đã viết**, **test offline đạt**, **live đã kiểm tra tại thời điểm cụ thể** và **vận hành liên tục**. Các số 145 nến, 0 correlation match hoặc 0 violations là bằng chứng lịch sử; không xác nhận hiện trạng ngày review.
- `plan/03` gọi service nhận WebSocket là consumer và nói restart tiếp tục theo offset. Service này thực tế là Kafka producer; downtime của nó làm ngừng nhận Binance trades, không có offset Kafka để lấy lại trades chưa từng publish. Test stop/start chứng minh hồi phục kết nối, chưa chứng minh không mất dữ liệu trong khoảng dừng. Cần nêu rõ có chấp nhận gap hay thêm backfill.
- `plan/04` còn PRICE_SPIKE, change 5m/15m; roadmap giới hạn lại scope và triển khai volume spike bằng batch. Cần đánh dấu deferred hoặc đổi acceptance của plan gốc để tránh hiểu rằng mọi mục đã xong.
- Comment “kafka-python không có idempotence” trong tests/roadmap không phù hợp source package 3.0.11 đang cài: package có `enable_idempotence` mặc định true và code xử lý cấu hình tương ứng. Producer idempotence cũng không loại event được application chủ động publish lại; vẫn cần business trade ID nếu yêu cầu dedupe ở tầng dữ liệu.
- README ghi 6 assets và 86 unit tests, trong khi definitions hiện có 8 assets và lần review chạy 97 tests đạt. Dashboard liệt kê 6 containers, compose có 7 services; cần đưa `dagster-code` vào kiểm tra nếu dashboard được mô tả là sức khỏe toàn hệ thống.

Ưu tiên vòng làm việc tiếp theo: sửa delivery accounting và health reporting; giữ trade ID để sửa tie/dedupe OHLCV; sau đó hoàn thiện detector, quarantine, metrics/alerts, DLQ và CI. Cuối cùng chạy lại E2E trên Linux và cập nhật các nhãn DONE với command, ngày kiểm tra và kết quả cụ thể.
