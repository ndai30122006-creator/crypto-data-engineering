# Bật và kiểm tra schedules local

Stack dùng gRPC code location `crypto-data-platform`, repository `__repository__`.
Webserver, code server và daemon phải dùng cùng named volume `dagster_home`.

```powershell
uv run python scripts/start_schedules.py
uv run python scripts/start_schedules.py --check
```

Script gửi startSchedule qua GraphQL của webserver rồi đọc trạng thái hai
schedules trong chính workspace đó; check không mutate và exit lỗi nếu
thiếu schedule hoặc chưa RUNNING. Có thể đổi URL qua `--url`.
UI Automation vẫn dùng được để bật/tắt thủ công.

Nghiệm thu 02/10/2026: CLI báo started, storage có RUNNING nhưng CLI list
trong daemon vẫn STOPPED; chưa kết luận nguyên nhân nội bộ của khác biệt.
Chuyển sang webserver workspace đã cho cả hai RUNNING; kiểm tra lại sau
restart webserver/daemon vẫn RUNNING. Daemon đã launch news schedule tick
12:25 UTC, run `9e51cd5e-ad45-4946-95ac-e2b0b94bfda4`.
Không dùng thông báo CLI started làm bằng chứng schedules hoạt động.
Đối chiếu trạng thái webserver và tick/launch thật từ daemon.
