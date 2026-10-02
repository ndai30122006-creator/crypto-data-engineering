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

Nghiệm thu 02/10/2026: CLI schedule start trong code container tạo state
với ManagedGrpcPythonEnvCodeLocationOrigin, trong khi daemon/webserver dùng
gRPC workspace origin khác. CLI báo started nhưng daemon list vẫn STOPPED.
Chuyển sang webserver workspace đã cho cả hai RUNNING; kiểm tra lại sau
restart webserver/daemon vẫn RUNNING. Không dùng thông báo CLI started làm
bằng chứng schedules hoạt động. Daemon heartbeat/health phải xanh riêng.
