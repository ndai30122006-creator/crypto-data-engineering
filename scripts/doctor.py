"""Local doctor: kiểm tra môi trường trước khi chạy pipeline.

Không cần Docker. Dùng để phát hiện thiếu dependency, env hoặc definitions.
"""
import importlib
import os
import sys

REQUIRED_MODULES = ["dagster", "psycopg2", "httpx", "msgspec", "orjson", "kafka", "yaml"]
REQUIRED_ENV_FOR_DAGSTER = ["DATABASE_URL", "DAGSTER_ENVIRONMENT"]


def main() -> int:
    failed = False
    for module in REQUIRED_MODULES:
        try:
            importlib.import_module(module)
            print(f"[OK] module {module}")
        except Exception as exc:  # noqa: BLE001
            failed = True
            print(f"[FAIL] module {module}: {exc}")
    for key in REQUIRED_ENV_FOR_DAGSTER:
        if os.getenv(key):
            print(f"[OK] env {key}")
        else:
            print(f"[WARN] env {key} missing (Dagster CLI local can set)")
    try:
        import dagster_project.definitions  # noqa: F401

        print("[OK] Dagster definitions import")
    except Exception as exc:  # noqa: BLE001
        failed = True
        print(f"[FAIL] Dagster definitions: {exc}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
