"""Runner migrations SQL có version, idempotent.

Quy ước:
- File đặt ở database/migrations/, tên NN_mo_ta.sql (vd 002_add_index.sql).
- schema.sql là baseline lúc init volume mới; migrations là thay đổi SAU đó.
- Bảng schema_migrations(version PRIMARY KEY, applied_at) ghi file đã chạy.

Chạy: uv run python scripts/migrate.py [--dir database/migrations]
Conn: env DATABASE_URL (default localhost local).
"""
import os
import re
import sys
from pathlib import Path

import psycopg2

MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "database" / "migrations"
FILENAME_RE = re.compile(r"^(\d{3})_.+\.sql$")
DEFAULT_URL = "postgresql://admin:secret@localhost:5432/crypto_db"


def pending(conn, directory: Path) -> list[Path]:
    """Liệt kê file migration chưa applied, theo thứ tự version."""
    with conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations "
            "(version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT NOW());"
        )
        cur.execute("SELECT version FROM schema_migrations;")
        applied = {row[0] for row in cur.fetchall()}
    versioned: list[tuple[str, Path]] = []
    for p in directory.glob("*.sql"):
        m = FILENAME_RE.match(p.name)
        if m and m.group(1) not in applied:
            versioned.append((m.group(1), p))
    return [p for _, p in sorted(versioned)]


def apply(conn, files: list[Path]) -> list[str]:
    """Chạy từng file trong 1 transaction/file, ghi version khi xong."""
    done = []
    for path in files:
        version = FILENAME_RE.match(path.name).group(1)
        sql = path.read_text(encoding="utf-8")
        with conn, conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO schema_migrations (version) VALUES (%s) "
                "ON CONFLICT DO NOTHING;",
                (version,),
            )
        done.append(version)
        print(f"applied {path.name}")
    return done


def main() -> int:
    directory = Path(sys.argv[1]) if len(sys.argv) > 1 else MIGRATIONS_DIR
    url = os.getenv("DATABASE_URL", DEFAULT_URL)
    try:
        conn = psycopg2.connect(url)
    except Exception as exc:  # noqa: BLE001
        print(f"cannot connect DB: {exc}")
        return 1
    try:
        files = pending(conn, directory)
        if not files:
            print("migrations up to date")
            return 0
        apply(conn, files)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
