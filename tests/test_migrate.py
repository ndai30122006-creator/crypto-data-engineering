"""Unit tests cho migrate runner (mock conn, file thật trong tmp_path)."""
import socket
from unittest.mock import MagicMock

import pytest

from scripts.migrate import apply, pending


def _conn_with(applied):
    conn, cur = MagicMock(), MagicMock()
    conn.__enter__.return_value = conn
    conn.cursor.return_value.__enter__.return_value = cur
    cur.fetchall.return_value = [(v,) for v in applied]
    return conn, cur


def test_pending_skips_applied(tmp_path):
    (tmp_path / "001_a.sql").write_text("SELECT 1;")
    (tmp_path / "002_b.sql").write_text("SELECT 2;")
    (tmp_path / "note.txt").write_text("ignore me")
    conn, _ = _conn_with(applied=["001"])
    assert [p.name for p in pending(conn, tmp_path)] == ["002_b.sql"]


def test_apply_runs_in_order_and_records(tmp_path):
    (tmp_path / "002_b.sql").write_text("SELECT 2;")
    (tmp_path / "001_a.sql").write_text("SELECT 1;")
    conn, cur = _conn_with(applied=[])
    assert apply(conn, [tmp_path / "001_a.sql", tmp_path / "002_b.sql"]) == ["001", "002"]
    statements = [c.args[0] for c in cur.execute.call_args_list]
    assert statements[0] == "SELECT 1;"
    assert any("schema_migrations" in s for s in statements)


def _pg_up() -> bool:
    try:
        socket.create_connection(("localhost", 5432), timeout=2).close()
        return True
    except OSError:
        return False


@pytest.mark.skipif(not _pg_up(), reason="cần Postgres local")
def test_migrate_py_up_to_date_live():
    """Chạy runner thật vào DB local: hiện 0 migration → up to date."""
    import subprocess
    import sys

    proc = subprocess.run(
        [sys.executable, "scripts/migrate.py"],
        capture_output=True, text=True, timeout=30, check=False,
    )
    assert proc.returncode == 0
    assert "up to date" in proc.stdout
