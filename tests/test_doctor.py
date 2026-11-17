import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import doctor


def test_doctor_returns_zero_with_required_modules(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://x")
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", "local")
    assert doctor.main() == 0
