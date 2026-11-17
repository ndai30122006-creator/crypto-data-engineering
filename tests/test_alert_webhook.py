import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import alert


def test_webhook_not_called_when_no_alerts():
    with patch("alert.urllib.request.urlopen") as mock_open:
        alert._send_webhook([])
    mock_open.assert_not_called()


def test_webhook_called_with_alerts(monkeypatch):
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://example.test/hook")
    with patch("alert.urllib.request.urlopen") as mock_open:
        alert._send_webhook(["ALERT x"])
    mock_open.assert_called_once()
