"""Tests for slack_alerter.py -- Slack webhook/bot alerter."""

from __future__ import annotations

import json
import threading
import time
from unittest.mock import MagicMock, patch
from typing import Any, Dict

import pytest

# We need to patch env vars before importing
import os


@pytest.fixture(autouse=True)
def _reset_module_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Reset module-level state before each test."""
    monkeypatch.setenv(
        "SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T00/B00/xxx"
    )
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test-bot-token")
    monkeypatch.setenv("SLACK_ALERT_CHANNEL", "#test-alerts")

    # Force reimport to pick up env vars
    import importlib
    import slack_alerter

    importlib.reload(slack_alerter)

    # Reset internal state
    slack_alerter._send_timestamps.clear()
    slack_alerter._consecutive_failures = 0
    slack_alerter._circuit_open_until = 0.0

    yield


class TestBuildAlertBlocks:
    """Tests for _build_alert_blocks."""

    def test_returns_blocks_and_color(self) -> None:
        from slack_alerter import _build_alert_blocks

        blocks, color = _build_alert_blocks("critical", "Test Title", "Test body")
        assert isinstance(blocks, list)
        assert color == "#FF0000"

    def test_severity_colors(self) -> None:
        from slack_alerter import _build_alert_blocks

        _, color_crit = _build_alert_blocks("critical", "t", "m")
        _, color_high = _build_alert_blocks("high", "t", "m")
        _, color_med = _build_alert_blocks("medium", "t", "m")
        _, color_low = _build_alert_blocks("low", "t", "m")
        _, color_info = _build_alert_blocks("info", "t", "m")
        assert color_crit == "#FF0000"
        assert color_high == "#FF8C00"
        assert color_med == "#FFD700"
        assert color_low == "#36A64F"
        assert color_info == "#2196F3"

    def test_unknown_severity_defaults_to_info(self) -> None:
        from slack_alerter import _build_alert_blocks

        _, color = _build_alert_blocks("unknown_sev", "t", "m")
        assert color == "#2196F3"

    def test_header_block_format(self) -> None:
        from slack_alerter import _build_alert_blocks

        blocks, _ = _build_alert_blocks("high", "Server Down", "Details here")
        header = blocks[0]
        assert header["type"] == "header"
        assert "[HIGH]" in header["text"]["text"]
        assert "Server Down" in header["text"]["text"]

    def test_section_block_has_message(self) -> None:
        from slack_alerter import _build_alert_blocks

        blocks, _ = _build_alert_blocks("info", "t", "My message body")
        section = blocks[1]
        assert section["type"] == "section"
        assert "My message body" in section["text"]["text"]


class TestSendSlackAlert:
    """Tests for send_slack_alert (webhook)."""

    @patch("slack_alerter.urllib.request.urlopen")
    def test_successful_webhook_send(self, mock_urlopen: MagicMock) -> None:
        from slack_alerter import send_slack_alert

        mock_resp = MagicMock()
        mock_resp.read.return_value = b"ok"
        mock_resp.status = 200
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = send_slack_alert("critical", "Test Alert", "Something broke")
        assert result is True
        mock_urlopen.assert_called_once()

    @patch("slack_alerter.urllib.request.urlopen")
    def test_webhook_posts_json(self, mock_urlopen: MagicMock) -> None:
        from slack_alerter import send_slack_alert

        mock_resp = MagicMock()
        mock_resp.read.return_value = b"ok"
        mock_resp.status = 200
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        send_slack_alert("high", "Title", "Body")
        call_args = mock_urlopen.call_args
        req = call_args[0][0]
        payload = json.loads(req.data.decode("utf-8"))
        assert "attachments" in payload
        assert payload["attachments"][0]["color"] == "#FF8C00"

    def test_no_webhook_url_returns_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SLACK_WEBHOOK_URL", "")
        import importlib
        import slack_alerter

        importlib.reload(slack_alerter)
        result = slack_alerter.send_slack_alert("info", "Test", "Body")
        assert result is False

    @patch("slack_alerter.urllib.request.urlopen")
    def test_http_error_returns_false(self, mock_urlopen: MagicMock) -> None:
        from slack_alerter import send_slack_alert
        import urllib.error

        mock_urlopen.side_effect = urllib.error.HTTPError(
            url="https://hooks.slack.com/test",
            code=500,
            msg="Server Error",
            hdrs=None,  # type: ignore[arg-type]
            fp=MagicMock(read=MagicMock(return_value=b"error")),
        )
        result = send_slack_alert("critical", "Test", "Body")
        assert result is False

    @patch("slack_alerter.urllib.request.urlopen")
    def test_url_error_returns_false(self, mock_urlopen: MagicMock) -> None:
        from slack_alerter import send_slack_alert
        import urllib.error

        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")
        result = send_slack_alert("info", "Test", "Body")
        assert result is False


class TestSendSlackMessage:
    """Tests for send_slack_message (bot API)."""

    @patch("slack_alerter.urllib.request.urlopen")
    def test_successful_bot_message(self, mock_urlopen: MagicMock) -> None:
        from slack_alerter import send_slack_message

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps({"ok": True}).encode("utf-8")
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = send_slack_message("#general", "Hello world")
        assert result is True

    @patch("slack_alerter.urllib.request.urlopen")
    def test_bot_api_error_returns_false(self, mock_urlopen: MagicMock) -> None:
        from slack_alerter import send_slack_message

        mock_resp = MagicMock()
        mock_resp.read.return_value = json.dumps(
            {"ok": False, "error": "channel_not_found"}
        ).encode("utf-8")
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_resp

        result = send_slack_message("#nonexistent", "Hello")
        assert result is False

    def test_no_bot_token_returns_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("SLACK_BOT_TOKEN", "")
        import importlib
        import slack_alerter

        importlib.reload(slack_alerter)
        result = slack_alerter.send_slack_message("#general", "Hello")
        assert result is False


class TestRateLimiting:
    """Tests for rate limiting."""

    @patch("slack_alerter.urllib.request.urlopen")
    def test_rate_limit_enforcement(self, mock_urlopen: MagicMock) -> None:
        import slack_alerter

        # Fill up the rate limit
        now = time.time()
        slack_alerter._send_timestamps[:] = [now] * 30  # max is 30/min

        result = slack_alerter.send_slack_alert("info", "Test", "Body")
        assert result is False
        mock_urlopen.assert_not_called()


class TestCircuitBreaker:
    """Tests for circuit breaker."""

    def test_circuit_trips_after_threshold(self) -> None:
        import slack_alerter

        for _ in range(5):
            slack_alerter._record_failure()

        assert slack_alerter._is_circuit_open() is True
        assert slack_alerter._consecutive_failures == 5

    def test_circuit_resets_on_success(self) -> None:
        import slack_alerter

        for _ in range(3):
            slack_alerter._record_failure()

        slack_alerter._record_success()
        assert slack_alerter._consecutive_failures == 0

    def test_circuit_auto_closes_after_cooldown(self) -> None:
        import slack_alerter

        slack_alerter._circuit_open_until = time.time() - 1  # already expired
        assert slack_alerter._is_circuit_open() is False


class TestGetSlackStatus:
    """Tests for get_slack_status."""

    def test_status_returns_dict(self) -> None:
        from slack_alerter import get_slack_status

        status = get_slack_status()
        assert isinstance(status, dict)
        assert "webhook_configured" in status
        assert "bot_configured" in status
        assert "rate_limit" in status
        assert "circuit_breaker" in status

    def test_status_reflects_configuration(self) -> None:
        from slack_alerter import get_slack_status

        status = get_slack_status()
        assert status["webhook_configured"] is True
        assert status["bot_configured"] is True
        assert status["default_channel"] == "#test-alerts"
        assert status["rate_limit"]["max_per_minute"] == 30
