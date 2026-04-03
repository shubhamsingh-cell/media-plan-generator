"""slack_alerter.py -- Slack webhook/bot alerter (Tier 3 fallback).

Sends alerts to Slack via Incoming Webhooks or Bot API as a fallback
when Resend and SMTP delivery fail.

Features:
    - Incoming webhook alerts with Block Kit severity formatting
    - Bot token messages with rich blocks
    - Rate limiting: max 30 messages/minute with threading.Lock
    - Circuit breaker: trips after 5 consecutive failures, 30-min cooldown

Configuration (env vars):
    SLACK_ALERTS_WEBHOOK_URL -- Incoming webhook URL for system alerts (separate from plan notifications)
    SLACK_BOT_TOKEN          -- Bot token for Slack API (richer messages)
    SLACK_ALERT_CHANNEL      -- Default channel (optional, defaults to #alerts)

Note: Uses SLACK_ALERTS_WEBHOOK_URL (not SLACK_WEBHOOK_URL) to keep system
alerts separate from plan generation notifications in #nova-media-plans.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# -- Configuration -----------------------------------------------------------

_WEBHOOK_URL: str = os.environ.get("SLACK_ALERTS_WEBHOOK_URL") or ""
_BOT_TOKEN: str = os.environ.get("SLACK_BOT_TOKEN") or ""
_DEFAULT_CHANNEL: str = os.environ.get("SLACK_ALERT_CHANNEL") or "#alerts"

_SLACK_API_BASE = "https://slack.com/api"
_SEND_TIMEOUT = 15

# -- Severity colors ---------------------------------------------------------

SEVERITY_COLORS: Dict[str, str] = {
    "critical": "#FF0000",
    "high": "#FF8C00",
    "medium": "#FFD700",
    "low": "#36A64F",
    "info": "#2196F3",
}

# -- Rate limiting -----------------------------------------------------------

_RATE_LIMIT = 30  # max messages per minute
_RATE_WINDOW = 60.0  # seconds

_lock = threading.Lock()
_send_timestamps: list[float] = []

# -- Circuit breaker ---------------------------------------------------------

_CB_FAILURE_THRESHOLD = 5
_CB_COOLDOWN_SECONDS = 1800.0  # 30 minutes

_consecutive_failures: int = 0
_circuit_open_until: float = 0.0


def _is_rate_limited() -> bool:
    """Check if we are rate-limited. Must be called under _lock."""
    now = time.time()
    cutoff = now - _RATE_WINDOW
    _send_timestamps[:] = [ts for ts in _send_timestamps if ts > cutoff]
    return len(_send_timestamps) >= _RATE_LIMIT


def _record_send() -> None:
    """Record a successful send timestamp. Must be called under _lock."""
    _send_timestamps.append(time.time())


def _is_circuit_open() -> bool:
    """Check if the circuit breaker is tripped."""
    global _circuit_open_until
    if _circuit_open_until <= 0.0:
        return False
    if time.time() >= _circuit_open_until:
        _circuit_open_until = 0.0
        return False
    return True


def _record_failure() -> None:
    """Record a failure and trip circuit breaker if threshold reached."""
    global _consecutive_failures, _circuit_open_until
    with _lock:
        _consecutive_failures += 1
        if _consecutive_failures >= _CB_FAILURE_THRESHOLD:
            _circuit_open_until = time.time() + _CB_COOLDOWN_SECONDS
            logger.warning(
                "slack_alerter: circuit breaker OPEN after %d failures, "
                "cooldown %.0fs",
                _consecutive_failures,
                _CB_COOLDOWN_SECONDS,
            )


def _record_success() -> None:
    """Reset failure counter on success."""
    global _consecutive_failures
    with _lock:
        _consecutive_failures = 0


def _build_alert_blocks(
    severity: str, title: str, message: str
) -> List[Dict[str, Any]]:
    """Build Slack Block Kit blocks for an alert message."""
    color = SEVERITY_COLORS.get(severity.lower(), SEVERITY_COLORS["info"])
    severity_upper = severity.upper()
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

    blocks: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"[{severity_upper}] {title}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": message or "_No details provided._",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*Severity:* {severity_upper} | *Time:* {timestamp} | *Source:* Nova AI Suite",
                }
            ],
        },
        {"type": "divider"},
    ]

    return blocks, color


def send_slack_alert(
    severity: str,
    title: str,
    message: str,
    channel: Optional[str] = None,
) -> bool:
    """Send an alert via Slack incoming webhook.

    Uses the SLACK_WEBHOOK_URL for simple fire-and-forget alerts.
    Falls back gracefully on any error.

    Args:
        severity: One of 'critical', 'high', 'medium', 'low', 'info'.
        title: Alert title.
        message: Alert body text.
        channel: Override channel (only works if webhook supports it).

    Returns:
        True if the webhook accepted the message, False otherwise.
    """
    try:
        return _send_webhook_alert(severity, title, message, channel)
    except Exception as exc:
        logger.error(
            "slack_alerter: unexpected error in send_slack_alert: %s",
            exc,
            exc_info=True,
        )
        _record_failure()
        return False


def _send_webhook_alert(
    severity: str, title: str, message: str, channel: Optional[str]
) -> bool:
    """Inner implementation for webhook alerts."""
    webhook_url = _WEBHOOK_URL
    if not webhook_url:
        logger.debug("slack_alerter: SLACK_WEBHOOK_URL not set, skipping")
        return False

    with _lock:
        if _is_circuit_open():
            logger.debug("slack_alerter: circuit breaker open, skipping")
            return False
        if _is_rate_limited():
            logger.info("slack_alerter: rate limit reached, skipping")
            return False

    blocks, color = _build_alert_blocks(severity, title, message)

    payload: Dict[str, Any] = {
        "attachments": [
            {
                "color": color,
                "blocks": blocks,
            }
        ],
    }
    if channel:
        payload["channel"] = channel

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(webhook_url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if body.strip().lower() == "ok" or resp.status == 200:
                with _lock:
                    _record_send()
                _record_success()
                logger.info(
                    "slack_alerter: webhook alert sent (severity=%s, title=%s)",
                    severity,
                    title[:80],
                )
                return True
            logger.warning("slack_alerter: webhook unexpected response: %s", body[:200])
            _record_failure()
            return False

    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8", errors="replace")[:300]
        except OSError:
            pass
        logger.warning("slack_alerter: webhook HTTP %d: %s", http_err.code, error_body)
        _record_failure()
        return False

    except urllib.error.URLError as url_err:
        logger.warning("slack_alerter: webhook URLError: %s", url_err.reason)
        _record_failure()
        return False

    except (OSError, ValueError) as exc:
        logger.warning("slack_alerter: webhook error: %s", exc)
        _record_failure()
        return False


def send_slack_message(
    channel: str,
    text: str,
    blocks: Optional[List[Dict[str, Any]]] = None,
) -> bool:
    """Send a message via Slack Bot API for richer formatting.

    Uses SLACK_BOT_TOKEN for the chat.postMessage API.

    Args:
        channel: Slack channel ID or name (e.g. '#alerts' or 'C01234').
        text: Fallback text (shown in notifications).
        blocks: Optional Block Kit blocks for rich formatting.

    Returns:
        True if the message was sent successfully, False otherwise.
    """
    try:
        return _send_bot_message(channel, text, blocks)
    except Exception as exc:
        logger.error(
            "slack_alerter: unexpected error in send_slack_message: %s",
            exc,
            exc_info=True,
        )
        _record_failure()
        return False


def _send_bot_message(
    channel: str,
    text: str,
    blocks: Optional[List[Dict[str, Any]]],
) -> bool:
    """Inner implementation for bot API messages."""
    bot_token = _BOT_TOKEN
    if not bot_token:
        logger.debug("slack_alerter: SLACK_BOT_TOKEN not set, skipping")
        return False

    with _lock:
        if _is_circuit_open():
            logger.debug("slack_alerter: circuit breaker open, skipping")
            return False
        if _is_rate_limited():
            logger.info("slack_alerter: rate limit reached, skipping")
            return False

    payload: Dict[str, Any] = {
        "channel": channel or _DEFAULT_CHANNEL,
        "text": text,
    }
    if blocks:
        payload["blocks"] = blocks

    data = json.dumps(payload).encode("utf-8")
    url = f"{_SLACK_API_BASE}/chat.postMessage"
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {bot_token}")
    req.add_header("Content-Type", "application/json; charset=utf-8")

    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")
            resp_data = json.loads(resp_body)
            if resp_data.get("ok"):
                with _lock:
                    _record_send()
                _record_success()
                logger.info(
                    "slack_alerter: bot message sent to %s",
                    channel or _DEFAULT_CHANNEL,
                )
                return True
            error_msg = resp_data.get("error") or "unknown"
            logger.warning("slack_alerter: bot API error: %s", error_msg)
            _record_failure()
            return False

    except urllib.error.HTTPError as http_err:
        logger.warning("slack_alerter: bot API HTTP %d", http_err.code)
        _record_failure()
        return False

    except urllib.error.URLError as url_err:
        logger.warning("slack_alerter: bot API URLError: %s", url_err.reason)
        _record_failure()
        return False

    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.warning("slack_alerter: bot API error: %s", exc)
        _record_failure()
        return False


def get_slack_status() -> Dict[str, Any]:
    """Return health check status for the Slack alerter.

    Returns:
        Dictionary with configuration state, rate limit status,
        and circuit breaker state.
    """
    with _lock:
        now = time.time()
        cutoff = now - _RATE_WINDOW
        recent_sends = len([ts for ts in _send_timestamps if ts > cutoff])
        circuit_open = _is_circuit_open()
        remaining_cooldown = (
            max(0.0, _circuit_open_until - now) if circuit_open else 0.0
        )

    return {
        "webhook_configured": bool(_WEBHOOK_URL),
        "bot_configured": bool(_BOT_TOKEN),
        "default_channel": _DEFAULT_CHANNEL,
        "rate_limit": {
            "max_per_minute": _RATE_LIMIT,
            "current_count": recent_sends,
            "remaining": max(0, _RATE_LIMIT - recent_sends),
        },
        "circuit_breaker": {
            "open": circuit_open,
            "consecutive_failures": _consecutive_failures,
            "failure_threshold": _CB_FAILURE_THRESHOLD,
            "cooldown_remaining_seconds": round(remaining_cooldown, 1),
        },
    }
