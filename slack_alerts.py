"""slack_alerts.py -- Structured Slack alerting for Nova AI Suite MCP integration.

Provides high-level alert functions for deployment notifications, error alerts,
and health check failures. Built on top of slack_alerter.py for low-level
webhook delivery.

Functions:
    send_alert(channel, message, severity)  -- General alert dispatch
    send_deploy_notification()              -- Deploy success/failure notification
    send_error_alert(error)                 -- Application error alert

Configuration (env vars):
    SLACK_ALERTS_WEBHOOK_URL -- Incoming webhook URL for system alerts (separate from plan notifications)
    SLACK_ALERT_CHANNEL      -- Default channel (optional, defaults to #nova-alerts)
    RENDER_DEPLOY_URL        -- Render deployment URL (optional, for deploy links)

Note: Uses SLACK_ALERTS_WEBHOOK_URL (not SLACK_WEBHOOK_URL) to keep system
alerts separate from plan generation notifications in #nova-media-plans.

Rate limiting: max 10 alerts/minute per severity level to prevent alert storms.
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
_DEFAULT_CHANNEL: str = os.environ.get("SLACK_ALERT_CHANNEL") or "#nova-alerts"
_DEPLOY_URL: str = os.environ.get(
    "RENDER_DEPLOY_URL", "https://media-plan-generator.onrender.com"
)
_SEND_TIMEOUT: int = 10

# -- Severity config ---------------------------------------------------------

SEVERITY_EMOJI: Dict[str, str] = {
    "critical": ":rotating_light:",
    "high": ":warning:",
    "medium": ":large_yellow_circle:",
    "low": ":information_source:",
    "info": ":speech_balloon:",
}

SEVERITY_COLORS: Dict[str, str] = {
    "critical": "#FF0000",
    "high": "#FF8C00",
    "medium": "#FFD700",
    "low": "#36A64F",
    "info": "#2196F3",
}

# -- Rate limiting (10 alerts/minute per severity) ---------------------------

_RATE_LIMIT_PER_SEVERITY: int = 10
_RATE_WINDOW: float = 60.0

_lock = threading.Lock()
_send_timestamps: Dict[str, List[float]] = {}

# -- Circuit breaker ---------------------------------------------------------

_CB_FAILURE_THRESHOLD: int = 5
_CB_COOLDOWN_SECONDS: float = 600.0  # 10 minutes

_consecutive_failures: int = 0
_circuit_open_until: float = 0.0


def _is_available() -> bool:
    """Check if Slack alerting is configured and available."""
    return bool(_WEBHOOK_URL)


def _is_rate_limited(severity: str) -> bool:
    """Check if we have exceeded the per-severity rate limit.

    Must be called under _lock.

    Args:
        severity: Alert severity level.

    Returns:
        True if rate limited, False otherwise.
    """
    now = time.time()
    cutoff = now - _RATE_WINDOW
    key = severity.lower()
    timestamps = _send_timestamps.get(key, [])
    timestamps[:] = [ts for ts in timestamps if ts > cutoff]
    _send_timestamps[key] = timestamps
    return len(timestamps) >= _RATE_LIMIT_PER_SEVERITY


def _record_send(severity: str) -> None:
    """Record a successful send. Must be called under _lock.

    Args:
        severity: Alert severity level.
    """
    key = severity.lower()
    if key not in _send_timestamps:
        _send_timestamps[key] = []
    _send_timestamps[key].append(time.time())


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
    """Record a failure and trip circuit breaker if threshold is reached."""
    global _consecutive_failures, _circuit_open_until
    with _lock:
        _consecutive_failures += 1
        if _consecutive_failures >= _CB_FAILURE_THRESHOLD:
            _circuit_open_until = time.time() + _CB_COOLDOWN_SECONDS
            logger.warning(
                "slack_alerts: circuit breaker OPEN after %d failures, "
                "cooldown %.0fs",
                _consecutive_failures,
                _CB_COOLDOWN_SECONDS,
            )


def _record_success() -> None:
    """Reset failure counter on successful delivery."""
    global _consecutive_failures
    with _lock:
        _consecutive_failures = 0


def _post_webhook(payload: Dict[str, Any]) -> bool:
    """Send a payload to the Slack webhook URL.

    Args:
        payload: JSON-serializable Slack message payload.

    Returns:
        True if accepted by Slack, False otherwise.
    """
    if not _WEBHOOK_URL:
        logger.debug("slack_alerts: SLACK_WEBHOOK_URL not set, skipping")
        return False

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(_WEBHOOK_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if body.strip().lower() == "ok" or resp.status == 200:
                _record_success()
                return True
            logger.warning("slack_alerts: unexpected webhook response: %s", body[:200])
            _record_failure()
            return False
    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8", errors="replace")[:300]
        except OSError:
            pass
        logger.warning("slack_alerts: webhook HTTP %d: %s", http_err.code, error_body)
        _record_failure()
        return False
    except urllib.error.URLError as url_err:
        logger.warning("slack_alerts: webhook URLError: %s", url_err.reason)
        _record_failure()
        return False
    except (OSError, ValueError) as exc:
        logger.warning("slack_alerts: webhook error: %s", exc)
        _record_failure()
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════


def send_alert(
    channel: Optional[str],
    message: str,
    severity: str = "info",
) -> bool:
    """Send a general alert to Slack.

    Builds a Block Kit message with severity formatting, rate limits per
    severity level, and respects the circuit breaker.

    Args:
        channel: Slack channel override (None uses default).
        message: Alert message body (supports mrkdwn).
        severity: One of 'critical', 'high', 'medium', 'low', 'info'.

    Returns:
        True if the alert was sent successfully, False otherwise.
    """
    if not _is_available():
        logger.debug("slack_alerts: not configured, skipping alert")
        return False

    severity_key = severity.lower()

    with _lock:
        if _is_circuit_open():
            logger.debug("slack_alerts: circuit breaker open, skipping")
            return False
        if _is_rate_limited(severity_key):
            logger.info(
                "slack_alerts: rate limit reached for severity=%s, skipping",
                severity_key,
            )
            return False

    emoji = SEVERITY_EMOJI.get(severity_key, SEVERITY_EMOJI["info"])
    color = SEVERITY_COLORS.get(severity_key, SEVERITY_COLORS["info"])
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

    blocks: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} [{severity_key.upper()}] Nova AI Suite Alert",
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
                    "text": f"*Severity:* {severity_key.upper()} | *Time:* {timestamp}",
                }
            ],
        },
        {"type": "divider"},
    ]

    payload: Dict[str, Any] = {
        "attachments": [{"color": color, "blocks": blocks}],
    }
    if channel:
        payload["channel"] = channel

    try:
        success = _post_webhook(payload)
        if success:
            with _lock:
                _record_send(severity_key)
            logger.info(
                "slack_alerts: alert sent (severity=%s, channel=%s)",
                severity_key,
                channel or _DEFAULT_CHANNEL,
            )
        return success
    except Exception as exc:
        logger.error(
            "slack_alerts: unexpected error in send_alert: %s", exc, exc_info=True
        )
        _record_failure()
        return False


def send_deploy_notification(
    success: bool = True,
    version: str = "4.0.0",
    commit: str = "",
    details: str = "",
) -> bool:
    """Send a deployment notification to Slack.

    Args:
        success: Whether the deployment succeeded.
        version: Application version.
        commit: Git commit hash (optional).
        details: Additional deployment details (optional).

    Returns:
        True if the notification was sent, False otherwise.
    """
    if not _is_available():
        return False

    status_emoji = ":white_check_mark:" if success else ":x:"
    status_text = "SUCCESS" if success else "FAILED"
    color = "#36A64F" if success else "#FF0000"
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

    text_parts: List[str] = [
        f"*Deployment {status_text}*",
        f"*Version:* {version}",
    ]
    if commit:
        text_parts.append(f"*Commit:* `{commit[:8]}`")
    text_parts.append(f"*URL:* {_DEPLOY_URL}")
    if details:
        text_parts.append(f"*Details:* {details}")

    blocks: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{status_emoji} Nova AI Suite Deploy -- {status_text}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(text_parts)},
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"*Time:* {timestamp} | *Source:* Render.com",
                }
            ],
        },
        {"type": "divider"},
    ]

    payload: Dict[str, Any] = {
        "attachments": [{"color": color, "blocks": blocks}],
    }

    severity = "info" if success else "critical"
    try:
        result = _post_webhook(payload)
        if result:
            with _lock:
                _record_send(severity)
            logger.info("slack_alerts: deploy notification sent (success=%s)", success)
        return result
    except Exception as exc:
        logger.error("slack_alerts: deploy notification error: %s", exc, exc_info=True)
        _record_failure()
        return False


def send_error_alert(
    error: Exception | str,
    context: str = "",
    endpoint: str = "",
) -> bool:
    """Send an application error alert to Slack.

    Formats the error with traceback context for quick debugging.

    Args:
        error: The exception or error message string.
        context: Additional context about where the error occurred.
        endpoint: The API endpoint that triggered the error (optional).

    Returns:
        True if the alert was sent, False otherwise.
    """
    if not _is_available():
        return False

    error_msg = str(error)
    error_type = type(error).__name__ if isinstance(error, Exception) else "Error"

    text_parts: List[str] = [
        f"*Error Type:* `{error_type}`",
        f"*Message:* ```{error_msg[:500]}```",
    ]
    if endpoint:
        text_parts.append(f"*Endpoint:* `{endpoint}`")
    if context:
        text_parts.append(f"*Context:* {context}")

    message = "\n".join(text_parts)
    return send_alert(channel=None, message=message, severity="high")


def send_health_failure_alert(
    component: str,
    details: str = "",
) -> bool:
    """Send a health check failure alert.

    Args:
        component: The component/service that failed health check.
        details: Additional failure details.

    Returns:
        True if the alert was sent, False otherwise.
    """
    if not _is_available():
        return False

    message = f"*Health Check FAILED*\n*Component:* `{component}`"
    if details:
        message += f"\n*Details:* {details}"
    message += f"\n*Dashboard:* {_DEPLOY_URL}/health-dashboard"

    return send_alert(channel=None, message=message, severity="critical")


def get_status() -> Dict[str, Any]:
    """Return health/diagnostic status for the Slack alerts module.

    Returns:
        Dictionary with configuration state, rate limit status,
        and circuit breaker state.
    """
    with _lock:
        now = time.time()
        cutoff = now - _RATE_WINDOW
        total_recent = sum(
            len([ts for ts in timestamps if ts > cutoff])
            for timestamps in _send_timestamps.values()
        )
        circuit_open = _is_circuit_open()
        remaining_cooldown = (
            max(0.0, _circuit_open_until - now) if circuit_open else 0.0
        )

    return {
        "available": _is_available(),
        "default_channel": _DEFAULT_CHANNEL,
        "rate_limit": {
            "max_per_severity_per_minute": _RATE_LIMIT_PER_SEVERITY,
            "total_recent_sends": total_recent,
        },
        "circuit_breaker": {
            "open": circuit_open,
            "consecutive_failures": _consecutive_failures,
            "failure_threshold": _CB_FAILURE_THRESHOLD,
            "cooldown_remaining_seconds": round(remaining_cooldown, 1),
        },
    }
