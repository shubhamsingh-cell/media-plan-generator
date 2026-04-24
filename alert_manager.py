"""alert_manager.py -- Multi-tier infrastructure alert delivery.

Three-tier fallback chain for maximum reliability:
    Tier 1: Resend API (primary email delivery)
    Tier 2: Slack webhook (via slack_alerter module)
    Tier 3: Local log file (/tmp/nova_alerts.log)

Callers:
    - data_enrichment.py: enrichment source failures after retries
    - data_matrix_monitor.py: check failures unresolved by self-healing
    - auto_qc.py: critical QC test failures

Uses stdlib only (urllib.request). Thread-safe, rate-limited,
and deduplicating. Fails silently so callers are never impacted.

Rate limiting:
    - Max 10 alerts per rolling hour (across all tiers)
    - Subject-level deduplication: same subject suppressed for 1 hour

Configuration (env vars):
    RESEND_API_KEY   -- Tier 1: Resend API key
    ALERT_EMAIL      -- Recipient. Default: shubhamsingh@joveo.com
    SLACK_WEBHOOK_URL -- Tier 2: Slack incoming webhook
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict

logger = logging.getLogger(__name__)

# -- Configuration ------------------------------------------------------------

_RESEND_ENDPOINT = "https://api.resend.com/emails"
_FROM_EMAIL = "Nova Alerts <onboarding@resend.dev>"
_API_KEY: str = os.environ.get("RESEND_API_KEY") or ""
_ALERT_EMAIL: str = os.environ.get("ALERT_EMAIL") or "shubhamsingh@joveo.com"

_HOURLY_LIMIT = 10
# S63: Extended 3600s (1h) -> 14400s (4h). Ongoing incidents were re-paging
# every hour for the same subject because dedup expired. 4h matches typical
# incident triage + ack cycle. Real new alerts still break through via new
# subjects; identical subjects hold.
_DEDUP_WINDOW = 14400.0  # 4 hours in seconds
_SEND_TIMEOUT = 15  # HTTP timeout for Resend API

_LOG_FILE = Path("/tmp/nova_alerts.log")

# -- Thread-safe state --------------------------------------------------------

_lock = threading.Lock()
_send_timestamps: list[float] = []
_dedup_cache: Dict[str, float] = {}  # subject -> last_sent_timestamp
_tier_stats: Dict[str, int] = {
    "resend": 0,
    "slack": 0,
    "logfile": 0,
    "failed": 0,
}

# -- Severity colours for HTML body -------------------------------------------

_SEVERITY_COLORS: Dict[str, str] = {
    "critical": "#dc2626",
    "warning": "#f59e0b",
    "info": "#3b82f6",
}

# -- Severity mapping for slack_alerter compatibility -------------------------

_SEVERITY_TO_SLACK: Dict[str, str] = {
    "critical": "critical",
    "warning": "high",
    "info": "info",
}


def send_alert(
    subject: str,
    body: str,
    severity: str = "warning",
) -> bool:
    """Send an infrastructure alert through the 4-tier fallback chain.

    Tries each tier in order until one succeeds. Silently returns False
    on complete failure -- this function must never crash the caller.

    Args:
        subject: Alert subject line (used as dedup key).
        body: HTML body content for the alert.
        severity: One of 'critical', 'warning', 'info'. Controls the
            colour accent in the email template.

    Returns:
        True if the alert was delivered by any tier, False if all tiers
        failed or the alert was rate-limited/deduplicated.
    """
    try:
        return _send_alert_impl(subject, body, severity)
    except Exception as exc:
        logger.error(
            "alert_manager: unexpected error in send_alert: %s",
            exc,
            exc_info=True,
        )
        return False


def _send_alert_impl(subject: str, body: str, severity: str) -> bool:
    """Inner implementation -- separated so the outer wrapper catches everything."""
    to_email = _ALERT_EMAIL
    if not to_email:
        logger.debug("alert_manager: ALERT_EMAIL not set, skipping alert")
        return False

    # -- Rate limit + dedup check (under lock) --------------------------------
    now = time.time()
    dedup_key = (subject or "").strip()

    with _lock:
        # Prune timestamps older than 1 hour
        cutoff = now - 3600.0
        _send_timestamps[:] = [ts for ts in _send_timestamps if ts > cutoff]

        if len(_send_timestamps) >= _HOURLY_LIMIT:
            logger.info(
                "alert_manager: hourly rate limit reached (%d/%d), skipping",
                len(_send_timestamps),
                _HOURLY_LIMIT,
            )
            return False

        # Subject-based dedup: same subject not sent twice within 1 hour
        last_sent = _dedup_cache.get(dedup_key, 0.0)
        if (now - last_sent) < _DEDUP_WINDOW:
            logger.debug(
                "alert_manager: dedup suppressed (subject=%s, age=%.0fs)",
                dedup_key[:60],
                now - last_sent,
            )
            return False

        # Prune stale dedup entries
        stale_cutoff = now - (_DEDUP_WINDOW * 2)
        stale_keys = [k for k, ts in _dedup_cache.items() if ts < stale_cutoff]
        for k in stale_keys:
            del _dedup_cache[k]

    # -- Build HTML body ------------------------------------------------------
    color = _SEVERITY_COLORS.get(severity, _SEVERITY_COLORS["warning"])
    severity_label = (severity or "warning").upper()
    html = (
        f'<div style="font-family:sans-serif;max-width:600px;margin:0 auto;">'
        f'<div style="background:{color};color:#fff;padding:12px 16px;'
        f'border-radius:6px 6px 0 0;font-size:14px;font-weight:600;">'
        f"[{severity_label}] {subject or 'Alert'}</div>"
        f'<div style="background:#1e1e2e;color:#e4e4e7;padding:16px;'
        f'border-radius:0 0 6px 6px;font-size:13px;line-height:1.5;">'
        f"{body or 'No details provided.'}</div></div>"
    )

    # -- Tier 1: Resend API ---------------------------------------------------
    if _try_resend(subject, html, severity_label, to_email, dedup_key):
        return True

    # -- Tier 2: Slack webhook ------------------------------------------------
    if _try_slack(subject, body, severity, dedup_key):
        return True

    # -- Tier 4: Local log file -----------------------------------------------
    _write_to_logfile(subject, body, severity_label)
    return True  # logfile always "succeeds"


def _record_send(tier: str, dedup_key: str) -> None:
    """Record a successful send for rate limiting and stats."""
    with _lock:
        _send_timestamps.append(time.time())
        _dedup_cache[dedup_key] = time.time()
        _tier_stats[tier] = _tier_stats.get(tier, 0) + 1


def _try_resend(
    subject: str, html: str, severity_label: str, to_email: str, dedup_key: str
) -> bool:
    """Tier 1: Send alert via Resend API.

    Args:
        subject: Alert subject.
        html: Pre-formatted HTML body.
        severity_label: Uppercase severity string.
        to_email: Recipient email.
        dedup_key: Dedup cache key.

    Returns:
        True if sent successfully.
    """
    api_key = _API_KEY
    if not api_key:
        logger.debug("alert_manager: Tier 1 (Resend) -- API key not set, skipping")
        return False

    payload = {
        "from": _FROM_EMAIL,
        "to": [to_email],
        "subject": f"[{severity_label}] {subject or 'Infrastructure Alert'}",
        "html": html,
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        _RESEND_ENDPOINT,
        data=data,
        method="POST",
    )
    req.add_header("Authorization", f"Bearer {api_key}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "Nova-AI-Suite/4.0.0")

    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")
            resp_data = json.loads(resp_body)
            email_id = resp_data.get("id") or ""
            logger.info(
                "alert_manager: Tier 1 (Resend) sent (id=%s, severity=%s, subject=%s)",
                email_id,
                severity_label,
                (subject or "")[:80],
            )
            _record_send("resend", dedup_key)
            return True

    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8", errors="replace")[:300]
        except OSError:
            pass
        logger.warning(
            "alert_manager: Tier 1 (Resend) HTTP %d: %s",
            http_err.code,
            error_body,
        )
        return False

    except urllib.error.URLError as url_err:
        logger.warning("alert_manager: Tier 1 (Resend) URLError: %s", url_err.reason)
        return False

    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.warning("alert_manager: Tier 1 (Resend) failed: %s", exc)
        return False


def _try_slack(subject: str, body: str, severity: str, dedup_key: str) -> bool:
    """Tier 3: Send alert via Slack webhook.

    Imports slack_alerter and delegates to send_slack_alert().

    Args:
        subject: Alert subject/title.
        body: Plain text alert body.
        severity: Severity level string.
        dedup_key: Dedup cache key.

    Returns:
        True if sent successfully.
    """
    try:
        from slack_alerter import send_slack_alert
    except ImportError:
        logger.debug("alert_manager: Tier 3 (Slack) -- slack_alerter not importable")
        return False

    slack_severity = _SEVERITY_TO_SLACK.get(severity, "info")

    try:
        result = send_slack_alert(
            severity=slack_severity,
            title=subject or "Infrastructure Alert",
            message=body or "No details provided.",
        )
        if result:
            logger.info(
                "alert_manager: Tier 3 (Slack) sent (severity=%s, subject=%s)",
                severity,
                (subject or "")[:80],
            )
            _record_send("slack", dedup_key)
        return result

    except Exception as exc:
        logger.warning("alert_manager: Tier 3 (Slack) failed: %s", exc)
        return False


def _write_to_logfile(subject: str, body: str, severity_label: str) -> None:
    """Tier 4: Write alert to local log file as last resort.

    Always succeeds (or silently fails). Appends to /tmp/nova_alerts.log.

    Args:
        subject: Alert subject.
        body: Alert body text.
        severity_label: Uppercase severity string.
    """
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
    log_line = (
        f"[{timestamp}] [{severity_label}] {subject or 'Alert'}\n"
        f"  Body: {(body or 'No details.')[:500]}\n"
        f"  Note: All upstream tiers (Resend, Slack) failed.\n\n"
    )

    try:
        with _lock:
            _LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
            with _LOG_FILE.open("a", encoding="utf-8") as f:
                f.write(log_line)
            _tier_stats["logfile"] = _tier_stats.get("logfile", 0) + 1
        logger.info("alert_manager: Tier 4 (logfile) written to %s", _LOG_FILE)
    except OSError as exc:
        logger.error(
            "alert_manager: Tier 4 (logfile) failed to write: %s",
            exc,
            exc_info=True,
        )
        with _lock:
            _tier_stats["failed"] = _tier_stats.get("failed", 0) + 1


def get_alert_stats() -> Dict[str, Any]:
    """Return alert delivery statistics across all tiers.

    Returns:
        Dictionary with per-tier delivery counts and rate limit status.
    """
    with _lock:
        now = time.time()
        cutoff = now - 3600.0
        recent = len([ts for ts in _send_timestamps if ts > cutoff])
        return {
            "tier_stats": dict(_tier_stats),
            "rate_limit": {
                "max_per_hour": _HOURLY_LIMIT,
                "current_count": recent,
                "remaining": max(0, _HOURLY_LIMIT - recent),
            },
            "configuration": {
                "tier_1_resend": bool(_API_KEY),
                "tier_2_slack": bool(os.environ.get("SLACK_WEBHOOK_URL")),
                "tier_3_logfile": str(_LOG_FILE),
            },
        }
