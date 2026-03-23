"""alert_manager.py -- Infrastructure alert delivery via Resend API.

Lightweight email alerting for the three infrastructure modules:
    - data_enrichment.py: enrichment source failures after retries
    - data_matrix_monitor.py: check failures unresolved by self-healing
    - auto_qc.py: critical QC test failures

Uses stdlib only (urllib.request). Thread-safe, rate-limited, and
deduplicating. Fails silently so callers are never impacted.

Rate limiting:
    - Max 10 emails per rolling hour
    - Subject-level deduplication: same subject suppressed for 1 hour

Configuration (env vars):
    RESEND_API_KEY  -- Required. No emails sent without it.
    ALERT_EMAIL     -- Recipient. Default: shubhamsingh@joveo.com
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

# -- Configuration ------------------------------------------------------------

_RESEND_ENDPOINT = "https://api.resend.com/emails"
_FROM_EMAIL = "alerts@nova-ai-suite.com"
_API_KEY: str = os.environ.get("RESEND_API_KEY") or ""
_ALERT_EMAIL: str = os.environ.get("ALERT_EMAIL") or "shubhamsingh@joveo.com"

_HOURLY_LIMIT = 10
_DEDUP_WINDOW = 3600.0  # 1 hour in seconds
_SEND_TIMEOUT = 15  # HTTP timeout for Resend API

# -- Thread-safe state --------------------------------------------------------

_lock = threading.Lock()
_send_timestamps: list[float] = []
_dedup_cache: Dict[str, float] = {}  # subject -> last_sent_timestamp

# -- Severity colours for HTML body -------------------------------------------

_SEVERITY_COLORS: Dict[str, str] = {
    "critical": "#dc2626",
    "warning": "#f59e0b",
    "info": "#3b82f6",
}


def send_alert(
    subject: str,
    body: str,
    severity: str = "warning",
) -> bool:
    """Send an infrastructure alert email via the Resend API.

    Silently returns False on any failure -- this function must never
    crash the caller.

    Args:
        subject: Email subject line (used as dedup key).
        body: HTML body content for the alert email.
        severity: One of 'critical', 'warning', 'info'. Controls the
            colour accent in the email template.

    Returns:
        True if the email was accepted by Resend, False otherwise
        (disabled, rate-limited, deduplicated, or API error).
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
    api_key = _API_KEY
    if not api_key:
        logger.debug("alert_manager: RESEND_API_KEY not set, skipping alert")
        return False

    to_email = _ALERT_EMAIL
    if not to_email:
        logger.debug("alert_manager: ALERT_EMAIL not set, skipping alert")
        return False

    # -- Rate limit + dedup check (under lock) --------------------------------
    now = time.time()
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
        dedup_key = (subject or "").strip()
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
        f'<div style="background:#1e1e2e;color:#d4d4d8;padding:16px;'
        f'border-radius:0 0 6px 6px;font-size:13px;line-height:1.5;">'
        f"{body or 'No details provided.'}</div></div>"
    )

    # -- Send via Resend API --------------------------------------------------
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

    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")
            resp_data = json.loads(resp_body)
            email_id = resp_data.get("id") or ""
            logger.info(
                "alert_manager: sent email (id=%s, severity=%s, subject=%s)",
                email_id,
                severity,
                (subject or "")[:80],
            )
            # Record successful send
            with _lock:
                _send_timestamps.append(time.time())
                _dedup_cache[dedup_key] = time.time()
            return True

    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8", errors="replace")[:300]
        except OSError:
            pass
        logger.warning(
            "alert_manager: Resend API HTTP %d: %s",
            http_err.code,
            error_body,
        )
        return False

    except urllib.error.URLError as url_err:
        logger.warning(
            "alert_manager: Resend API URLError: %s",
            url_err.reason,
        )
        return False

    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.warning("alert_manager: failed to send email: %s", exc)
        return False
