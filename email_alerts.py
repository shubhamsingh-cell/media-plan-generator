"""
email_alerts.py -- Email notification system via Resend API.

Sends alert emails for critical errors, circuit breaker trips,
generation failures, and daily digest summaries. All emails are
formatted as clean HTML with contextual styling (red for errors,
neutral for digest summaries).

Gracefully disabled when RESEND_API_KEY is not set -- all public
functions become no-ops that return immediately.

Rate limiting:
    - Max 10 emails/hour (configurable via RESEND_HOURLY_LIMIT)
    - Deduplication with exponential backoff: identical error_type+message
      suppressed with escalating windows: 30 min -> 1 hour -> 2 hours -> 4 hours

Integration points (callers to wire up separately):
    - llm_router.py: call send_circuit_breaker_alert() when a provider's
      circuit breaker opens (_ProviderState.record_failure)
    - app.py: call send_generation_failure_alert() when a media plan
      generation request fails with an unrecoverable error
    - monitoring.py: call send_daily_digest() from a scheduled health
      check or external cron hitting /api/health

Stdlib-only, thread-safe.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

_RESEND_ENDPOINT = "https://api.resend.com/emails"

# Core credentials -- module is entirely disabled when API key is absent
_API_KEY: str = (os.environ.get("RESEND_API_KEY") or "").strip()

# "From" address: Resend requires a verified domain.  The onboarding@resend.dev
# address is provided by Resend for initial testing before domain verification.
_FROM_EMAIL: str = os.environ.get(
    "RESEND_FROM_EMAIL",
    "onboarding@resend.dev",
).strip()

# "To" address: required alongside RESEND_API_KEY for the module to activate.
_TO_EMAIL: str = (os.environ.get("ALERT_EMAIL_TO") or "").strip()

# Rate limiting
_HOURLY_LIMIT: int = int(os.environ.get("RESEND_HOURLY_LIMIT", "10"))

# Deduplication: exponential backoff windows (seconds).
# Each successive duplicate of the same alert type extends the suppression
# window: 30 min -> 1 hour -> 2 hours -> 4 hours (capped).
_DEDUP_BACKOFF_LEVELS: tuple = (1800.0, 3600.0, 7200.0, 14400.0)
_DEDUP_MAX_LEVEL: int = len(_DEDUP_BACKOFF_LEVELS) - 1

# HTTP timeout for the Resend API call (seconds)
_SEND_TIMEOUT: int = 15

# Server version -- imported lazily to avoid circular dependency
_SERVER_VERSION: str = ""


def _get_server_version() -> str:
    """Lazily fetch the server version from monitoring.py."""
    global _SERVER_VERSION
    if not _SERVER_VERSION:
        try:
            from monitoring import VERSION

            _SERVER_VERSION = VERSION
        except Exception:
            _SERVER_VERSION = "unknown"
    return _SERVER_VERSION


# ═══════════════════════════════════════════════════════════════════════════════
# THREAD-SAFE RATE TRACKING & DEDUPLICATION
# ═══════════════════════════════════════════════════════════════════════════════

_lock = threading.Lock()

# Timestamps of emails sent within the current rolling hour window
_send_timestamps: List[float] = []

# Deduplication cache: {dedup_key: (last_sent_timestamp, escalation_level)}
# escalation_level indexes into _DEDUP_BACKOFF_LEVELS for exponential backoff.
_dedup_cache: Dict[str, tuple] = {}

# Runtime stats for observability
_email_stats: Dict[str, Any] = {
    "total_sent": 0,
    "total_failed": 0,
    "total_rate_limited": 0,
    "total_deduplicated": 0,
    "last_sent_time": None,
    "last_sent_subject": None,
    "last_error": None,
    "last_error_time": None,
    "last_error_status": None,
    "emails_by_type": {},
}


def _is_enabled() -> bool:
    """Check whether the email alert system is active.

    Returns True only when both RESEND_API_KEY and ALERT_EMAIL_TO are
    configured.  This is called at the top of every public function so
    the module is a complete no-op in environments without credentials.
    """
    return bool(_API_KEY) and bool(_TO_EMAIL)


def _can_send(dedup_key: str = "") -> bool:
    """Check rate limit and deduplication under the global lock.

    Must be called while NOT holding _lock (this function acquires it).

    Args:
        dedup_key: Optional key for deduplication.  When non-empty, an
            email with the same key is suppressed within an exponentially
            growing backoff window (30m -> 1h -> 2h -> 4h).

    Returns:
        True if the email should be sent, False if rate-limited or
        deduplicated.
    """
    now = time.time()

    with _lock:
        # --- Hourly rate limit ---
        # Prune timestamps older than 1 hour
        cutoff = now - 3600.0
        _send_timestamps[:] = [ts for ts in _send_timestamps if ts > cutoff]

        if len(_send_timestamps) >= _HOURLY_LIMIT:
            logger.info(
                "email_alerts: hourly rate limit reached (%d/%d), skipping",
                len(_send_timestamps),
                _HOURLY_LIMIT,
            )
            _email_stats["total_rate_limited"] += 1
            return False

        # --- Deduplication with exponential backoff ---
        if dedup_key:
            entry = _dedup_cache.get(dedup_key)
            if entry is not None:
                last_sent, level = entry
                window = _DEDUP_BACKOFF_LEVELS[min(level, _DEDUP_MAX_LEVEL)]
                if (now - last_sent) < window:
                    logger.debug(
                        "email_alerts: dedup suppressed (key=%s, age=%.0fs, "
                        "window=%.0fs, level=%d)",
                        dedup_key[:60],
                        now - last_sent,
                        window,
                        level,
                    )
                    _email_stats["total_deduplicated"] += 1
                    return False

        # --- Prune stale dedup entries (older than 2x the max backoff window) ---
        stale_cutoff = now - (_DEDUP_BACKOFF_LEVELS[-1] * 2)
        stale_keys = [k for k, entry in _dedup_cache.items() if entry[0] < stale_cutoff]
        for k in stale_keys:
            del _dedup_cache[k]

        return True


def _record_send(dedup_key: str = "") -> None:
    """Record that an email was successfully sent.

    Updates the rate-limit timestamp list and dedup cache.
    Escalates the backoff level for the dedup key (30m -> 1h -> 2h -> 4h).
    Must be called after a successful API response.
    """
    now = time.time()
    with _lock:
        _send_timestamps.append(now)
        if dedup_key:
            # Escalate backoff: if key already exists, bump the level
            existing = _dedup_cache.get(dedup_key)
            if existing is not None:
                _, prev_level = existing
                new_level = min(prev_level + 1, _DEDUP_MAX_LEVEL)
            else:
                new_level = 0
            _dedup_cache[dedup_key] = (now, new_level)


# ═══════════════════════════════════════════════════════════════════════════════
# CORE EMAIL SENDER
# ═══════════════════════════════════════════════════════════════════════════════


def _send_email(to: str, subject: str, html: str) -> bool:
    """Send a single email via the Resend API.

    Args:
        to: Recipient email address.
        subject: Email subject line.
        html: HTML body content.

    Returns:
        True if the API accepted the email, False on any error.
        Never raises exceptions -- errors are logged as warnings.
    """
    payload = {
        "from": _FROM_EMAIL,
        "to": [to],
        "subject": subject,
        "html": html,
    }

    body = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        _RESEND_ENDPOINT,
        data=body,
        method="POST",
    )
    req.add_header("Authorization", f"Bearer {_API_KEY}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", "Nova-AI-Suite/4.0.0")

    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))
            email_id = resp_data.get("id") or ""
            logger.info(
                "email_alerts: sent email (id=%s, subject=%s)",
                email_id,
                subject[:80],
            )
            with _lock:
                _email_stats["total_sent"] += 1
                _email_stats["last_sent_time"] = time.time()
                _email_stats["last_sent_subject"] = subject[:100]
            return True

    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8")[:500]
        except Exception:
            pass
        error_detail = (
            f"HTTP {http_err.code}: {error_body[:200]}"
            if error_body
            else f"HTTP {http_err.code}"
        )
        logger.warning(
            "email_alerts: Resend API %s",
            error_detail,
        )
        with _lock:
            _email_stats["total_failed"] += 1
            _email_stats["last_error"] = error_detail
            _email_stats["last_error_time"] = time.time()
            _email_stats["last_error_status"] = http_err.code
        return False

    except Exception as exc:
        error_detail = f"{type(exc).__name__}: {exc}"
        logger.warning("email_alerts: failed to send email: %s", error_detail)
        with _lock:
            _email_stats["total_failed"] += 1
            _email_stats["last_error"] = error_detail
            _email_stats["last_error_time"] = time.time()
            _email_stats["last_error_status"] = None
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# HTML TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════


def _build_footer() -> str:
    """Build the standard HTML footer with timestamp and server info."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    version = _get_server_version()
    environment = os.environ.get("RENDER_SERVICE_NAME", "local")

    return (
        '<div style="margin-top:32px; padding-top:16px; border-top:1px solid #e0e0e0;'
        ' font-size:12px; color:#888888; font-family:monospace;">'
        f"Timestamp: {now_utc}<br>"
        f"Server: Media Plan Generator v{version}<br>"
        f"Environment: {environment}<br>"
        f"Host: {os.environ.get('RENDER_INSTANCE_ID', os.environ.get('HOSTNAME', 'unknown'))}"
        "</div>"
    )


def _wrap_html(body_content: str) -> str:
    """Wrap body content in a full HTML email structure."""
    return (
        "<!DOCTYPE html>"
        '<html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1.0">'
        "</head>"
        '<body style="margin:0; padding:0; background-color:#f5f5f5;'
        " font-family:-apple-system, BlinkMacSystemFont, Segoe UI, Roboto,"
        ' Helvetica Neue, Arial, sans-serif;">'
        '<div style="max-width:600px; margin:20px auto; background:#ffffff;'
        ' border-radius:8px; overflow:hidden; box-shadow:0 1px 4px rgba(0,0,0,0.1);">'
        f"{body_content}"
        "</div>"
        "</body></html>"
    )


def _build_error_html(
    title: str,
    error_type: str,
    error_message: str,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Build a severity-themed HTML email body.

    Uses red for critical alerts, amber for warnings, blue for info.
    """
    # Pick colors based on title keyword
    if "Warning" in title:
        banner_bg = "#f57c00"  # amber
        msg_bg = "#fff8e1"
        msg_border = "#f57c00"
        msg_text = "#e65100"
    elif "Info" in title:
        banner_bg = "#1976d2"  # blue
        msg_bg = "#e3f2fd"
        msg_border = "#1976d2"
        msg_text = "#0d47a1"
    else:
        banner_bg = "#d32f2f"  # red (critical)
        msg_bg = "#fff3f3"
        msg_border = "#d32f2f"
        msg_text = "#b71c1c"

    # Header banner
    header = (
        f'<div style="background:{banner_bg}; padding:20px 24px;">'
        f'<h1 style="margin:0; color:#ffffff; font-size:20px; font-weight:600;">'
        f"{_html_escape(title)}</h1>"
        "</div>"
    )

    # Body
    body_parts = [
        '<div style="padding:24px;">',
        f'<p style="margin:0 0 8px; font-size:14px; color:#666666;">Error Type</p>',
        f'<p style="margin:0 0 20px; font-size:16px; color:#333333; font-weight:600;">'
        f"{_html_escape(error_type)}</p>",
        f'<p style="margin:0 0 8px; font-size:14px; color:#666666;">Message</p>',
        f'<div style="background:{msg_bg}; border-left:4px solid {msg_border};'
        f' padding:12px 16px; margin:0 0 20px; border-radius:0 4px 4px 0;">'
        f'<pre style="margin:0; white-space:pre-wrap; word-break:break-word;'
        f' font-size:13px; color:{msg_text}; font-family:monospace;">'
        f"{_html_escape(error_message)}</pre></div>",
    ]

    # Context key-value pairs
    if context:
        body_parts.append(
            '<p style="margin:0 0 8px; font-size:14px; color:#666666;">Context</p>'
        )
        body_parts.append(
            '<table style="width:100%; border-collapse:collapse; margin:0 0 20px;">'
        )
        for key, value in context.items():
            body_parts.append(
                f'<tr style="border-bottom:1px solid #f0f0f0;">'
                f'<td style="padding:8px 12px; font-size:13px; color:#666666;'
                f' font-weight:600; white-space:nowrap; vertical-align:top;">'
                f"{_html_escape(str(key))}</td>"
                f'<td style="padding:8px 12px; font-size:13px; color:#333333;'
                f' word-break:break-word;">'
                f"{_html_escape(str(value))}</td>"
                f"</tr>"
            )
        body_parts.append("</table>")

    body_parts.append(_build_footer())
    body_parts.append("</div>")

    return _wrap_html(header + "".join(body_parts))


def _build_digest_html(stats: Dict[str, Any]) -> str:
    """Build a digest-themed (neutral/blue) HTML email body."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Header banner
    header = (
        '<div style="background:#1565c0; padding:20px 24px;">'
        f'<h1 style="margin:0; color:#ffffff; font-size:20px; font-weight:600;">'
        f"Daily Digest &mdash; {now_utc}</h1>"
        "</div>"
    )

    # Summary table
    rows = []
    # Define the display order and labels for known stat keys
    display_keys = [
        ("plans_generated", "Plans Generated"),
        ("plans_failed", "Plans Failed"),
        ("total_requests", "Total Requests"),
        ("total_errors", "Total Errors"),
        ("error_rate", "Error Rate"),
        ("llm_calls", "LLM Calls"),
        ("llm_failures", "LLM Failures"),
        ("avg_latency_ms", "Avg Latency (ms)"),
        ("active_providers", "Active LLM Providers"),
        ("uptime_hours", "Uptime (hours)"),
    ]

    for key, label in display_keys:
        if key in stats:
            value = stats[key]
            # Format percentages
            if key == "error_rate" and isinstance(value, (int, float)):
                value = f"{value:.1f}%"
            rows.append(
                f'<tr style="border-bottom:1px solid #f0f0f0;">'
                f'<td style="padding:10px 16px; font-size:14px; color:#555555;'
                f' font-weight:600;">{_html_escape(label)}</td>'
                f'<td style="padding:10px 16px; font-size:14px; color:#333333;'
                f' text-align:right; font-weight:500;">'
                f"{_html_escape(str(value))}</td>"
                f"</tr>"
            )

    # Include any additional stats not in the predefined list
    known_keys = {k for k, _ in display_keys}
    for key, value in stats.items():
        if key not in known_keys:
            rows.append(
                f'<tr style="border-bottom:1px solid #f0f0f0;">'
                f'<td style="padding:10px 16px; font-size:14px; color:#555555;'
                f' font-weight:600;">{_html_escape(str(key))}</td>'
                f'<td style="padding:10px 16px; font-size:14px; color:#333333;'
                f' text-align:right; font-weight:500;">'
                f"{_html_escape(str(value))}</td>"
                f"</tr>"
            )

    body = (
        '<div style="padding:24px;">'
        '<table style="width:100%; border-collapse:collapse; margin:0 0 20px;'
        ' border:1px solid #e0e0e0; border-radius:4px;">'
        '<thead><tr style="background:#f5f7fa;">'
        '<th style="padding:12px 16px; text-align:left; font-size:13px;'
        ' color:#666666; text-transform:uppercase; letter-spacing:0.5px;">Metric</th>'
        '<th style="padding:12px 16px; text-align:right; font-size:13px;'
        ' color:#666666; text-transform:uppercase; letter-spacing:0.5px;">Value</th>'
        "</tr></thead>"
        "<tbody>" + "".join(rows) + "</tbody></table>" + _build_footer() + "</div>"
    )

    return _wrap_html(header + body)


def _html_escape(text: str) -> str:
    """Minimal HTML escaping for untrusted strings.

    Escapes &, <, >, ", and ' to prevent XSS in email HTML content.
    Uses manual replacement to avoid importing html module (though it
    is stdlib, keeping the import list minimal for consistency).
    """
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC ALERT FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════


def send_error_alert(
    error_type: str,
    error_message: str,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """Send a critical error alert email.

    Args:
        error_type: Category of the error (e.g. "UnhandledException",
            "APITimeout", "DataCorruption").
        error_message: Human-readable error description or traceback.
        context: Optional dict of additional context (request_id,
            endpoint, user info, etc.).

    This is for urgent, actionable errors that need immediate attention.
    Deduplicated on error_type + error_message to prevent alert storms.
    """
    if not _is_enabled():
        logger.debug("email_alerts: disabled (no RESEND_API_KEY), skipping error alert")
        return

    dedup_key = f"error:{error_type}:{error_message}"

    if not _can_send(dedup_key):
        return

    # Derive email title from severity context (fixes "Critical" title on WARNING alerts)
    severity = "critical"
    if isinstance(context, dict):
        severity = context.get("severity", "critical") or "critical"
    severity_titles = {
        "critical": "Critical Error Alert",
        "warning": "Warning Alert",
        "info": "Info Alert",
    }
    title = severity_titles.get(severity, "Critical Error Alert")
    subject_prefix = "CRITICAL" if severity == "critical" else severity.upper()
    subject = f"[{subject_prefix}] {error_type}"
    html = _build_error_html(
        title=title,
        error_type=error_type,
        error_message=error_message,
        context=context,
    )

    if _send_email(_TO_EMAIL, subject, html):
        _record_send(dedup_key)


def send_circuit_breaker_alert(
    provider_id: str,
    failure_count: int,
) -> None:
    """Send an alert when an LLM provider's circuit breaker opens.

    Args:
        provider_id: The LLM provider identifier (e.g. "gemini", "groq").
        failure_count: Number of consecutive failures that triggered the
            circuit breaker.

    Deduplicated per provider_id so the same provider tripping multiple
    times within 30 minutes sends only one email.
    """
    if not _is_enabled():
        logger.debug("email_alerts: disabled, skipping circuit breaker alert")
        return

    dedup_key = f"circuit_breaker:{provider_id}"

    if not _can_send(dedup_key):
        return

    context = {
        "Provider ID": provider_id,
        "Consecutive Failures": failure_count,
        "Status": "Circuit OPEN -- provider temporarily disabled",
    }

    subject = f"[CIRCUIT BREAKER] {provider_id} tripped ({failure_count} failures)"
    html = _build_error_html(
        title="LLM Circuit Breaker Tripped",
        error_type="CircuitBreakerOpen",
        error_message=(
            f"Provider '{provider_id}' has been temporarily disabled after "
            f"{failure_count} consecutive failures. The circuit breaker will "
            f"attempt recovery after the cooldown period."
        ),
        context=context,
    )

    if _send_email(_TO_EMAIL, subject, html):
        _record_send(dedup_key)


def send_generation_failure_alert(
    client_name: str,
    error: str,
    request_id: str = "",
) -> None:
    """Send an alert when media plan generation fails.

    Args:
        client_name: Name of the client whose plan failed to generate.
        error: Error description or traceback.
        request_id: Optional request ID for tracing.

    Deduplicated on the error message so the same underlying issue
    does not flood the inbox.
    """
    if not _is_enabled():
        logger.debug("email_alerts: disabled, skipping generation failure alert")
        return

    dedup_key = f"gen_failure:{error}"

    if not _can_send(dedup_key):
        return

    context: Dict[str, Any] = {
        "Client": client_name,
    }
    if request_id:
        context["Request ID"] = request_id

    subject = f"[GENERATION FAILED] {client_name}"
    html = _build_error_html(
        title="Media Plan Generation Failed",
        error_type="GenerationFailure",
        error_message=error,
        context=context,
    )

    if _send_email(_TO_EMAIL, subject, html):
        _record_send(dedup_key)


def send_daily_digest(stats: Dict[str, Any]) -> None:
    """Send a daily summary digest email.

    Args:
        stats: Dictionary of daily statistics. Expected keys include
            (all optional):
            - plans_generated (int): successful plan count
            - plans_failed (int): failed plan count
            - total_requests (int): total HTTP requests served
            - total_errors (int): total error responses
            - error_rate (float): error percentage
            - llm_calls (int): total LLM API calls
            - llm_failures (int): failed LLM calls
            - avg_latency_ms (float): average response latency
            - active_providers (int): count of healthy LLM providers
            - uptime_hours (float): server uptime

    The digest is NOT deduplicated (it is expected to be called once
    per day), but it still respects the hourly rate limit.
    """
    if not _is_enabled():
        logger.debug("email_alerts: disabled, skipping daily digest")
        return

    # No dedup_key for digest -- allow one per invocation (rate limit still applies)
    if not _can_send():
        return

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subject = f"[DIGEST] Media Plan Generator -- {today}"
    html = _build_digest_html(stats)

    if _send_email(_TO_EMAIL, subject, html):
        _record_send()


def send_custom_alert(subject: str, html_body: str) -> None:
    """Send a generic alert email with custom subject and HTML body.

    Args:
        subject: Email subject line (will be prefixed with [ALERT]).
        html_body: Raw HTML content for the email body. The standard
            footer will NOT be appended -- the caller is responsible
            for the full body content.

    This is a low-level escape hatch for one-off notifications that
    do not fit the other alert categories. Rate limiting and the
    enabled check still apply, but there is no deduplication.
    """
    if not _is_enabled():
        logger.debug("email_alerts: disabled, skipping custom alert")
        return

    if not _can_send():
        return

    prefixed_subject = f"[ALERT] {subject}"

    if _send_email(_TO_EMAIL, prefixed_subject, html_body):
        _record_send()


# ═══════════════════════════════════════════════════════════════════════════════
# STATUS & DIAGNOSTICS
# ═══════════════════════════════════════════════════════════════════════════════


def get_alert_status() -> Dict[str, Any]:
    """Return the current status of the email alert system.

    Useful for health check endpoints and debugging.

    Returns:
        Dict with enabled status, rate limit info, and dedup cache size.
    """
    now = time.time()
    with _lock:
        # Prune old timestamps for accurate count
        cutoff = now - 3600.0
        active_timestamps = [ts for ts in _send_timestamps if ts > cutoff]
        dedup_count = len(_dedup_cache)
        stats_snapshot = dict(_email_stats)

    return {
        "enabled": _is_enabled(),
        "api_key_set": bool(_API_KEY),
        "to_email_set": bool(_TO_EMAIL),
        "from_email": _FROM_EMAIL,
        "hourly_limit": _HOURLY_LIMIT,
        "emails_sent_this_hour": len(active_timestamps),
        "remaining_this_hour": max(0, _HOURLY_LIMIT - len(active_timestamps)),
        "dedup_cache_size": dedup_count,
        "total_sent": stats_snapshot.get("total_sent") or 0,
        "total_failed": stats_snapshot.get("total_failed") or 0,
        "total_rate_limited": stats_snapshot.get("total_rate_limited") or 0,
        "total_deduplicated": stats_snapshot.get("total_deduplicated") or 0,
        "last_sent_time": stats_snapshot.get("last_sent_time"),
        "last_sent_subject": stats_snapshot.get("last_sent_subject"),
        "last_error": stats_snapshot.get("last_error"),
        "last_error_time": stats_snapshot.get("last_error_time"),
        "last_error_status": stats_snapshot.get("last_error_status"),
    }


def diagnose_resend() -> Dict[str, Any]:
    """Run a lightweight diagnostic check on Resend API connectivity.

    Validates configuration and tests the API key by hitting the Resend
    domains endpoint (GET /domains -- lightweight, no side effects).

    Returns a dict with: ok (bool), detail (str), config_warnings (list).
    """
    warnings_list: List[str] = []

    # Check env vars
    if not _API_KEY:
        return {
            "ok": False,
            "detail": "RESEND_API_KEY not set",
            "config_warnings": warnings_list,
        }
    if not _TO_EMAIL:
        return {
            "ok": False,
            "detail": "ALERT_EMAIL_TO not set",
            "config_warnings": warnings_list,
        }

    # Check from_email configuration
    if _FROM_EMAIL == "onboarding@resend.dev":
        warnings_list.append(
            "Using default sender 'onboarding@resend.dev' -- this only works for "
            "the account owner's email. Set RESEND_FROM_EMAIL to a verified domain sender."
        )

    # Test API key validity by hitting GET /domains (read-only, no side effects)
    try:
        req = urllib.request.Request(
            "https://api.resend.com/domains",
            method="GET",
            headers={
                "Authorization": f"Bearer {_API_KEY}",
                "Accept": "application/json",
                "User-Agent": "Nova-AI-Suite/4.0.0",
            },
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))

        # Check if any domains are verified
        domains = resp_data.get("data") or []
        verified = [d for d in domains if d.get("status") == "verified"]
        if not verified and _FROM_EMAIL != "onboarding@resend.dev":
            warnings_list.append(
                f"No verified domains found. Sender '{_FROM_EMAIL}' may not work."
            )
        elif verified:
            verified_names = [d.get("name", "?") for d in verified]
            # Check if from_email domain matches a verified domain
            from_domain = _FROM_EMAIL.split("@")[-1] if "@" in _FROM_EMAIL else ""
            if (
                from_domain
                and from_domain not in verified_names
                and _FROM_EMAIL != "onboarding@resend.dev"
            ):
                warnings_list.append(
                    f"Sender domain '{from_domain}' not in verified domains: {verified_names}"
                )

        detail = f"API key valid, {len(verified)} verified domain(s)"
        if verified:
            detail += f": {', '.join(d.get('name', '?') for d in verified)}"
        return {"ok": True, "detail": detail, "config_warnings": warnings_list}

    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8")[:300]
        except Exception:
            pass
        detail = f"HTTP {http_err.code}"
        if http_err.code == 401:
            detail += " Unauthorized -- RESEND_API_KEY is invalid"
        elif http_err.code == 403:
            detail += " Forbidden -- API key lacks permissions"
        if error_body:
            detail += f" | {error_body[:200]}"
        return {"ok": False, "detail": detail, "config_warnings": warnings_list}

    except Exception as exc:
        return {
            "ok": False,
            "detail": f"{type(exc).__name__}: {exc}",
            "config_warnings": warnings_list,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# MODULE SELF-TEST
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")

    print("=" * 60)
    print("  Email Alerts Status")
    print("=" * 60)

    status = get_alert_status()
    for key, value in status.items():
        print(f"  {key:<28s} {value}")

    if not status["enabled"]:
        print()
        print("  Module is DISABLED.")
        print("  Set RESEND_API_KEY and ALERT_EMAIL_TO to enable.")
        sys.exit(0)

    # If enabled and --test flag passed, send a test email
    if "--test" in sys.argv:
        print()
        print("  Sending test error alert...")
        send_error_alert(
            error_type="TestAlert",
            error_message="This is a test alert from email_alerts.py self-test.",
            context={
                "trigger": "manual --test flag",
                "purpose": "verify Resend API integration",
            },
        )
        print("  Done. Check your inbox.")
    else:
        print()
        print("  Pass --test to send a test alert email.")
