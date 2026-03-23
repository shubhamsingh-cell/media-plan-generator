"""
Email delivery of completed media plans via Resend API.

Sends branded HTML emails with optional ZIP file attachments containing
the generated media plan deliverables. Uses the same Resend API pattern
as email_alerts.py (stdlib urllib, JSON payload, Bearer auth).

Rate limiting: max 5 emails/hour per IP address, thread-safe.
Graceful fallback when RESEND_API_KEY is not configured.
"""

from __future__ import annotations

import base64
import json
import html
import logging
import os
import re
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
_API_KEY: str = os.environ.get("RESEND_API_KEY") or "".strip()
_FROM_EMAIL: str = os.environ.get(
    "RESEND_FROM_EMAIL",
    "onboarding@resend.dev",
).strip()

_SEND_TIMEOUT: int = 20  # seconds

# Joveo brand colors (email-safe, inline CSS)
_PORT_GORE = "#202058"
_BLUE_VIOLET = "#5A54BD"
_TEXT_DARK = "#1a1a2e"
_TEXT_MUTED = "#666677"
_BG_LIGHT = "#f4f4f9"

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD-SAFE RATE LIMITING
# ═══════════════════════════════════════════════════════════════════════════════

_lock = threading.Lock()

# Per-IP rate tracking: {ip_str: [timestamp_float, ...]}
_rate_tracker: Dict[str, List[float]] = {}
_MAX_SENDS_PER_HOUR = 5


def _check_and_record_rate_limit(ip: str) -> bool:
    """Atomically check rate limit and record the send if allowed.

    Returns True if allowed (and records the send), False if rate-limited.
    Thread-safe -- single lock acquisition prevents TOCTOU race.
    """
    now = time.time()
    cutoff = now - 3600.0

    with _lock:
        timestamps = _rate_tracker.get(ip, [])
        # Prune entries older than 1 hour
        timestamps = [ts for ts in timestamps if ts > cutoff]

        # Clean up empty entries to prevent memory leak from abandoned IPs
        if not timestamps:
            _rate_tracker.pop(ip, None)
            _rate_tracker[ip] = [now]
            return True

        if len(timestamps) >= _MAX_SENDS_PER_HOUR:
            _rate_tracker[ip] = timestamps
            return False

        timestamps.append(now)
        _rate_tracker[ip] = timestamps
        return True


# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

# Basic email format check -- not RFC 5322 compliant but catches common errors
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def _validate_email(email: str) -> bool:
    """Basic email format validation."""
    if not email or not isinstance(email, str):
        return False
    return bool(_EMAIL_RE.match(email.strip()))


# ═══════════════════════════════════════════════════════════════════════════════
# HTML EMAIL TEMPLATE
# ═══════════════════════════════════════════════════════════════════════════════


def _safe(value: Any) -> str:
    """HTML-escape user-provided values."""
    if value is None:
        return ""
    return html.escape(str(value))


def _format_currency(value: Any) -> str:
    """Format a numeric value as currency."""
    try:
        num = float(value)
        if num >= 1_000_000:
            return f"${num / 1_000_000:,.1f}M"
        if num >= 1_000:
            return f"${num:,.0f}"
        return f"${num:,.2f}"
    except (TypeError, ValueError):
        return _safe(value)


def _build_email_html(
    client_name: str,
    plan_summary: Dict[str, Any],
) -> str:
    """Build the HTML email body.

    Uses table-based layout for maximum email client compatibility.
    No backdrop-filter, no flexbox, no grid -- pure tables and inline CSS.

    Parameters
    ----------
    client_name : str
        The client/company name.
    plan_summary : dict
        Summary data with optional keys: industry, budget, num_channels,
        total_clicks, total_applies, total_hires, top_channels (list of str).
    """
    industry = _safe(plan_summary.get("industry") or "")
    budget = _format_currency(plan_summary.get("budget") or 0)
    num_channels = _safe(plan_summary.get("num_channels") or "")
    total_clicks = _safe(plan_summary.get("total_clicks") or "")
    total_applies = _safe(plan_summary.get("total_applies") or "")
    total_hires = _safe(plan_summary.get("total_hires") or "")
    top_channels = plan_summary.get("top_channels") or []

    now_utc = datetime.now(timezone.utc).strftime("%B %d, %Y at %H:%M UTC")

    # Top channels list
    channels_html = ""
    if top_channels:
        items = "".join(
            f'<li style="padding: 4px 0; color: {_TEXT_DARK};">{_safe(ch)}</li>'
            for ch in top_channels[:5]
        )
        channels_html = f"""
        <tr><td style="padding: 16px 0 8px 0; font-weight: 600; color: {_PORT_GORE}; font-size: 14px;">
          Top Channels
        </td></tr>
        <tr><td style="padding: 0 0 16px 0;">
          <ul style="margin: 0; padding-left: 20px; color: {_TEXT_DARK}; font-size: 14px;">{items}</ul>
        </td></tr>
        """

    # Metrics row (table-based, 3 columns)
    metrics_html = ""
    metric_cells = []
    if total_clicks:
        metric_cells.append(("Projected Clicks", total_clicks))
    if total_applies:
        metric_cells.append(("Projected Applies", total_applies))
    if total_hires:
        metric_cells.append(("Projected Hires", total_hires))

    if metric_cells:
        cells = ""
        cell_width = f"{100 // max(len(metric_cells), 1)}%"
        for label, value in metric_cells:
            cells += (
                f'<td style="width: {cell_width}; text-align: center; padding: 16px 8px;'
                f' background: {_BG_LIGHT}; border-radius: 6px;">'
                f'<div style="font-size: 11px; text-transform: uppercase; letter-spacing: 1px;'
                f' color: {_TEXT_MUTED}; margin-bottom: 4px;">{label}</div>'
                f'<div style="font-size: 20px; font-weight: 700; color: {_PORT_GORE};">{value}</div>'
                f"</td>"
            )
        metrics_html = f"""
        <tr><td style="padding: 16px 0;">
          <table width="100%" cellpadding="0" cellspacing="8" style="border-collapse: separate;">
            <tr>{cells}</tr>
          </table>
        </td></tr>
        """

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"></head>
<body style="margin: 0; padding: 0; background-color: #f0f0f5; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color: #f0f0f5; padding: 32px 0;">
    <tr><td align="center">
      <table width="600" cellpadding="0" cellspacing="0" style="background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 2px 12px rgba(0,0,0,0.08);">

        <!-- Header -->
        <tr><td style="background: linear-gradient(135deg, {_PORT_GORE} 0%, {_BLUE_VIOLET} 100%); padding: 32px 40px; text-align: center;">
          <div style="font-size: 22px; font-weight: 700; color: #ffffff; letter-spacing: 1px;">Joveo</div>
          <div style="font-size: 12px; color: rgba(255,255,255,0.7); margin-top: 4px; letter-spacing: 0.5px;">Media Plan Generator</div>
        </td></tr>

        <!-- Body -->
        <tr><td style="padding: 32px 40px;">
          <table width="100%" cellpadding="0" cellspacing="0">

            <!-- Headline -->
            <tr><td style="padding-bottom: 20px;">
              <h1 style="margin: 0; font-size: 22px; font-weight: 700; color: {_PORT_GORE};">
                Your Media Plan is Ready
              </h1>
              <p style="margin: 8px 0 0 0; font-size: 14px; color: {_TEXT_MUTED}; line-height: 1.5;">
                The recruitment advertising media plan for <strong style="color: {_TEXT_DARK};">{_safe(client_name)}</strong> has been generated and is attached to this email.
              </p>
            </td></tr>

            <!-- Summary -->
            <tr><td style="padding: 16px 0; border-top: 1px solid #e8e8f0;">
              <table width="100%" cellpadding="0" cellspacing="0" style="font-size: 14px; color: {_TEXT_DARK};">
                <tr>
                  <td style="padding: 6px 0; font-weight: 600; color: {_TEXT_MUTED}; width: 140px;">Client</td>
                  <td style="padding: 6px 0;">{_safe(client_name)}</td>
                </tr>
                {"<tr><td style='padding: 6px 0; font-weight: 600; color: " + _TEXT_MUTED + "; width: 140px;'>Industry</td><td style='padding: 6px 0;'>" + industry + "</td></tr>" if industry else ""}
                <tr>
                  <td style="padding: 6px 0; font-weight: 600; color: {_TEXT_MUTED}; width: 140px;">Budget</td>
                  <td style="padding: 6px 0; font-weight: 700; color: {_PORT_GORE};">{budget}</td>
                </tr>
                {"<tr><td style='padding: 6px 0; font-weight: 600; color: " + _TEXT_MUTED + "; width: 140px;'>Channels</td><td style='padding: 6px 0;'>" + num_channels + "</td></tr>" if num_channels else ""}
              </table>
            </td></tr>

            <!-- Metrics -->
            {metrics_html}

            <!-- Top Channels -->
            {channels_html}

            <!-- CTA -->
            <tr><td style="padding: 24px 0 0 0; text-align: center;">
              <p style="font-size: 13px; color: {_TEXT_MUTED}; line-height: 1.5;">
                {"The media plan files are attached to this email as a ZIP archive. " if True else ""}
                For questions or adjustments, reply to this email or contact your Joveo representative.
              </p>
            </td></tr>

          </table>
        </td></tr>

        <!-- Footer -->
        <tr><td style="background: {_BG_LIGHT}; padding: 20px 40px; text-align: center; border-top: 1px solid #e0e0e8;">
          <div style="font-size: 12px; color: {_TEXT_MUTED};">
            Powered by <strong style="color: {_BLUE_VIOLET};">Joveo Media Plan Generator</strong>
          </div>
          <div style="font-size: 11px; color: #999; margin-top: 4px;">{now_utc}</div>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════


def send_plan_email(
    recipient_email: str,
    client_name: str,
    plan_summary: Dict[str, Any],
    zip_file_path: Optional[str] = None,
    zip_bytes: Optional[bytes] = None,
    sender_ip: str = "unknown",
) -> Dict[str, Any]:
    """Send a media plan email via Resend API.

    Parameters
    ----------
    recipient_email : str
        The recipient's email address.
    client_name : str
        Client / company name.
    plan_summary : dict
        Summary data for the email body. Keys: industry, budget,
        num_channels, total_clicks, total_applies, total_hires,
        top_channels (list of str).
    zip_file_path : str, optional
        Path to a ZIP file to attach. Mutually exclusive with zip_bytes.
    zip_bytes : bytes, optional
        Raw ZIP file bytes to attach. Mutually exclusive with zip_file_path.
    sender_ip : str
        IP address of the sender for rate limiting.

    Returns
    -------
    dict
        {success: bool, message: str, message_id: str (if success)}
    """
    # ── Validate API key ──
    if not _API_KEY:
        logger.warning(
            "plan_delivery: RESEND_API_KEY not set -- email delivery disabled"
        )
        return {
            "success": False,
            "message": "Email delivery is not configured. RESEND_API_KEY environment variable is required.",
        }

    # ── Validate email ──
    recipient_email = (recipient_email or "").strip()
    if not _validate_email(recipient_email):
        return {
            "success": False,
            "message": "Invalid email address format",
        }

    # ── Rate limit check ──
    if not _check_and_record_rate_limit(sender_ip):
        return {
            "success": False,
            "message": "Rate limit exceeded. Maximum 5 emails per hour. Please try again later.",
        }

    # ── Build email HTML ──
    client_name = (client_name or "").strip() or "Unknown Client"
    email_html = _build_email_html(client_name, plan_summary or {})
    subject = f"Media Plan Ready: {client_name}"

    # ── Build API payload ──
    payload: Dict[str, Any] = {
        "from": _FROM_EMAIL,
        "to": [recipient_email],
        "subject": subject,
        "html": email_html,
    }

    # ── Attach ZIP if provided ──
    attachment_bytes: Optional[bytes] = None
    if zip_bytes:
        attachment_bytes = zip_bytes
    elif zip_file_path:
        try:
            with open(zip_file_path, "rb") as f:
                attachment_bytes = f.read()
        except (OSError, IOError) as e:
            logger.warning(
                "plan_delivery: failed to read ZIP file %s: %s", zip_file_path, e
            )
            return {
                "success": False,
                "message": f"Failed to read attachment file: {e}",
            }

    if attachment_bytes:
        safe_client = re.sub(r"[^a-zA-Z0-9_\-]", "_", client_name)[:50]
        filename = f"media_plan_{safe_client}.zip"
        payload["attachments"] = [
            {
                "filename": filename,
                "content": base64.b64encode(attachment_bytes).decode("ascii"),
                "type": "application/zip",
            }
        ]

    # ── Send via Resend API ──
    body = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        _RESEND_ENDPOINT,
        data=body,
        method="POST",
    )
    req.add_header("Authorization", f"Bearer {_API_KEY}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header(
        "User-Agent",
        "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)",
    )

    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
            resp_data = json.loads(resp.read().decode("utf-8"))
            message_id = resp_data.get("id") or ""
            logger.info(
                "plan_delivery: email sent (id=%s, to=%s, client=%s)",
                message_id,
                recipient_email,
                client_name[:40],
            )
            # Rate limit already recorded atomically in _check_and_record_rate_limit
            return {
                "success": True,
                "message": f"Media plan emailed to {recipient_email}",
                "message_id": message_id,
            }

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
        logger.warning("plan_delivery: Resend API error: %s", error_detail)
        return {
            "success": False,
            "message": f"Email delivery failed: {error_detail}",
        }

    except Exception as exc:
        error_detail = f"{type(exc).__name__}: {exc}"
        logger.warning("plan_delivery: failed to send email: %s", error_detail)
        return {
            "success": False,
            "message": f"Email delivery failed: {error_detail}",
        }
