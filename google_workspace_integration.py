"""Google Workspace integration -- Calendar milestones + Gmail/Resend email.

Reuses service-account auth from sheets_export.py (GOOGLE_SLIDES_CREDENTIALS_B64).
Gmail send requires domain-wide delegation; falls back to Resend automatically.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

# Reuse shared auth helpers from the Sheets module
from sheets_export import _get_access_token, _load_credentials  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)

_CALENDAR_BASE = "https://www.googleapis.com/calendar/v3"
_GMAIL_BASE = "https://gmail.googleapis.com/gmail/v1"
_RESEND_ENDPOINT = "https://api.resend.com/emails"
_TIMEOUT = 15
_CALENDAR_ID: str = os.environ.get("GOOGLE_CALENDAR_ID") or "primary"
_RESEND_KEY: str = (os.environ.get("RESEND_API_KEY") or "").strip()
_FROM_EMAIL: str = (
    os.environ.get("ALERT_EMAIL_FROM") or "onboarding@resend.dev"
).strip()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _api_request(method: str, url: str, body: dict | None = None) -> dict | None:
    """Authenticated request to a Google API endpoint."""
    token = _get_access_token()
    if not token:
        return None
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, context=ctx, timeout=_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {"status": "ok"}
    except urllib.error.HTTPError as exc:
        err = ""
        try:
            err = exc.read().decode("utf-8")[:500]
        except OSError:
            pass
        logger.error(
            "workspace: %s %s HTTP %d: %s", method, url, exc.code, err, exc_info=True
        )
    except (urllib.error.URLError, json.JSONDecodeError) as exc:
        logger.error("workspace: %s %s error: %s", method, url, exc, exc_info=True)
    return None


def _parse_campaign_start(plan_data: dict) -> datetime:
    """Extract campaign start datetime from plan data, defaulting to tomorrow 09:00 UTC."""
    timeline = plan_data.get("timeline") or plan_data.get("campaign_timeline") or []
    date_str = ""
    if isinstance(timeline, list) and timeline:
        first = timeline[0] if isinstance(timeline[0], dict) else {}
        date_str = first.get("start_date") or first.get("date") or ""
    try:
        if date_str:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        pass
    tomorrow = datetime.now(timezone.utc) + timedelta(days=1)
    return tomorrow.replace(hour=9, minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# PUBLIC: Calendar
# ---------------------------------------------------------------------------


def create_campaign_events(plan_data: dict, calendar_id: str = "primary") -> list[dict]:
    """Create Calendar events for campaign milestones (launch, checkpoints, close).

    Returns list of created event dicts or empty list if not configured.
    """
    if not _load_credentials():
        logger.info("workspace: Calendar not configured, skipping events")
        return []

    cal_id = urllib.parse.quote(calendar_id or _CALENDAR_ID, safe="")
    client = plan_data.get("client_name") or "Client"
    role = plan_data.get("job_title") or plan_data.get("role") or "Campaign"
    budget = plan_data.get("budget") or plan_data.get("monthly_budget") or ""
    start = _parse_campaign_start(plan_data)

    milestones = [
        (
            0,
            1,
            f"{client} - {role} Launch",
            f"Launch for {client}. Role: {role}, Budget: {budget}. Activate channels, verify tracking.",
        ),
        (
            7,
            1,
            f"{client} - Week 1 Checkpoint",
            "Review CTR, CPC, apply rates. Pause underperformers.",
        ),
        (
            14,
            2,
            f"{client} - Mid-Campaign Review",
            "Budget reallocation, channel optimization, A/B test results.",
        ),
        (
            21,
            1,
            f"{client} - Week 3 Checkpoint",
            "Scale top channels, finalize creative rotation.",
        ),
        (
            28,
            2,
            f"{client} - Campaign Close & Report",
            "Final metrics, ROI analysis, recommendations.",
        ),
    ]

    created: list[dict] = []
    for offset_days, hours, summary, desc in milestones:
        ev_start = start + timedelta(days=offset_days)
        ev_end = ev_start + timedelta(hours=hours)
        body: dict[str, Any] = {
            "summary": summary,
            "description": desc,
            "start": {"dateTime": ev_start.isoformat(), "timeZone": "UTC"},
            "end": {"dateTime": ev_end.isoformat(), "timeZone": "UTC"},
            "reminders": {
                "useDefault": False,
                "overrides": [
                    {"method": "popup", "minutes": 30},
                    {"method": "email", "minutes": 1440},
                ],
            },
        }
        result = _api_request(
            "POST", f"{_CALENDAR_BASE}/calendars/{cal_id}/events", body=body
        )
        if result and result.get("id"):
            created.append(
                {
                    "id": result["id"],
                    "htmlLink": result.get("htmlLink") or "",
                    "summary": summary,
                    "start": ev_start.isoformat(),
                }
            )
            logger.info("workspace: created '%s'", summary)
        else:
            logger.warning("workspace: failed to create '%s'", summary)
    return created


def schedule_reminder(event_title: str, event_date: str, description: str = "") -> dict:
    """Schedule a single reminder event on Google Calendar.

    Args:
        event_title: Title for the reminder.
        event_date: ISO date (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS).
        description: Optional description.

    Returns:
        Created event dict or empty dict on failure.
    """
    if not _load_credentials():
        return {}
    if not event_title or not event_date:
        logger.warning("workspace: event_title and event_date required")
        return {}
    try:
        if "T" in event_date:
            dt = datetime.fromisoformat(event_date.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(event_date).replace(
                hour=9, minute=0, second=0, tzinfo=timezone.utc
            )
    except (ValueError, TypeError) as exc:
        logger.error("workspace: bad date '%s': %s", event_date, exc)
        return {}

    cal_id = urllib.parse.quote(_CALENDAR_ID, safe="")
    body: dict[str, Any] = {
        "summary": event_title,
        "description": description or f"Reminder: {event_title}",
        "start": {"dateTime": dt.isoformat(), "timeZone": "UTC"},
        "end": {"dateTime": (dt + timedelta(hours=1)).isoformat(), "timeZone": "UTC"},
        "reminders": {
            "useDefault": False,
            "overrides": [{"method": "popup", "minutes": 15}],
        },
    }
    result = _api_request(
        "POST", f"{_CALENDAR_BASE}/calendars/{cal_id}/events", body=body
    )
    if result and result.get("id"):
        logger.info("workspace: reminder '%s' on %s", event_title, event_date)
        return {
            "id": result["id"],
            "htmlLink": result.get("htmlLink") or "",
            "summary": event_title,
            "start": dt.isoformat(),
        }
    return {}


# ---------------------------------------------------------------------------
# PUBLIC: Email (Gmail + Resend fallback)
# ---------------------------------------------------------------------------


def send_plan_email(
    to_emails: list[str],
    subject: str,
    body_html: str,
    attachments: list[dict] | None = None,
) -> bool:
    """Send media plan via Gmail API with Resend fallback.

    Args:
        to_emails: Recipient email addresses.
        subject: Email subject.
        body_html: HTML body.
        attachments: Optional list of dicts with 'filename' and 'content_b64'.

    Returns:
        True if sent via either provider, False on total failure.
    """
    if not to_emails or not subject:
        logger.warning("workspace: to_emails and subject required")
        return False

    # Try Gmail first
    if _load_credentials():
        if _send_gmail(to_emails, subject, body_html, attachments):
            return True
        logger.info("workspace: Gmail failed, trying Resend")

    # Resend fallback
    if _RESEND_KEY:
        return _send_resend(to_emails, subject, body_html)

    logger.warning("workspace: no email provider configured")
    return False


def _send_gmail(
    to_emails: list[str],
    subject: str,
    body_html: str,
    attachments: list[dict] | None = None,
) -> bool:
    """Send via Gmail API (requires domain-wide delegation)."""
    creds = _load_credentials()
    if not creds:
        return False
    sender = creds.get("client_email") or ""
    boundary = f"nova_{int(time.time())}"
    parts = [
        f"From: {sender}",
        f"To: {', '.join(to_emails)}",
        f"Subject: {subject}",
        "MIME-Version: 1.0",
        f'Content-Type: multipart/mixed; boundary="{boundary}"',
        "",
        f"--{boundary}",
        "Content-Type: text/html; charset=UTF-8",
        "Content-Transfer-Encoding: base64",
        "",
        base64.b64encode(body_html.encode("utf-8")).decode("ascii"),
    ]
    for att in attachments or []:
        fn = att.get("filename") or "attachment"
        parts.extend(
            [
                f"--{boundary}",
                f'Content-Type: application/octet-stream; name="{fn}"',
                f'Content-Disposition: attachment; filename="{fn}"',
                "Content-Transfer-Encoding: base64",
                "",
                att.get("content_b64") or "",
            ]
        )
    parts.append(f"--{boundary}--")
    raw = base64.urlsafe_b64encode("\r\n".join(parts).encode("utf-8")).decode("ascii")
    result = _api_request(
        "POST", f"{_GMAIL_BASE}/users/me/messages/send", body={"raw": raw}
    )
    if result and result.get("id"):
        logger.info("workspace: Gmail sent id=%s", result["id"])
        return True
    return False


def _send_resend(to_emails: list[str], subject: str, body_html: str) -> bool:
    """Send via Resend API as fallback."""
    data = json.dumps(
        {"from": _FROM_EMAIL, "to": to_emails, "subject": subject, "html": body_html}
    ).encode("utf-8")
    req = urllib.request.Request(_RESEND_ENDPOINT, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {_RESEND_KEY}")
    req.add_header("Content-Type", "application/json")
    req.add_header("User-Agent", "Nova-AI-Suite/4.0.0")
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT) as resp:
            rid = json.loads(resp.read().decode("utf-8")).get("id") or ""
            logger.info("workspace: Resend sent id=%s", rid)
            return True
    except urllib.error.HTTPError as exc:
        err = ""
        try:
            err = exc.read().decode("utf-8")[:500]
        except OSError:
            pass
        logger.error("workspace: Resend HTTP %d: %s", exc.code, err, exc_info=True)
    except (urllib.error.URLError, json.JSONDecodeError, OSError) as exc:
        logger.error("workspace: Resend error: %s", exc, exc_info=True)
    return False


# ---------------------------------------------------------------------------
# PUBLIC: Health check
# ---------------------------------------------------------------------------


def get_status() -> dict:
    """Health check for Calendar + Gmail/Resend integration.

    Returns:
        Dict with 'calendar' and 'email' sub-status dicts.
    """
    creds = _load_credentials()
    has_creds = creds is not None
    return {
        "calendar": {
            "configured": has_creds,
            "calendar_id": _CALENDAR_ID,
            "service_account": (
                (creds.get("client_email") or "unknown") if creds else "not_configured"
            ),
        },
        "email": {
            "gmail_configured": has_creds,
            "resend_configured": bool(_RESEND_KEY),
            "provider": (
                "gmail+resend"
                if has_creds and _RESEND_KEY
                else "gmail" if has_creds else "resend" if _RESEND_KEY else "none"
            ),
        },
    }
