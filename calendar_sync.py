"""calendar_sync.py -- Google Calendar integration for hiring campaign milestones.

Provides scheduling and event management for the media planner to create
hiring campaign milestone events (e.g., campaign launch, midpoint review,
close date) on a shared Google Calendar.

Uses Google Calendar API via service account for server-to-server auth.
Falls back gracefully if credentials are not configured.

Functions:
    create_hiring_event(title, date, details)  -- Create a calendar event
    get_upcoming_events(days)                  -- List upcoming events
    get_status()                               -- Health/diagnostic status

Configuration (env vars):
    GOOGLE_CALENDAR_CREDENTIALS  -- JSON string of service account credentials
    GOOGLE_CALENDAR_ID           -- Calendar ID (defaults to 'primary')

All functions return empty/None on failure (never raise).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# -- Configuration -----------------------------------------------------------

_CREDENTIALS_JSON: str = os.environ.get("GOOGLE_CALENDAR_CREDENTIALS") or ""
_CALENDAR_ID: str = os.environ.get("GOOGLE_CALENDAR_ID") or "primary"
_API_BASE: str = "https://www.googleapis.com/calendar/v3"
_TOKEN_URL: str = "https://oauth2.googleapis.com/token"
_REQUEST_TIMEOUT: int = 15

# -- Token cache -------------------------------------------------------------

_lock = threading.Lock()
_cached_token: Optional[str] = None
_token_expires_at: float = 0.0

# -- Service account credentials (parsed once) --------------------------------

_service_account: Optional[Dict[str, Any]] = None
_credentials_parsed: bool = False


def _parse_credentials() -> Optional[Dict[str, Any]]:
    """Parse service account credentials from environment variable.

    Returns:
        Parsed credentials dict, or None if not configured/invalid.
    """
    global _service_account, _credentials_parsed

    if _credentials_parsed:
        return _service_account

    _credentials_parsed = True

    if not _CREDENTIALS_JSON:
        logger.debug("calendar_sync: GOOGLE_CALENDAR_CREDENTIALS not set")
        return None

    try:
        creds = json.loads(_CREDENTIALS_JSON)
        required_keys = ["client_email", "private_key", "token_uri"]
        for key in required_keys:
            if key not in creds:
                logger.warning(
                    "calendar_sync: credentials missing required key '%s'", key
                )
                return None
        _service_account = creds
        logger.info(
            "calendar_sync: service account loaded (email=%s)",
            creds.get("client_email", "unknown"),
        )
        return _service_account
    except json.JSONDecodeError as exc:
        logger.error(
            "calendar_sync: failed to parse GOOGLE_CALENDAR_CREDENTIALS: %s",
            exc,
            exc_info=True,
        )
        return None


def _is_available() -> bool:
    """Check if Google Calendar integration is configured."""
    return _parse_credentials() is not None


def _build_jwt(creds: Dict[str, Any]) -> str:
    """Build a signed JWT for Google service account authentication.

    Uses stdlib-only approach with base64 + hmac. For production, this
    requires the `cryptography` or `PyJWT` package for RS256 signing.
    Falls back to a direct token exchange if JWT signing is unavailable.

    Args:
        creds: Service account credentials dict.

    Returns:
        Signed JWT string.

    Raises:
        ImportError: If no JWT signing library is available.
    """
    import base64
    import hashlib

    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    payload = {
        "iss": creds["client_email"],
        "scope": "https://www.googleapis.com/auth/calendar",
        "aud": creds.get("token_uri", _TOKEN_URL),
        "iat": now,
        "exp": now + 3600,
    }

    def _b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")

    header_b64 = _b64url(json.dumps(header).encode("utf-8"))
    payload_b64 = _b64url(json.dumps(payload).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}"

    # Try to use cryptography library for RS256 signing
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding

        private_key = serialization.load_pem_private_key(
            creds["private_key"].encode("utf-8"), password=None
        )
        signature = private_key.sign(
            signing_input.encode("utf-8"),
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        signature_b64 = _b64url(signature)
        return f"{signing_input}.{signature_b64}"
    except ImportError:
        logger.warning(
            "calendar_sync: 'cryptography' package not installed; "
            "Google Calendar JWT signing unavailable"
        )
        raise


def _get_access_token() -> Optional[str]:
    """Obtain a Google API access token via service account JWT.

    Caches the token and refreshes it 5 minutes before expiry.

    Returns:
        Access token string, or None on failure.
    """
    global _cached_token, _token_expires_at

    with _lock:
        now = time.time()
        if _cached_token and now < (_token_expires_at - 300):
            return _cached_token

    creds = _parse_credentials()
    if not creds:
        return None

    try:
        jwt_token = _build_jwt(creds)
    except ImportError:
        return None
    except (ValueError, TypeError, KeyError) as exc:
        logger.error("calendar_sync: JWT build failed: %s", exc, exc_info=True)
        return None

    # Exchange JWT for access token
    token_url = creds.get("token_uri", _TOKEN_URL)
    post_data = urllib.parse.urlencode(
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": jwt_token,
        }
    ).encode("utf-8")

    try:
        req = urllib.request.Request(token_url, data=post_data, method="POST")
        req.add_header("Content-Type", "application/x-www-form-urlencoded")

        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
            body = resp.read().decode("utf-8")
            result = json.loads(body)

        access_token = result.get("access_token")
        expires_in = result.get("expires_in", 3600)

        if not access_token:
            logger.warning("calendar_sync: no access_token in token response")
            return None

        with _lock:
            _cached_token = access_token
            _token_expires_at = time.time() + expires_in

        logger.debug("calendar_sync: access token obtained (expires_in=%d)", expires_in)
        return access_token

    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8")[:500]
        except OSError:
            pass
        logger.error(
            "calendar_sync: token exchange HTTP %d: %s",
            exc.code,
            error_body,
            exc_info=True,
        )
        return None
    except urllib.error.URLError as exc:
        logger.error(
            "calendar_sync: token exchange URLError: %s", exc.reason, exc_info=True
        )
        return None
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        logger.error("calendar_sync: token exchange error: %s", exc, exc_info=True)
        return None


def _api_request(
    method: str,
    path: str,
    body: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Make an authenticated request to the Google Calendar API.

    Args:
        method: HTTP method (GET, POST, PUT, DELETE).
        path: API path relative to the calendar base URL.
        body: Optional JSON body for POST/PUT requests.

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    token = _get_access_token()
    if not token:
        logger.debug("calendar_sync: no access token, skipping API request")
        return None

    url = f"{_API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode("utf-8") if body else None

    try:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
            resp_body = resp.read().decode("utf-8")
            if resp_body:
                return json.loads(resp_body)
            return {"status": "ok"}
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8")[:500]
        except OSError:
            pass
        logger.error(
            "calendar_sync: API %s %s HTTP %d: %s",
            method,
            path,
            exc.code,
            error_body,
            exc_info=True,
        )
        return None
    except urllib.error.URLError as exc:
        logger.error(
            "calendar_sync: API %s %s URLError: %s",
            method,
            path,
            exc.reason,
            exc_info=True,
        )
        return None
    except (json.JSONDecodeError, OSError, ValueError) as exc:
        logger.error(
            "calendar_sync: API %s %s error: %s", method, path, exc, exc_info=True
        )
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════


def create_hiring_event(
    title: str,
    date: str,
    details: str = "",
    duration_hours: int = 1,
    attendees: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Create a hiring campaign milestone event on Google Calendar.

    Args:
        title: Event title (e.g., 'ProAmpac Campaign Launch').
        date: Event date in ISO format (YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS).
        details: Event description/details.
        duration_hours: Event duration in hours (default 1).
        attendees: Optional list of attendee email addresses.

    Returns:
        Created event dict with id, htmlLink, summary, start, end,
        or None on failure.
    """
    if not _is_available():
        logger.debug("calendar_sync: not configured, cannot create event")
        return None

    if not title or not date:
        logger.warning("calendar_sync: title and date are required")
        return None

    # Parse the date and build event body
    try:
        if "T" in date:
            start_dt = datetime.fromisoformat(date.replace("Z", "+00:00"))
        else:
            start_dt = datetime.fromisoformat(date).replace(
                hour=9, minute=0, second=0, tzinfo=timezone.utc
            )
        end_dt = start_dt + timedelta(hours=duration_hours)
    except (ValueError, TypeError) as exc:
        logger.error("calendar_sync: invalid date format '%s': %s", date, exc)
        return None

    event_body: Dict[str, Any] = {
        "summary": title,
        "description": details or f"Hiring campaign milestone: {title}",
        "start": {
            "dateTime": start_dt.isoformat(),
            "timeZone": "UTC",
        },
        "end": {
            "dateTime": end_dt.isoformat(),
            "timeZone": "UTC",
        },
        "reminders": {
            "useDefault": False,
            "overrides": [
                {"method": "popup", "minutes": 30},
                {"method": "email", "minutes": 1440},  # 1 day before
            ],
        },
    }

    if attendees:
        event_body["attendees"] = [{"email": email} for email in attendees]

    calendar_id = urllib.parse.quote(_CALENDAR_ID, safe="")
    result = _api_request("POST", f"/calendars/{calendar_id}/events", body=event_body)

    if result and result.get("id"):
        logger.info(
            "calendar_sync: event created (id=%s, title='%s', date=%s)",
            result["id"],
            title,
            date,
        )
        return {
            "id": result["id"],
            "htmlLink": result.get("htmlLink") or "",
            "summary": result.get("summary") or title,
            "start": result.get("start") or {},
            "end": result.get("end") or {},
            "status": result.get("status") or "confirmed",
        }

    logger.warning("calendar_sync: failed to create event '%s'", title)
    return None


def get_upcoming_events(
    days: int = 30,
    max_results: int = 50,
) -> List[Dict[str, Any]]:
    """Retrieve upcoming calendar events within a time window.

    Args:
        days: Number of days ahead to look (default 30).
        max_results: Maximum events to return (default 50).

    Returns:
        List of event dicts with id, summary, start, end, description.
        Returns empty list on failure.
    """
    if not _is_available():
        logger.debug("calendar_sync: not configured, returning empty events")
        return []

    now = datetime.now(timezone.utc)
    time_min = now.isoformat()
    time_max = (now + timedelta(days=days)).isoformat()

    calendar_id = urllib.parse.quote(_CALENDAR_ID, safe="")
    params = urllib.parse.urlencode(
        {
            "timeMin": time_min,
            "timeMax": time_max,
            "maxResults": max_results,
            "singleEvents": "true",
            "orderBy": "startTime",
        }
    )

    result = _api_request("GET", f"/calendars/{calendar_id}/events?{params}")

    if not result:
        return []

    items = result.get("items") or []
    events: List[Dict[str, Any]] = []

    for item in items:
        events.append(
            {
                "id": item.get("id") or "",
                "summary": item.get("summary") or "(No title)",
                "start": item.get("start") or {},
                "end": item.get("end") or {},
                "description": (item.get("description") or "")[:500],
                "htmlLink": item.get("htmlLink") or "",
                "status": item.get("status") or "confirmed",
            }
        )

    logger.debug(
        "calendar_sync: fetched %d upcoming events (next %d days)",
        len(events),
        days,
    )
    return events


def get_status() -> Dict[str, Any]:
    """Return health/diagnostic status for the Calendar Sync module.

    Returns:
        Dictionary with configuration state and token status.
    """
    creds = _parse_credentials()
    has_token = bool(_cached_token and time.time() < _token_expires_at)

    return {
        "available": creds is not None,
        "calendar_id": _CALENDAR_ID,
        "service_account_email": (
            creds.get("client_email", "unknown") if creds else "not_configured"
        ),
        "token_cached": has_token,
        "token_expires_in_seconds": (
            round(max(0, _token_expires_at - time.time()), 1) if has_token else 0
        ),
    }
