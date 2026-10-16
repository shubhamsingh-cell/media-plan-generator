"""slack_plan_notifier.py -- Slack notification for media plan generation events.

Sends a rich Block Kit message to a designated Slack channel every time
a media plan is successfully generated.

Configuration (env vars):
    SLACK_WEBHOOK_URL       -- Incoming webhook URL (required for notifications)
    SLACK_BOT_TOKEN         -- Bot token alternative (used if webhook not set)
    SLACK_PLAN_CHANNEL      -- Channel for plan notifications (default: #media-plans)

Non-blocking: fires in a daemon thread so generation response is not delayed.
Graceful degradation: logs a warning if Slack credentials are missing, never crashes.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import re
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# -- Configuration -----------------------------------------------------------

_WEBHOOK_URL: str = os.environ.get("SLACK_WEBHOOK_URL") or ""
if _WEBHOOK_URL and not _WEBHOOK_URL.startswith("https://hooks.slack.com/"):
    logger.warning("SLACK_WEBHOOK_URL does not point to hooks.slack.com -- disabled")
    _WEBHOOK_URL = ""
_BOT_TOKEN: str = os.environ.get("SLACK_BOT_TOKEN") or ""
_PLAN_CHANNEL: str = os.environ.get("SLACK_PLAN_CHANNEL") or "#media-plans"
_BASE_URL: str = (
    os.environ.get("BASE_URL")
    or os.environ.get("RENDER_EXTERNAL_URL")
    or "https://media-plan-generator.onrender.com"
)
_SEND_TIMEOUT = 15

# How long a Slack download button keeps working. The plain /api/jobs/<id>
# link dies with the 24h job expiry, and the "permanent" Drive copy has never
# uploaded in prod (service accounts have no Drive quota without a shared-
# drive NOVA_DRIVE_FOLDER_ID), so every button in #nova-media-plans was a
# 404 by the next day. The S47 Supabase copy keeps the ZIP; this link reads
# it for SLACK_DOWNLOAD_LINK_DAYS behind a token only the Slack message has.
_SLACK_LINK_DAYS_DEFAULT = 30


def slack_download_link_seconds() -> int:
    """Lifetime of a Slack download link, in seconds (env-tunable)."""
    raw = (os.environ.get("SLACK_DOWNLOAD_LINK_DAYS") or "").strip()
    try:
        days = int(raw) if raw else _SLACK_LINK_DAYS_DEFAULT
    except ValueError:
        days = _SLACK_LINK_DAYS_DEFAULT
    return max(1, min(days, 365)) * 86400


_SLACK_LINK_KEY_ENVS = (
    "SESSION_SIGNING_SECRET",
    "SUPABASE_SERVICE_ROLE_KEY",
    "SUPABASE_KEY",
)


def _is_public_supabase_key(val: str) -> bool:
    """True for a Supabase key that is public by design (anon).

    Either the value equals SUPABASE_ANON_KEY (the key /api/config serves to
    every browser) or it is a JWT whose payload carries role "anon". The
    payload is only decoded, never verified: this is a "is it public?" test,
    and anything unparseable is treated as not public.
    """
    anon = (os.environ.get("SUPABASE_ANON_KEY") or "").strip()
    if anon and val == anon:
        return True
    parts = val.split(".")
    if len(parts) != 3:
        return False
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
    except (ValueError, TypeError, UnicodeError):
        return False
    return isinstance(claims, dict) and claims.get("role") == "anon"


def _slack_link_keys() -> List[bytes]:
    """Every configured link-signing key, in minting-priority order.

    Read fresh on every call (never cached at import): a test or a rotated
    secret must not leave a stale key behind. Blank/whitespace values count
    as unset, a value repeated across env vars is listed once, and a public
    Supabase key (see _is_public_supabase_key) is never a candidate -- a link
    signed or verified with a key every browser can read is forgeable by
    anyone, even if someone parks the anon key in SUPABASE_KEY.
    """
    keys: List[bytes] = []
    for name in _SLACK_LINK_KEY_ENVS:
        val = (os.environ.get(name) or "").strip()
        if not val or _is_public_supabase_key(val):
            continue
        key = val.encode("utf-8")
        if key not in keys:
            keys.append(key)
    return keys


def _slack_link_key() -> bytes:
    """The key new links are minted with: the first configured one."""
    keys = _slack_link_keys()
    return keys[0] if keys else b""


def _token_for_key(key: bytes, job_id: str) -> str:
    msg = f"nova-slack-plan-download:v1:{job_id}".encode("utf-8")
    return hmac.new(key, msg, hashlib.sha256).hexdigest()[:32]


def slack_download_token(job_id: str) -> str:
    """HMAC token binding a Slack download link to one job ("" if no key).

    Always minted with the FIRST configured key (see _slack_link_keys).
    """
    key = _slack_link_key()
    if not key or not job_id:
        return ""
    return _token_for_key(key, job_id)


def verify_slack_download_token(job_id: str, token: str) -> bool:
    """Constant-time check of a Slack download token.

    A token is accepted when it was minted with ANY currently configured
    candidate key (the same ordered list minting draws its first key from),
    not only the key that would mint today. That is what lets a key rotation
    keep outstanding links alive for their remaining lifetime
    (SLACK_DOWNLOAD_LINK_DAYS): e.g. links minted with
    SUPABASE_SERVICE_ROLE_KEY keep opening after SESSION_SIGNING_SECRET is
    set and takes over minting. A token minted with a key that is no longer
    configured anywhere is rejected -- removing a key from the environment is
    how its links are retired.

    Public Supabase keys (SUPABASE_ANON_KEY, or any JWT with role "anon")
    are excluded from the candidate list, so they neither mint nor verify.
    """
    token = (token or "").strip().lower()
    # Shape-check before compare_digest: it raises TypeError on a non-ASCII
    # str (?t=%C3%A9), which would surface as an unauthenticated 500.
    if not job_id or not re.fullmatch(r"[0-9a-f]{32}", token):
        return False
    ok = False
    # No early exit: every candidate is compared (constant-time each), so
    # response time does not reveal which key a token matched.
    for key in _slack_link_keys():
        if hmac.compare_digest(_token_for_key(key, job_id), token):
            ok = True
    return ok


def slack_download_url(job_id: str) -> str:
    """The Slack button's link: the long-lived signed route when a key is
    configured, else the 24h job link."""
    base = _BASE_URL.rstrip("/")
    token = slack_download_token(job_id)
    if token:
        return f"{base}/api/plans/{job_id}/download?t={token}"
    return f"{base}/api/jobs/{job_id}"


def _format_budget(budget_val: Any) -> str:
    """Format budget value for display."""
    if not budget_val:
        return "N/A"
    try:
        num = float(str(budget_val).replace(",", "").replace("$", "").strip())
        if num >= 1_000_000:
            return f"${num / 1_000_000:,.1f}M"
        if num >= 1_000:
            return f"${num:,.0f}"
        return f"${num:,.2f}"
    except (ValueError, TypeError):
        return str(budget_val)


def qa_line(plan_data: Dict[str, Any]) -> str:
    """The notification's delivery-gate line, e.g. "QA: 2 critical
    (snake_case_leak, unsourced_competitor_claim) -- 3 auto-repaired".

    Empty string when the caller passed no QA result at all (older call
    sites / bundle_qa unavailable) so the message never claims a verdict it
    was not given. A bundle is delivered even with criticals (bundle_qa.
    gate_bundle policy) -- this line is what keeps that from being silent.
    """
    if "qa_critical_count" not in plan_data:
        return ""
    if plan_data.get("qa_status") == "timeout":
        return (
            f"QA: not checked -- qa_gate_timeout after "
            f"{plan_data.get('qa_elapsed_s') or '?'}s (bundle delivered unchecked)"
        )
    try:
        crit = int(plan_data.get("qa_critical_count") or 0)
    except (TypeError, ValueError):
        crit = 0
    codes = [str(c) for c in (plan_data.get("qa_critical_codes") or [])][:6]
    try:
        repaired = int(plan_data.get("qa_repaired_count") or 0)
    except (TypeError, ValueError):
        repaired = 0
    line = f"QA: {crit} critical"
    if crit and codes:
        line += f" ({', '.join(codes)})"
    if repaired:
        line += f" -- {repaired} auto-repaired before delivery"
    return line


def _build_plan_notification_blocks(plan_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build Slack Block Kit blocks for a plan generation notification.

    Args:
        plan_data: Dictionary with plan generation details.

    Returns:
        List of Block Kit block dicts.
    """
    client_name = plan_data.get("client_name") or "Unknown"
    industry = plan_data.get("industry") or "N/A"
    budget = _format_budget(plan_data.get("budget"))
    user_name = plan_data.get("user_name") or "Unknown"
    user_email = plan_data.get("user_email") or "Unknown"
    role_count = plan_data.get("role_count", 0)
    location_count = plan_data.get("location_count", 0)
    channel_count = plan_data.get("channel_count", 0)
    channels_list = plan_data.get("channels_list") or []
    gen_time = plan_data.get("generation_time_seconds", 0)
    job_id = plan_data.get("job_id") or ""
    timestamp = plan_data.get("timestamp") or time.strftime(
        "%Y-%m-%d %H:%M:%S UTC", time.gmtime()
    )
    filename = plan_data.get("filename") or ""

    # Build download URL -- prefer permanent Google Drive link over ephemeral job link
    drive_url = plan_data.get("drive_url") or ""
    download_url = ""
    if drive_url:
        download_url = drive_url
    elif job_id:
        download_url = slack_download_url(job_id)

    # Channels display (show up to 5, then "+N more")
    channels_display = "N/A"
    if channels_list:
        shown = channels_list[:5]
        channels_display = ", ".join(str(c) for c in shown)
        if len(channels_list) > 5:
            channels_display += f" +{len(channels_list) - 5} more"

    blocks: List[Dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "New Media Plan Generated",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Client:*\n{client_name}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Industry:*\n{industry}",
                },
            ],
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Budget:*\n{budget}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Generated by:*\n{user_name} ({user_email})",
                },
            ],
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Roles:* {role_count} | *Locations:* {location_count} | *Channels:* {channel_count}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Generation time:*\n{gen_time:.1f}s",
                },
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Channels:* {channels_display}",
            },
        },
    ]

    _qa = qa_line(plan_data)
    if _qa:
        _crit_ids = [str(i) for i in (plan_data.get("qa_critical_ids") or [])][:5]
        # Bold only when something is still critical -- that is the line a
        # reviewer must not scroll past.
        _qa_text = _qa if _qa.startswith("QA: 0 critical") else f"*{_qa}*"
        if _crit_ids:
            _qa_text += "\n" + "\n".join(f"• {i}" for i in _crit_ids)
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": _qa_text}})

    # Add download button if job_id available
    if download_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": f"Download {filename or 'Plan'}",
                            "emoji": True,
                        },
                        "url": download_url,
                        "style": "primary",
                    }
                ],
            }
        )

    # Context footer
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Nova AI Suite | {timestamp}",
                }
            ],
        }
    )
    blocks.append({"type": "divider"})

    return blocks


def _send_via_webhook(blocks: List[Dict[str, Any]], fallback_text: str) -> bool:
    """Send notification via Slack incoming webhook.

    Args:
        blocks: Block Kit blocks.
        fallback_text: Plain text fallback.

    Returns:
        True if sent successfully.
    """
    if not _WEBHOOK_URL:
        return False

    payload: Dict[str, Any] = {
        "text": fallback_text,
        "blocks": blocks,
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(_WEBHOOK_URL, data=data, method="POST")
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if body.strip().lower() == "ok" or resp.status == 200:
                logger.info("slack_plan_notifier: webhook notification sent")
                return True
            logger.warning(
                "slack_plan_notifier: webhook unexpected response: %s", body[:200]
            )
            return False
    except urllib.error.HTTPError as http_err:
        error_body = ""
        try:
            error_body = http_err.read().decode("utf-8", errors="replace")[:300]
        except OSError:
            pass
        logger.warning(
            "slack_plan_notifier: webhook HTTP %d: %s", http_err.code, error_body
        )
        return False
    except (urllib.error.URLError, OSError, ValueError) as exc:
        logger.warning("slack_plan_notifier: webhook error: %s", exc)
        return False


def _send_via_bot(blocks: List[Dict[str, Any]], fallback_text: str) -> bool:
    """Send notification via Slack Bot API.

    Args:
        blocks: Block Kit blocks.
        fallback_text: Plain text fallback.

    Returns:
        True if sent successfully.
    """
    if not _BOT_TOKEN:
        return False

    payload: Dict[str, Any] = {
        "channel": _PLAN_CHANNEL,
        "text": fallback_text,
        "blocks": blocks,
    }

    data = json.dumps(payload).encode("utf-8")
    url = "https://slack.com/api/chat.postMessage"
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Authorization", f"Bearer {_BOT_TOKEN}")
    req.add_header("Content-Type", "application/json; charset=utf-8")

    try:
        with urllib.request.urlopen(req, timeout=_SEND_TIMEOUT) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")
            resp_data = json.loads(resp_body)
            if resp_data.get("ok"):
                logger.info(
                    "slack_plan_notifier: bot message sent to %s", _PLAN_CHANNEL
                )
                return True
            error_msg = resp_data.get("error") or "unknown"
            logger.warning("slack_plan_notifier: bot API error: %s", error_msg)
            return False
    except (urllib.error.HTTPError, urllib.error.URLError, OSError, ValueError) as exc:
        logger.warning("slack_plan_notifier: bot API error: %s", exc)
        return False


def notify_plan_generated(plan_data: Dict[str, Any]) -> None:
    """Send a Slack notification for a successfully generated media plan.

    This function is non-blocking: it fires the notification in a daemon
    thread so the caller is not delayed. If Slack credentials are missing,
    it logs a debug message and returns immediately.

    Args:
        plan_data: Dictionary containing plan generation details:
            - client_name: str
            - industry: str
            - budget: str or number
            - user_name: str (from Google auth or requester_name)
            - user_email: str (from Google auth or requester_email)
            - role_count: int
            - location_count: int
            - channel_count: int
            - channels_list: list[str] (channel names)
            - generation_time_seconds: float
            - job_id: str (for download link)
            - filename: str (result filename)
            - timestamp: str (ISO format)
    """
    if not _WEBHOOK_URL and not _BOT_TOKEN:
        logger.debug(
            "slack_plan_notifier: no SLACK_WEBHOOK_URL or SLACK_BOT_TOKEN set, "
            "skipping plan notification"
        )
        return

    def _send():
        try:
            blocks = _build_plan_notification_blocks(plan_data)
            client = plan_data.get("client_name") or "Unknown"
            user = plan_data.get("user_name") or "Unknown"
            fallback_text = f"New Media Plan Generated: {client} by {user}"
            _qa = qa_line(plan_data)
            if _qa:
                fallback_text += f" | {_qa}"

            # Try webhook first, then bot API
            sent = _send_via_webhook(blocks, fallback_text)
            if not sent:
                sent = _send_via_bot(blocks, fallback_text)
            if not sent:
                logger.warning(
                    "slack_plan_notifier: failed to send notification for job %s",
                    plan_data.get("job_id", "?"),
                )
        except Exception as exc:
            logger.error(
                "slack_plan_notifier: unexpected error: %s", exc, exc_info=True
            )

    t = threading.Thread(target=_send, daemon=True, name="slack-plan-notify")
    t.start()
