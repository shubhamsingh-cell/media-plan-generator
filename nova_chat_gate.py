"""Signed widget tokens for the Nova chat endpoints (NOVA_CHAT_GATE).

Today a browser on an allowed host reaches /api/chat, /api/chat/stream and
/ws/chat as a "same-origin guest": the server trusts the Origin / Referer
header, which any non-browser client can set. ``NOVA_CHAT_GATE=signed``
additionally requires a short-lived HMAC-signed widget token for every guest
chat turn:

* the page that embeds a chat client mints one with
  ``POST /api/chat/widget-token`` (body ``{"conversation_id": ...}``),
* the token is bound to that conversation id and expires after
  ``TOKEN_TTL_SECONDS``; every authorised turn returns a fresh one for the
  conversation the turn actually used,
* HTTP chat calls send it in the ``X-Nova-Widget-Token`` header, WebSocket
  chat messages in a ``widget_token`` field.

Replay safety comes from the existing per-conversation session token: a
widget token only names a conversation, and a stored conversation is resumed
only with its own session token (``nova_persistence.resolve_chat_conversation``),
so a replayed widget token never opens another visitor's conversation.

``NOVA_CHAT_GATE`` unset or ``off`` (the default) leaves every chat endpoint
exactly as before. See docs/NOVA_CHAT_GATE.md for the flip and blast radius.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import re
import secrets
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

GATE_ENV = "NOVA_CHAT_GATE"
SECRET_ENV = "NOVA_WIDGET_TOKEN_SECRET"
WIDGET_TOKEN_HEADER = "X-Nova-Widget-Token"
WIDGET_TOKEN_FIELD = "widget_token"
MINT_PATH = "/api/chat/widget-token"
TOKEN_TTL_SECONDS = 900
# Code returned (HTTP JSON body and WebSocket error frame) when a guest turn
# lacks a valid widget token; clients re-mint once on it.
TOKEN_REQUIRED_CODE = "WIDGET_TOKEN_REQUIRED"

MODE_OFF = "off"
MODE_SIGNED = "signed"

_VERSION = "v1"
_CONVERSATION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")

# Generated at import. Under gunicorn --preload the app (and this module) is
# imported once in the master before the workers fork, so every worker shares
# it; it changes on every deploy, which invalidates outstanding tokens (the
# clients re-mint on the 401). Set NOVA_WIDGET_TOKEN_SECRET (or
# SESSION_SIGNING_SECRET) when the app runs without --preload.
_BOOT_SECRET = secrets.token_bytes(32)

_warned_lock = threading.Lock()
_warned_values: set = set()


def gate_mode() -> str:
    """``signed`` when NOVA_CHAT_GATE asks for it, else ``off``.

    Unknown values keep the gate off (a typo must not 401 every visitor) and
    log one WARNING per distinct value.
    """
    raw = (os.environ.get(GATE_ENV) or "").strip().lower()
    if raw == MODE_SIGNED:
        return MODE_SIGNED
    if raw not in ("", MODE_OFF):
        with _warned_lock:
            if raw not in _warned_values:
                _warned_values.add(raw)
                logger.warning(
                    "%s=%r is not 'off' or 'signed'; the chat gate stays OFF",
                    GATE_ENV,
                    raw,
                )
    return MODE_OFF


def is_signed() -> bool:
    """True when guest chat turns need a widget token."""
    return gate_mode() == MODE_SIGNED


def secret_source() -> str:
    """Which secret signs widget tokens (for the boot log; never the value)."""
    if (os.environ.get(SECRET_ENV) or "").strip():
        return SECRET_ENV
    if (os.environ.get("SESSION_SIGNING_SECRET") or "").strip():
        return "SESSION_SIGNING_SECRET (derived)"
    return "boot-random (rotates on deploy)"


def _signing_key() -> bytes:
    explicit = (os.environ.get(SECRET_ENV) or "").strip()
    if explicit:
        return explicit.encode("utf-8")
    session_secret = (os.environ.get("SESSION_SIGNING_SECRET") or "").strip()
    if session_secret:
        # Domain-separated so a widget token can never double as a cookie
        # signature (both are HMAC-SHA256 over short strings).
        return hmac.new(
            session_secret.encode("utf-8"), b"nova-widget-token/v1", hashlib.sha256
        ).digest()
    return _BOOT_SECRET


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _signature(conversation_id: str, expires_at: int) -> bytes:
    msg = f"{_VERSION}|{conversation_id}|{expires_at}".encode("utf-8")
    return hmac.new(_signing_key(), msg, hashlib.sha256).digest()


def valid_conversation_id(conversation_id: object) -> bool:
    """A conversation id a token may be bound to (printable, <= 128 chars)."""
    return isinstance(conversation_id, str) and bool(
        _CONVERSATION_ID_RE.match(conversation_id)
    )


def mint(conversation_id: str, now: Optional[float] = None) -> str:
    """A widget token for ``conversation_id`` valid for TOKEN_TTL_SECONDS.

    Raises:
        ValueError: ``conversation_id`` is not a valid id.
    """
    if not valid_conversation_id(conversation_id):
        raise ValueError("invalid conversation id")
    expires_at = int(now if now is not None else time.time()) + TOKEN_TTL_SECONDS
    return ".".join(
        (
            _VERSION,
            _b64(conversation_id.encode("utf-8")),
            str(expires_at),
            _b64(_signature(conversation_id, expires_at)),
        )
    )


def check(token: object, conversation_id: object, now: Optional[float] = None) -> str:
    """Verdict for ``token`` presented for ``conversation_id``.

    Returns "ok", or the reason it fails: "missing", "malformed",
    "bad_signature", "expired", "wrong_conversation" or "bad_conversation".
    The signature is checked before the binding, so a forged token always
    reads "bad_signature".
    """
    if not isinstance(token, str) or not token:
        return "missing"
    parts = token.split(".")
    if len(parts) != 4 or parts[0] != _VERSION or not parts[2].isdigit():
        return "malformed"
    try:
        bound_id = _unb64(parts[1]).decode("utf-8")
        presented_sig = _unb64(parts[3])
    except (ValueError, UnicodeDecodeError):
        return "malformed"
    expires_at = int(parts[2])
    if not hmac.compare_digest(presented_sig, _signature(bound_id, expires_at)):
        return "bad_signature"
    current = now if now is not None else time.time()
    if expires_at <= current:
        return "expired"
    if not valid_conversation_id(conversation_id):
        return "bad_conversation"
    if not hmac.compare_digest(
        bound_id.encode("utf-8"), str(conversation_id).encode("utf-8")
    ):
        return "wrong_conversation"
    return "ok"


def response_fields(conversation_id: object) -> dict:
    """Extra response fields for a chat turn: a fresh widget token under
    ``signed`` for the conversation the turn used, nothing under ``off``."""
    if not is_signed() or not valid_conversation_id(conversation_id):
        return {}
    return {WIDGET_TOKEN_FIELD: mint(str(conversation_id))}


def log_boot_notice() -> None:
    """One INFO line naming the gate mode (and secret source when signed)."""
    mode = gate_mode()
    if mode == MODE_SIGNED:
        logger.info(
            "Nova chat gate: %s=signed (guest chat turns need a widget token; "
            "signing secret: %s; TTL %ss)",
            GATE_ENV,
            secret_source(),
            TOKEN_TTL_SECONDS,
        )
    else:
        logger.info(
            "Nova chat gate: %s=off (same-origin guests admitted as before)",
            GATE_ENV,
        )
