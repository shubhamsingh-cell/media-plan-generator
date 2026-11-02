"""Chat GET route handlers.

Extracted from app.py to reduce its size.  Every public function here
accepts ``handler`` (a ``MediaPlanHandler`` instance) and ``path`` (the
parsed URL path string).  Returns ``True`` if the route was handled.
"""

import sys
import logging
import urllib.parse
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_chat_get_routes(handler, path: str, parsed: Any) -> bool:
    """Dispatch chat-related GET routes.  Returns True if handled."""
    _fn = _CHAT_GET_ROUTE_MAP.get(path)
    if _fn is not None:
        _fn(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# Individual route handlers
# ---------------------------------------------------------------------------


SESSION_TOKEN_HEADER = "X-Nova-Session-Token"


def request_has_conversation_token(handler, conversation_id: str) -> bool:
    """True when the request presents ``conversation_id``'s session token.

    Convention for every endpoint that reads a stored conversation: the token
    the chat endpoints returned with the conversation id is sent in the
    ``X-Nova-Session-Token`` request header (never in the URL, so it stays
    out of access logs and Referer headers).
    """
    token = handler.headers.get(SESSION_TOKEN_HEADER) or ""
    try:
        from nova_persistence import verify_conversation_token
    except ImportError:
        logger.error("nova_persistence unavailable for token check", exc_info=True)
        return False
    return verify_conversation_token(conversation_id, token)


def handle_widget_token_post(handler) -> None:
    """POST /api/chat/widget-token -- mint a widget token for a conversation.

    Body: ``{"conversation_id": "<id>"}``. Admitted on the same terms the
    chat endpoints use today (a real credential or a same-origin page), and
    rate-limited with the other /api POST routes. Under NOVA_CHAT_GATE=off it
    answers ``{"gate": "off"}`` and mints nothing, so clients can call it
    unconditionally. The token is bound to the conversation id and expires
    after ``nova_chat_gate.TOKEN_TTL_SECONDS``.
    """
    import json

    import nova_chat_gate

    if not (
        handler._check_joveo_auth()
        or handler._check_admin_auth()
        or handler._check_same_origin_auth()
    ):
        handler._send_error("Authentication required", "AUTH_REQUIRED", 401)
        return
    if not nova_chat_gate.is_signed():
        handler._send_json({"gate": nova_chat_gate.MODE_OFF})
        return
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
    except (ValueError, TypeError):
        content_len = 0
    if content_len <= 0 or content_len > 4096:
        handler._send_error("Body must be a small JSON object", "VALIDATION_ERROR", 400)
        return
    try:
        data = json.loads(handler.rfile.read(content_len))
    except (ValueError, RecursionError):
        handler._send_error("Invalid JSON", "VALIDATION_ERROR", 400)
        return
    conversation_id = data.get("conversation_id") if isinstance(data, dict) else None
    if not nova_chat_gate.valid_conversation_id(conversation_id):
        handler._send_error(
            "conversation_id is required", "VALIDATION_ERROR", 400, field="conversation_id"
        )
        return
    handler._send_json(
        {
            "gate": nova_chat_gate.MODE_SIGNED,
            "conversation_id": conversation_id,
            "widget_token": nova_chat_gate.mint(conversation_id),
            "expires_in": nova_chat_gate.TOKEN_TTL_SECONDS,
        }
    )


def _handle_chat_history(handler, path: str, parsed: Any) -> None:
    """/api/chat/history -- load one conversation's stored messages.

    Requires the conversation's session token in the ``X-Nova-Session-Token``
    header; answers 403 without a matching token.
    """
    qs = urllib.parse.parse_qs(parsed.query)
    conv_id = (qs.get("conversation_id") or [None])[0]
    if not conv_id:
        handler._send_json(
            {"error": "conversation_id parameter required"}, status_code=400
        )
        return
    if not request_has_conversation_token(handler, conv_id):
        handler._send_error(
            "A valid session token is required for this conversation",
            "FORBIDDEN",
            403,
        )
        return
    _app = sys.modules.get("app") or sys.modules.get("__main__")
    _load_conversation_history = getattr(_app, "_load_conversation_history", None)

    history = _load_conversation_history(conv_id)
    handler._send_json({"conversation_id": conv_id, "messages": history})


def _handle_chat_conversations(handler, path: str, parsed: Any) -> None:
    """/api/chat/conversations -- list recent conversations (admin only)."""
    if not handler._check_admin_auth():
        handler._send_error("Admin authentication required", "AUTH_REQUIRED", 401)
        return
    qs = urllib.parse.parse_qs(parsed.query)
    limit_str = (qs.get("limit") or ["50"])[0]
    try:
        limit_val = min(int(limit_str), 200)
    except (ValueError, TypeError):
        limit_val = 50
    _app = sys.modules.get("app") or sys.modules.get("__main__")
    _list_conversations = getattr(_app, "_list_conversations", None)

    conversations = _list_conversations(limit_val)
    handler._send_json({"conversations": conversations, "count": len(conversations)})


def _handle_chat_conversations_search(handler, path: str, parsed: Any) -> None:
    """/api/nova/conversations/search -- search conversations (admin only)."""
    if not handler._check_admin_auth():
        handler._send_error("Admin authentication required", "AUTH_REQUIRED", 401)
        return
    qs = urllib.parse.parse_qs(parsed.query)
    query = (qs.get("q") or [""])[0].strip()
    if not query:
        handler._send_json({"error": "q parameter required"}, status_code=400)
        return

    limit_str = (qs.get("limit") or ["20"])[0]
    try:
        limit_val = min(int(limit_str), 100)
    except (ValueError, TypeError):
        limit_val = 20

    try:
        _app = sys.modules.get("app") or sys.modules.get("__main__")
        _supabase_rest = getattr(_app, "_supabase_rest", None)
        if not _supabase_rest:
            handler._send_json({"conversations": [], "count": 0, "query": query})
            return

        # Search by title (ilike) in Supabase
        params = (
            f"?select=id,title,updated_at,messages"
            f"&title=ilike.*{urllib.parse.quote(query, safe='')}*"
            f"&order=updated_at.desc"
            f"&limit={limit_val}"
        )
        result = _supabase_rest("nova_conversations", method="GET", params=params)
        if not isinstance(result, list):
            result = []

        conversations = []
        for row in result:
            cid = row.get("id") or ""
            messages = row.get("messages") or []
            last_msg = ""
            if isinstance(messages, list):
                for msg in reversed(messages):
                    if isinstance(msg, dict) and msg.get("role") == "user":
                        last_msg = (msg.get("content") or "")[:100]
                        break
            conversations.append(
                {
                    "conversation_id": cid,
                    "title": row.get("title") or "New Chat",
                    "updated_at": row.get("updated_at") or "",
                    "preview": last_msg,
                    "message_count": len(messages) if isinstance(messages, list) else 0,
                }
            )

        handler._send_json(
            {
                "conversations": conversations,
                "count": len(conversations),
                "query": query,
            }
        )
    except Exception as exc:
        logger.error("Conversation search failed: %s", exc, exc_info=True)
        handler._send_json(
            {"conversations": [], "count": 0, "query": query, "error": str(exc)}
        )


def _handle_chat_migrate(handler, path: str, parsed: Any) -> None:
    """/api/chat/migrate -- one-time migration (admin-protected)."""
    if not handler._check_admin_auth():
        handler.send_error(401, "Unauthorized")
        return
    try:
        from nova_persistence import migrate_row_per_turn_data

        stats = migrate_row_per_turn_data()
        handler._send_json({"status": "ok", "migration": stats})
    except ImportError:
        handler._send_json(
            {"error": "nova_persistence module not available"}, status_code=500
        )
    except Exception as mig_err:
        logger.error("Migration endpoint error: %s", mig_err, exc_info=True)
        handler._send_json({"error": f"Migration failed: {mig_err}"}, status_code=500)


# ---------------------------------------------------------------------------
# Route map
# ---------------------------------------------------------------------------

_CHAT_GET_ROUTE_MAP: dict[str, Any] = {
    "/api/chat/history": _handle_chat_history,
    "/api/chat/conversations": _handle_chat_conversations,
    "/api/nova/conversations/search": _handle_chat_conversations_search,
    "/api/chat/migrate": _handle_chat_migrate,
}
