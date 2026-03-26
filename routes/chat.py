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


def _handle_chat_history(handler, path: str, parsed: Any) -> None:
    """/api/chat/history -- load conversation history from Supabase."""
    qs = urllib.parse.parse_qs(parsed.query)
    conv_id = (qs.get("conversation_id") or [None])[0]
    if not conv_id:
        handler._send_json(
            {"error": "conversation_id parameter required"}, status_code=400
        )
        return
    _app = sys.modules.get("app") or sys.modules.get("__main__")
    _load_conversation_history = getattr(_app, "_load_conversation_history", None)

    history = _load_conversation_history(conv_id)
    handler._send_json({"conversation_id": conv_id, "messages": history})


def _handle_chat_conversations(handler, path: str, parsed: Any) -> None:
    """/api/chat/conversations -- list recent conversations from Supabase."""
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
    """/api/nova/conversations/search -- search conversations by query term."""
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
            f"&title=ilike.*{urllib.parse.quote(query)}*"
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
