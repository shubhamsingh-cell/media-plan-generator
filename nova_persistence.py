"""
Nova Conversation Persistence Layer

Handles all Supabase operations for Nova chatbot:
- Conversation management (create, read, update, delete)
- Document storage and retrieval (for RAG)
- Shared conversation links
- Theme and avatar preferences
- Thread-safe operations with locks
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from secrets import token_hex
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# File-based error logging for persistence failures (P1-15 diagnostic)
# ---------------------------------------------------------------------------
_PERSISTENCE_LOG_DIR = Path(os.environ.get("NOVA_LOG_DIR", "/tmp"))
_PERSISTENCE_LOG_FILE = _PERSISTENCE_LOG_DIR / "nova_persistence_errors.log"


def _log_persistence_error(operation: str, conversation_id: str, error: str) -> None:
    """Write persistence errors to a local file for debugging.

    This supplements the standard logger -- errors are written to a dedicated
    file so they survive log rotation and can be checked even when structured
    logging is noisy.

    Args:
        operation: The operation that failed (e.g. 'append_message').
        conversation_id: The conversation ID involved.
        error: The error message or repr.
    """
    try:
        ts = datetime.utcnow().isoformat() + "Z"
        line = f"[{ts}] {operation} cid={conversation_id} err={error}\n"
        with open(_PERSISTENCE_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        pass  # best-effort; do not let logging errors cascade


# Supabase imports (graceful fallback)
try:

    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    logger.warning(
        "Supabase client not available; persistence features will be limited"
    )


# ---------------------------------------------------------------------------
# Thread-safe Supabase Client Singleton (delegates to supabase_client.py)
# ---------------------------------------------------------------------------

# Schema detection cache: None = unknown, True = new (document model), False = old (row-per-turn)
_schema_is_document_model: Optional[bool] = None
_schema_detection_lock = threading.Lock()

# Column name used to look up conversations by the widget-generated ID.
# Old schema (pre-001 migration): "conversation_id" TEXT column.
# New schema (001 migration): "id" UUID column -- but widget IDs are not UUIDs,
# so a TEXT "conversation_id" column must be added or the lookup column detected.
_conversation_lookup_column: str = "conversation_id"


def _get_supabase():
    """Get or initialize Supabase client (lazy, thread-safe).

    Delegates to the shared singleton in supabase_client.py.
    """
    try:
        from supabase_client import get_client

        return get_client()
    except ImportError:
        logger.warning("supabase_client module not available")
        return None


def _detect_schema() -> bool:
    """Detect whether nova_conversations uses the document model (new) or row-per-turn (old).

    Also detects the correct column name for conversation lookups and caches it
    in ``_conversation_lookup_column``.

    Checks once and caches the result. The new schema has a 'messages' JSONB column;
    the old schema has 'user_message' and 'assistant_response' columns.

    Returns:
        True if document model (new schema), False if row-per-turn (old schema).
    """
    global _schema_is_document_model, _conversation_lookup_column

    if _schema_is_document_model is not None:
        return _schema_is_document_model

    with _schema_detection_lock:
        # Double-check after acquiring lock
        if _schema_is_document_model is not None:
            return _schema_is_document_model

        sb = _get_supabase()
        if not sb:
            logger.warning(
                "Cannot detect schema: Supabase unavailable; assuming old schema"
            )
            _schema_is_document_model = False
            return False

        try:
            # Fetch one row and inspect its keys
            result = sb.table("nova_conversations").select("*").limit(1).execute()
            if result.data:
                columns = set(result.data[0].keys())
                # Detect lookup column: prefer 'conversation_id' if it exists,
                # fall back to 'id' (001 migration UUID schema)
                if "conversation_id" in columns:
                    _conversation_lookup_column = "conversation_id"
                else:
                    _conversation_lookup_column = "id"
                if "messages" in columns:
                    _schema_is_document_model = True
                    logger.info(
                        "Detected document-model schema (lookup_col=%s)",
                        _conversation_lookup_column,
                    )
                else:
                    _schema_is_document_model = False
                    logger.info(
                        "Detected row-per-turn schema (lookup_col=%s)",
                        _conversation_lookup_column,
                    )
            else:
                # Empty table -- try inserting a test row with new schema fields.
                # Do NOT specify 'id' -- let the DB auto-generate it.
                # Use 'conversation_id' column if it exists; otherwise insert
                # without it and detect the resulting columns.
                test_cid = f"__schema_test__{uuid.uuid4()}"
                try:
                    insert_data: Dict[str, Any] = {
                        "user_id": "__schema_test__",
                        "messages": [],
                    }
                    # Try with conversation_id first (old table with S37 migration)
                    try:
                        ins_result = (
                            sb.table("nova_conversations")
                            .insert({**insert_data, "conversation_id": test_cid})
                            .execute()
                        )
                        _conversation_lookup_column = "conversation_id"
                    except Exception:
                        # conversation_id column does not exist (001 migration)
                        ins_result = (
                            sb.table("nova_conversations").insert(insert_data).execute()
                        )
                        _conversation_lookup_column = "id"

                    # Clean up test row
                    if ins_result.data:
                        _row_id = ins_result.data[0].get("id")
                        if _row_id:
                            sb.table("nova_conversations").delete().eq(
                                "id", _row_id
                            ).execute()
                    _schema_is_document_model = True
                    logger.info(
                        "Detected document-model schema (empty table, lookup_col=%s)",
                        _conversation_lookup_column,
                    )
                except Exception as ins_exc:
                    _schema_is_document_model = False
                    _conversation_lookup_column = "conversation_id"
                    logger.info(
                        "Detected row-per-turn schema (empty table, insert test "
                        "failed: %s)",
                        ins_exc,
                    )

        except Exception as exc:
            logger.error(
                "Schema detection failed: %s; assuming old schema", exc, exc_info=True
            )
            _schema_is_document_model = False
            _conversation_lookup_column = "conversation_id"
            _log_persistence_error("_detect_schema", "N/A", repr(exc))

        return _schema_is_document_model


def reset_schema_cache() -> None:
    """Reset the schema detection cache so next operation re-detects.

    Call this after running the migration SQL to switch from old to new schema
    without restarting the server.
    """
    global _schema_is_document_model, _conversation_lookup_column
    with _schema_detection_lock:
        _schema_is_document_model = None
        _conversation_lookup_column = "conversation_id"
    logger.info("Schema detection cache reset; will re-detect on next operation")


# ---------------------------------------------------------------------------
# Conversation Operations
# ---------------------------------------------------------------------------


def create_conversation(
    user_id: str,
    title: str = "New Chat",
    theme: str = "dark",
    avatar_style: str = "default",
) -> Optional[Dict[str, Any]]:
    """Create a new conversation.

    Args:
        user_id: Anonymous user ID (from localStorage)
        title: Conversation title
        theme: 'dark' or 'light'
        avatar_style: Avatar persona style

    Returns:
        Conversation dict with id, or None on error
    """
    if not user_id or not isinstance(user_id, str):
        logger.error("Invalid user_id: %s", user_id)
        return None

    sb = _get_supabase()
    if not sb:
        logger.warning("Supabase unavailable; cannot create conversation")
        return None

    if not _detect_schema():
        # Old row-per-turn schema -- cannot create document-model conversations
        logger.warning("Old schema detected; create_conversation is a no-op")
        return {
            "id": str(uuid.uuid4()),
            "user_id": user_id,
            "title": title,
            "messages": [],
        }

    try:
        result = (
            sb.table("nova_conversations")
            .insert(
                {
                    "user_id": user_id,
                    "title": title,
                    "theme": theme,
                    "avatar_style": avatar_style,
                    "messages": [],
                }
            )
            .execute()
        )

        if result.data:
            logger.info("Created conversation: %s", result.data[0].get("id"))
            return result.data[0]

        logger.error("Failed to create conversation: empty result")
        return None

    except Exception as e:
        logger.error("Error creating conversation: %s", e, exc_info=True)
        return None


def get_conversation(conversation_id: str) -> Optional[Dict[str, Any]]:
    """Get conversation by ID.

    Handles both old (row-per-turn) and new (document-model) schemas.

    Args:
        conversation_id: UUID of conversation

    Returns:
        Conversation dict, or None if not found
    """
    sb = _get_supabase()
    if not sb:
        return None

    # Old schema: build a synthetic conversation from rows
    if not _detect_schema():
        try:
            result = (
                sb.table("nova_conversations")
                .select("*")
                .eq("conversation_id", conversation_id)
                .order("timestamp", desc=False)
                .execute()
            )
            if not result.data:
                return None
            messages = load_conversation_messages(conversation_id)
            first_row = result.data[0]
            return {
                "id": conversation_id,
                "user_id": "anonymous",
                "title": (first_row.get("user_message") or "New Chat")[:100],
                "messages": messages,
                "created_at": first_row.get("timestamp") or "",
                "updated_at": result.data[-1].get("timestamp") or "",
            }
        except Exception as exc:
            logger.error(
                "get_conversation(%s) old-schema failed: %s",
                conversation_id,
                exc,
                exc_info=True,
            )
            return None

    try:
        _col = _conversation_lookup_column
        result = (
            sb.table("nova_conversations")
            .select("*")
            .eq(_col, conversation_id)
            .limit(1)
            .execute()
        )
        return result.data[0] if result.data else None
    except Exception as e:
        logger.error(
            "Error fetching conversation %s (col=%s): %s",
            conversation_id,
            _conversation_lookup_column,
            e,
            exc_info=True,
        )
        _log_persistence_error("get_conversation", conversation_id, repr(e))
        return None


def list_conversations(user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """List conversations for a user.

    Args:
        user_id: Anonymous user ID
        limit: Max conversations to return

    Returns:
        List of conversation dicts (sorted by recency)
    """
    sb = _get_supabase()
    if not sb:
        return []

    # Old schema: no user_id column, return empty
    if not _detect_schema():
        logger.info("Old schema: list_conversations not supported")
        return []

    try:
        result = (
            sb.table("nova_conversations")
            .select("*")
            .eq("user_id", user_id)
            .order("updated_at", desc=True)
            .limit(limit)
            .execute()
        )
        return result.data or []
    except Exception as e:
        logger.error(
            "Error listing conversations for user %s: %s", user_id, e, exc_info=True
        )
        return []


def update_conversation(
    conversation_id: str,
    **kwargs: Any,
) -> Optional[Dict[str, Any]]:
    """Update conversation fields.

    Args:
        conversation_id: UUID of conversation
        **kwargs: Fields to update (title, messages, theme, avatar_style, etc.)

    Returns:
        Updated conversation dict, or None on error
    """
    sb = _get_supabase()
    if not sb:
        return None

    # Validate input
    allowed_fields = {
        "title",
        "messages",
        "theme",
        "avatar_style",
        "shared_link_id",
        "session_token",
    }
    for key in kwargs:
        if key not in allowed_fields:
            logger.warning("Ignoring invalid field: %s", key)
            kwargs.pop(key, None)

    if not kwargs:
        logger.warning("No valid fields to update")
        return None

    try:
        _col = _conversation_lookup_column
        result = (
            sb.table("nova_conversations")
            .update(kwargs)
            .eq(_col, conversation_id)
            .execute()
        )
        return result.data[0] if result.data else None
    except Exception as e:
        logger.error(
            "Error updating conversation %s (col=%s): %s",
            conversation_id,
            _conversation_lookup_column,
            e,
            exc_info=True,
        )
        _log_persistence_error("update_conversation", conversation_id, repr(e))
        return None


def delete_conversation(conversation_id: str) -> bool:
    """Delete a conversation and all associated data.

    Args:
        conversation_id: UUID of conversation

    Returns:
        True if successful, False otherwise
    """
    sb = _get_supabase()
    if not sb:
        return False

    try:
        _col = _conversation_lookup_column
        sb.table("nova_conversations").delete().eq(_col, conversation_id).execute()
        logger.info("Deleted conversation: %s", conversation_id)
        return True
    except Exception as e:
        logger.error(
            "Error deleting conversation %s: %s", conversation_id, e, exc_info=True
        )
        _log_persistence_error("delete_conversation", conversation_id, repr(e))
        return False


# ---------------------------------------------------------------------------
# Conversation-level locks for thread-safe message appending
# ---------------------------------------------------------------------------
_conversation_locks: Dict[str, threading.Lock] = {}
_conversation_locks_guard = threading.Lock()

# Retry queue for failed writes (drained on next successful write)
_retry_queue: List[Dict[str, Any]] = []
_retry_queue_lock = threading.Lock()

MAX_RETRY_QUEUE = 200  # cap to avoid unbounded memory growth


def _get_conversation_lock(conversation_id: str) -> threading.Lock:
    """Get or create a per-conversation lock (thread-safe).

    Args:
        conversation_id: UUID of conversation.

    Returns:
        Lock object for the given conversation.
    """
    with _conversation_locks_guard:
        if conversation_id not in _conversation_locks:
            _conversation_locks[conversation_id] = threading.Lock()
        return _conversation_locks[conversation_id]


def _retry_with_backoff(
    fn: Any,
    max_retries: int = 1,
    delay_seconds: float = 1.0,
) -> Any:
    """Execute fn(), retrying once on failure after a delay.

    Args:
        fn: Callable to execute.
        max_retries: Number of retries after initial failure.
        delay_seconds: Seconds to wait between retries.

    Returns:
        Result of fn() on success, or raises the last exception.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1 + max_retries):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                logger.warning(
                    "Supabase write failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1,
                    1 + max_retries,
                    delay_seconds,
                    exc,
                )
                time.sleep(delay_seconds)
    raise last_exc  # type: ignore[misc]


def get_or_create_conversation(
    conversation_id: str,
    user_id: str = "anonymous",
    title: str = "New Chat",
) -> Optional[Dict[str, Any]]:
    """Get an existing conversation or create one with the given ID.

    The widget sends a session-generated conversation_id.  If a row with
    that ID already exists we return it; otherwise we INSERT a new row
    using that ID so both app.py and nova_persistence.py share the same
    document.

    Handles both old (row-per-turn) and new (document-model) schemas.

    Args:
        conversation_id: UUID string (caller-generated or from the widget).
        user_id: Anonymous user identifier.
        title: Conversation title for new conversations.

    Returns:
        Conversation dict, or None on error.
    """
    sb = _get_supabase()
    if not sb:
        return None

    # Old schema fallback: return a synthetic conversation dict
    if not _detect_schema():
        logger.info(
            "Old schema: returning synthetic conversation for %s", conversation_id
        )
        return {
            "id": conversation_id,
            "user_id": user_id,
            "title": title,
            "messages": [],
        }

    try:
        _col = _conversation_lookup_column
        result = (
            sb.table("nova_conversations")
            .select("*")
            .eq(_col, conversation_id)
            .order("id", desc=True)
            .limit(1)
            .execute()
        )
        if result.data:
            return result.data[0]

        # Does not exist -- create a new row.
        # Build row_data using the detected lookup column so the INSERT
        # succeeds regardless of whether the table has 'conversation_id'
        # (old + S37) or only 'id' (001 migration).
        new_token = token_hex(32)
        row_data: Dict[str, Any] = {
            "user_id": user_id,
            "title": title,
            "messages": [],
        }
        # If the table uses a 'conversation_id' TEXT column, set it.
        # If it uses 'id' UUID, do NOT set it (let DB auto-generate).
        if _col == "conversation_id":
            row_data["conversation_id"] = conversation_id

        def _insert_with_token() -> Any:
            return (
                sb.table("nova_conversations")
                .insert({**row_data, "session_token": new_token})
                .execute()
            )

        def _insert_without_token() -> Any:
            return sb.table("nova_conversations").insert(row_data).execute()

        # Try with session_token first; fall back if column doesn't exist
        try:
            insert_result = _retry_with_backoff(_insert_with_token)
        except Exception:
            logger.info("session_token column may not exist; retrying without it")
            insert_result = _retry_with_backoff(_insert_without_token)

        if insert_result.data:
            logger.info(
                "Created conversation (col=%s) cid=%s row_id=%s",
                _col,
                conversation_id,
                insert_result.data[0].get("id"),
            )
            return insert_result.data[0]

        _log_persistence_error(
            "get_or_create_conversation", conversation_id, "empty insert result"
        )
        return None

    except Exception as exc:
        logger.error(
            "get_or_create_conversation(%s) failed (col=%s): %s",
            conversation_id,
            _conversation_lookup_column,
            exc,
            exc_info=True,
        )
        _log_persistence_error("get_or_create_conversation", conversation_id, repr(exc))
        return None


def verify_conversation_token(
    conversation_id: str,
    session_token: str,
) -> bool:
    """Verify that the session_token matches the conversation's stored token.

    If the conversation has no session_token stored (legacy), allow access.

    Args:
        conversation_id: UUID of the conversation.
        session_token: Token from the client.

    Returns:
        True if token matches or no token is set on the conversation.
    """
    if not session_token:
        return False

    sb = _get_supabase()
    if not sb:
        return True  # Allow if persistence unavailable

    try:
        _col = _conversation_lookup_column
        result = (
            sb.table("nova_conversations")
            .select("session_token")
            .eq(_col, conversation_id)
            .execute()
        )
        if not result.data:
            return True  # Conversation doesn't exist yet; allow creation

        stored_token = result.data[0].get("session_token") or ""
        if not stored_token:
            return True  # Legacy conversation without token

        return stored_token == session_token
    except Exception as exc:
        logger.error(
            "verify_conversation_token(%s) failed: %s",
            conversation_id,
            exc,
            exc_info=True,
        )
        return True  # Fail open if verification itself errors


def _append_message_old_schema(
    conversation_id: str,
    role: str,
    content: str,
    model_used: str = "",
    sources: Optional[List[Any]] = None,
    confidence: float = 0.0,
) -> bool:
    """Append a message using the old row-per-turn schema.

    Inserts user+assistant as paired rows. Only the assistant role
    triggers an insert (with the last user message cached in _pending_user_messages).

    Args:
        conversation_id: Conversation identifier.
        role: 'user' or 'assistant'.
        content: Message text.
        model_used: LLM model string.
        sources: Data sources list.
        confidence: Confidence score.

    Returns:
        True on success.
    """
    sb = _get_supabase()
    if not sb:
        return False

    # For the old schema, we store user+assistant as a single row
    # Cache user messages and write when assistant responds
    if role == "user":
        _pending_user_messages[conversation_id] = content[:10000]
        return True

    if role == "assistant":
        user_msg = _pending_user_messages.pop(conversation_id, "")
        sources_str = json.dumps(sources) if sources else "[]"
        try:
            sb.table("nova_conversations").insert(
                {
                    "conversation_id": conversation_id,
                    "user_message": user_msg,
                    "assistant_response": content[:10000],
                    "model_used": model_used or "",
                    "sources": sources_str,
                    "confidence": confidence or 0.0,
                }
            ).execute()
            logger.info("Saved turn to old schema for %s", conversation_id)
            return True
        except Exception as exc:
            logger.error(
                "Old-schema insert failed for %s: %s",
                conversation_id,
                exc,
                exc_info=True,
            )
            return False

    return False


# Cache for user messages when using old schema (paired writes)
_pending_user_messages: Dict[str, str] = {}


def append_message(
    conversation_id: str,
    role: str,
    content: str,
    *,
    user_id: str = "anonymous",
    model_used: str = "",
    sources: Optional[List[Any]] = None,
    confidence: float = 0.0,
    message_id: str = "",
) -> bool:
    """Append a message to a conversation's messages JSONB array.

    Thread-safe: acquires a per-conversation lock so concurrent
    appends to the same conversation are serialised.

    On failure the message is queued in ``_retry_queue`` so a
    subsequent successful call can drain it.

    Handles both old (row-per-turn) and new (document-model) schemas.

    Args:
        conversation_id: UUID of the conversation.
        role: 'user' or 'assistant'.
        content: Message text (truncated to 10 000 chars).
        user_id: Anonymous user ID (used only if conversation must be created).
        model_used: LLM provider/model string.
        sources: Data sources used for the response.
        confidence: Confidence score (0.0 -- 1.0).
        message_id: Optional unique message identifier.

    Returns:
        True on success, False on failure (message queued for retry).
    """
    sb = _get_supabase()
    if not sb:
        return False

    # Old schema fallback: use row-per-turn inserts
    if not _detect_schema():
        return _append_message_old_schema(
            conversation_id,
            role,
            content,
            model_used=model_used,
            sources=sources,
            confidence=confidence,
        )

    msg_obj: Dict[str, Any] = {
        "role": role,
        "content": content[:10000],
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }
    if model_used:
        msg_obj["model_used"] = model_used
    if sources:
        msg_obj["sources"] = sources
    if confidence:
        msg_obj["confidence"] = confidence
    if message_id:
        msg_obj["message_id"] = message_id

    lock = _get_conversation_lock(conversation_id)

    try:
        with lock:
            # Ensure the conversation row exists
            conv = get_or_create_conversation(conversation_id, user_id=user_id)
            if conv is None:
                _log_persistence_error(
                    "append_message",
                    conversation_id,
                    f"get_or_create returned None (col={_conversation_lookup_column})",
                )
                raise RuntimeError(
                    f"Could not get or create conversation {conversation_id}"
                )

            existing_messages: List[Dict[str, Any]] = conv.get("messages") or []
            existing_messages.append(msg_obj)

            # Update by primary key 'id' (works for both integer and UUID PKs)
            _conv_row_id = conv.get("id")

            def _update() -> Any:
                return (
                    sb.table("nova_conversations")
                    .update({"messages": existing_messages})
                    .eq("id", _conv_row_id)
                    .execute()
                )

            _retry_with_backoff(_update)

        # On success, try to drain any queued messages
        _drain_retry_queue()
        return True

    except Exception as exc:
        logger.error(
            "append_message(%s, %s) failed (col=%s), queueing for retry: %s",
            conversation_id,
            role,
            _conversation_lookup_column,
            exc,
            exc_info=True,
        )
        _log_persistence_error("append_message", conversation_id, repr(exc))
        _enqueue_retry(conversation_id, msg_obj, user_id)
        return False


def _enqueue_retry(
    conversation_id: str,
    msg_obj: Dict[str, Any],
    user_id: str,
) -> None:
    """Add a failed message to the retry queue.

    Args:
        conversation_id: Target conversation UUID.
        msg_obj: The message dict that failed to persist.
        user_id: User identifier.
    """
    with _retry_queue_lock:
        if len(_retry_queue) < MAX_RETRY_QUEUE:
            _retry_queue.append(
                {
                    "conversation_id": conversation_id,
                    "msg": msg_obj,
                    "user_id": user_id,
                }
            )
        else:
            logger.warning(
                "Retry queue full (%d items), dropping message for %s",
                MAX_RETRY_QUEUE,
                conversation_id,
            )


def _drain_retry_queue() -> None:
    """Attempt to flush queued messages.  Called after a successful write."""
    with _retry_queue_lock:
        items = list(_retry_queue)
        _retry_queue.clear()

    if not items:
        return

    sb = _get_supabase()
    if not sb:
        # Put them back
        with _retry_queue_lock:
            _retry_queue.extend(items)
        return

    for item in items:
        cid = item["conversation_id"]
        msg = item["msg"]
        uid = item["user_id"]
        try:
            lock = _get_conversation_lock(cid)
            with lock:
                conv = get_or_create_conversation(cid, user_id=uid)
                if conv is None:
                    raise RuntimeError(f"Cannot get conversation {cid}")
                msgs = conv.get("messages") or []
                msgs.append(msg)
                _drain_col = _conversation_lookup_column
                _drain_pk = conv.get("id")
                (
                    sb.table("nova_conversations")
                    .update({"messages": msgs})
                    .eq("id", _drain_pk)
                    .execute()
                )
            logger.info("Drained retry-queue message for %s", cid)
        except Exception as exc:
            logger.error(
                "Failed to drain retry message for %s: %s", cid, exc, exc_info=True
            )
            with _retry_queue_lock:
                if len(_retry_queue) < MAX_RETRY_QUEUE:
                    _retry_queue.append(item)


def load_conversation_messages(conversation_id: str) -> List[Dict[str, Any]]:
    """Load conversation messages.

    Handles both old (row-per-turn) and new (document-model JSONB array) schemas.

    Args:
        conversation_id: UUID of conversation.

    Returns:
        List of message dicts, or empty list on error.
    """
    sb = _get_supabase()
    if not sb:
        return []

    # Old schema: query rows by conversation_id and build messages list
    if not _detect_schema():
        try:
            result = (
                sb.table("nova_conversations")
                .select("*")
                .eq("conversation_id", conversation_id)
                .order("timestamp", desc=False)
                .execute()
            )
            messages: List[Dict[str, Any]] = []
            for row in result.data or []:
                user_msg = row.get("user_message") or ""
                asst_msg = row.get("assistant_response") or ""
                ts = row.get("timestamp") or ""
                if user_msg:
                    messages.append(
                        {"role": "user", "content": user_msg, "timestamp": ts}
                    )
                if asst_msg:
                    msg_obj: Dict[str, Any] = {
                        "role": "assistant",
                        "content": asst_msg,
                        "timestamp": ts,
                    }
                    if row.get("model_used"):
                        msg_obj["model_used"] = row["model_used"]
                    sources_raw = row.get("sources") or "[]"
                    try:
                        parsed = (
                            json.loads(sources_raw)
                            if isinstance(sources_raw, str)
                            else sources_raw
                        )
                        if parsed:
                            msg_obj["sources"] = parsed
                    except (json.JSONDecodeError, TypeError):
                        pass
                    if row.get("confidence"):
                        msg_obj["confidence"] = row["confidence"]
                    messages.append(msg_obj)
            return messages
        except Exception as exc:
            logger.error(
                "load_conversation_messages(%s) old-schema failed: %s",
                conversation_id,
                exc,
                exc_info=True,
            )
            return []

    # New schema: read from JSONB messages array
    try:
        _col = _conversation_lookup_column
        result = (
            sb.table("nova_conversations")
            .select("messages")
            .eq(_col, conversation_id)
            .execute()
        )
        if result.data:
            return result.data[0].get("messages") or []
        return []
    except Exception as exc:
        logger.error(
            "load_conversation_messages(%s) failed: %s",
            conversation_id,
            exc,
            exc_info=True,
        )
        return []


def list_conversations_summary(
    user_id: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """List conversations with summary info for the sidebar.

    Handles both old (row-per-turn) and new (document-model) schemas.

    Args:
        user_id: Optional filter by user. If None, lists all.
        limit: Max rows.

    Returns:
        List of dicts with id, title, updated_at, and last_message preview.
    """
    sb = _get_supabase()
    if not sb:
        return []

    # Old schema: group rows by conversation_id
    if not _detect_schema():
        try:
            result = (
                sb.table("nova_conversations")
                .select("*")
                .order("timestamp", desc=True)
                .limit(500)
                .execute()
            )
            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for row in result.data or []:
                cid = row.get("conversation_id") or ""
                if cid:
                    grouped.setdefault(cid, []).append(row)

            summaries: List[Dict[str, Any]] = []
            for cid, turns in grouped.items():
                last_ts = max(t.get("timestamp") or "" for t in turns)
                first_msg = (
                    turns[-1].get("user_message") or "New Chat" if turns else "New Chat"
                )
                summaries.append(
                    {
                        "conversation_id": cid,
                        "title": first_msg[:100],
                        "last_message": (turns[0].get("user_message") or "")[:100],
                        "updated_at": last_ts,
                        "message_count": len(turns) * 2,
                    }
                )
            return summaries[:limit]
        except Exception as exc:
            logger.error(
                "list_conversations_summary (old schema) failed: %s", exc, exc_info=True
            )
            return []

    try:
        query = (
            sb.table("nova_conversations")
            .select("id,user_id,title,messages,updated_at")
            .order("updated_at", desc=True)
            .limit(limit)
        )
        if user_id:
            query = query.eq("user_id", user_id)

        result = query.execute()
        summaries_list: List[Dict[str, Any]] = []
        for row in result.data or []:
            messages = row.get("messages") or []
            last_user_msg = ""
            for msg in reversed(messages):
                if msg.get("role") == "user":
                    last_user_msg = (msg.get("content") or "")[:100]
                    break
            summaries_list.append(
                {
                    "conversation_id": row.get("id") or "",
                    "title": row.get("title") or "New Chat",
                    "last_message": last_user_msg,
                    "updated_at": row.get("updated_at") or "",
                    "message_count": len(messages),
                }
            )
        return summaries_list

    except Exception as exc:
        logger.error("list_conversations_summary failed: %s", exc, exc_info=True)
        return []


def migrate_row_per_turn_data() -> Dict[str, Any]:
    """One-time migration: merge row-per-turn data into the document model.

    Detects rows that have the old schema (conversation_id, user_message,
    assistant_response columns instead of messages JSONB array).  Groups
    them by conversation_id, builds the messages array, upserts the
    document-model row, then deletes the old rows.

    Safe to run multiple times -- it is a no-op when no legacy rows exist.

    Returns:
        Dict with migration stats: conversations_migrated, turns_migrated,
        errors.
    """
    sb = _get_supabase()
    if not sb:
        return {"error": "Supabase unavailable"}

    stats: Dict[str, Any] = {
        "conversations_migrated": 0,
        "turns_migrated": 0,
        "errors": [],
    }

    try:
        # Detect legacy rows: they have a 'user_message' key (old schema)
        # but the document model has 'messages' JSONB.
        # PostgREST: select rows where user_message is not null
        result = (
            sb.table("nova_conversations")
            .select("*")
            .not_.is_("user_message", "null")
            .order("timestamp", desc=False)
            .limit(5000)
            .execute()
        )

        legacy_rows = result.data or []
        if not legacy_rows:
            logger.info("No legacy row-per-turn data found; migration is a no-op")
            return stats

        # Group by conversation_id
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in legacy_rows:
            cid = row.get("conversation_id") or ""
            if cid:
                grouped.setdefault(cid, []).append(row)

        for cid, turns in grouped.items():
            try:
                # Build messages array from turns
                messages: List[Dict[str, Any]] = []
                for turn in turns:
                    ts = turn.get("timestamp") or datetime.utcnow().isoformat() + "Z"
                    user_msg = turn.get("user_message") or ""
                    asst_msg = turn.get("assistant_response") or ""
                    model = turn.get("model_used") or ""
                    sources_raw = turn.get("sources") or "[]"
                    try:
                        sources_parsed = (
                            json.loads(sources_raw)
                            if isinstance(sources_raw, str)
                            else sources_raw
                        )
                    except (json.JSONDecodeError, TypeError):
                        sources_parsed = []
                    conf = turn.get("confidence") or 0.0

                    if user_msg:
                        messages.append(
                            {"role": "user", "content": user_msg, "timestamp": ts}
                        )
                    if asst_msg:
                        asst_obj: Dict[str, Any] = {
                            "role": "assistant",
                            "content": asst_msg,
                            "timestamp": ts,
                        }
                        if model:
                            asst_obj["model_used"] = model
                        if sources_parsed:
                            asst_obj["sources"] = sources_parsed
                        if conf:
                            asst_obj["confidence"] = conf
                        messages.append(asst_obj)

                # Check if a document-model row already exists for this cid
                _lkup = _conversation_lookup_column
                existing = (
                    sb.table("nova_conversations")
                    .select("id,messages")
                    .eq(_lkup, cid)
                    .not_.is_("messages", "null")
                    .execute()
                )
                existing_row = (existing.data or [None])[0] if existing.data else None

                if existing_row and isinstance(existing_row.get("messages"), list):
                    # Merge: prepend legacy messages before existing ones
                    merged = messages + (existing_row.get("messages") or [])
                    sb.table("nova_conversations").update({"messages": merged}).eq(
                        "conversation_id", cid  # S47 fix: was .eq("id", cid)
                    ).execute()
                else:
                    # Determine a title from the first user message
                    first_msg = (
                        messages[0].get("content", "New Chat")[:100]
                        if messages
                        else "New Chat"
                    )
                    sb.table("nova_conversations").upsert(
                        {
                            "id": cid,
                            "user_id": turns[0].get("user_id") or "anonymous",
                            "title": first_msg,
                            "messages": messages,
                        }
                    ).execute()

                # Delete old row-per-turn rows
                for turn in turns:
                    row_id = turn.get("id")
                    if row_id:
                        sb.table("nova_conversations").delete().eq(
                            "id", row_id
                        ).execute()

                stats["conversations_migrated"] += 1
                stats["turns_migrated"] += len(turns)
                logger.info("Migrated conversation %s (%d turns)", cid, len(turns))

            except Exception as conv_exc:
                error_msg = f"Error migrating {cid}: {conv_exc}"
                logger.error(error_msg, exc_info=True)
                stats["errors"].append(error_msg)

    except Exception as exc:
        error_msg = f"Migration query failed: {exc}"
        logger.error(error_msg, exc_info=True)
        stats["errors"].append(error_msg)

    return stats


# ---------------------------------------------------------------------------
# Document Operations (for RAG)
# ---------------------------------------------------------------------------


def create_document(
    conversation_id: str,
    filename: str,
    file_path: str,
    content_text: Optional[str] = None,
    file_type: str = "txt",
    size_bytes: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Create a document record.

    Args:
        conversation_id: UUID of parent conversation
        filename: Original filename
        file_path: Local path or S3 key
        content_text: Extracted text (for RAG embedding)
        file_type: 'pdf', 'docx', 'txt', 'xlsx'
        size_bytes: File size in bytes

    Returns:
        Document dict with id, or None on error
    """
    sb = _get_supabase()
    if not sb:
        return None

    try:
        result = (
            sb.table("nova_documents")
            .insert(
                {
                    "conversation_id": conversation_id,
                    "filename": filename,
                    "file_path": file_path,
                    "content_text": content_text,
                    "file_type": file_type,
                    "size_bytes": size_bytes,
                    "metadata": {},
                }
            )
            .execute()
        )

        if result.data:
            logger.info("Created document: %s", result.data[0].get("id"))
            return result.data[0]

        return None
    except Exception as e:
        logger.error("Error creating document: %s", e, exc_info=True)
        return None


def list_documents(conversation_id: str) -> List[Dict[str, Any]]:
    """List documents in a conversation.

    Args:
        conversation_id: UUID of conversation

    Returns:
        List of document dicts
    """
    sb = _get_supabase()
    if not sb:
        return []

    try:
        result = (
            sb.table("nova_documents")
            .select("*")
            .eq("conversation_id", conversation_id)
            .order("uploaded_at", desc=True)
            .execute()
        )
        return result.data or []
    except Exception as e:
        logger.error("Error listing documents: %s", e, exc_info=True)
        return []


def get_document(document_id: str) -> Optional[Dict[str, Any]]:
    """Get document by ID.

    Args:
        document_id: UUID of document

    Returns:
        Document dict, or None if not found
    """
    sb = _get_supabase()
    if not sb:
        return None

    try:
        result = (
            sb.table("nova_documents")
            .select("*")
            .eq("id", document_id)
            .single()
            .execute()
        )
        return result.data if result.data else None
    except Exception as e:
        logger.error("Error fetching document %s: %s", document_id, e, exc_info=True)
        return None


def delete_document(document_id: str) -> bool:
    """Delete a document.

    Args:
        document_id: UUID of document

    Returns:
        True if successful
    """
    sb = _get_supabase()
    if not sb:
        return False

    try:
        sb.table("nova_documents").delete().eq("id", document_id).execute()
        logger.info("Deleted document: %s", document_id)
        return True
    except Exception as e:
        logger.error("Error deleting document %s: %s", document_id, e, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Share Link Operations
# ---------------------------------------------------------------------------


def create_share_link(
    conversation_id: str,
    ttl_days: Optional[int] = None,
) -> Optional[str]:
    """Create a shareable link for a conversation.

    Args:
        conversation_id: UUID of conversation
        ttl_days: Optional TTL in days (None = never expires)

    Returns:
        Share link ID (UUID), or None on error
    """
    sb = _get_supabase()
    if not sb:
        return None

    try:
        share_id = str(uuid.uuid4())
        expires_at = None
        if ttl_days:
            expires_at = (datetime.utcnow() + timedelta(days=ttl_days)).isoformat()

        # Update conversation with share link
        sb.table("nova_conversations").update({"shared_link_id": share_id}).eq(
            "id", conversation_id
        ).execute()

        # Create share record
        result = (
            sb.table("nova_shared_conversations")
            .insert(
                {
                    "share_id": share_id,
                    "conversation_id": conversation_id,
                    "expires_at": expires_at,
                    "access_count": 0,
                }
            )
            .execute()
        )

        if result.data:
            logger.info("Created share link: %s", share_id)
            return share_id

        return None
    except Exception as e:
        logger.error("Error creating share link: %s", e, exc_info=True)
        return None


def get_shared_conversation(share_id: str) -> Optional[Dict[str, Any]]:
    """Get a shared conversation.

    Args:
        share_id: Share link ID

    Returns:
        Conversation dict, or None if not found or expired
    """
    sb = _get_supabase()
    if not sb:
        return None

    try:
        # Check if share link exists and is not expired
        share = (
            sb.table("nova_shared_conversations")
            .select("*")
            .eq("share_id", share_id)
            .single()
            .execute()
        )

        if not share.data:
            logger.warning("Share link not found: %s", share_id)
            return None

        # Check expiry
        expires_at = share.data.get("expires_at")
        if expires_at and datetime.fromisoformat(expires_at) < datetime.utcnow():
            logger.warning("Share link expired: %s", share_id)
            return None

        # Get conversation
        conv_id = share.data.get("conversation_id")
        conversation = get_conversation(conv_id)

        # Increment access count
        try:
            access_count = share.data.get("access_count", 0)
            sb.table("nova_shared_conversations").update(
                {"access_count": access_count + 1}
            ).eq("share_id", share_id).execute()
        except Exception as e:
            logger.warning("Failed to update access count: %s", e)

        return conversation

    except Exception as e:
        logger.error(
            "Error fetching shared conversation %s: %s", share_id, e, exc_info=True
        )
        return None


def delete_share_link(share_id: str) -> bool:
    """Delete a share link.

    Args:
        share_id: Share link ID

    Returns:
        True if successful
    """
    sb = _get_supabase()
    if not sb:
        return False

    try:
        # Clear shared_link_id from conversation
        sb.table("nova_conversations").update({"shared_link_id": None}).eq(
            "shared_link_id", share_id
        ).execute()

        # Delete share record
        sb.table("nova_shared_conversations").delete().eq(
            "share_id", share_id
        ).execute()

        logger.info("Deleted share link: %s", share_id)
        return True
    except Exception as e:
        logger.error("Error deleting share link %s: %s", share_id, e, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Health Check
# ---------------------------------------------------------------------------


def health_check() -> bool:
    """Check if Supabase is available and responsive.

    Returns:
        True if healthy, False otherwise
    """
    sb = _get_supabase()
    if not sb:
        return False

    try:
        # Simple query to test connection -- works with both schemas
        sb.table("nova_conversations").select("id").limit(1).execute()
        return True
    except Exception as e:
        logger.error("Supabase health check failed: %s", e, exc_info=True)
        return False


def get_conversation_count() -> Dict[str, Any]:
    """Return nova_conversations row count and schema diagnostics.

    Used by the /api/health endpoint to surface P1-15 persistence status.

    Returns:
        Dict with 'row_count', 'schema', 'lookup_column', 'retry_queue_size',
        and 'error_log_path'.
    """
    sb = _get_supabase()
    result: Dict[str, Any] = {
        "row_count": -1,
        "schema": "unknown",
        "lookup_column": _conversation_lookup_column,
        "retry_queue_size": len(_retry_queue),
        "error_log_path": str(_PERSISTENCE_LOG_FILE),
    }
    if not sb:
        result["error"] = "supabase_unavailable"
        return result

    try:
        # Count rows (Supabase supports head+count but supabase-py uses select)
        count_result = (
            sb.table("nova_conversations")
            .select("id", count="exact")
            .limit(0)
            .execute()
        )
        result["row_count"] = (
            count_result.count
            if hasattr(count_result, "count") and count_result.count is not None
            else -1
        )
        # If count attribute not available, fall back to a SELECT + len
        if result["row_count"] == -1:
            fallback = sb.table("nova_conversations").select("id").limit(500).execute()
            result["row_count"] = len(fallback.data) if fallback.data else 0
    except Exception as exc:
        result["error"] = str(exc)
        logger.error("get_conversation_count failed: %s", exc, exc_info=True)

    result["schema"] = "document_model" if _schema_is_document_model else "row_per_turn"
    return result
