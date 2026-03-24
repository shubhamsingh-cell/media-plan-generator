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
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Supabase imports (graceful fallback)
try:
    from supabase import create_client

    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    logger.warning(
        "Supabase client not available; persistence features will be limited"
    )


# ---------------------------------------------------------------------------
# Thread-safe Supabase Client Singleton
# ---------------------------------------------------------------------------
_supabase_client = None
_supabase_lock = threading.Lock()


def _get_supabase():
    """Get or initialize Supabase client (lazy, thread-safe)."""
    global _supabase_client
    if _supabase_client is None:
        with _supabase_lock:
            if _supabase_client is None:
                if not SUPABASE_AVAILABLE:
                    logger.error("Supabase client not available")
                    return None

                try:
                    url = os.getenv("SUPABASE_URL") or ""
                    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
                    if not url or not key:
                        logger.error(
                            "SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set"
                        )
                        return None
                    _supabase_client = create_client(url, key)
                    logger.info("Supabase client initialized")
                except Exception as e:
                    logger.error("Failed to initialize Supabase: %s", e, exc_info=True)
                    _supabase_client = False  # sentinel: tried and failed

    return _supabase_client if _supabase_client is not False else None


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

    Args:
        conversation_id: UUID of conversation

    Returns:
        Conversation dict, or None if not found
    """
    sb = _get_supabase()
    if not sb:
        return None

    try:
        result = (
            sb.table("nova_conversations")
            .select("*")
            .eq("id", conversation_id)
            .single()
            .execute()
        )
        return result.data if result.data else None
    except Exception as e:
        logger.error(
            "Error fetching conversation %s: %s", conversation_id, e, exc_info=True
        )
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
    allowed_fields = {"title", "messages", "theme", "avatar_style", "shared_link_id"}
    for key in kwargs:
        if key not in allowed_fields:
            logger.warning("Ignoring invalid field: %s", key)
            kwargs.pop(key, None)

    if not kwargs:
        logger.warning("No valid fields to update")
        return None

    try:
        result = (
            sb.table("nova_conversations")
            .update(kwargs)
            .eq("id", conversation_id)
            .execute()
        )
        return result.data[0] if result.data else None
    except Exception as e:
        logger.error(
            "Error updating conversation %s: %s", conversation_id, e, exc_info=True
        )
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
        # This will cascade delete documents and shared links due to ON DELETE CASCADE
        sb.table("nova_conversations").delete().eq("id", conversation_id).execute()
        logger.info("Deleted conversation: %s", conversation_id)
        return True
    except Exception as e:
        logger.error(
            "Error deleting conversation %s: %s", conversation_id, e, exc_info=True
        )
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

    try:
        # Try fetching first
        result = (
            sb.table("nova_conversations")
            .select("*")
            .eq("id", conversation_id)
            .execute()
        )
        if result.data:
            return result.data[0]

        # Does not exist -- create with the explicit ID
        def _insert() -> Any:
            return (
                sb.table("nova_conversations")
                .insert(
                    {
                        "id": conversation_id,
                        "user_id": user_id,
                        "title": title,
                        "messages": [],
                    }
                )
                .execute()
            )

        insert_result = _retry_with_backoff(_insert)
        if insert_result.data:
            logger.info("Created conversation with explicit ID: %s", conversation_id)
            return insert_result.data[0]
        return None

    except Exception as exc:
        logger.error(
            "get_or_create_conversation(%s) failed: %s",
            conversation_id,
            exc,
            exc_info=True,
        )
        return None


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
                raise RuntimeError(
                    f"Could not get or create conversation {conversation_id}"
                )

            existing_messages: List[Dict[str, Any]] = conv.get("messages") or []
            existing_messages.append(msg_obj)

            def _update() -> Any:
                return (
                    sb.table("nova_conversations")
                    .update({"messages": existing_messages})
                    .eq("id", conversation_id)
                    .execute()
                )

            _retry_with_backoff(_update)

        # On success, try to drain any queued messages
        _drain_retry_queue()
        return True

    except Exception as exc:
        logger.error(
            "append_message(%s, %s) failed, queueing for retry: %s",
            conversation_id,
            role,
            exc,
            exc_info=True,
        )
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
                (
                    sb.table("nova_conversations")
                    .update({"messages": msgs})
                    .eq("id", cid)
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
    """Load conversation messages from the JSONB array.

    Args:
        conversation_id: UUID of conversation.

    Returns:
        List of message dicts, or empty list on error.
    """
    sb = _get_supabase()
    if not sb:
        return []

    try:
        result = (
            sb.table("nova_conversations")
            .select("messages")
            .eq("id", conversation_id)
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

    Args:
        user_id: Optional filter by user. If None, lists all.
        limit: Max rows.

    Returns:
        List of dicts with id, title, updated_at, and last_message preview.
    """
    sb = _get_supabase()
    if not sb:
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
        summaries: List[Dict[str, Any]] = []
        for row in result.data or []:
            messages = row.get("messages") or []
            last_user_msg = ""
            for msg in reversed(messages):
                if msg.get("role") == "user":
                    last_user_msg = (msg.get("content") or "")[:100]
                    break
            summaries.append(
                {
                    "conversation_id": row.get("id") or "",
                    "title": row.get("title") or "New Chat",
                    "last_message": last_user_msg,
                    "updated_at": row.get("updated_at") or "",
                    "message_count": len(messages),
                }
            )
        return summaries

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
                existing = (
                    sb.table("nova_conversations")
                    .select("id,messages")
                    .eq("id", cid)
                    .not_.is_("messages", "null")
                    .execute()
                )
                existing_row = (existing.data or [None])[0] if existing.data else None

                if existing_row and isinstance(existing_row.get("messages"), list):
                    # Merge: prepend legacy messages before existing ones
                    merged = messages + (existing_row.get("messages") or [])
                    sb.table("nova_conversations").update({"messages": merged}).eq(
                        "id", cid
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
# Avatar Operations
# ---------------------------------------------------------------------------


def list_avatars() -> List[Dict[str, Any]]:
    """List all available avatars.

    Returns:
        List of avatar dicts
    """
    sb = _get_supabase()
    if not sb:
        return []

    try:
        result = sb.table("nova_avatars").select("*").order("created_at").execute()
        return result.data or []
    except Exception as e:
        logger.error("Error listing avatars: %s", e, exc_info=True)
        return []


def create_avatar(
    persona_name: str,
    style: str,
    image_url: Optional[str] = None,
    color: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Create a new avatar.

    Args:
        persona_name: Name of persona (e.g., "Assistant", "Expert")
        style: 'ai-generated', 'gradient', 'emoji', 'initials'
        image_url: URL to avatar image
        color: Hex color for gradient/initials

    Returns:
        Avatar dict, or None on error
    """
    sb = _get_supabase()
    if not sb:
        return None

    try:
        result = (
            sb.table("nova_avatars")
            .insert(
                {
                    "persona_name": persona_name,
                    "style": style,
                    "image_url": image_url,
                    "color": color,
                    "metadata": {},
                }
            )
            .execute()
        )

        if result.data:
            logger.info("Created avatar: %s", persona_name)
            return result.data[0]

        return None
    except Exception as e:
        logger.error("Error creating avatar: %s", e, exc_info=True)
        return None


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
        # Simple query to test connection
        sb.table("nova_conversations").select("count").limit(1).execute()
        return True
    except Exception as e:
        logger.error("Supabase health check failed: %s", e, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Module Usage Tracking (v4.0)
# ---------------------------------------------------------------------------
#
# Table schema (run via Supabase SQL editor):
#
# CREATE TABLE IF NOT EXISTS nova_module_usage (
#     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
#     module_name TEXT NOT NULL,
#     action TEXT NOT NULL,
#     user_id TEXT NOT NULL DEFAULT 'anonymous',
#     timestamp TIMESTAMPTZ DEFAULT now(),
#     latency_ms FLOAT DEFAULT 0,
#     success BOOLEAN DEFAULT true,
#     metadata JSONB DEFAULT '{}'::jsonb,
#     created_at TIMESTAMPTZ DEFAULT now()
# );
# CREATE INDEX idx_module_usage_module ON nova_module_usage(module_name);
# CREATE INDEX idx_module_usage_timestamp ON nova_module_usage(timestamp);
# CREATE INDEX idx_module_usage_user ON nova_module_usage(user_id);


def track_module_usage(
    module_name: str,
    action: str,
    user_id: str = "anonymous",
    latency_ms: float = 0.0,
    success: bool = True,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Track a module usage event in Supabase.

    Fire-and-forget: errors are logged but do not propagate.

    Args:
        module_name: Module identifier (command_center, intelligence_hub, nova_ai).
        action: Action performed (e.g., 'generate_plan', 'chat_message', 'web_search').
        user_id: Anonymous user identifier.
        latency_ms: Request latency in milliseconds.
        success: Whether the action succeeded.
        metadata: Optional JSONB metadata (model used, sources, etc.).

    Returns:
        True if tracking succeeded, False otherwise.
    """
    sb = _get_supabase()
    if not sb:
        return False

    try:
        row = {
            "module_name": module_name,
            "action": action,
            "user_id": user_id or "anonymous",
            "latency_ms": round(latency_ms, 1),
            "success": success,
            "metadata": metadata or {},
        }
        sb.table("nova_module_usage").insert(row).execute()
        return True
    except Exception as e:
        logger.error("Failed to track module usage: %s", e, exc_info=True)
        return False


# ---------------------------------------------------------------------------
# Campaign Context Persistence (v4.0)
# ---------------------------------------------------------------------------
#
# Table schema (run via Supabase SQL editor):
#
# CREATE TABLE IF NOT EXISTS nova_campaigns (
#     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
#     user_id TEXT NOT NULL DEFAULT 'anonymous',
#     campaign_name TEXT NOT NULL DEFAULT 'Untitled Campaign',
#     campaign_data JSONB NOT NULL DEFAULT '{}'::jsonb,
#     module_source TEXT NOT NULL DEFAULT 'command_center',
#     status TEXT NOT NULL DEFAULT 'draft',
#     created_at TIMESTAMPTZ DEFAULT now(),
#     updated_at TIMESTAMPTZ DEFAULT now()
# );
# CREATE INDEX idx_campaigns_user ON nova_campaigns(user_id);
# CREATE INDEX idx_campaigns_status ON nova_campaigns(status);


def save_campaign(
    user_id: str,
    campaign_name: str,
    campaign_data: Dict[str, Any],
    module_source: str = "command_center",
    status: str = "draft",
    campaign_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Save or update a campaign context to Supabase.

    Args:
        user_id: User identifier.
        campaign_name: Human-readable campaign name.
        campaign_data: Full campaign data dict (from NovaContext).
        module_source: Which module created this (command_center, nova_ai, etc.).
        status: Campaign status (draft, active, completed, archived).
        campaign_id: Optional UUID to update an existing campaign.

    Returns:
        Campaign dict with id, or None on error.
    """
    sb = _get_supabase()
    if not sb:
        return None

    try:
        row = {
            "user_id": user_id or "anonymous",
            "campaign_name": campaign_name or "Untitled Campaign",
            "campaign_data": campaign_data,
            "module_source": module_source,
            "status": status,
        }

        if campaign_id:
            result = (
                sb.table("nova_campaigns").update(row).eq("id", campaign_id).execute()
            )
        else:
            result = sb.table("nova_campaigns").insert(row).execute()

        if result.data:
            logger.info("Saved campaign: %s", result.data[0].get("id"))
            return result.data[0]
        return None
    except Exception as e:
        logger.error("Failed to save campaign: %s", e, exc_info=True)
        return None


def list_campaigns(
    user_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 50,
) -> List[Dict[str, Any]]:
    """List campaigns, optionally filtered by user and status.

    Args:
        user_id: Optional filter by user.
        status: Optional filter by status (draft, active, completed).
        limit: Max campaigns to return.

    Returns:
        List of campaign dicts.
    """
    sb = _get_supabase()
    if not sb:
        return []

    try:
        query = (
            sb.table("nova_campaigns")
            .select(
                "id,user_id,campaign_name,module_source,status,created_at,updated_at"
            )
            .order("updated_at", desc=True)
            .limit(limit)
        )
        if user_id:
            query = query.eq("user_id", user_id)
        if status:
            query = query.eq("status", status)

        result = query.execute()
        return result.data or []
    except Exception as e:
        logger.error("Failed to list campaigns: %s", e, exc_info=True)
        return []


def get_campaign(campaign_id: str) -> Optional[Dict[str, Any]]:
    """Get a campaign by ID.

    Args:
        campaign_id: UUID of the campaign.

    Returns:
        Campaign dict, or None if not found.
    """
    sb = _get_supabase()
    if not sb:
        return None

    try:
        result = (
            sb.table("nova_campaigns")
            .select("*")
            .eq("id", campaign_id)
            .single()
            .execute()
        )
        return result.data if result.data else None
    except Exception as e:
        logger.error("Failed to get campaign %s: %s", campaign_id, e, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Analytics Aggregation Functions (v4.0)
# ---------------------------------------------------------------------------


def get_daily_active_users_per_module(days: int = 7) -> Dict[str, int]:
    """Get count of distinct active users per module over the last N days.

    Uses the nova_module_usage table.

    Args:
        days: Look-back window in days.

    Returns:
        Dict mapping module_name to distinct user count.
    """
    sb = _get_supabase()
    if not sb:
        return {}

    try:
        from datetime import timedelta

        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"

        result = {}
        for module in ("command_center", "intelligence_hub", "nova_ai"):
            rows = (
                sb.table("nova_module_usage")
                .select("user_id")
                .eq("module_name", module)
                .gte("timestamp", cutoff)
                .execute()
            )
            unique_users = set()
            for row in rows.data or []:
                uid = row.get("user_id") or ""
                if uid and uid != "anonymous":
                    unique_users.add(uid)
            result[module] = len(unique_users)

        return result
    except Exception as e:
        logger.error("Failed to get DAU per module: %s", e, exc_info=True)
        return {}


def get_most_used_features(limit: int = 10) -> List[Dict[str, Any]]:
    """Get most frequently used actions across all modules.

    Args:
        limit: Number of top features to return.

    Returns:
        List of dicts with module_name, action, and usage_count.
    """
    sb = _get_supabase()
    if not sb:
        return []

    try:
        # Query all recent usage and aggregate in Python
        rows = (
            sb.table("nova_module_usage")
            .select("module_name,action")
            .order("timestamp", desc=True)
            .limit(5000)
            .execute()
        )

        counts: Dict[str, int] = {}
        for row in rows.data or []:
            key = f"{row.get('module_name') or 'unknown'}:{row.get('action') or 'unknown'}"
            counts[key] = counts.get(key, 0) + 1

        sorted_features = sorted(counts.items(), key=lambda x: x[1], reverse=True)[
            :limit
        ]
        result = []
        for key, count in sorted_features:
            parts = key.split(":", 1)
            result.append(
                {
                    "module_name": parts[0],
                    "action": parts[1] if len(parts) > 1 else "unknown",
                    "usage_count": count,
                }
            )
        return result
    except Exception as e:
        logger.error("Failed to get most used features: %s", e, exc_info=True)
        return []


def get_error_rates_by_module(days: int = 7) -> Dict[str, Dict[str, Any]]:
    """Get error rates per module over the last N days.

    Args:
        days: Look-back window in days.

    Returns:
        Dict mapping module_name to {total, errors, error_rate_pct}.
    """
    sb = _get_supabase()
    if not sb:
        return {}

    try:
        from datetime import timedelta

        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat() + "Z"

        result = {}
        for module in ("command_center", "intelligence_hub", "nova_ai"):
            rows = (
                sb.table("nova_module_usage")
                .select("success")
                .eq("module_name", module)
                .gte("timestamp", cutoff)
                .execute()
            )
            total = len(rows.data or [])
            errors = sum(1 for r in (rows.data or []) if not r.get("success", True))
            error_rate = (errors / max(1, total)) * 100
            result[module] = {
                "total": total,
                "errors": errors,
                "error_rate_pct": round(error_rate, 2),
            }

        return result
    except Exception as e:
        logger.error("Failed to get error rates by module: %s", e, exc_info=True)
        return {}
