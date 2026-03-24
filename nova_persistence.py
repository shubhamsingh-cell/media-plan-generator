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
