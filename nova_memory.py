#!/usr/bin/env python3
"""Persistent memory for Nova AI chatbot.

Stores conversation summaries and user preferences across sessions.
Injects relevant memory into system prompts for continuity.

Architecture:
- Short-term: Last 5 conversation summaries (injected every call)
- Long-term: User preferences, campaign history, learned facts
- Storage: Supabase nova_memory table + local fallback
"""

import json
import logging
import os
import time
import threading
from collections import deque

logger = logging.getLogger(__name__)

_MAX_SHORT_TERM = 5  # Last N conversation summaries to inject
_MAX_LONG_TERM = 50  # Max long-term memory entries
_SUMMARY_MAX_CHARS = 500  # Max chars per conversation summary


class NovaMemory:
    """Persistent memory manager for Nova AI."""

    def __init__(self, user_id: str = "default"):
        self._user_id = user_id
        self._short_term: deque = deque(maxlen=_MAX_SHORT_TERM)
        self._long_term: list = []
        self._preferences: dict = {}
        self._lock = threading.Lock()
        self._loaded = False

    def load(self) -> None:
        """Load memory from Supabase or local fallback."""
        if self._loaded:
            return

        # Try Supabase first
        try:
            from supabase_client import get_client

            client = get_client()
            if client:
                # Load conversation summaries
                result = (
                    client.table("nova_memory")
                    .select("*")
                    .eq("user_id", self._user_id)
                    .order("created_at", desc=True)
                    .limit(_MAX_SHORT_TERM + _MAX_LONG_TERM)
                    .execute()
                )

                if result and result.data:
                    for row in result.data:
                        mem_type = row.get("memory_type") or "short_term"
                        entry = {
                            "id": row.get("id"),
                            "content": row.get("content") or "",
                            "memory_type": mem_type,
                            "created_at": row.get("created_at"),
                            "metadata": row.get("metadata") or {},
                        }
                        if mem_type == "short_term":
                            self._short_term.append(entry)
                        elif mem_type == "long_term":
                            self._long_term.append(entry)
                        elif mem_type == "preference":
                            key = (row.get("metadata") or {}).get("key") or ""
                            if key:
                                self._preferences[key] = entry["content"]

                    logger.info(
                        "[NovaMemory] Loaded %d short-term, %d long-term, %d preferences from Supabase",
                        len(self._short_term),
                        len(self._long_term),
                        len(self._preferences),
                    )
                    self._loaded = True
                    return
        except Exception as e:
            logger.debug("[NovaMemory] Supabase load failed, trying local: %s", e)

        # Local fallback
        try:
            mem_file = os.path.join("data", f"nova_memory_{self._user_id}.json")
            if os.path.exists(mem_file):
                with open(mem_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for entry in data.get("short_term") or []:
                    self._short_term.append(entry)
                self._long_term = data.get("long_term") or []
                self._preferences = data.get("preferences") or {}
                logger.info("[NovaMemory] Loaded from local file")
        except Exception as e:
            logger.debug("[NovaMemory] Local load failed: %s", e)

        self._loaded = True

    def save_conversation_summary(
        self, conversation_id: str, messages: list, summary: str = ""
    ) -> None:
        """Save a conversation summary to memory.

        Args:
            conversation_id: Unique conversation ID.
            messages: List of message dicts [{role, text}].
            summary: Pre-computed summary, or auto-generate from messages.
        """
        if not summary:
            # Auto-generate summary from last few messages
            summary = self._auto_summarize(messages)

        if not summary:
            return

        entry = {
            "content": summary[:_SUMMARY_MAX_CHARS],
            "memory_type": "short_term",
            "created_at": time.time(),
            "metadata": {
                "conversation_id": conversation_id,
                "message_count": len(messages),
            },
        }

        with self._lock:
            self._short_term.append(entry)

        # Persist async
        threading.Thread(target=self._persist_entry, args=(entry,), daemon=True).start()

    def learn_fact(self, fact: str, category: str = "general") -> None:
        """Store a long-term fact learned from conversation.

        Examples: "User prefers LinkedIn for tech roles"
                  "Client Acme Corp budget is $50K/quarter"
        """
        entry = {
            "content": fact[:_SUMMARY_MAX_CHARS],
            "memory_type": "long_term",
            "created_at": time.time(),
            "metadata": {"category": category},
        }

        with self._lock:
            # Deduplicate
            existing = [e for e in self._long_term if e["content"] == fact]
            if not existing:
                self._long_term.append(entry)
                if len(self._long_term) > _MAX_LONG_TERM:
                    self._long_term = self._long_term[-_MAX_LONG_TERM:]

        threading.Thread(target=self._persist_entry, args=(entry,), daemon=True).start()

    def set_preference(self, key: str, value: str) -> None:
        """Store a user preference."""
        with self._lock:
            self._preferences[key] = value

        entry = {
            "content": value,
            "memory_type": "preference",
            "created_at": time.time(),
            "metadata": {"key": key},
        }
        threading.Thread(target=self._persist_entry, args=(entry,), daemon=True).start()

    def get_context_injection(self) -> str:
        """Get memory context to inject into LLM system prompt.

        Returns a formatted string suitable for appending to the system prompt.
        """
        parts = []

        with self._lock:
            # Preferences
            if self._preferences:
                prefs = "; ".join(f"{k}: {v}" for k, v in self._preferences.items())
                parts.append(f"User preferences: {prefs}")

            # Long-term facts
            if self._long_term:
                facts = [e["content"] for e in self._long_term[-10:]]
                parts.append("Known facts:\n- " + "\n- ".join(facts))

            # Recent conversation summaries
            if self._short_term:
                summaries = [e["content"] for e in self._short_term]
                parts.append(
                    "Recent conversation context:\n" + "\n---\n".join(summaries)
                )

        if not parts:
            return ""

        return (
            "\n\n[MEMORY - Previous sessions]\n" + "\n\n".join(parts) + "\n[/MEMORY]\n"
        )

    def _auto_summarize(self, messages: list) -> str:
        """Auto-generate a conversation summary from messages."""
        if not messages:
            return ""

        # Extract key info from last few messages
        recent = messages[-6:]  # Last 3 exchanges
        parts = []
        for msg in recent:
            role = msg.get("role") or ""
            text = (msg.get("text") or msg.get("content") or "")[:200]
            if role == "user":
                parts.append(f"User asked: {text}")
            elif role in ("nova", "assistant"):
                parts.append(f"Nova answered about: {text[:100]}")

        return " | ".join(parts)[:_SUMMARY_MAX_CHARS]

    def _persist_entry(self, entry: dict) -> None:
        """Persist a memory entry to Supabase or local file."""
        # Try Supabase
        try:
            from supabase_client import get_client

            client = get_client()
            if client:
                client.table("nova_memory").insert(
                    {
                        "user_id": self._user_id,
                        "content": entry["content"],
                        "memory_type": entry["memory_type"],
                        "metadata": json.dumps(entry.get("metadata") or {}),
                    }
                ).execute()
                return
        except Exception as e:
            logger.debug("[NovaMemory] Supabase persist failed: %s", e)

        # Local fallback
        try:
            mem_file = os.path.join("data", f"nova_memory_{self._user_id}.json")
            existing: dict = {"short_term": [], "long_term": [], "preferences": {}}
            if os.path.exists(mem_file):
                with open(mem_file, "r", encoding="utf-8") as f:
                    existing = json.load(f)

            if entry["memory_type"] == "short_term":
                existing["short_term"].append(entry)
                existing["short_term"] = existing["short_term"][-_MAX_SHORT_TERM:]
            elif entry["memory_type"] == "long_term":
                existing["long_term"].append(entry)
                existing["long_term"] = existing["long_term"][-_MAX_LONG_TERM:]
            elif entry["memory_type"] == "preference":
                key = (entry.get("metadata") or {}).get("key") or ""
                if key:
                    existing["preferences"][key] = entry["content"]

            with open(mem_file, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
        except Exception as e:
            logger.debug("[NovaMemory] Local persist failed: %s", e)

    def get_stats(self) -> dict:
        """Get memory statistics."""
        with self._lock:
            return {
                "short_term_count": len(self._short_term),
                "long_term_count": len(self._long_term),
                "preference_count": len(self._preferences),
                "loaded": self._loaded,
            }


# Singleton
_memory_instances: dict = {}
_global_lock = threading.Lock()


def get_memory(user_id: str = "default") -> NovaMemory:
    """Get or create a NovaMemory instance for a user."""
    with _global_lock:
        if user_id not in _memory_instances:
            mem = NovaMemory(user_id)
            mem.load()
            _memory_instances[user_id] = mem
        return _memory_instances[user_id]
