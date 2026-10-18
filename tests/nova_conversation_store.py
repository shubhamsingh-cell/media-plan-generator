"""In-memory stand-in for the ``nova_conversations`` table, for tests.

Supports the query chains nova_persistence issues against Supabase
(select/eq/order/limit, insert, update) so conversation lookup and token
checks run through the real persistence code.
"""

from __future__ import annotations

import copy
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import nova_memory
import nova_persistence


class _Query:
    def __init__(self, store: "FakeConversationStore") -> None:
        self._store = store
        self._op = "select"
        self._filters: List[tuple] = []
        self._payload: Optional[Dict[str, Any]] = None

    def select(self, _cols: str = "*") -> "_Query":
        self._op = "select"
        return self

    def insert(self, row: Dict[str, Any]) -> "_Query":
        self._op = "insert"
        self._payload = row
        return self

    def update(self, values: Dict[str, Any]) -> "_Query":
        self._op = "update"
        self._payload = values
        return self

    def eq(self, key: str, value: Any) -> "_Query":
        self._filters.append((key, value))
        return self

    def order(self, *_a: Any, **_k: Any) -> "_Query":
        return self

    def limit(self, _n: int) -> "_Query":
        return self

    def execute(self) -> SimpleNamespace:
        if self._store.fail_reads and self._op == "select":
            raise RuntimeError("simulated lookup failure")
        rows = self._store.rows
        if self._op == "insert":
            row = dict(self._payload or {})
            row["id"] = len(rows) + 1
            rows.append(row)
            return SimpleNamespace(data=[copy.deepcopy(row)])
        matched = [r for r in rows if all(r.get(k) == v for k, v in self._filters)]
        if self._op == "update":
            for r in matched:
                r.update(copy.deepcopy(self._payload or {}))
        return SimpleNamespace(data=copy.deepcopy(matched[-1:]))


class FakeConversationStore:
    """Rows of nova_conversations, keyed by the ``conversation_id`` column."""

    def __init__(self) -> None:
        self.rows: List[Dict[str, Any]] = []
        self.fail_reads = False

    def table(self, _name: str) -> _Query:
        return _Query(self)

    def add(self, conversation_id: str, session_token: str, messages: list) -> None:
        self.rows.append(
            {
                "id": len(self.rows) + 1,
                "conversation_id": conversation_id,
                "session_token": session_token,
                "messages": copy.deepcopy(messages),
                "user_id": "anonymous",
                "title": "t",
            }
        )

    def row(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        for r in self.rows:
            if r.get("conversation_id") == conversation_id:
                return r
        return None


def clear_rate_limits() -> None:
    """Reset the per-IP and global request limiters the chat routes consult."""
    import app as app_module

    app_module._rate_limit_store.clear()
    app_module._global_chat_timestamps.clear()
    for limiter in (app_module._rl_chat, app_module._rl_general):
        with limiter._lock:
            limiter._requests.clear()


def install(monkeypatch: Any) -> FakeConversationStore:
    """Route nova_persistence to a fresh fake store (document-model schema)."""
    store = FakeConversationStore()
    monkeypatch.setattr(nova_persistence, "_get_supabase", lambda: store)
    monkeypatch.setattr(nova_persistence, "_schema_is_document_model", True)
    monkeypatch.setattr(
        nova_persistence, "_conversation_lookup_column", "conversation_id"
    )
    return store


def seed_memory(monkeypatch: Any, conversation_id: str, fact: str) -> None:
    """Give one conversation a remembered fact (no persistence round trip)."""
    memories = type(nova_memory._conversation_memories)()
    monkeypatch.setattr(nova_memory, "_conversation_memories", memories)
    key = nova_memory.conversation_memory_key(conversation_id)
    mem = nova_memory.NovaMemory(key, persist_local=False)
    mem._loaded = True
    mem._long_term.append({"content": fact, "memory_type": "long_term"})
    memories[key] = mem


def memory_block(conversation_id: str) -> str:
    """The memory block a turn bound to ``conversation_id`` would be given."""
    key = nova_memory.conversation_memory_key(conversation_id)
    mem = nova_memory._conversation_memories.get(key) if key else None
    return mem.get_context_injection() if mem is not None else ""
