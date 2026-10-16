"""Nova memory is scoped to one conversation, never a shared bucket.

Before: every chat called ``get_memory()`` with no argument, so one shared
"default" instance (Supabase ``nova_memory`` rows / data/nova_memory_default.json)
collected every user's summaries and "User said: ..." facts, and every new
chat's system prompt got them back as "[MEMORY - Previous sessions]".
"""

from __future__ import annotations

import re
import threading
from collections import OrderedDict
from pathlib import Path
from unittest.mock import patch

import nova
import nova_memory

ROOT = Path(__file__).resolve().parent.parent


def _isolated():
    """Patch storage away and give the test fresh memory registries."""
    return (
        patch.object(nova_memory.NovaMemory, "load", lambda self: None),
        patch.object(nova_memory.NovaMemory, "_persist_entry", lambda self, e: None),
        patch.object(nova_memory, "_conversation_memories", OrderedDict()),
        patch.object(nova_memory, "_memory_instances", {}),
    )


class _Isolated:
    def __enter__(self):
        self._ps = _isolated()
        for p in self._ps:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in self._ps:
            p.stop()


def _run_turn(conv_id, message, response="Here is an answer.") -> bool:
    return nova._save_conversation_memory(conv_id, [], message, response)


def _inject_for(conv_id) -> str:
    nova._set_turn_tool_capture({"conversation_id": conv_id or "", "tools": {}})
    try:
        return nova._conversation_memory_context()
    finally:
        nova._set_turn_tool_capture(None)


def test_two_conversations_never_see_each_others_facts():
    with _Isolated():
        _run_turn("conv-aaa", "My budget is 40000 for Acme Health nurses")
        _run_turn("conv-bbb", "Hiring warehouse associates in Dallas, TX")
        a = _inject_for("conv-aaa")
        b = _inject_for("conv-bbb")
    assert "Acme Health" in a and "Dallas" not in a
    assert "Dallas" in b and "Acme Health" not in b


def test_request_without_id_neither_saves_nor_injects():
    with _Isolated():
        assert _run_turn("", "My budget is 40000 for Acme Health") is False
        assert _run_turn(None, "I work at Acme Health") is False
        assert nova_memory._conversation_memories == {}
        assert nova_memory._memory_instances == {}
        # Even after a keyed conversation saved a fact, an id-less turn sees nothing.
        _run_turn("conv-aaa", "My budget is 40000 for Acme Health")
        assert _inject_for("") == ""
        assert _inject_for(None) == ""


def test_get_conversation_memory_is_none_without_id_and_stable_with_one():
    with _Isolated():
        assert nova_memory.get_conversation_memory("") is None
        assert nova_memory.get_conversation_memory(None) is None
        assert nova_memory.get_conversation_memory("   ") is None
        m1 = nova_memory.get_conversation_memory("conv-aaa")
        assert m1 is nova_memory.get_conversation_memory("conv-aaa")
        assert m1 is not nova_memory.get_conversation_memory("conv-bbb")


def test_client_id_is_hashed_and_cannot_reach_legacy_default_bucket():
    key = nova_memory.conversation_memory_key("default")
    assert key.startswith("conv_") and key != "default"
    traversal = nova_memory.conversation_memory_key("../../etc/passwd")
    assert re.fullmatch(r"conv_[0-9a-f]{32}", traversal)


def test_conversation_memory_never_writes_local_json(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data").mkdir()
    mem = nova_memory.NovaMemory("conv_x", persist_local=False)
    with patch("supabase_client.get_client", return_value=None):
        mem._persist_entry(
            {"content": "x", "memory_type": "long_term", "metadata": {}}
        )
    assert list((tmp_path / "data").iterdir()) == []


def test_conversation_registry_is_lru_bounded_and_thread_safe():
    with _Isolated():
        with patch.object(nova_memory, "_MAX_CONVERSATION_MEMORIES", 20):
            threads = [
                threading.Thread(
                    target=lambda i=i: nova_memory.get_conversation_memory(f"c{i}")
                )
                for i in range(60)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            assert len(nova_memory._conversation_memories) <= 20


def test_no_zero_arg_shared_get_memory_calls_remain():
    src = (ROOT / "nova.py").read_text(encoding="utf-8")
    assert not re.search(r"\bget_memory\(\s*\)", src)
    sig = nova_memory.get_memory.__code__
    assert sig.co_argcount == 1 and not nova_memory.get_memory.__defaults__


def test_profile_injection_never_uses_shared_default_profile():
    nova_memory.get_user_profile("default").update("nurses in Boston", [])
    try:
        nova._set_turn_tool_capture(None)
        assert nova._inject_user_profile_context([], "") == ""
    finally:
        nova_memory._profile_instances.pop("default", None)


class _StubIQ:
    def chat(self, user_message, **kwargs):
        return {
            "response": f"Answer about {user_message[:30]}",
            "sources": [],
            "confidence": 0.9,
            "tools_used": [],
        }


def _all_memory_instances():
    found = list(getattr(nova_memory, "_memory_instances", {}).values())
    found += list(getattr(nova_memory, "_conversation_memories", {}).values())
    return found


def _texts(mem) -> str:
    return " ".join(
        e.get("content") or "" for e in list(mem._short_term) + list(mem._long_term)
    )


def test_handle_chat_request_end_to_end_isolation():
    """Drives the real request handler; passes only if no instance is shared."""
    patches = [
        patch.object(nova_memory.NovaMemory, "load", lambda self: None),
        patch.object(nova_memory.NovaMemory, "_persist_entry", lambda s, e: None),
        patch.object(nova_memory, "_memory_instances", {}),
        patch.object(
            nova_memory, "_conversation_memories", OrderedDict(), create=True
        ),
        patch.object(nova, "_get_iq", lambda: _StubIQ()),
    ]
    for p in patches:
        p.start()
    try:
        nova.handle_chat_request(
            {"message": "I work at Acme Health and my budget is 40000", "conversation_id": "c-1"}
        )
        nova.handle_chat_request(
            {"message": "I work at Dallas Freight and my budget is 9000", "conversation_id": "c-2"}
        )
        nova.handle_chat_request({"message": "I work at Idless Corp, always"})
        blobs = [_texts(m) for m in _all_memory_instances()]
    finally:
        for p in reversed(patches):
            p.stop()
    assert not any("Acme Health" in b and "Dallas Freight" in b for b in blobs)
    assert not any("Idless Corp" in b for b in blobs)
    assert any("Acme Health" in b for b in blobs)
