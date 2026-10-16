"""Answers shaped by one user's history/context must never be shared.

Both response caches, the request coalescer and the planner hint were keyed
by the message alone, so an answer built from user A's prior turns (or client
context) could be served to user B sending the same message.
"""

from __future__ import annotations

import copy
import threading
from unittest.mock import patch

import pytest

import nova
import nova_cache
import request_coalescing

MSG = (
    "Explain how a hiring funnel for forklift operators in Reno Nevada "
    "should be structured across paid channels"
)
SECRET = "Acme Hospital, Dallas"
HIST = [
    {"role": "user", "content": f"We are hiring nurses for {SECRET}"},
    {"role": "assistant", "content": "Noted."},
]


@pytest.fixture()
def isolated(tmp_path):
    with patch.object(nova, "_response_cache", {}), patch.object(
        nova, "RESPONSE_CACHE_FILE", tmp_path / "resp.json"
    ), patch.object(nova, "_upstash_enabled", False), patch.object(
        nova_cache, "_memory_cache", {}
    ), patch.object(nova_cache, "_supabase_enabled", False), patch.object(
        nova_cache, "_upstash_enabled", False
    ), patch.object(nova, "_intelligent_cache_available", True):
        yield


def _drive(requests):
    """Run requests through the real handle_chat_request -> Nova.chat path with a
    model stub that echoes whatever history/context it was handed."""
    calls = []

    def fake(self, user_message, conversation_history=None, enrichment_context=None, *a, **k):
        calls.append(1)
        seen = " ".join(m.get("content", "") for m in (conversation_history or []))
        ctx = str(enrichment_context or "")
        return {
            "response": "Recommended channels with 30% programmatic. " + seen + " " + ctx + " " * 5 + "x" * 40,
            "sources": ["KB"],
            "confidence": 0.9,
            "tools_used": ["query_channels"],
            "llm_provider": "stub",
        }

    out = []
    with patch.object(nova.Nova, "_chat_with_free_llm_tools", fake), patch.object(
        nova.Nova, "_chat_with_llm_router", lambda *a, **k: None
    ), patch.object(nova.Nova, "_chat_with_claude", fake):
        for req in requests:
            out.append(nova.handle_chat_request(copy.deepcopy(req)))
    return calls, out


def test_probe_history_answer_is_not_served_to_a_stateless_request(isolated):
    calls, (a, b) = _drive([{"message": MSG, "history": HIST}, {"message": MSG}])
    assert SECRET in a["response"]  # the stub really personalised A's answer
    assert SECRET not in b["response"]
    assert len(calls) == 2, "B must not be served from a cache A wrote"


def test_probe_history_answer_is_not_served_to_another_history_user(isolated):
    other = [{"role": "user", "content": "unrelated"}, {"role": "assistant", "content": "ok"}]
    calls, (a, b) = _drive(
        [{"message": MSG, "history": HIST}, {"message": MSG, "history": other}]
    )
    assert SECRET not in b["response"]
    assert len(calls) == 2


def test_stateless_answer_is_not_served_to_a_request_with_history(isolated):
    """A request with history is never read from the cache either."""
    calls, (a, b) = _drive([{"message": MSG}, {"message": MSG, "history": HIST}])
    assert len(calls) == 2
    assert SECRET in b["response"]  # B got an answer built from ITS history


def test_client_context_answer_is_not_shared(isolated):
    ctx = {"campaign_client": SECRET}
    calls, (a, b) = _drive([{"message": MSG, "context": ctx}, {"message": MSG}])
    assert SECRET in a["response"] and SECRET not in b["response"]
    assert len(calls) == 2


def test_stateless_first_turns_are_still_cached(isolated):
    calls, (a, b) = _drive([{"message": MSG}, {"message": MSG}])
    assert len(calls) == 1 and b["response"][:60] == a["response"][:60]


def test_nova_cache_skips_any_prior_history():
    skip, reason = nova_cache._should_skip_caching(MSG, HIST[:1])
    assert skip and reason == "followup"
    assert nova_cache._should_skip_caching(MSG, [])[0] is False


# ---------------------------------------------------------------------------
# L-B: request coalescing
# ---------------------------------------------------------------------------
class _FakeRedis:
    def __init__(self):
        self.kv = {}

    def get(self, k):
        return self.kv.get(k)

    def set_nx(self, k, v, ttl):
        if k in self.kv:
            return False
        self.kv[k] = v
        return True

    def set_ex(self, k, v, ttl):
        self.kv[k] = v

    def delete(self, k):
        self.kv.pop(k, None)


@pytest.fixture()
def coalescer():
    r = _FakeRedis()
    with patch.object(request_coalescing, "_ENABLED", True), patch.object(
        request_coalescing, "_redis_get", r.get
    ), patch.object(request_coalescing, "_redis_set_nx", r.set_nx), patch.object(
        request_coalescing, "_redis_set_ex", r.set_ex
    ), patch.object(request_coalescing, "_redis_del", r.delete):
        yield request_coalescing.RequestCoalescer(), r


def test_coalescer_hash_depends_on_the_scope_marker(coalescer):
    c, r = coalescer
    _, h_plain, _ = c.check_or_register(MSG)
    _, h_scoped, _ = c.check_or_register(MSG, scope="stateless")
    assert h_plain != h_scoped


def test_only_stateless_requests_may_coalesce():
    def scope(state):
        nova._set_turn_tool_capture(state)
        try:
            return nova._coalesce_scope()
        finally:
            nova._set_turn_tool_capture(None)

    assert scope({"conversation_id": "", "stateless": True})
    assert scope({"conversation_id": "", "stateless": False}) is None  # history/context
    assert scope({"conversation_id": "conv-abcdefghijklmnop", "stateless": True}) is None
    assert scope({"conversation_id": "", "stateless": True, "personalized": True}) is None
    assert nova._coalesce_scope() is None  # no turn state -> never coalesce


class _RecordingCoalescer:
    def __init__(self):
        self.completed, self.released = [], []

    def complete(self, h, r):
        self.completed.append((h, r))

    def release(self, h):
        self.released.append(h)


def test_a_personalised_result_is_never_stored_for_followers():
    rec = _RecordingCoalescer()
    nova._set_turn_tool_capture({"conversation_id": "", "stateless": True, "personalized": True})
    try:
        nova._complete_coalesced(rec, "h1", {"response": SECRET})
    finally:
        nova._set_turn_tool_capture(None)
    assert rec.completed == [] and rec.released == ["h1"]


def test_a_stateless_result_is_stored():
    rec = _RecordingCoalescer()
    nova._set_turn_tool_capture({"conversation_id": "", "stateless": True})
    try:
        nova._complete_coalesced(rec, "h2", {"response": "generic"})
    finally:
        nova._set_turn_tool_capture(None)
    assert rec.completed == [("h2", {"response": "generic"})] and rec.released == []


def test_released_slot_lets_followers_proceed_without_a_result(coalescer):
    c, r = coalescer
    leader, h, _ = c.check_or_register(MSG, scope="stateless")
    assert leader
    got = {}

    def follower():
        got["r"] = c.check_or_register(MSG, scope="stateless")

    t = threading.Thread(target=follower)
    t.start()
    c.release(h)
    t.join(timeout=5)
    assert not t.is_alive(), "follower must not spin until the window expires"
    is_leader, _, result = got["r"]
    assert result is None
    assert not any(k.endswith(h) and "res:" in k for k in r.kv)


def test_chat_path_uses_the_scoped_coalescer():
    src = open(nova.__file__, encoding="utf-8").read()
    import re

    assert re.search(r"check_or_register\(\s*user_message, scope=", src)
    assert "_coalescer.complete(_qhash, _result)" not in src


# ---------------------------------------------------------------------------
# L-C: planner hint is request scoped, intent cache never holds history-derived intents
# ---------------------------------------------------------------------------
def test_planner_hint_is_request_scoped_across_interleaved_threads():
    barrier = threading.Barrier(2)
    seen = {}

    def worker(name):
        nova._set_turn_tool_capture({"conversation_id": "", "tools": {}})
        nova._set_intent_hint({"role": name})
        barrier.wait(timeout=5)  # both have stored their hint before either reads
        seen[name] = (nova._get_intent_hint() or {}).get("role")
        nova._set_turn_tool_capture(None)

    ts = [threading.Thread(target=worker, args=(n,)) for n in ("nurse", "driver")]
    for t in ts:
        t.start()
    for t in ts:
        t.join(timeout=10)
    assert seen == {"nurse": "nurse", "driver": "driver"}


def test_no_shared_instance_hint_remains():
    src = open(nova.__file__, encoding="utf-8").read()
    assert "self._intent_hint" not in src
    assert 'getattr(self, "_intent_hint"' not in src


def test_planner_never_caches_or_reads_history_derived_intents():
    n = nova.Nova.__new__(nova.Nova)
    n._intent_cache = nova.OrderedDict() if hasattr(nova, "OrderedDict") else __import__("collections").OrderedDict()
    n._intent_cache_lock = threading.Lock()
    summaries = []

    def fake_llm(message, history_summary=""):
        summaries.append(history_summary)
        return {"intent": ["media_plan"], "role": "nurse", "reason": history_summary or "none"}

    msg = "compare cpa for nurses in two cities"
    with patch.object(nova, "_t3_flag", lambda *_: True), patch.object(
        nova, "_is_planner_eligible_query", lambda *_: True
    ), patch.object(nova, "_call_planner_llm", fake_llm):
        a = n._run_planner_step(msg, HIST)
        b = n._run_planner_step(msg, None)
        c = n._run_planner_step(msg, None)
    assert SECRET in a["reason"]
    assert SECRET not in b["reason"], "B was served an intent derived from A's history"
    assert c == b and len(summaries) == 2  # stateless intents are still cached
