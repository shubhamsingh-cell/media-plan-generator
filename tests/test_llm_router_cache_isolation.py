"""The LLM router's own response cache must not share history-shaped answers.

llm_router keyed its process-wide (+Upstash) cache on the first 200 chars of the
system prompt and the last user message, so an answer built from user A's
history (or memory/profile text appended to the system prompt) was served to
user B sending the same short message. These tests drive the real
handle_chat_request -> _chat_with_llm_router -> call_llm path and stub ONLY the
provider boundary (``_call_single_provider``); the router cache is the real one.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

import llm_router
import nova

SECRET = "Acme Hospital, Dallas"
HIST = [
    {"role": "user", "content": f"We are hiring nurses for {SECRET}"},
    {"role": "assistant", "content": "Great, noted."},
]
LONG_ID_A = "conv-aaaaaaaaaaaaaaaaaaaaaaaa"
LONG_ID_B = "conv-bbbbbbbbbbbbbbbbbbbbbbbb"


@pytest.fixture()
def router_env(tmp_path):
    calls = []

    def provider(pid, messages, system_prompt, max_tokens, tools=None, **kw):
        calls.append(1)
        seen = " | ".join(str(m.get("content")) for m in messages)
        return {
            "text": "Glad to help. Context seen: " + seen + " " + "pad " * 10,
            "provider": pid,
            "model": "stub-model",
            "input_tokens": 5,
            "output_tokens": 5,
            "latency_ms": 1,
        }

    status = {"providers": {"gemini": {"configured": True}}}
    llm_router._response_cache._store.clear()
    with patch.dict(os.environ, {"GEMINI_API_KEY": "test-key"}), patch.object(
        llm_router, "_call_single_provider", provider
    ), patch.object(
        llm_router, "get_router_status", lambda: status
    ), patch.object(nova, "_bounded_vector_search", lambda *a, **k: []), patch.object(
        nova, "_response_cache", {}
    ), patch.object(nova, "RESPONSE_CACHE_FILE", tmp_path / "r.json"), patch.object(
        nova, "_upstash_enabled", False
    ), patch.object(
        llm_router, "_UPSTASH_ENABLED", False
    ):
        yield calls
    llm_router._response_cache._store.clear()


def _chat(req):
    # force the router path with a stubbed provider id (A/B forced provider)
    with patch.object(nova.Nova, "_chat_with_claude", lambda *a, **k: None), patch.object(
        nova.Nova, "_chat_with_free_llm_tools", lambda *a, **k: None
    ):
        return nova.handle_chat_request(dict(req))


def test_history_answer_is_not_served_to_other_users_via_the_router_cache(router_env):
    calls = router_env
    a = _chat({"message": "perfect", "history": HIST})
    b = _chat({"message": "perfect", "conversation_id": LONG_ID_B})
    c = _chat({"message": "perfect"})
    assert a and SECRET in a["response"], "A's answer must be built from A's history"
    # A reaches the model; B (own id, empty memory) and C (no id) are both
    # stateless first turns, so they may legitimately share ONE entry between
    # them -- but never A's: that needs more than 1 call in total.
    assert len(calls) == 2, "B/C must not be served A's history-shaped answer"
    assert SECRET not in b["response"] and SECRET not in c["response"]


def test_stateless_small_talk_may_still_use_the_router_cache(router_env):
    calls = router_env
    _chat({"message": "perfect"})
    _chat({"message": "perfect"})
    # whichever layer served the repeat, the model is not asked twice
    assert len(calls) == 1


def test_router_key_covers_the_full_system_prompt_and_message_list(router_env):
    calls = router_env
    long_prefix = "S" * 400
    base = [{"role": "user", "content": "perfect"}]
    llm_router.call_llm(base, system_prompt=long_prefix + " tail-A", task_type="conversational", force_provider="gemini")
    # same 200-char system prefix + same last user message, different tail
    r2 = llm_router.call_llm(base, system_prompt=long_prefix + " tail-B", task_type="conversational", force_provider="gemini")
    # same system prompt + last message, different earlier turn
    r3 = llm_router.call_llm(HIST + base, system_prompt=long_prefix + " tail-A", task_type="conversational", force_provider="gemini")
    assert r2["cache_hit"] is False and r3["cache_hit"] is False
    assert len(calls) == 3
    # identical full inputs do hit
    r4 = llm_router.call_llm(base, system_prompt=long_prefix + " tail-A", task_type="conversational", force_provider="gemini")
    assert r4["cache_hit"] is True and len(calls) == 3


def test_router_key_is_a_hash_of_the_full_inputs_never_a_prefix():
    make = llm_router._ResponseCache._make_key
    k1 = make("conversational", "S" * 300 + "A", "u" * 700 + "1")
    k2 = make("conversational", "S" * 300 + "B", "u" * 700 + "1")
    k3 = make("conversational", "S" * 300 + "A", "u" * 700 + "2")
    assert len({k1, k2, k3}) == 3
    assert len(k1) == 64 and int(k1, 16) >= 0


def test_router_call_is_uncached_unless_the_turn_is_stateless_and_unpersonalised(router_env):
    seen = []
    real = llm_router.call_llm

    def spy(*a, **k):
        seen.append(k.get("use_cache", True))
        return real(*a, **k)

    with patch.object(llm_router, "call_llm", spy):
        _chat({"message": "perfect", "history": HIST})
        _chat({"message": "perfect"})
    assert seen[0] is False  # history-bearing turn
    assert seen[1] is True  # stateless turn


def test_summarize_transcript_key_uses_the_whole_transcript(router_env):
    calls = router_env
    shared = "User: " + "x" * 600
    t1 = shared + "\nNova: ok A"
    t2 = shared + "\nNova: ok B"
    kw = dict(system_prompt="sum", task_type="context_summarize", force_provider="gemini")
    llm_router.call_llm([{"role": "user", "content": t1}], **kw)
    r = llm_router.call_llm([{"role": "user", "content": t2}], **kw)
    assert r["cache_hit"] is False and len(calls) == 2
