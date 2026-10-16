"""One user's per-user fields must never come back from the shared cache.

The response cache stored the SAME dict handle_chat_request returned, and the
follow-up / profile post-processing mutated it in place, so user A's
``user_profile`` (including A's raw conversation id) was served to user B
sending the identical message without an id.
"""

from __future__ import annotations

import copy
import json
from unittest.mock import patch

import pytest

import nova

SECRET_ID = "conv-AAAAAAAAAAAAAAAA-secret-id"
MSG = (
    "Explain how a hiring funnel for forklift operators in Reno Nevada "
    "should be structured across paid channels"
)
RESP = {
    "response": "Here is a long enough media plan answer with numbers 12 and 30%. " * 4,
    "sources": ["KB"],
    "confidence": 0.9,
    "tools_used": ["query_channels"],
    "llm_provider": "stub",
}


@pytest.fixture()
def isolated_caches(tmp_path):
    with patch.object(nova, "_response_cache", {}), patch.object(
        nova, "RESPONSE_CACHE_FILE", tmp_path / "resp.json"
    ), patch.object(nova, "_upstash_enabled", False), patch.object(
        nova, "_intelligent_cache_get", lambda *a, **k: None
    ), patch.object(
        nova, "_intelligent_cache_set", lambda *a, **k: False
    ):
        yield


def _fake_model():
    calls = []

    def fake(self, *a, **k):
        calls.append(1)
        return copy.deepcopy(RESP)

    return calls, fake


def _run_two_users(first_request, second_request):
    calls, fake = _fake_model()
    with patch.object(nova.Nova, "_chat_with_free_llm_tools", fake), patch.object(
        nova.Nova, "_chat_with_llm_router", lambda *a, **k: None
    ), patch.object(nova.Nova, "_chat_with_claude", fake), patch.object(
        nova_memory_load(), "load", lambda self: None
    ), patch.object(
        nova_memory_load(), "_persist_entry", lambda s, e: None
    ):
        a = nova.handle_chat_request(dict(first_request))
        b = nova.handle_chat_request(dict(second_request))
    return calls, a, b


def nova_memory_load():
    import nova_memory

    return nova_memory.NovaMemory


def test_second_user_gets_nothing_of_the_first_users_profile_or_id(isolated_caches):
    calls, a, b = _run_two_users(
        {"message": MSG, "conversation_id": SECRET_ID}, {"message": MSG}
    )
    assert a["user_profile"]  # A really did get a profile
    assert len(calls) == 1, "second request must be served from the cache"
    assert b is not a
    assert "user_profile" not in b
    assert SECRET_ID not in json.dumps(b, default=str)
    assert SECRET_ID not in json.dumps(nova._response_cache, default=str)
    # What the cache holds is the model output as it was BEFORE per-user
    # post-processing (profile-personalised follow-ups etc.) mutated it.
    for entry in nova._response_cache.values():
        data = entry["data"]
        assert "follow_ups" not in data and "user_profile" not in data
        assert "you might also want to know" not in data["response"].lower()


def test_cache_never_hands_out_a_shared_dict(isolated_caches):
    nova._set_response_cache("k", {"response": "x" * 80, "user_profile": {"a": 1}})
    first = nova._get_response_cache("k")
    first["response"] = "mutated"
    first["injected"] = True
    second = nova._get_response_cache("k")
    assert second["response"] == "x" * 80 and "injected" not in second
    assert "user_profile" not in second and "user_profile" not in first


def test_caller_mutation_after_set_does_not_reach_the_cache(isolated_caches):
    live = {"response": "y" * 80, "sources": []}
    nova._set_response_cache("k2", live)
    live["user_profile"] = {"session_id": SECRET_ID}
    live["response"] += "\n\n**You might also want to know:** personalised"
    cached = nova._get_response_cache("k2")
    assert "user_profile" not in cached
    assert "personalised" not in cached["response"]


def test_profile_dict_in_responses_carries_no_conversation_id():
    import nova_memory

    profile = nova_memory.UserProfile(SECRET_ID)
    profile.update("nurses in Boston", [])
    assert SECRET_ID not in json.dumps(profile.to_dict())
    assert "session_id" not in profile.to_dict()


def test_id_less_request_never_returns_a_user_profile(isolated_caches):
    calls, fake = _fake_model()

    def fake_with_profile(self, *a, **k):
        r = fake(self, *a, **k)
        r["user_profile"] = {"session_id": SECRET_ID}
        return r

    with patch.object(nova.Nova, "_chat_with_free_llm_tools", fake_with_profile), patch.object(
        nova.Nova, "_chat_with_llm_router", lambda *a, **k: None
    ), patch.object(nova.Nova, "_chat_with_claude", fake_with_profile):
        b = nova.handle_chat_request({"message": MSG})
    assert "user_profile" not in b


def test_personalised_turns_are_not_cached(isolated_caches):
    """A turn whose prompt carried this conversation's memory is not shareable."""
    nova._set_turn_tool_capture({"conversation_id": "c", "tools": {}, "personalized": True})
    try:
        nova._set_response_cache("k3", {"response": "z" * 80})
    finally:
        nova._set_turn_tool_capture(None)
    assert nova._get_response_cache("k3") is None

