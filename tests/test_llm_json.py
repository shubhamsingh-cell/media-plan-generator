"""Tests for llm_router structured-JSON primitive (S89, L3.1).

Covers the robust extractor (_extract_json) and call_llm_json's validate +
corrective-retry behavior, with call_llm mocked (no network).

Runs under pytest, or standalone: ``python3 tests/test_llm_json.py``.
"""

import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import llm_router  # noqa: E402

SCHEMA = {
    "type": "object",
    "properties": {"a": {"type": "integer"}, "b": {"type": "string"}},
    "required": ["a", "b"],
}


def test_extract_plain_json():
    assert llm_router._extract_json('{"a": 1, "b": "x"}') == {"a": 1, "b": "x"}


def test_extract_fenced_json():
    txt = 'Here you go:\n```json\n{"a": 2, "b": "y"}\n```\nThanks!'
    assert llm_router._extract_json(txt) == {"a": 2, "b": "y"}


def test_extract_json_with_leading_prose():
    txt = 'Sure! {"a": 3, "b": "z"} hope that helps'
    assert llm_router._extract_json(txt) == {"a": 3, "b": "z"}


def test_extract_array():
    assert llm_router._extract_json("[1, 2, 3]") == [1, 2, 3]


def test_extract_none_on_garbage():
    assert llm_router._extract_json("no json here at all") is None
    assert llm_router._extract_json("") is None


def test_call_llm_json_success_first_try():
    with mock.patch.object(
        llm_router, "call_llm",
        return_value={"text": '{"a": 1, "b": "ok"}', "provider": "gemini"},
    ) as m:
        out = llm_router.call_llm_json([{"role": "user", "content": "go"}], SCHEMA)
    assert out["ok"] is True
    assert out["data"] == {"a": 1, "b": "ok"}
    assert out["provider"] == "gemini"
    assert m.call_count == 1
    # schema contract must be injected into the system prompt
    assert "JSON Schema" in m.call_args.kwargs["system_prompt"]
    # structured calls must not serve cached JSON
    assert m.call_args.kwargs["use_cache"] is False


def test_call_llm_json_retries_then_succeeds():
    responses = [
        {"text": "I think the answer is 1 and ok", "provider": "groq"},  # no JSON
        {"text": '{"a": 1, "b": "ok"}', "provider": "groq"},  # corrected
    ]
    with mock.patch.object(llm_router, "call_llm", side_effect=responses) as m:
        out = llm_router.call_llm_json([{"role": "user", "content": "go"}], SCHEMA)
    assert out["ok"] is True
    assert out["data"]["a"] == 1
    assert m.call_count == 2  # one retry
    # retry must include a corrective user message
    retry_msgs = m.call_args_list[1].kwargs["messages"]
    assert any("valid JSON" in (msg.get("content") or "") for msg in retry_msgs)


def test_call_llm_json_missing_required_key_fails():
    with mock.patch.object(
        llm_router, "call_llm",
        return_value={"text": '{"a": 1}', "provider": "x"},  # missing 'b'
    ):
        out = llm_router.call_llm_json([{"role": "user", "content": "go"}], SCHEMA)
    assert out["ok"] is False
    assert out["data"] is None
    assert "b" in out["error"]


def test_call_llm_json_handles_call_llm_exception():
    with mock.patch.object(llm_router, "call_llm", side_effect=RuntimeError("boom")):
        out = llm_router.call_llm_json([{"role": "user", "content": "go"}], SCHEMA)
    assert out["ok"] is False
    assert out["data"] is None


if __name__ == "__main__":
    _failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                _failures += 1
                print(f"FAIL {_name}: {exc}")
    sys.exit(1 if _failures else 0)
