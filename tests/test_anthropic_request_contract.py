"""Request-contract tests for every Anthropic Messages call the repo makes.

Offline: builds payloads and parses source; nothing is sent.  The schema under
test is Anthropic's published contract, retrieved 2026-10-01 from
platform.claude.com/docs/en (models/overview, about-claude/model-deprecations,
models/sonnet-5-5/migration-guide, models/opus-5/migration-guide,
build-with-claude/prompt-caching):

* Default model IDs must be Active in the model-deprecations table.
* ``temperature`` / ``top_p`` / ``top_k``: non-default values return HTTP 400 on
  Claude 4.7 and later -- no Anthropic payload may carry them.
* ``thinking.type = "enabled"`` + ``budget_tokens`` returns 400 on Opus 4.7+ /
  Sonnet 5.x; ``thinking.type = "disabled"`` returns 400 on Sonnet 5.5 and Opus
  5.5; forced ``tool_choice`` (``any`` / ``tool``) returns 400 on Sonnet 5.5 /
  Opus 5.5.  The repo has no need for any of them, so none may appear.
* An assistant-message prefill (last message from the assistant) returns 400 on
  Opus 4.6+/Sonnet 4.6+/5/5.5; the router builder drops it.
"""

from __future__ import annotations

import ast
import importlib
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Model-deprecations table, retrieved 2026-10-01, re-checked 2026-10-16
# (state, tentative retirement floor).
DOCUMENTED_ACTIVE_MODELS = {
    "claude-opus-5-5": ("Active", "2027-09-22"),
    "claude-opus-5": ("Active", "2027-07-24"),
    "claude-opus-4-8": ("Active", "2027-05-28"),
    "claude-sonnet-5-5": ("Active", "2027-09-28"),
    "claude-sonnet-5": ("Active", "2027-06-30"),
    "claude-sonnet-4-6": ("Active", "2027-02-17"),
    "claude-haiku-5-5": ("Active", "2027-10-07"),
    "claude-haiku-4-5-20251001": ("Active", "2026-10-15"),
}

# IDs whose documented retirement floor has passed (as of 2026-10-16) or that
# the repo has deliberately moved off: none may remain as a live default. The
# date-suffixed and bare spellings are both matched.
RETIRED_OR_SUPERSEDED_MODEL_RE = re.compile(
    r"claude-(haiku-4-5(-\d{8})?|sonnet-4-6|sonnet-4-5(-\d{8})?|"
    r"opus-4-[0-7](-\d{8})?|3-\d(-\d)?-(haiku|sonnet|opus))"
)

#: Fields the router's Anthropic builders may emit (Messages API top-level).
ALLOWED_PAYLOAD_KEYS = {
    "model",
    "max_tokens",
    "messages",
    "system",
    "tools",
    "stream",
    # 2026-10-16: `output_config.effort` (GA, no beta header) -- sent only for
    # ANTHROPIC_LOW_EFFORT_TASK_TYPES on effort-capable models; see
    # tests/test_llm_router_thinking_only.py.
    "output_config",
}
#: Fields that return 400 on the current IDs (or are unused and risky to add).
FORBIDDEN_PAYLOAD_KEYS = {
    "temperature",
    "top_p",
    "top_k",
    "thinking",
    "tool_choice",
    "output_format",
    "stop_sequences",
}


@pytest.fixture()
def router():
    """llm_router with env overrides cleared (defaults), restored afterwards."""
    keys = ("CLAUDE_HAIKU_MODEL", "CLAUDE_SONNET_MODEL", "CLAUDE_OPUS_MODEL")
    with mock.patch.dict(os.environ, {}, clear=False):
        for k in keys:
            os.environ.pop(k, None)
        mod = importlib.reload(importlib.import_module("llm_router"))
        yield mod
    importlib.reload(importlib.import_module("llm_router"))


def _messages_with_tool_turn() -> List[Dict[str, Any]]:
    return [
        {"role": "user", "content": "Pull LinkedIn CPC for nurses."},
        {
            "role": "assistant",
            "content": "Checking.",
            "tool_calls": [
                {
                    "id": "toolu_1",
                    "type": "function",
                    "function": {
                        "name": "query_benchmarks",
                        "arguments": json.dumps({"platform": "linkedin"}),
                    },
                }
            ],
        },
        {"role": "tool", "tool_call_id": "toolu_1", "content": '{"cpc": 4.1}'},
        {"role": "user", "content": "Summarize it."},
    ]


TOOLS = [
    {
        "name": "query_benchmarks",
        "description": "Look up recruitment benchmarks.",
        "input_schema": {"type": "object", "properties": {}},
    }
]


# ---------------------------------------------------------------------------
# Model IDs
# ---------------------------------------------------------------------------


def test_default_model_ids_are_documented_active(router):
    for pid, expected in (
        (router.CLAUDE_HAIKU, "claude-haiku-5-5"),
        (router.CLAUDE, "claude-sonnet-5-5"),
        (router.CLAUDE_OPUS, "claude-opus-5-5"),
    ):
        model = router.PROVIDER_CONFIG[pid]["model"]
        assert model == expected
        assert DOCUMENTED_ACTIVE_MODELS[model][0] == "Active"


def test_env_overrides_still_win(router):
    with mock.patch.dict(
        os.environ,
        {
            "CLAUDE_HAIKU_MODEL": "claude-haiku-4-5-20251001",
            "CLAUDE_SONNET_MODEL": "claude-sonnet-5",
            "CLAUDE_OPUS_MODEL": "claude-opus-4-8",
        },
    ):
        mod = importlib.reload(router)
        assert (
            mod.PROVIDER_CONFIG[mod.CLAUDE_HAIKU]["model"]
            == "claude-haiku-4-5-20251001"
        )
        assert mod.PROVIDER_CONFIG[mod.CLAUDE]["model"] == "claude-sonnet-5"
        assert mod.PROVIDER_CONFIG[mod.CLAUDE_OPUS]["model"] == "claude-opus-4-8"
        _, _, body = mod._build_anthropic_request(
            [{"role": "user", "content": "hi"}], "", 100, provider_id=mod.CLAUDE_OPUS
        )
        assert json.loads(body)["model"] == "claude-opus-4-8"


def test_cost_table_matches_documented_prices(router):
    costs = router._PROVIDER_COST_PER_M_TOKENS
    assert costs[router.CLAUDE_HAIKU] == {"input": 0.10, "output": 0.50}  # Haiku 5.5
    assert costs[router.CLAUDE] == {"input": 2.0, "output": 10.0}  # Sonnet 5.5
    assert costs[router.CLAUDE_OPUS] == {"input": 4.0, "output": 20.0}  # Opus 5.5


# ---------------------------------------------------------------------------
# Router builder payload contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("which", ["CLAUDE_HAIKU", "CLAUDE", "CLAUDE_OPUS"])
def test_builder_payload_matches_documented_schema(router, which):
    pid = getattr(router, which)
    url, headers, body = router._build_anthropic_request(
        _messages_with_tool_turn(), "You are Nova.", 4096, TOOLS, provider_id=pid
    )
    payload = json.loads(body)
    assert url == "https://api.anthropic.com/v1/messages"
    assert headers["anthropic-version"] == "2023-06-01"
    assert set(payload) <= ALLOWED_PAYLOAD_KEYS
    assert not (set(payload) & FORBIDDEN_PAYLOAD_KEYS)
    assert payload["model"] == router.PROVIDER_CONFIG[pid]["model"]
    assert isinstance(payload["max_tokens"], int) and payload["max_tokens"] >= 1
    # system is a list of text blocks carrying the 1h ephemeral cache breakpoint
    assert payload["system"] == [
        {
            "type": "text",
            "text": "You are Nova.",
            "cache_control": {"type": "ephemeral", "ttl": "1h"},
        }
    ]
    # tools: documented keys only; breakpoint on the last tool only
    for tool in payload["tools"]:
        assert set(tool) <= {"name", "description", "input_schema", "cache_control"}
    assert "cache_control" in payload["tools"][-1]


@pytest.mark.parametrize("which", ["CLAUDE", "CLAUDE_OPUS"])
def test_tool_turn_is_valid_anthropic_message_sequence(router, which):
    _, _, body = router._build_anthropic_request(
        _messages_with_tool_turn(), "", 1000, TOOLS, provider_id=getattr(router, which)
    )
    msgs = json.loads(body)["messages"]
    assert [m["role"] for m in msgs] == ["user", "assistant", "user", "user"]
    use = msgs[1]["content"][-1]
    assert use["type"] == "tool_use" and use["id"] == "toolu_1"
    assert use["input"] == {"platform": "linkedin"}
    result = msgs[2]["content"][0]
    assert result["type"] == "tool_result" and result["tool_use_id"] == "toolu_1"


@pytest.mark.parametrize(
    "model,rejects",
    [
        ("claude-opus-5-5", True),
        ("claude-opus-5", True),
        ("claude-opus-4-8", True),
        ("claude-opus-4-7", True),
        ("claude-opus-4-6", True),
        ("claude-sonnet-5-5", True),
        ("claude-sonnet-5", True),
        ("claude-sonnet-4-6", True),
        ("claude-fable-5-1", True),
        ("claude-haiku-5-5", True),
        ("claude-haiku-5", True),
        ("claude-haiku-4-5-20251001", False),
        ("claude-haiku-4-5", False),
        ("claude-sonnet-4-5-20250929", False),
        ("claude-opus-4-5-20251101", False),
        ("", False),
    ],
)
def test_prefill_rejection_table(router, model, rejects):
    assert router._anthropic_model_rejects_prefill(model) is rejects


def test_trailing_assistant_prefill_is_dropped_only_where_it_would_400(router):
    convo = [
        {"role": "user", "content": "Return JSON for the plan."},
        {"role": "assistant", "content": "{"},  # classic prefill
    ]
    for pid, kept in (
        (router.CLAUDE, False),
        (router.CLAUDE_OPUS, False),
        (router.CLAUDE_HAIKU, False),  # Haiku 5.5 rejects prefill too
    ):
        _, _, body = router._build_anthropic_request(convo, "", 200, provider_id=pid)
        roles = [m["role"] for m in json.loads(body)["messages"]]
        assert (roles == ["user", "assistant"]) is kept, pid
        assert roles[0] == "user"
    # Env rollback to Haiku 4.5 keeps the prefill (that ID accepts it).
    with mock.patch.dict(
        os.environ, {"CLAUDE_HAIKU_MODEL": "claude-haiku-4-5-20251001"}
    ):
        mod = importlib.reload(router)
        _, _, body = mod._build_anthropic_request(
            convo, "", 200, provider_id=mod.CLAUDE_HAIKU
        )
        assert [m["role"] for m in json.loads(body)["messages"]] == [
            "user",
            "assistant",
        ]


def test_prefill_guard_never_empties_the_conversation_or_eats_tool_use(router):
    # A lone assistant message is left alone (dropping it would send no messages).
    _, _, body = router._build_anthropic_request(
        [{"role": "assistant", "content": "hello"}], "", 50, provider_id=router.CLAUDE
    )
    assert len(json.loads(body)["messages"]) == 1
    # A trailing assistant TOOL_USE turn is not a prefill and must be preserved.
    convo = [
        {"role": "user", "content": "go"},
        {
            "role": "assistant",
            "content": None,
            "tool_calls": [
                {
                    "id": "t1",
                    "type": "function",
                    "function": {"name": "f", "arguments": "{}"},
                }
            ],
        },
    ]
    _, _, body = router._build_anthropic_request(
        convo, "", 50, TOOLS, provider_id=router.CLAUDE
    )
    last = json.loads(body)["messages"][-1]
    assert last["role"] == "assistant" and last["content"][0]["type"] == "tool_use"


def test_stream_builder_payload_and_prefill_guard(router):
    captured: Dict[str, Any] = {}

    class _Boom(Exception):
        pass

    def _fake_urlopen(req, timeout=None):
        captured["body"] = json.loads(req.data.decode("utf-8"))
        raise router.urllib.error.URLError("stop here")

    convo = [
        {"role": "user", "content": "Write two lines."},
        {"role": "assistant", "content": "Line one:"},
    ]
    with mock.patch.object(
        router.urllib.request, "urlopen", _fake_urlopen
    ), mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "k"}):
        list(router._stream_anthropic(convo, "sys", 100, provider_id=router.CLAUDE))
    payload = captured["body"]
    assert set(payload) <= ALLOWED_PAYLOAD_KEYS
    assert not (set(payload) & FORBIDDEN_PAYLOAD_KEYS)
    assert [m["role"] for m in payload["messages"]] == ["user"]


# ---------------------------------------------------------------------------
# Every direct call site (AST audit): no 400-prone fields in any Anthropic payload
# ---------------------------------------------------------------------------


def _anthropic_payload_dicts(path: Path, only_funcs: tuple = ()) -> List[tuple]:
    """(lineno, keys) for dict literals shaped like a Messages API payload."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: List[tuple] = []

    def scan(node: ast.AST) -> None:
        for sub in ast.walk(node):
            if isinstance(sub, ast.Dict):
                keys = {
                    k.value
                    for k in sub.keys
                    if isinstance(k, ast.Constant) and isinstance(k.value, str)
                }
                if {"model", "max_tokens", "messages"} <= keys:
                    found.append((sub.lineno, keys))

    if only_funcs:
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in only_funcs:
                scan(node)
    else:
        scan(tree)
    return found


@pytest.mark.parametrize(
    "filename,funcs,expected_min",
    [
        ("llm_router.py", ("_build_anthropic_request", "_stream_anthropic"), 2),
        ("nova.py", (), 5),  # every such dict in nova.py is a direct Anthropic call
        ("app.py", (), 1),  # APP_FALLBACK_SONNET_MODEL direct fallback
    ],
)
def test_no_call_site_sends_rejected_fields(filename, funcs, expected_min):
    dicts = _anthropic_payload_dicts(PROJECT_ROOT / filename, funcs)
    assert len(dicts) >= expected_min, f"{filename}: audit scope shrank: {dicts}"
    for lineno, keys in dicts:
        bad = keys & FORBIDDEN_PAYLOAD_KEYS
        assert not bad, f"{filename}:{lineno} sends {sorted(bad)} (400 on Claude 4.7+)"
        assert keys <= ALLOWED_PAYLOAD_KEYS, (
            f"{filename}:{lineno} has undocumented payload keys "
            f"{sorted(keys - ALLOWED_PAYLOAD_KEYS)}"
        )


# ---------------------------------------------------------------------------
# Retired / superseded model IDs must not survive as live literals
# ---------------------------------------------------------------------------


def _string_constants(path: Path) -> List[tuple]:
    """(lineno, value) for every str constant in the module -- comments are not
    constants, so rollback notes like ``CLAUDE_HAIKU_MODEL=claude-haiku-4-5-...``
    in ``#`` comments are allowed; docstrings and f-string parts are scanned."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        (node.lineno, node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]


@pytest.mark.parametrize("filename", ["nova.py", "llm_router.py", "app.py"])
def test_no_retired_model_id_literals(filename):
    """2026-10-16: Haiku 4.5's retirement floor (2026-10-15) has passed and the
    repo moved Sonnet 4.6 -> 5.5; neither may remain as a live default. Only
    ``#`` comments may still name them (env-rollback instructions)."""
    offenders = [
        f"{filename}:{lineno}: {value!r}"
        for lineno, value in _string_constants(PROJECT_ROOT / filename)
        if RETIRED_OR_SUPERSEDED_MODEL_RE.search(value)
    ]
    assert (
        not offenders
    ), "retired model IDs still live outside comments:\n  " + "\n  ".join(offenders)


def _module_level_default(path: Path, name: str) -> str:
    """Resolve ``NAME = os.environ.get("X") or "<default>"`` (or a bare string
    / parenthesised string) at module level without importing the module."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in node.targets
        ):
            value = node.value
            if isinstance(value, ast.BoolOp) and isinstance(value.op, ast.Or):
                value = value.values[-1]
            assert isinstance(value, ast.Constant) and isinstance(value.value, str)
            return value.value
    raise AssertionError(f"{path.name}: no module-level assignment to {name}")


def test_nova_direct_call_defaults_are_current_and_env_overridable():
    nova_path = PROJECT_ROOT / "nova.py"
    assert (
        _module_level_default(nova_path, "CLAUDE_MODEL_PRIMARY") == "claude-haiku-5-5"
    )
    assert (
        _module_level_default(nova_path, "CLAUDE_MODEL_COMPLEX") == "claude-sonnet-5-5"
    )
    for model in ("claude-haiku-5-5", "claude-sonnet-5-5"):
        assert DOCUMENTED_ACTIVE_MODELS[model][0] == "Active"
    src = nova_path.read_text(encoding="utf-8")
    assert 'os.environ.get("CLAUDE_MODEL_PRIMARY")' in src
    assert 'os.environ.get("CLAUDE_MODEL_COMPLEX")' in src
    # every direct Anthropic payload in nova.py names a model via the constants
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Dict):
            keys = [k.value if isinstance(k, ast.Constant) else None for k in node.keys]
            if {"model", "max_tokens", "messages"} <= set(keys):
                model_node = node.values[keys.index("model")]
                assert not isinstance(
                    model_node, ast.Constant
                ), f"nova.py:{node.lineno} hardcodes a model ID; use CLAUDE_MODEL_*"


def test_app_fallback_sonnet_default_is_current():
    src = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")
    assert 'os.environ.get("APP_FALLBACK_SONNET_MODEL") or "claude-sonnet-5-5"' in src
