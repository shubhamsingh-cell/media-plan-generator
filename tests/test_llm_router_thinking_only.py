"""Thinking-only Anthropic replies must count as failures and route onward.

Background (prod, 2026-10-08 21:18:59Z and 2026-10-09 20:36:09Z): the
executive-narrative call (``TASK_PLAN_NARRATIVE``, ``max_tokens=600``) hit
Claude Sonnet 5.5, which runs adaptive thinking by default; thinking tokens
count toward ``max_tokens`` even when their text is omitted (Sonnet 5.5
migration guide: "Thinking counts toward `max_tokens` even when its text isn't
returned"), so the reply was ``stop_reason=max_tokens`` with a single
``thinking`` block and no text. The router's has-response test accepted any
non-empty ``raw_content``, logged "responded ... (in=1490, out=600)", and
returned ``text=""`` to the caller without trying Haiku / DeepSeek / Gemini.

These tests pin the fix:

* a reply with no ``text`` block and no ``tool_calls`` is a failure in both
  ``_call_single_provider`` and ``call_llm``'s exclude-and-retry loop;
* the failure carries ``stop_reason`` and the content block types;
* ``TASK_PLAN_NARRATIVE``-class calls ask effort-capable Anthropic models for
  ``output_config.effort = "low"`` (the guide's first remedy), never
  ``thinking.type = between_tools`` (Sonnet 5.5-only; 400 on Haiku/Opus);
* every non-excel ``TASK_PLAN_NARRATIVE`` call site passes ``max_tokens >= 2048``.

Offline: ``urllib.request.urlopen`` is patched; nothing is sent.
"""

from __future__ import annotations

import ast
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


THINKING_ONLY_REPLY: Dict[str, Any] = {
    "id": "msg_thinking_only",
    "type": "message",
    "role": "assistant",
    "model": "claude-sonnet-5-5",
    "content": [{"type": "thinking", "thinking": "", "signature": "sig"}],
    "stop_reason": "max_tokens",
    "usage": {"input_tokens": 1490, "output_tokens": 600},
}

TEXT_REPLY: Dict[str, Any] = {
    "id": "msg_text",
    "type": "message",
    "role": "assistant",
    "model": "claude-haiku-5-5",
    "content": [{"type": "text", "text": "Narrative from the next provider."}],
    "stop_reason": "end_turn",
    "usage": {"input_tokens": 1490, "output_tokens": 42},
}


class _FakeResponse:
    def __init__(self, payload: Dict[str, Any]) -> None:
        self._payload = payload

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")


@pytest.fixture()
def router():
    """llm_router reloaded with only the Anthropic key set and model overrides
    cleared, so TASK_PLAN_NARRATIVE routes CLAUDE -> CLAUDE_HAIKU -> ... with
    the code-default model IDs; restored afterwards."""
    cleared = (
        "CLAUDE_HAIKU_MODEL",
        "CLAUDE_SONNET_MODEL",
        "CLAUDE_OPUS_MODEL",
        "DEEPSEEK_API_KEY",
    )
    with mock.patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
        for key in cleared:
            os.environ.pop(key, None)
        mod = importlib.reload(importlib.import_module("llm_router"))
        yield mod
    importlib.reload(importlib.import_module("llm_router"))


# ---------------------------------------------------------------------------
# Single provider: thinking-only reply is a failure with a diagnostic error
# ---------------------------------------------------------------------------


def test_single_provider_marks_thinking_only_reply_as_failure(router):
    with mock.patch(
        "urllib.request.urlopen", return_value=_FakeResponse(THINKING_ONLY_REPLY)
    ):
        result = router._call_single_provider(
            router.CLAUDE,
            [{"role": "user", "content": "Write the executive summary."}],
            "You are a strategist.",
            600,
            task_type=router.TASK_PLAN_NARRATIVE,
        )
    assert result["text"] == ""
    assert not result.get("tool_calls")
    assert result["content_block_types"] == ["thinking"]
    assert result["stop_reason"] == "max_tokens"
    # The caller sees WHY it was empty (stop_reason + block types), not "unknown".
    error = result.get("error") or ""
    assert "max_tokens" in error and "thinking" in error, error
    assert "600/600" in error


def test_single_provider_text_reply_is_still_success(router):
    with mock.patch("urllib.request.urlopen", return_value=_FakeResponse(TEXT_REPLY)):
        result = router._call_single_provider(
            router.CLAUDE_HAIKU,
            [{"role": "user", "content": "hi"}],
            "",
            600,
            task_type=router.TASK_PLAN_NARRATIVE,
        )
    assert result["text"] == "Narrative from the next provider."
    assert not result.get("error")
    assert result["content_block_types"] == ["text"]


def test_tool_use_only_reply_is_still_success(router):
    """A tool_use turn has no text block either; it must stay a success."""
    tool_reply = {
        "content": [
            {"type": "tool_use", "id": "toolu_1", "name": "f", "input": {"a": 1}}
        ],
        "stop_reason": "tool_use",
        "usage": {"input_tokens": 5, "output_tokens": 5},
    }
    with mock.patch("urllib.request.urlopen", return_value=_FakeResponse(tool_reply)):
        result = router._call_single_provider(
            router.CLAUDE,
            [{"role": "user", "content": "go"}],
            "",
            600,
            tools=[
                {"name": "f", "description": "d", "input_schema": {"type": "object"}}
            ],
        )
    assert result["tool_calls"][0]["function"]["name"] == "f"
    assert not result.get("error")


# ---------------------------------------------------------------------------
# call_llm: thinking-only Sonnet reply -> routing continues to the next provider
# ---------------------------------------------------------------------------


def test_call_llm_fails_over_after_thinking_only_reply(router):
    """Fails on the pre-fix code: raw_content made the thinking-only reply a
    success, so call_llm returned text="" from CLAUDE and never tried Haiku."""
    bodies: List[Dict[str, Any]] = []
    replies = iter([THINKING_ONLY_REPLY, TEXT_REPLY])

    def _fake_urlopen(req, timeout=None):
        bodies.append(json.loads(req.data.decode("utf-8")))
        return _FakeResponse(next(replies))

    with mock.patch("urllib.request.urlopen", side_effect=_fake_urlopen):
        result = router.call_llm(
            messages=[{"role": "user", "content": "Write the executive summary."}],
            system_prompt="You are a strategist.",
            max_tokens=600,
            task_type=router.TASK_PLAN_NARRATIVE,
            use_cache=False,
        )

    # Both Anthropic tiers were tried, in route order, with the code-default IDs.
    assert [b["model"] for b in bodies] == ["claude-sonnet-5-5", "claude-haiku-5-5"]
    assert result["text"] == "Narrative from the next provider."
    assert result["provider"] == router.CLAUDE_HAIKU
    assert result["fallback_used"] is True
    attempts = result["attempts"]
    assert [a["provider"] for a in attempts] == [router.CLAUDE, router.CLAUDE_HAIKU]
    assert attempts[0]["status"] == "failed"
    assert "max_tokens" in attempts[0]["error"] and "thinking" in attempts[0]["error"]
    assert attempts[1]["status"] == "success"


# ---------------------------------------------------------------------------
# Request shape for short-prose task classes
# ---------------------------------------------------------------------------


def _payload(router, provider_id: str, task_type: str) -> Dict[str, Any]:
    _, _, body = router._build_anthropic_request(
        [{"role": "user", "content": "hi"}],
        "sys",
        600,
        provider_id=provider_id,
        task_type=task_type,
    )
    return json.loads(body)


@pytest.mark.parametrize("which", ["CLAUDE", "CLAUDE_HAIKU", "CLAUDE_OPUS"])
def test_narrative_tasks_request_low_effort_on_every_anthropic_tier(router, which):
    for task_type in router.ANTHROPIC_LOW_EFFORT_TASK_TYPES:
        payload = _payload(router, getattr(router, which), task_type)
        assert payload["output_config"] == {"effort": "low"}
        # Never `thinking`: `disabled` 400s on the 5.5 family and
        # `between_tools` is Sonnet 5.5-only.
        assert "thinking" not in payload
    assert router.TASK_PLAN_NARRATIVE in router.ANTHROPIC_LOW_EFFORT_TASK_TYPES


@pytest.mark.parametrize("task_type", ["", "chat", "campaign_plan", "research"])
def test_other_tasks_keep_default_effort(router, task_type):
    for which in ("CLAUDE", "CLAUDE_HAIKU", "CLAUDE_OPUS"):
        payload = _payload(router, getattr(router, which), task_type)
        assert "output_config" not in payload, (which, task_type)


def test_call_llm_threads_task_type_into_the_anthropic_body(router):
    captured: Dict[str, Any] = {}

    def _fake_urlopen(req, timeout=None):
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return _FakeResponse(TEXT_REPLY)

    with mock.patch("urllib.request.urlopen", side_effect=_fake_urlopen):
        router.call_llm(
            messages=[{"role": "user", "content": "Write a summary."}],
            system_prompt="You are a strategist.",
            max_tokens=2048,
            task_type=router.TASK_PLAN_NARRATIVE,
            force_provider=router.CLAUDE,
            use_cache=False,
        )
    assert captured["body"]["output_config"] == {"effort": "low"}
    assert captured["body"]["max_tokens"] == 2048


def test_env_rollback_to_haiku_4_5_never_receives_effort(router):
    """`output_config.effort` errors on Haiku 4.5, so the rollback path must
    not send it even for narrative tasks."""
    with mock.patch.dict(
        os.environ, {"CLAUDE_HAIKU_MODEL": "claude-haiku-4-5-20251001"}
    ):
        mod = importlib.reload(router)
        payload = _payload(mod, mod.CLAUDE_HAIKU, mod.TASK_PLAN_NARRATIVE)
    assert payload["model"] == "claude-haiku-4-5-20251001"
    assert "output_config" not in payload


@pytest.mark.parametrize(
    "model,supported",
    [
        ("claude-haiku-5-5", True),
        ("claude-sonnet-5-5", True),
        ("claude-sonnet-5", True),
        ("claude-sonnet-4-6", True),
        ("claude-opus-5-5", True),
        ("claude-opus-4-8", True),
        ("claude-opus-4-5-20251101", True),
        ("claude-fable-5-1", True),
        ("claude-haiku-4-5-20251001", False),
        ("claude-haiku-4-5", False),
        ("claude-sonnet-4-5-20250929", False),
        ("", False),
    ],
)
def test_effort_support_table(router, model, supported):
    assert router._anthropic_model_supports_effort(model) is supported


# ---------------------------------------------------------------------------
# Every TASK_PLAN_NARRATIVE call site leaves room for thinking + the reply
# ---------------------------------------------------------------------------

#: excel_v2.py's three `max_tokens=600` narrative sites are being raised by a
#: separate change (2026-10-16 audit); add it here once that lands.
NARRATIVE_CALL_SITE_FILES = ("app.py", "data_synthesizer.py", "nova.py")
MIN_NARRATIVE_MAX_TOKENS = 2048


def _int_constants(node: ast.AST) -> List[int]:
    """Every integer literal a max_tokens expression can resolve to."""
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        return [node.value]
    if isinstance(node, ast.IfExp):
        return _int_constants(node.body) + _int_constants(node.orelse)
    return []


def _resolve_name_in_scope(scope: Optional[ast.AST], name: str) -> List[int]:
    """Resolve ``name`` inside the enclosing function: parameter default or
    the integer literals of its assignments."""
    if scope is None:
        return []
    values: List[int] = []
    if isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
        args = scope.args
        positional = args.posonlyargs + args.args
        defaults = [None] * (len(positional) - len(args.defaults)) + list(args.defaults)
        for arg, default in zip(positional, defaults):
            if arg.arg == name and default is not None:
                values += _int_constants(default)
        for kwarg, default in zip(args.kwonlyargs, args.kw_defaults):
            if kwarg.arg == name and default is not None:
                values += _int_constants(default)
    for sub in ast.walk(scope):
        if isinstance(sub, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == name for t in sub.targets
        ):
            values += _int_constants(sub.value)
    return values


def _narrative_call_sites(path: Path) -> List[tuple]:
    """(lineno, resolved max_tokens literals) for call_llm / call_llm_json
    calls whose task_type names TASK_PLAN_NARRATIVE."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    parents: Dict[ast.AST, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node

    def enclosing_function(node: ast.AST) -> Optional[ast.AST]:
        cur = parents.get(node)
        while cur is not None and not isinstance(
            cur, (ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            cur = parents.get(cur)
        return cur

    sites: List[tuple] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        fname = (
            func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        )
        if fname not in {"call_llm", "call_llm_json", "_llm_call"}:
            continue
        kw = {k.arg: k.value for k in node.keywords if k.arg}
        task = kw.get("task_type")
        if task is None:
            continue
        task_src = ast.unparse(task)
        if isinstance(task, ast.Name):
            # e.g. app.py: `task = task_type or getattr(router, "TASK_PLAN_NARRATIVE", ...)`
            scope = enclosing_function(node)
            for sub in ast.walk(scope) if scope is not None else ():
                if isinstance(sub, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == task.id for t in sub.targets
                ):
                    task_src += " " + ast.unparse(sub.value)
        if "TASK_PLAN_NARRATIVE" not in task_src:
            continue
        mt = kw.get("max_tokens")
        if mt is None:
            sites.append((node.lineno, []))
            continue
        values = _int_constants(mt)
        if not values and isinstance(mt, ast.Name):
            values = _resolve_name_in_scope(enclosing_function(node), mt.id)
        sites.append((node.lineno, values))
    return sites


@pytest.mark.parametrize("filename", NARRATIVE_CALL_SITE_FILES)
def test_narrative_call_sites_leave_room_for_thinking(filename):
    sites = _narrative_call_sites(PROJECT_ROOT / filename)
    if filename == "app.py":
        # data_synthesizer.py lost its narrative call site in b810ea8 (dead
        # ai_narratives path removed); it stays in the scan in case one returns.
        assert sites, f"{filename}: audit scope shrank -- no narrative call sites"
    for lineno, values in sites:
        assert values, (
            f"{filename}:{lineno}: TASK_PLAN_NARRATIVE call_llm without a resolvable "
            f"max_tokens literal (default-argument or assignment expected)"
        )
        low = min(values)
        assert low >= MIN_NARRATIVE_MAX_TOKENS, (
            f"{filename}:{lineno}: TASK_PLAN_NARRATIVE max_tokens can be {low} "
            f"(< {MIN_NARRATIVE_MAX_TOKENS}); the 5.5 models' default-on thinking "
            f"counts toward max_tokens and can leave no text block"
        )


def test_app_narrative_sites_get_the_workbook_narrative_cap():
    """app.py's TASK_PLAN_NARRATIVE call sites (``_generate_narrative``) leave
    the same room as excel_v2's ``_NARRATIVE_MAX_TOKENS`` (4096, e94ec1f) --
    one route, one ceiling. They sat at 2048 (and at 300 before 9cb9ff9)."""
    import excel_v2

    sites = _narrative_call_sites(PROJECT_ROOT / "app.py")
    assert sites, "app.py: audit scope shrank -- no narrative call sites"
    for lineno, values in sites:
        assert values and min(values) >= excel_v2._NARRATIVE_MAX_TOKENS, (
            f"app.py:{lineno}: TASK_PLAN_NARRATIVE max_tokens {values} "
            f"< excel_v2._NARRATIVE_MAX_TOKENS ({excel_v2._NARRATIVE_MAX_TOKENS})"
        )
