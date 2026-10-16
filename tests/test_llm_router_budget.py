"""Cap-starved providers are skipped per task; one provider cannot eat the budget.

Background (prod, 2026-10-16 from 12:15Z): ``POST /api/copilot/suggest`` logged
``deepseek returned no text in 6332ms (in=368, out=300, stop_reason=length)``
followed by ``only 3.7s remaining in budget, stopping early`` (x28), 34 SLOW
ENDPOINT lines (1.0-24.2 s) and 18 of 300 responses with an empty suggestion.

Mechanism: the brief suggestion calls ``TASK_PLAN_STRUCTURED`` with
``max_tokens=300`` and a 10 s budget. DEEPSEEK is first in that chain and runs
``deepseek-v4-pro`` (a reasoning model), which spends the whole 300-token cap
on reasoning -> empty text, ``finish_reason=length``, after ~6 s. The router
handed that first provider the ENTIRE remaining budget as its timeout, then the
fixed 5 s ``_MIN_REMAINING_BUDGET`` floor refused to start Gemini/Claude with
3.7 s left. Same class as the 2026-10-01 narrative bug (thinking ate
``max_tokens=600``).

Offline: ``urllib.request.urlopen`` and ``_call_single_provider`` are faked.
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

_MSG = [{"role": "user", "content": "Suggest improvements to this brief."}]

# DeepSeek reasoning model: whole cap spent on reasoning -> no content.
_DEEPSEEK_STARVED = {
    "choices": [{"message": {"content": ""}, "finish_reason": "length"}],
    "usage": {"prompt_tokens": 368, "completion_tokens": 300},
    "model": "deepseek-v4-pro",
}
_GEMINI_OK = {
    "candidates": [{"content": {"parts": [{"text": '[{"text": "Add a timeline"}]'}]}}],
    "usageMetadata": {"promptTokenCount": 368, "candidatesTokenCount": 20},
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
    """llm_router reloaded with only DeepSeek + Gemini keys; circuit mesh off
    so a pass can only come from the router's own skip logic."""
    env = {"DEEPSEEK_API_KEY": "k-ds", "GEMINI_API_KEY": "k-gem"}
    with mock.patch.dict(os.environ, env, clear=False):
        for key in (
            "ANTHROPIC_API_KEY",
            "OPENAI_API_KEY",
            "GROQ_API_KEY",
            "CEREBRAS_API_KEY",
        ):
            os.environ.pop(key, None)
        mod = importlib.reload(importlib.import_module("llm_router"))
        mod._circuit_mesh = None
        yield mod
    importlib.reload(importlib.import_module("llm_router"))


# ---------------------------------------------------------------------------
# (a) cap-starved provider is skipped for that task on the next call
# ---------------------------------------------------------------------------


def test_cap_starved_provider_skipped_on_second_call(router) -> None:
    hits: List[str] = []

    def fake_urlopen(req, timeout=None):  # noqa: ANN001
        url = req.full_url
        if "deepseek" in url:
            hits.append("deepseek")
            return _FakeResponse(_DEEPSEEK_STARVED)
        hits.append("gemini")
        return _FakeResponse(_GEMINI_OK)

    with mock.patch("urllib.request.urlopen", side_effect=fake_urlopen):
        first = router.call_llm(
            _MSG,
            max_tokens=300,
            task_type=router.TASK_PLAN_STRUCTURED,
            use_cache=False,
            timeout_budget=10.0,
        )
        second = router.call_llm(
            _MSG,
            max_tokens=300,
            task_type=router.TASK_PLAN_STRUCTURED,
            use_cache=False,
            timeout_budget=10.0,
        )
    assert first["text"] and second["text"]
    assert hits.count("deepseek") == 1, hits  # probed once, then skipped
    assert hits.count("gemini") == 2, hits
    assert second["attempts"][0]["provider"] != "deepseek"


def test_starved_marker_is_per_task_type_and_cap(router) -> None:
    """A different task type, or a much larger cap, still tries DeepSeek."""
    with mock.patch(
        "urllib.request.urlopen", return_value=_FakeResponse(_DEEPSEEK_STARVED)
    ):
        router._call_single_provider(
            router.DEEPSEEK,
            _MSG,
            "",
            300,
            task_type=router.TASK_PLAN_STRUCTURED,
        )
    assert router._is_cap_starved(router.DEEPSEEK, router.TASK_PLAN_STRUCTURED, 300)
    assert not router._is_cap_starved(router.DEEPSEEK, router.TASK_RESEARCH, 300)
    assert not router._is_cap_starved(
        router.DEEPSEEK, router.TASK_PLAN_STRUCTURED, 4096
    )


def test_starved_marker_expires(router) -> None:
    router._mark_cap_starved(router.DEEPSEEK, router.TASK_PLAN_STRUCTURED, 300)
    assert router._is_cap_starved(router.DEEPSEEK, router.TASK_PLAN_STRUCTURED, 300)
    real = time.time()
    with mock.patch.object(
        router.time, "time", return_value=real + router._CAP_STARVED_TTL_S + 1
    ):
        assert not router._is_cap_starved(
            router.DEEPSEEK, router.TASK_PLAN_STRUCTURED, 300
        )


# ---------------------------------------------------------------------------
# (b) per-provider time slice
# ---------------------------------------------------------------------------


def test_time_slice_math(router) -> None:
    f = router._provider_time_slice
    assert f(10.0, 1, 10.0) == pytest.approx(5.0)  # half, one fallback left
    assert f(10.0, 3, 10.0) == pytest.approx(5.0)  # still half (50% cap)
    assert f(2.0, 2, 10.0) == pytest.approx(1.5)  # floor
    assert f(1.0, 2, 10.0) == pytest.approx(1.0)  # never above what is left
    assert f(10.0, 0, 10.0) == pytest.approx(10.0)  # last provider gets it all


def test_slice_only_applies_to_short_budgets(router) -> None:
    f = router._provider_time_slice
    cap = router._SHORT_BUDGET_MAX_S
    assert cap == pytest.approx(12.0)
    assert f(cap, 2, cap) == pytest.approx(cap / 2)  # at the threshold: sliced
    assert f(35.0, 2, 35.0) == pytest.approx(35.0)  # default budget: old behaviour
    assert f(20.0, 2, 20.0) == pytest.approx(20.0)  # nova 20s: old behaviour
    assert f(cap + 0.1, 2, cap + 0.1) == pytest.approx(cap + 0.1)  # just above


def _first_timeout(router, budget: float) -> float:
    seen: List[float] = []

    def fake_single(
        provider_id, *args, timeout_override=None, **kwargs
    ):  # noqa: ANN001
        seen.append(timeout_override)
        return {"text": "", "provider": provider_id, "error": "boom"}

    with mock.patch.object(router, "_call_single_provider", side_effect=fake_single):
        router.call_llm(
            _MSG,
            max_tokens=300,
            task_type=router.TASK_PLAN_STRUCTURED,
            use_cache=False,
            timeout_budget=budget,
        )
    return seen[0]


def test_long_budget_first_provider_keeps_whole_remainder(router) -> None:
    assert _first_timeout(router, 35.0) > 34.0
    assert _first_timeout(router, 20.0) > 19.0


def test_unavailable_fallbacks_do_not_shrink_the_primary_slice(router) -> None:
    """Primary + one circuit-open fallback: nothing can take over, so the
    primary must get the full remaining budget, not half."""
    seen: List[float] = []

    def fake_single(
        provider_id, *args, timeout_override=None, **kwargs
    ):  # noqa: ANN001
        seen.append(timeout_override)
        return {"text": "ok", "provider": provider_id, "latency_ms": 5}

    # Only DEEPSEEK + the two Gemini tiers (one shared key) are keyed; open
    # both Gemini circuits so no usable fallback remains.
    for pid in (router.GEMINI, router.GEMINI_FLASH_LITE):
        router._provider_states[pid].circuit_open_until = time.time() + 600
    with mock.patch.object(router, "_call_single_provider", side_effect=fake_single):
        res = router.call_llm(
            _MSG,
            max_tokens=300,
            task_type=router.TASK_PLAN_STRUCTURED,
            preferred_providers=[router.DEEPSEEK, router.GEMINI],
            use_cache=False,
            timeout_budget=10.0,
        )
    assert res["provider"] == router.DEEPSEEK
    assert seen[0] > 9.0, seen  # full remainder (gemini keyed but circuit-open)


def test_usable_fallback_still_halves_the_primary_slice(router) -> None:
    seen: List[float] = []

    def fake_single(
        provider_id, *args, timeout_override=None, **kwargs
    ):  # noqa: ANN001
        seen.append(timeout_override)
        return {"text": "", "provider": provider_id, "error": "boom"}

    with mock.patch.object(router, "_call_single_provider", side_effect=fake_single):
        router.call_llm(
            _MSG,
            max_tokens=300,
            task_type=router.TASK_PLAN_STRUCTURED,
            preferred_providers=[router.DEEPSEEK, router.GEMINI],
            use_cache=False,
            timeout_budget=10.0,
        )
    assert seen[0] <= 5.05, seen


def test_first_provider_gets_half_the_remaining_budget(router) -> None:
    seen: List[float] = []

    def fake_single(
        provider_id, *args, timeout_override=None, **kwargs
    ):  # noqa: ANN001
        seen.append(timeout_override)
        return {"text": "", "provider": provider_id, "error": "boom"}

    with mock.patch.object(router, "_call_single_provider", side_effect=fake_single):
        router.call_llm(
            _MSG,
            max_tokens=300,
            task_type=router.TASK_PLAN_STRUCTURED,
            use_cache=False,
            timeout_budget=10.0,
        )
    assert len(seen) >= 2, seen
    assert seen[0] <= 5.0 + 0.05, seen  # not the whole 10 s (short budget)
    assert seen[0] >= 1.5


def test_short_budget_still_reaches_the_fallback(router) -> None:
    """The prod trace: first provider burns 6.3 s of a 10 s budget; the old fixed
    5 s floor then refused to start Gemini. The fallback must still run."""
    real = time.time()
    clock = {"t": real}
    calls: List[str] = []

    def fake_single(
        provider_id, *args, timeout_override=None, **kwargs
    ):  # noqa: ANN001
        calls.append(provider_id)
        if provider_id == router.DEEPSEEK:
            clock["t"] += 6.3
            return {"text": "", "provider": provider_id, "error": "empty"}
        return {"text": "ok", "provider": provider_id, "latency_ms": 100}

    with mock.patch.object(router, "_call_single_provider", side_effect=fake_single):
        with mock.patch.object(router.time, "time", side_effect=lambda: clock["t"]):
            res = router.call_llm(
                _MSG,
                max_tokens=300,
                task_type=router.TASK_PLAN_STRUCTURED,
                preferred_providers=[router.DEEPSEEK],
                use_cache=False,
                timeout_budget=10.0,
            )
    assert res["text"] == "ok", (calls, res.get("error"))
    assert calls[0] == router.DEEPSEEK and len(calls) >= 2


# ---------------------------------------------------------------------------
# (c) copilot suggest chain
# ---------------------------------------------------------------------------


def test_copilot_suggest_chain_order(router) -> None:
    chain = router.TASK_ROUTING["copilot_suggest"]
    assert chain[0] in (router.GEMINI, router.GEMINI_FLASH_LITE), chain
    assert chain.index(router.GEMINI) < chain.index(router.DEEPSEEK)
    assert chain.index(router.GEMINI_FLASH_LITE) < chain.index(router.DEEPSEEK)
    assert router.DEEPSEEK in chain  # still a fallback
    assert router.CLAUDE_HAIKU in chain


def test_copilot_suggest_uses_deepseek_fast_model(router) -> None:
    assert "copilot_suggest" in router.DEEPSEEK_FAST_TASK_TYPES
    _u, _h, body = router._build_openai_request(
        router.DEEPSEEK, _MSG, "", 300, task_type="copilot_suggest"
    )
    assert json.loads(body)["model"] == "deepseek-v4-flash"


def test_deepseek_fast_tier_requests_disable_thinking(router) -> None:
    """Thinking is DeepSeek V4's default mode, flash included
    (api-docs.deepseek.com/guides/thinking_mode/): every fast-tier call must
    carry thinking.type=disabled, others must not."""
    for task in ("copilot_suggest", router.TASK_PLAN_NARRATIVE, router.TASK_NARRATIVE):
        _u, _h, body = router._build_openai_request(
            router.DEEPSEEK, _MSG, "", 300, task_type=task
        )
        assert json.loads(body)["thinking"] == {"type": "disabled"}, task
    _u, _h, body = router._build_openai_request(
        router.DEEPSEEK, _MSG, "", 300, task_type=router.TASK_PLAN_STRUCTURED
    )
    assert "thinking" not in json.loads(body)


def test_brief_suggestion_uses_copilot_suggest_task(monkeypatch) -> None:
    import types

    import app

    captured: Dict[str, Any] = {}

    def _call_llm_json(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return {"ok": True, "data": [{"text": "Add a timeline"}], "provider": "gemini"}

    fake = types.SimpleNamespace(
        call_llm_json=_call_llm_json, TASK_PLAN_STRUCTURED="plan_structured"
    )
    monkeypatch.setattr(app, "_lazy_llm_router", lambda: fake)
    app._copilot_suggest_brief("We need 200 warehouse hires in Texas by Q3.", {})
    assert captured["task_type"] == "copilot_suggest"
