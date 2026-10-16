"""Regressions for production exceptions seen in the Render logs on 2026-10-16.

1. ``name 'urllib' is not defined`` on the Path B synthesis-force branch.
2. ``Unknown format code 'f' for object of type 'str'`` in the chat summary.
3. ``'str' object has no attribute 'get'`` in the Copilot Adzuna location lookup.
4. ``No currency mapping for country 'United States'``.
"""

from __future__ import annotations

import json
import sys
import types
from unittest.mock import MagicMock, patch

import nova


# ---------------------------------------------------------------------------
# 1. Path B synthesis-force must have urllib in scope
# ---------------------------------------------------------------------------
def test_path_b_synthesis_force_reaches_the_anthropic_call(monkeypatch, caplog):
    """Tool loop exhausts with tool results -> synthesis-force must POST, not NameError."""
    tool_call = {
        "id": "t1",
        "function": {"name": "query_channels", "arguments": "{}"},
    }

    def fake_call_llm(**kwargs):
        return {
            "provider": "stub",
            "text": "",
            "tool_calls": [tool_call],
            "raw_message": {"content": None},
        }

    fake_router = types.SimpleNamespace(
        call_llm=fake_call_llm,
        classify_task=lambda *_a, **_k: "x",
        TASK_COMPLEX="complex",
        TASK_CHATBOT_TOOL_CALL="tool",
        PROVIDER_CONFIG={},
        parallel_distribute=lambda **_k: [],
    )
    synthesized = "Synthesized answer with enough characters to clear the fifty char floor. " * 2
    body = json.dumps({"content": [{"type": "text", "text": synthesized}]}).encode()
    resp = MagicMock()
    resp.read.return_value = body
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key-not-real")
    import urllib.request

    n = nova.Nova.__new__(nova.Nova)
    with patch.dict(sys.modules, {"llm_router": fake_router}), patch.object(
        nova.Nova, "_get_tool_handler_names", lambda self: ["query_channels"]
    ), patch.object(
        nova.Nova, "execute_tool", lambda self, name, inp: json.dumps({"ok": 1})
    ), patch.object(
        nova.Nova, "get_tool_definitions", lambda self: []
    ), patch.object(
        nova, "get_tools_for_provider", lambda defs, provider_name=None: []
    ), patch.object(
        nova, "_bounded_vector_search", lambda *a, **k: []
    ), patch.object(
        urllib.request, "urlopen", lambda *a, **k: resp
    ):
        out = n._chat_with_free_llm_tools("compare job boards for nurses", None, None)

    assert "is not defined" not in caplog.text, caplog.text[-600:]
    assert out is not None, "synthesis-force raised (urllib NameError) and fell through"
    assert out["response"] == synthesized
    assert out["llm_provider"] == "claude_haiku"


# ---------------------------------------------------------------------------
# 2. Chat summary must tolerate string-valued numbers from the client context
# ---------------------------------------------------------------------------
_STRING_CONTEXT = {
    "budget": "$50,000",
    "enriched": {
        "salary": {
            "median": "85000",
            "min": "60,000",
            "max": "110000",
            "percentile_25": "70000",
            "percentile_75": "95000",
        },
        "demand": {"job_count": "1,200"},
    },
}


def test_summarize_enrichment_accepts_string_numbers():
    out = nova._summarize_enrichment(_STRING_CONTEXT)
    assert "Budget: $50,000" in out
    assert "Median: $85,000" in out
    assert "Range: $60,000-$110,000" in out
    assert "P25-P75: $70,000-$95,000" in out
    assert "1,200" in out


def test_summarize_enrichment_keeps_unparseable_text_instead_of_raising():
    out = nova._summarize_enrichment(
        {"budget": "about fifty grand", "enriched": {"salary": {"median": "n/a"}}}
    )
    assert "about fifty grand" in out
    assert "n/a" in out


def test_one_word_query_with_string_budget_context_does_not_raise():
    """Reachable path: the free-LLM tool path builds the prompt from the context."""
    fake_router = types.SimpleNamespace(
        call_llm=lambda **k: {"provider": "stub", "text": "ok " * 40},
        classify_task=lambda *_a, **_k: "x",
        TASK_COMPLEX="complex",
        TASK_CHATBOT_TOOL_CALL="tool",
        PROVIDER_CONFIG={},
        parallel_distribute=lambda **_k: [],
    )
    n = nova.Nova.__new__(nova.Nova)
    with patch.dict(sys.modules, {"llm_router": fake_router}), patch.object(
        nova.Nova, "_get_tool_handler_names", lambda self: []
    ), patch.object(nova.Nova, "get_tool_definitions", lambda self: []), patch.object(
        nova, "get_tools_for_provider", lambda defs, provider_name=None: []
    ), patch.object(
        nova, "_bounded_vector_search", lambda *a, **k: []
    ):
        # Must not raise ValueError("Unknown format code 'f' for object of type 'str'").
        n._chat_with_free_llm_tools("nurses", None, dict(_STRING_CONTEXT))


# ---------------------------------------------------------------------------
# 3. Copilot Adzuna location lookup: a string `location` must not raise
# ---------------------------------------------------------------------------
def test_copilot_adzuna_string_location_is_not_an_error():
    import app

    stub = types.SimpleNamespace(
        search_jobs=lambda *a, **k: {
            "results": [
                {"location": "Austin, Texas"},  # string, not {"display_name": ...}
                {"location": {"display_name": "Denver, Colorado"}},
                "not-a-job-dict",
                {"location": None},
            ]
        }
    )
    with patch.object(app, "_api_integrations_available", True), patch.object(
        app, "_api_adzuna", stub
    ):
        out = app._copilot_suggest_locations("", "healthcare", "", ["nurse"], [])

    texts = [s["text"] for s in out]
    assert "Austin, Texas" in texts
    assert "Denver, Colorado" in texts


# ---------------------------------------------------------------------------
# 4. Long country names resolve a currency through the shared alias table
# ---------------------------------------------------------------------------
def test_long_country_names_resolve_without_a_missing_mapping_warning(caplog):
    expected = {
        "United States": "USD",
        "United States of America": "USD",
        "USA": "USD",
        "United Kingdom": "GBP",  # already in the nova table: unchanged
        "UK": "GBP",
        "Great Britain": "GBP",
        "Germany": "EUR",
    }
    with caplog.at_level("WARNING", logger="nova"):
        for country, code in expected.items():
            caplog.clear()
            assert nova._get_currency_for_country(country) == code, country
            assert "No currency mapping" not in caplog.text, caplog.text


def test_unknown_country_still_defaults_to_usd_and_warns(caplog):
    with caplog.at_level("WARNING", logger="nova"):
        assert nova._get_currency_for_country("Atlantis") == "USD"
    assert "No currency mapping for country 'Atlantis'" in caplog.text
