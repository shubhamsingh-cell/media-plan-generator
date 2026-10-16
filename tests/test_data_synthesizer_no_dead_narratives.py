"""data_synthesizer no longer builds ``ai_narratives``.

Verified 2026-10-16 before the change:

  * nothing reads ``ai_narratives`` -- not the deck, the workbook, a template
    or the API (``grep -rn ai_narratives`` hits only data_synthesizer.py and a
    handoff note);
  * for a real request the LLM call never fired: app.py rewrites
    ``budget`` to a canonical string ("$1,500,000"), and
    ``_build_narrative_context`` formatted it with ``f"${budget:,.0f}"`` ->
    ``ValueError: Unknown format code 'f' for object of type 'str'``,
    swallowed by synthesize()'s try/except, leaving ``ai_narratives = {}``;
  * for a request with a NUMERIC budget the call did fire (one 10 s-budget
    ``call_llm_json`` per synthesis) and its result was thrown away.

So the call path was either a silent crash or paid latency and cost for
output nobody sees. It is gone; these tests keep it gone.
"""

from __future__ import annotations

import os
import re
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import data_synthesizer  # noqa: E402
import llm_router  # noqa: E402

_BASE_INPUT = {
    "company_name": "Acme Corp",
    "industry": "healthcare_medical",
    "roles": ["Registered Nurse"],
    "locations": ["Austin, TX"],
}


@pytest.fixture
def llm_calls(monkeypatch):
    """Record every LLM-router entry point synthesize() could reach."""
    calls: list[str] = []

    def _recorder(name):
        def _fake(*args, **kwargs):
            calls.append(name)
            return {"ok": True, "data": {"executive_summary": "x"}, "provider": "fake"}

        return _fake

    for name in ("call_llm_json", "call_llm"):
        if hasattr(llm_router, name):
            monkeypatch.setattr(llm_router, name, _recorder(name))
    return calls


class TestSynthesizeSpendsNothingOnNarratives:
    # "$50,000" is what app.py hands over; 50000 is the shape that used to fire
    # the paid call; 0 and None exercise the remaining branches.
    @pytest.mark.parametrize("budget", ["$50,000", 50000, 0, None])
    def test_no_ai_narratives_key_and_no_llm_call(self, llm_calls, budget):
        result = data_synthesizer.synthesize({}, {}, dict(_BASE_INPUT, budget=budget))
        assert "ai_narratives" not in result
        assert llm_calls == [], (
            "synthesize() reached the LLM router for output nobody renders: "
            f"{llm_calls}"
        )

    def test_other_synthesis_sections_are_still_produced(self, llm_calls):
        result = data_synthesizer.synthesize(
            {}, {}, dict(_BASE_INPUT, budget="$50,000")
        )
        for key in ("salary_intelligence", "confidence_scores", "data_quality"):
            assert key in result, key


class TestDeadCodeIsGone:
    def test_generator_functions_removed(self):
        assert not hasattr(data_synthesizer, "generate_ai_narratives")
        assert not hasattr(data_synthesizer, "_build_narrative_context")

    def test_generator_source_never_mentions_ai_narratives(self):
        path = os.path.join(PROJECT_ROOT, "data_synthesizer.py")
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        assert not re.search(r"ai_narrative", source, re.IGNORECASE)

    def test_no_other_module_reads_the_key(self):
        # the consumers of a synthesis dict
        for module in ("ppt_generator.py", "excel_v2.py", "app.py", "nova.py"):
            path = os.path.join(PROJECT_ROOT, module)
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as handle:
                assert "ai_narratives" not in handle.read(), module
