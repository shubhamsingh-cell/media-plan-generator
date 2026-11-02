"""When the writing model is down, a direct tool answer is readable and honest.

2026-11-03 chat sweep: with every LLM provider unavailable, "cpc for tech in
Berlin" returned the tool's raw JSON in a code block: euro figures with no
symbol, the internal instruction "Format every monetary figure with EUR; do
NOT use $" shown to the user, and percentages bolded inside the JSON. That
degraded answer was also cached, so it kept being served after the model
came back.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nova  # noqa: E402


@pytest.fixture(scope="module")
def nv():
    return nova.Nova()


def _dispatch(nv, message: str):
    cached: list = []
    with mock.patch("llm_router.call_llm", side_effect=RuntimeError("no providers")), mock.patch.object(
        nova, "_set_response_cache", lambda k, v: cached.append(k)
    ), mock.patch.object(nova, "_intelligent_cache_set", lambda *a, **k: cached.append("semantic")):
        res = nv._try_direct_tool_dispatch(
            message, message.lower(), [], time.time(), "ck-degraded-test", None
        )
    return res, cached


def test_cpc_answer_without_llm_is_plain_local_currency(nv) -> None:
    res, _ = _dispatch(nv, "cpc for tech in Berlin")
    text = res["response"]
    assert "```" not in text and '"current_cpc"' not in text
    assert "€" in text  # euro figures carry the euro sign
    assert "do NOT use" not in text and "currency_note" not in text
    assert "usd_rate" not in text
    assert "unavailable" in text.lower()  # says why the answer is bare


def test_degraded_answer_is_not_cached(nv) -> None:
    _, cached = _dispatch(nv, "cpc for tech in Berlin")
    assert cached == []


def test_renderer_formats_money_and_drops_internal_fields() -> None:
    out = nova._render_tool_data_plain(
        "CPC trend",
        {
            "current_cpc": {"indeed": 1.36, "linkedin": 2.18},
            "trend": {"indeed": "-5%"},
            "currency": "GBP",
            "currency_symbol": "£",
            "usd_rate": 1.33,
            "currency_note": "Format every monetary figure with GBP",
            "_internal": "x",
            "source": "Nova CPC Trend Tracker",
        },
    )
    assert "Indeed £1.36" in out and "LinkedIn £2.18" in out
    assert "-5%" in out
    for leaked in ("usd_rate", "Format every monetary", "_internal", "1.33"):
        assert leaked not in out


def test_ratio_and_count_fields_never_get_a_currency_symbol() -> None:
    """Review 2026-11-03: money was chosen by substring, so
    "Budget Share: €35.00", "Cost Index: €1.08", "Forecast Hires: €1,200"."""
    out = nova._render_tool_data_plain(
        "Market",
        {
            "currency": "EUR",
            "cost_of_living_index": 1.08,
            "cpc_change_pct": -5.0,
            "salary_premium_pct": 12.5,
            "budget_share": 35.0,
            "forecast_hires": 1200,
            "salary_growth": 3.1,
            "cost_ratio": 0.4,
            "median_salary": 61000,
        },
    )
    for line in out.splitlines():
        if line.startswith("- Median Salary"):
            assert "€61,000" in line
        elif line.startswith("- "):
            assert "€" not in line, line
