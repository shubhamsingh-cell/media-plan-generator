"""Re-review 2026-11-03 LOW findings, each pinned."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import llm_router  # noqa: E402
import nova  # noqa: E402
import vector_search  # noqa: E402

fmt = nova._enrich_markdown_formatting


def test_factor_keys_are_not_money() -> None:
    out = nova._render_tool_data_plain(
        "Adjustments",
        {"currency": "EUR", "cost_per_quality_hire_factor": 1.1, "budget_adjustment_factor": 0.9},
    )
    assert "€" not in out, out


def test_small_liquid_route_after_claude_in_structured_chain() -> None:
    chain = llm_router.TASK_ROUTING[llm_router.TASK_STRUCTURED]
    assert chain.index(llm_router.OPENROUTER_LIQUID) > chain.index(llm_router.CLAUDE)


def test_word_adjacent_money_is_not_bolded() -> None:
    assert fmt("Code #A$100X here") == "Code #A$100X here"


def test_dot_grouped_figures_are_not_split() -> None:
    out = fmt("Custo R$1.200,50 por mes")
    assert "**R$1.200**" not in out and "**R$1**" not in out


def test_schemeless_domain_with_query_is_protected() -> None:
    out = fmt("see example.com?x=50% and shop.example.org/sale-20%-off today")
    assert "example.com?x=50%" in out and "shop.example.org/sale-20%-off" in out


def test_us_kb_chunk_is_labelled_when_the_question_names_no_country() -> None:
    chunk = [
        {
            "content": "[labor_market_outlook_2026.json] current_labor_market_may_2026\n"
            "unemployment_rate_note: Held steady; range 4.3-4.5% since July 2025\n"
            "nonfarm_payrolls_note: Beat ~80-85k consensus"
        }
    ]
    with mock.patch.object(vector_search, "search_bounded", lambda q, top_k=3, timeout_s=3.0: chunk):
        out = nova._fetch_kb_context_for_query("what is the unemployment rate trend")
    assert "United States" in out


def test_turkey_the_bird_is_not_the_country() -> None:
    assert nova._detect_country("turkey processing plant workers in Arkansas") == "United States"
    assert nova._detect_country("turkey farm hands needed") is None
    assert nova._countries_named("turkey meat plant jobs") == set()
    assert nova._detect_country("software engineers in Turkey") == "Turkey"
    assert nova._countries_named("jobs in Türkiye") == {"Turkey"} or nova._detect_country("jobs in Türkiye") in (None, "Turkey")
