"""Thin-answer KB context never shows index internals to the user.

2026-11-03 chat sweep: the "### Additional Context" block appended raw
retrieval chunks -- "[supply_ecosystem_intelligence.json]
programmatic_ecosystem.key_concepts.apply_rate.by_industry" headers, snake
case keys, and text cut mid-word ("standing-up jo").
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import nova  # noqa: E402
import vector_search  # noqa: E402

CHUNKS = [
    {
        "content": "[supply_ecosystem_intelligence.json] programmatic_ecosystem.key_concepts.apply_rate.by_industry\n"
        "healthcare: Lower than average (hardest to fill)\n"
        "construction_trades: Lower than average (standing-up jobs with long shifts and a lot of extra words here to pass the cut)"
    },
    {"content": "[international_sources.json] sources.eurostat.datasets.unemployment\ndescription: Unemployment rate by age and sex"},
]


def _ctx() -> str:
    with mock.patch.object(vector_search, "search_bounded", lambda q, top_k=3, timeout_s=3.0: CHUNKS):
        return nova._fetch_kb_context_for_query("apply rate healthcare")


def test_no_file_or_key_path_headers() -> None:
    out = _ctx()
    assert out
    assert ".json]" not in out and "key_concepts" not in out
    assert "construction_trades" not in out  # keys read as words


def test_text_is_cut_on_a_word_boundary() -> None:
    out = _ctx()
    for line in out.splitlines():
        assert not line.rstrip().endswith(" jo"), line
        assert len(line) <= 230


# Review 2026-11-03: dropping the key path dropped the ENTITY. For a Germany
# question the after-transcript (13_oecd_data.json) showed
# "- Oct dec 2025: 5.2% (highest since 2021)" -- the UK's unemployment rate --
# and an unlabelled US labour-market note.
OECD_CASE = [
    {"content": "[international_sources.json] sources.eurostat.datasets.unemployment\ndescription: Unemployment rate by age and sex"},
    {"content": "[labor_market_outlook_2026.json] current_labor_market_may_2026\nunemployment_rate_note: Held steady; range 4.3-4.5% since July 2025\nnonfarm_payrolls_note: Beat ~80-85k consensus"},
    {"content": "[recruitment_industry_knowledge.json] regional_data.united_kingdom.unemployment_rate\noct_dec_2025: 5.2% (highest since 2021)"},
]
GERMANY_Q = "What is the current unemployment rate in Germany according to OECD data, and how does it compare to France?"


def _ctx_for(query: str, chunks) -> str:
    with mock.patch.object(vector_search, "search_bounded", lambda q, top_k=3, timeout_s=3.0: chunks):
        return nova._fetch_kb_context_for_query(query)


def test_other_country_chunks_are_dropped() -> None:
    out = _ctx_for(GERMANY_Q, OECD_CASE)
    assert "5.2%" not in out  # the UK figure never answers a Germany question
    assert "nonfarm" not in out.lower()  # nor the US payrolls note


def test_entity_label_kept_when_region_matches() -> None:
    out = _ctx_for("unemployment rate in the UK", OECD_CASE)
    assert "United Kingdom · unemployment rate" in out
    assert "5.2%" in out
