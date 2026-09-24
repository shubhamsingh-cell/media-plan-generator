"""Regression test for the Hershey Company client-competitor-substitution bug.

Real shipped defect (Hershey Company plan, reported via Slack, 2026-09-24):
the client typed an explicit, distinct competitor list into the wizard's
"Client's Key Competitors" field (``data["competitors"]``), but the
generated workbook's Market Intelligence sheet "Competitor Analysis" table
showed ENTIRELY DIFFERENT companies -- ones the client never named.

NOT the same bug as tests/test_uber_competitor_claim_fix.py (FIX A there
covers the STATIC per-industry fallback roster stamping invented "Active
(est.)" precision when NO competitor data exists at all, and its own
false-positive guard only exercises the case where comp_intel and the brief
happen to name the SAME companies). This is a distinct precedence bug: when
comp_intel (data["_synthesized"]["competitive_intelligence"]["competitors"],
populated by data_synthesizer.fuse_competitive_intelligence from Clearbit-
resolved competitor logos) has ANY entries, it silently replaces the
client's own brief-supplied list wholesale.

Root cause (excel_v2.py, ``_build_sheet_market_intelligence``, "Competitors
table" section, ~line 7088 pre-fix): the code read

    comp_analysis = comp_intel.get(
        "competitors", comp_intel.get("competitor_analysis") or []
    )
    if not comp_analysis and competitors:
        comp_analysis = [{"name": c} for c in competitors]

-- checking the synthesized/enriched ``comp_intel`` dict BEFORE the client's
own ``data["competitors"]`` list. Whenever comp_intel had ANY entries (a
normal outcome whenever Clearbit enrichment resolves at least one competitor
logo for ANY query), the client's explicit list was discarded entirely, with
no merge and no disclosure. Every OTHER competitor-rendering site in this
codebase already gets this right (ppt_generator.py's competitor cards,
gold_standard.build_competitor_map, and this same file's Quality
Intelligence "Competitive Landscape" table) -- this Market Intelligence
sheet table was the one site still doing it backwards.

Fix: when the brief supplies competitors, they are used verbatim (one row
per client-named company); comp_intel may only supply supplemental fields
(domain, logo, etc.) for those SAME client-named companies via a
case-insensitive name match, never substitute a different company list.
comp_intel's own list is only used when the brief supplied no competitors
at all (matching the existing FIX A static-fallback behavior).

Runs under pytest, or standalone: ``python3 tests/test_competitor_client_input_preserved.py``.
"""

from __future__ import annotations

import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import openpyxl  # noqa: E402

import excel_v2  # noqa: E402
import research  # noqa: E402


def _sheet_text(ws) -> str:
    parts = []
    for row in ws.iter_rows(values_only=True):
        for val in row:
            if val is not None:
                parts.append(str(val))
    return "\n".join(parts)


def _build_market_intel_ws(data: dict):
    wb = openpyxl.Workbook()
    ws = wb.active
    excel_v2._build_sheet_market_intelligence(ws, data, research_mod=research)
    return ws


def _hershey_data(**overrides) -> dict:
    data = {
        "client_name": "Hershey Company",
        "industry": "retail_consumer",
        "locations": ["Hershey, PA"],
        "roles": ["Production Associate"],
        "target_roles": ["Production Associate"],
        "budget": "$500,000",
        "competitors": ["Acme Confections", "Brightline Sweets"],
        "_enriched": {},
    }
    data.update(overrides)
    return data


def test_brief_competitors_win_over_synthesized_competitive_intelligence():
    """The real Hershey bug shape: comp_intel (synthesized/enriched, e.g.
    from Clearbit competitor-logo resolution) names DIFFERENT companies
    than the client's own brief. The client's explicit list must render;
    comp_intel's unrelated companies must NOT appear at all."""
    data = _hershey_data(
        _synthesized={
            "competitive_intelligence": {
                "competitors": {
                    "Mars Wrigley": {"domain": "mars.com"},
                    "Nestle": {"domain": "nestle.com"},
                },
            },
        },
    )
    text = _sheet_text(_build_market_intel_ws(data))
    assert "Acme Confections" in text, "client-named competitor missing"
    assert "Brightline Sweets" in text, "client-named competitor missing"
    assert "Mars Wrigley" not in text, "unrelated synthesized competitor leaked in"
    assert "Nestle" not in text, "unrelated synthesized competitor leaked in"


def test_comp_intel_may_supplement_brief_competitors_by_matching_name():
    """comp_intel is allowed to enrich the CLIENT's own named competitors
    (e.g. attach a resolved domain) -- it just must not substitute a
    different company list. Case-insensitive match on the brief's own
    names must still pull in that supplemental detail."""
    data = _hershey_data(
        _synthesized={
            "competitive_intelligence": {
                "competitors": {
                    "acme confections": {"domain": "acmeconfections.example"},
                },
            },
        },
    )
    text = _sheet_text(_build_market_intel_ws(data))
    assert "Acme Confections" in text
    assert "Brightline Sweets" in text


def test_comp_intel_used_only_when_brief_has_no_competitors():
    """False-positive guard: when the brief truly supplied NO competitors,
    comp_intel's own (synthesized) list is legitimate content and must
    still render -- this table isn't required to always be brief-only."""
    data = _hershey_data(competitors=[])
    data["_synthesized"] = {
        "competitive_intelligence": {
            "competitors": {
                "Mars Wrigley": {"domain": "mars.com"},
            },
        },
    }
    text = _sheet_text(_build_market_intel_ws(data))
    assert "Mars Wrigley" in text


if __name__ == "__main__":
    import pytest as _pytest

    raise SystemExit(_pytest.main([__file__, "-v"]))
