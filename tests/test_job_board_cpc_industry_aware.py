"""Regression: global job-board CPC follows the industry (audit 2026-10-01,
F §3.7 / §4.5).

Pre-fix (f99beef): budget_engine's CPC cascade tried the flat
``live_benchmark`` tier (one Indeed figure, $1.62) before the industry-aware
trend_engine tier, so every industry's Global Job Boards channel priced at
exactly $1.62 -- retail (repo KB $0.35, trend_engine ~$0.7) and tech (repo
KB $0.80, trend_engine ~$2-3) alike.
"""

from __future__ import annotations

import budget_engine as be

_CHANNELS = {
    "programmatic_dsp": 35,
    "global_boards": 20,
    "niche_boards": 15,
    "social_media": 12,
    "regional_boards": 8,
    "employer_branding": 5,
}


def _job_board(industry, role, city, state):
    res = be.calculate_budget_allocation(
        total_budget=250_000.0,
        roles=[{"title": role, "count": 1, "tier": "Professional"}],
        locations=[{"city": city, "state": state, "country": "United States"}],
        industry=industry,
        channel_percentages=dict(_CHANNELS),
        knowledge_base=None,
        campaign_start_month=10,
    )
    return res["channel_allocations"]


def test_retail_and_tech_job_board_cpcs_differ_in_the_kb_direction():
    retail = _job_board("retail_consumer", "Retail Sales Associate", "Columbus", "OH")
    tech = _job_board("tech_engineering", "Software Developer", "Austin", "TX")
    r, t = retail["global_boards"], tech["global_boards"]
    assert r["cpc_source"] == "trend_engine", r["cpc_source"]
    assert t["cpc_source"] == "trend_engine", t["cpc_source"]
    assert r["cpc"] < 1.62 < t["cpc"], (r["cpc"], t["cpc"])


def test_indeed_priced_regional_boards_keep_their_industry_cpc():
    """job_board and regional both price off trend_engine's Indeed series;
    the shared-fallback dedup must not flatten both to the static table."""
    chans = _job_board("retail_consumer", "Retail Sales Associate", "Columbus", "OH")
    for name in ("global_boards", "regional_boards"):
        assert not str(chans[name]["cpc_source"]).startswith("static_benchmark"), (
            name,
            chans[name]["cpc_source"],
        )
    assert chans["global_boards"]["cpc"] == chans["regional_boards"]["cpc"]


def test_social_keeps_its_existing_source():
    """Deliberately unchanged: trend_engine's Meta series is general
    commercial 2025 data, further from LocaliQ 2026 than the live tier."""
    chans = _job_board("tech_engineering", "Software Developer", "Austin", "TX")
    assert chans["social_media"]["cpc_source"] == "live_benchmark"
