"""One brief, one set of projected numbers -- on every surface.

Root cause this pins (investigation 2026-10-16, $60k / 3 months):

* the wizard's /api/estimate ran budget_engine (warehouse, Dallas: 19 hires,
  $3,158 per hire),
* Quick Plan ran its OWN formula (applications x a flat collar hire rate, no
  cost-per-hire floor): 245 hires, "$245 per hire" -- beside its own
  "$2,500 minimum viable" warning,
* Nova's projection tool defaulted the industry to "general" and guessed a
  role tier from keywords (an RN plan read 35 hires, the engine says 11),
* roi_projector printed an INVERTED cost-per-hire range (low 1,133 /
  expected 2,500 / high 1,888).

``plan_projection.project_plan`` is now the one front door; these tests hold
the surfaces to the same low/high for four shapes.
"""

from __future__ import annotations

import logging
import os
import sys
import types

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

for _name in (
    "anthropic",
    "openai",
    "supabase",
    "redis",
    "qdrant_client",
    "sentence_transformers",
    "posthog",
):
    if _name not in sys.modules:
        sys.modules[_name] = types.ModuleType(_name)

logging.disable(logging.CRITICAL)

# (role, location, industry key the wizard's dropdown would send)
SHAPES = [
    ("Warehouse Associate", "Dallas, TX", "logistics_supply_chain"),
    ("Registered Nurse", "Columbus, OH", "healthcare_medical"),
    ("Software Engineer", "Remote, US", "tech_engineering"),
    ("Customer Service Representative", "United States", "general_entry_level"),
]
BUDGET = 60_000
_METRICS = ("applications", "hires", "cpa", "cph")


def _wizard(role, loc, ind):
    import app

    return app._compute_plan_estimate(
        {
            "budget": "$60,000",
            "industry": ind,
            "roles": [role],
            "locations": [loc],
            "campaign_duration": "3 months",
            "client_name": "Acme",
        }
    )


def _quick(role, loc, ind):
    import quick_plan

    return quick_plan.generate_quick_plan(role, loc, BUDGET, ind)


def _nova(role, loc, ind):
    from nova import Nova

    nova = Nova.__new__(Nova)
    nova._data_cache = {}
    return nova._query_budget_projection(
        {"budget": BUDGET, "roles": [role], "locations": [loc], "industry": ind}
    )


def _band(proj):
    return {m: (proj[m]["low"], proj[m]["high"]) for m in _METRICS}


@pytest.mark.parametrize("role,loc,ind", SHAPES)
def test_quick_plan_nova_and_estimate_share_the_same_low_high(role, loc, ind):
    wiz = _wizard(role, loc, ind)["projection"]
    quick = _quick(role, loc, ind)["total_projected"]["projection"]
    nova = _nova(role, loc, ind)["projection"]
    assert _band(quick) == _band(wiz), f"Quick Plan != wizard for {role}"
    assert _band(nova) == _band(wiz), f"Nova != wizard for {role}"
    # and every band is a real range ordered low <= high
    for m in _METRICS:
        lo, hi = wiz[m]["low"], wiz[m]["high"]
        assert lo is not None and hi is not None and lo <= hi, (m, lo, hi)


@pytest.mark.parametrize("role,loc,ind", SHAPES)
def test_point_figures_agree_across_surfaces(role, loc, ind):
    """The legacy point keys must also stop disagreeing (pre-fix: Quick Plan
    245 hires vs wizard 19)."""
    wiz = _wizard(role, loc, ind)
    quick = _quick(role, loc, ind)["total_projected"]
    nova = _nova(role, loc, ind)["total_projected"]
    assert quick["total_hires"] == wiz["est_hires"] == nova["hires"]
    assert quick["total_applies"] == wiz["est_applications"] == nova["applications"]
    assert quick["blended_cph"] == pytest.approx(wiz["est_cph"], abs=0.01)


def test_quick_plan_never_reports_a_cost_per_hire_below_its_own_minimum():
    """Quick Plan said '$245 per hire' while warning the minimum viable
    cost per hire is $2,500."""
    for role, loc, ind in SHAPES:
        plan = _quick(role, loc, ind)
        cph_low = plan["total_projected"]["cph_low"]
        # the engine's plan-efficiency floor is half the industry midpoint
        assert cph_low >= 1_000, f"{role}: cost per hire {cph_low}"
        channel_hires = sum(c["projected_hires"] for c in plan["channel_recommendations"])
        assert channel_hires == plan["total_projected"]["total_hires"]


def test_nova_does_not_default_industry_to_general_when_roles_signal_one():
    out = _nova("Registered Nurse", "Columbus, OH", "general")
    assert out["industry"] == "healthcare_medical"
    assert out["industry_source"] == "classified_from_roles"
    assert out["total_projected"]["hires"] == 11


def test_brief_without_industry_classifies_from_role():
    import plan_projection

    proj = plan_projection.project_plan(
        {
            "budget": BUDGET,
            "roles": ["Registered Nurse"],
            "locations": ["Columbus, OH"],
        }
    )
    assert proj["ok"] and proj["industry"] == "healthcare_medical"
    assert proj["industry_source"] == "classified_from_roles"


def test_budget_engine_exposes_project_plan():
    import budget_engine

    proj = budget_engine.project_plan(
        {"budget": BUDGET, "roles": ["Warehouse Associate"], "locations": ["Dallas, TX"]}
    )
    assert proj["ok"] and set(_METRICS) <= set(proj)
    for m in _METRICS:
        assert set(proj[m]) == {"low", "mid", "high"}


def test_split_location_matches_the_apps_helper():
    import app
    import plan_projection

    for text in ("Dallas, TX", "Dallas, Texas", "London, United Kingdom", "Paris", "A, B, C"):
        assert plan_projection.split_location(text) == app._split_city_state_country(text)


def test_roi_projector_cost_per_hire_range_is_ordered():
    """Pre-fix: RN @ $10k read low 1,133 / expected 2,500 / high 1,888."""
    import roi_projector

    for industry in ("healthcare", "technology", "logistics", "retail", "general"):
        for channel in ("Indeed", "LinkedIn", "programmatic"):
            res = roi_projector.project_roi(channel, 10_000, industry, "Registered Nurse")
            cph = res["roi_metrics"]["cost_per_hire"]
            assert cph["low"] <= cph["expected"] <= cph["high"], (industry, channel, cph)
            p = res["projections"]
            assert p["pessimistic"]["hires"] <= p["expected"]["hires"] <= p["optimistic"]["hires"]
            assert p["optimistic"]["cpa"] <= p["expected"]["cpa"] <= p["pessimistic"]["cpa"]
