"""Regressions from an end-to-end regeneration of Brendan Macomber's Hershey
plan (2026-09-24): The Hershey Company, Food & Beverage manufacturing,
$90,000, 8 plant roles, 10 sites, Employer Branding NOT selected.

Driving the real /api/generate handler locally surfaced six defects the
offline harness (tools_regen_bundles.py) cannot see, because it skips the
request boundary and the Phase-0 standardizer:

1. The boundary sanitizer str()-ed every JSON boolean, so the wizard's
   channel_categories toggles arrived as "True"/"False" (both truthy) and
   the unselected Employer Branding channel still got $2,700.
2. standardizer aliased the wizard key "food_beverage" to "hospitality"
   and app.py overwrote data["industry"] with it BEFORE classify_industry,
   so a confectionery manufacturer was planned as Hospitality & Travel.
3. h1b_data matched on any single shared word: "Machine Operator" got the
   Data Scientist H-1B wage ($140K), "Electrical Technician" the
   Electrical Engineer wage ($115K).
4. data_synthesizer's keyword fallback matched "product" inside
   "Production Supervisor" ($130K Product-Manager band).
5. A single-point salary produced Min > P25 and Max < P75.
6. Market Intelligence and Quality Intelligence / the deck priced the
   same role differently (per_role_salaries convergence was driver-only).
Plus the still-needed food_beverage niche-board data tables (ported from
the unshipped worktree-agent-a2fea5303ae70ae85 branch, data only).
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app  # noqa: E402
import data_synthesizer  # noqa: E402
import h1b_data  # noqa: E402
import standardizer  # noqa: E402

_ROOT = os.path.join(os.path.dirname(__file__), "..")

_HERSHEY_ROLES = [
    "Industrial Maintenance Technician",
    "Lead Electrical Controls Specialist",
    "Electrical Technician",
    "Electrical Controls Engineer",
    "Production Supervisor",
    "Machine Operator",
    "Quality Assurance Technician",
    "Warehouse Associate",
]

_RESTAURANT_BOARDS = {
    "iHireHospitality",
    "CulinaryAgents.com",
    "Culinary Agents",
    "Poached Jobs",
    "Poached",
    "Restaurant Business Jobs",
    "QSRMagazine Careers",
    "National Restaurant Association Jobs",
}


# ── 1. Unselected channel must receive no budget ──────────────────────────


def _hospitality_like_pcts() -> dict:
    return {
        "programmatic_dsp": 40,
        "global_boards": 22,
        "niche_boards": 8,
        "social_media": 20,
        "regional_boards": 7,
        "employer_branding": 3,
    }


def test_sanitized_wizard_payload_keeps_employer_branding_unfunded():
    """The wizard's JSON false for Employer Branding must survive the
    request-boundary sanitizer AND be honoured by channel selection."""
    payload = {
        "channel_categories": {
            "programmatic_dsp": True,
            "global_boards": True,
            "niche_boards": True,
            "social_media": True,
            "regional_boards": True,
            "employer_branding": False,
        }
    }
    sanitized = {k: app._sanitize_request_value(v) for k, v in payload.items()}
    assert sanitized["channel_categories"]["employer_branding"] is False
    out = app._apply_channel_selection(_hospitality_like_pcts(), sanitized)
    assert "employer_branding" not in out
    assert abs(sum(out.values()) - 100) < 1e-6


def test_stringified_false_is_still_an_explicit_off():
    """Defence in depth: a legacy/stringified "False" is an off toggle,
    not a truthy value (this is exactly what the old sanitizer produced)."""
    data = {
        "channel_categories": {"employer_branding": "False", "social_media": "True"}
    }
    out = app._apply_channel_selection(_hospitality_like_pcts(), data)
    assert "employer_branding" not in out
    assert "social_media" in out


def test_sanitizer_still_strips_markup_and_keeps_numbers_as_strings():
    assert app._sanitize_request_value("<b>Hershey</b>") == "Hershey"
    assert app._sanitize_request_value({"n": 5}) == {"n": "5"}
    assert app._sanitize_request_value([True, None]) == [True, ""]


# ── 2. Food & Beverage manufacturer must not become hospitality ──────────


def test_wizard_food_beverage_key_is_not_canonicalised_to_hospitality():
    canon = standardizer.normalize_industry("food_beverage")
    assert canon != "hospitality"
    assert canon == "food_beverage"
    meta = standardizer.CANONICAL_INDUSTRIES[canon]
    assert meta["deep_bench_key"] == "food_beverage"
    assert meta["kb_key"] == "manufacturing"


def test_standardized_food_beverage_still_classifies_as_food_manufacturing():
    """app.py feeds the standardizer's canonical key into classify_industry;
    that must land on food_beverage (NAICS 31), not hospitality_travel."""
    canon = standardizer.normalize_industry("food_beverage")
    profile = app.classify_industry(canon, "The Hershey Company", _HERSHEY_ROLES)
    assert profile["legacy_key"] == "food_beverage"
    assert profile["bls_sector"] == "Manufacturing"


def test_restaurants_still_route_to_hospitality():
    assert standardizer.normalize_industry("restaurant") == "hospitality"
    assert standardizer.normalize_industry("hospitality_travel") == "hospitality"


def test_food_beverage_benchmark_maps_point_at_manufacturing():
    assert app._INDUSTRY_KEY_TO_KB_KEY["food_beverage"] == "manufacturing"
    assert data_synthesizer._INDUSTRY_TO_KB_KEY["food_beverage"] == "manufacturing"
    assert (
        data_synthesizer._INDUSTRY_TO_GOOGLE_ADS_CATEGORY["food_beverage"]
        == data_synthesizer._INDUSTRY_TO_GOOGLE_ADS_CATEGORY["manufacturing"]
    )


def test_food_beverage_deep_benchmark_cph_is_not_accommodation_figure():
    with open(os.path.join(_ROOT, "data", "recruitment_benchmarks_deep.json")) as fh:
        deep = json.load(fh)
    fb = (deep.get("industry_benchmarks") or deep)["food_beverage"]
    assert "accommodation" not in (fb["cph"].get("total_cost_per_hire") or "")


# ── 3/4. Salary sources must match the occupation ────────────────────────


def test_h1b_does_not_price_plant_roles_off_white_collar_occupations():
    for role in (
        "Machine Operator",
        "Electrical Technician",
        "Lead Electrical Controls Specialist",
        "Industrial Maintenance Technician",
        "Warehouse Associate",
    ):
        assert h1b_data._normalize_role(role) is None, role


def test_h1b_still_matches_genuine_titles():
    assert h1b_data._normalize_role("Senior Software Engineer") == "software_engineer"
    assert h1b_data._normalize_role("RN") == "registered_nurse"
    assert (
        h1b_data._normalize_role("Electrical Controls Engineer")
        == "electrical_engineer"
    )
    assert h1b_data._normalize_role("Data Scientist II") == "data_scientist"


def test_production_supervisor_is_not_priced_as_product_manager():
    assert not data_synthesizer._title_has_keyword("production supervisor", "product")
    assert data_synthesizer._title_has_keyword("senior product manager", "product")
    assert data_synthesizer._title_has_keyword("warehouse associates", "warehouse")


def _fuse(roles: list) -> dict:
    return data_synthesizer.fuse_salary_intelligence(
        {},
        {},
        {"roles": roles, "locations": ["Hershey, PA"], "industry": "food_beverage"},
    )


def test_hershey_plant_roles_get_no_h1b_or_product_salary():
    result = _fuse(["Machine Operator", "Production Supervisor"])
    for role in ("Machine Operator", "Production Supervisor"):
        sal = result.get(role) or {}
        assert "DOL H-1B/LCA" not in (sal.get("sources") or []), role
        assert (sal.get("median") or 0) < 100000, (role, sal.get("median"))


# ── 5. Salary ladder is monotonic ─────────────────────────────────────────


def test_single_point_salary_ladder_is_monotonic():
    result = _fuse(["Electrical Controls Engineer"])
    sal = result["Electrical Controls Engineer"]
    ladder = [sal["min"], sal["p25"], sal["median"], sal["p75"], sal["max"]]
    assert ladder == sorted(ladder), ladder


# ── 6. One salary per role across sheets ──────────────────────────────────


def test_per_role_salaries_cover_every_priced_role_not_only_drivers():
    synth = data_synthesizer.synthesize(
        {},
        {},
        {
            "roles": ["Electrical Controls Engineer", "Warehouse Associate"],
            "locations": ["Hershey, PA"],
            "industry": "food_beverage",
        },
    )
    per_role = synth.get("per_role_salaries") or {}
    sal_intel = synth.get("salary_intelligence") or {}
    for role in ("Electrical Controls Engineer", "Warehouse Associate"):
        assert role in per_role, role
        assert per_role[role]["median"] == sal_intel[role]["median"]


# ── 7. Niche boards: manufacturing, not restaurant ───────────────────────


def test_app_food_beverage_niche_fallback_has_no_restaurant_boards():
    boards = set(app.INDUSTRY_NICHE_CHANNELS["food_beverage"])
    assert not boards & _RESTAURANT_BOARDS, boards & _RESTAURANT_BOARDS


def test_channels_db_food_beverage_niche_list_has_no_restaurant_boards():
    with open(os.path.join(_ROOT, "data", "channels_db.json")) as fh:
        db = json.load(fh)
    boards = set(db["traditional_channels"]["niche_by_industry"]["food_beverage"])
    assert not boards & _RESTAURANT_BOARDS, boards & _RESTAURANT_BOARDS
