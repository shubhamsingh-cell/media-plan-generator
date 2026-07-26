"""Regression tests for the salary-intelligence defect class (2026-07).

Real shipped defect (Uber, GBP 2M, "commercial cab driver", UK): the
Market Intelligence sheet's salary table (data_synthesizer.
fuse_salary_intelligence, via the module-level _ROLE_SALARY_FALLBACKS
dict) and the Quality Intelligence sheet's salary table (gold_standard.
enrich_city_level_data, via its own _ROLE_SALARY_RANGES dict) each
independently derived a salary for the SAME role in the SAME market and
disagreed by 53% (median 52,000 vs 34,000) -- both numbers were actually a
US SALARIED trucking/delivery-employee wage stamped onto a UK GIG/
private-hire cab driver, with a hardcoded 50% "confidence" that was never
computed from anything.

Three root causes, all fixed at the data source (data_synthesizer.py /
research.py), not in the renderers:
    1. Wrong wage model -- no gig/contract/on-demand bucket existed at all;
       "driver" was a single flat keyword conflating a passenger-transport
       gig contractor with a salaried CDL/trucking employee.
    2. No currency/market handling -- a USD-basis figure got stamped with
       whatever symbol the plan used, unlabelled.
    3. Confidence was a hardcoded literal (renderer's ``.get("confidence",
       0.5)`` default), never a real, derived value.

Fix: research.resolve_driver_role_wage() is now the ONE place a
driver-family role resolves to a wage. data_synthesizer.
fuse_salary_intelligence() calls it directly (Market Intelligence);
data_synthesizer.synthesize() copies its result into
synthesis["per_role_salaries"], which gold_standard.
enrich_city_level_data() reads as a verbatim override for the same role
(Quality Intelligence) -- so the two sheets can no longer take independent
code paths to the same role+market.
"""

from __future__ import annotations

import data_synthesizer
import gold_standard as gs
import research


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _uk_cab_driver_plan() -> dict:
    return {
        "roles": ["Commercial Cab Driver"],
        "target_roles": [{"title": "Commercial Cab Driver", "count": 500}],
        "locations": ["United Kingdom"],
        "industry": "hospitality_travel",
    }


def _run_full_pipeline(input_data: dict) -> tuple[dict, dict]:
    """Run the real two-stage pipeline (synthesize, then quality gates) the
    way app.py does, and return (market_intelligence_row, quality_
    intelligence_row) for the plan's single role."""
    synthesis = data_synthesizer.synthesize({}, {}, input_data)
    data = dict(input_data)
    data["_synthesized"] = synthesis
    city_data = gs.enrich_city_level_data(data)
    city_name = next(iter(city_data))
    per_role = city_data[city_name]["per_role_salary"]
    role_title = input_data["target_roles"][0]["title"]
    return synthesis["salary_intelligence"][role_title], per_role[role_title]


# ---------------------------------------------------------------------------
# 1. Headline defect: the two tables must agree for the same role+market.
# ---------------------------------------------------------------------------


def test_market_and_quality_intelligence_agree_for_uk_cab_driver():
    """The actual reported defect: Market Intelligence's salary_intelligence
    and Quality Intelligence's per_role_salary must resolve to the SAME
    figures for "Commercial Cab Driver" in the UK -- not just both be
    non-empty. Pre-fix these disagreed by 53% (52,000 vs 34,000 median)."""
    mi_row, qi_row = _run_full_pipeline(_uk_cab_driver_plan())

    assert mi_row["min"] == qi_row["min"], (mi_row, qi_row)
    assert mi_row["median"] == qi_row["median"], (mi_row, qi_row)
    assert mi_row["max"] == qi_row["max"], (mi_row, qi_row)
    assert mi_row["p25"] == qi_row["p25"], (mi_row, qi_row)
    assert mi_row["p75"] == qi_row["p75"], (mi_row, qi_row)


def test_market_and_quality_intelligence_agree_for_generic_driver_multi_market():
    """Equality must hold for driver-family roles generically, not only the
    one keyword phrase in the shipped incident -- check a plain "Driver"
    role too, in a different (US) market."""
    plan = {
        "roles": ["Driver"],
        "target_roles": [{"title": "Driver", "count": 10}],
        "locations": ["United States"],
        "industry": "logistics_supply_chain",
    }
    mi_row, qi_row = _run_full_pipeline(plan)
    assert mi_row["median"] == qi_row["median"], (mi_row, qi_row)
    assert mi_row["min"] == qi_row["min"], (mi_row, qi_row)
    assert mi_row["max"] == qi_row["max"], (mi_row, qi_row)


# ---------------------------------------------------------------------------
# 2. Wrong wage model: gig driver must not resolve to salaried trucking.
# ---------------------------------------------------------------------------


def test_uk_gig_driver_does_not_resolve_to_us_salaried_trucking_figure():
    """Pre-fix: "Commercial Cab Driver" in the UK resolved to the flat
    _ROLE_SALARY_FALLBACKS["driver"] bucket (median 52,000) on Market
    Intelligence -- a US salaried CDL/trucking-employee wage, not a
    gig/private-hire driver's. Post-fix it must resolve to the distinct,
    lower gig bucket instead."""
    mi_row, qi_row = _run_full_pipeline(_uk_cab_driver_plan())

    OLD_SALARIED_TRUCKING_MEDIAN = 52_000
    assert mi_row["median"] != OLD_SALARIED_TRUCKING_MEDIAN, mi_row
    assert qi_row["median"] != OLD_SALARIED_TRUCKING_MEDIAN, qi_row

    # Must land in the gig-passenger band, strictly below the salaried
    # CDL/trucking band (45,000-88,000) this role used to be priced off.
    assert mi_row["median"] < 45_000, mi_row
    assert mi_row["median"] == 34_000, mi_row
    assert mi_row["_meta"]["driver_category"] == "gig_passenger", mi_row


def test_gig_driver_wage_resolver_classifies_passenger_vs_salaried_vs_delivery():
    """The specific keyword-precedence bug this fixes: "cab driver" /
    "rideshare driver" / "taxi driver" / "private hire driver" must resolve
    to the gig bucket, never the salaried CDL/truck bucket, even though the
    bare substring "driver" is common to all of them."""
    gig_titles = [
        "Commercial Cab Driver",
        "Private Hire Driver",
        "Rideshare Driver",
        "Taxi Driver",
        "Uber Driver",
    ]
    for title in gig_titles:
        wage = research.resolve_driver_role_wage(title)
        assert wage is not None, title
        assert wage["category"] == "gig_passenger", (title, wage)
        assert wage["median"] == 34_000, (title, wage)

    salaried_titles = ["CDL Driver", "Truck Driver", "HGV Driver"]
    for title in salaried_titles:
        wage = research.resolve_driver_role_wage(title)
        assert wage is not None, title
        assert wage["category"] == "salaried_trucking", (title, wage)
        assert wage["median"] == 65_000, (title, wage)

    delivery_wage = research.resolve_driver_role_wage("Delivery Driver")
    assert delivery_wage["category"] == "gig_delivery", delivery_wage


# ---------------------------------------------------------------------------
# 3. Confidence must be derived, not the renderer's hardcoded 0.5 literal.
# ---------------------------------------------------------------------------


def test_gig_driver_confidence_is_derived_not_constant_half():
    """fuse_salary_intelligence() previously never set "confidence" at all
    for a fallback role, so the excel_v2 renderer's
    ``.get("confidence", 0.5)`` default silently fired for every row. The
    resolved row must now carry an explicit, non-0.5 numeric confidence."""
    mi_row, qi_row = _run_full_pipeline(_uk_cab_driver_plan())

    assert "confidence" in mi_row
    assert mi_row["confidence"] != 0.5
    assert mi_row["confidence"] == 0.30
    assert isinstance(mi_row["confidence"], float)

    # Quality Intelligence's contract uses a string enum ("benchmark" /
    # "estimated") that the excel_v2 renderer already keys its "(est.)"
    # amber-highlight off of -- a keyword-matched, unbenchmarked gig
    # estimate must land on "estimated", not the false-confident
    # "benchmark" label gold_standard's bare "driver" match used to emit.
    assert qi_row["confidence"] == "estimated"


def test_confidence_differs_by_provenance_not_a_second_constant():
    """Proves confidence is genuinely DERIVED, not just swapped for a
    different fixed number: a pure keyword-fallback role (no API data) and
    a driver-family role must show DIFFERENT confidence, and neither may be
    the old silent 0.5 default."""
    driver_input = _uk_cab_driver_plan()
    driver_result = data_synthesizer.fuse_salary_intelligence({}, {}, driver_input)
    driver_confidence = driver_result["Commercial Cab Driver"]["confidence"]

    generic_input = {
        "roles": ["Executive Assistant"],
        "target_roles": [{"title": "Executive Assistant", "count": 5}],
        "locations": ["United States"],
        "industry": "general",
    }
    generic_result = data_synthesizer.fuse_salary_intelligence({}, {}, generic_input)
    generic_confidence = generic_result["Executive Assistant"]["confidence"]

    assert driver_confidence != 0.5
    assert generic_confidence != 0.5
    assert driver_confidence != generic_confidence, (
        driver_confidence,
        generic_confidence,
    )


def test_empty_salary_result_is_honestly_zero_not_fifty_percent():
    empty = data_synthesizer._empty_salary_result("Some Role")
    assert empty["confidence"] == 0.0
    assert empty["confidence"] != 0.5


# ---------------------------------------------------------------------------
# 4. US false-positive guard -- the regression that matters most.
# ---------------------------------------------------------------------------


def test_us_non_driver_roles_salary_fallbacks_unchanged():
    """Non-driver keyword fallbacks must be completely untouched by this
    fix -- only the "driver" bucket was ever wrong."""
    assert data_synthesizer._ROLE_SALARY_FALLBACKS["nurse"] == {
        "median": 82000,
        "min": 55000,
        "p25": 68000,
        "p75": 95000,
        "max": 120000,
    }
    assert data_synthesizer._ROLE_SALARY_FALLBACKS["software"]["median"] == 130000
    assert data_synthesizer._ROLE_SALARY_FALLBACKS["director"]["median"] == 155000


def test_flat_driver_bucket_is_gone_from_role_salary_fallbacks():
    """The old single, wrong "driver" keyword (median 52,000, conflating
    every driver sub-type) must no longer exist in the fallback dict --
    driver-family roles are resolved exclusively via
    research.resolve_driver_role_wage() now."""
    assert "driver" not in data_synthesizer._ROLE_SALARY_FALLBACKS


def test_us_cdl_truck_driver_still_resolves_to_sensible_salaried_wage():
    """A US CDL/truck driver (a genuinely salaried role, not the reported
    defect) must still price as salaried trucking, not accidentally drop
    into the new gig bucket."""
    plan = {
        "roles": ["CDL Driver"],
        "target_roles": [{"title": "CDL Driver", "count": 20}],
        "locations": ["United States"],
        "industry": "logistics_supply_chain",
    }
    mi_row, qi_row = _run_full_pipeline(plan)
    assert 45_000 <= mi_row["median"] <= 88_000, mi_row
    assert mi_row["median"] == qi_row["median"]
    assert mi_row["_meta"]["driver_category"] == "salaried_trucking"


def test_gold_standard_taxonomy_suite_behavior_unaffected_without_synthesized():
    """gold_standard.enrich_city_level_data() must behave EXACTLY as before
    when data["_synthesized"] isn't populated (every existing caller in
    tests/test_gold_standard_taxonomy.py, and any pipeline stage that runs
    before synthesize()) -- the new override path is additive and must be a
    pure no-op without it."""
    data = {
        "locations": ["New York, NY"],
        "target_roles": ["Driver", "Nurse", "Dishwasher"],
    }
    city_data = gs.enrich_city_level_data(data)
    per_role = city_data[next(iter(city_data))]["per_role_salary"]
    # Unchanged gold_standard behavior: bare "driver" still matches its own
    # _ROLE_SALARY_RANGES entry directly (30,000-50,000 national, scaled by
    # NYC's multiplier), labelled "Industry Benchmark" / "benchmark" exactly
    # as it always has been absent any synthesized override.
    assert per_role["Driver"]["source"] == "Industry Benchmark"
    assert per_role["Driver"]["confidence"] == "benchmark"


def test_usd_plan_salary_pipeline_runs_clean_end_to_end():
    """Full false-positive guard: a USD plan for a driver-family role must
    run the whole pipeline without error and produce sane, agreeing
    numbers -- this is the path exercised by every plan, not just
    international ones."""
    plan = {
        "roles": ["Delivery Driver"],
        "target_roles": [{"title": "Delivery Driver", "count": 50}],
        "locations": ["United States"],
        "industry": "logistics_supply_chain",
    }
    mi_row, qi_row = _run_full_pipeline(plan)
    assert mi_row["median"] == qi_row["median"]
    assert mi_row["min"] <= mi_row["median"] <= mi_row["max"]
    assert qi_row["min"] <= qi_row["median"] <= qi_row["max"]


if __name__ == "__main__":
    import sys

    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
