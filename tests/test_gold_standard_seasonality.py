"""Tests for gold_standard.py seasonality/activation-calendar fixes.

Covers:
    1. Sub-vertical seasonal override in build_activation_calendar() --
       verified defect strategy:manpower#1 (CRITICAL): the generic
       "Logistics & Supply Chain" activation calendar used e-commerce/
       freight seasonality (peak Aug-Nov, trough Jan-Mar) for a propane
       distributor (AmeriGas), which is backwards -- propane delivery
       demand peaks in winter heating season and hiring must ramp
       Aug-Oct to be staffed before it. Also covers the atria senior-living
       case (year-round + flu-season Q4 bump + Jan budget-cycle bump)
       instead of the generic acute-care-hospital healthcare calendar.
    2. Generic (non-matching) plans are unaffected by the sub-vertical layer.
    3. 'fallback_uniform' marker in enrich_city_level_data() -- cities with
       no known city/state/country/metro data are flagged so renderers can
       collapse identical-default rows instead of presenting them as
       distinct per-market figures.

No production code outside gold_standard.py is touched or imported here
beyond stdlib + gold_standard itself.
"""

from __future__ import annotations

import gold_standard as gs


# ---------------------------------------------------------------------------
# 1a. Sub-vertical override -- fuel/heating delivery (AmeriGas / propane)
# ---------------------------------------------------------------------------


def _manpower_amerigas_data(campaign_start_month: int = 7) -> dict:
    return {
        "client_name": "Manpower - Amerigas",
        "industry": "logistics_supply_chain",
        "campaign_start_month": campaign_start_month,
        "roles": ["CDL A Driver"],
        "notes": "Blue Collar talent profile. On-Site work model.",
    }


def test_amerigas_propane_gets_heating_season_not_freight_season():
    """Regression test for strategy:manpower#1 (CRITICAL).

    The generic 'Logistics & Supply Chain' calendar told AmeriGas to wind
    down recruiting in December ('Minimal active recruiting -- focus on
    pipeline building for January') right as winter heating-season demand
    for propane delivery drivers begins. The sub-vertical override must
    replace that with heating-season logic: hiring ramps Aug-Oct (BEFORE
    the Oct-Mar peak), not a December trough.
    """
    cal = gs.build_activation_calendar(_manpower_amerigas_data(campaign_start_month=7))
    assert cal["subvertical"] == "fuel_heating_delivery"

    by_month = {m["month"]: m for m in cal["timeline"]}

    # September (pre-season ramp peak) must be the highest-intensity month
    # in the Jul-Dec window -- NOT a trough.
    assert by_month[9]["hiring_intensity"] == "very_high"
    assert by_month[9]["budget_weight"] >= by_month[12]["budget_weight"]

    # August-October must be at or above "high" intensity (the pre-winter
    # staffing ramp), the opposite of the old generic calendar's Aug-Nov
    # "peak" being mistaken for AmeriGas's actual trough season.
    for m in (8, 9, 10):
        assert by_month[m]["hiring_intensity"] in ("high", "very_high"), (
            m,
            by_month[m],
        )

    # December must NOT carry the generic "Minimal active recruiting --
    # focus on pipeline building for January" trough message -- crews
    # should already be staffed for the winter heating peak by then.
    dec_rec = by_month[12]["recommendation"].lower()
    assert "minimal active recruiting" not in dec_rec
    assert "pipeline building for january" not in dec_rec

    # Rationale/source must be present and non-fabricated (domain reasoning,
    # not an invented statistic) so renderers can cite it.
    assert "heating" in cal["subvertical_rationale"].lower()
    assert cal["subvertical_source"]
    assert "heating" in cal["budget_phasing_note"].lower()


def test_amerigas_keeps_climate_independent_events():
    """DOT compliance deadlines / CDL school graduation cycles are valid
    regardless of the seasonality fix and must be retained."""
    cal = gs.build_activation_calendar(_manpower_amerigas_data(campaign_start_month=1))
    all_events = " ".join(" ".join(m["key_events"]) for m in cal["timeline"]).lower()
    assert "dot compliance" in all_events
    assert "cdl school" in all_events


def test_propane_keyword_alone_triggers_override_without_amerigas_name():
    """The keyword layer must be general -- any propane/heating-fuel client
    in the trucking/blue-collar industry should match, not just AmeriGas."""
    data = {
        "client_name": "Acme Propane Co",
        "industry": "logistics_supply_chain",
        "campaign_start_month": 7,
        "roles": ["Delivery Driver"],
    }
    cal = gs.build_activation_calendar(data)
    assert cal["subvertical"] == "fuel_heating_delivery"


# ---------------------------------------------------------------------------
# 1b. Sub-vertical override -- senior living (atria)
# ---------------------------------------------------------------------------


def test_atria_senior_living_gets_flu_season_and_budget_cycle_bumps():
    data = {
        "client_name": "atria Senior living",
        "industry": "healthcare_medical",
        "campaign_start_month": 1,
        "roles": ["Memory Care Associate", "Nurse", "Cook"],
    }
    cal = gs.build_activation_calendar(data)
    assert cal["subvertical"] == "senior_living"

    by_month = {m["month"]: m for m in cal["timeline"]}
    # January budget-cycle bump.
    assert by_month[1]["hiring_intensity"] == "high"
    assert "budget" in by_month[1]["season"].lower() or any(
        "budget" in e.lower() for e in by_month[1]["key_events"]
    )


def test_senior_living_flu_season_bump_in_q4():
    data = {
        "client_name": "atria Senior living",
        "industry": "healthcare_medical",
        "campaign_start_month": 9,
        "roles": ["Nurse"],
    }
    cal = gs.build_activation_calendar(data)
    by_month = {m["month"]: m for m in cal["timeline"]}
    for m in (10, 11):
        assert by_month[m]["hiring_intensity"] == "high"
        assert "flu" in " ".join(by_month[m]["key_events"]).lower()


# ---------------------------------------------------------------------------
# 1c. Generic (non-matching) plans are unaffected
# ---------------------------------------------------------------------------


def test_generic_logistics_client_unaffected_by_subvertical_layer():
    """A plain warehouse/logistics client with no fuel/heating keywords must
    still fall through to the pre-existing generic calendar behavior."""
    data = {
        "client_name": "Acme Warehousing",
        "industry": "logistics_supply_chain",
        "campaign_start_month": 7,
        "roles": ["Warehouse Associate"],
    }
    cal = gs.build_activation_calendar(data)
    assert cal.get("subvertical") is None
    assert "subvertical_rationale" not in cal
    by_month = {m["month"]: m for m in cal["timeline"]}
    assert by_month[12]["seasonal_phase"] == "normal"


def test_generic_healthcare_client_unaffected_by_subvertical_layer():
    data = {
        "client_name": "General Hospital",
        "industry": "healthcare_medical",
        "campaign_start_month": 1,
        "roles": ["Registered Nurse"],
    }
    cal = gs.build_activation_calendar(data)
    assert cal.get("subvertical") is None


# ---------------------------------------------------------------------------
# 2. fallback_uniform marker in enrich_city_level_data()
# ---------------------------------------------------------------------------


def test_fallback_uniform_flags_cities_with_no_known_data():
    """Cities with no city/state/country/metro signal at all must be marked
    fallback_uniform=True so renderers can collapse the identical rows they
    'd otherwise produce; a well-known city with real data must not be."""
    data = {
        "locations": ["New York, NY", "Nowheresville, ZZ", "Someplace, ZZ"],
        "target_roles": ["Nurse"],
    }
    city_data = gs.enrich_city_level_data(data)

    assert city_data["New York"]["fallback_uniform"] is False

    assert city_data["Nowheresville"]["fallback_uniform"] is True
    assert city_data["Someplace"]["fallback_uniform"] is True
    # The two fallback cities must indeed be byte-identical on the fields
    # driven by salary_multiplier/hiring_difficulty -- confirming the flag
    # correctly identifies rows that would otherwise look like fabricated
    # per-market precision.
    assert (
        city_data["Nowheresville"]["salary_multiplier"]
        == city_data["Someplace"]["salary_multiplier"]
    )
    assert (
        city_data["Nowheresville"]["hiring_difficulty"]
        == city_data["Someplace"]["hiring_difficulty"]
    )


def test_fallback_uniform_field_always_present_and_boolean():
    data = {"locations": ["Chicago, IL"], "target_roles": ["Cook"]}
    city_data = gs.enrich_city_level_data(data)
    row = city_data["Chicago"]
    assert "fallback_uniform" in row
    assert isinstance(row["fallback_uniform"], bool)
