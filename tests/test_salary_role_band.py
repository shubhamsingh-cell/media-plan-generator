"""Audit F 3.3 (item 1): a role's salary must respect the role's OWN band.

Shipped defect: gold_standard took data_synthesizer's per-role override
verbatim, and the override came from generic keyword buckets -- "sales"
($90,000) priced a Retail Sales Associate, "manager" ($105,000) a Store
Manager, "medical" ($95,000) a Medical Assistant; the H-1B "nurse" alias
priced a Nurse Practitioner and an LPN at the Registered Nurse wage. The
Columbus, OH retail deck printed "$86K (est.)" on the Role Breakdown slide
against the role's own $28-42K band (BLS retail salespersons median
$17.03/h, May 2025). The H-1B metro lookup also matched the "la" alias
inside Cleveland / Philadelphia / Orlando / Oakland / Salt Lake City /
Auckland and priced those plans off Los Angeles wages.
"""

from __future__ import annotations

import pytest

import data_synthesizer
import gold_standard as gs
import h1b_data
import ppt_generator

_SWEEP_ROLES = [
    "Retail Sales Associate", "Sales Associate", "Cashier", "Store Manager",
    "Assistant Store Manager", "Stock Associate", "District Manager",
    "Warehouse Associate", "Warehouse Manager", "Forklift Operator",
    "Delivery Driver", "Truck Driver", "Dispatcher",
    "Customer Service Representative", "Call Center Agent", "Receptionist",
    "Administrative Assistant", "Executive Assistant", "Accountant",
    "Financial Analyst", "Data Analyst", "Business Analyst",
    "Software Engineer", "Data Scientist", "Data Engineer", "Product Manager",
    "Project Manager", "Marketing Manager", "Registered Nurse",
    "Licensed Practical Nurse", "Certified Nursing Assistant",
    "Medical Assistant", "Nurse Practitioner", "Pharmacist",
    "Physical Therapist", "Home Health Aide", "Maintenance Technician",
    "HVAC Technician", "Electrician", "Welder", "Machinist", "Line Cook",
    "Housekeeper", "Janitor", "Teacher", "Paralegal", "Nurse Manager",
    "Case Worker", "Dental Hygienist", "Medical Technologist",
]
_SWEEP_CITIES = ["Columbus, OH", "San Francisco, CA", "Dallas, TX"]


def _pipeline(roles, locations, industry="general_entry_level"):
    data = {
        "industry": industry,
        "target_roles": list(roles),
        "locations": list(locations),
        "target_region": "us_only",
        "_enriched": {},
    }
    synth = data_synthesizer.synthesize({}, {}, data)
    data["_synthesized"] = synth
    return synth, gs.enrich_city_level_data(data), data


def test_retail_keys_precede_generic_sales_and_manager_buckets():
    keys = list(data_synthesizer._ROLE_SALARY_FALLBACKS)
    for specific in ("retail sales", "sales associate", "store manager"):
        assert specific in keys
        assert keys.index(specific) < keys.index("sales"), specific
        assert keys.index(specific) < keys.index("manager"), specific
    assert data_synthesizer._ROLE_SALARY_FALLBACKS["retail sales"]["median"] == 35400


def test_columbus_retail_roles_priced_inside_their_bands():
    roles = ["Retail Sales Associate", "Cashier", "Store Manager", "Stock Associate"]
    synth, city_data, data = _pipeline(roles, ["Columbus, OH"], "retail_consumer")
    row = city_data["Columbus"]["per_role_salary"]
    mult = city_data["Columbus"]["salary_multiplier"]
    assert 28_000 * mult <= row["Retail Sales Associate"]["median"] <= 42_000 * mult
    assert 45_000 * mult <= row["Store Manager"]["median"] <= 75_000 * mult
    mi = synth["salary_intelligence"]
    assert mi["Retail Sales Associate"]["median"] == 35_400
    assert mi["Store Manager"]["median"] == 60_000
    # Deck Role Breakdown reads the same rows.
    median, _est = ppt_generator._role_breakdown_median_salary(
        {"city_level_data": city_data}, "Retail Sales Associate"
    )
    assert median < 50_000
    assert ppt_generator._format_salary(median) == "$34K"


def test_out_of_band_override_is_clamped_to_band_midpoint():
    """gold_standard alone (no data_synthesizer clamp upstream): a USD
    override far outside the band falls through to band midpoint x city."""
    data = {
        "target_roles": ["Retail Sales Associate", "Registered Nurse"],
        "locations": ["Columbus, OH"],
        "_synthesized": {
            "per_role_salaries": {
                "Retail Sales Associate": {
                    "min": 50_000, "p25": 70_000, "median": 90_000,
                    "p75": 115_000, "max": 160_000, "source": "Industry Benchmark",
                    "confidence": "estimated", "currency": "USD", "city_adjust": True,
                },
                "Registered Nurse": {
                    "min": 50_700, "p25": 63_960, "median": 78_000,
                    "p75": 92_040, "max": 113_100, "source": "DOL H-1B/LCA",
                    "confidence": "estimated", "currency": "USD", "city_adjust": True,
                },
            }
        },
    }
    info = gs.enrich_city_level_data(data)["Columbus"]
    mult = info["salary_multiplier"]
    rsa = info["per_role_salary"]["Retail Sales Associate"]
    assert rsa["median"] == round(35_000 * mult)
    assert rsa["source"] == "Industry Benchmark"
    # In-band override is still used as-is, city-adjusted.
    assert info["per_role_salary"]["Registered Nurse"]["median"] == round(78_000 * mult)
    assert info["per_role_salary"]["Registered Nurse"]["source"] == "DOL H-1B/LCA"


def test_plan_local_override_on_non_us_market_is_not_band_clamped():
    """A "" (plan-local) figure on a non-US market is in local currency; a
    USD band cannot judge it."""
    data = {
        "target_roles": ["Retail Sales Associate"],
        "locations": ["London, UK"],
        "_synthesized": {
            "per_role_salaries": {
                "Retail Sales Associate": {
                    "min": 18_000, "p25": 20_000, "median": 22_000,
                    "p75": 24_000, "max": 26_000, "source": "Jooble Market Benchmarks",
                    "confidence": "estimated", "currency": "", "city_adjust": False,
                }
            }
        },
    }
    row = gs.enrich_city_level_data(data)["London"]["per_role_salary"][
        "Retail Sales Associate"
    ]
    assert row["median"] == 22_000


@pytest.mark.parametrize("city", _SWEEP_CITIES)
def test_sweep_every_banded_role_lands_within_25pct_of_its_band(city):
    synth, city_data, _ = _pipeline(_SWEEP_ROLES, [city])
    info = next(iter(city_data.values()))
    for role, row in info["per_role_salary"].items():
        band, _kw = gs._match_role_to_salary_range(role.lower())
        if band is None:
            continue
        lo, hi = band
        applied = row["multiplier"]
        assert lo * applied * 0.75 <= row["median"] <= hi * applied * 1.25, (
            city, role, row["median"], band, applied,
        )
        mi = synth["salary_intelligence"].get(role) or {}
        if mi.get("median"):
            assert lo * 0.75 <= mi["median"] <= hi * 1.25, (
                city, role, mi["median"], band,
            )


def test_clamped_market_intelligence_equals_quality_intelligence_base():
    """One figure per role across sheets (Hershey invariant) survives the
    clamp: the per-city base IS the Market Intelligence figure."""
    roles = ["Medical Assistant", "Nurse Practitioner", "Warehouse Manager"]
    synth, _city_data, _ = _pipeline(roles, ["Columbus, OH"])
    for role in roles:
        mi = synth["salary_intelligence"][role]
        assert mi["kb_validation"]["flag"] == "role_band_clamped", role
        assert mi["sources"] == ["Industry Benchmark"]
        assert synth["per_role_salaries"][role]["median"] == mi["median"]


@pytest.mark.parametrize(
    "location",
    [
        "Cleveland, OH",
        "Philadelphia, PA",
        "Orlando, FL",
        "Oakland, CA",
        "Salt Lake City, UT",
        "Auckland, New Zealand",
    ],
)
def test_h1b_metro_la_alias_does_not_match_inside_words(location):
    assert h1b_data._normalize_metro(location) != "los_angeles"


@pytest.mark.parametrize(
    "location,metro",
    [
        ("Los Angeles, CA", "los_angeles"),
        ("LA", "los_angeles"),
        ("Greater Los Angeles Area", "los_angeles"),
        ("San Francisco Bay Area", "san_francisco"),
        ("Seattle, WA", "seattle"),
        ("Washington, DC", "washington_dc"),
    ],
)
def test_h1b_metro_real_matches_still_resolve(location, metro):
    assert h1b_data._normalize_metro(location) == metro
