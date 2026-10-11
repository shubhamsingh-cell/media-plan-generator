"""R1 (review 2026-10-11): bare foreign city names stay foreign.

plan_location tags "Toronto" / "London" as AMBIGUOUS US cities; that verdict
must not cancel country detection (US$ figures on a UK plan).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import geo_audit_helpers as h  # noqa: E402
import gold_standard  # noqa: E402
import plan_location  # noqa: E402
import research  # noqa: E402


def _res(loc):
    return plan_location.resolve_locations([loc])[0].to_dict()


# ── R1: bare foreign city names stay foreign ───────────────────────────────
FOREIGN = {
    "Toronto": "Canada",
    "London": "United Kingdom",
    "Paris": "France",
    "Dublin": "Ireland",
    "Berlin": "Germany",
    "Delhi": "India",
    "Rome": "Italy",
    "Montreal": "Canada",
    "Zurich": "Switzerland",
    "Warsaw": "Poland",
}


@pytest.mark.parametrize("city", sorted(FOREIGN))
def test_bare_foreign_city_is_not_turned_into_a_us_place(city):
    info = research.get_location_info(city, _res(city))
    assert info["country"] != "United States", (city, info["country"], info["metro_name"])
    assert info.get("is_international") is True


def test_geo_market_leaves_ambiguous_foreign_names_alone():
    import geo_market

    for city in FOREIGN:
        m = geo_market.resolve_market(city, _res(city))
        assert m.level == "generic" and m.kind == "unknown", (city, m)


def test_a_resolved_us_city_with_a_foreign_homonym_stays_us():
    info = research.get_location_info("London, KY", _res("London, KY"))
    assert info["country"] == "United States"


@pytest.mark.parametrize(
    "locations", [["London", "Manchester, UK", "Dublin"], ["Toronto", "Chicago, IL"]]
)
def test_foreign_bare_cities_pass_the_bundle_gate(locations):
    import io

    import excel_v2
    import ppt_generator
    from bundle_qa import run_bundle_qa

    data = h.build_plan(locations)
    data["_gold_standard"] = gold_standard.apply_all_quality_gates(data)
    xlsx = excel_v2.generate_excel_v2(data, research_mod=research)
    pptx = ppt_generator.generate_pptx(data)
    findings = run_bundle_qa(pptx, xlsx, data)
    assert [f for f in findings if f.get("code") == "us_data_on_non_us_plan"] == []
    wb = h.workbook(data)
    foreign = {"London", "Manchester, UK", "Dublin", "Toronto"} & set(locations)
    for table in (h.context_table(wb), h.intelligence_table(wb)):
        for loc, row in table.items():
            if loc in foreign:
                assert row.get("Country") != "United States", (loc, row)
                assert "US$" not in " ".join(row.values()), (loc, row)


