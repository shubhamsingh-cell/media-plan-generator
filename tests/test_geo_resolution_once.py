"""G2-G5: one location is resolved ONCE and every table reads the same answer.

RTI + Hershey audit (2026-10-11):

* G2  plan_location's verdict (``data["_location_resolution"]``) was computed and
  never read, so five Hershey sites all showed "PA Pennsylvania (statewide)"
  although the resolver knew Harrisburg-Carlisle / Scranton--Wilkes-Barre /
  Lancaster / Reading; ambiguous bare cities were never flagged as assumed.
* G3  "Washington" was Washington State in one table, the DC metro in another
  (and 1.25x vs DC's 1.00x on the salary sheet).
* G4  a state entry got ONE metro's figures (MD -> Baltimore, PA -> Philadelphia,
  NC -> Charlotte, OH -> Columbus) in research and the national default in the
  salary path.
* G5  Remote / Nationwide / Cleveland printed an unlabelled "~3.5% | $60,000".
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
import research  # noqa: E402


@pytest.fixture(scope="module")
def rti():
    wb = h.workbook(h.build_plan(h.RTI_LOCATIONS))
    return h.context_table(wb), h.intelligence_table(wb)


@pytest.fixture(scope="module")
def hershey():
    wb = h.workbook(h.build_plan(h.HERSHEY_LOCATIONS, industry="food_beverage", role="Production Operator"))
    return h.context_table(wb)


# ── G2 ──────────────────────────────────────────────────────────────────────
def test_hershey_pa_sites_name_their_own_msa_not_one_statewide_label(hershey):
    notes = {loc: row["Context"] for loc, row in hershey.items()}
    assert "Harrisburg-Carlisle" in notes["Hershey, PA"]
    assert "Scranton" in notes["Hazleton, PA"]
    assert "Lancaster" in notes["Lancaster, PA"]
    assert "Reading" in notes["Reading, PA"]
    pa = [notes[k] for k in ("Hershey, PA", "Hazleton, PA", "Lancaster, PA", "Reading, PA", "Harrisburg, PA")]
    assert len(set(pa)) >= 4
    # the MSA is the site's, but the figures are still the state's -- say so
    for note in pa:
        assert "Pennsylvania statewide" in note


def test_ambiguous_bare_cities_carry_an_explicit_assumed_note(rti):
    context, intelligence = rti
    expected = {
        "Philadelphia": "Philadelphia, PA",
        "Jacksonville": "Jacksonville, FL",
        "Washington": "Washington, DC",
        "Baltimore": "Baltimore, MD",
        "Cleveland": "Cleveland, OH",
        "Columbus": "Columbus, OH",
        "Jackson": "Jackson, MS",
    }
    for loc, assumed in expected.items():
        assert f"Assumed {assumed}" in context[loc]["Context"], (loc, context[loc]["Context"])
        assert f"Assumed {assumed}" in intelligence[loc]["Why This Market"], loc
    # a unique city needs no assumption
    assert "Assumed" not in context["Pittsburgh, PA"]["Context"]


def test_research_reads_the_resolver_verdict_directly():
    import plan_location

    res = plan_location.resolve_locations(["Hershey, PA"])[0].to_dict()
    info = research.get_location_info("Hershey, PA", res)
    assert "Harrisburg-Carlisle" in info["metro_name"]
    assert info["geo_level"] == "state"
    # no verdict supplied -> the legacy honest label is unchanged
    assert research.get_location_info("Hershey, PA")["metro_name"] == "Pennsylvania (statewide)"


# ── G3 ──────────────────────────────────────────────────────────────────────
def test_washington_is_one_market_everywhere(rti):
    context, intelligence = rti
    wash, dc = context["Washington"], context["District of Columbia"]
    assert wash["Unemployment"] == dc["Unemployment"]
    assert wash["Median Salary"] == dc["Median Salary"]
    assert research.get_location_info("Washington")["state"] != "WA"
    # salary path: same multiplier for "DC" and "Washington" (1.25x vs 1.00x before)
    rows = gold_standard.enrich_city_level_data(
        {"locations": ["District of Columbia", "Washington"], "target_roles": ["RN"]}
    )
    assert rows["District of Columbia"]["salary_multiplier"] == rows["Washington"]["salary_multiplier"]
    assert rows["District of Columbia"]["hiring_difficulty"] == rows["Washington"]["hiring_difficulty"]
    assert intelligence["Washington"]["Population"] == intelligence["District of Columbia"]["Population"]


def test_wa_and_washington_state_stay_the_state():
    import plan_location

    res = plan_location.resolve_locations(["WA"])[0].to_dict()
    info = research.get_location_info("Washington", res)
    assert info["state"] == "WA"
    assert info["metro_name"] == "Washington (statewide)"
    rows = gold_standard.enrich_city_level_data(
        {"locations": ["Washington"], "_location_resolution": [res], "target_roles": ["RN"]}
    )
    assert rows["Washington"]["geo_basis"] == "state"
    assert rows["Washington"]["geo_basis_area"] == "WA"


# ── G4 ──────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "loc,state_name",
    [
        ("MD", "Maryland"),
        ("Maryland", "Maryland"),
        ("NC", "North Carolina"),
        ("North Carolina", "North Carolina"),
        ("OH", "Ohio"),
        ("PA", "Pennsylvania"),
        ("NY", "New York"),
    ],
)
def test_state_entry_is_statewide_not_one_metro(loc, state_name):
    info = research.get_location_info(loc)
    assert info["metro_name"] == f"{state_name} (statewide)"
    assert info["geo_level"] == "state"
    assert "MSA" not in info["metro_name"]


def test_new_york_state_entry_is_not_new_york_city_when_the_resolver_says_state(rti):
    context, _ = rti
    assert context["New York"]["Context"].startswith("New York (statewide)")


def test_salary_path_prices_state_entries_statewide_and_says_so():
    rows = gold_standard.enrich_city_level_data(
        {"locations": ["MD", "Maryland", "NC", "Ohio"], "target_roles": ["RN"]}
    )
    expected = {"MD": 1.1, "Maryland": 1.1, "NC": 1.0, "Ohio": 0.94}
    for loc, mult in expected.items():
        row = rows[loc]
        assert row["salary_multiplier"] == mult, loc
        assert row["geo_basis"] == "state", loc
        assert row["fallback_uniform"] is False, loc
    # a state named "New Mexico" is not Mexico
    nm = gold_standard.enrich_city_level_data({"locations": ["New Mexico"], "target_roles": ["RN"]})
    assert nm["New Mexico"]["geo_basis"] == "state"


# ── G5 ──────────────────────────────────────────────────────────────────────
def test_remote_and_nationwide_show_a_dash_not_a_placeholder_market(rti):
    context, intelligence = rti
    for loc in ("Remote", "Nationwide (US)"):
        assert context[loc]["Unemployment"] == "—", loc
        assert context[loc]["Median Salary"] == "—", loc
        assert "3.5" not in context[loc]["Unemployment"]
        assert "60,000" not in context[loc]["Median Salary"]


def test_cleveland_is_labelled_and_never_an_unlabelled_placeholder(rti):
    context, _ = rti
    row = context["Cleveland"]
    assert "~3.5%" not in row["Unemployment"]
    assert "Assumed Cleveland, OH" in row["Context"]


def test_a_generic_fallback_figure_is_flagged_on_the_surface():
    wb = h.workbook(h.build_plan(["Qzxwvk"]))
    row = h.context_table(wb)["Qzxwvk"]
    assert row["Unemployment"].endswith("*") and row["Median Salary"].endswith("*")
    assert "placeholder" in row["Context"].lower() or "no market data" in row["Context"].lower()
    text = " ".join(h.sheet_text(wb["Market Intelligence"]))
    assert "flat national placeholders" in text
