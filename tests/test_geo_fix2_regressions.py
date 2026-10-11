"""Final-review regressions on shipped 69fb235 (F1-F3)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import geo_audit_helpers as h  # noqa: E402
import plan_location  # noqa: E402
import research  # noqa: E402


def _res(loc):
    return plan_location.resolve_locations([loc])[0].to_dict()


# ── F1: an AMBIGUOUS name's CBSA is a tie-break pick, not evidence ──────────
@pytest.mark.parametrize("name", ["Manhattan", "Kirkland", "Cary", "Concord", "Wilmington"])
def test_ambiguous_bare_name_gets_no_borrowed_metro(name):
    res = _res(name)
    assert res["status"] == "ambiguous"
    info = research.get_location_info(name, res)
    assert info["geo_level"] == "generic", (name, info["metro_name"])
    assert not info["assumed_location"], (name, info["assumed_location"])
    assert "no market assumed" in info["ambiguity_note"]


@pytest.mark.parametrize(
    "city,msa",
    [
        ("Fort Worth, TX", "Dallas-Fort Worth-Arlington MSA"),
        ("Oakland, CA", "San Francisco-Oakland-Berkeley MSA"),
        ("Jersey City, NJ", "New York-Newark-Jersey City MSA"),
        ("Alexandria, VA", "Washington-Arlington-Alexandria MSA"),
    ],
)
def test_resolved_secondary_city_still_gets_its_msa(city, msa):
    assert research.get_location_info(city, _res(city))["metro_name"] == msa


# ── F2: resolutions are matched by position, never by display name ──────────
def _contexts(raw):
    return h.context_table(h.workbook(h.build_plan(raw)))


def test_ny_and_new_york_keep_their_own_verdicts_in_either_order():
    rows = _contexts(["New York", "NY", "Buffalo, NY"])
    assert "Assumed New York, NY" in rows["New York"]["Context"]
    assert "(statewide)" in rows["New York#2"]["Context"]
    rows = _contexts(["NY", "New York"])
    assert "(statewide)" in rows["New York"]["Context"]
    assert "Assumed New York, NY" in rows["New York#2"]["Context"]


def test_washington_and_wa_keep_their_own_verdicts_in_either_order():
    rows = _contexts(["Washington", "WA"])
    assert "Assumed Washington, DC" in rows["Washington"]["Context"]
    assert "(statewide)" in rows["Washington#2"]["Context"]
    rows = _contexts(["WA", "Washington"])
    assert "(statewide)" in rows["Washington"]["Context"]
    assert "Assumed Washington, DC" in rows["Washington#2"]["Context"]


def test_resolution_list_is_positional():
    import geo_market

    locations, sidecar = h.canonical_plan_locations(["New York", "NY"])
    assert locations[0] == locations[1]  # the collision
    data = {"locations": locations, "_location_resolution": sidecar}
    lst = geo_market.resolution_list(data, locations)
    assert [r["input"] for r in lst] == ["New York", "NY"]


# ── F3: the state is the ADDRESS's, the metro name is the CBSA's ───────────
@pytest.mark.parametrize(
    "city,state,msa",
    [
        ("Jersey City, NJ", "NJ", "New York-Newark-Jersey City MSA"),
        ("Alexandria, VA", "VA", "Washington-Arlington-Alexandria MSA"),
        ("Vancouver, WA", "WA", "Portland-Vancouver-Hillsboro MSA"),
        ("Hillsboro, OR", "OR", "Portland-Vancouver-Hillsboro MSA"),
    ],
)
def test_state_comes_from_the_address_not_the_cbsa(city, state, msa):
    info = research.get_location_info(city, _res(city))
    assert info["state"] == state
    assert info["metro_name"] == msa
