"""G1: a city is matched to curated data by EXACT normalised name, never substring.

RTI audit (2026-10-11): "Jackson" was answered with Jacksonville's population
(954,614), employers (Mayo Clinic FL) and unemployment, because research.py's
``_find_metro`` tested ``key in norm or norm in key`` and excel_v2 matched
location profiles with ``loc.lower() in loc_key.lower()``. Both now require
exact normalised equality (plus documented aliases / official metro names).
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import geo_audit_helpers as h  # noqa: E402
import research  # noqa: E402


def test_jackson_is_not_jacksonville():
    key, _ = research._find_metro("Jackson")
    assert key != "jacksonville"
    info = research.get_location_info("Jackson")
    assert "Jacksonville" not in info["metro_name"]
    assert info["population"] != "1.6M metro"


def test_jacksonville_still_matches_itself():
    assert research._find_metro("Jacksonville")[0] == "jacksonville"
    assert research._find_metro("Jacksonville, FL")[0] == "jacksonville"


def test_bare_washington_is_not_substring_matched_to_the_dc_metro():
    # "washington" is contained in "washingtondc"; only an alias / official
    # name may name that metro, never containment.
    assert research._find_metro("Washington")[0] is None
    assert research._find_metro("Washington, DC")[0] == "washington_dc"
    assert research._find_metro("Washington-Arlington")[0] == "washington_dc"
    assert research._find_metro("Washington-Arlington-Alexandria MSA")[0] == "washington_dc"


def test_state_suffix_must_agree_with_the_metro_state():
    assert research._find_metro("Columbus, OH")[0] == "columbus"
    assert research._find_metro("Columbus, GA")[0] is None
    assert research._find_metro("Portland, ME")[0] == "portland_me"
    assert research._find_metro("Portland, OR")[0] == "portland"


def test_documented_aliases_still_resolve():
    assert research._find_metro("NYC")[0] == "new_york"
    assert research._find_metro("Dallas-Fort Worth")[0] == "dallas"
    assert research._find_metro("St. Louis, MO")[0] == "st_louis"


def _drop_and_inject(data, drop_key, new_key, population):
    profiles = dict(data["_synthesized"]["location_profiles"])
    profiles.pop(drop_key, None)
    profiles[new_key] = {
        "location": new_key,
        "population": population,
        "unemployment_rate": 1.1,
        "major_employers": f"Employer of {new_key}",
    }
    data["_synthesized"]["location_profiles"] = profiles


def test_workbook_jackson_row_does_not_borrow_jacksonvilles_profile():
    data = h.build_plan(["Jacksonville", "Jackson"])
    wb = h.workbook(data)
    rows = h.intelligence_table(wb)
    jax, jackson = rows["Jacksonville"], rows["Jackson"]
    assert jax["Population"] != jackson["Population"]
    assert "Mayo Clinic" not in jackson["Key Industries"]
    assert "Mayo Clinic" in jax["Key Industries"]


def test_workbook_washington_row_does_not_borrow_a_washington_arlington_profile():
    data = h.build_plan(["Washington"])
    _drop_and_inject(data, "Washington", "Washington-Arlington-Alexandria", 7_777_777)
    wb = h.workbook(data)
    row = h.intelligence_table(wb)["Washington"]
    assert "7,777,777" not in row["Population"]
    assert "Washington-Arlington-Alexandria" not in row["Key Industries"]
