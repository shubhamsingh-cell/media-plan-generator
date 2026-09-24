"""Regression tests for research.py's ``_STATE_PRIMARY_METRO`` fallback
over-firing on named cities (the Hershey Company client incident).

Background (real client bug, reported via Slack): a Hershey Company media
plan listing Pennsylvania cities including Hershey, PA and Hazleton, PA
showed BOTH mapped into the Philadelphia-Camden-Wilmington MSA. Neither
city is in that MSA -- Hershey (Derry Township, Dauphin County) sits in
the Harrisburg-Carlisle, PA MSA (OMB CBSA 25420) and Hazleton (Luzerne
County) sits in the Scranton--Wilkes-Barre, PA MSA (OMB CBSA 42540) --
per ``data/geo/cbsa_by_county.tsv`` (OMB Bulletin 23-01) and confirmed
against Census.gov's CBSA delineation file. ``plan_location.py``'s
CBSA-based resolver already gets this right (it reads that same
committed, Census-derived county->CBSA table) -- this bug lives entirely
in ``research.py``'s SEPARATE, hand-curated ``METRO_DATA``/
``_STATE_PRIMARY_METRO`` tables, used for cost-of-living/labour-market
context, not for the location's display name.

Root cause (research.py:get_location_info, pre-fix ~line 4648):
``_STATE_PRIMARY_METRO`` is documented ("Used by get_location_info() when
a bare state abbreviation is passed as the location, e.g. 'GA', 'IL'") as
a fallback for a BARE state code with no city. The code instead fired
whenever ``_find_metro()`` found no direct match AND a state was
detected -- true for ANY named city that simply isn't one of the ~100
metros curated in ``METRO_DATA`` (Hershey/Hazleton/Harrisburg/Scranton,
PA are all real cities, none of them in that curated list). Every one of
those silently inherited the state's single flagship metro's cost/labour
data as if it specifically described that city -- e.g. even Harrisburg
and Scranton themselves (real, distinct MSAs) were mislabeled as
Philadelphia.

Fix: ``_is_bare_state_location()`` gates the ``_STATE_PRIMARY_METRO``
branch to true bare-state inputs (the string IS the state, e.g. "PA" or
"Pennsylvania" alone). A named city with no METRO_DATA entry now falls
through to the existing, already-correct statewide branch
(``metro_name`` = "Pennsylvania (statewide)"), which is honestly
imprecise rather than confidently WRONG.

VACUOUSNESS: run against a throwaway pre-fix worktree (git worktree add
--detach HEAD at the parent commit) -- see the task report for the
observed pre-fix failures (every PA test city below asserted
metro_name == "Philadelphia-Camden-Wilmington MSA").
"""

from __future__ import annotations

import research


# ---------------------------------------------------------------------------
# 1. The exact client incident: Hershey, PA and Hazleton, PA must not
#    resolve to the Philadelphia MSA.
# ---------------------------------------------------------------------------


def test_hershey_pa_does_not_map_to_philadelphia():
    info = research.get_location_info("Hershey, PA")
    assert info.get("metro_name") != "Philadelphia-Camden-Wilmington MSA"


def test_hazleton_pa_does_not_map_to_philadelphia():
    info = research.get_location_info("Hazleton, PA")
    assert info.get("metro_name") != "Philadelphia-Camden-Wilmington MSA"


def test_hershey_and_hazleton_fall_through_to_honest_statewide_fallback():
    """Neither city is in research.py's curated METRO_DATA -- the correct
    behavior is the existing, honestly-imprecise "PA (statewide)" branch,
    not a specific-looking but WRONG metro claim."""
    for loc in ("Hershey, PA", "Hazleton, PA"):
        info = research.get_location_info(loc)
        assert info.get("metro_name") == "Pennsylvania (statewide)", loc
        # This is the designed statewide tier, not the generic-national
        # fallback tier -- real (if coarse) state data, so no
        # is_generic_fallback flag.
        assert "is_generic_fallback" not in info, loc


# ---------------------------------------------------------------------------
# 2. Same defect class, broader proof: OTHER PA cities absent from
#    METRO_DATA (including two that are themselves real, distinct MSAs)
#    must not be silently promoted to Philadelphia's metro data either.
# ---------------------------------------------------------------------------


def test_harrisburg_and_scranton_are_not_mislabeled_philadelphia():
    """Harrisburg and Scranton are themselves real MSAs (Harrisburg-
    Carlisle; Scranton--Wilkes-Barre) -- not curated in research.py's
    METRO_DATA, but definitely not Philadelphia either."""
    for loc in ("Harrisburg, PA", "Scranton, PA"):
        info = research.get_location_info(loc)
        assert info.get("metro_name") != "Philadelphia-Camden-Wilmington MSA", loc


# ---------------------------------------------------------------------------
# 3. The _STATE_PRIMARY_METRO fallback's actual documented purpose (a bare
#    state code/name with NO city) must still work -- this fix narrows
#    the branch, it must not disable it.
# ---------------------------------------------------------------------------


def test_bare_state_abbreviation_still_uses_primary_metro():
    info = research.get_location_info("PA")
    assert info.get("metro_name") == "Philadelphia-Camden-Wilmington MSA"


def test_bare_full_state_name_still_uses_primary_metro():
    info = research.get_location_info("Pennsylvania")
    assert info.get("metro_name") == "Philadelphia-Camden-Wilmington MSA"


def test_bare_state_abbreviation_other_state_unaffected():
    """Sibling state, same mechanism -- confirms the fix is general, not a
    PA-only special case."""
    info = research.get_location_info("GA")
    assert info.get("metro_name") == "Atlanta-Sandy Springs-Alpharetta MSA"


# ---------------------------------------------------------------------------
# 4. Known metros must be completely unaffected by this change (no
#    regression on the working, common case).
# ---------------------------------------------------------------------------


def test_known_metro_city_unaffected():
    info = research.get_location_info("Dallas, TX")
    assert info.get("metro_name") == "Dallas-Fort Worth-Arlington MSA"


def test_known_metro_city_within_primary_metro_state_unaffected():
    """Atlanta, GA is both METRO_DATA's own direct-match entry AND GA's
    _STATE_PRIMARY_METRO target -- must resolve identically either way."""
    info = research.get_location_info("Atlanta, GA")
    assert info.get("metro_name") == "Atlanta-Sandy Springs-Alpharetta MSA"


# ---------------------------------------------------------------------------
# 5. Helper unit tests -- pin _is_bare_state_location's own contract.
# ---------------------------------------------------------------------------


def test_is_bare_state_location_true_cases():
    assert research._is_bare_state_location("PA", "PA") is True
    assert research._is_bare_state_location("pa", "PA") is True
    assert research._is_bare_state_location(" PA ", "PA") is True
    assert research._is_bare_state_location("Pennsylvania", "PA") is True
    assert research._is_bare_state_location("pennsylvania", "PA") is True


def test_is_bare_state_location_false_for_named_city():
    assert research._is_bare_state_location("Hershey, PA", "PA") is False
    assert research._is_bare_state_location("Harrisburg, PA", "PA") is False


def test_is_bare_state_location_false_for_empty_or_missing_args():
    assert research._is_bare_state_location("", "PA") is False
    assert research._is_bare_state_location("PA", None) is False
    assert research._is_bare_state_location("PA", "") is False
