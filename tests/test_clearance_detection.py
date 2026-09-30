"""C-18 + K-14: security-clearance segmentation on government/defense plans.

C-18: clearance tiers were chosen by raw substring match, so "Data
Scientist" / "Research Scientist" priced as Top Secret / SCI ("sci" inside
"scientist"), "Administrative Secretary" as Secret, and "Social Worker"
matched the "cia" keyword.

K-14: the US gate read the token after the last comma of a bare "City, ST"
location ("va", "dc") as a failed country lookup, so every US federal plan
in the production location shape got the non-US reference-only note.
C-18 is asserted with a country tail so it holds independently of K-14.
"""

from __future__ import annotations

import pytest

import gold_standard as gs


def _detect(roles, locations, industry="aerospace_defense", client="Probe Co"):
    return gs.detect_clearance_requirements(
        {
            "industry": industry,
            "target_roles": roles,
            "locations": locations,
            "client_name": client,
        }
    )


def _level(result):
    return ((result or {}).get("primary_clearance") or {}).get("level")


_US_TAIL = ["Arlington, VA, United States"]


@pytest.mark.parametrize("role", ["Data Scientist", "Research Scientist"])
def test_scientist_is_not_top_secret_sci(role):
    result = _detect([role], _US_TAIL)
    assert _level(result) == "Public Trust"


def test_secretary_is_not_secret():
    result = _detect(["Administrative Secretary"], _US_TAIL)
    assert _level(result) == "Public Trust"
    assert "secret" not in result["detected_keywords"]


def test_social_worker_does_not_match_cia():
    result = _detect(["Social Worker"], _US_TAIL)
    assert "cia" not in result["detected_keywords"]


@pytest.mark.parametrize(
    "role,level",
    [
        ("Intelligence Analyst (TS/SCI)", "Top Secret / SCI"),
        ("Cyber Analyst - TS/SCI with Poly", "Top Secret / SCI"),
        ("Top Secret Cleared Engineer", "Top Secret"),
        ("Security Officer, Secret clearance", "Secret"),
        ("Cleared Software Engineer", "Secret"),
    ],
)
def test_explicit_clearance_tokens_still_detected(role, level):
    assert _level(_detect([role], _US_TAIL)) == level


def test_explicit_agency_in_client_name_still_detected():
    result = _detect(["Analyst"], _US_TAIL, client="CIA Directorate")
    assert "cia" in result["detected_keywords"]


def test_non_eligible_industry_never_gets_clearance():
    assert _detect(["TS/SCI Analyst"], _US_TAIL, industry="healthcare_medical") is None


@pytest.mark.parametrize(
    "location",
    ["Washington, DC", "Arlington, VA", "Huntsville, AL", "Colorado Springs, CO"],
)
def test_us_city_state_plan_gets_us_clearance_tiers(location):
    result = _detect(["Intelligence Analyst (TS/SCI)"], [location])
    assert not result.get("us_framework_only")
    assert _level(result) == "Top Secret / SCI"
    assert result["all_clearance_tiers"]


@pytest.mark.parametrize("location", ["Washington, DC", "Huntsville, AL"])
def test_us_city_state_plan_with_scientist_stays_public_trust(location):
    """Fixing K-14 must not expose C-18 on bare "City, ST" gov plans."""
    assert _level(_detect(["Data Scientist"], [location])) == "Public Trust"


@pytest.mark.parametrize("location", ["Auckland, New Zealand", "Ottawa, Canada"])
def test_non_us_plan_gets_reference_only_note(location):
    result = _detect(["Intelligence Analyst (TS/SCI)"], [location])
    assert result["us_framework_only"] is True
    assert result["primary_clearance"] is None


def test_detected_keywords_are_deterministic():
    first = _detect(["Top Secret Cleared Engineer"], _US_TAIL)["detected_keywords"]
    assert first == sorted(first)
