"""A remote marker is a work mode, not a place, in counter-strategy prose.

Verified 2026-10-16 (output sweep) before the fix: a plan whose locations were
"Remote" plus real cities rendered "candidates in Remote" -- the Remote entry
was phrased like a city, so a US plan read as having one more location than it
has. plan_location classifies the entry as kind="remote"; insight_composer now
follows that classification and says "remote roles" instead of "in Remote".
Every expected string below is typed by hand.
"""

from __future__ import annotations

import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import insight_composer  # noqa: E402
import plan_location  # noqa: E402

# One remote entry + two cities, as a plan's locations list would carry them.
PLAN_LOCATIONS = ["Remote", "Boston, MA", "Austin, TX"]


def _sentence(city: str, role: str = "Nurse", evidence: bool = True) -> str:
    return insight_composer.compose_counter_strategy(
        "Acme Health",
        {"role": role, "city": city, "ordinal": 0, "has_evidence": evidence},
    )


@pytest.mark.parametrize("evidence", [True, False])
@pytest.mark.parametrize("role", ["Nurse", ""])
class TestRemoteIsNotAPlace:
    def test_remote_never_reads_as_a_place(self, evidence, role):
        text = _sentence("Remote", role, evidence)
        assert "in Remote" not in text
        assert "Remote candidates" not in text
        assert "remote roles" in text

    def test_remote_wordings_all_say_remote_roles(self, evidence, role):
        for wording in ("remote", "REMOTE", "Work From Home", "WFH"):
            text = _sentence(wording, role, evidence)
            assert "remote roles" in text, wording
            assert f"in {wording}" not in text, wording


class TestOneRemotePlusTwoCities:
    def test_only_the_cities_read_as_places(self):
        sentences = {loc: _sentence(loc) for loc in PLAN_LOCATIONS}
        places = [
            loc for loc, text in sentences.items() if f"candidates in {loc}" in text
        ]
        assert places == ["Boston, MA", "Austin, TX"]  # two locations, not three
        assert "remote roles" in sentences["Remote"]

    def test_the_role_stays_in_the_remote_phrase(self):
        assert "Nurse candidates for remote roles" in _sentence("Remote")
        assert "candidates for remote roles" in _sentence("Remote", role="")


class TestCitiesAndMissingCityUnchanged:
    def test_city_and_role(self):
        assert "Nurse candidates in Boston, MA" in _sentence("Boston, MA")

    def test_city_only(self):
        assert "candidates in Austin, TX" in _sentence("Austin, TX", role="")

    def test_no_city_no_role(self):
        assert "candidates in this pool" in _sentence("", role="")


def test_composer_follows_plan_locations_classification(monkeypatch):
    """The composer keeps no remote word list of its own: whatever
    plan_location calls kind="remote" is phrased as remote roles."""
    real = plan_location.resolve_location

    def fake(raw):
        res = real(raw)
        if raw == "Telecommute - anywhere in the US":
            res.kind = "remote"
        return res

    monkeypatch.setattr(plan_location, "resolve_location", fake)
    text = _sentence("Telecommute - anywhere in the US")
    assert "Nurse candidates for remote roles" in text
    assert "in Telecommute" not in text
