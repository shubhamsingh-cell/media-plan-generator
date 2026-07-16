"""Tests for plan_geo -- single source of truth for "is this plan US-only?".

Covers the two real production briefs that misfired under the old
ppt_generator._is_us_only_campaign (hard-return-False-on-first-miss bug),
plus the NZ/Gedu regressions mirrored from tests/test_ppt_polish.py's
TestUsOnlyCampaignDetection.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from plan_geo import is_us_plan, non_us_signals  # noqa: E402


class TestRealProdBriefRepros:
    def test_new_england_multi_state_list_is_us_only(self):
        # Real prod brief: a list of bare New England state names plus one
        # "City, ST" entry. The old blocklist had no state-name table at all
        # and no notion of continuing past an unresolvable candidate.
        data = {
            "locations": [
                "Massachusetts",
                "Maine",
                "New Hampshire",
                "Rhode Island",
                "Connecticut",
                "Denver, CO",
            ]
        }
        assert is_us_plan(data) is True
        assert non_us_signals(data) == []

    def test_new_york_ny_is_us_only(self):
        data = {"locations": ["New York, NY"]}
        assert is_us_plan(data) is True
        assert non_us_signals(data) == []


class TestBasicResolution:
    def test_auckland_bare_city_is_not_us(self):
        assert is_us_plan({"locations": ["Auckland"]}) is False

    def test_london_uk_is_not_us(self):
        assert is_us_plan({"locations": ["London, UK"]}) is False

    def test_mixed_us_and_intl_is_not_us(self):
        data = {"locations": ["New York, NY", "London, UK"]}
        assert is_us_plan(data) is False
        assert non_us_signals(data) == ["London, UK"]

    def test_no_locations_defaults_domestic(self):
        assert is_us_plan({}) is True
        assert is_us_plan({"locations": []}) is True
        assert non_us_signals({}) == []


class TestTargetRegionOverride:
    def test_us_only_short_circuits_true(self):
        assert is_us_plan({"target_region": "us_only"}) is True
        assert (
            is_us_plan({"target_region": "us_only", "locations": ["London, UK"]})
            is True
        )

    @pytest.mark.parametrize("region", ["global", "emea", "apac", "custom"])
    def test_non_us_regions_short_circuit_false(self, region):
        assert is_us_plan({"target_region": region}) is False
        assert (
            is_us_plan({"target_region": region, "locations": ["United States"]})
            is False
        )


class TestNeverHardFailsOnFirstMiss:
    def test_unresolvable_candidate_does_not_poison_the_rest(self):
        # "Remote" is unresolvable (no currency signal, no state signal, no
        # intl token) -- it must be skipped, not treated as a False signal.
        data = {"locations": ["Remote", "Dallas, TX"]}
        assert is_us_plan(data) is True
        assert non_us_signals(data) == []

    def test_unresolvable_first_then_real_intl_still_detected(self):
        data = {"locations": ["Remote", "London, UK"]}
        assert is_us_plan(data) is False
        assert non_us_signals(data) == ["London, UK"]


class TestUSStateTable:
    @pytest.mark.parametrize(
        "loc",
        [
            "Massachusetts",
            "Maine",
            "New Hampshire",
            "Rhode Island",
            "Connecticut",
            "Texas",
            "California",
        ],
    )
    def test_bare_state_names_resolve_us(self, loc):
        assert is_us_plan({"locations": [loc]}) is True

    def test_city_space_state_no_comma_resolves_us(self):
        assert is_us_plan({"locations": ["Denver CO"]}) is True

    def test_city_comma_state_resolves_us(self):
        assert is_us_plan({"locations": ["Denver, CO"]}) is True

    def test_indianapolis_does_not_false_positive_on_india_substring(self):
        assert is_us_plan({"locations": ["Indianapolis, IN"]}) is True


class TestNZGeduMirrors:
    """Mirrors tests/test_ppt_polish.py::TestUsOnlyCampaignDetection."""

    def test_new_zealand_via_locations_and_country(self):
        data = {"locations": ["Auckland, New Zealand"], "country": "New Zealand"}
        assert is_us_plan(data) is False

    def test_new_zealand_via_country_field_only(self):
        assert is_us_plan({"country": "New Zealand"}) is False

    def test_new_zealand_via_locations_only(self):
        assert is_us_plan({"locations": ["Auckland, New Zealand"]}) is False

    def test_countries_missing_from_hardcoded_blocklist_are_detected(self):
        for loc in (
            "Manila, Philippines",
            "Warsaw, Poland",
            "Dublin, Ireland",
            "Johannesburg, South Africa",
            "Seoul, South Korea",
            "Jakarta, Indonesia",
            "Lisbon, Portugal",
            "Zurich, Switzerland",
            "Moscow, Russia",
            "Stockholm, Sweden",
        ):
            assert is_us_plan({"locations": [loc]}) is False, loc

    def test_plain_us_plan_is_still_us_only(self):
        assert is_us_plan({"locations": ["United States"]}) is True
        assert is_us_plan({"locations": ["San Francisco, CA", "New York, NY"]}) is True
        assert is_us_plan({"locations": ["Dallas, TX"]}) is True
        assert is_us_plan({"locations": ["Dallas"]}) is True

    def test_dict_locations_resolve_via_country_key(self):
        assert (
            is_us_plan({"locations": [{"city": "London", "country": "United Kingdom"}]})
            is False
        )

    def test_dict_locations_without_country_key_fall_back_to_city_state(self):
        assert (
            is_us_plan({"locations": [{"city": "Austin", "state": "TX"}, "Remote"]})
            is True
        )


class TestDictCandidates:
    def test_dict_with_location_key(self):
        assert is_us_plan({"locations": [{"location": "London, UK"}]}) is False

    def test_dict_with_only_city(self):
        assert is_us_plan({"locations": [{"city": "Chicago"}]}) is True

    def test_dict_with_no_usable_keys_is_skipped(self):
        assert is_us_plan({"locations": [{"foo": "bar"}]}) is True
