"""Tests for insight_composer -- client-specific prose fragments shared by
the deck and workbook generators."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from insight_composer import (  # noqa: E402
    compose_counter_strategy,
    geography_rationale,
    role_requirements_callout,
)


class TestComposeCounterStrategy:
    def test_interpolates_competitor_name(self):
        sentence = compose_counter_strategy("Acme Staffing", {"role": "CDL Driver"})
        assert "Acme Staffing" in sentence

    def test_interpolates_role_angle(self):
        sentence = compose_counter_strategy("Acme", {"role": "Registered Nurse"})
        assert "Registered Nurse" in sentence

    def test_interpolates_city_angle_when_no_role(self):
        sentence = compose_counter_strategy("Acme", {"city": "Denver"})
        assert "Denver" in sentence

    def test_empty_ctx_is_safe_and_still_names_competitor(self):
        sentence = compose_counter_strategy("Acme", {})
        assert "Acme" in sentence
        assert sentence  # non-empty, no crash

    def test_no_ctx_arg_at_all(self):
        sentence = compose_counter_strategy("Acme", None)
        assert "Acme" in sentence

    def test_blank_competitor_name_safe(self):
        sentence = compose_counter_strategy("", {"role": "Nurse"})
        assert sentence
        assert "This competitor" in sentence

    def test_no_lorem_or_placeholder_tokens(self):
        sentence = compose_counter_strategy("Acme", {"role": "Nurse", "city": "Boise"})
        lowered = sentence.lower()
        for banned in ("lorem", "ipsum", "{competitor}", "{angle}", "todo", "tbd"):
            assert banned not in lowered

    def test_two_competitors_same_bucket_are_not_byte_identical(self):
        ctx = {"role": "Warehouse Associate", "competitor_type": "direct_employer"}
        a = compose_counter_strategy("Acme Corp", ctx)
        b = compose_counter_strategy("Globex Inc", ctx)
        assert a != b

    def test_deterministic_across_calls(self):
        ctx = {"role": "Nurse", "city": "Austin", "competitor_type": "staffing_agency"}
        first = compose_counter_strategy("Acme", ctx)
        second = compose_counter_strategy("Acme", ctx)
        assert first == second

    def test_all_competitor_type_buckets_produce_varied_text(self):
        # >=4 distinct skeletons per bucket -- sample enough competitor/role
        # combos to observe more than one skeleton per bucket.
        for ctype in ("staffing_agency", "direct_employer", "gig_platform", "default"):
            seen = set()
            for i in range(12):
                sentence = compose_counter_strategy(
                    f"Competitor{i}",
                    {"role": f"Role{i}", "competitor_type": ctype},
                )
                seen.add(sentence)
            assert len(seen) > 1, f"bucket {ctype} never varied across 12 samples"

    def test_intensity_adds_priority_framing(self):
        sentence = compose_counter_strategy(
            "Acme", {"role": "Nurse", "intensity": "high"}
        )
        assert "priority" in sentence.lower()

    def test_no_intensity_omits_priority_framing(self):
        sentence = compose_counter_strategy("Acme", {"role": "Nurse"})
        assert "priority lane" not in sentence.lower()


class TestRoleRequirementsCallout:
    def test_propane_fuel_logistics_cdl(self):
        result = role_requirements_callout(
            "propane_fuel_logistics", ["CDL Driver", "Dispatcher"]
        )
        assert len(result) == 1
        assert "Hazmat (H)" in result[0]
        assert "Tanker (N)" in result[0]

    def test_healthcare_nurse_licensure(self):
        result = role_requirements_callout("healthcare_medical", ["Registered Nurse"])
        assert len(result) == 1
        assert "licensure" in result[0].lower()

    def test_senior_living_caregiver_memory_care(self):
        result = role_requirements_callout("senior_living", ["Caregiver"])
        assert len(result) == 1
        assert "memory-care" in result[0].lower()

    def test_empty_when_nothing_curated_matches(self):
        assert role_requirements_callout("retail_consumer", ["Cashier"]) == []
        assert role_requirements_callout("tech_engineering", ["Software Engineer"]) == []

    def test_empty_roles_list(self):
        assert role_requirements_callout("healthcare_medical", []) == []

    def test_industry_matches_but_role_does_not(self):
        # Healthcare industry with a non-nursing role shouldn't trigger the
        # nursing-specific callout.
        assert role_requirements_callout("healthcare_medical", ["Receptionist"]) == []

    def test_none_safe(self):
        assert role_requirements_callout("", []) == []


class TestGeographyRationale:
    def test_empty_city_data_is_honest_generic(self):
        assert (
            geography_rationale("Austin, TX", None)
            == "Austin, TX was selected by client footprint."
        )
        assert (
            geography_rationale("Austin, TX", {})
            == "Austin, TX was selected by client footprint."
        )

    def test_uses_available_unemployment_field(self):
        sentence = geography_rationale("Austin, TX", {"unemployment": 3.2})
        assert "3.2%" in sentence
        assert "Austin, TX" in sentence

    def test_uses_hiring_difficulty(self):
        sentence = geography_rationale("Austin, TX", {"hiring_difficulty": "High"})
        assert "high hiring difficulty" in sentence.lower()

    def test_combines_multiple_available_fields(self):
        sentence = geography_rationale(
            "Austin, TX",
            {"unemployment": 3.2, "supply_tier": "Tier 1", "salary_index": 1.1},
        )
        assert "3.2%" in sentence
        assert "tier 1" in sentence.lower()
        assert "1.1" in sentence

    def test_never_fabricates_numbers_not_in_city_data(self):
        sentence = geography_rationale("Austin, TX", {"hiring_difficulty": "High"})
        # No unemployment/salary_index provided -- none should appear.
        assert "%" not in sentence
        assert "salary index" not in sentence.lower()

    def test_blank_location_safe(self):
        sentence = geography_rationale("", None)
        assert sentence
        assert "selected by client footprint" in sentence
