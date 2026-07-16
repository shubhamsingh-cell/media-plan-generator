"""Tests for the data/joveo_media_plan_deck_2026.json restructure.

Verifies:
    - 'cpa_reference' and 'case_study' are industry-keyed, with only
      'ai_training' populated (owner rule: never fabricate client proof or
      un-sourced CPA ranges for other industries).
    - The old flat top-level keys are gone (a legacy consumer reading
      deck['cpa_reference']['roles'] directly gets nothing, by design --
      the next phase updates the readers in ppt_generator.py).
    - The ai_training content is preserved verbatim (same shape/values as
      the pre-restructure v1.0 flat block).
    - _meta documents the new schema.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_DECK_PATH = (
    Path(__file__).resolve().parent.parent / "data" / "joveo_media_plan_deck_2026.json"
)


@pytest.fixture(scope="module")
def deck() -> dict:
    with open(_DECK_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def test_deck_json_is_valid(deck: dict):
    assert isinstance(deck, dict)


def test_cpa_reference_is_industry_keyed(deck: dict):
    cpa_ref = deck["cpa_reference"]
    assert isinstance(cpa_ref, dict)
    # Old flat keys must be gone -- a legacy reader doing
    # deck["cpa_reference"].get("roles") / .get("description") gets nothing.
    assert "roles" not in cpa_ref
    assert "description" not in cpa_ref
    assert "currency" not in cpa_ref

    assert "ai_training" in cpa_ref
    ai_training = cpa_ref["ai_training"]
    assert ai_training["currency"] == "USD"
    assert isinstance(ai_training["roles"], list)
    assert len(ai_training["roles"]) == 7
    categories = {r["category"] for r in ai_training["roles"]}
    assert "Backend / Frontend Developers" in categories
    assert "Computational Scientists (Physics, Bio)" in categories


def test_case_study_is_industry_keyed(deck: dict):
    case_study = deck["case_study"]
    assert isinstance(case_study, dict)
    assert "title" not in case_study
    assert "results" not in case_study

    assert "ai_training" in case_study
    ai_training = case_study["ai_training"]
    assert ai_training["title"] == "Hiring high-quality AI Trainers at scale"
    assert len(ai_training["results"]) == 3
    assert len(ai_training["challenges"]) == 4
    assert len(ai_training["solution"]) == 4


def test_no_other_industries_fabricated(deck: dict):
    """Owner rule: never fabricate client case studies or un-sourced CPA
    ranges. Only 'ai_training' should be populated in either block until a
    future session sources real data for another industry.
    """
    assert list(deck["cpa_reference"].keys()) == ["ai_training"]
    assert list(deck["case_study"].keys()) == ["ai_training"]


def test_meta_documents_the_new_schema(deck: dict):
    meta = deck["_meta"]
    assert meta["version"] == "2.0"
    schema_notes = meta["schema_notes"]
    assert "breaking_change" in schema_notes
    assert "cpa_reference_schema" in schema_notes
    assert "case_study_schema" in schema_notes
    # Loudly flag that ppt_generator.py's readers are stale until rewired.
    assert "ppt_generator" in schema_notes["breaking_change"]


def test_unrelated_deck_sections_untouched(deck: dict):
    """Sections outside cpa_reference/case_study must be byte-identical in
    shape to the pre-restructure content (not part of this remediation).
    """
    assert deck["tagline"] == "High Performance, AI-Led Recruitment Marketing Platform"
    assert len(deck["campaign_methodology"]["steps"]) == 6
    assert len(deck["channel_mix"]["channels"]) == 7
    assert deck["hiring_difficulty"]["rows"]
    assert deck["sample_pricing_model"]["campaigns"]
    assert deck["why_joveo"]["differentiators"]
    assert deck["next_steps"]
    assert deck["example_brief"]["client"] == "Invisible Technologies"
