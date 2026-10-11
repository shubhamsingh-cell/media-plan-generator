"""Plan-output audit (2026-10-11) -- C6: gold_standard industry aliases must
resolve to keys that exist in _INDUSTRY_TOP_EMPLOYERS, and a plan industry
with no employer roster must be FLAGGED as fallback instead of silently
reading the generic Amazon/Walmart/UPS list."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import gold_standard  # noqa: E402


def test_every_alias_targets_a_real_roster_key():
    missing = {
        alias: target
        for alias, target in gold_standard._INDUSTRY_ALIASES.items()
        if target not in gold_standard._INDUSTRY_TOP_EMPLOYERS
    }
    assert not missing, f"aliases pointing at absent roster keys: {missing}"


def test_canonical_plan_industries_resolve_to_their_own_roster():
    cases = {
        "healthcare_medical": "healthcare_medical",
        "finance_banking": "finance_banking",
        "retail_consumer": "retail_consumer",
        "insurance": "finance_banking",
        "pharma": "healthcare_medical",
    }
    for raw, expected in cases.items():
        assert gold_standard._resolve_industry_key(raw) == expected, raw


def test_healthcare_plan_gets_healthcare_employers_not_the_general_list():
    data = {
        "industry": "healthcare_medical",
        "roles": ["RN"],
        "competitors": [],
    }
    cmap = gold_standard.build_competitor_map(
        data, {"Baltimore, MD": {"hiring_difficulty": 6.0}}
    )
    employers = " | ".join(cmap["Baltimore, MD"]["top_employers"])
    assert "UnitedHealth" in employers or "HCA" in employers or "Kaiser" in employers
    assert "Amazon" not in employers and "Walmart" not in employers


def test_food_beverage_has_no_roster_and_is_flagged_as_fallback():
    # No food / CPG-manufacturing roster exists in the repo to source one
    # honestly, so food_beverage must fall back AND say so.
    assert gold_standard._resolve_industry_key("food_beverage") == ""
    assert gold_standard._industry_roster_is_fallback("food_beverage") is True
    assert gold_standard._industry_roster_is_fallback("cpg") is True
    assert gold_standard._industry_roster_is_fallback("healthcare_medical") is False


def test_roster_less_resolved_key_is_also_flagged():
    # an alias to an absent key must never look like a real roster
    saved = dict(gold_standard._INDUSTRY_ALIASES)
    try:
        gold_standard._INDUSTRY_ALIASES["zzz-test"] = "no_such_roster"
        assert gold_standard._industry_roster_is_fallback("zzz-test") is True
    finally:
        gold_standard._INDUSTRY_ALIASES.clear()
        gold_standard._INDUSTRY_ALIASES.update(saved)
