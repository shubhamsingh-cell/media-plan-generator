"""Plant / frontline titles keep their hourly tier after whole-phrase matching.

943c07e moved gold_standard._lookup_role_difficulty to
role_match.match_role_phrase, which correctly stopped "Machine Operator"
borrowing the ML-engineer profile -- but common plant titles then matched
nothing and fell to _lookup_role_tier's "professional" default. On a
Hershey, PA / food_beverage plan: Production Worker and Sanitation Worker
went $42,075 frontline -> $76,500, Janitorial Staff $33,150 -> $76,500 and
job_boards 0.6 -> niche_boards 1.8 ("staff" read as a senior keyword), and
Line / Packaging Operator skilled_trade -> professional. "Janitorial" also
lost the janitor salary band.

Expected values are 932bb65's output for the same plan (at-or-better:
Machine Operator was an ML-engineer 'senior / executive_search' there).
"""

from __future__ import annotations

import pytest

import gold_standard as gs

_PLAN = {
    "locations": ["Hershey, PA"],
    "industry": "food_beverage",
    "target_roles": [
        "Production Worker",
        "Sanitation Worker",
        "Janitorial Staff",
        "Line Operator",
        "Packaging Operator",
        "Machine Operator",
    ],
}

# title -> (median, tier) as 932bb65 produced (Machine Operator: skilled
# trade like the other operators, not the old ML-engineer professional).
_EXPECTED_SALARY = {
    "Production Worker": (42_075, "frontline"),
    "Sanitation Worker": (42_075, "frontline"),
    "Janitorial Staff": (33_150, "frontline"),
    "Line Operator": (65_025, "skilled_trade"),
    "Packaging Operator": (65_025, "skilled_trade"),
    "Machine Operator": (65_025, "skilled_trade"),
}

_EXPECTED_DIFFICULTY = {
    "Production Worker": (0.6, "job_boards"),
    "Sanitation Worker": (0.6, "job_boards"),
    "Janitorial Staff": (0.6, "job_boards"),
    "Line Operator": (1.0, "balanced"),
    "Packaging Operator": (1.0, "balanced"),
    "Machine Operator": (1.0, "balanced"),
}


def test_hershey_plant_salaries_keep_their_tier():
    city_data = gs.enrich_city_level_data(dict(_PLAN))
    rows = next(iter(city_data.values()))["per_role_salary"]
    got = {t: (r["median"], r["tier"]) for t, r in rows.items()}
    assert got == _EXPECTED_SALARY
    assert all(r["tier"] != "professional" for r in rows.values())


def test_hershey_plant_difficulty_keeps_hourly_channels():
    rows = gs.classify_difficulty(dict(_PLAN))
    got = {r["role_title"]: (r["budget_weight"], r["channel_emphasis"]) for r in rows}
    assert got == _EXPECTED_DIFFICULTY


def test_janitorial_keeps_janitor_salary_band():
    band, _kw = gs._match_role_to_salary_range("janitorial staff")
    assert band == gs._ROLE_SALARY_RANGES["janitor"]


@pytest.mark.parametrize(
    "title,tier",
    [
        ("Production Associate", "frontline"),
        ("Processing Worker", "frontline"),
        ("Line Worker", "frontline"),
        ("General Laborer", "frontline"),
        ("Cleaner", "frontline"),
        ("Mixer Operator", "skilled_trade"),
        ("Production Operator", "skilled_trade"),
        # head-noun rule: unknown titles with an hourly head noun
        ("Candy Line Worker", "frontline"),
        ("Bakery Associate", "frontline"),
        ("Shift Helper - Nights", "frontline"),
        ("Crew Members", "frontline"),
        ("Material Handler", "frontline"),
    ],
)
def test_plant_titles_are_never_professional(title, tier):
    assert gs._lookup_role_tier(title) == (tier, "role_profile_match")


@pytest.mark.parametrize(
    "title",
    [
        "Senior Associate",
        "Social Worker",
        "Research Associate",
        "Production Supervisor",
        "Director of Operations",
    ],
)
def test_head_noun_rule_leaves_senior_and_professional_titles(title):
    assert gs._lookup_role_tier(title)[0] == "professional"


@pytest.mark.parametrize(
    "title", ["Machine Operator", "Machine Operators", "Packaging Machine Operator"]
)
def test_machine_operator_is_not_ml_or_data_science(title):
    profile = gs._lookup_role_difficulty(title) or {}
    assert profile.get("tier") == "skilled_trade"
    assert profile.get("base_difficulty") == 5
    assert gs._lookup_role_difficulty("Machine Learning Engineer")["tier"] == "professional"
    assert gs._lookup_role_difficulty("Senior Software Engineer")["seniority"] == "senior"
