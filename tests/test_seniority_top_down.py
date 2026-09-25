"""Seniority is resolved top-down; common titles keep 932bb65's tier.

d6ca718 stopped _lookup_role_difficulty matching "cto" inside "director"
and borrowing a profile on ONE shared word. Titles that had only been
rated correctly by those accidents then fell to classify_difficulty's
seniority-keyword loop, which walked _SENIORITY_KEYWORDS bottom-up
(intern, junior, mid, ...) and stopped at the first hit -- so "associate"
/ "assistant" made "Associate Director", "Assistant Director" and "Senior
Associate" junior (0.6, job_boards), and "Senior Director" only senior.
"Case Worker" hit the frontline head-noun rule's "case" blocker and fell
to the professional default ($76,500 in Hershey, PA).

Parity table: 932bb65's output for the same Hershey, PA / food_beverage
plan, for every title where 932bb65 was correct. Titles where 932bb65 was
wrong by accident are pinned separately with the corrected value.
"""

from __future__ import annotations

import pytest

import gold_standard as gs

_LOCATION = ["Hershey, PA"]

# title, seniority, budget_weight, channel_emphasis, salary median, tier
_PARITY_932BB65 = [
    ("Senior Associate", "senior", 1.8, "niche_boards", 76_500, "professional"),
    ("Production Worker", "entry", 0.6, "job_boards", 42_075, "frontline"),
    ("Sanitation Worker", "entry", 0.6, "job_boards", 42_075, "frontline"),
    ("Janitorial Staff", "entry", 0.6, "job_boards", 33_150, "frontline"),
    ("Line Operator", "mid", 1.0, "balanced", 65_025, "skilled_trade"),
    ("Packaging Operator", "mid", 1.0, "balanced", 65_025, "skilled_trade"),
    ("Senior Specialist", "senior", 1.8, "niche_boards", 76_500, "professional"),
    ("Software Engineer", "mid", 1.8, "niche_boards", 147_900, "professional"),
    ("Senior Software Engineer", "senior", 1.8, "niche_boards", 147_900, "professional"),
    ("Registered Nurse", "mid", 1.0, "balanced", 89_250, "licensed_clinical"),
    ("Truck Driver", "entry", 0.6, "job_boards", 66_300, "frontline"),
    ("CDL Driver", "mid", 1.0, "balanced", 66_300, "skilled_trade"),
    ("Warehouse Associate", "entry", 0.6, "job_boards", 38_250, "frontline"),
    ("Sales Associate", "entry", 0.6, "job_boards", 35_700, "frontline"),
    ("Retail Associate", "entry", 0.6, "job_boards", 42_075, "frontline"),
    ("Store Manager", "mid", 1.0, "balanced", 61_200, "professional"),
    ("Operations Manager", "mid", 1.0, "balanced", 76_500, "professional"),
    ("Plant Manager", "mid", 1.0, "balanced", 76_500, "professional"),
    ("Maintenance Technician", "mid", 1.0, "balanced", 45_900, "skilled_trade"),
    ("Electrician", "mid", 1.8, "niche_boards", 63_750, "skilled_trade"),
    ("Quality Assurance Technician", "mid", 1.0, "balanced", 65_025, "skilled_trade"),
    ("Food Safety Specialist", "mid", 1.0, "balanced", 76_500, "professional"),
    ("HR Generalist", "mid", 1.0, "balanced", 76_500, "professional"),
    ("Recruiter", "mid", 1.0, "balanced", 76_500, "professional"),
    ("Accountant", "mid", 1.0, "balanced", 73_950, "professional"),
    ("Financial Analyst", "mid", 1.0, "balanced", 89_250, "professional"),
    ("Data Analyst", "mid", 1.0, "balanced", 86_700, "professional"),
    ("Marketing Manager", "mid", 1.0, "balanced", 102_000, "professional"),
    ("VP of Sales", "executive", 3.0, "executive_search", 76_500, "professional"),
    ("Customer Service Representative", "entry", 0.6, "job_boards", 38_250, "frontline"),
    ("Forklift Operator", "entry", 0.6, "job_boards", 40_800, "frontline"),
    ("Line Cook", "entry", 0.6, "job_boards", 32_640, "frontline"),
    ("Intern", "intern", 0.4, "campus_recruiting", 76_500, "professional"),
    ("Associate Vice President", "executive", 3.0, "executive_search", 76_500, "professional"),
]

# 932bb65 was wrong by accident here (e.g. "cto" inside "director" made every
# Director title C-suite; "Machine Operator" borrowed the ML-engineer
# profile). Pinned at the corrected value.
_CORRECTED = [
    ("Associate Director", "director", 2.5, "executive_search", 76_500, "professional"),
    ("Assistant Director", "director", 2.5, "executive_search", 76_500, "professional"),
    ("Senior Director", "director", 2.5, "executive_search", 76_500, "professional"),
    ("Director of Operations", "director", 2.5, "executive_search", 76_500, "professional"),
    ("Chief Financial Officer", "executive", 3.5, "executive_search", 76_500, "professional"),
    ("Machine Operator", "mid", 1.0, "balanced", 65_025, "skilled_trade"),
    ("Team Member", "entry", 0.6, "job_boards", 42_075, "frontline"),
    ("Crew Member", "entry", 0.6, "job_boards", 42_075, "frontline"),
    ("Associate Engineer", "junior", 0.6, "job_boards", 76_500, "professional"),
    # Social-services professional; salary from the BLS "Social and Human
    # Service Assistants" band ($46,000 midpoint x Hershey 1.02), channel
    # weight as 932bb65 (0.6 / job_boards).
    ("Case Worker", "entry", 0.6, "job_boards", 46_920, "professional"),
]

_ALL = _PARITY_932BB65 + _CORRECTED


@pytest.fixture(scope="module")
def _plan_rows():
    plan = {
        "locations": list(_LOCATION),
        "industry": "food_beverage",
        "target_roles": [row[0] for row in _ALL],
    }
    difficulty = {r["role_title"]: r for r in gs.classify_difficulty(dict(plan))}
    city = gs.enrich_city_level_data(dict(plan))
    salary = next(iter(city.values()))["per_role_salary"]
    return difficulty, salary


@pytest.mark.parametrize("title,seniority,weight,emphasis,median,tier", _ALL)
def test_title_tier_parity(_plan_rows, title, seniority, weight, emphasis, median, tier):
    difficulty, salary = _plan_rows
    d = difficulty[title]
    assert (d["seniority_level"], d["budget_weight"], d["channel_emphasis"]) == (
        seniority,
        weight,
        emphasis,
    )
    assert (salary[title]["median"], salary[title]["tier"]) == (median, tier)


@pytest.mark.parametrize(
    "title,level",
    [
        ("associate director", "director"),
        ("assistant director", "director"),
        ("senior associate", "senior"),
        ("senior director", "director"),
        ("associate vice president", "director"),
        ("senior staff associate", "senior"),
        ("chief of staff", "executive"),
        ("associate", "junior"),
        ("assistant", "junior"),
        ("student intern", "intern"),
        ("specialist ii", "mid"),
    ],
)
def test_seniority_resolves_top_down(title, level):
    assert gs._detect_seniority_level(title) == level


def test_precedence_covers_every_seniority_level():
    assert sorted(gs._SENIORITY_PRECEDENCE) == sorted(gs._SENIORITY_KEYWORDS)
