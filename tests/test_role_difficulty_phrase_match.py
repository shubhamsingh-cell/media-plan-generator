"""Role-difficulty lookup must not match on a single shared word.

Hershey plant plan (2026-09-24): "Machine Operator" shared the word
"machine" with "machine learning engineer" and was rated Senior, 9/10,
Executive Search on deck slide 7. The raw-substring pass also matched
"cto" inside "director". gold_standard now uses the same whole-phrase /
all-words rule as h1b_data (role_match.match_role_phrase).
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import gold_standard  # noqa: E402
from role_match import match_role_phrase  # noqa: E402

_PLANT_ROLES = [
    "Machine Operator",
    "Production Supervisor",
    "Electrical Technician",
    "Industrial Maintenance Technician",
    "Quality Assurance Technician",
    "Warehouse Associate",
]


def _by_title(roles: list[str]) -> dict[str, dict]:
    rows = gold_standard.classify_difficulty(
        {"target_roles": roles, "locations": ["Hershey, PA"]}
    )
    return {r["role_title"]: r for r in rows}


def test_machine_operator_is_not_rated_senior_executive_search():
    # Its own plant profile (skilled trade), never the ML/data-science one.
    profile = gold_standard._lookup_role_difficulty("Machine Operator")
    assert profile is not None and profile["tier"] == "skilled_trade"
    row = _by_title(["Machine Operator"])["Machine Operator"]
    assert row["seniority_level"] not in ("senior", "staff", "director", "executive")
    assert row["complexity_score"] < 7
    assert row["channel_emphasis"] != "executive_search"


def test_hershey_plant_roles_never_get_executive_search():
    rows = _by_title(_PLANT_ROLES)
    for title in _PLANT_ROLES:
        assert rows[title]["channel_emphasis"] != "executive_search", title
        assert rows[title]["complexity_score"] < 9, title


def test_substring_inside_a_word_does_not_match():
    # "cto" is a raw substring of "director"; that must not pick the CTO profile.
    assert match_role_phrase("director of operations", ["cto"]) is None
    assert match_role_phrase("machine operator", ["machine learning engineer"]) is None


def test_genuinely_senior_titles_still_rate_senior():
    senior = ("senior", "staff", "director", "executive")
    rows = _by_title(
        [
            "Senior Software Engineer",
            "VP of Engineering",
            "Chief Financial Officer",
            "Director of Operations",
            "CFO",
            "Principal Engineer",
        ]
    )
    for title, row in rows.items():
        assert row["seniority_level"] in senior, (title, row["seniority_level"])
    assert rows["VP of Engineering"]["complexity_score"] >= 9
    assert rows["CFO"]["complexity_score"] >= 9


def test_all_words_match_in_any_order_still_works():
    assert gold_standard._lookup_role_difficulty("Senior Software Engineer")[
        "seniority"
    ] == "senior"
    assert match_role_phrase(
        "electrical controls engineer", ["electrical engineer", "data engineer"]
    ) == "electrical engineer"
