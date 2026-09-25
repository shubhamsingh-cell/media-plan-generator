"""Regressions in role-title matching (role_match.match_role_phrase and its
consumers h1b_data._normalize_role, gold_standard._lookup_role_difficulty,
gold_standard._match_role_to_salary_range).

1. 70e65a6/d6ca718 made H-1B matching require the whole phrase or all its
   words, which also dropped plurals and developer titles: "Software
   Engineers", "Registered Nurses", "Accountants", "Mechanical Engineers",
   "Java Developer", "Frontend Developer", "Full-Stack Developer" returned
   None (they were matched before).
2. gold_standard._match_role_to_salary_range still used a raw substring
   test: "Cookie Packer" priced as a cook, "Nursery Worker" as a nurse,
   "Observer" as a server, "Driverless Car Tester" as a driver.

"Machine Operator" must still never match the data-scientist / machine
learning profiles.
"""

from __future__ import annotations

import pytest

import gold_standard as gs
import h1b_data
from role_match import match_role_phrase


@pytest.mark.parametrize(
    "title,expected",
    [
        ("Software Engineers", "software_engineer"),
        ("Registered Nurses", "registered_nurse"),
        ("Accountants", "accountant"),
        ("Mechanical Engineers", "mechanical_engineer"),
        ("Java Developer", "software_engineer"),
        ("Frontend Developer", "software_engineer"),
        ("Full-Stack Developer", "software_engineer"),
        ("Senior Python Developers", "software_engineer"),
        ("Data Scientists", "data_scientist"),
    ],
)
def test_h1b_matches_plurals_and_developer_titles(title, expected):
    assert h1b_data._normalize_role(title) == expected


@pytest.mark.parametrize(
    "title",
    [
        "Machine Operator",
        "Machine Operators",
        "Electrical Technician",
        "Real Estate Developer",
        "Business Developer",
        "Production Supervisor",
    ],
)
def test_h1b_still_rejects_blue_collar_and_non_software_titles(title):
    assert h1b_data._normalize_role(title) is None


@pytest.mark.parametrize(
    "title,expected",
    [
        ("cookie packer", None),
        ("nursery worker", None),
        ("observer", None),
        ("driverless car tester", None),
        ("production supervisor", None),
        ("production manager", None),
        ("machine operator", None),
        ("registered nurses", "registered nurse"),
        ("line cooks", "line cook"),
        ("delivery drivers", "delivery driver"),
        ("java developer", "software engineer"),
        ("full-stack developer", "full stack"),
        ("senior product manager", "product manager"),
        ("rn", "registered nurse"),
    ],
)
def test_salary_range_lookup_is_whole_word(title, expected):
    rng, kw = gs._match_role_to_salary_range(title)
    assert (kw or None) == expected, (title, kw)
    assert (rng is None) == (expected is None)


def test_machine_operator_never_gets_machine_learning_profiles():
    for phrases in (
        list(h1b_data._ROLE_ALIASES),
        list(gs._ROLE_SALARY_RANGES),
    ):
        hit = match_role_phrase("machine operator", phrases)
        assert hit is None or "learning" not in hit, hit
    diff = gs._lookup_role_difficulty("Machine Operator")
    assert not diff or "machine learning" not in str(diff).lower()
