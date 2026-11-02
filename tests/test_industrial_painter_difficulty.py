"""Sanity check of the "Industrial Painter" difficulty profile (handoff
2026-10-23 section 4 item 11; profile added in 0eaa532).

Checked 2026-11-03 against its neighbours on a US and a German plan: the
profile scores 6 (US) / 8 (Germany), the same as Welder (same skilled-trade
tier, by design), below Electrician (8 / 10), and a Paint Shop Supervisor
never scores below the painters it supervises. What was not sane: the other
paint-shop titles the standardizer files under the same occupation
(industrial_painter, SOC 51-9124: spray painter, coating operator, paint
line operator, ...) missed the profile and fell to the unclassified
default (5 / 7), so two titles for one job scored differently in one plan.
"""

from __future__ import annotations

import pytest

import gold_standard as gs
import standardizer

PAINT_SHOP_TITLES = [
    "Spray Painter",
    "Coating Operator",
    "Paint Line Operator",
    "Paint Technician",
    "Powder Coater",
]


def _scores(roles, loc):
    out = gs.classify_difficulty(
        {"target_roles": roles, "locations": [loc], "industry": "automotive"}
    )
    return {r["role_title"]: r for r in out}


@pytest.mark.parametrize("loc", ["Columbus, OH", "Berlin, Germany"])
def test_industrial_painter_sits_with_welder_below_electrician(loc):
    s = _scores(
        ["Industrial Painter", "Welder", "Electrician", "Paint Shop Supervisor"], loc
    )
    painter = s["Industrial Painter"]["complexity_score"]
    assert painter == s["Welder"]["complexity_score"]
    assert painter < s["Electrician"]["complexity_score"]
    assert s["Paint Shop Supervisor"]["complexity_score"] >= painter


@pytest.mark.parametrize("loc", ["Columbus, OH", "Berlin, Germany"])
@pytest.mark.parametrize("title", PAINT_SHOP_TITLES)
def test_every_paint_shop_title_scores_as_the_occupation_it_is(title, loc):
    assert standardizer.normalize_role(title) == "industrial_painter"
    s = _scores(["Industrial Painter", title], loc)
    assert s[title]["role_profile_matched"] is True
    assert s[title]["complexity_score"] == s["Industrial Painter"]["complexity_score"]
    assert s[title]["avg_time_to_fill_days"] == s["Industrial Painter"]["avg_time_to_fill_days"]
