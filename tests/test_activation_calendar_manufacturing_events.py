"""Activation Calendar events must fit a manufacturing / food-manufacturing
client.

Hershey plan (2026-09-24): industry "food_beverage" (the wizard key, food &
beverage MANUFACTURING) resolved to no _INDUSTRY_MONTHLY_EVENTS bucket, so the
calendar fell back to the generic/tech list -- "Grace Hopper (diversity/tech)"
in October, "SXSW (tech)", "Dreamforce", "AWS re:Invent prep". General
manufacturers got the construction-flavoured blue_collar_trades list.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import gold_standard as gs  # noqa: E402

_TECH_MARKERS = (
    "grace hopper",
    "sxsw",
    "dreamforce",
    "re:invent",
    "hr tech",
    "linkedin talent connect",
    "google i/o",
    "wwdc",
    "github",
    "construction",
)

_FOOD_INDUSTRIES = [
    "food_beverage",
    "Food & Beverage",
    "Food & Beverage Manufacturing",
    "food_manufacturing",
]
_MFG_INDUSTRIES = ["manufacturing", "Manufacturing", "industrial_manufacturing"]


def _events(industry: str) -> dict[int, list[str]]:
    cal = gs.build_activation_calendar(
        {"industry": industry, "campaign_start_month": 1}
    )
    return {m["month"]: m["key_events"] for m in cal["timeline"]}


def _all_text(industry: str) -> str:
    cal = gs.build_activation_calendar(
        {"industry": industry, "campaign_start_month": 1}
    )
    parts = [e for m in cal["timeline"] for e in m["key_events"]]
    parts += cal["industry_events"]
    return " | ".join(parts).lower()


@pytest.mark.parametrize("industry", _FOOD_INDUSTRIES + _MFG_INDUSTRIES)
def test_manufacturing_calendar_has_no_tech_or_construction_events(industry):
    text = _all_text(industry)
    for marker in _TECH_MARKERS:
        assert marker not in text, (industry, marker)


@pytest.mark.parametrize("industry", _FOOD_INDUSTRIES + _MFG_INDUSTRIES)
def test_october_is_manufacturing_day(industry):
    october = " ".join(_events(industry)[10]).lower()
    assert "manufacturing day" in october


@pytest.mark.parametrize("industry", _FOOD_INDUSTRIES)
def test_food_manufacturer_gets_food_industry_trade_shows(industry):
    ev = _events(industry)
    assert any("IFT FIRST" in e for e in ev[7])
    assert any("PACK EXPO" in e for e in ev[10])
    assert any("IPPE" in e for e in ev[1])
    assert _all_text(industry).count("plant shutdown") >= 1


def test_food_beverage_calendar_keeps_manufacturing_seasonal_overlay():
    # seasonal_hiring_trends.json "manufacturing" peaks in January.
    cal = gs.build_activation_calendar(
        {"industry": "food_beverage", "campaign_start_month": 1}
    )
    jan = cal["timeline"][0]
    assert jan["seasonal_phase"] == "peak"
    assert jan["seasonal_multiplier"] == 1.2


def test_restaurants_and_tech_keep_their_own_calendars():
    assert gs._get_industry_key("restaurant") == "hospitality"
    assert gs._get_industry_key("food service") == "hospitality"
    assert gs._get_industry_key("technology") == "tech"
    assert any("Grace Hopper" in e for e in _events("technology")[10])
    assert gs._get_industry_key("skilled_trades") == "blue_collar_trades"
