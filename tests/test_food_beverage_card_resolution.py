"""Regression: the wizard's Food & Beverage card ("CPG, QSR, Agriculture")
is ambiguous -- restaurants and food manufacturers both pick it.

70e65a6 made the card key ``food_beverage`` mean food MANUFACTURING (NAICS
311) unconditionally, so restaurant clients (Chipotle: Crew Member, Kitchen
Manager; Darden: Line Cook, Server; Starbucks: Barista) were planned with
production boards (iHireManufacturing, FoodProcessing.com) and manufacturing
benchmarks, with no conflict flag. The card is now resolved by role/client
signals, identically in Phase 0 (standardizer canonical key) and in
classify_industry. Hershey (plant roles / known manufacturer) still resolves
to food manufacturing.
"""

from __future__ import annotations

import pytest

import app

_RESTAURANT_BOARDS = {"Hcareers", "Poached", "Culinary Agents", "Harri", "OysterLink"}
_MANUFACTURING_BOARDS = set(app.INDUSTRY_NICHE_CHANNELS["food_beverage"])

RESTAURANT_CASES = [
    ("Chipotle", ["Crew Member", "Kitchen Manager"]),
    ("Darden Restaurants", ["Line Cook", "Server"]),
    ("Starbucks", ["Barista"]),
    ("Acme Group", ["Line Cook", "Dishwasher"]),
]
MANUFACTURER_CASES = [
    ("The Hershey Company", ["Machine Operator", "Maintenance Technician", "Production Supervisor"]),
    ("The Hershey Company", []),
    ("Acme Foods", ["Production Supervisor", "Packaging Operator"]),
    ("PepsiCo", ["Sales Representative"]),
]


def _boards(legacy_key: str) -> set:
    return set(app.INDUSTRY_NICHE_CHANNELS.get(legacy_key) or [])


@pytest.mark.parametrize("company,roles", RESTAURANT_CASES)
def test_restaurant_on_food_beverage_card_is_hospitality(company, roles):
    profile = app.classify_industry("food_beverage", company, roles)
    assert profile["legacy_key"] == "hospitality_travel", profile["legacy_key"]
    assert profile["bls_sector"] == "Leisure and Hospitality"
    boards = _boards(profile["legacy_key"])
    assert not boards & _MANUFACTURING_BOARDS
    assert app._canonical_industry_for_request(
        {"industry": "food_beverage", "client_name": company, "roles": roles}
    ) == "hospitality"


@pytest.mark.parametrize("company,roles", MANUFACTURER_CASES)
def test_manufacturer_on_food_beverage_card_is_food_manufacturing(company, roles):
    profile = app.classify_industry("food_beverage", company, roles)
    assert profile["legacy_key"] == "food_beverage"
    assert profile["bls_sector"] == "Manufacturing"
    assert not _boards("food_beverage") & _RESTAURANT_BOARDS
    assert app._canonical_industry_for_request(
        {"industry": "food_beverage", "client_name": company, "roles": roles}
    ) == "food_beverage"


def test_no_signal_defaults_to_hospitality_like_pre_70e65a6():
    # Before 70e65a6 the standardizer aliased food_beverage -> hospitality,
    # so a plain "Food & Beverage" plan with no signals was hospitality.
    assert app.classify_industry("food_beverage", "", [])["legacy_key"] == "hospitality_travel"
    assert app._canonical_industry_for_request({"industry": "food_beverage"}) == "hospitality"


def test_manufacturer_wins_a_split_signal():
    # Manufacturer name outweighs one restaurant-sounding role.
    assert app._resolve_food_beverage_card("The Hershey Company", ["Cook"]) == "food_beverage"


def test_dict_shaped_roles_are_read():
    assert (
        app._resolve_food_beverage_card("", [{"title": "Barista"}, {"title": "Shift Manager"}])
        == "hospitality"
    )
