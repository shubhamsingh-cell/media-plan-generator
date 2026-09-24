"""Regression test for the Hershey Company niche-board mismatch (Slack report,
2026-09-24, confirmed NOT touched by any fix as of commit ec676f5).

Client's concrete example: restaurant job sites (Poached, Culinary Agents,
RestaurantJobs.com) were recommended for a company in food & beverage
MANUFACTURING (not restaurants) -- a different industry despite superficial
keyword overlap ("food").

Root cause: excel_v2.INDUSTRY_NICHE_CHANNELS["food_beverage"] held a board
list copy-pasted from "hospitality_travel" (Poached, Culinary Agents,
RestaurantJobs.com are all restaurant/dining-industry vendors). app.py's
classify_industry() correctly resolves a raw industry string like "food &
beverage manufacturing" to the "food_beverage" NAICS sector (bls_sector
"Manufacturing", talent_profile "Food Service & Production Workforce",
legacy_key "food_beverage") -- the classifier is not at fault. But the niche
table keyed off that same legacy_key returned restaurant-dining boards
instead of food/beverage PRODUCTION boards (breweries, distilleries,
bakeries, dairy, meat processing), so the wrong-industry list reached the
client. Fixed by correcting the "food_beverage" table entry to real
food-manufacturing-specific boards (CareersInFood.com, IFT Career Center,
iHireManufacturing), verified to exist via live web search before being
added -- no fabricated vendor names.

The "hospitality_travel" key (real restaurants/dining) is untouched and
still carries Poached/Culinary Agents/Harri/Hcareers, which are correct
there.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402
from excel_v2 import INDUSTRY_NICHE_CHANNELS  # noqa: E402

# Restaurant/dining-industry vendor names that must never appear under the
# "food_beverage" (manufacturing/production) niche key.
_RESTAURANT_ONLY_BOARDS = {"Poached", "Culinary Agents", "RestaurantJobs.com", "Harri", "Hcareers"}


def _niche_boards_for(raw_industry: str) -> list[str]:
    """Same lookup path bundle generation uses: classify raw industry text,
    then key INDUSTRY_NICHE_CHANNELS off the resulting legacy_key."""
    profile = app.classify_industry(raw_industry, company_name="", roles=[])
    legacy_key = profile.get("legacy_key")
    return INDUSTRY_NICHE_CHANNELS.get(legacy_key, [])


def test_food_beverage_manufacturing_does_not_get_restaurant_boards():
    """DISCRIMINATING: pre-fix, this returned
    ['Poached', 'Culinary Agents', 'RestaurantJobs.com'] -- restaurant
    boards for a manufacturing client. Post-fix it must return
    food-production-specific boards instead."""
    boards = _niche_boards_for("food & beverage manufacturing")

    assert boards, "expected a non-empty niche board list for food_beverage"
    leaked = _RESTAURANT_ONLY_BOARDS & set(boards)
    assert not leaked, (
        f"restaurant-industry boards leaked into a food & beverage "
        f"MANUFACTURING client's niche recommendations: {leaked} (full list: {boards})"
    )
    # The corrected entry should be food-manufacturing-relevant, not a
    # generic/unrelated substitution.
    assert any("food" in b.lower() or "manufactur" in b.lower() for b in boards), boards


def test_beverage_manufacturing_variants_do_not_get_restaurant_boards():
    """Same industry, differently-worded raw input -- two more real
    non-matching-industry phrasings a client might type."""
    for raw in ("beverage manufacturing", "food production plant"):
        boards = _niche_boards_for(raw)
        leaked = _RESTAURANT_ONLY_BOARDS & set(boards)
        assert not leaked, (raw, leaked, boards)


def test_hospitality_travel_still_gets_restaurant_boards():
    """Guard against overcorrection: an ACTUAL restaurant/dining industry
    plan (legacy_key hospitality_travel) must keep its real restaurant
    boards -- this incident is about the food_beverage key only."""
    boards = INDUSTRY_NICHE_CHANNELS.get("hospitality_travel", [])
    assert "Poached" in boards and "Culinary Agents" in boards, boards
