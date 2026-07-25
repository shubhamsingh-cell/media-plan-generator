"""Regression tests for the 2026-07 Uber ("commercial cab driver") industry
misclassification incident: a GBP 2M / 6-market plan shipped classified as
"Hospitality & Travel" (Marriott/Hilton/Hyatt competitors, hotel seasonality,
a Social Media-heavy channel mix) instead of Rideshare & Gig Economy.

Three root causes, all in app.py's classify_industry region:

1. No Rideshare/Gig card existed in the wizard's industry picker
   (templates/partials/index/body_content.html) even though app.py's
   INDUSTRY_NAICS_MAP already had a "rideshare" entry with "uber"/"lyft"/
   "gig"/"doordash" keywords -- so an Uber-type client had no correct card
   to click. Fixed by adding a "Rideshare & Gig Economy" card
   (data-industry="rideshare").

2. classify_industry()'s Step 1 (explicit legacy-key match) and Step 4
   (mixed fuzzy fallback) both returned immediately on any industry match,
   never comparing that result against what company_name/roles
   independently suggest. Fixed by adding an `industry_conflict` field to
   the returned dict (never silently overriding the explicit selection --
   see _infer_industry_from_signals in app.py).

3. The fuzzy fallback (Step 4) scored purely by keyword length
   (`score += len(kw)`), so a generic word like "travel" (6 chars) beat an
   unambiguous brand keyword like "uber" (4 chars). Also, _ROLE_INDUSTRY_MAP
   had zero driver/cab/taxi/transport/rideshare entries, so a driver-type
   role with no explicit industry fell through to the generic fallback
   instead of Rideshare/Transportation. Both fixed in app.py.

Vacuousness: every test below that asserts NEW behavior (industry_conflict
presence/absence, the new UI card, and the role-map-only inference) was
proven to fail against the pre-fix code (throwaway worktree at the parent
commit, see task report) and pass after the fix. Two tests
(`test_uber_cab_driver_empty_industry_resolves_to_rideshare` and the
false-positive guard) already held pre-fix too -- documented inline as
non-discriminating regression locks, not fix-proof, since neither the
rideshare NAICS entry nor the "no conflict when signals agree" state is new.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402

BACKWARD_COMPAT_KEYS = {"naics", "sector", "bls_sector", "talent_profile", "legacy_key", "keywords"}


# ---------------------------------------------------------------------------
# Root cause 1: the missing Rideshare/Gig wizard card
# ---------------------------------------------------------------------------


def test_new_rideshare_card_exists_with_correct_wording_and_round_trips():
    """The wizard's 21-card industry picker must have a Rideshare/Gig/
    On-Demand card so an Uber/Lyft/DoorDash-type client has a correct
    option to click -- pre-fix, no such card existed (only "Hospitality &
    Travel" mentioned "Travel", and "Logistics & Supply Chain" said
    "Freight, Warehousing", neither obviously fitting a rideshare client).
    """
    html_path = PROJECT_ROOT / "templates" / "partials" / "index" / "body_content.html"
    html = html_path.read_text(encoding="utf-8")

    marker = 'data-industry="rideshare"'
    idx = html.index(marker)  # raises ValueError (-> test failure) if absent
    # Slice out just this card's block (from its opening <div to the next
    # industry-card's opening <div, or EOF) to scope the wording check.
    block_start = html.rfind("<div", 0, idx)
    next_card_idx = html.find('class="industry-card"', idx + len(marker))
    block_end = html.find("<div", next_card_idx - 40) if next_card_idx != -1 else len(html)
    card_block = html[block_start:block_end].lower()

    assert 'onclick="selectindustry(this)"' in card_block, (
        "new card must use the same selectIndustry(this) wiring as every "
        "other industry-card"
    )
    assert 'role="button"' in card_block and 'tabindex="0"' in card_block, (
        "new card must match the existing cards' accessibility attributes"
    )
    # Description must name rideshare/gig/on-demand/delivery so it's the
    # obvious pick for Uber/Lyft/DoorDash-type clients.
    for term in ("rideshare", "gig", "on-demand", "delivery"):
        assert term in card_block, (
            f"new card's wording must mention {term!r} so it's recognizable "
            "for Uber/Lyft/DoorDash-type clients"
        )

    # The value must round-trip through classify_industry to the rideshare
    # NAICS entry (Step 2: direct top-level key match).
    profile = app.classify_industry("rideshare", "Uber", ["commercial cab driver"])
    assert profile["sector"] == "Rideshare & Gig Economy"
    assert profile["legacy_key"] == "logistics_supply_chain"
    assert profile.get("industry_conflict") is None, (
        "selecting the rideshare card for an actual rideshare company/role "
        "must not itself raise a conflict"
    )


# ---------------------------------------------------------------------------
# Root cause 2 + 3: industry_conflict signal (never silently overriding an
# explicit selection) + brand-keyword-outranks-generic-word inference
# ---------------------------------------------------------------------------


def test_uber_cab_driver_empty_industry_resolves_to_rideshare():
    """With no explicit industry selection, company_name="Uber" + role
    "commercial cab driver" must resolve to Rideshare & Gig Economy, not
    the generic/hospitality fallback.

    NON-DISCRIMINATING regression lock: this exact call already returned
    the correct sector pre-fix too (the rideshare NAICS entry + its "uber"
    keyword already existed; with raw_industry empty there's no competing
    generic keyword to lose to). Kept here to pin the behavior the other
    two root causes must not disturb, not as fix-proof by itself.
    """
    profile = app.classify_industry("", "Uber", ["commercial cab driver"])
    assert profile["sector"] == "Rideshare & Gig Economy"
    assert profile["legacy_key"] == "logistics_supply_chain"
    assert profile.get("industry_conflict") is None


def test_uber_cab_driver_hospitality_selected_still_hospitality_but_flags_conflict():
    """THE incident's exact repro: explicit "Hospitality & Travel" selection
    for company_name="Uber" + role "commercial cab driver" must still
    return Hospitality (never silently overridden -- see root cause 2's
    "do not silently override an explicit selection") but must now carry
    an industry_conflict signal identifying the disagreement, so
    downstream surfaces / the bundle QA linter can catch it before ship.

    DISCRIMINATING: pre-fix, classify_industry has no industry_conflict
    concept at all -- `profile.get("industry_conflict")` was always None.
    """
    profile = app.classify_industry(
        "Hospitality & Travel", "Uber", ["commercial cab driver"]
    )
    assert profile["sector"] == "Hospitality & Tourism", (
        "explicit selection must still win -- never silently overridden"
    )
    assert profile["legacy_key"] == "hospitality_travel"

    conflict = profile.get("industry_conflict")
    assert conflict is not None, (
        "company_name='Uber' + role 'commercial cab driver' disagreeing "
        "with an explicit Hospitality pick must be flagged"
    )
    assert conflict["selected_sector"] == "Hospitality & Tourism"
    assert conflict["selected_legacy_key"] == "hospitality_travel"
    assert conflict["inferred_sector"] == "Rideshare & Gig Economy"
    assert conflict["inferred_legacy_key"] == "logistics_supply_chain"
    assert conflict.get("signal")

    # Backward compatibility: every original key must still be present and
    # unchanged (many call sites read this dict) -- the conflict is an
    # ADDITION, not a replacement.
    assert BACKWARD_COMPAT_KEYS.issubset(profile.keys())


def test_uber_cab_driver_hospitality_selected_via_legacy_key_also_flags_conflict():
    """Same scenario, but the explicit selection arrives as the wizard's
    actual data-industry legacy-key value ("hospitality_travel") rather
    than the free-text sector label -- this is classify_industry's Step 1
    (app.py ~3107-3110), the path the root-cause-2 writeup cites directly.
    """
    profile = app.classify_industry(
        "hospitality_travel", "Uber", ["commercial cab driver"]
    )
    assert profile["sector"] == "Hospitality & Tourism"
    assert profile["legacy_key"] == "hospitality_travel"
    conflict = profile.get("industry_conflict")
    assert conflict is not None
    assert conflict["inferred_legacy_key"] == "logistics_supply_chain"


def test_marriott_hotel_housekeeper_hospitality_selected_no_conflict():
    """FALSE-POSITIVE GUARD (matters most): a genuinely hospitality client
    -- Marriott, "hotel housekeeper", explicit "Hospitality & Travel" pick
    -- must classify correctly AND carry NO conflict signal. If the
    conflict check fired here too, it would be noise on every legitimate
    hospitality plan, defeating the whole point of the signal.

    NON-DISCRIMINATING by construction: since classify_industry had no
    industry_conflict concept pre-fix, "no conflict" trivially held before
    the fix as well (the feature didn't exist to produce a false positive
    with). Kept as the guard that must keep holding as the feature evolves.
    """
    profile = app.classify_industry(
        "Hospitality & Travel", "Marriott", ["hotel housekeeper"]
    )
    assert profile["sector"] == "Hospitality & Tourism"
    assert profile["legacy_key"] == "hospitality_travel"
    assert profile.get("industry_conflict") is None, (
        "a genuinely hospitality company/role must not be flagged as "
        "conflicting with its own explicit hospitality selection"
    )


def test_brand_keyword_outranks_generic_word_in_conflict_inference():
    """Root cause 3, isolated: a company name containing an unambiguous
    brand/rideshare keyword ("Uber") must not lose to a generic industry
    word ("travel") that only appears in a role title, when inferring the
    company/role-only signal used for conflict detection. Without the
    brand-priority fix, "travel" (6 chars) would still beat "uber" (4
    chars) purely on keyword length, exactly like the original incident,
    and this scenario's conflict would go undetected (a false negative).

    DISCRIMINATING: pre-fix there is no conflict-inference code path at
    all, so this always returned None.
    """
    profile = app.classify_industry(
        "Hospitality & Travel", "Uber", ["travel coordinator"]
    )
    assert profile["sector"] == "Hospitality & Tourism"  # explicit pick still wins
    conflict = profile.get("industry_conflict")
    assert conflict is not None, (
        "'Uber' (brand) + a role containing the generic word 'travel' must "
        "still surface as a rideshare-vs-hospitality conflict"
    )
    assert conflict["inferred_sector"] == "Rideshare & Gig Economy"


def test_role_map_cab_driver_infers_rideshare_without_brand_name():
    """Root cause 3's second gap, isolated: _ROLE_INDUSTRY_MAP had ZERO
    driver/cab/taxi/transport/rideshare entries. With no explicit industry
    AND a company name that is NOT itself a rideshare brand, a "cab
    driver" role title alone must still infer Rideshare & Gig Economy via
    the role-keyword map, not the generic "General / Multi-Industry"
    fallback.

    DISCRIMINATING: pre-fix, "cab"/"driver" matched nothing in
    _ROLE_INDUSTRY_MAP, and no keyword in any NAICS profile matches "Acme
    Corp cab driver" either, so this fell all the way through to the final
    fallback (general_entry_level).
    """
    profile = app.classify_industry("", "Acme Corp", ["cab driver"])
    assert profile["sector"] == "Rideshare & Gig Economy"
    assert profile["legacy_key"] == "logistics_supply_chain"


@pytest.mark.parametrize(
    "role_title,expected_legacy_key",
    [
        ("CDL A Driver", "logistics_supply_chain"),
        ("Delivery Driver", "logistics_supply_chain"),
        ("Long-Haul Trucking Driver", "logistics_supply_chain"),
        ("Taxi Driver", "logistics_supply_chain"),
        ("Chauffeur", "logistics_supply_chain"),
        ("Courier", "logistics_supply_chain"),
    ],
)
def test_role_map_transport_keywords_all_resolve_to_logistics_legacy_key(
    role_title, expected_legacy_key
):
    """All of the newly-added role keywords (driver, cab, taxi, chauffeur,
    courier, trucking, transport, delivery) must resolve to a NAICS entry
    whose legacy_key is "logistics_supply_chain" -- the single bucket every
    downstream system (excel_v2, ppt_generator, research.py,
    competitive_intel, trend_engine, budget_engine, ...) already keys
    logistics/transportation/rideshare content by. Whether the specific
    winning NAICS entry is "rideshare" or "transportation" is immaterial
    downstream since both share this legacy_key.

    Discriminating vs. not (verified against the pre-fix parent commit):
    "CDL A Driver"/"Taxi Driver"/"Chauffeur" fail pre-fix (fall through to
    the generic "general_entry_level" fallback -- none of "cdl"/"driver"/
    "taxi"/"chauffeur" is a keyword anywhere in INDUSTRY_NAICS_MAP itself,
    only in the now-fixed _ROLE_INDUSTRY_MAP). "Delivery Driver"/"Long-Haul
    Trucking Driver"/"Courier" already passed pre-fix too -- "delivery",
    "trucking", and "courier" were already literal keywords in the
    transportation NAICS profile, so Step 4's fuzzy match already caught
    them; kept here as regression locks, not fix-proof.
    """
    profile = app.classify_industry("", "Acme Corp", [role_title])
    assert profile["legacy_key"] == expected_legacy_key


# ---------------------------------------------------------------------------
# Backward compatibility: unrelated / already-correct classifications must
# not change, and must never spuriously acquire a conflict signal.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_industry,company_name,roles,expected_sector",
    [
        ("tech_engineering", "TechCorp", ["software engineer"], "Technology & Software"),
        ("healthcare_medical", "Mercy Hospital", ["registered nurse"], "Healthcare & Life Sciences"),
        ("logistics_supply_chain", "FedEx", ["CDL A Driver"], "Transportation & Logistics"),
        ("", "Acme Bank", ["financial analyst"], "Financial Services & Insurance"),
        ("retail_consumer", "Target", ["cashier"], "Retail & E-Commerce"),
    ],
)
def test_self_consistent_briefs_never_flag_a_conflict(
    raw_industry, company_name, roles, expected_sector
):
    """When the explicit selection and the company/role signal agree (the
    overwhelming majority of real briefs), industry_conflict must be
    absent and the resolved sector must be unchanged from pre-fix
    behavior -- this is the backward-compatibility guard for Root Cause 2
    (never silently override) and Root Cause 3 (brand-priority gating must
    not fire when there's nothing to disagree with).
    """
    profile = app.classify_industry(raw_industry, company_name, roles)
    assert profile["sector"] == expected_sector
    assert profile.get("industry_conflict") is None


def test_classify_industry_return_shape_is_backward_compatible_without_conflict():
    profile = app.classify_industry("tech_engineering", "TechCorp", ["developer"])
    assert BACKWARD_COMPAT_KEYS.issubset(profile.keys())
    assert "industry_conflict" not in profile


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
