"""G9: the deck's market count is distinct PHYSICAL sites.

RTI audit (2026-10-11): "Geographic Coverage 16 markets" counted Remote,
Nationwide and DC + Washington (one place) against a 3-5 market benchmark. Remote
and Nationwide are targeting modes, not places; with Nationwide in the plan there
is nothing honest to compare, so the claim is omitted.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import geo_audit_helpers as h  # noqa: E402


def test_deck_omits_the_market_count_claim_when_nationwide_is_in_the_plan():
    texts = [t for _, t in h.deck_texts(h.build_plan(h.RTI_LOCATIONS))]
    assert not any(t == "Geographic Coverage" for t in texts)
    assert not any("16 markets" in t or "16 locations" in t for t in texts)


def test_market_count_is_distinct_physical_sites_not_len_locations():
    # Remote is a mode, DC + Washington are one place -> 3 sites, not 5 entries
    plan = h.build_plan(["Remote", "Dallas, TX", "Austin, TX", "DC", "Washington"])
    texts = h.deck_texts(plan)
    flat = [t for _, t in texts]
    assert "Geographic Coverage" in flat
    assert any(t.startswith("3 markets") for t in flat)
    assert not any(t.startswith("5 markets") for t in flat)


def test_executive_summary_counts_physical_sites_too():
    plan = h.build_plan(["Remote", "Dallas, TX", "Austin, TX", "DC", "Washington"])
    flat = " ".join(t for _, t in h.deck_texts(plan))
    assert "3 locations" in flat
    assert "5 locations" not in flat
