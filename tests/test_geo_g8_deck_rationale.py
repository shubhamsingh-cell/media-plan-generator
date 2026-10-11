"""G8: the deck never invents a reason a market was chosen.

RTI audit (2026-10-11): the deck said "Remote was prioritized based on a balanced
talent-supply tier" -- invented from the flat default row -- while the workbook
said "selected by client footprint". A fallback or state-level row says nothing
about THIS market, so it can't be the reason it was chosen.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import geo_audit_helpers as h  # noqa: E402
import gold_standard  # noqa: E402
import insight_composer  # noqa: E402


def _rows(locations):
    return gold_standard.enrich_city_level_data({"locations": locations, "target_roles": ["Cook"]})


def test_fallback_or_state_level_rows_never_justify_a_market_choice():
    rows = _rows(["Remote", "Hershey, PA", "Philadelphia, PA"])
    assert insight_composer.geography_rationale("Remote", rows["Remote"]) == (
        "Remote was selected by client footprint."
    )
    assert insight_composer.geography_rationale("Hershey, PA", rows["Hershey"]) == (
        "Hershey, PA was selected by client footprint."
    )
    # a genuine city-level row still earns its data-backed sentence
    assert "prioritized based on" in insight_composer.geography_rationale(
        "Philadelphia, PA", rows["Philadelphia"]
    )


def test_deck_does_not_invent_a_rationale_for_remote():
    texts = [t for _, t in h.deck_texts(h.build_plan(h.RTI_LOCATIONS))]
    assert not any("Remote was prioritized" in t for t in texts)
    assert any("Remote was selected by client footprint" in t for t in texts)
