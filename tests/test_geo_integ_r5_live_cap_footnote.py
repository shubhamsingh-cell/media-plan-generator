"""R5 (review 2026-10-11): a hit live-lookup cap is footnoted on Market Intelligence.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "tests")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import geo_audit_helpers as h  # noqa: E402
import gold_standard  # noqa: E402
import plan_location  # noqa: E402
import research  # noqa: E402


def _res(loc):
    return plan_location.resolve_locations([loc])[0].to_dict()


# ── R5: a hit live-lookup cap is footnoted on the location table ───────────
def test_live_lookup_cap_is_footnoted_on_market_intelligence():
    data = h.build_plan(["Dallas, TX", "Austin, TX"])
    data["_enriched"] = {
        "geonames_data": {
            "locations": {},
            "live_lookup_cap": {
                "limit": 10,
                "requested": 13,
                "local_data_only": ["a", "b", "c"],
                "no_data": [],
            },
        }
    }
    wb = h.workbook(data)
    text = " ".join(h.sheet_text(wb["Market Intelligence"]))
    assert "Live lookups covered the first 10 locations" in text
    assert "curated data" in text


def test_no_cap_footnote_when_the_cap_was_not_hit():
    wb = h.workbook(h.build_plan(["Dallas, TX", "Austin, TX"]))
    assert "Live lookups covered" not in " ".join(h.sheet_text(wb["Market Intelligence"]))
