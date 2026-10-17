"""Step 4's "Market Pricing Benchmarks" read the benchmark data, with ONE multiplier.

Round-6 trust-lens finding: /api/pricing/live printed CPA $36.59-$287.50 and
CPH $8,131-$14,520 for a Los Angeles nurse from a table typed into
routes/pricing.py (per-channel bases x an industry multiplier of 2.2 for
healthcare x a city index with Los Angeles at 1.20 x a seniority multiplier,
with Adzuna / Supabase overlays scaled by typed factors). Step 1 quoted the
benchmark file's 1.24 for the same city, and every CPH shown was above the
$7.8K nursing upper quartile the preview cites.

The endpoint now returns only what the benchmark file carries: US channel ranges
(unadjusted), the industry's quartiles, and the metro multipliers step 1 uses,
shown beside each other and never multiplied together.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import cph_benchmark  # noqa: E402
import plan_copilot  # noqa: E402
from routes import pricing  # noqa: E402


def _file():
    with open(os.path.join(ROOT, "data", "recruitment_benchmarks_2026_deep.json"), encoding="utf-8") as fh:
        return json.load(fh)


NURSE = ("Registered Nurse", "Los Angeles, CA", "healthcare_medical")


def test_channel_ranges_are_the_files_by_channel_figures_unadjusted():
    got = pricing.build_live_pricing(*NURSE)
    by_channel = {k: v for k, v in _file()["by_channel"].items() if not k.startswith("_")}
    assert len(got["channels"]) == len(by_channel) == 7
    names = {c["name"]: c for c in got["channels"]}
    for key, node in by_channel.items():
        name = pricing._CHANNEL_NAMES[key]
        cpc, cpa = names[name]["cpc"], names[name]["cpa"]
        assert (cpc["low"], cpc["median"], cpc["high"]) == (
            node["typical_cpc_usd"]["low"], node["typical_cpc_usd"]["median"], node["typical_cpc_usd"]["high"],
        )
        assert (cpa["low"], cpa["median"], cpa["high"]) == (
            node["typical_cpa_usd"]["low"], node["typical_cpa_usd"]["median"], node["typical_cpa_usd"]["high"],
        )
    # the SAME ranges for a nurse in Los Angeles and a roofer in Fargo: nothing is
    # adjusted by industry, role or city (those are shown beside, not multiplied in)
    other = pricing.build_live_pricing("Roofer", "Fargo, ND", "blue_collar_trades")
    assert [c["cpa"] for c in other["channels"]] == [c["cpa"] for c in got["channels"]]


def test_the_channels_are_ordered_by_the_datas_median_cpa():
    medians = [c["cpa"]["median"] for c in pricing.build_live_pricing(*NURSE)["channels"]]
    assert medians == sorted(medians)


def test_the_industry_row_carries_no_cost_per_hire_quartiles_while_they_are_unverified():
    got = pricing.build_live_pricing(*NURSE)
    ind = got["industry"]
    assert ind["label"] == "nursing"
    row = _file()["by_industry"]["nursing"]
    assert row["cph_range_usd"]["status"] == "illustrative_unverified"
    # CPC / CPA are still the file's own sourced quartiles ...
    assert (ind["cpa"]["low"], ind["cpa"]["high"]) == (row["cpa_range_usd"]["p25"], row["cpa_range_usd"]["p75"])
    assert (ind["cpc"]["low"], ind["cpc"]["high"]) == (row["cpc_range_usd"]["p25"], row["cpc_range_usd"]["p75"])
    # ... but the cost-per-hire quartiles are withheld, with no source line either
    assert ind["cph"] is None and ind["cph_source"] == ""
    assert cph_benchmark.lookup("healthcare_medical", ["Registered Nurse"]) is None
    assert "4200" not in str(got) and "7800" not in str(got)


def test_the_metro_multiplier_is_the_one_step_1_quotes():
    got = pricing.build_live_pricing(*NURSE)
    metro = got["metro"]
    step1 = plan_copilot.metro_benchmark("Los Angeles, CA")
    assert step1 is not None
    assert (metro["name"], metro["cpa_mult"], metro["cpc_mult"]) == ("Los Angeles", step1["mult"], step1["cpc_mult"])
    assert metro["cpa_mult"] == 1.24 and metro["cpc_mult"] == 1.28  # not the typed 1.20
    assert metro["source"] == "C2ER 2024 and BLS 2025"
    # and step 1's own copy prints that same figure
    text = " ".join(n["message"] for n in plan_copilot.get_copilot_nudges_multi("location", "Los Angeles, CA", {}))
    assert "1.24x" in text


@pytest.mark.parametrize(
    "role, location, industry, label",
    [
        ("Software Engineer", "Austin, TX", "tech_engineering", "tech"),
        ("Cashier", "Dallas, TX", "retail_consumer", "retail"),
        ("CDL Driver", "Chicago, IL", "logistics_supply_chain", "logistics"),
    ],
)
def test_other_industries_resolve_to_their_own_rows(role, location, industry, label):
    got = pricing.build_live_pricing(role, location, industry)
    assert got["industry"]["label"] == label
    assert got["metro"] is not None


def test_no_fitting_row_or_metro_means_none_not_a_guess():
    got = pricing.build_live_pricing("Barista", "Reykjavik", "general_entry_level")
    assert got["industry"] is None and got["metro"] is None
    assert len(got["channels"]) == 7  # the US channel ranges still apply as labelled
    empty = pricing.build_live_pricing("", "", "")
    assert empty["industry"] is None and empty["metro"] is None


def test_the_source_line_and_date_are_the_files_own():
    got = pricing.build_live_pricing(*NURSE)
    meta = _file()["metadata"]
    assert got["data_updated"] == meta["last_updated"]  # kept for compatibility
    # the freshness label is the cells' data period, not the edit date
    assert got["data_vintage"] == meta["data_period"]
    assert got["reviewed"] == meta["last_updated"]
    assert got["data_vintage"] != got["reviewed"] and got["data_vintage"].startswith("2024-01 through 2025-12")
    assert got["source"].startswith("Benchmark data, sources listed in the file: ")
    assert "Appcast 2025" in got["source"]
    # the unverified cost-per-hire row no longer lends SHRM / NAHS names to the line
    for name in ("SHRM 2024", "NAHS 2024", "SHRM 2025"):
        assert name not in got["source"]
    # no live-looking "updated now" timestamp, no Supabase / Adzuna overlay
    assert "updated_at" not in got
    assert "supabase" not in got["source"].lower() and "adzuna" not in got["source"].lower()


class _Handler:
    def __init__(self, query: str) -> None:
        self.path = "/api/pricing/live?" + query
        self.sent = None

    def _send_json(self, payload, status_code=200):
        self.sent = (status_code, payload)


def test_the_route_serves_the_same_payload():
    handler = _Handler("role=Registered+Nurse&location=Los+Angeles%2C+CA&industry=healthcare_medical")
    assert pricing.handle_pricing_get_routes(handler, "/api/pricing/live", None)
    assert handler.sent is not None
    status, payload = handler.sent
    assert status == 200 and payload == pricing.build_live_pricing(*NURSE)


def test_the_hand_typed_tables_are_gone_from_the_module():
    source = open(os.path.join(ROOT, "routes", "pricing.py"), encoding="utf-8").read()
    for dead in ("cpc_base", "cpa_base", "cph_base", "_pl_us_index", "_pl_industry_mult", '"los angeles": 1.20', "_pl_comp_factor"):
        assert dead not in source, dead


def test_the_pricing_panel_label_shows_vintage_not_the_edit_date():
    """body_app_js.html words the source line from data_vintage + reviewed; the
    old "Data last updated <last_updated>" read a provenance edit as fresh data."""
    js = open(os.path.join(ROOT, "templates", "partials", "index", "body_app_js.html"), encoding="utf-8").read()
    assert "Data last updated" not in js
    assert '"Data vintage "' in js and '"file reviewed "' in js
    assert "pricingData.data_vintage" in js and "pricingData.reviewed" in js
    from us_only_markers import _US_ONLY_MARKERS

    for printed in ("Data vintage ", "file reviewed "):
        assert not [m for m in _US_ONLY_MARKERS if m.lower() in printed.lower()]
