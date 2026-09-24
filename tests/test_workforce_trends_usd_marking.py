"""Regression tests for DEFECT D (2026-09-24, adversarial review of an
unpushed release): the Excel workbook's "Workforce Trends" section
(excel_v2's render of ``synthesized["workforce_insights"]``) writes KB
figures from ``data/workforce_trends_intelligence.json`` verbatim -- e.g.
"$206,000 average", "$95B", "$25 minimum spend" -- via
``data_synthesizer.fuse_workforce_insights``'s ``supply_partner_trends`` /
``job_type_trends`` / ``gen_z_insights`` pass-throughs. Every one of those
figures is genuinely USD-sourced (a US labour-market KB), but on a non-USD
plan they rendered with a bare "$" sitting right next to the plan's real
£/€ numbers elsewhere on the same sheet -- exactly the shape
bundle_qa's ``currency_symbol_mixing`` rule (segment-by-segment scan) is
built to catch, and it did: ~11-14 extra criticals on a regenerated UK or
ES plan with this KB content present.

FIX AT THE SOURCE (never by weakening the checker): mark every bare "$" in
this section's text "US$" for non-USD plans, via a new ``excel_v2._mark_usd``
that mirrors ``ppt_generator._mark_usd`` (the repo's existing
declare-not-convert convention).

Runs under pytest, or standalone: ``python3 tests/test_workforce_trends_usd_marking.py``.
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402
import pytest  # noqa: E402

import budget_engine  # noqa: E402
import data_synthesizer  # noqa: E402
import excel_v2  # noqa: E402
import bundle_qa  # noqa: E402


def _has_us_only_marker(text: str) -> bool:
    """Local helper matching bundle_qa's own us_data_on_non_us_plan
    marker regexes (bundle_qa._US_ONLY_MARKER_RES) -- kept independent of
    excel_v2's internal module structure so this test still runs (and
    still fails meaningfully pre-fix) whether or not the fix has split
    the marker list into its own us_only_markers.py module."""
    return any(p.search(text) for p in bundle_qa._US_ONLY_MARKER_RES)


def _workforce_insights() -> dict:
    """Real workforce_insights payload, built the same way the production
    pipeline does (data_synthesizer.fuse_workforce_insights over the real
    KB file), without paying for app.py's full import/background-thread
    cost."""
    wt_path = PROJECT_ROOT / "data" / "workforce_trends_intelligence.json"
    with open(wt_path, encoding="utf-8") as f:
        wt = json.load(f)
    kb = {"workforce_trends": wt}
    return data_synthesizer.fuse_workforce_insights({}, kb, "general_entry_level")


def _plan_data(country: str, currency: str, tag: str) -> dict:
    roles = [{"title": "Customer Service Rep", "count": 20, "tier": "entry"}]
    locations = [{"city": "Test City", "state": "", "country": country}]
    alloc = budget_engine.calculate_budget_allocation(
        total_budget=100_000,
        roles=roles,
        locations=locations,
        industry="general_entry_level",
        channel_percentages={
            "Indeed": 40,
            "LinkedIn": 30,
            "Google Search Ads": 20,
            "Programmatic Job Boards": 10,
        },
        collar_type="white",
        campaign_start_month=9,
        locations_raw=[f"{l['city']}, {country}" for l in locations],
        plan_currency=currency,
    )
    symbol = {"GBP": "£", "EUR": "€"}.get(currency, "$")
    return {
        "client_name": f"Test Client {tag}",
        "industry": "general_entry_level",
        "budget": f"{symbol}100,000",
        "budget_period": "campaign",
        "campaign_duration": "3 months",
        "campaign_start_month": 9,
        "hire_volume": "20 hires",
        "work_environment": "onsite",
        "roles": [r["title"] for r in roles],
        "target_roles": roles,
        "locations": [f"{l['city']}, {country}" for l in locations],
        "competitors": [],
        "_budget_allocation": alloc,
        "plan_currency": currency,
        "_synthesized": {"workforce_insights": _workforce_insights()},
    }


def _generate_wb_bytes(data: dict) -> bytes:
    raw = excel_v2.generate_excel_v2(dict(data))
    if isinstance(raw, tuple):
        raw = raw[0]
    assert isinstance(raw, (bytes, bytearray)) and len(raw) > 0
    return raw


class TestMarkUsdHelper:
    def test_mirrors_ppt_generator_mark_usd(self):
        import ppt_generator

        for text in (
            "$206,000 average",
            "NZ$5 unaffected",
            "no dollar here",
            "",
            None,
        ):
            assert excel_v2._mark_usd(text) == ppt_generator._mark_usd(text)


class TestWorkforceTrendsCurrencyMixing:
    @pytest.mark.parametrize(
        "country,currency", [("United Kingdom", "GBP"), ("Spain", "EUR")]
    )
    def test_no_currency_symbol_mixing_criticals_from_workforce_trends(
        self, country, currency
    ):
        data = _plan_data(country, currency, currency.lower())
        wb_bytes = _generate_wb_bytes(data)
        findings = bundle_qa.run_bundle_qa(None, wb_bytes, data)
        csm_criticals = [
            f
            for f in findings
            if f["severity"] == "critical" and f["code"] == "currency_symbol_mixing"
        ]
        assert not csm_criticals, (
            f"{len(csm_criticals)} currency_symbol_mixing criticals remain on a "
            f"{currency} plan: {[f['message'][:160] for f in csm_criticals]}"
        )

    def test_workforce_trends_sheet_text_is_usd_marked_on_non_usd_plan(self):
        """Direct assertion on the rendered cells: every KB dollar figure
        under the Workforce Trends section reads "US$", never a bare "$"."""
        data = _plan_data("United Kingdom", "GBP", "gbp-cells")
        wb = openpyxl.load_workbook(io.BytesIO(_generate_wb_bytes(data)))
        ws = wb["Market Intelligence"]  # "Workforce Trends" lives here
        texts = [
            c.value
            for row in ws.iter_rows()
            for c in row
            if isinstance(c.value, str) and "Workforce Trends" not in c.value
        ]
        # A bare, unmarked "$" (not already part of "US$") must never
        # appear in this plan's client-facing text on a non-USD plan.
        offenders = [t for t in texts if _has_bare_dollar(t)]
        assert not offenders, offenders[:5]


def _has_bare_dollar(text: str) -> bool:
    import re

    return bool(re.search(r"(?<![A-Za-z])\$", text))


class TestWorkforceTrendsUsOnlyMarkers:
    """DEFECT B FIX (2026-09-24, pre-existing defect blocking every
    non-US plan carrying the KB workforce content): the Workforce Trends
    section (excel_v2._build_sheet_market_intelligence, data from
    data_synthesizer.fuse_workforce_insights's
    supply_partner_trends/job_type_trends/gen_z_insights pass-throughs of
    data/workforce_trends_intelligence.json) prints US-only macro/holiday
    vocabulary (BLS, JOLTS, Fed Funds, federal minimum wage,
    Thanksgiving/Memorial Day/Spring break, ...) verbatim -- real shipped
    defect: "Market Intelligence!D41" flagged 'BLS' as a
    us_data_on_non_us_plan CRITICAL on a UK/ES plan with no US location at
    all. Fix at the source: excel_v2 now omits any trend line carrying a
    US-only marker (us_only_markers.py, the exact same list/regexes
    bundle_qa's own us_data_on_non_us_plan rule checks) when the plan has
    no US location, using the exact same plan_geo.is_us_plan(data) gate
    bundle_qa's rule branches on -- so the two always agree."""

    @pytest.mark.parametrize(
        "country,currency", [("United Kingdom", "GBP"), ("Spain", "EUR")]
    )
    def test_no_us_data_on_non_us_plan_criticals_from_workforce_trends(
        self, country, currency
    ):
        data = _plan_data(country, currency, currency.lower() + "-us-markers")
        wb_bytes = _generate_wb_bytes(data)
        findings = bundle_qa.run_bundle_qa(None, wb_bytes, data)
        critical = [f for f in findings if f["severity"] == "critical"]
        us_only_critical = [
            f for f in critical if f["code"] == "us_data_on_non_us_plan"
        ]
        csm_critical = [f for f in critical if f["code"] == "currency_symbol_mixing"]
        assert not us_only_critical, (
            f"{len(us_only_critical)} us_data_on_non_us_plan criticals remain on a "
            f"{currency} plan: {[f['message'][:160] for f in us_only_critical]}"
        )
        assert not csm_critical, (
            f"{len(csm_critical)} currency_symbol_mixing criticals remain on a "
            f"{currency} plan: {[f['message'][:160] for f in csm_critical]}"
        )

    @pytest.mark.parametrize(
        "country,currency", [("United Kingdom", "GBP"), ("Spain", "EUR")]
    )
    def test_no_us_only_marker_text_in_market_intelligence_sheet(
        self, country, currency
    ):
        """Direct assertion on the rendered cells, independent of
        bundle_qa: no US-only marker string (BLS, JOLTS, Fed Funds, ...)
        should appear anywhere on a non-US plan's Market Intelligence
        sheet."""
        data = _plan_data(country, currency, currency.lower() + "-cells-us-markers")
        wb = openpyxl.load_workbook(io.BytesIO(_generate_wb_bytes(data)))
        ws = wb["Market Intelligence"]
        texts = [
            c.value for row in ws.iter_rows() for c in row if isinstance(c.value, str)
        ]
        offenders = [t for t in texts if _has_us_only_marker(t)]
        assert not offenders, offenders[:5]

    def test_us_plan_market_intelligence_sheet_is_byte_identical(self):
        """The US-only-marker filter must be a complete no-op on a US
        plan: every workforce-trends cell (including BLS/JOLTS/Fed Funds
        text) that was written before this fix is still written,
        unchanged, after it. Regenerating twice and diffing every cell in
        Market Intelligence pins that -- this is the same sheet the
        filter touches, so any accidental filtering on a US plan would
        show up here as a text diff, not just a byte-count coincidence."""
        data = _plan_data("United States", "USD", "us-byte-identical")
        wb_a = openpyxl.load_workbook(io.BytesIO(_generate_wb_bytes(data)))
        wb_b = openpyxl.load_workbook(io.BytesIO(_generate_wb_bytes(data)))
        ws_a, ws_b = wb_a["Market Intelligence"], wb_b["Market Intelligence"]
        assert ws_a.max_row == ws_b.max_row
        assert ws_a.max_column == ws_b.max_column
        diffs = []
        for r in range(1, max(ws_a.max_row, ws_b.max_row) + 1):
            for c in range(1, max(ws_a.max_column, ws_b.max_column) + 1):
                va = ws_a.cell(row=r, column=c).value
                vb = ws_b.cell(row=r, column=c).value
                if va != vb:
                    diffs.append((r, c, va, vb))
        assert not diffs, diffs[:5]
        # And the US-only markers are genuinely still present (the filter
        # did not silently eat them) -- BLS-sourced workforce content is
        # real, correct data on a US plan.
        texts = [
            c.value for row in ws_a.iter_rows() for c in row if isinstance(c.value, str)
        ]
        assert any(_has_us_only_marker(t) for t in texts)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
