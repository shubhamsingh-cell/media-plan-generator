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


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
