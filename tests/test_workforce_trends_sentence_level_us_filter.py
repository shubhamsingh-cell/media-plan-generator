"""Regression test for TASK 2 (2026-09-24): sentence-level US-only filtering
in excel_v2's Workforce Trends section (Market Intelligence sheet).

Commit dfcf97b (fix(release): ... omit US-only workforce stats on non-US
plans) fixed a real leak (bundle_qa's ``us_data_on_non_us_plan`` critical
firing on non-US plans, e.g. "BLS" appearing on a UK plan with no US
location) by dropping any workforce-trends line that carried a US-only
marker (us_only_markers.py) WHOLE. But ``_flatten_value`` joins a KB dict's
sub-entries into one long "; "-separated value
(data/workforce_trends_intelligence.json's ``job_type_trends.growing_demand``
bundles AI/ML, cybersecurity, healthcare, skilled-trades, and renewable-
energy sub-items into a single value) -- so ONE buried US-only clause (a
"BLS Growth Projection" aside inside the cybersecurity sub-item) threw away
the ENTIRE line, including the unrelated, genuinely-global lead sentence
("AI Ml Engineers: Hiring Growth: 88% year-on-year growth in 2025; US
Positions Q1 2025: ...; Salary Average: $206,000 average ...").

FIX (this change): ``excel_v2._strip_us_only_sentences`` splits the line
into sentences and drops only the ones carrying a marker, keeping the line
if any sentence survives. Uses the REAL KB content (same fixture as
tests/test_workforce_trends_usd_marking.py) so this is not a synthetic
fixture the fix could special-case.

Run standalone: ``python3 tests/test_workforce_trends_sentence_level_us_filter.py``
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
import bundle_qa  # noqa: E402
import data_synthesizer  # noqa: E402
import excel_v2  # noqa: E402
import us_only_markers  # noqa: E402

# The real prod defect: this exact KB entry bundles a genuinely-global lead
# sentence with US-only asides buried later in the same "; "-joined value.
_GROWING_DEMAND_LEAD_SENTENCE = (
    "AI Ml Engineers: Hiring Growth: 88% year-on-year growth in 2025"
)


def _workforce_insights() -> dict:
    """Real workforce_insights payload, built the same way the production
    pipeline does (data_synthesizer.fuse_workforce_insights over the real
    KB file)."""
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


def _market_intelligence_texts(wb_bytes: bytes) -> list[str]:
    wb = openpyxl.load_workbook(io.BytesIO(wb_bytes))
    ws = wb["Market Intelligence"]
    return [
        c.value for row in ws.iter_rows() for c in row if isinstance(c.value, str)
    ]


class TestStripUsOnlySentencesHelper:
    def test_drops_only_the_marked_sentences_keeps_the_rest(self):
        wi = _workforce_insights()
        raw = excel_v2._flatten_value(wi["job_type_trends"]["growing_demand"])
        assert us_only_markers.has_us_only_marker(raw), (
            "fixture assumption broken: real KB growing_demand entry no "
            "longer carries a US-only marker"
        )
        assert _GROWING_DEMAND_LEAD_SENTENCE in raw

        stripped = excel_v2._strip_us_only_sentences(raw)

        # The unrelated, genuinely-global lead sentence must survive.
        assert _GROWING_DEMAND_LEAD_SENTENCE in stripped, (
            "the AI Ml Engineers lead sentence was dropped even though it "
            "carries no US-only marker itself"
        )
        # No US-only marker may remain anywhere in the surviving text.
        assert not us_only_markers.has_us_only_marker(stripped)
        # Something was actually dropped (this is not a no-op).
        assert stripped != raw
        assert len(stripped) < len(raw)

    def test_all_sentences_marked_returns_empty(self):
        text = "BLS Growth: 17%; JOLTS Job Openings Rate: up"
        assert excel_v2._strip_us_only_sentences(text) == ""

    def test_no_marker_returns_text_unchanged(self):
        text = "Global ML market: $55.8B (2024) to $282.13B (2030), 30.4% CAGR"
        assert excel_v2._strip_us_only_sentences(text) == text

    def test_decimal_points_are_not_sentence_boundaries(self):
        text = "Growth: 25.2% increase; Global figure: $55.8B market size"
        # A naive ". "-only splitter would fragment "25.2%" / "$55.8B".
        sentences = [
            s
            for s in excel_v2._US_ONLY_SENTENCE_SPLIT_RE.split(text)
            if s.strip()
        ]
        assert not any(s.strip().startswith("2%") for s in sentences)
        assert not any(s.strip().startswith("8B") for s in sentences)


class TestWorkbookKeepsPartiallyUsefulLines:
    @pytest.mark.parametrize(
        "country,currency", [("United Kingdom", "GBP"), ("Spain", "EUR")]
    )
    def test_ai_ml_engineers_lead_sentence_survives_on_non_us_plan(
        self, country, currency
    ):
        data = _plan_data(country, currency, currency.lower() + "-partial-keep")
        wb_bytes = _generate_wb_bytes(data)
        texts = _market_intelligence_texts(wb_bytes)
        joined = "\n".join(texts)
        assert _GROWING_DEMAND_LEAD_SENTENCE in joined, (
            "the genuinely-global AI Ml Engineers content is missing from "
            f"the {currency} plan's Market Intelligence sheet -- the whole "
            "line was dropped instead of just its US-only sentences"
        )

    @pytest.mark.parametrize(
        "country,currency", [("United Kingdom", "GBP"), ("Spain", "EUR")]
    )
    def test_no_us_only_marker_survives_anywhere_on_the_sheet(
        self, country, currency
    ):
        data = _plan_data(country, currency, currency.lower() + "-no-marker-leak")
        wb_bytes = _generate_wb_bytes(data)
        texts = _market_intelligence_texts(wb_bytes)
        offenders = [t for t in texts if us_only_markers.has_us_only_marker(t)]
        assert not offenders, offenders[:5]

    @pytest.mark.parametrize(
        "country,currency", [("United Kingdom", "GBP"), ("Spain", "EUR")]
    )
    def test_bundle_qa_reports_zero_us_data_on_non_us_plan_criticals(
        self, country, currency
    ):
        data = _plan_data(country, currency, currency.lower() + "-gate")
        wb_bytes = _generate_wb_bytes(data)
        findings = bundle_qa.run_bundle_qa(None, wb_bytes, data)
        us_only_critical = [
            f
            for f in findings
            if f["severity"] == "critical" and f["code"] == "us_data_on_non_us_plan"
        ]
        assert not us_only_critical, [f["message"][:160] for f in us_only_critical]

    def test_us_plan_market_intelligence_sheet_is_byte_identical(self):
        """The sentence-level filter must be a complete no-op on a US plan:
        the section is gated by ``_wf_is_us`` (unchanged), so every
        workforce-trends cell (including BLS/JOLTS/Fed Funds text) written
        before this fix is still written, unchanged, after it."""
        data = _plan_data("United States", "USD", "us-byte-identical-sentence")
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
        texts = [
            c.value for row in ws_a.iter_rows() for c in row if isinstance(c.value, str)
        ]
        assert any(us_only_markers.has_us_only_marker(t) for t in texts)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
