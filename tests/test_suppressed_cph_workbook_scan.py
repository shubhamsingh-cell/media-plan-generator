"""Regression: a plan with NO verified local cost-per-hire benchmark prints
no per-hire figure anywhere in the workbook (numbers verifier round 2,
2026-10-01, item 1; design-judge round 2, item 2).

Pre-fix (branch at 2f92a64), Brazil R$1.2M: the Executive Summary card
said "--" but ROI Projections D4 printed 8759.12 ("Avg Cost/Hire") and
H8:H12 a per-channel cost per hire, the Confidence Intervals sheet printed
pessimistic/expected/optimistic cost per hire (D:F, 15 cells), and the
methodology line (ROI Projections) and Niche Board Matching D3 said hire
totals are "anchored to industry cost-per-hire benchmarks" -- for a market
that has none. Every such figure is bounded by an FX-translated US value.
"""

from __future__ import annotations

import io
import os
import re
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import excel_v2  # noqa: E402
import tools_regen_bundles as T  # noqa: E402
from kb_loader import load_knowledge_base  # noqa: E402
from openpyxl import load_workbook  # noqa: E402

_PER_HIRE = re.compile(r"cost per hire|cost/hire|\bCPH\b|cost-per-hire", re.I)


def _brief(name, industry, budget, locations, roles, hire_volume):
    return {
        "client_name": name,
        "industry": industry,
        "budget": budget,
        "campaign_duration": "6 months",
        "hire_volume": hire_volume,
        "work_environment": "onsite",
        "locations": locations,
        "roles": roles,
        "target_roles": roles,
    }


@pytest.fixture(scope="module", params=["brazil", "uk"])
def suppressed_wb(request):
    brief = {
        "brazil": _brief(
            "Probe Contact BR", "general_entry_level", "R$1,200,000",
            ["São Paulo, Brazil", "Rio de Janeiro, Brazil"],
            ["Customer Service Representative", "Telemarketing Agent"], "500+ hires"),
        "uk": _brief(
            "Probe Bank", "finance_banking", "£150,000", ["London, UK"],
            ["Financial Analyst", "Accountant"], "50 hires"),
    }[request.param]
    data = T.build_plan_data(brief)
    assert data["_budget_allocation"]["metadata"]["industry_cph"]["claim_suppressed"]
    raw = excel_v2.generate_excel_v2(dict(data), load_kb_fn=load_knowledge_base)
    raw = raw[0] if isinstance(raw, tuple) else raw
    return load_workbook(io.BytesIO(raw))


def _per_hire_numbers(wb):
    """Numeric cells whose column header (nearest string above, <=40 rows)
    or row label (nearest string to the left) names a per-hire metric."""
    hits = []
    for ws in wb.worksheets:
        grid = {
            (c.row, c.column): c.value
            for row in ws.iter_rows()
            for c in row
            if c.value not in (None, "")
        }
        for (r, col), v in grid.items():
            if not isinstance(v, (int, float)) or isinstance(v, bool):
                continue
            hdr = next(
                (grid[(rr, col)] for rr in range(r - 1, max(0, r - 40), -1)
                 if isinstance(grid.get((rr, col)), str)),
                "",
            )
            lab = next(
                (grid[(r, cc)] for cc in range(col - 1, 0, -1)
                 if isinstance(grid.get((r, cc)), str)),
                "",
            )
            if _PER_HIRE.search(hdr) or _PER_HIRE.search(lab):
                hits.append((ws.title, ws.cell(row=r, column=col).coordinate, v))
    return hits


def _all_text(wb):
    return [
        (ws.title, c.coordinate, c.value)
        for ws in wb.worksheets
        for row in ws.iter_rows()
        for c in row
        if isinstance(c.value, str)
    ]


def test_no_per_hire_number_on_any_sheet(suppressed_wb):
    assert _per_hire_numbers(suppressed_wb) == []


def test_no_money_per_hire_in_text(suppressed_wb):
    # an amount quoted per hire: "R$8,759/hire", "£3,261 per hire",
    # "blended cost of R$8,759.12 per hire", "cost per hire of ¥789,474"
    per_hire_amount = re.compile(
        r"(?:US\$|R\$|[$£€¥₹])\s?[\d][\d,.]*\s*[KMk]?\s*(?:/hire|per hire)"
        r"|cost[- ]per[- ]hire (?:of|is|at) (?:US\$|R\$|[$£€¥₹])",
        re.I,
    )
    bad = [t for t in _all_text(suppressed_wb) if per_hire_amount.search(t[2])]
    assert bad == []


def test_methodology_does_not_claim_a_benchmark(suppressed_wb):
    texts = [t[2] for t in _all_text(suppressed_wb)]
    assert not any("anchored to industry cost-per-hire" in t for t in texts)
    assert not any("anchors total hires to cost-per-hire" in t for t in texts)
    assert any(
        t.startswith("Methodology: this market has no verified local cost-per-hire")
        for t in texts
    )


def test_one_indicative_note_per_sheet(suppressed_wb):
    for name in ("ROI Projections", "Confidence Intervals"):
        notes = [
            c.value for row in suppressed_wb[name].iter_rows() for c in row
            if isinstance(c.value, str) and c.value.startswith("Cost per hire: —")
        ]
        assert len(notes) == 1, (name, notes)
        assert suppressed_wb["ROI Projections"]["D4"].value == "—"
