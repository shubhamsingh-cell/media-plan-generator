"""Talent Heatmap salaries carry their own currency and are never ranked across
currencies (2026-11-03).

research.COUNTRY_DATA's median_salary is now a national statistics figure in
LOCAL currency for 16 countries (an annual equivalent where the office
publishes a weekly or monthly figure). The heatmap download printed that
number after a literal "$" (Australia "$90,532" = A$1,741 a week x52, New
Zealand "$89,960" = a weekly mean, Switzerland "$84,288") and ranked those
rows against US dollars. These tests pin the fix on the Excel and PPT writers.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402
from pptx import Presentation  # noqa: E402

import research  # noqa: E402
import talent_heatmap as th  # noqa: E402

ROLE = "Software Engineer"
LOCATIONS = [
    "Dallas, TX",
    "Sydney, Australia",
    "Auckland, New Zealand",
    "Zurich, Switzerland",
    "London, UK",
    "Berlin, Germany",
    "Chicago, IL",
    "Mumbai, India",
]
# the annualised local figures that must never reach a heatmap surface as "$"
_FORBIDDEN = ["$90,532", "$89,960", "$84,288", "$40,911", "$69,300", "$54,066"]


@pytest.fixture(scope="module")
def analysis() -> Dict[str, Any]:
    return {
        "role": ROLE,
        "industry": "tech_engineering",
        "salary_map": th.get_salary_map(ROLE, LOCATIONS, "tech_engineering"),
        "comparison": th.compare_locations(ROLE, LOCATIONS, "tech_engineering"),
        "budget": 250000,
        "num_hires": 3,
    }


@pytest.fixture(scope="module")
def workbook(analysis):
    return openpyxl.load_workbook(io.BytesIO(th.generate_heatmap_excel(analysis)))


@pytest.fixture(scope="module")
def deck_text(analysis) -> str:
    prs = Presentation(io.BytesIO(th.generate_heatmap_ppt(analysis)))
    return "\n".join(
        sh.text_frame.text for s in prs.slides for sh in s.shapes if sh.has_text_frame
    )


def _workbook_text(wb) -> str:
    return "\n".join(
        str(c.value)
        for ws in wb.worksheets
        for row in ws.iter_rows()
        for c in row
        if c.value is not None
    )


def test_no_annualised_local_figure_is_printed_as_dollars(workbook, deck_text):
    for text in (_workbook_text(workbook), deck_text):
        for bad in _FORBIDDEN:
            assert bad not in text, bad


def test_published_weekly_and_monthly_figures_are_shown_as_published(workbook, deck_text):
    text = _workbook_text(workbook)
    for label in (
        "CHF 7,024 a month (FSO 2024)",
        "A$1,741 a week (ABS 2025)",
        "NZ$1,730 a week (Stats NZ 2026, mean, FTE)",
    ):
        assert label in text, label
        assert label in deck_text, label
    assert research.NO_LOCAL_SALARY_BENCHMARK in text  # Mumbai: unsourced


def test_annual_median_countries_use_their_own_symbol(analysis):
    by_name = {s["display_name"]: s for s in analysis["salary_map"]}
    uk = by_name["United Kingdom"]
    assert uk["currency"] == "GBP" and uk["salary_numeric"]
    assert uk["estimated_display"].startswith("£")
    assert uk["median_display"] == "£40,911 (ONS 2026, provisional)"
    de = by_name["Germany"]
    assert de["currency"] == "EUR" and de["estimated_display"].startswith("€")
    au = by_name["Australia"]
    assert au["salary_numeric"] is False and au["estimated_salary"] is None


def test_salary_comparison_sheet_never_ranks_across_currencies(workbook):
    ws = workbook["Salary Comparison"]
    header = [c.value for c in ws[4]]
    assert "Currency" in header, header
    cur_col = header.index("Currency") + 1
    est_col = header.index("Estimated Salary") + 1
    currencies: List[str] = []
    usd_values: List[int] = []
    r = 5
    while ws.cell(row=r, column=2).value:
        cur = str(ws.cell(row=r, column=cur_col).value)
        currencies.append(cur)
        if cur == "USD":
            usd_values.append(int(str(ws.cell(row=r, column=est_col).value).strip("$").replace(",", "")))
        r += 1
    assert len(currencies) == len(LOCATIONS)
    # every currency is one contiguous group (no interleaving = no cross rank)
    seen: List[str] = []
    for cur in currencies:
        if not seen or seen[-1] != cur:
            assert cur not in seen, currencies
            seen.append(cur)
    assert currencies[0] == "USD"
    # ranked only within US$
    assert usd_values == sorted(usd_values, reverse=True) and len(usd_values) == 2
    note = "\n".join(
        str(c.value) for row in ws.iter_rows(min_row=r) for c in row if c.value
    )
    assert "never ranked across currencies" in note


def test_cost_analysis_never_compares_a_local_total_with_the_us_dollar_budget(workbook):
    ws = workbook["Cost Analysis"]
    rows = {
        str(ws.cell(row=r, column=2).value): str(ws.cell(row=r, column=7).value)
        for r in range(6, ws.max_row + 1)
        if ws.cell(row=r, column=2).value
    }
    assert rows["United Kingdom"] == "Not compared (GBP)"
    assert rows["Australia"] == "Not estimated"
    assert rows["Switzerland"] == "Not estimated"


def test_recommendation_score_salary_term_is_us_dollar_only(analysis):
    """A pound / franc / weekly figure never enters the US$-scaled salary term:
    such rows score the neutral 50 for it."""
    for loc in analysis["comparison"]["locations"]:
        sal = loc["salary"]
        if sal["currency"] != "USD" or not sal["numeric"]:
            assert isinstance(sal["estimated_display"], str)
            assert not sal["estimated_display"].startswith("$")


def test_cost_of_living_rows_carry_their_currency():
    rows = {r["location"]: r for r in th.get_cost_of_living_factor(LOCATIONS)}
    assert rows["Sydney, Australia"]["median_salary"] is None
    assert rows["Sydney, Australia"]["median_salary_display"] == "A$1,741 a week (ABS 2025)"
    assert rows["London, UK"]["median_salary_currency"] == "GBP"
    assert rows["Dallas, TX"]["median_salary_currency"] == "USD"
