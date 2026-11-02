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


def _sheet_groups(ws) -> List[Dict[str, Any]]:
    """[{heading, rows: [(name, currency, estimated cell)]}] from Salary Comparison."""
    header = [c.value for c in ws[4]]
    cur_col = header.index("Currency") + 1
    est_col = header.index("Estimated Salary") + 1
    groups: List[Dict[str, Any]] = []
    r = 5
    while ws.cell(row=r, column=2).value:
        name = str(ws.cell(row=r, column=2).value)
        cur = ws.cell(row=r, column=cur_col).value
        if cur is None:  # a group heading row
            groups.append({"heading": name, "rows": []})
        else:
            groups[-1]["rows"].append((name, str(cur), str(ws.cell(row=r, column=est_col).value)))
        r += 1
    return groups


def _amount(cell: str) -> int:
    return int("".join(ch for ch in cell if ch.isdigit()))


@pytest.mark.parametrize(
    "locations",
    [
        LOCATIONS,
        # a foreign figure numerically ABOVE a US one: a plain descending sort
        # across currencies would put Berlin's euros above Memphis's dollars
        ["Memphis, TN", "Berlin, Germany", "London, UK", "Dallas, TX"],
    ],
)
def test_salary_comparison_sheet_never_ranks_across_currencies(locations):
    sal = th.get_salary_map(ROLE, locations, "tech_engineering")
    analysis = {
        "role": ROLE,
        "industry": "tech_engineering",
        "salary_map": sal,
        "comparison": th.compare_locations(ROLE, locations, "tech_engineering"),
        "budget": 250000,
        "num_hires": 3,
    }
    by_name = {s["display_name"]: s for s in sal}
    if "Memphis, TN" in locations:
        assert by_name["Germany"]["estimated_salary"] > min(
            s["estimated_salary"] for s in sal if s["currency"] == "USD"
        ), "fixture no longer has a foreign figure above a US one"
    wb = openpyxl.load_workbook(io.BytesIO(th.generate_heatmap_excel(analysis)))
    ws = wb["Salary Comparison"]
    groups = _sheet_groups(ws)
    assert groups[0]["heading"] == "US$ salaries"
    rows = [row for g in groups for row in g["rows"]]
    assert len(rows) == len(locations)
    for g in groups:
        curs = {cur for _, cur, _ in g["rows"]}
        if g["heading"].endswith("salaries"):
            assert len(curs) == 1, g  # one currency per group
            values = [_amount(est) for _, _, est in g["rows"]]
            assert values == sorted(values, reverse=True), g  # ranked within it
    headings = [g["heading"] for g in groups]
    assert len(headings) == len(set(headings)), headings  # contiguous groups
    note = "\n".join(
        str(c.value) for row in ws.iter_rows(min_row=5) for c in row if c.value
    )
    assert "never ranked across currencies" in note


def _slide_bars(prs, slide_index: int) -> List[float]:
    """Widths (inches) of the bar rectangles on a salary slide."""
    from pptx.util import Emu

    out = []
    for sh in prs.slides[slide_index].shapes:
        if sh.shape_type == 1 and not sh.has_text_frame or (
            sh.shape_type == 1 and not sh.text_frame.text
        ):
            if Emu(sh.height).inches < 0.3 and Emu(sh.left).inches >= 3.9:
                out.append(round(Emu(sh.width).inches, 2))
    return out


def test_a_currency_with_one_market_gets_no_bar_on_the_deck():
    locs = ["Dallas, TX", "Chicago, IL", "Berlin, Germany", "London, UK", "Zurich, Switzerland"]
    analysis = {
        "role": ROLE,
        "industry": "tech_engineering",
        "salary_map": th.get_salary_map(ROLE, locs, "tech_engineering"),
        "comparison": th.compare_locations(ROLE, locs, "tech_engineering"),
        "budget": 250000,
        "num_hires": 3,
    }
    prs = Presentation(io.BytesIO(th.generate_heatmap_ppt(analysis)))
    slide = next(
        i
        for i, sl in enumerate(prs.slides)
        if any(sh.has_text_frame and sh.text_frame.text == "Salary Comparison" for sh in sl.shapes)
    )
    bars = _slide_bars(prs, slide)
    # only the two US$ markets (Dallas, Chicago) are drawn; Germany and the UK
    # are alone in their currency, Switzerland has no annual figure
    assert len(bars) == 2, bars
    text = "\n".join(
        sh.text_frame.text for sh in prs.slides[slide].shapes if sh.has_text_frame
    )
    for heading in ("US$ salaries", "EUR salaries", "GBP salaries"):
        assert heading in text, heading


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


def test_recommendation_score_leaves_out_off_scale_terms(analysis):
    """A pound / franc / weekly figure never enters the US$-scaled salary term
    and a non-US cost-of-living index never enters the score: the term is left
    out (not scored as a neutral US$75k) and the row says so. A US row keeps
    the original four-term formula."""
    locs = analysis["comparison"]["locations"]
    for loc in locs:
        sal = loc["salary"]
        if sal["currency"] != "USD" or not sal["numeric"]:
            assert "Salary not scored" in loc["not_scored"], loc["location"]
            assert not str(sal["estimated_display"]).startswith("$")
    dallas = next(l for l in locs if l["location"] == "Dallas, TX")
    assert dallas["not_scored"] == ""
    expected = (
        dallas["talent_density"]["score"] * 0.4
        + (100 - dallas["competition"]["score"]) * 0.3
        + (200 - dallas["cost_of_living"]["index"]) * 0.15
        + max(0, 100 - dallas["salary"]["estimated"] / 1500) * 0.15
    )
    assert dallas["recommendation_score"] == round(min(100, max(0, expected)), 1)
    # fully scored locations are ranked first; Best Match is one of them
    assert locs[0]["is_best"] and locs[0]["not_scored"] == ""
    seen_partial = False
    for loc in locs:
        if loc["not_scored"]:
            seen_partial = True
        else:
            assert not seen_partial, "a fully scored row ranked below a partial one"


@pytest.mark.parametrize(
    "locations",
    [
        ["Dallas, TX", "London, UK", "Zurich, Switzerland", "Sydney, Australia", "Mumbai, India", "Berlin, Germany"],
        ["Dallas, TX", "Tokyo, Japan"],
    ],
)
def test_run_heatmap_analysis_scores_hiring_difficulty_for_every_location(locations):
    """estimated_salary None (no annual median) used to raise in the salary
    pressure term and empty the difficulty list for EVERY location."""
    result = th.run_heatmap_analysis(
        role=ROLE, industry="tech_engineering", locations=locations, budget=250000, num_hires=3
    )
    diffs = result["difficulties"]
    assert len(diffs) == len(locations), result.get("errors")
    for d in diffs:
        comps = d["components"]
        if d["location"] == "Dallas, TX":
            assert comps["salary_pressure"] is not None and comps["cost_of_living"] is not None
            assert d["not_scored"] == ""
        else:
            assert comps["salary_pressure"] is None and comps["cost_of_living"] is None
            assert "Salary not scored" in d["recommendation"]


def test_cost_of_living_rows_carry_their_currency():
    rows = {r["location"]: r for r in th.get_cost_of_living_factor(LOCATIONS)}
    assert rows["Sydney, Australia"]["median_salary"] is None
    assert rows["Sydney, Australia"]["median_salary_display"] == "A$1,741 a week (ABS 2025)"
    assert rows["London, UK"]["median_salary_currency"] == "GBP"
    assert rows["Dallas, TX"]["median_salary_currency"] == "USD"
