"""Talent Heatmap workbook: every "Location Rankings" header sits over the
field it names.

Found 2026-11-03 (pre-existing): the header row named column H "Hiring
Difficulty" and column I "Recommendation", but H held the recommendation
score (higher = better, the opposite direction of a difficulty) and I held
the "Best" marker. Hiring difficulty has its own sheet. The test reads the
generated sheet back and checks each header's column against the analysis.
"""

from __future__ import annotations

import io

import openpyxl
import pytest

import talent_heatmap as th

LOCS = ["Dallas, TX", "Austin, TX", "London, UK"]


@pytest.fixture(scope="module")
def result():
    return th.run_heatmap_analysis(
        role="Software Engineer",
        industry="tech_engineering",
        locations=LOCS,
        budget=150000,
        num_hires=3,
    )


def _sheet(result):
    wb = openpyxl.load_workbook(io.BytesIO(th.generate_heatmap_excel(result)))
    return wb["Location Rankings"]


def test_each_rankings_header_sits_over_its_field(result):
    ws = _sheet(result)
    headers = {ws.cell(row=5, column=c).value: c for c in range(2, 12)}
    assert "Hiring Difficulty" not in headers  # that is the next sheet
    by_name = {th._rankings_label(loc): loc for loc in result["comparison"]["locations"]}
    seen = 0
    for r in range(6, ws.max_row + 1):
        name = ws.cell(row=r, column=headers["Location"]).value
        loc = by_name.get(name)
        if loc is None:
            continue
        seen += 1
        score = ws.cell(row=r, column=headers["Recommendation Score"]).value
        assert str(score).startswith(f"{loc['recommendation_score']}/100"), (name, score)
        best = ws.cell(row=r, column=headers["Best Location"]).value
        assert (best == "Best") is bool(loc.get("is_best")), (name, best)
        density = ws.cell(row=r, column=headers["Talent Density"]).value
        assert str(density).startswith(f"{loc['talent_density']['score']}/100")
        comp = ws.cell(row=r, column=headers["Competition"]).value
        assert str(comp).startswith(f"{loc['competition']['score']}/100")
    assert seen == len(LOCS)


def test_a_foreign_city_row_is_named_by_city_with_national_figures(result):
    ws = _sheet(result)
    names = [ws.cell(row=r, column=2).value for r in range(6, ws.max_row + 1)]
    uk = [n for n in names if n and "London" in str(n)]
    assert uk, names
    assert "national" in uk[0].lower() and "United Kingdom" in uk[0], uk[0]
    assert "United Kingdom" not in names  # never the bare country as the row label


def test_cost_of_living_reads_n_a_outside_the_us_and_the_score_wraps(result):
    ws = _sheet(result)
    headers = {ws.cell(row=5, column=c).value: c for c in range(2, 12)}
    for r in range(6, ws.max_row + 1):
        name = str(ws.cell(row=r, column=2).value or "")
        col = ws.cell(row=r, column=headers["Cost of Living"]).value
        if "London" in name:
            assert str(col).startswith("n/a"), col
        elif name.startswith(("Dallas", "Austin")):
            assert not str(col).startswith("n/a"), col
        score = ws.cell(row=r, column=headers["Recommendation Score"])
        if score.value:
            assert score.alignment.wrap_text, name


_LABEL_SHEETS = ("Location Rankings", "Salary Comparison", "Hiring Difficulty", "Cost Analysis")


def _wb(result):
    return openpyxl.load_workbook(io.BytesIO(th.generate_heatmap_excel(result)))


def test_every_sheet_names_the_foreign_city_the_same_way(result):
    wb = _wb(result)
    for title in _LABEL_SHEETS:
        ws = wb[title]
        labels = [str(ws.cell(row=r, column=2).value or "") for r in range(1, ws.max_row + 1)]
        london = [v for v in labels if "London" in v]
        assert london and all("national figures" in v for v in london), (title, london)
        assert "United Kingdom" not in labels, title


def test_no_sheet_applies_the_us_relative_cost_of_living_to_a_non_us_row(result):
    wb = _wb(result)
    checked = set()
    for ws in wb.worksheets:
        header_row = None
        for r in range(1, 8):
            vals = [str(ws.cell(row=r, column=c).value or "") for c in range(2, 12)]
            if "Location" in vals:
                header_row = r
                break
        if header_row is None:
            continue
        col_cols = [
            c for c in range(2, 12)
            if "CoL" in str(ws.cell(row=header_row, column=c).value or "")
            or "Cost of Living" in str(ws.cell(row=header_row, column=c).value or "")
        ]
        for r in range(header_row + 1, ws.max_row + 1):
            name = str(ws.cell(row=r, column=2).value or "")
            if "London" not in name:
                continue
            for c in col_cols:
                v = str(ws.cell(row=r, column=c).value)
                assert v.startswith(("n/a", "—", "Not scored")), (ws.title, c, v)
                checked.add(ws.title)
    # both sheets with a cost-of-living column were actually checked
    assert {"Location Rankings", "Salary Comparison"} <= checked, checked


def test_location_cells_wrap_and_each_row_has_one_vertical_alignment(result):
    wb = _wb(result)
    for title in _LABEL_SHEETS:
        ws = wb[title]
        for r in range(6, ws.max_row + 1):
            cells = [ws.cell(row=r, column=c) for c in range(2, 10) if ws.cell(row=r, column=c).value]
            if len(cells) < 2:
                continue
            assert cells[0].alignment.wrap_text, (title, r)
            assert len({c.alignment.vertical for c in cells}) == 1, (title, r)
