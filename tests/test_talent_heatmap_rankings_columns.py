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
    by_name = {
        (loc.get("display_name") or loc["location"]): loc
        for loc in result["comparison"]["locations"]
    }
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
