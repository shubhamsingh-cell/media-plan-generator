"""bundle_qa's "BLS" marker is judged by the plan's US market presence
(2026-10-18).

With BLS wages live, an Austin + Berlin plan's workbook correctly cites BLS on
the Austin row ("Sources: BLS OES, DOL H-1B/LCA" in the Market Intelligence
salary table, "BLS OES" in Quality Intelligence) and prints "n/a" on the Berlin
row. ``_check_us_data_on_non_us_plan`` decided "is this a US plan" with
``plan_geo.is_us_plan`` (False for any plan with a non-US market), so it
reported two false ``us_data_on_non_us_plan`` criticals on a correct bundle. The
parent branch, which never printed a BLS label, was clean.

The "BLS" source label now follows the same per-market test as a US holiday:
fine on any plan with a US market, a defect on a plan with none. The other
markers (Fed Funds, CPI, JOLTS, "Bls Sector Code", US pay statistics) are US
macro content and keep the plan-wide gate (the holiday-gate suite pins that).
"""

from __future__ import annotations

import io
import sys
from pathlib import Path
from typing import List, Tuple

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import bundle_qa  # noqa: E402

CODE = "us_data_on_non_us_plan"

AUSTIN_BERLIN = {"locations": ["Austin, TX", "Berlin, Germany"]}
BERLIN_ONLY = {"locations": ["Berlin, Germany"]}
US_ONLY = {"locations": ["Austin, TX"]}
US_ON_GLOBAL_REGION = {
    "locations": ["Austin, TX", "Dallas, TX"],
    "target_region": "global",
}

# The two cells the verifier's real Austin + Berlin bundle flagged.
SOURCES_LINE = ("Market Intelligence!B42", "Sources: BLS OES, DOL H-1B/LCA")
SOURCE_LABEL = ("Quality Intelligence!J14", "BLS OES")
BLS_CELLS: List[Tuple[str, str]] = [SOURCES_LINE, SOURCE_LABEL]


def _findings(plan: dict, *texts: str) -> list:
    units = [bundle_qa._TextUnit(t, f"Sheet!A{i + 1}") for i, t in enumerate(texts)]
    out: list = []
    bundle_qa._check_us_data_on_non_us_plan(units, None, plan, out)
    return [f for f in out if f["code"] == CODE]


# ---------------------------------------------------------------------------
# The reported case: Austin + Berlin
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("location,text", BLS_CELLS)
def test_a_bls_source_label_is_clean_on_an_austin_and_berlin_plan(location, text):
    assert bundle_qa.plan_geo.is_us_plan(AUSTIN_BERLIN) is False  # the old gate
    assert _findings(AUSTIN_BERLIN, text) == []


def test_the_berlin_row_printing_n_a_is_clean():
    assert _findings(AUSTIN_BERLIN, "n/a") == []


# ---------------------------------------------------------------------------
# The gate keeps its teeth
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("location,text", BLS_CELLS)
def test_a_bls_label_on_a_berlin_only_plan_is_still_critical(location, text):
    found = _findings(BERLIN_ONLY, text)
    assert len(found) == 1 and found[0]["severity"] == "critical", found
    assert "BLS" in found[0]["message"]


@pytest.mark.parametrize(
    "text",
    [
        "Fed Funds Rate: 3.64%",
        "CPI Index: 3.1%",
        "JOLTS openings (US markets)",
        "BLS JOLTS openings (US markets)",  # BLS passes, JOLTS does not
        "Bls Sector Code: 54",  # BLS-prefixed US macro key, not the source label
        "Expected Starting Salary: US$101,500 (US markets)",
    ],
)
def test_us_macro_markers_are_still_critical_on_an_austin_and_berlin_plan(text):
    found = _findings(AUSTIN_BERLIN, text)
    assert len(found) == 1 and found[0]["severity"] == "critical", found


def test_a_bls_label_does_not_hide_a_macro_marker_in_the_same_cell():
    found = _findings(AUSTIN_BERLIN, "Sources: BLS OES; Fed Funds Rate: 3.64%")
    assert len(found) == 1 and "Fed Funds" in found[0]["message"], found


# ---------------------------------------------------------------------------
# Unaffected plans
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("plan", [US_ONLY, US_ON_GLOBAL_REGION])
@pytest.mark.parametrize("location,text", BLS_CELLS)
def test_a_bls_label_on_an_all_us_plan_is_clean(plan, location, text):
    assert _findings(plan, text) == []


def test_the_scoped_markers_are_a_subset_of_the_shared_marker_list():
    shared = {m.lower() for m in bundle_qa._US_ONLY_MARKERS}
    assert bundle_qa._US_MARKET_SCOPED_MARKERS == frozenset({"bls"})
    assert bundle_qa._US_MARKET_SCOPED_MARKERS <= shared


# ---------------------------------------------------------------------------
# End to end through run_bundle_qa with a real workbook, at the real cells
# ---------------------------------------------------------------------------
def _bundle(cells: List[Tuple[str, str]]) -> Tuple[bytes, bytes]:
    openpyxl = pytest.importorskip("openpyxl")
    pptx_mod = pytest.importorskip("pptx")

    wb = openpyxl.Workbook()
    first = True
    for ref, text in cells:
        sheet, coord = ref.split("!")
        if first:
            ws = wb.active
            ws.title = sheet
            first = False
        elif sheet not in wb.sheetnames:
            ws = wb.create_sheet(sheet)
        else:
            ws = wb[sheet]
        ws[coord] = text
    xbuf = io.BytesIO()
    wb.save(xbuf)

    prs = pptx_mod.Presentation()
    prs.slides.add_slide(prs.slide_layouts[6])
    pbuf = io.BytesIO()
    prs.save(pbuf)
    return pbuf.getvalue(), xbuf.getvalue()


def _run(plan: dict, cells: List[Tuple[str, str]]) -> list:
    pptx_bytes, xlsx_bytes = _bundle(cells)
    findings = bundle_qa.run_bundle_qa(pptx_bytes, xlsx_bytes, plan)
    return [f for f in findings if f["code"] == CODE]


def test_an_austin_and_berlin_bundle_has_no_us_data_critical():
    assert _run(AUSTIN_BERLIN, BLS_CELLS) == []


def test_a_berlin_only_bundle_with_the_same_cells_reports_both_at_their_cells():
    found = _run(BERLIN_ONLY, BLS_CELLS)
    assert {f["location"] for f in found} == {SOURCES_LINE[0], SOURCE_LABEL[0]}
    assert all(f["severity"] == "critical" for f in found)


def test_a_us_only_bundle_is_unaffected():
    assert _run(US_ONLY, BLS_CELLS) == []


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
