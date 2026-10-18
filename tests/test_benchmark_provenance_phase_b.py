"""Phase B of the client-facing benchmark provenance fix (2026-10-18).

Phase A (data only, live) stamped every ``industry_benchmarks`` row with a
``provenance`` string and removed the false attributions. These tests pin the
code half -- what the deliverables PRINT of those rows:

1. the workbook's Executive Summary "Industry Benchmarks" block prints a key
   WHITELIST of the KB row (not every key, with its inline "Appcast 2026" /
   "Recruitics" asides, trend notes and sub-statistics) and ONE footnote taken
   from ``plan_trust.BENCHMARK_FOOTNOTE`` -- the same string the deck prints;
   merged-cell value rows are given a height that shows every line;
2. the deck's benchmark slide no longer cites "Joveo Recruitment Benchmarks KB
   (2026)" (the table citing itself) -- it prints the shared footnote -- and
   the competitive-landscape slide's knowledge-base fallback rows get the same
   note instead of an invented "curated knowledge base" source;
3. no "Industry Cost-per-Hire" is ever the media-only
   ``recruitment_marketing_only`` figure (all-in total or nothing);
4. "Model vs. benchmark" compares to the CPA median (see
   tests/test_model_vs_benchmark_currency.py);
5. the deck's plan-vs-industry column is labelled a Nova working estimate, not
   an "<industry> Average";
6. no deliverable calls the assumed all-in Nova cost-per-hire table a
   "benchmark".
"""

from __future__ import annotations

import io
import logging
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest
from openpyxl import Workbook, load_workbook
from pptx import Presentation

import budget_engine
import bundle_qa
import excel_v2
import plan_trust
import ppt_generator
import tools_regen_bundles as regen
import us_only_markers

_SECTION_FOOTNOTE = getattr(plan_trust, "BENCHMARK_FOOTNOTE", None)

# KB keys whose rows may reach the client in this block (the whitelist).
_ALLOWED_LABELS = {
    "CPA",
    "CPC",
    "Industry Cost-per-Hire",
    "Apply Rate",
    "Time To Fill",
    "Seasonal Patterns",
}
# Rows the block used to print that are free text / sub-statistics.
_RETIRED_LABELS = (
    "Provenance",
    "Offer Acceptance Rate",
    "Quality Of Hire",
    "Source Of Hire",
)


def _bundle(brief: Dict[str, Any]) -> Dict[str, Any]:
    logging.disable(logging.CRITICAL)
    try:
        return regen.generate_bundle(dict(brief), Path(tempfile.mkdtemp()), "t")
    finally:
        logging.disable(logging.NOTSET)


@pytest.fixture(scope="module")
def healthcare() -> Dict[str, Any]:
    return _bundle(dict(regen.ATRIA_BRIEF, industry="Healthcare & Medical"))


@pytest.fixture(scope="module")
def telecom() -> Dict[str, Any]:
    return _bundle(dict(regen.ATRIA_BRIEF, industry="Telecommunications"))


@pytest.fixture(scope="module")
def uk_plan() -> Dict[str, Any]:
    brief = dict(regen.ATRIA_BRIEF)
    brief.update(
        {
            "client_name": "London NHS Trust",
            "industry": "Healthcare & Medical",
            "budget": "£2,000,000",
            "locations": ["London, UK"],
            "roles": ["Registered Nurse"],
            "target_roles": [{"title": "Registered Nurse", "count": 40, "tier": "Hourly"}],
            "hire_volume": "40 hires",
            "competitors": [],
        }
    )
    return _bundle(brief)


def _block_rows(xlsx_bytes: bytes) -> List[Tuple[str, str]]:
    """(label, value) of every row of the Executive Summary's "Industry
    Benchmarks" block, footnote rows included (they carry text in column B)."""
    ws = load_workbook(io.BytesIO(xlsx_bytes))["Executive Summary"]
    rows: List[Tuple[str, str]] = []
    inside = False
    for r in range(1, ws.max_row + 1):
        label = ws.cell(r, 2).value
        value = ws.cell(r, 4).value
        if isinstance(label, str) and label.startswith("Industry Benchmarks"):
            inside = True
            continue
        if inside:
            if not label:
                break
            rows.append((str(label), str(value or "")))
    return rows


def _deck_slide(pptx_bytes: bytes, title: str):
    prs = Presentation(io.BytesIO(pptx_bytes))
    for slide in prs.slides:
        if title in [s.text_frame.text for s in slide.shapes if s.has_text_frame]:
            return slide
    raise AssertionError(f"no slide titled {title!r}")


def _texts(slide) -> List[str]:
    return [s.text_frame.text for s in slide.shapes if s.has_text_frame and s.text_frame.text.strip()]


# ---------------------------------------------------------------- 1. workbook
def test_workbook_block_prints_only_whitelisted_rows(healthcare):
    rows = _block_rows(healthcare["xlsx_bytes"])
    labels = [lbl for lbl, _ in rows if not lbl.startswith(("Industry ranges", "Model vs."))]
    assert not [lbl for lbl in labels if lbl in _RETIRED_LABELS], labels
    assert all(
        any(lbl.startswith(ok) for ok in _ALLOWED_LABELS) for lbl in labels
    ), labels


def test_workbook_block_values_carry_no_free_text_asides(healthcare):
    by_label = {lbl.split(" (")[0]: v for lbl, v in _block_rows(healthcare["xlsx_bytes"])}
    cpa, cpc, apply_rate = by_label["CPA"], by_label["CPC"], by_label["Apply Rate"]
    for text in (cpa, cpc, apply_rate):
        assert "Notes" not in text and "Hard To Fill" not in text, text
    assert "Median" in cpa and "Range" in cpa, cpa
    assert "Search CPC" not in cpc and "Social CPC" not in cpc and "Easy To Fill" not in cpc, cpc
    assert "Yoy Change" not in apply_rate and "2024" not in apply_rate, apply_rate
    # the deck's CPC row prints the same YoY trend, so the workbook still quotes it
    assert "Trend Yoy" in cpc, cpc


def test_workbook_prints_one_shared_footnote_under_the_block(healthcare):
    rows = _block_rows(healthcare["xlsx_bytes"])
    assert not [lbl for lbl, _ in rows if lbl == "Provenance"], rows
    assert _SECTION_FOOTNOTE, "plan_trust.BENCHMARK_FOOTNOTE is missing"
    notes = [lbl for lbl, _ in rows if lbl == _SECTION_FOOTNOTE]
    assert len(notes) == 1, rows
    # directly under the benchmark rows, before the model-vs-benchmark note
    assert rows.index((_SECTION_FOOTNOTE, "")) >= 1
    first_note = next(i for i, (lbl, _) in enumerate(rows) if lbl.startswith(("Industry ranges", "Model vs.")))
    assert rows[first_note][0] == _SECTION_FOOTNOTE, rows


def test_telecom_cph_row_is_all_in_never_media_only(telecom):
    rows = dict(_block_rows(telecom["xlsx_bytes"]))
    cph = rows["Industry Cost-per-Hire"]
    assert "800" not in cph and "1,500" not in cph, cph
    assert "5,000" in cph, cph
    deck = _deck_slide(telecom["pptx_bytes"], "Channel Strategy & Investment")
    deck_text = " | ".join(_texts(deck))
    assert "$800-$1,500" not in deck_text, deck_text


def test_write_kv_row_sizes_a_wrapped_merged_value():
    ws = Workbook().active
    for col, width in zip("ABCDEFGHI", (3, 14, 14, 18, 18, 18, 18, 18, 3)):
        ws.column_dimensions[col].width = width
    excel_v2._write_kv_row(ws, 1, "Short", "one line")
    excel_v2._write_kv_row(ws, 2, "Long", "x " * 150)
    assert ws.row_dimensions[1].height is None
    # 300 chars over a ~90-unit merge is 4 lines: the row must show them all
    assert ws.row_dimensions[2].height is not None and ws.row_dimensions[2].height >= 40, (
        ws.row_dimensions[2].height
    )


# ------------------------------------------------- footnote / marker contract
def test_footnotes_clear_the_us_only_marker_gate():
    assert _SECTION_FOOTNOTE, "plan_trust.BENCHMARK_FOOTNOTE is missing"
    assert not us_only_markers.has_us_only_marker(_SECTION_FOOTNOTE)
    assert not us_only_markers.has_us_only_marker(plan_trust.INDUSTRY_REFERENCE_LABEL)
    # wording the brief fixed: names the estimate basis, no invented source
    assert "Nova working estimates" in _SECTION_FOOTNOTE
    assert "not independently audited" in _SECTION_FOOTNOTE


def test_non_us_plan_prints_the_footnote_and_passes_the_us_marker_gate(uk_plan):
    assert _SECTION_FOOTNOTE, "plan_trust.BENCHMARK_FOOTNOTE is missing"
    assert any(lbl == _SECTION_FOOTNOTE for lbl, _ in _block_rows(uk_plan["xlsx_bytes"]))
    deck = _deck_slide(uk_plan["pptx_bytes"], "Channel Strategy & Investment")
    assert _SECTION_FOOTNOTE in _texts(deck)
    findings = bundle_qa.run_bundle_qa(
        uk_plan["pptx_bytes"], uk_plan["xlsx_bytes"], uk_plan["data"]
    )
    assert [f for f in findings if f.get("code") == "us_data_on_non_us_plan"] == []


# ------------------------------------------------------------------- 2. deck
@pytest.mark.parametrize("fixture", ["healthcare", "telecom"])
def test_benchmark_slide_source_line_is_the_shared_footnote(fixture, request):
    bundle = request.getfixturevalue(fixture)
    slide = _deck_slide(bundle["pptx_bytes"], "Channel Strategy & Investment")
    texts = _texts(slide)
    assert not [t for t in texts if "Joveo Recruitment Benchmarks KB" in t], texts
    assert _SECTION_FOOTNOTE, "plan_trust.BENCHMARK_FOOTNOTE is missing"
    note = next((s for s in slide.shapes if s.has_text_frame and s.text_frame.text == _SECTION_FOOTNOTE), None)
    assert note is not None, texts
    # the table kept its Apply Rate row and the note clears the attribution heading
    assert "Apply Rate" in texts, texts
    heading = next(s for s in slide.shapes if s.has_text_frame and s.text_frame.text == "CHANNEL CATEGORY ATTRIBUTION")
    assert (note.top + note.height) / 914400 <= heading.top / 914400 + 0.02, (
        (note.top + note.height) / 914400,
        heading.top / 914400,
    )


def test_competitive_landscape_fallback_rows_get_the_shared_footnote():
    fallback = {"hiring_trends": {"avg_time_to_fill": {"average_days": 49}, "offer_acceptance_rate": "78-85%"}}
    line = ppt_generator._competitive_landscape_sources_line(fallback)
    assert "curated knowledge base" not in line, line
    assert _SECTION_FOOTNOTE, "plan_trust.BENCHMARK_FOOTNOTE is missing"
    assert line == _SECTION_FOOTNOTE, line
    # a synthesized profile with real sources keeps naming them
    real = {
        "company_profile": {"wikipedia_url": "https://en.wikipedia.org/wiki/X"},
        "hiring_trends": {"kb_context": "x"},
    }
    assert "Wikipedia" in ppt_generator._competitive_landscape_sources_line(real)


# ------------------------------------------------------- 3. media-only never
_MEDIA_ONLY_ROW = {"cph": {"recruitment_marketing_only": "$800-$1,500", "notes": "media spend"}}


def _kb_with(row: Dict[str, Any]) -> Dict[str, Any]:
    return {"recruitment_benchmarks": {"industry_benchmarks": {"x_industry": row}}}


def test_kb_cph_selector_never_returns_the_media_only_figure():
    assert budget_engine.kb_cph_node_text(_MEDIA_ONLY_ROW["cph"]) == ""
    both = {"recruitment_marketing_only": "$800-$1,500", "total_cost_per_hire": "$5,000-$8,000"}
    assert budget_engine.kb_cph_node_text(both) == "$5,000-$8,000"


def test_engine_kb_cph_row_is_none_for_a_media_only_node():
    assert budget_engine._kb_industry_cph_row(_kb_with(_MEDIA_ONLY_ROW), "x_industry") is None


def test_deck_kb_extract_range_never_returns_the_media_only_figure():
    assert ppt_generator._kb_extract_range({"recruitment_marketing_only": "$800-$1,500"}) == ""
    bm = ppt_generator._kb_recruitment_industry_benchmark(
        "x_industry",
        {"_knowledge_base": _kb_with({**_MEDIA_ONLY_ROW, "cpa": {"range": "$20-$55"}})},
    )
    assert bm is not None and bm["cph"] == "", bm
    assert bm["cpa"] == "$20-$55"


# ------------------------------------------------- 5. comparison panel label
def test_comparison_slide_labels_its_industry_column_a_nova_estimate(healthcare):
    slide = _deck_slide(healthcare["pptx_bytes"], "Plan Comparison & Implementation")
    texts = _texts(slide)
    assert not [t for t in texts if t.rstrip().endswith("Average")], texts
    assert not [t for t in texts if "industry averages" in t], texts
    assert any(plan_trust.INDUSTRY_REFERENCE_LABEL in t for t in texts if t.lstrip("○ ").startswith(plan_trust.INDUSTRY_REFERENCE_LABEL)), texts


# ---------------------------------------- 6. the Nova table is not a benchmark
def _alloc(info: Dict[str, Any]) -> Dict[str, Any]:
    return {"metadata": {"industry_cph": info}}


def test_methodology_and_niche_wording_name_the_nova_table(healthcare):
    nova_table = _alloc({"basis": "us_benchmark", "industry_matched": True})
    methodology = excel_v2._roi_methodology_note(nova_table)
    assert "assumed all-in industry cost per hire (Nova table, unsourced)" in methodology, methodology
    assert "benchmark" not in methodology.lower(), methodology
    local = _alloc(
        {"basis": "local_kb", "source_names": ["Eurojob-Consulting"], "value_label": "midpoint of cited range"}
    )
    assert "Eurojob-Consulting" in excel_v2._roi_methodology_note(local)
    assert "benchmark" not in excel_v2._roi_methodology_note(local).lower()

    wb = load_workbook(io.BytesIO(healthcare["xlsx_bytes"]))
    cells = [
        str(c.value)
        for ws in wb.worksheets
        for row in ws.iter_rows()
        for c in row
        if isinstance(c.value, str)
    ]
    assert not [t for t in cells if "anchored to industry cost-per-hire" in t], "ROI methodology"
    assert not [t for t in cells if "anchors total hires to cost-per-hire benchmarks" in t], "niche purpose"


def test_funnel_assumption_note_does_not_call_the_table_a_benchmark():
    channels = {
        "programmatic_dsp": {"projected_applications": 8565, "projected_hires": 18, "category": "programmatic"},
        "niche_boards": {"projected_applications": 1461, "projected_hires": 6, "category": "niche_board"},
    }
    note = budget_engine.compute_funnel_stages(channels, 24)["assumption_note"]
    assert "cost-per-hire benchmarks" not in note, note
    assert "assumed all-in industry cost per hire" in note, note


def test_workbook_cph_readers_never_take_the_media_only_figure():
    kb = _kb_with(_MEDIA_ONLY_ROW)
    assert excel_v2._kb_industry_cph_benchmark("x_industry", kb=kb) == 0.0
    both = _kb_with({"cph": {"recruitment_marketing_only": "$800", "total_cost_per_hire": "$5,000-$8,000"}})
    assert excel_v2._kb_industry_cph_benchmark("x_industry", kb=both) == 6500.0
    facts = excel_v2._build_narrative_facts_block(
        {"ind_bench": _MEDIA_ONLY_ROW, "client_name": "X", "budget_num": 0}
    )
    assert "Industry Cost/Hire" not in facts, facts

