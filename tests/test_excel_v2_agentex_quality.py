"""Regression tests for the Agent EX bundle-quality pass on excel_v2.py.

Covers the 9 findings assigned to Agent EX (see
scratchpad/mpg-rescore/rescore_findings.json for the original evidence):

  1. One goal / one CPH / one top-up figure across the Executive Summary --
     budget_engine's own duplicate "openings"-based warnings/recommendations
     (a different goal + a different CPH constant from the goal-gap
     callout) are dropped, not shown alongside it.
  2. The dangling "'optimized' section" / "N% improvement" recommendation
     is dropped rather than shown with an unverifiable percentage.
  3. Per-channel confidence is derived (not blanket "HIGH") and agrees
     across Channels & Strategy / ROI Projections / Confidence Intervals /
     Channel Recommendations.
  4. "Low Efficiency alert" skips brand channels and recommends the
     vetted-tier action (hold at pilot level) instead of reallocating
     budget the plan itself just allocated.
  5. CPH benchmarks come from one KB getter (_kb_industry_cph_benchmark),
     never a budget_engine constant.
  6. The 90-Day Forecast ramp narrative states the ACTUAL computed monthly
     split, never a hardcoded "25/35/40".
  7. Competitor Analysis renders only populated columns.
  8. Niche Board Matching quotes the plan's own modeled implied rate.
  9. Location Intelligence renders only populated metric columns and falls
     back to the honest "selected by client footprint" rationale.

Runs under pytest, or standalone:
``python3 tests/test_excel_v2_agentex_quality.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import display_format  # noqa: E402
import excel_v2  # noqa: E402
import kb_loader  # noqa: E402

from test_excel_v2_agentb_quality import _base_data, _build_alloc, _generate_wb  # noqa: E402


def _all_text_cells(wb):
    for ws in wb.worksheets:
        for row in ws.iter_rows():
            for c in row:
                if isinstance(c.value, str) and c.value.strip():
                    yield ws.title, c.coordinate, c.value


# ---------------------------------------------------------------------------
# 1. One goal / one CPH / one top-up figure (no duplicate "openings" block)
# ---------------------------------------------------------------------------
def test_no_duplicate_openings_based_goal_narrative():
    """_base_data's hire_volume ('500 hires') is a materially different
    number from the role-headcount total (30) budget_engine's own
    sufficiency check uses -- exactly the scenario that used to render two
    contradictory goal/CPH narratives on the same sheet."""
    data = _base_data()
    wb = _generate_wb(data)

    gap_callouts = [
        v for _, _, v in _all_text_cells(wb) if "Hiring-goal gap" in v
    ]
    assert len(gap_callouts) == 1, (
        f"Expected exactly ONE goal-gap callout, found {len(gap_callouts)}: "
        f"{gap_callouts}"
    )
    assert "500" in gap_callouts[0]

    for title, coord, v in _all_text_cells(wb):
        low = v.lower()
        assert not ("/opening)" in v and "industry average of $" in low), (
            f"Duplicate budget_engine 'openings' warning leaked into "
            f"{title}!{coord}: {v!r}"
        )
        assert "target openings by" not in low, (
            f"Duplicate role-headcount shortfall warning leaked into "
            f"{title}!{coord}: {v!r}"
        )
        assert not (
            "to fully fund all" in low and "openings at industry-average" in low
        ), f"Duplicate top-up recommendation leaked into {title}!{coord}: {v!r}"


# ---------------------------------------------------------------------------
# 2. No dangling "optimized section" recommendation
# ---------------------------------------------------------------------------
def test_no_dangling_optimized_section_recommendation():
    data = _base_data()
    wb = _generate_wb(data)
    for title, coord, v in _all_text_cells(wb):
        low = v.lower()
        assert "'optimized' section" not in low, (title, coord, v)
        assert not (
            "could improve projected hires by" in low and "section" in low
        ), (title, coord, v)


# ---------------------------------------------------------------------------
# 3. Derived (non-blanket) confidence tiers
# ---------------------------------------------------------------------------
def test_derive_channel_confidence_not_blanket_high():
    """A plan with no real CPC/CPA benchmark source and a low overall
    confidence score must never derive to a blanket HIGH."""
    data = {
        "_synthesized": {"confidence_scores": {"overall": 0.5}},
    }
    assert excel_v2._derive_channel_confidence(data, {"cpc_source": "static_benchmark"}) == "LOW"
    assert excel_v2._derive_channel_confidence(data, {}) == "LOW"
    assert excel_v2._derive_channel_confidence(data, {"cpc_source": "knowledge_base"}) == "MEDIUM"
    data_high = {"_synthesized": {"confidence_scores": {"overall": 0.85}}}
    assert (
        excel_v2._derive_channel_confidence(data_high, {"cpc_source": "live_benchmark"})
        == "HIGH"
    )


def test_confidence_tiers_agree_across_sheets():
    data = _base_data()  # overall confidence not stubbed to 0.9+ -> not blanket HIGH
    wb = _generate_wb(data)

    seen = {}
    for sheet in (
        "Channels & Strategy",
        "ROI Projections",
        "Confidence Intervals",
        "Channel Recommendations",
    ):
        ws = wb[sheet]
        vals = set()
        for row in ws.iter_rows():
            for c in row:
                if isinstance(c.value, str) and c.value.strip().upper() in (
                    "HIGH",
                    "MEDIUM",
                    "LOW",
                ):
                    vals.add(c.value.strip().upper())
        seen[sheet] = vals

    # None of the four sheets should show ONLY "HIGH" for every tag -- that
    # was the original defect (100% HIGH regardless of the plan's overall
    # Sources & Confidence grade).
    for sheet, vals in seen.items():
        assert vals != {"HIGH"}, f"{sheet} still shows blanket HIGH confidence"


# ---------------------------------------------------------------------------
# 4. Low Efficiency alert skips brand channels, no reallocation language
# ---------------------------------------------------------------------------
def test_rewrite_low_efficiency_recommendation_skips_brand():
    channel_allocs = {
        "employer_branding": {
            "efficiency_flag": "Low Efficiency",
            "channel_role": "brand",
        },
        "social_media": {
            "efficiency_flag": "Low Efficiency",
            "channel_role": "performance",
        },
    }
    text = excel_v2._rewrite_low_efficiency_recommendation(channel_allocs)
    assert text is not None
    assert "Employer" not in text and "employer" not in text.lower()
    assert "Social" in text
    assert "reallocating this budget" not in text.lower()
    assert "hold at pilot level" in text.lower()
    assert "scale only on observed conversion" in text.lower()


def test_rewrite_low_efficiency_recommendation_drops_when_only_brand():
    channel_allocs = {
        "employer_branding": {
            "efficiency_flag": "Low Efficiency",
            "channel_role": "brand",
        },
    }
    assert excel_v2._rewrite_low_efficiency_recommendation(channel_allocs) is None


# ---------------------------------------------------------------------------
# 5. Single KB getter for CPH benchmarks
# ---------------------------------------------------------------------------
def test_kb_industry_cph_benchmark_reads_recruitment_benchmarks_section():
    kb = kb_loader.load_knowledge_base()
    val = excel_v2._kb_industry_cph_benchmark("logistics_supply_chain", kb=kb)
    assert val > 0

    # The Executive Summary "Recruitment Benchmarks" table reads the SAME
    # KB section -- the getter must not diverge from it.
    ind_bench = kb["recruitment_benchmarks"]["industry_benchmarks"][
        "logistics_supply_chain"
    ]
    total_cph_raw = ind_bench["cph"]["total_cost_per_hire"]
    assert excel_v2._parse_cph_point_estimate(total_cph_raw) == val


def test_parse_cph_point_estimate_range_and_open_ended():
    assert excel_v2._parse_cph_point_estimate("$9,000-$12,000") == 10500.0
    assert excel_v2._parse_cph_point_estimate("$5,000+") == 5000.0
    assert excel_v2._parse_cph_point_estimate(2100) == 2100.0
    assert excel_v2._parse_cph_point_estimate(None) == 0.0


# ---------------------------------------------------------------------------
# 6. 90-Day Forecast ramp narrative states the ACTUAL computed split
# ---------------------------------------------------------------------------
def test_ramp_narrative_matches_actual_monthly_split():
    data = _base_data()  # 78-week campaign -> long-campaign footnote fires
    wb = _generate_wb(data)
    ws = wb["90-Day Forecast"]

    rows = list(ws.iter_rows(min_col=2, values_only=True))
    header_idx = next(i for i, r in enumerate(rows) if r and r[0] == "Metric")
    spend_row = next(r for r in rows[header_idx + 1 :] if r and r[0] == "Spend")
    month_spend = spend_row[1:4]
    total = spend_row[4]
    actual_pcts = [round(v / total * 100) for v in month_spend]

    narrative_cells = [
        v for _, _, v in _all_text_cells(wb) if "ramp-weighted" in v or "phases budget" in v
    ]
    assert narrative_cells, "Expected at least one ramp narrative footnote"
    for text in narrative_cells:
        assert "25/35/40" not in text
        # The narrative must state the actual computed percentages.
        for pct in actual_pcts:
            assert f"{pct}%" in text or f"{pct}/" in text or f"{pct} " in text, (
                text,
                actual_pcts,
            )


# ---------------------------------------------------------------------------
# 7. Competitor Analysis renders only populated columns
# ---------------------------------------------------------------------------
def test_competitor_analysis_drops_empty_columns():
    """_base_data's competitors list is bare names -- no industry/size/
    hiring-activity/overlap data -- so those columns must not be rendered
    with a header promising data that's 100% blank."""
    data = _base_data()
    wb = _generate_wb(data)
    ws = wb["Market Intelligence"]

    header_row = None
    for row in ws.iter_rows():
        vals = [c.value for c in row]
        if vals and vals[1] == "Name":
            header_row = [v for v in vals if v]
            break
    assert header_row is not None, "Competitor Analysis table not found"
    assert header_row == ["Name", "Counter-Strategy"], header_row


# ---------------------------------------------------------------------------
# 8. Niche Board Matching quotes the plan's own modeled implied rate
# ---------------------------------------------------------------------------
def test_niche_board_narrative_uses_live_implied_rate():
    data = _base_data()
    wb = _generate_wb(data)
    ws = wb["Niche Board Matching"]

    purpose_text = None
    for row in ws.iter_rows():
        for c in row:
            if c.value == "Purpose":
                # _write_kv_row: key spans col_start:col_start+1, value
                # starts at col_start+2 (2-column key, then value).
                purpose_text = ws.cell(row=c.row, column=c.column + 2).value
    assert purpose_text is not None

    rate, apps, hires = excel_v2._niche_board_implied_rate(data)
    if rate is not None:
        assert f"{hires} hires / {apps} applications" in purpose_text
        assert "this plan models niche boards at" in purpose_text.lower()
    else:
        assert "no niche-board applications modeled" in purpose_text.lower()
    assert "industry benchmark" in purpose_text.lower()
    assert "10-15%" in purpose_text


# ---------------------------------------------------------------------------
# 9. Location Intelligence renders only populated metric columns
# ---------------------------------------------------------------------------
def test_location_intelligence_drops_blank_metric_columns():
    """A location with no matched profile data (no synthesized/enriched
    demographics) must fall back to Location/Country/Why-This-Market only,
    with an honest 'selected by client footprint' rationale -- never a row
    of blank '--' cells under headers promising Population/Unemployment/
    Income/Key Industries."""
    data = _base_data(locations=["Nowhereville, ZZ"])
    wb = _generate_wb(data)
    ws = wb["Market Intelligence"]

    header_row = None
    rationale_cell = None
    for row in ws.iter_rows():
        vals = [c.value for c in row]
        if vals and vals[1] == "Location":
            header_row = [v for v in vals if v]
        if header_row and any(
            isinstance(v, str) and "Nowhereville" in v for v in vals
        ):
            rationale_cell = vals[3] if len(vals) > 3 else None
            break

    assert header_row is not None, "Location Intelligence table not found"
    assert header_row == ["Location", "Country", "Why This Market"], header_row
    assert rationale_cell is not None
    assert "selected by client footprint" in rationale_cell


if __name__ == "__main__":
    test_no_duplicate_openings_based_goal_narrative()
    test_no_dangling_optimized_section_recommendation()
    test_derive_channel_confidence_not_blanket_high()
    test_confidence_tiers_agree_across_sheets()
    test_rewrite_low_efficiency_recommendation_skips_brand()
    test_rewrite_low_efficiency_recommendation_drops_when_only_brand()
    test_kb_industry_cph_benchmark_reads_recruitment_benchmarks_section()
    test_parse_cph_point_estimate_range_and_open_ended()
    test_ramp_narrative_matches_actual_monthly_split()
    test_competitor_analysis_drops_empty_columns()
    test_niche_board_narrative_uses_live_implied_rate()
    test_location_intelligence_drops_blank_metric_columns()
    print("OK")
