"""Regression gate: regenerate the two reference bundles (manpower/logistics
and atria/healthcare -- the ones the July bundle audits used) and assert
``bundle_qa.run_bundle_qa`` reports ZERO critical findings.

This is the test that turns "a human reviewer eventually caught it" into
"CI catches it" -- the whole reason bundle_qa.py exists is that this exact
class of defect (snake_case leaking into a cell, a fabricated "beating
benchmark" badge, near-duplicate competitor prose, ...) shipped twice
before a human flagged it.

Also includes focused positive-control unit tests proving each check
actually fires on an injected defect (not just that the regexes exist) --
a linter with zero findings on clean input and zero findings on broken
input is worse than no linter, because it looks like coverage.

Runs under pytest, or standalone:
``python3 tests/test_bundle_qa_regression.py``.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import bundle_qa  # noqa: E402
import tools_regen_bundles as trb  # noqa: E402

_REFERENCE_BRIEFS = {
    "manpower": trb.MANPOWER_BRIEF,
    "atria": trb.ATRIA_BRIEF,
}


def _regen_and_scan(slug: str, tmp_path: Path) -> tuple[dict, list[dict]]:
    brief = _REFERENCE_BRIEFS[slug]
    result = trb.generate_bundle(brief, tmp_path, slug)
    assert not result["errors"], f"{slug} bundle failed to generate: {result['errors']}"
    findings = bundle_qa.run_bundle_qa(
        result["pptx_bytes"], result["xlsx_bytes"], result["data"]
    )
    return result, findings


def test_manpower_bundle_has_zero_critical_findings(tmp_path):
    _, findings = _regen_and_scan("manpower", tmp_path)
    critical = [f for f in findings if f["severity"] == "critical"]
    assert not critical, (
        "manpower/logistics reference bundle has critical bundle_qa "
        f"findings: {critical}"
    )


def test_atria_bundle_has_zero_critical_findings(tmp_path):
    _, findings = _regen_and_scan("atria", tmp_path)
    critical = [f for f in findings if f["severity"] == "critical"]
    assert not critical, (
        "atria/healthcare reference bundle has critical bundle_qa "
        f"findings: {critical}"
    )


def test_findings_have_the_required_shape(tmp_path):
    """Every finding -- on either bundle -- carries the documented API
    shape, regardless of severity (structural contract, not a content
    check)."""
    for slug in _REFERENCE_BRIEFS:
        _, findings = _regen_and_scan(slug, tmp_path)
        for f in findings:
            assert f["severity"] in ("critical", "warn")
            assert isinstance(f["code"], str) and f["code"]
            assert isinstance(f["message"], str) and f["message"]
            assert isinstance(f["location"], str)


# ---------------------------------------------------------------------------
# Positive controls: prove each check actually fires
# ---------------------------------------------------------------------------
def test_detects_snake_case_leak():
    units = [bundle_qa._TextUnit("Rates: warehouse_hourly $12-$18", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    assert any(f["code"] == "snake_case_leak" for f in findings)


def test_detects_multi_underscore_snake_case_leak():
    """The literal spec regex r'\\b[a-z0-9]+_[a-z0-9]+\\b' cannot match a
    token with 2+ underscores (no internal \\b for a fixed two-group
    pattern to anchor on) -- bundle_qa uses a repeating-group regex instead
    so a real KB key like "cdl_drivers_hourly" is still caught."""
    units = [bundle_qa._TextUnit("cdl_drivers_hourly: $25-$50", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    assert any(f["code"] == "snake_case_leak" for f in findings)


def test_ignores_snake_case_inside_urls_and_emails():
    units = [
        bundle_qa._TextUnit("See our_data at https://example.com/api_docs", "X!A1"),
        bundle_qa._TextUnit("Contact hiring_team@example.com for details", "X!A2"),
    ]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    assert not any(f["code"] == "snake_case_leak" for f in findings)


def test_detects_pluralization_artifact():
    units = [bundle_qa._TextUnit("5 role(s) selected", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    assert any(f["code"] == "pluralization_artifact" for f in findings)


def test_detects_raw_float_precision():
    units = [bundle_qa._TextUnit("CPA is $12.345678", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    assert any(f["code"] == "raw_float_precision" for f in findings)


def test_detects_dollar_point_zero_k():
    units = [bundle_qa._TextUnit("Budget: $150.0K", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    assert any(f["code"] == "dollar_point_zero_k" for f in findings)


def test_detects_over_ongoing_grammar():
    """prod-Atria defect: 'Finalize weekly budget (25000 over Ongoing) and
    success metrics' -- an unbounded duration spliced verbatim into the
    '<budget> over <duration>' template, plus the raw unformatted budget
    digits that shipped alongside it. Both artifact guards must fire."""
    units = [
        bundle_qa._TextUnit(
            "Finalize weekly budget (25000 over Ongoing) and success metrics",
            "X!A1",
        )
    ]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    codes = {f["code"] for f in findings}
    assert "over_ongoing_grammar" in codes
    assert "raw_number_before_over" in codes


def test_allows_fixed_ongoing_phrasing_and_formatted_money():
    """The corrected phrasing this codebase now emits ('$25K, ongoing' /
    '$25K on an ongoing basis' / '$25K over 6 months') must NOT
    false-positive on either new check."""
    units = [
        bundle_qa._TextUnit(
            "Finalize weekly budget ($25K, ongoing) and success metrics",
            "X!A1",
        ),
        bundle_qa._TextUnit(
            "Launch within 2 business weeks of feed integration — $25K "
            "on an ongoing basis",
            "X!A2",
        ),
        bundle_qa._TextUnit(
            "Finalize weekly budget ($25K over 6 months) and success metrics",
            "X!A3",
        ),
    ]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    codes = {f["code"] for f in findings}
    assert "over_ongoing_grammar" not in codes
    assert "raw_number_before_over" not in codes


def test_raw_number_before_over_ignores_out_of_range_digit_runs():
    """The 4-7 digit window targets an unformatted BUDGET specifically -- a
    short 2-digit rank or an 8+ digit reference number before 'over' is a
    different shape and must not be flagged."""
    units = [
        bundle_qa._TextUnit("Rank 12 over 500 applicants", "X!A1"),
        bundle_qa._TextUnit("Reference 123456789 over quota", "X!A2"),
    ]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    assert not any(f["code"] == "raw_number_before_over" for f in findings)


def test_detects_location_advisory_leak_unconditionally():
    units = [bundle_qa._TextUnit("Location advisory: verify targeting", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {}, findings)
    assert any(f["code"] == "location_advisory_leak" for f in findings)


def test_detects_non_us_text_on_us_plan():
    units = [bundle_qa._TextUnit("This reflects a non-US market signal", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {"locations": ["Austin, TX"]}, findings)
    assert any(f["code"] == "non_us_text_on_us_plan" for f in findings)


def test_allows_non_us_text_on_non_us_plan():
    units = [bundle_qa._TextUnit("This reflects a non-US market signal", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {"locations": ["London, UK"]}, findings)
    assert not any(f["code"] == "non_us_text_on_us_plan" for f in findings)


def test_detects_ai_training_vocab_on_non_ai_training_plan():
    units = [bundle_qa._TextUnit("Our AI Trainer network is ready", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(
        units, {"industry": "healthcare_medical", "roles": ["Registered Nurse"]}, findings
    )
    assert any(f["code"] == "ai_training_vocab_leak" for f in findings)


def test_allows_ai_training_vocab_on_ai_training_plan():
    units = [bundle_qa._TextUnit("Our AI Trainer network is ready", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(
        units, {"industry": "ai_training", "roles": ["AI Trainer"]}, findings
    )
    assert not any(f["code"] == "ai_training_vocab_leak" for f in findings)


def test_detects_duration_label_drifted_from_campaign_weeks():
    units = [bundle_qa._TextUnit("Duration: 6 months (~30 weeks)", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {"campaign_weeks": 24}, findings)
    assert any(
        f["code"] == "duration_label_drifted_from_campaign_weeks" for f in findings
    )


def test_allows_bucketed_duration_label_matching_campaign_weeks():
    """A "6 months (~24 weeks)" label is a legitimate fixed marketing
    bucket (4 weeks/month), NOT a 52/12 conversion -- it must not be
    flagged as long as it agrees with the bundle's own campaign_weeks."""
    units = [bundle_qa._TextUnit("Duration: 6 months (~24 weeks)", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_text_patterns(units, {"campaign_weeks": 24}, findings)
    assert not any(
        f["code"].startswith("duration_") for f in findings
    ), findings


def test_detects_client_name_wrong_casing():
    units = [bundle_qa._TextUnit("Plan for atria Senior living", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_client_name_casing(
        units, {"client_name": "atria Senior living"}, findings
    )
    assert any(f["code"] == "client_name_wrong_casing" for f in findings)


def test_allows_canonical_client_name_casing():
    units = [bundle_qa._TextUnit("Plan for Atria Senior Living", "X!A1")]
    findings: list[dict] = []
    bundle_qa._check_client_name_casing(
        units, {"client_name": "atria Senior living"}, findings
    )
    assert not any(f["code"] == "client_name_wrong_casing" for f in findings)


def test_detects_near_duplicate_counter_strategy():
    """Two DIFFERENT competitors, same table/view, near-identical text --
    must flag."""
    findings: list[dict] = []
    bundle_qa._check_counter_strategy_distinctness(
        [
            (
                "UPS's presence in this pool means CDL drivers have "
                "options -- lead with total-comp clarity and a same-week "
                "interview slot.",
                "a",
                "UPS",
                "Market Intelligence#10",
            ),
            (
                "FedEx's presence in this pool means CDL drivers have "
                "options -- lead with total-comp clarity and a same-week "
                "interview slot.",
                "b",
                "FedEx",
                "Market Intelligence#10",
            ),
        ],
        findings,
    )
    assert any(f["code"] == "counter_strategy_near_duplicate" for f in findings)


def test_allows_distinct_counter_strategy():
    findings: list[dict] = []
    bundle_qa._check_counter_strategy_distinctness(
        [
            (
                "UPS is actively staffing CDL drivers through third-party "
                "agencies -- compress time-to-offer.",
                "a",
                "UPS",
                "Market Intelligence#10",
            ),
            (
                "Expect FedEx to keep pressure on CDL drivers; a faster "
                "interview-to-offer cycle is the clearest lever.",
                "b",
                "FedEx",
                "Market Intelligence#10",
            ),
        ],
        findings,
    )
    assert not any(f["code"] == "counter_strategy_near_duplicate" for f in findings)


def test_allows_same_competitor_near_duplicate_across_different_artifacts():
    """S92 bug #1: the SAME competitor legitimately carries the SAME
    composed sentence across DIFFERENT artifacts (deck card vs Market
    Intelligence row vs Quality Intelligence row) -- that's intentional
    single-sourcing from insight_composer, not competitors reading as
    interchangeable. Cross-view pairs must never be flagged, regardless of
    identity."""
    findings: list[dict] = []
    bundle_qa._check_counter_strategy_distinctness(
        [
            (
                "ManpowerGroup is actively staffing CDL drivers through "
                "third-party agencies -- compress time-to-offer.",
                "deck loc",
                "ManpowerGroup",
                "deck",
            ),
            (
                "ManpowerGroup is actively staffing CDL drivers through "
                "third-party agencies -- compress time-to-offer.",
                "Market Intelligence!F5",
                "ManpowerGroup",
                "Market Intelligence#10",
            ),
        ],
        findings,
    )
    assert not any(f["code"] == "counter_strategy_near_duplicate" for f in findings)


def test_allows_same_competitor_near_duplicate_within_same_table():
    """Same view, SAME competitor identity repeating (e.g. the same top
    employer leads two different cities) -- expected, not a defect."""
    findings: list[dict] = []
    bundle_qa._check_counter_strategy_distinctness(
        [
            (
                "ManpowerGroup is actively staffing CDL drivers through "
                "third-party agencies -- compress time-to-offer.",
                "Quality Intelligence!F5",
                "ManpowerGroup",
                "Quality Intelligence#4",
            ),
            (
                "ManpowerGroup is actively staffing CDL drivers through "
                "third-party agencies -- compress time-to-offer.",
                "Quality Intelligence!F6",
                "ManpowerGroup",
                "Quality Intelligence#4",
            ),
        ],
        findings,
    )
    assert not any(f["code"] == "counter_strategy_near_duplicate" for f in findings)


def test_skips_pair_when_identity_not_collectable():
    """If identity can't be established for a pair, skip it rather than
    flag it -- absence of evidence isn't evidence of a defect."""
    findings: list[dict] = []
    bundle_qa._check_counter_strategy_distinctness(
        [
            (
                "UPS's presence in this pool means CDL drivers have "
                "options -- lead with total-comp clarity and a same-week "
                "interview slot.",
                "a",
                None,
                "deck",
            ),
            (
                "FedEx's presence in this pool means CDL drivers have "
                "options -- lead with total-comp clarity and a same-week "
                "interview slot.",
                "b",
                None,
                "deck",
            ),
        ],
        findings,
    )
    assert not any(f["code"] == "counter_strategy_near_duplicate" for f in findings)


def test_collects_xlsx_counter_strategies_bounded_to_table():
    """S92 bug #2: _collect_xlsx_counter_strategies must stop at the table
    boundary, not sweep into an unrelated table below that happens to
    reuse the same column letter -- e.g. Quality Intelligence's "Role
    Difficulty Classification" table puts "Location Modifier" in the same
    column F that "Competitive Landscape & Counter-Strategies" used for
    "Counter-Strategy". No blank row separates the two tables here on
    purpose, so this only passes if the header-style-based stop (not just
    the blank-row stop) is working."""
    pytest = __import__("pytest")
    pytest.importorskip("openpyxl")
    import openpyxl
    from openpyxl.styles import Font, PatternFill

    header_font = Font(bold=True, color="FFFFFFFF")
    header_fill = PatternFill(
        start_color="FF1F3A93", end_color="FF1F3A93", fill_type="solid"
    )
    body_font = Font(bold=False)
    body_fill = PatternFill(
        start_color="FFFFFFFF", end_color="FFFFFFFF", fill_type="solid"
    )

    def _write_header_row(ws, row_idx, headers):
        for ci, h in enumerate(headers, start=1):
            c = ws.cell(row=row_idx, column=ci, value=h)
            c.font = header_font
            c.fill = header_fill

    def _write_data_row(ws, row_idx, values):
        for ci, v in enumerate(values, start=1):
            c = ws.cell(row=row_idx, column=ci, value=v)
            c.font = body_font
            c.fill = body_fill

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Quality Intelligence"

    # Table 1: Competitive Landscape & Counter-Strategies -- F = Counter-Strategy
    _write_header_row(
        ws,
        1,
        [
            "City",
            "Top Employers",
            "Hiring Intensity",
            "Est. Competing Postings",
            "Why They Matter",
            "Counter-Strategy",
        ],
    )
    _write_data_row(
        ws,
        2,
        [
            "Houston",
            "ManpowerGroup",
            "High",
            500,
            "High hiring volume",
            "ManpowerGroup is actively staffing CDL drivers -- compress time-to-offer.",
        ],
    )

    # Table 2 immediately follows, NO blank row -- F = Location Modifier
    # (same column letter, unrelated field).
    _write_header_row(
        ws,
        3,
        [
            "Role Title",
            "Seniority Level",
            "Difficulty (1-10)",
            "Supply Level",
            "Avg Time-to-Fill",
            "Location Modifier",
            "Budget Weight",
            "Channel Emphasis",
            "Description",
        ],
    )
    _write_data_row(
        ws,
        4,
        [
            "CDL Driver",
            "Mid",
            6,
            "Moderate",
            "45 days",
            "+2.0 (Houston)",
            "1.1x",
            "Job Boards",
            "Standard CDL role",
        ],
    )

    out = bundle_qa._collect_xlsx_counter_strategies(wb)
    texts = [t for t, *_rest in out]
    assert any("ManpowerGroup" in t for t in texts)
    assert not any("Location Modifier" in t or "+2.0" in t for t in texts)
    assert len(out) == 1
    # Identity for the one collected row should resolve to the "Top
    # Employers" column value, not None.
    assert out[0][2] == "ManpowerGroup"


def test_detects_zero_hire_nonzero_cph():
    pytest = __import__("pytest")
    pytest.importorskip("openpyxl")
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Executive Summary"
    ws.append(["Channel", "Proj. Hires", "Cost Per Hire"])
    ws.append(["Niche Boards", 0, 625.0])
    findings: list[dict] = []
    bundle_qa._check_zero_hire_honesty(wb, findings)
    assert any(f["code"] == "zero_hire_nonzero_cph" for f in findings)


def test_allows_zero_hire_with_dash_cph():
    pytest = __import__("pytest")
    pytest.importorskip("openpyxl")
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Executive Summary"
    ws.append(["Channel", "Proj. Hires", "Cost Per Hire"])
    ws.append(["Niche Boards", 0, "—"])
    findings: list[dict] = []
    bundle_qa._check_zero_hire_honesty(wb, findings)
    assert not any(f["code"] == "zero_hire_nonzero_cph" for f in findings)


def test_detects_forecast_footing_mismatch():
    pytest = __import__("pytest")
    pytest.importorskip("openpyxl")
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "90-Day Forecast"
    ws.append([None] * 8)
    ws.append(
        [None, "Metric", "July 2026", "August 2026", "September 2026", "90-Day Total", "Trend", None]
    )
    ws.append([None, "Applications", 100, 200, 300, 999, "Increasing", None])  # 600 != 999
    findings: list[dict] = []
    bundle_qa._check_90_day_forecast_footing(wb, findings)
    assert any(f["code"] == "forecast_footing_mismatch" for f in findings)


def test_allows_correct_forecast_footing():
    pytest = __import__("pytest")
    pytest.importorskip("openpyxl")
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "90-Day Forecast"
    ws.append([None] * 8)
    ws.append(
        [None, "Metric", "July 2026", "August 2026", "September 2026", "90-Day Total", "Trend", None]
    )
    ws.append([None, "Applications", 100, 200, 300, 600, "Increasing", None])
    findings: list[dict] = []
    bundle_qa._check_90_day_forecast_footing(wb, findings)
    assert not any(f["code"] == "forecast_footing_mismatch" for f in findings)


def test_detects_beating_badge_next_to_non_comparable_benchmark():
    units = [
        bundle_qa._TextUnit("1.3x  ▲", "slide 8 / client", slide_idx=8, top=1000, left=100),
        bundle_qa._TextUnit("Varies", "slide 8 / industry", slide_idx=8, top=1000, left=5000),
    ]
    findings: list[dict] = []
    bundle_qa._check_comparison_badges(units, findings)
    assert any(f["code"] == "fabricated_beating_badge" for f in findings)


def test_allows_beating_badge_next_to_real_benchmark():
    units = [
        bundle_qa._TextUnit("1.3x  ▲", "slide 8 / client", slide_idx=8, top=1000, left=100),
        bundle_qa._TextUnit("1.0x", "slide 8 / industry", slide_idx=8, top=1000, left=5000),
    ]
    findings: list[dict] = []
    bundle_qa._check_comparison_badges(units, findings)
    assert not any(f["code"] == "fabricated_beating_badge" for f in findings)


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
