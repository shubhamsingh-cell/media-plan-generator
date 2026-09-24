"""Regression tests for the 7-finding Excel workbook audit (excel_v2.py).

Each finding below reproduces a real client-visible defect against a
generated workbook, then asserts the fix. See the fix-commit message for
the full audit context; per-test docstrings summarize root cause + fix.

Runs under pytest, or standalone:
``python3 tests/test_workbook_quality.py``.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402
import pytest  # noqa: E402

import budget_engine  # noqa: E402
import excel_v2  # noqa: E402
import intl_benchmark_lookup  # noqa: E402


def _build_alloc(total_budget, roles, locations, industry, channels, **kwargs):
    return budget_engine.calculate_budget_allocation(
        total_budget=total_budget,
        roles=roles,
        locations=locations,
        industry=industry,
        channel_percentages=channels,
        **kwargs,
    )


def _plan_data(
    country: str,
    budget: int,
    *,
    plan_currency: str | None = None,
    roles=None,
    channels=None,
    tag: str = "test",
):
    roles = roles or [{"title": "Customer Service Rep", "count": 20, "tier": "entry"}]
    locations = [{"city": "Test City", "state": "", "country": country}]
    channels = channels or {
        "Indeed": 40,
        "LinkedIn": 30,
        "Google Search Ads": 20,
        "Programmatic Job Boards": 10,
    }
    alloc = _build_alloc(
        budget,
        roles,
        locations,
        "general_entry_level",
        channels,
        collar_type="white",
        campaign_start_month=9,
        locations_raw=[f"{l['city']}, {country}" for l in locations],
        plan_currency=plan_currency,
    )
    symbol = {"GBP": "£", "EUR": "€", "INR": "₹"}.get(plan_currency, "$")
    data = {
        "client_name": f"Test Client {tag}",
        "industry": "general_entry_level",
        "budget": f"{symbol}{budget:,}",
        "budget_period": "campaign",
        "campaign_duration": "3 months",
        "campaign_start_month": 9,
        "hire_volume": f"{sum(r['count'] for r in roles)} hires",
        "work_environment": "onsite",
        "roles": [r["title"] for r in roles],
        "target_roles": roles,
        "locations": [f"{l['city']}, {country}" for l in locations],
        "competitors": [],
        "_budget_allocation": alloc,
    }
    if plan_currency:
        data["plan_currency"] = plan_currency
    return data, alloc


def _generate_wb(data: dict):
    raw = excel_v2.generate_excel_v2(dict(data))
    if isinstance(raw, tuple):
        raw = raw[0]
    assert isinstance(raw, (bytes, bytearray)) and len(raw) > 0
    return openpyxl.load_workbook(io.BytesIO(raw), data_only=False)


def _forecast_sheet(wb):
    for name in wb.sheetnames:
        if "Forecast" in name:
            return wb[name]
    raise AssertionError("No Forecast sheet found")


# ---------------------------------------------------------------------------
# Finding #2 / DEFECT C (2026-09-24 correction): the ">US$1,000" threshold
# text used to claim budget_engine's `dollars > 1000` check was a fixed-USD
# planning constant. It is NOT: `dollars` is `total_budget * pct / 100.0`,
# where `total_budget` is the plan's OWN typed figure in the plan's OWN
# currency (an INR plan typed as 5,000,000 is 5,000,000 rupees, never
# converted anywhere in that path) -- so labelling the threshold "US$1,000"
# on a non-USD plan claimed a wildly wrong (e.g. ~83x too large on an INR
# plan) US-dollar spend for a flag that actually tripped on a much smaller
# amount of the plan's own currency. Fix: render the threshold in the
# plan's own currency (``_fmt_currency``, which defaults to the active
# plan-currency symbol), matching how the flag is actually computed.
# budget_engine's `> 1000` threshold value itself is unchanged.
# ---------------------------------------------------------------------------
def test_low_efficiency_alert_uses_plan_currency_threshold():
    # Small goal (2 hires) against a $5,000 budget: every funded channel's
    # per-channel spend (>1,000) rounds to 0 projected hires, which is
    # exactly what trips budget_engine's "Low Efficiency" flag
    # (budget_engine.py: `projected_hires == 0 and dollars > 1000`). Non-USD
    # plan so a mislabeled-currency bug would actually show up.
    #
    # Deliberately a market NOT in the 38-country
    # international_benchmarks_2026.json dataset (e.g. "United Kingdom"
    # resolves real, cheap local UK job-board CPC via
    # intl_benchmark_lookup.get_locale_cpc_basis, which reliably funds an
    # actual hire and never trips this flag) -- this fixture exercises the
    # currency-LABEL path on the pre-existing US-calibrated cascade, not
    # locale calibration (covered separately in
    # test_intl_locale_cpc_calibration.py).
    roles = [{"title": "Warehouse Associate", "count": 2, "tier": "entry"}]
    data, alloc = _plan_data(
        "Egypt",
        5_000,
        plan_currency="GBP",
        roles=roles,
        tag="lowseff",
    )
    ch_allocs = alloc.get("channel_allocations", {})
    assert any(
        isinstance(ch, dict) and ch.get("efficiency_flag") == "Low Efficiency"
        for ch in ch_allocs.values()
    ), "fixture must actually trigger a Low Efficiency channel"

    wb = _generate_wb(data)
    ws = wb["Executive Summary"]
    texts = [c.value for row in ws.iter_rows() for c in row if isinstance(c.value, str)]
    alerts = [t for t in texts if "Low Efficiency alert" in t]
    assert alerts, "expected a Low Efficiency alert recommendation"
    for alert in alerts:
        # The flag is computed on the plan's OWN currency figure -- the
        # threshold text must match, not claim a fixed-USD amount.
        assert "£1,000" in alert, alert
        assert "US$1,000" not in alert, alert


# ---------------------------------------------------------------------------
# Finding #5: Confidence Intervals sheet Cost Per Hire Low/High must be
# derived from the SAME printed variance rate as every other cost-metric
# band (CPA), not from truncated integer hire counts (which desyncs from
# the printed "+/-X%" for small hire counts).
# ---------------------------------------------------------------------------
def test_confidence_intervals_cph_band_matches_printed_variance():
    roles = [{"title": "Warehouse Associate", "count": 10, "tier": "entry"}]
    data, alloc = _plan_data("United States", 20_000, roles=roles, tag="cphvar")
    wb = _generate_wb(data)
    ws = wb["Confidence Intervals"]

    rows = list(ws.iter_rows(min_row=1, values_only=False))
    found = 0
    for i, row in enumerate(rows):
        vals = [c.value for c in row]
        if len(vals) < 7 or vals[2] != "Cost Per Hire":
            continue
        lo, expected, hi = vals[3], vals[4], vals[5]
        variance_str = vals[6]  # e.g. "+/-20%"
        assert isinstance(variance_str, str) and variance_str.startswith("+/-")
        rate = float(variance_str.replace("+/-", "").replace("%", "")) / 100.0
        assert lo == pytest.approx(expected * (1 + rate)), (lo, expected, rate)
        assert hi == pytest.approx(expected * (1 - rate)), (hi, expected, rate)
        assert lo >= expected >= hi, (lo, expected, hi)
        found += 1
    assert found > 0, "expected at least one Cost Per Hire row"


# ---------------------------------------------------------------------------
# Finding #16: Executive Summary "ROI Score" column (last column, J, of the
# channel table) must be visible -- not hidden by the pie-chart helper
# block, which used to park at the generic COL_END+2 (= column J) instead
# of past this table's OWN (wider, 9-column) last column.
# ---------------------------------------------------------------------------
def test_roi_score_column_not_hidden():
    data, _ = _plan_data("United States", 150_000, tag="roivis")
    wb = _generate_wb(data)
    ws = wb["Executive Summary"]

    header_row = None
    roi_col = None
    for row in ws.iter_rows(min_row=1, max_row=30):
        for c in row:
            if c.value == "ROI Score":
                header_row = c.row
                roi_col = c.column
        if roi_col:
            break
    assert roi_col is not None, "ROI Score header not found"

    from openpyxl.utils import get_column_letter

    col_letter = get_column_letter(roi_col)
    dim = ws.column_dimensions.get(col_letter)
    assert (
        dim is None or not dim.hidden
    ), f"ROI Score column {col_letter} is hidden (dim.hidden={getattr(dim, 'hidden', None)})"
    # The cell directly below the header must carry a real numeric ROI value.
    data_cell = ws.cell(row=header_row + 1, column=roi_col)
    assert isinstance(data_cell.value, (int, float)), data_cell.value


# ---------------------------------------------------------------------------
# Finding #18: Forecast sheet Trend column must be derived from the row's
# own first-vs-last period values, not a hardcoded "Increasing"/"Decreasing".
# ---------------------------------------------------------------------------
def test_forecast_trend_derived_not_hardcoded():
    from openpyxl.styles import Font

    # A genuinely FALLING CPA / RISING applications & hires series (a
    # legitimate improving campaign): the old hardcoded logic ALSO said
    # "Increasing"/"Decreasing" here so this alone doesn't distinguish --
    # use the helper directly for a case the hardcoded version got wrong.
    trend, font = excel_v2._derive_forecast_trend([30, 20, 10], True, True)
    assert trend == "Decreasing", trend  # CPA falling
    assert font.color.rgb.endswith(excel_v2.GREEN), "falling CPA should be green (good)"

    trend, font = excel_v2._derive_forecast_trend([10, 20, 30], True, True)
    assert trend == "Increasing", trend  # CPA rising
    assert font.color.rgb.endswith(excel_v2.RED), "rising CPA should be red (bad)"

    trend, font = excel_v2._derive_forecast_trend([10, 20, 30], False, True)
    assert trend == "Increasing", trend  # hires/apps rising
    assert font.color.rgb.endswith(
        excel_v2.GREEN
    ), "rising hires should be green (good)"

    trend, font = excel_v2._derive_forecast_trend([100, 101, 102], False, True)
    assert trend == "Stable", trend  # <5% change

    trend, font = excel_v2._derive_forecast_trend([10], False, True)
    assert trend == "—", trend  # single period, no trend to claim


def test_forecast_sheet_cpa_row_uses_derived_trend():
    data, _ = _plan_data("United States", 150_000, tag="fctrend")
    wb = _generate_wb(data)
    ws = _forecast_sheet(wb)
    label_col = None
    cpa_row = None
    for row in ws.iter_rows(min_row=1):
        for c in row:
            if c.value == "CPA (Cost Per Application)":
                label_col = c.column
                cpa_row = row
        if cpa_row is not None:
            break
    assert cpa_row is not None, "CPA row not found on Forecast sheet"
    trend_cell = [c for c in cpa_row if c.column > label_col and c.value is not None][
        -1
    ]
    assert trend_cell.value in ("Increasing", "Decreasing", "Stable", "—")


# ---------------------------------------------------------------------------
# Finding #19: non-US niche-board fallback must name the PLAN'S OWN market's
# boards (from data/international_benchmarks_2026.json), never a single
# hardcoded example market's boards (New Zealand's "Seek, Trade Me Jobs")
# shown to every non-US market.
# ---------------------------------------------------------------------------
def test_niche_board_fallback_is_market_aware():
    uk_data, _ = _plan_data(
        "United Kingdom", 50_000, plan_currency="GBP", tag="ukboards"
    )
    india_data, _ = _plan_data("India", 50_000, plan_currency="INR", tag="indiaboards")

    uk_wb = _generate_wb(uk_data)
    india_wb = _generate_wb(india_data)

    def _status_text(wb):
        ws = wb["Niche Board Matching"]
        for row in ws.iter_rows():
            for c in row:
                if c.value == "Status":
                    return row[c.column - 1 + 2].value  # D column, 2 to the right of B
        raise AssertionError("Status row not found")

    uk_text = _status_text(uk_wb)
    india_text = _status_text(india_wb)

    assert "Seek" not in uk_text and "Trade Me" not in uk_text, uk_text
    assert "Seek" not in india_text and "Trade Me" not in india_text, india_text
    # UK and India must not show the SAME board list.
    assert uk_text != india_text
    # Real UK platforms from the dataset should appear.
    uk_platforms = intl_benchmark_lookup.get_market_platform_names("United Kingdom")
    assert uk_platforms and any(p in uk_text for p in uk_platforms), (
        uk_platforms,
        uk_text,
    )


def test_niche_board_fallback_generic_when_market_unmapped():
    # Spain is not in international_benchmarks_2026.json's country table.
    spain_data, _ = _plan_data("Spain", 50_000, plan_currency="EUR", tag="esboards")
    wb = _generate_wb(spain_data)
    ws = wb["Niche Board Matching"]
    for row in ws.iter_rows():
        for c in row:
            if c.value == "Status":
                text = row[c.column - 1 + 2].value
                assert "Seek" not in text and "Trade Me" not in text, text
                assert "LinkedIn" in text or "Indeed" in text, text
                return
    raise AssertionError("Status row not found")


# ---------------------------------------------------------------------------
# Finding #20: Forecast sheet CPA must render with 2 decimals (_usd2_fmt),
# matching every other CPA in the workbook, not the whole-dollar _usd0_fmt.
# ---------------------------------------------------------------------------
def test_forecast_cpa_uses_two_decimal_format():
    data, _ = _plan_data("United States", 150_000, tag="cpafmt")
    wb = _generate_wb(data)
    ws = _forecast_sheet(wb)
    label_col = None
    cpa_row = None
    for row in ws.iter_rows(min_row=1):
        for c in row:
            if c.value == "CPA (Cost Per Application)":
                label_col = c.column
                cpa_row = row
        if cpa_row is not None:
            break
    assert cpa_row is not None, "CPA row not found on Forecast sheet"
    numeric_cells = [
        c for c in cpa_row if c.column > label_col and isinstance(c.value, (int, float))
    ]
    assert numeric_cells, "expected numeric CPA cells"
    for cell in numeric_cells:
        assert cell.number_format.endswith("0.00"), cell.number_format


# ---------------------------------------------------------------------------
# Finding #8: a CPC that fell through to the US-calibrated cascade
# (cpc_source not "intl_*"/not local-converted, e.g. Spain -- not one of
# the 38 international_benchmarks_2026.json countries) must render with an
# explicit USD format, never the plan's own non-USD currency symbol.
# ---------------------------------------------------------------------------
def test_unconverted_cpc_marked_usd_not_local_currency():
    spain_data, alloc = _plan_data("Spain", 50_000, plan_currency="EUR", tag="escpc")
    ch_allocs = alloc.get("channel_allocations", {})
    # Confirm the fixture actually exercises the un-converted USD-cascade
    # path (Spain has no intl_benchmarks_2026 entry).
    assert any(
        isinstance(ch, dict)
        and not str(ch.get("cpc_source") or "").startswith("intl_")
        and "->" not in str(ch.get("cpc_source") or "")
        for ch in ch_allocs.values()
    )

    wb = _generate_wb(spain_data)
    ws = wb["Executive Summary"]
    header_row = None
    cpc_col = amount_col = None
    for row in ws.iter_rows(min_row=1, max_row=30):
        for c in row:
            if c.value == "CPC":
                cpc_col = c.column
                header_row = c.row
            if c.value == "Amount":
                amount_col = c.column
    assert cpc_col is not None and amount_col is not None

    cpc_cell = ws.cell(row=header_row + 1, column=cpc_col)
    amount_cell = ws.cell(row=header_row + 1, column=amount_col)
    # The plan's OWN figure (Amount) stays in EUR...
    assert "€" in amount_cell.number_format, amount_cell.number_format
    # ...but the never-converted CPC constant must be marked USD, not EUR.
    assert "$" in cpc_cell.number_format, cpc_cell.number_format
    assert "€" not in cpc_cell.number_format, cpc_cell.number_format


def test_localized_cpc_keeps_local_currency():
    # UK IS in international_benchmarks_2026.json -- a single-market GBP
    # plan should get the "local" basis path, so its CPC legitimately
    # renders in the plan's own (GBP) currency, not forced to USD.
    #
    # NOTE: intl_benchmark_lookup's country matcher only resolves a BARE
    # country string (a separate, pre-existing gap from this fix's 7
    # findings -- it doesn't split "City, Country"), so locations_raw is
    # passed bare here to exercise the localized path this test targets.
    roles = [{"title": "Customer Service Rep", "count": 20, "tier": "entry"}]
    channels = {
        "Indeed": 40,
        "LinkedIn": 30,
        "Google Search Ads": 20,
        "Programmatic Job Boards": 10,
    }
    alloc = _build_alloc(
        50_000,
        roles,
        [{"city": "London", "state": "", "country": "United Kingdom"}],
        "general_entry_level",
        channels,
        collar_type="white",
        campaign_start_month=9,
        locations_raw=["United Kingdom"],
        plan_currency="GBP",
    )
    uk_data = {
        "client_name": "Test Client ukcpc",
        "industry": "general_entry_level",
        "budget": "£50,000",
        "hire_volume": "20 hires",
        "work_environment": "onsite",
        "roles": [r["title"] for r in roles],
        "target_roles": roles,
        "locations": ["London, United Kingdom"],
        "competitors": [],
        "plan_currency": "GBP",
        "_budget_allocation": alloc,
    }
    ch_allocs = alloc.get("channel_allocations", {})
    sources = {
        name: ch.get("cpc_source")
        for name, ch in ch_allocs.items()
        if isinstance(ch, dict)
    }
    assert any(
        str(s or "").startswith("intl_") or "->" in str(s or "")
        for s in sources.values()
    ), sources

    wb = _generate_wb(uk_data)
    ws = wb["Executive Summary"]
    header_row = cpc_col = None
    for row in ws.iter_rows(min_row=1, max_row=30):
        for c in row:
            if c.value == "CPC":
                cpc_col = c.column
                header_row = c.row
    cpc_cell = ws.cell(row=header_row + 1, column=cpc_col)
    assert "£" in cpc_cell.number_format, cpc_cell.number_format


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))
