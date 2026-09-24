"""Regression tests: the 90-Day Forecast narrative must never contradict the
SAME workbook's own Activation Event Calendar for the same month.

Real client-reported defect (Hershey Company plan, via Slack, confirmed NOT
touched by any fix as of commit ec676f5): the 90-day/seasonal forecast
narrative claimed the campaign's heaviest spend fell in a specific month,
while the SAME generated document's own seasonal calendar elsewhere called
that exact month "slow" (Low hiring intensity / "Year End -- minimal active
recruiting").

Root cause (excel_v2.py): the 90-Day Forecast sheet's monthly budget split
(``_seasonal_monthly_phasing``) was computed from an independent reload of
``data/seasonal_hiring_trends.json``, keyed by its OWN industry-string
matching -- a completely different computation, on a different key
namespace, than ``gold_standard.build_activation_calendar``'s per-month
``hiring_intensity`` / ``budget_weight`` (the Activation Event Calendar
section, Section 7 of the SAME workbook). For a manufacturing plan starting
in October, the forecast crowned December -- the calendar's own "Low" /
"Year End" month -- the heaviest-spend, hardcoded "(peak performance)"
period, contradicting the calendar two sheets over in the exact same file.

Fix: ``_seasonal_monthly_phasing`` now accepts the plan's own
``activation_calendar["timeline"]`` (the SAME per-month data the Activation
Event Calendar table renders) and derives its weights from that instead of a
second, independent lookup. The ramp narrative's "(learning)" /
"(optimizing)" / "(peak performance)" labels -- previously hardcoded onto
Month 1/2/3 by POSITION -- are now derived from each period's ACTUAL rank
among the real computed shares (``_period_spend_labels``), so the label can
never claim a month is "heaviest" when the printed numbers (or the
calendar) say otherwise.

This file intentionally does NOT touch or duplicate
tests/test_gold_standard_seasonality.py's coverage (sub-vertical overrides,
fallback_uniform) -- this is a narrative/calendar CONSISTENCY bug, not a
seasonality-data-correctness bug.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402

import budget_engine  # noqa: E402
import excel_v2  # noqa: E402
import gold_standard  # noqa: E402


def _hershey_manufacturing_data() -> dict:
    """A realistic manufacturing (candy/confectionery) plan starting in
    October -- this plan's own Activation Event Calendar (built below) puts
    December (the 3rd forecast month) at "Low" hiring intensity ("Year End
    -- minimal active recruiting"), the exact shape of the client-reported
    defect.
    """
    roles = [
        {"title": "Production Associate", "count": 30, "tier": "entry"},
        {"title": "Packaging Technician", "count": 15, "tier": "entry"},
    ]
    locations = [{"city": "Hershey", "state": "PA", "country": "United States"}]
    channels = {
        "niche_boards": 15,
        "programmatic_dsp": 25,
        "global_boards": 20,
        "social_media": 10,
        "employer_branding": 10,
        "regional_boards": 20,
    }
    alloc = budget_engine.calculate_budget_allocation(
        total_budget=300_000,
        roles=roles,
        locations=locations,
        industry="manufacturing",
        channel_percentages=channels,
        collar_type="blue_collar",
        campaign_start_month=10,
    )
    return {
        "client_name": "Hershey Company",
        "industry": "manufacturing",
        "budget": "$300,000",
        "campaign_duration": "18 months",
        "campaign_weeks": 78,
        "campaign_start_month": 10,
        "hire_volume": "45 hires",
        "work_environment": "onsite",
        "roles": [r["title"] for r in roles],
        "target_roles": roles,
        "locations": [f"{loc['city']}, {loc['state']}" for loc in locations],
        "_budget_allocation": alloc,
    }


_MONTH_NAMES_OCT_DEC = ["October", "November", "December"]


# ---------------------------------------------------------------------------
# 1. Unit-level: the forecast's per-month weight must come from the SAME
#    activation_calendar data the Activation Event Calendar table uses, and
#    must never crown the calendar's lowest-intensity month "heaviest".
# ---------------------------------------------------------------------------


def test_forecast_phasing_never_crowns_the_calendars_low_month_heaviest():
    data = _hershey_manufacturing_data()
    cal = gold_standard.build_activation_calendar(data)
    timeline = cal["timeline"]
    by_name = {m["month_name"]: m for m in timeline}

    # Sanity: this plan's own calendar really does call December "Low" --
    # this is what makes the repro real, not contrived.
    assert by_name["December"]["hiring_intensity"] == "low"
    assert by_name["October"]["hiring_intensity"] in ("high", "very_high")

    monthly_pcts = excel_v2._seasonal_monthly_phasing(
        data["industry"], data["campaign_start_month"], timeline
    )
    assert len(monthly_pcts) == 3
    assert round(sum(monthly_pcts), 4) == 1.0

    heaviest_idx = monthly_pcts.index(max(monthly_pcts))
    heaviest_month = _MONTH_NAMES_OCT_DEC[heaviest_idx]

    assert heaviest_month != "December", (
        f"Forecast claims heaviest spend falls in {heaviest_month} "
        f"({monthly_pcts[heaviest_idx] * 100:.1f}%), but this plan's own "
        "Activation Event Calendar calls December 'Low' hiring intensity "
        "('Year End -- minimal active recruiting') -- the exact "
        "narrative/calendar contradiction reported by the client."
    )
    # The month the forecast crowns heaviest must itself be at least a
    # "moderate" calendar month -- never the plan's own "low" month.
    assert by_name[heaviest_month]["hiring_intensity"] != "low"


def test_period_spend_labels_match_actual_rank_not_position():
    """_period_spend_labels must label whichever period actually holds the
    largest share "heaviest planned spend" -- never a fixed position -- so
    the ramp narrative sentence can't contradict its own printed numbers."""
    # A case where the middle period is deliberately the largest (mirrors
    # the Hershey repro: October < November > December).
    pcts = [0.30, 0.45, 0.25]
    labels = excel_v2._period_spend_labels(pcts)
    assert labels[1] == "heaviest planned spend"
    assert labels[2] == "lightest planned spend"
    assert labels[0] == "transitional spend"

    # The classic monotonically-increasing case still labels Month 3 heaviest.
    pcts2 = [0.25, 0.35, 0.40]
    labels2 = excel_v2._period_spend_labels(pcts2)
    assert labels2[2] == "heaviest planned spend"
    assert labels2[0] == "lightest planned spend"


# ---------------------------------------------------------------------------
# 2. Full-pipeline: generate the REAL workbook (excel_v2.generate_excel_v2)
#    and confirm the 90-Day Forecast sheet's ramp narrative and the Quality
#    Intelligence sheet's Activation Event Calendar table -- two different
#    sheets in the SAME generated document -- agree on December.
# ---------------------------------------------------------------------------


def _generate_wb(data: dict):
    raw = excel_v2.generate_excel_v2(data)
    assert isinstance(raw, (bytes, bytearray)) and len(raw) > 0
    return openpyxl.load_workbook(io.BytesIO(raw), data_only=False)


def _activation_calendar_row(ws, month_name: str) -> list:
    """Read the Activation Event Calendar table row for ``month_name`` from
    the Quality Intelligence worksheet."""
    printing = False
    for row in ws.iter_rows():
        vals = [c.value for c in row]
        if vals and vals[1] == "Activation Event Calendar":
            printing = True
            continue
        if printing and vals and vals[1] == month_name:
            return vals
    return []


def test_generated_workbook_forecast_never_contradicts_activation_calendar():
    data = _hershey_manufacturing_data()
    gold = gold_standard.apply_all_quality_gates(data)
    data["_gold_standard"] = gold
    assert (gold.get("activation_calendar") or {}).get("timeline"), (
        "Activation calendar must be built for this test to be meaningful"
    )

    wb = _generate_wb(data)

    # -- Activation Event Calendar (Quality Intelligence sheet) --
    qi_ws = wb["Quality Intelligence"]
    dec_row = _activation_calendar_row(qi_ws, "December")
    assert dec_row, "December row not found in Activation Event Calendar"
    # Row shape: [None, "December", "Year End", "Low", "0.7x", events, rec]
    dec_intensity = str(dec_row[3] or "").strip().lower()
    assert dec_intensity == "low", dec_row

    # -- 90-Day Forecast ramp narrative + Spend row (90-Day Forecast sheet) --
    fc_ws = wb["90-Day Forecast"]
    ramp_notes = [
        c.value
        for row in fc_ws.iter_rows()
        for c in row
        if isinstance(c.value, str) and "phases budget" in c.value
    ]
    assert ramp_notes, "Expected the ramp-phasing footnote on 90-Day Forecast"
    ramp_note = ramp_notes[0]

    rows = list(fc_ws.iter_rows(min_col=2, values_only=True))
    header_idx = next(i for i, r in enumerate(rows) if r and r[0] == "Metric")
    header = rows[header_idx]
    spend_row = next(r for r in rows[header_idx + 1 :] if r and r[0] == "Spend")
    month_spend = list(spend_row[1:4])
    month_headers = [str(h) for h in header[1:4]]

    assert any("December" in h for h in month_headers), month_headers
    dec_col = next(i for i, h in enumerate(month_headers) if "December" in h)
    dec_spend = month_spend[dec_col]

    # The contradiction reported by the client: December (the calendar's own
    # "Low" month) must never be the column with the largest Spend, and the
    # ramp narrative must never call it "heaviest"/"peak" while a real
    # higher-intensity month sits in the same 3-month window.
    assert dec_spend == min(month_spend) or dec_spend < max(month_spend), (
        "December (Activation Calendar: Low) must not be this forecast's "
        f"heaviest-spend month. Spend row: {dict(zip(month_headers, month_spend))}"
    )
    assert "December" not in ramp_note or "heaviest" not in ramp_note.split(
        "December"
    )[0][-40:], ramp_note
    # Belt-and-suspenders: the word "peak performance" (the old hardcoded,
    # position-based label that caused the original contradiction) must not
    # appear in the ramp narrative at all anymore.
    assert "peak performance" not in ramp_note, ramp_note


if __name__ == "__main__":
    test_forecast_phasing_never_crowns_the_calendars_low_month_heaviest()
    test_period_spend_labels_match_actual_rank_not_position()
    test_generated_workbook_forecast_never_contradicts_activation_calendar()
    print("All seasonality narrative/calendar consistency tests passed.")
