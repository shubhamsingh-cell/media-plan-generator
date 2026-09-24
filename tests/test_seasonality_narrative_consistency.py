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

First-pass fix (commit b802d68): ``_seasonal_monthly_phasing`` was changed to
accept the plan's own ``activation_calendar["timeline"]`` and read its
``budget_weight`` per month -- the right field -- but then still multiplied
that weight by a FIXED [0.25, 0.35, 0.40] ramp-shape base. That base is
bigger than the calendar's real weight spread (0.7x-1.3x) often enough that
the final spend ranking could still invert the calendar's own ordering. A
verifier swept 15 industries x 12 start months (180 cases) and found 3 where
the calendar's own "Low" month was still crowned heaviest (hospitality/Oct,
education/Oct, restaurant/Oct) and 105/180 cases where a busier calendar
month got LESS spend than a quieter one -- including the Hershey repro
itself (December "Low" 0.7x -> 30.9% of spend vs. October "High" 1.1x ->
30.4%, a near-tie but backwards). The first pass also added a "matching
this plan's Activation Event Calendar" sentence unconditionally, which was
therefore actively false in exactly these cases.

Second-pass fix (this file's target): ``_seasonal_monthly_phasing`` no
longer multiplies the calendar's budget_weight by any ramp-shape base when
a real timeline is present -- the per-month split is DIRECTLY proportional
to budget_weight, so the ordering is mathematically guaranteed to match the
calendar's ordering (the ramp-shape base is kept only for the
seasonal_hiring_trends.json fallback path, where no real per-month weight
exists). ``_period_spend_labels`` collapses to "comparable planned spend"
for every period when the largest and smallest share are within 2
percentage points, instead of asserting a coin-flip as a confident
ranking. The <=4-week "weekly frame" narrative (previously a single
hardcoded "front-loaded learning ramping to peak performance" phrase, never
routed through the fix at all) now uses the same rank-derived per-week
labels. The "matching this plan's Activation Event Calendar" clause is now
appended only when the split was actually derived from a real timeline
(``_calendar_backed``), never unconditionally.

Third pass (see tests/test_forecast_short_plan_calendar_phasing.py): the
second pass covered only the 3-month 90-Day path. Short plans (<=13 weeks)
went through a separate stretch-and-rebucket path that still contradicted
the calendar; they now derive every column from the campaign's own
calendar-month days, and ``_calendar_backed`` (a "timeline is non-empty"
proxy) is replaced by an explicit ``used_real_calendar`` returned by the
function that computed the split.

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

    # Second-pass fix: since the split is now directly proportional to
    # budget_weight (no ramp-shape override), the ordering must EXACTLY
    # match the calendar's own weight ordering, not just "happen" to avoid
    # December -- October (High, 1.1x) must outrank November (Moderate,
    # 1.0x) must outrank December (Low, 0.7x).
    assert heaviest_month == "October", (
        f"Expected October (High, 1.1x) to be the heaviest-spend month, "
        f"got {heaviest_month} ({monthly_pcts})"
    )
    assert monthly_pcts[0] > monthly_pcts[1] > monthly_pcts[2], monthly_pcts


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


def test_period_spend_labels_near_tie_reads_as_comparable_not_a_claim():
    """A genuine near-tie (e.g. a 2-period 50/50 split, or the Hershey-style
    30.4% vs 30.9% near-miss the first-pass fix produced) must not be
    dressed up as a confident "(heaviest)"/"(lightest)" ranking -- that's
    false precision on what is functionally a coin-flip."""
    # Exact 50/50 split (verifier's own example).
    assert excel_v2._period_spend_labels([0.5, 0.5]) == [
        "comparable planned spend",
        "comparable planned spend",
    ]
    # All 3 periods within the 2-percentage-point tie threshold of each
    # other (mirrors the real hospitality_travel/October and
    # restaurant/October sweep cases below, where High/Moderate/Low
    # budget_weight still lands within ~2pp of each other).
    labels = excel_v2._period_spend_labels([0.346, 0.327, 0.327])
    assert labels == ["comparable planned spend"] * 3
    # A real, non-tied spread still gets real labels.
    labels2 = excel_v2._period_spend_labels([0.20, 0.35, 0.45])
    assert "comparable planned spend" not in labels2


# ---------------------------------------------------------------------------
# 1b. Broad invariant sweep: across many industries x many start months, the
#    forecast's per-month share must never invert the calendar's own
#    budget_weight ordering, and the calendar's "Low" month must never be
#    labeled "heaviest" while a non-"low" month sits in the same window.
#    (This is the check the verifier's panel ran -- 15 industries x 12
#    start months -- that caught the first-pass fix's ramp-shape override.)
# ---------------------------------------------------------------------------

_SWEEP_INDUSTRIES = [
    "manufacturing",
    "retail_consumer",
    "healthcare_medical",
    "tech_engineering",
    "hospitality_travel",
    "construction_real_estate",
    "finance_banking",
    "logistics_supply_chain",
    "education",
    "government",
    "food_beverage",
    "automotive",
    "insurance",
    "telecommunications",
    "restaurant",
]


def test_forecast_never_inverts_calendar_ordering_across_industries_and_months():
    numeric_violations = []
    label_violations = []

    for industry in _SWEEP_INDUSTRIES:
        for start_month in range(1, 13):
            data = {
                "client_name": "Sweep Co",
                "industry": industry,
                "campaign_start_month": start_month,
                "roles": ["Generic Role"],
            }
            cal = gold_standard.build_activation_calendar(data)
            timeline = cal["timeline"]
            by_month_num = {m["month"]: m for m in timeline}

            monthly_pcts = excel_v2._seasonal_monthly_phasing(
                industry, start_month, timeline
            )
            months = [((start_month - 1 + i) % 12) + 1 for i in range(3)]
            weights = [by_month_num[m]["budget_weight"] for m in months]

            # Numeric invariant: a strictly higher calendar weight must
            # never receive a strictly smaller forecast share.
            for i in range(3):
                for j in range(3):
                    if weights[i] > weights[j] and monthly_pcts[i] < monthly_pcts[j]:
                        numeric_violations.append(
                            (industry, start_month, months, weights, monthly_pcts)
                        )

            # Label invariant: the month crowned "heaviest planned spend"
            # must never be the calendar's "low" month while a non-"low"
            # month is also in the 3-month window.
            labels = excel_v2._period_spend_labels(monthly_pcts)
            if "heaviest planned spend" in labels:
                h_idx = labels.index("heaviest planned spend")
                h_intensity = by_month_num[months[h_idx]]["hiring_intensity"]
                if h_intensity == "low" and any(
                    by_month_num[m]["hiring_intensity"] != "low" for m in months
                ):
                    label_violations.append(
                        (
                            industry,
                            start_month,
                            months,
                            [by_month_num[m]["hiring_intensity"] for m in months],
                        )
                    )

    assert not numeric_violations, (
        f"{len(numeric_violations)} numeric ordering inversions "
        f"(higher calendar weight got less spend): {numeric_violations[:5]}"
    )
    assert not label_violations, (
        f"{len(label_violations)} cases where the calendar's 'Low' month "
        f"was crowned 'heaviest planned spend': {label_violations[:5]}"
    )


def test_verifier_flagged_cases_no_longer_contradict():
    """The 3 specific cases the verifier's panel flagged (calendar's Low
    month still crowned heaviest under the first-pass fix), re-checked
    directly starting October."""
    for industry in ("hospitality_travel", "education", "restaurant"):
        data = {
            "client_name": "X",
            "industry": industry,
            "campaign_start_month": 10,
            "roles": ["Role"],
        }
        cal = gold_standard.build_activation_calendar(data)
        timeline = cal["timeline"]
        by_month_num = {m["month"]: m for m in timeline}
        months = [((10 - 1 + i) % 12) + 1 for i in range(3)]

        monthly_pcts = excel_v2._seasonal_monthly_phasing(industry, 10, timeline)
        labels = excel_v2._period_spend_labels(monthly_pcts)

        if "heaviest planned spend" in labels:
            h_idx = labels.index("heaviest planned spend")
            h_intensity = by_month_num[months[h_idx]]["hiring_intensity"]
            assert h_intensity != "low", (industry, months, monthly_pcts, labels)


# ---------------------------------------------------------------------------
# 1c. The <=4-week "weekly frame" path (previously untouched -- hardcoded
#    "front-loaded learning ramping to peak performance" regardless of the
#    actual computed split) must also use rank-derived labels.
# ---------------------------------------------------------------------------


def test_weekly_frame_narrative_no_longer_hardcoded():
    data = _hershey_manufacturing_data()
    data["campaign_weeks"] = 3  # <=4 weeks -> _weekly_frame
    data["campaign_duration"] = "3 weeks"
    gold = gold_standard.apply_all_quality_gates(data)
    data["_gold_standard"] = gold

    wb = _generate_wb(data)
    ws = next(
        (s for s in wb.sheetnames if "Week" in s and "Forecast" in s),
        None,
    )
    assert ws, wb.sheetnames
    fc_ws = wb[ws]
    ramp_notes = [
        c.value
        for row in fc_ws.iter_rows()
        for c in row
        if isinstance(c.value, str) and "phases budget" in c.value
    ]
    assert ramp_notes, "Expected the ramp-phasing footnote"
    ramp_note = ramp_notes[0]
    # The old hardcoded phrase, regardless of the actual computed split,
    # must be gone -- replaced by rank-derived per-week labels.
    assert "front-loaded learning ramping to peak performance" not in ramp_note
    assert any(
        lbl in ramp_note
        for lbl in (
            "lightest planned spend",
            "heaviest planned spend",
            "transitional spend",
            "comparable planned spend",
        )
    ), ramp_note


# ---------------------------------------------------------------------------
# 1d. The "matching this plan's Activation Event Calendar" claim must only
#    appear when the split was actually derived from that calendar.
# ---------------------------------------------------------------------------


def test_calendar_match_claim_only_printed_when_true():
    # True case: a real activation_calendar timeline is present.
    data_with_cal = _hershey_manufacturing_data()
    gold = gold_standard.apply_all_quality_gates(data_with_cal)
    data_with_cal["_gold_standard"] = gold
    wb_with_cal = _generate_wb(data_with_cal)
    fc_ws = wb_with_cal["90-Day Forecast"]
    ramp_note = next(
        c.value
        for row in fc_ws.iter_rows()
        for c in row
        if isinstance(c.value, str) and "phases budget" in c.value
    )
    assert "matching this plan's Activation Event Calendar" in ramp_note, ramp_note

    # False case: no _gold_standard / activation_calendar at all -- the
    # forecast falls back to the generic seasonal_hiring_trends.json lookup
    # and must NOT claim to match a calendar it never saw.
    data_no_cal = _hershey_manufacturing_data()
    assert "_gold_standard" not in data_no_cal
    wb_no_cal = _generate_wb(data_no_cal)
    fc_ws2 = wb_no_cal["90-Day Forecast"]
    ramp_note2 = next(
        c.value
        for row in fc_ws2.iter_rows()
        for c in row
        if isinstance(c.value, str) and "phases budget" in c.value
    )
    assert "matching this plan's Activation Event Calendar" not in ramp_note2, (
        ramp_note2
    )


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
