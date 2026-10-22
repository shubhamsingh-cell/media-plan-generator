"""Plan-integrity rules: hires suppression reaches the TOTAL row, phases fit the
plan length, zero-hire channels are not funded, benchmark-gap and low-confidence
plans say so, market-data blocks carry their geography."""

import openpyxl

import excel_v2


def _channel_table_workbook():
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Channel Recommendations"
    ws.append(["Channel", "Spend", "Apps", "Hires"])
    ws.append(["Programmatic", 100, 50, 4])
    ws.append(["Job Boards", 80, 40, 3])
    ws.append([None, None, None, None])  # spacer row, as the real sheet writes
    ws.append(["TOTAL", 180, 90, 77])
    return wb


class TestTotalRowHiresSuppression:
    def test_total_row_reads_na_when_hires_suppressed(self):
        wb = _channel_table_workbook()
        excel_v2._strip_hires_from_workbook(wb)
        ws = wb["Channel Recommendations"]
        assert [ws.cell(row=r, column=4).value for r in (2, 3)] == ["n/a", "n/a"]
        assert ws.cell(row=5, column=4).value == "n/a"
        # money and applications on the TOTAL row are untouched
        assert ws.cell(row=5, column=2).value == 180
        assert ws.cell(row=5, column=3).value == 90

    def test_numbers_under_an_unrelated_later_block_are_kept(self):
        wb = _channel_table_workbook()
        ws = wb["Channel Recommendations"]
        ws.cell(row=7, column=1, value="Other block")
        ws.cell(row=7, column=4, value=12)
        excel_v2._strip_hires_from_workbook(wb)
        assert ws.cell(row=7, column=4).value == 12


import re

import ppt_generator


def _week_pairs(campaign_weeks):
    out = []
    for ph in ppt_generator._timeline_phases(campaign_weeks):
        nums = [int(n) for n in re.findall(r"\d+", ph["weeks"])]
        out.append((nums[0], nums[-1]))
    return out


class TestTimelineWeeks:
    def test_two_week_plan_never_runs_backwards_or_past_the_end(self):
        pairs = _week_pairs(2)
        assert all(a <= b <= 2 for a, b in pairs)
        assert len(pairs) <= 2

    def test_thirty_day_plan_has_no_empty_or_repeated_range(self):
        labels = [p["weeks"] for p in ppt_generator._timeline_phases(4)]
        assert "Weeks 4-4" not in labels
        assert "Weeks 3-4" not in labels or labels[-1] != "Weeks 4-4"
        pairs = _week_pairs(4)
        assert pairs[-1][1] == 4 and all(a <= b <= 4 for a, b in pairs)

    def test_every_duration_tiles_the_plan_exactly(self):
        for weeks in (1, 2, 3, 4, 5, 6, 7, 8, 12, 13, 26, 27, 39, 52, 90 // 7):
            pairs = _week_pairs(weeks)
            assert pairs[0][0] == 1, weeks
            assert pairs[-1][1] == weeks, weeks
            for (a, b), (c, _d) in zip(pairs, pairs[1:]):
                assert a <= b and c == b + 1, (weeks, pairs)

    def test_ninety_day_plan_keeps_three_phases(self):
        assert _week_pairs(13) == [(1, 3), (4, 6), (7, 13)]

    def test_missing_or_bad_weeks_fall_back_to_twelve(self):
        assert _week_pairs(None)[-1][1] == 12
        assert _week_pairs("n/a")[-1][1] == 12


class TestCpqaFromPhaseOne:
    def test_phase_one_tracks_cpqa_in_every_duration(self):
        for weeks in (1, 2, 4, 13, 40):
            first = ppt_generator._timeline_phases(weeks)[0]
            assert any("CPQA" in b for b in first["bullets"]), weeks


import copy
import io

import budget_engine
import plan_validator


def _german_plan(confidence=None, cpa_eur=None, usd_per_local="keep"):
    """A Germany manufacturing plan as the generators receive it. ``cpa_eur``
    overrides the blended cost per application (test input, to place the plan
    below or inside the market's benchmark range)."""
    roles = [
        {"title": "Maintenance Technician", "count": 50, "tier": "mid"},
        {"title": "Production Supervisor", "count": 50, "tier": "mid"},
    ]
    locations = [{"city": "Berlin", "state": "", "country": "Germany"}]
    channels = {
        "Programmatic DSP": 34,
        "Global Job Boards": 24,
        "Regional Boards": 20,
        "Niche Boards": 12,
        "Referral": 10,
    }
    alloc = budget_engine.calculate_budget_allocation(
        total_budget=250_000,
        roles=roles,
        locations=locations,
        industry="manufacturing",
        channel_percentages=channels,
        collar_type="blue",
        campaign_start_month=10,
    )
    alloc = copy.deepcopy(alloc)
    if cpa_eur is not None:
        alloc["total_projected"]["cost_per_application"] = cpa_eur
    if usd_per_local != "keep":
        alloc["metadata"]["intl_cpc_basis"]["usd_per_local"] = usd_per_local
    data = {
        "client_name": "Acme Works",
        "industry": "manufacturing",
        "budget": "€250,000",
        "budget_period": "campaign",
        "campaign_duration": "3 months",
        "campaign_weeks": 13,
        "campaign_start_month": 10,
        "hire_volume": "100",
        "roles": [r["title"] for r in roles],
        "target_roles": roles,
        "locations": ["Berlin, Germany"],
        "country": "Germany",
        "_budget_allocation": alloc,
    }
    if confidence is not None:
        data["_synthesized"] = {"confidence_scores": {"overall": confidence}}
    return data


class TestBenchmarkGap:
    def test_blended_cpa_below_the_low_end_is_flagged_with_the_range(self):
        gap = plan_validator.cpa_benchmark_gap(_german_plan(cpa_eur=5.0))
        assert gap["status"] == "below"
        msg = plan_validator.benchmark_gap_message(gap)
        assert "€5.00" in msg
        assert (
            "below this market's industry benchmark range (US$8–25 per application)"
            in msg
        )
        assert "Treat it as optimistic until a test confirms it." in msg

    def test_cpa_inside_the_range_is_not_flagged(self):
        gap = plan_validator.cpa_benchmark_gap(_german_plan(cpa_eur=14.0))
        assert gap["status"] in ("within", "channel_below")
        if gap["status"] == "within":
            assert plan_validator.benchmark_gap_message(gap) == ""

    def test_compares_in_one_currency_the_plans_own_rate(self):
        # EUR 7.50 is below US$8 only if converted at the plan's own rate
        # (1.1355 USD per EUR => US$8.52 -> NOT below); at parity it would be.
        gap = plan_validator.cpa_benchmark_gap(_german_plan(cpa_eur=7.5))
        assert gap["plan_cpa_usd"] > 8.0
        assert gap["status"] != "below"

    def test_no_exchange_rate_means_not_compared(self):
        gap = plan_validator.cpa_benchmark_gap(
            _german_plan(cpa_eur=5.0, usd_per_local=None)
        )
        assert gap["status"] == "not_comparable"
        assert "different currencies" in plan_validator.benchmark_gap_message(gap)

    def test_us_plan_has_no_market_range_to_compare(self):
        data = _german_plan(cpa_eur=5.0)
        data["locations"] = ["Phoenix, AZ, United States"]
        data["country"] = "United States"
        assert plan_validator.cpa_benchmark_gap(data) is None

    def test_validator_warns_instead_of_passing_clean(self):
        data = _german_plan(cpa_eur=5.0, confidence=0.47)
        summary = plan_validator.validate_plan(data)
        checks = {f["check"]: f for f in summary["findings"]}
        assert checks["cpa_vs_benchmark"]["severity"] == "medium"
        assert checks["low_data_confidence"]["severity"] == "medium"
        assert summary["total_findings"] >= 2


class TestInternalDraftStamp:
    def test_below_fifty_percent_is_stamped(self):
        assert (
            plan_validator.internal_draft_stamp(_german_plan(confidence=0.47))
            == "Internal draft: data confidence 47%. Review before sharing with a client."
        )

    def test_fifty_and_above_or_unknown_is_not(self):
        assert plan_validator.internal_draft_stamp(_german_plan(confidence=0.5)) == ""
        assert plan_validator.internal_draft_stamp(_german_plan(confidence=0.9)) == ""
        assert plan_validator.internal_draft_stamp(_german_plan()) == ""


def _workbook_texts(data):
    raw = excel_v2.generate_excel_v2(data)
    raw = raw[0] if isinstance(raw, tuple) else raw
    wb = openpyxl.load_workbook(io.BytesIO(raw))
    out = {}
    for ws in wb.worksheets:
        out[ws.title] = [
            str(c.value) for row in ws.iter_rows() for c in row if c.value is not None
        ]
    return out


class TestWorkbookIntegrityNotes:
    def test_low_confidence_plan_is_stamped_on_summary_sources_and_review(self):
        texts = _workbook_texts(_german_plan(confidence=0.47))
        stamp = (
            "Internal draft: data confidence 47%. Review before sharing with a client."
        )
        assert stamp in texts["Executive Summary"]
        assert stamp in texts["Sources & Confidence"]
        assert any(stamp in t for t in texts["Review before sending"])

    def test_confident_plan_carries_no_stamp(self):
        texts = _workbook_texts(_german_plan(confidence=0.8))
        assert not any("Internal draft" in t for ts in texts.values() for t in ts)

    def test_benchmark_gap_is_said_on_summary_and_review_sheet(self):
        texts = _workbook_texts(_german_plan(cpa_eur=5.0, confidence=0.8))
        key = (
            "is below this market's industry benchmark range (US$8–25 per application)"
        )
        assert any(key in t for t in texts["Executive Summary"])
        assert any(key in t for t in texts["Review before sending"])


from pptx import Presentation
from pptx.util import Inches


def _deck_slide_texts(data):
    blob = ppt_generator.generate_pptx(copy.deepcopy(data))
    prs = Presentation(io.BytesIO(blob))
    slides = []
    for slide in prs.slides:
        texts = []
        for shape in slide.shapes:
            if shape.has_text_frame:
                texts.append(shape.text_frame.text)
        slides.append(texts)
    return slides


class TestDeckIntegrityNotes:
    def test_cover_carries_internal_draft_stamp_when_confidence_is_low(self):
        slides = _deck_slide_texts(_german_plan(confidence=0.47))
        assert (
            "Internal draft: data confidence 47%. Review before sharing with a client."
            in slides[0]
        )

    def test_cover_is_clean_when_confidence_is_fine(self):
        slides = _deck_slide_texts(_german_plan(confidence=0.8))
        assert not any("Internal draft" in t for t in slides[0])

    def test_benchmark_gap_is_said_on_the_comparison_slide(self):
        slides = _deck_slide_texts(_german_plan(cpa_eur=5.0, confidence=0.8))
        joined = [" ".join(s) for s in slides]
        hit = [j for j in joined if "benchmark (US$8–25)" in j]
        assert hit, "no slide states that the planned CPA is below the benchmark"
        assert "IMPLEMENTATION TIMELINE" in hit[0]

    @staticmethod
    def _slide9_gap_geometry(data):
        """(gap note shape, right card shape, timeline header shape) on the
        comparison slide, read off the generated deck."""
        prs = Presentation(io.BytesIO(ppt_generator.generate_pptx(copy.deepcopy(data))))
        for slide in prs.slides:
            shapes = list(slide.shapes)
            note = [
                sh
                for sh in shapes
                if sh.has_text_frame and "optimistic until" in sh.text_frame.text
            ]
            header = [
                sh
                for sh in shapes
                if sh.has_text_frame
                and sh.text_frame.text.startswith("IMPLEMENTATION TIMELINE")
            ]
            est = [
                sh
                for sh in shapes
                if sh.has_text_frame and "Nova working estimate" in sh.text_frame.text
            ]
            if note and header and est:
                inside = [
                    sh
                    for sh in shapes
                    if sh.shape_type == 1
                    and sh.left <= est[0].left
                    and sh.top <= est[0].top
                    and sh.left + sh.width >= est[0].left + est[0].width
                ]
                card = max(inside, key=lambda sh: sh.width)
                return note[0], card, header[0]
        raise AssertionError("comparison slide with a gap note not found")

    def test_gap_note_box_stays_inside_the_right_card(self):
        note, card, header = self._slide9_gap_geometry(
            _german_plan(cpa_eur=5.0, confidence=0.8)
        )
        assert note.left + note.width <= card.left + card.width
        assert note.top + note.height <= header.top

    def test_a_long_gap_note_wraps_inside_the_card_and_clears_the_timeline(
        self, monkeypatch
    ):
        long_note = (
            "Planned CPA \u20ac13.75 is below the industry benchmark (US$20\u201360) "
            "for this market and role mix: optimistic until a test confirms it, "
            "so hold budget back until then."
        )
        monkeypatch.setattr(
            plan_validator,
            "benchmark_gap_message",
            lambda gap, short=False: long_note,
        )
        note, card, header = self._slide9_gap_geometry(
            _german_plan(cpa_eur=5.0, confidence=0.8)
        )
        assert note.text_frame.text == long_note
        assert note.left + note.width <= card.left + card.width
        assert note.height > Inches(0.22)  # room for the second line
        assert note.top + note.height <= header.top

    def test_no_gap_note_when_cpa_is_inside_the_range(self):
        slides = _deck_slide_texts(_german_plan(cpa_eur=14.0, confidence=0.8))
        assert not any("market's industry benchmark" in " ".join(s) for s in slides)


_WORKFORCE = {
    "gen_z_insights": {
        "job_search_platforms": "22.1% of Gen Z use TikTok as a job search tool",
    },
    "employer_branding": {
        # Real shape: a row of named items, each tagged (or not) on its own.
        "channel_effectiveness": {
            "linkedin": {"reach": "1.15 billion monthly active users worldwide"},
        },
    },
    "untagged_block": {"stat": "A survey figure with no stated country"},
}


def _with_workforce(data):
    data = copy.deepcopy(data)
    data.setdefault("_synthesized", {})["workforce_insights"] = copy.deepcopy(
        _WORKFORCE
    )
    return data


class TestMarketDataBlockGeography:
    def test_untagged_and_us_blocks_are_hidden_on_a_germany_plan(self):
        texts = _workbook_texts(_with_workforce(_german_plan(confidence=0.8)))
        blob = " ".join(texts["Market Intelligence"])
        assert "TikTok" not in blob and "Gen Z" not in blob
        assert "untagged" not in blob.lower() and "no stated country" not in blob

    def test_global_block_stays_and_carries_its_geography(self):
        texts = _workbook_texts(_with_workforce(_german_plan(confidence=0.8)))
        assert "Employer Branding (Global)" in texts["Market Intelligence"]

    def test_us_plan_keeps_every_block(self):
        data = _with_workforce(_german_plan(confidence=0.8))
        data["locations"] = ["Phoenix, AZ, United States"]
        data["country"] = "United States"
        texts = _workbook_texts(data)
        blob = " ".join(texts["Market Intelligence"])
        assert "TikTok" in blob and "no stated country" in blob

    def test_every_workforce_block_the_synthesizer_emits_is_tagged(self):
        import re

        src = open("data_synthesizer.py").read()
        i = src.index("def fuse_workforce_insights")
        body = src[i : src.index("\ndef ", i + 10)]
        emitted = set(re.findall(r'result\["(\w+)"\]', body))
        assert emitted <= set(excel_v2._WORKFORCE_BLOCK_GEOGRAPHY)


def _with_calendar(data, timeline):
    data = copy.deepcopy(data)
    data["_gold_standard"] = {"activation_calendar": {"timeline": timeline}}
    return data


def _forecast_rows(data):
    raw = excel_v2.generate_excel_v2(data)
    raw = raw[0] if isinstance(raw, tuple) else raw
    wb = openpyxl.load_workbook(io.BytesIO(raw))
    ws = wb["13-Week Forecast"]
    rows = {}
    notes = []
    for r in ws.iter_rows(values_only=True):
        vals = [v for v in r if v is not None]
        if vals and vals[0] in (
            "Spend",
            "Applications",
            "Hires",
            "CPA (Cost Per Application)",
        ):
            rows[vals[0]] = vals[1:]
        elif (
            vals and isinstance(vals[0], str) and "Activation Calendar rates" in vals[0]
        ):
            notes.append(vals[0])
    return rows, notes


_CAL = [
    {"month": 10, "hiring_intensity": "high", "budget_weight": 1.10},
    {"month": 11, "hiring_intensity": "moderate", "budget_weight": 1.0},
    {"month": 12, "hiring_intensity": "low", "budget_weight": 0.70},
]


class TestSeasonalCpa:
    def test_cpa_is_not_flat_through_a_month_the_calendar_rates_high(self):
        flat, _ = _forecast_rows(_german_plan(confidence=0.8))
        seasonal, notes = _forecast_rows(
            _with_calendar(_german_plan(confidence=0.8), _CAL)
        )
        cpa = seasonal["CPA (Cost Per Application)"]
        assert len(set(round(v, 4) for v in cpa[:3])) > 1, cpa
        assert cpa[0] > cpa[1]  # October (High, 1.10x) costs more than November
        assert notes and "October" in notes[0] and "x1.10" in notes[0]
        # the campaign total of applications does not move
        assert seasonal["Applications"][-2] == flat["Applications"][-2]

    def test_word_only_rating_uses_the_labelled_estimate(self):
        cal = [{"month": 10, "hiring_intensity": "high"}] + _CAL[1:]
        rows, notes = _forecast_rows(_with_calendar(_german_plan(confidence=0.8), cal))
        assert "Nova estimate" in notes[0]
        mults, basis = excel_v2._calendar_cpa_multipliers([10, 11], cal)
        assert mults == [excel_v2._PEAK_CPA_MULTIPLIER_ESTIMATE["high"], 1.0]
        assert basis == ["Nova estimate", ""]

    def test_quiet_months_are_not_given_a_discount(self):
        mults, _ = excel_v2._calendar_cpa_multipliers([12], _CAL)
        assert mults == [1.0]

    def test_no_calendar_leaves_the_forecast_flat(self):
        rows, notes = _forecast_rows(_german_plan(confidence=0.8))
        cpa = rows["CPA (Cost Per Application)"]
        assert not notes
        assert max(cpa[:3]) - min(cpa[:3]) < 0.05 * max(cpa[:3])


class TestCpqaColumn:
    def _sheet(self, data):
        raw = excel_v2.generate_excel_v2(data)
        raw = raw[0] if isinstance(raw, tuple) else raw
        return openpyxl.load_workbook(io.BytesIO(raw))["Channel Recommendations"]

    def test_cpqa_sits_next_to_cpa_and_uses_the_plans_own_qualified_apps(self):
        data = _german_plan(confidence=0.8)
        ws = self._sheet(data)
        header_row = next(
            r
            for r in range(1, ws.max_row + 1)
            if ws.cell(row=r, column=7).value == "CPA"
        )
        assert ws.cell(row=header_row, column=8).value == "CPQA"
        funnel = data["_budget_allocation"]["metadata"]["funnel"]["per_channel"]
        checked = 0
        for r in range(header_row + 1, ws.max_row + 1):
            label = ws.cell(row=r, column=2).value
            spend = ws.cell(row=r, column=5).value
            cpqa = ws.cell(row=r, column=8).value
            if not label or not isinstance(spend, (int, float)) or label == "TOTAL":
                continue
            name = next(
                n
                for n in funnel
                if n.replace("_", " ").title().lower() == str(label).lower()
                or str(label).lower().startswith(n.replace("_", " ").lower()[:6])
            )
            qualified = funnel[name]["qualified_apps"]
            if qualified > 0 and isinstance(cpqa, (int, float)):
                assert abs(cpqa - spend / qualified) < 0.01
                checked += 1
        assert checked >= 3

    def test_basis_is_labelled_under_the_table(self):
        ws = self._sheet(_german_plan(confidence=0.8))
        text = " ".join(str(c.value) for row in ws.iter_rows() for c in row if c.value)
        assert "CPQA = cost per qualified application" in text
        assert "same application-to-qualified rates as the Recruitment Funnel" in text


class TestShortPlanTimelineSlide:
    def test_two_week_plan_shows_two_phase_cards_and_one_arrow(self):
        data = _german_plan(confidence=0.8)
        data["campaign_weeks"] = 2
        slides = _deck_slide_texts(data)
        slide = next(
            s for s in slides if any("IMPLEMENTATION TIMELINE" in t for t in s)
        )
        joined = " ".join(slide)
        assert "Weeks 3-2" not in joined and "PHASE 3" not in joined
        assert "Week 1" in joined and "Week 2" in joined
        assert sum(t.count("▶") for t in slide) == 1


class TestBenchmarkGapFromTheIntlBenchmarksSheet:
    """A plan whose industry has no market range of its own is compared with the
    range the Intl Benchmarks sheet prints for its country (lead platform)."""

    def _plan(self, cpa_eur):
        data = _german_plan(cpa_eur=cpa_eur, confidence=0.8)
        data["industry"] = "automotive"  # no industry-level German range
        data["_intl_benchmarks"] = {
            "countries": {
                "germany": {
                    "name": "Germany",
                    "platforms": [
                        {"name": "StepStone", "cpa_usd": {"min": 10.9, "max": 54.5}},
                        {
                            "name": "Indeed Germany",
                            "cpa_usd": {"min": 5.45, "max": 38.15},
                        },
                    ],
                }
            }
        }
        return data

    def test_below_the_printed_range_is_flagged(self):
        # US$4.54: under every printed platform low (StepStone 10.9, Indeed 5.45)
        gap = plan_validator.cpa_benchmark_gap(self._plan(4.0))
        assert gap["status"] == "below"
        # A span of platform ranges is never called a market or industry benchmark.
        msg = plan_validator.benchmark_gap_message(gap)
        assert (
            "below every platform CPA range printed for Germany "
            "(lowest: Indeed Germany US$5.45)" in msg
        )
        assert "market's benchmark" not in msg and "industry benchmark" not in msg

    def test_inside_the_printed_range_is_not(self):
        gap = plan_validator.cpa_benchmark_gap(self._plan(20.0))
        assert gap["status"] in ("within", "channel_below")
