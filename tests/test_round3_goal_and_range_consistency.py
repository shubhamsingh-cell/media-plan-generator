"""Regression: design-judge round 3 (2026-10-01) on mpg-projection-honesty.

Pre-fix (branch at 9bec63d):
1. India (714 hires at plan efficiency vs a 500 goal) lost its goal
   statement: the slide-9 goal band rendered only when hires were UNDER
   goal, and the slide-2 "... buys 434 hires against a goal of 500" line was
   generated and then trimmed by _autofit_textframe.
2. Workbook ranges disagreed internally: India's Confidence Intervals hires
   summed to 533-891 (891 = below the plan's floor) and hospital's to 35-56,
   while the Executive Summary range said 434-714 / 23-47.
3. US decks never defined "plan efficiency" (= half the range midpoint).
4. The media-only note printed on markets with no cost-per-hire range.
5. The KPI sublabel said "local-avg cost" for a range midpoint and sat
   0.06in from the band edge.
6. The workbook's KB cost-per-hire row printed "Recruitment Marketing
   Only: $350-$700" beside the all-in basis statement.
"""

from __future__ import annotations

import io
import os
import re
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import excel_v2  # noqa: E402
import ppt_generator  # noqa: E402
import tools_regen_bundles as T  # noqa: E402
from kb_loader import load_knowledge_base  # noqa: E402
from openpyxl import load_workbook  # noqa: E402
from pptx import Presentation  # noqa: E402
from pptx.util import Inches, Pt  # noqa: E402

EMU = 914400
_MEDIA = "Plan budget covers media spend"


def _brief(name, industry, budget, locations, roles, hire_volume="Not specified"):
    return {
        "client_name": name,
        "industry": industry,
        "budget": budget,
        "campaign_duration": "6 months",
        "hire_volume": hire_volume,
        "work_environment": "onsite",
        "locations": locations,
        "roles": roles,
        "target_roles": roles,
    }


class _Bundle:
    def __init__(self, brief):
        self.data = T.build_plan_data(brief)
        self.prs = Presentation(io.BytesIO(ppt_generator.generate_pptx(dict(self.data))))
        raw = excel_v2.generate_excel_v2(dict(self.data), load_kb_fn=load_knowledge_base)
        raw = raw[0] if isinstance(raw, tuple) else raw
        self.wb = load_workbook(io.BytesIO(raw))

    @property
    def tp(self):
        return self.data["_budget_allocation"]["total_projected"]

    def paras(self, n):
        return [
            p.text
            for sh in self.prs.slides[n - 1].shapes
            if sh.has_text_frame
            for p in sh.text_frame.paragraphs
            if p.text.strip()
        ]

    def texts(self, sheet):
        return [
            str(c.value) for r in self.wb[sheet].iter_rows() for c in r
            if isinstance(c.value, str)
        ]


@pytest.fixture(scope="module")
def india():
    return _Bundle(_brief(
        "Probe Infotech", "tech_engineering", "₹25,000,000",
        ["Bengaluru, Karnataka, India", "Pune, Maharashtra, India"],
        ["Java Developer", "QA Engineer"], "500+ hires"))


@pytest.fixture(scope="module")
def india_full():
    """The design-judge sweep brief (g_india_it): six roles, three metros,
    two campaign goals -- enough content to overfill the RESOLUTION card."""
    roles = [
        "Software Engineer (Fresher)", "Java Developer", "QA Engineer",
        "Senior Data Engineer (Lateral)", "DevOps Engineer", "Business Analyst",
    ]
    brief = _brief(
        "Nimbus Infotech Services", "tech_engineering", "₹25,000,000",
        ["Bengaluru, Karnataka, India", "Hyderabad, Telangana, India",
         "Pune, Maharashtra, India"],
        roles, "500+ hires")
    brief["campaign_goals"] = ["Direct Hiring", "Talent Pipeline"]
    brief["campaign_duration"] = "6-12 months"
    brief["work_environment"] = ["Hybrid", "On-site"]
    return _Bundle(brief)


@pytest.fixture(scope="module")
def hospital():
    return _Bundle(_brief(
        "Probe Health", "Healthcare & Medical", "$250,000", ["Dallas, TX"],
        ["Registered Nurse", "Pharmacist"], "100 hires"))


@pytest.fixture(scope="module")
def uk():
    return _Bundle(_brief(
        "Probe Bank", "finance_banking", "£150,000", ["London, UK"],
        ["Financial Analyst"], "50 hires"))


@pytest.fixture(scope="module")
def hershey():
    return _Bundle(_brief(
        "Probe Foods", "food_beverage", "$45,000", ["Hershey, PA", "Reading, PA"],
        ["Machine Operator", "Sanitation"], "100-500 hires"))


class TestGoalStatementSurvives:
    def test_autofit_never_trims_the_client_goal_line(self):
        prs = Presentation()
        slide = prs.slides.add_slide(prs.slide_layouts[6])
        box = slide.shapes.add_textbox(Inches(1), Inches(1), Inches(3), Inches(1))
        tf = box.text_frame
        lines = ["MARKET THESIS"] + [f"filler line {i}" for i in range(14)] + [
            "Client goal: 500 hires — met at plan efficiency (714); at the "
            "midpoint cost the plan buys 434 hires against a goal of 500",
            "2026 Market Data:",
            "cited metric one",
        ]
        for i, text in enumerate(lines):
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            r = p.add_run()
            r.text = text
            r.font.size = Pt(9)
        ppt_generator._autofit_textframe(tf, 3.0, 1.2)
        kept = [p.text for p in tf.paragraphs]
        assert any(t.startswith("Client goal: 500 hires") for t in kept), kept
        assert "cited metric one" not in kept  # trailing filler still goes

    def test_india_slide2_keeps_the_conservative_goal_line(self, india_full):
        # round 4 (F1): one break-even phrasing on every surface
        assert any(
            p.startswith("Client goal: 500 hires — this plan projects 434–714; "
                         "goal met if hires cost ≤ ₹50K each")
            for p in india_full.paras(2)
        ), india_full.paras(2)

    def test_india_comparison_band_shows_the_range_against_the_goal(self, india):
        # the Plan Comparison slide (slide 9 on the sweep brief; its index
        # moves with optional slides, so find it by its band)
        band = [
            p
            for n in range(1, len(india.prs.slides) + 1)
            for p in india.paras(n)
            if p.startswith("CLIENT GOAL")
        ]
        assert band, "no CLIENT GOAL band on any slide"
        # round 4 (F1): the break-even is budget ÷ goal (₹25M ÷ 500 = ₹50K),
        # not the plan's own ₹35K cost per hire
        assert (
            "500 hires target  —  this plan projects 434–714; "
            "goal met if hires cost ≤ ₹50K each" in band[0]
        ), band[0]


def _sheet_hires(wb):
    ci = [[c.value for c in r] for r in wb["Confidence Intervals"].iter_rows()]
    lo = sum(int(v[3]) for v in ci if len(v) > 5 and v[2] == "Hires")
    ex = sum(int(v[4]) for v in ci if len(v) > 5 and v[2] == "Hires")
    hi = sum(int(v[5]) for v in ci if len(v) > 5 and v[2] == "Hires")
    roi_lo = roi_hi = 0
    for r in wb["ROI Projections"].iter_rows():
        for c in r:
            m = re.match(r"^(\d+) - (\d+)$", str(c.value or ""))
            if m:
                roi_lo += int(m.group(1))
                roi_hi += int(m.group(2))
    return (lo, ex, hi), (roi_lo, roi_hi)


class TestWorkbookRangesAgree:
    @pytest.mark.parametrize("name", ["india", "hospital"])
    def test_confidence_intervals_and_roi_add_up_to_the_plan_range(self, name, request):
        b = request.getfixturevalue(name)
        tp = b.tp
        ci, roi = _sheet_hires(b.wb)
        assert ci == (tp["hires_low"], tp["hires"], tp["hires_high"]), ci
        assert roi == (tp["hires_low"], tp["hires_high"]), roi
        b16 = [t for t in b.texts("Executive Summary") if "Projected hires:" in t]
        assert f"Projected hires: {tp['hires_low']:,}–{tp['hires_high']:,}." in b16[0]


class TestPlanEfficiencyDefined:
    def test_us_footnotes_define_it(self, hospital):
        for n in (2, 5):
            assert any(
                p.startswith("Plan efficiency: $5,250/hire (half the $10.5K midpoint)")
                for p in hospital.paras(n)
            ), (n, hospital.paras(n)[-4:])


class TestMediaNoteOnlyWithARange:
    def test_absent_without_a_cost_per_hire_range(self, uk):
        for n in (2, 5):
            assert not any(_MEDIA in p for p in uk.paras(n)), n
        assert not any("media spend" in t for t in uk.texts("Executive Summary"))

    def test_present_with_one(self, hospital):
        assert any(_MEDIA in p for p in hospital.paras(2))


class TestKpiQualifier:
    def test_range_midpoint_wording_and_clearance(self, india):
        s2 = india.prs.slides[1]
        sub = next(
            sh for sh in s2.shapes
            if sh.has_text_frame and sh.text_frame.paragraphs[0].text.startswith("434 at ")
        )
        assert [p.text for p in sub.text_frame.paragraphs] == [
            "434 at range midpoint", "714 at plan efficiency"]
        bar = next(
            sh for sh in s2.shapes
            if abs(sh.top / EMU - 5.35) < 0.01 and abs(sh.width / EMU - 12.2) < 0.01
            and sh.height / EMU > 1.0
        )
        # >= 0.12in (1 EMU of rounding slack)
        assert (bar.top + bar.height) - (sub.top + sub.height) >= int(0.12 * EMU) - 1


class TestMediaOnlyKbRowLabelled:
    def test_no_bare_marketing_only_figure(self, hershey):
        # round 4 (F5): the row is now the ONE industry row the deck shows
        # (no media-only or sister-sector figure in the cell at all)
        texts = hershey.texts("Executive Summary")
        assert not any("Recruitment Marketing Only" in t for t in texts)
        rows = [r for r in hershey.wb["Executive Summary"].iter_rows()]
        cph = [
            [c.value for c in r if c.value]
            for r in rows
            if any(c.value == "Industry Cost-per-Hire" for c in r)
        ]
        assert cph and cph[0][1] == "$3,000-$5,000 (food manufacturing)", cph
