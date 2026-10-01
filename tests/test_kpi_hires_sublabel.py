"""Regression: the slide-2 "Projected Hires" KPI explains its range and sits
on the same baseline as its siblings; the slide-5 cost-per-hire row and its
sources line stay on one line (design-judge panel, 2026-10-01, item 4).

Pre-fix (branch at e7f7f67):
* the sublabel read "range 23–47" -- it did not say that 23 is the budget
  at the industry-average cost per hire and 47 the plan's own efficiency;
* "Projected Hires" and its sublabel shared one textbox placed at 0.66in
  into the bar, so the label sat ~0.06in (about 10px) above "Channels",
  "Avg CPA" and the other labels at 0.72in;
* a UK plan's slide-5 row "No local benchmark for this market" and an
  India plan's sources line (measured at 7pt although _set_font floors
  text at 8pt) both wrapped onto a second line.
"""

from __future__ import annotations

import io
import os
import sys

import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

import ppt_generator  # noqa: E402
import tools_regen_bundles as T  # noqa: E402
from pptx import Presentation  # noqa: E402

EMU = 914400
# 5 secondary metrics share 8.3in; 0.2in of side insets
_KPI_TEXT_W = 8.3 / 5 - 0.2


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


def _deck(data):
    return Presentation(io.BytesIO(ppt_generator.generate_pptx(dict(data))))


def _shape_with_text(slide, text):
    # matched on the FIRST paragraph so a label sharing a box with other
    # lines is still found (and its top compared)
    for sh in slide.shapes:
        if sh.has_text_frame and sh.text_frame.paragraphs[0].text == text:
            return sh
    raise AssertionError(f"no shape with text {text!r}")


def _sublabel(slide):
    for sh in slide.shapes:
        if sh.has_text_frame and " at plan " in sh.text_frame.text:
            return sh
    return None


def _kpi_bar(slide):
    # the navy band: 12.2in wide, starts at 5.35in
    for sh in slide.shapes:
        if (
            abs(sh.top / EMU - 5.35) < 0.01
            and abs(sh.width / EMU - 12.2) < 0.01
            and sh.height / EMU > 1.0
        ):
            return sh
    raise AssertionError("KPI bar not found")


@pytest.fixture(scope="module")
def hospital():
    data = T.build_plan_data(_brief(
        "Probe Health", "Healthcare & Medical", "$250,000", ["Dallas, TX"],
        ["Registered Nurse", "Pharmacist"], "100 hires"))
    return data, _deck(data)


@pytest.fixture(scope="module")
def india_it():
    data = T.build_plan_data(_brief(
        "Probe Infotech", "tech_engineering", "₹25,000,000",
        ["Bengaluru, Karnataka, India", "Pune, Maharashtra, India"],
        ["Java Developer", "QA Engineer"], "500+ hires"))
    return data, _deck(data)


@pytest.fixture(scope="module")
def uk_finance():
    data = T.build_plan_data(_brief(
        "Probe Bank", "finance_banking", "£150,000", ["London, UK"],
        ["Financial Analyst"], "50 hires"))
    return data, _deck(data)


class TestProjectedHiresKpi:
    def test_label_shares_the_sibling_baseline(self, hospital):
        s2 = hospital[1].slides[1]
        tops = {
            lbl: _shape_with_text(s2, lbl).top
            for lbl in ("Channels", "Projected Hires", "Avg CPA")
        }
        assert len(set(tops.values())) == 1, {k: v / EMU for k, v in tops.items()}

    def test_sublabel_says_what_each_end_assumes(self, hospital):
        data, prs = hospital
        tp = data["_budget_allocation"]["total_projected"]
        assert (tp["hires_low"], tp["hires"], tp["hires_high"]) == (23, 47, 47)
        s2 = prs.slides[1]
        # headline unchanged for a US plan
        assert _shape_with_text(s2, "47")
        sub = _sublabel(s2)
        assert sub is not None
        lines = [p.text for p in sub.text_frame.paragraphs]
        assert lines == ["23 at industry-avg cost", "47 at plan efficiency"], lines
        for line in lines:
            assert ppt_generator._estimate_lines(line, _KPI_TEXT_W, 8.0) == 1, line

    def test_sublabel_sits_below_the_label_inside_the_bar(self, hospital):
        s2 = hospital[1].slides[1]
        label = _shape_with_text(s2, "Projected Hires")
        sub = _sublabel(s2)
        bar = _kpi_bar(s2)
        assert sub.top >= label.top + int(0.2 * EMU)
        assert sub.top + sub.height <= bar.top + bar.height

    def test_local_basis_is_named_local(self, india_it):
        sub = _sublabel(india_it[1].slides[1])
        lines = [p.text for p in sub.text_frame.paragraphs]
        assert lines == ["434 at local-avg cost", "869 at plan efficiency"], lines

    def test_suppressed_plan_has_no_range(self, uk_finance):
        s2 = uk_finance[1].slides[1]
        assert _sublabel(s2) is None
        # its bar keeps the standard depth
        assert abs(_kpi_bar(s2).height / EMU - 1.15) < 0.005


class TestOneLineRows:
    def test_uk_cph_row_is_one_line(self, uk_finance):
        texts = [
            sh.text_frame.text
            for sh in uk_finance[1].slides[4].shapes
            if sh.has_text_frame
        ]
        val = texts[texts.index("Industry Cost-per-Hire") + 1]
        assert val == "No local benchmark", val
        assert ppt_generator._estimate_lines(val, 2.3, 10.0) == 1

    def test_india_sources_line_is_one_line_at_the_rendered_size(self, india_it):
        for sh in india_it[1].slides[4].shapes:
            if sh.has_text_frame and sh.text_frame.text.startswith("Sources:"):
                txt = sh.text_frame.text
                assert "Shework 2026 (India)" in txt, txt
                width = sh.width / EMU - 0.2
                assert ppt_generator._estimate_lines(txt, width, 8.0) == 1, txt
                return
        raise AssertionError("no Sources line on slide 5")


def test_wide_column_keeps_one_line_with_both_ends():
    lines = ppt_generator._hires_range_sublabel({}, (23, 47), 4.0)
    assert lines == ["23 at industry-average cost · 47 at plan efficiency"]
