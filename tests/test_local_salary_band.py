"""A local salary is ONE published, role-matched band that a client can find
on its cited page -- never a span across statistics, never a figure the page
does not state, never a midpoint labelled "median".

Design-judge round 1 (2026-10-01): the UK finance deck printed "£39,000 -
£49,983 (GBP) local benchmark" -- an all-occupations median paired with a
category average. Round 2: India's "₹500K median (₹400K-₹600K)" was the
arithmetic midpoint of a KB note ("Entry: ₹4-6 LPA") that the cited page
(plugscale.com, lowest figure "₹5 lakh to ₹25 lakh") does not contain. An
audit of all 35 rows the resolver could return kept only the 6 whose cited
page states the band (intl_benchmark_lookup._AUDITED_LOCAL_BANDS).
"""

from __future__ import annotations

import io

import pytest
from openpyxl import load_workbook
from pptx import Presentation

import cited_data_block
import data_synthesizer
import excel_v2
import gold_standard as gs
import intl_benchmark_lookup as ibl
import ppt_generator
import research as research_mod
import tools_regen_bundles as T

_IE = ["Dublin, Ireland"]
_IE_ROLES = ["Clinical Nurse Specialist", "Software Engineer", "Public Health Nurse", "Data Analyst"]
_INDIA = ["Bengaluru, Karnataka, India"]

# Rows the round-2 audit dropped: the cited page does not state the band,
# the citation is for something else, or the page is unreachable.
_DROPPED_SOURCE_HOSTS = (
    "plugscale.com", "shework.in", "rcn.org.uk", "reed.com", "linkedin.com",
    "asanify.com", "eurodev.com", "workstaff360.com", "mexicobusiness.news",
    "mavenside.co", "huduri.com", "alcor.com", "rfsonshr.com", "adzuna.com",
    "melmc.edu.au", "gitnux.org", "hiringlab.org",
)


def test_audited_rows_carry_their_evidence():
    assert len(ibl._AUDITED_LOCAL_BANDS) == 6
    for key, row in ibl._AUDITED_LOCAL_BANDS.items():
        assert row["url"].startswith("https://"), key
        assert row["quote"] and row["retrieved"] == "2026-10-01", key
        assert 0 < row["low"] < row["high"], key
        if row["median"] is not None:
            assert row["low"] <= row["median"] <= row["high"], key
        assert not any(h in row["url"] for h in _DROPPED_SOURCE_HOSTS), key


@pytest.mark.parametrize(
    "location,role",
    [
        ("Bengaluru, Karnataka, India", "Software Engineer (Fresher)"),
        ("Bengaluru, Karnataka, India", "Java Developer"),
        ("Bangalore, India", "Registered Nurse"),
        ("London, UK", "Registered Nurse"),
        ("London, UK", "Warehouse Associate"),
        ("Toronto, Canada", "Registered Nurse"),
        ("Sydney, Australia", "Registered Nurse"),
        ("Dubai, UAE", "Registered Nurse"),
        ("Berlin, Germany", "Warehouse Associate"),
        ("Mexico City, Mexico", "Software Engineer"),
        ("Tokyo, Japan", "CFO"),
        ("Columbus, OH", "Registered Nurse"),
    ],
)
def test_unverified_or_us_rows_return_nothing(location, role):
    assert ibl.get_local_role_salary_band(location, role) is None


def test_band_is_exactly_the_audited_row():
    cns = ibl.get_local_role_salary_band("Dublin, Ireland", "Clinical Nurse Specialist")
    assert (cns["low"], cns["high"], cns["median"]) == (55_000, 65_000, None)
    assert cns["median_stated"] is False and cns["midpoint"] == 60_000
    assert cns["source_short"] == "frsrecruitment.com"
    swe = ibl.get_local_role_salary_band("Dublin, Ireland", "Software Engineer")
    assert (swe["low"], swe["high"], swe["median"]) == (36_000, 77_000, 51_000)
    assert swe["median_stated"] is True


def test_median_word_only_when_the_page_states_a_median():
    cns = ibl.get_local_role_salary_band("Dublin, Ireland", "Clinical Nurse Specialist")
    text = ibl.format_local_band(cns, "EUR", compact=True)
    assert text == (
        "€60K midpoint of published band (€55K-€65K) - Clinical Nurse Specialist "
        "[frsrecruitment.com]"
    )
    assert " median " not in text
    swe = ibl.get_local_role_salary_band("Dublin, Ireland", "Software Engineer")
    assert ibl.format_local_band(swe, "EUR", compact=True) == (
        "€51K median (€36K-€77K) - Software Engineer [payscale.com]"
    )


def test_salary_range_text_and_cited_line_share_the_decision():
    india = {"locations": _INDIA, "roles": ["Software Engineer (Fresher)"]}
    assert ppt_generator._local_salary_range_text(india) == "Local salary data n/a"
    assert cited_data_block.build_cited_2026_block(india)["salary_line"] == ""
    ie = {"locations": _IE, "roles": _IE_ROLES}
    assert "midpoint of published band (€55K-€65K)" in ppt_generator._local_salary_range_text(ie)
    assert "source: FRS Recruitment" in cited_data_block.build_cited_2026_block(ie)["salary_line"]


def test_market_and_quality_intelligence_carry_the_same_band():
    data = {"roles": _IE_ROLES, "locations": _IE, "industry": "healthcare"}
    synth = data_synthesizer.synthesize({}, {}, data)
    mi = synth["salary_intelligence"]["Clinical Nurse Specialist"]
    assert (mi["min"], mi["max"]) == (55_000, 65_000)
    assert mi["median_stated"] is False and mi["currency"] == "EUR"
    assert mi["sources"][0].startswith("FRS Recruitment")
    data["_synthesized"] = synth
    for path_data in (data, {"roles": _IE_ROLES, "locations": _IE}):
        info = next(iter(gs.enrich_city_level_data(path_data).values()))
        row = info["per_role_salary"]["Clinical Nurse Specialist"]
        assert (row["min"], row["max"], row["median_stated"]) == (55_000, 65_000, False)
        assert row["source"] == mi["sources"][0]
        swe = info["per_role_salary"]["Software Engineer"]
        assert (swe["median"], swe["median_stated"]) == (51_000, True)
        assert info["per_role_salary"]["Data Analyst"]["local_salary_na"] is True


@pytest.fixture(scope="module")
def ireland_bundle():
    brief = {
        "client_name": "Liffey Health Group",
        "budget": "€400,000",
        "campaign_duration": "3-6 months",
        "industry": "Healthcare",
        "locations": _IE,
        "roles": _IE_ROLES,
        "target_roles": [{"title": r, "count": 10} for r in _IE_ROLES],
    }
    data = T.build_plan_data(brief)  # no synthesis: the tools_regen path
    xlsx = excel_v2.generate_excel_v2(dict(data), research_mod=research_mod)
    pptx = ppt_generator.generate_pptx(dict(data))
    xlsx = xlsx.getvalue() if hasattr(xlsx, "getvalue") else xlsx
    pptx = pptx.getvalue() if hasattr(pptx, "getvalue") else pptx
    return {"xlsx": xlsx, "pptx": pptx}


def test_deck_prints_band_or_stated_median_never_a_midpoint_median(ireland_bundle):
    prs = Presentation(io.BytesIO(ireland_bundle["pptx"]))
    slides = [
        [sh.text_frame.text for sh in s.shapes if sh.has_text_frame] for s in prs.slides
    ]
    slide2 = " ".join(slides[1])
    assert "€60K midpoint of published band (€55K-€65K) - Clinical Nurse Specialist" in slide2
    rb = next(t for t in slides if "Role Breakdown" in t)
    assert "Salary Benchmark" in rb and "Est. Median Salary" not in rb
    assert "€55K-€65K band" in rb  # CNS: no published median
    assert "€51K median" in rb  # Software Engineer: PayScale states a median
    assert not any(t.endswith("median") and t.startswith("€60K") for t in rb)


def test_workbook_median_cell_only_for_a_stated_median(ireland_bundle):
    ws = load_workbook(io.BytesIO(ireland_bundle["xlsx"]))["Quality Intelligence"]
    rows = {
        r[2].value: [c.value for c in r[3:8]]
        for r in ws.iter_rows()
        if len(r) > 8 and r[1].value == "Dublin"
    }
    cns = rows["Clinical Nurse Specialist"]
    assert cns == [55_000, "—", "—", "—", 65_000], cns
    swe = rows["Software Engineer"]
    assert swe == [36_000, "—", 51_000, "—", 77_000], swe
