"""Provenance honesty for the client-facing "Recruitment Benchmarks" table.

``data/recruitment_benchmarks_deep.json`` -> ``industry_benchmarks`` (22 rows)
is printed verbatim, key by key, into every client workbook's Executive
Summary and feeds deck slide 5. The figures are hand-typed working estimates;
the 2026-10-17 audit (ledger R_kb_industry_benchmarks_sources.md) found four
attributions that do not hold and one media-only figure printed as "Industry
Cost-per-Hire". Rule: a client-facing figure traces to a real artifact or
carries an explicit label.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import budget_engine as be  # noqa: E402
import excel_v2  # noqa: E402
import ppt_generator as ppt  # noqa: E402
from kb_loader import load_knowledge_base  # noqa: E402

_PATH = PROJECT_ROOT / "data" / "recruitment_benchmarks_deep.json"
_RAW = _PATH.read_text(encoding="utf-8")
_DATA = json.loads(_RAW)
_ROWS = _DATA["industry_benchmarks"]
_WP_PATH = PROJECT_ROOT / "data" / "industry_white_papers.json"
_WP_RAW = _WP_PATH.read_text(encoding="utf-8")
_WP = json.loads(_WP_RAW)


def test_every_row_ends_with_provenance():
    assert len(_ROWS) == 22
    for name, row in _ROWS.items():
        # LAST key: nova's deep search truncates objects to their first keys,
        # so the note must never displace real data.
        assert list(row)[-1] == "provenance", name
        # Disclaimer FIRST: the workbook cell wraps without a row height,
        # so only the first ~90 characters are visible.
        assert row["provenance"].startswith("Not independently audited"), name
        assert "Reviewed 2026-10-17" in row["provenance"], name


def test_verified_facts_are_stamped_on_their_rows():
    assert "3.22% (Appcast 2026)" in _ROWS["healthcare_medical"]["provenance"]
    assert "7.14% (Appcast 2026)" in _ROWS["technology_engineering"]["provenance"]
    for name in ("blue_collar_skilled_trades", "construction_real_estate"):
        assert "439,000 construction workers" in _ROWS[name]["provenance"], name
        assert "ABC, Jan 2025" in _ROWS[name]["provenance"], name


def test_misattributed_figures_are_gone():
    for banned in (
        "Deloitte",
        "16,789",
        "20,707",
        "Recruitonomics",
        "22,750",
        "$1,364",
        "$169",
        "22.3%",
        "289,000",
        "skilled craft",
    ):
        assert banned not in _RAW, banned


def test_appcast_attribution_only_on_apply_rates_in_healthcare_and_tech():
    for name in ("healthcare_medical", "technology_engineering"):
        for metric in ("cpa", "cpc", "cph"):
            blob = json.dumps(_ROWS[name][metric])
            assert "Appcast" not in blob, (name, metric)
            assert "appcast" not in blob, (name, metric)


def test_no_appcast_tag_on_any_cost_object():
    """cpa / cpc / cph hold dollar figures; Appcast may only tag apply rates."""
    for name, row in _ROWS.items():
        for metric in ("cpa", "cpc", "cph"):
            blob = json.dumps(row.get(metric, {})).lower()
            assert "appcast" not in blob, (name, metric)


def test_removed_unverified_cost_per_stage_figures():
    for name in ("healthcare_medical", "technology_engineering"):
        cph = _ROWS[name]["cph"]
        for key in ("cost_per_screen", "cost_per_interview", "cost_per_offer"):
            assert key not in cph, (name, key)
    for banned in ("$156", "$286", "$773"):
        assert banned not in _RAW, banned


def test_nsi_figures_are_the_2026_edition():
    q = _ROWS["healthcare_medical"]["quality_of_hire"]
    assert q["first_year_rn_turnover"] == "22.7%"
    assert "$294,976" in q["cost_of_1pct_rn_turnover"]
    assert "$60,090" in q["cost_of_one_rn_turnover"]
    assert "78" in q["rn_time_to_fill"]
    assert "NSI 2026" in q["notes"]


def test_industry_cph_is_all_in_for_telecom_and_technology():
    kb = load_knowledge_base()
    telecom = be._kb_industry_cph_row(kb, "telecommunications")
    tech = be._kb_industry_cph_row(kb, "technology_engineering")
    assert telecom["text"].startswith("$5,000-$8,000")
    assert tech["text"].startswith("$6,000-$7,000")
    assert "$800-$1,500" not in telecom["text"]
    assert "$2,795" not in tech["text"]
    ib = kb["recruitment_benchmarks"]["industry_benchmarks"]
    # The deck's slide-5 picker resolves the same all-in range.
    assert ppt._kb_extract_range(ib["telecommunications"]["cph"]) == telecom["text"]
    assert ppt._kb_extract_range(ib["technology_engineering"]["cph"]) == tech["text"]


def test_sources_list_is_corrected():
    sources = {s["name"]: s for s in _DATA["sources"]}
    appcast_2026 = next(s for n, s in sources.items() if n.startswith("Appcast 2026"))
    assert "2026-recruitment-marketing-benchmarks-report" in appcast_2026["url"]
    rising = next(
        s for s in _DATA["sources"] if s.get("id") == "appcast_rising_cost_2026"
    )
    assert rising["url"] == "https://www.appcast.io/the-rising-cost-of-recruiting"
    nsi = next(s for n, s in sources.items() if n.startswith("NSI"))
    assert nsi["publication_year"] == 2026
    assert nsi["url"].endswith("nsi_national_health_care_retention_report.pdf")
    shrm = next(s for n, s in sources.items() if "$4,129" in n)
    assert "FY2015" in shrm["name"] and "one national average" in shrm["name"]
    assert not any("Deloitte" in n for n in sources)
    assert _DATA["last_updated"] == "2026-03-22"


@pytest.mark.parametrize(
    "industry,low,high",
    [("telecommunications", 5000, 8000), ("technology_engineering", 6000, 7000)],
)
def test_hires_range_sentence_has_one_bracket_and_parses(industry, low, high):
    """excel_v2._hires_range_line nests the industry row's single bracketed
    note via ^(.*?)\\s*\\((.*)\\)$; a two-bracket cost string garbled the
    Executive Summary sentence on every telecom plan."""
    row = be._kb_industry_cph_row(load_knowledge_base(), industry)
    assert row["low"] == low and row["high"] == high
    assert row["mid"] == (low + high) / 2
    cph_info = {
        "basis": "industry",
        "currency": "USD",
        "value": row["mid"],
        "floor": row["mid"] / 2,
        "display_row": row,
    }
    total_proj = {"hires_low": 53, "hires_high": 116}
    sentence = excel_v2._hires_range_line(total_proj, cph_info, 80, 500000.0)
    assert sentence, "builder returned no sentence"
    head = sentence.split("at plan efficiency")[0]
    assert head.count("(") == 1 and head.count(")") == 1, head
    assert f"${low:,}" in head and f"${high:,}" in head, head
    assert f"${row['mid']:,.0f}" in head, head


def test_white_paper_nova_source_drops_removed_figures():
    """nova.py reads data/industry_white_papers.json; the figures removed from
    the KB must not come back through the chat path."""
    assert "doubled" not in _WP_RAW
    assert "$2,795" not in _WP_RAW
    assert "$1,085" not in _WP_RAW
    bm = _WP["reports"]["appcast_benchmark_2026"]["benchmarks"]
    assert "technology" not in bm["cph_by_occupation_2025"]
    for key in (
        "cost_per_screen_by_occupation_2025",
        "cost_per_interview_by_occupation_2025",
        "cost_per_offer_by_occupation_2025",
    ):
        assert "healthcare" not in bm[key], key
    # $169 / $366 also occur as OTHER occupations' / a 2022 report's figures,
    # so they are checked against the removed healthcare slots only.
    assert "$169" not in json.dumps(bm["cost_per_screen_by_occupation_2025"])
    assert "$366" not in json.dumps(bm["cost_per_interview_by_occupation_2025"])
