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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import budget_engine as be  # noqa: E402
import ppt_generator as ppt  # noqa: E402
from kb_loader import load_knowledge_base  # noqa: E402

_PATH = PROJECT_ROOT / "data" / "recruitment_benchmarks_deep.json"
_RAW = _PATH.read_text(encoding="utf-8")
_DATA = json.loads(_RAW)
_ROWS = _DATA["industry_benchmarks"]


def test_every_row_ends_with_provenance():
    assert len(_ROWS) == 22
    for name, row in _ROWS.items():
        # LAST key: nova's deep search truncates objects to their first keys,
        # so the note must never displace real data.
        assert list(row)[-1] == "provenance", name
        assert "not independently audited" in row["provenance"], name
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
    rising = next(s for s in _DATA["sources"] if s.get("id") == "appcast_rising_cost_2026")
    assert rising["url"] == "https://www.appcast.io/the-rising-cost-of-recruiting"
    nsi = next(s for n, s in sources.items() if n.startswith("NSI"))
    assert nsi["publication_year"] == 2026
    assert nsi["url"].endswith("nsi_national_health_care_retention_report.pdf")
    shrm = next(s for n, s in sources.items() if "$4,129" in n)
    assert "FY2015" in shrm["name"] and "one national average" in shrm["name"]
    assert not any("Deloitte" in n for n in sources)
    assert _DATA["last_updated"] == "2026-03-22"
