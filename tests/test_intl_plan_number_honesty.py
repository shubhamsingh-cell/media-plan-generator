"""Plan numbers on non-US plans (international audit 2026-10-18).

Where a non-US plan printed a US figure as its own, or two surfaces of one
workbook disagreed.  Every test pairs a non-US plan with a US control that must
stay as it was:

  #3  Ad Platform table: static US$ constants wore the plan's symbol ("INR 5.81")
  #4  Median Salary: US$-magnitude numbers printed as local currency ("VND 5,000")
  #6  Niche / Employer Branding CPC: a US benchmark with no local board behind it
  #7  Hires "not projected" on the Executive Summary, yet printed elsewhere
  #9  LinkedIn "Apply Rates: <country>" that was the global record (and "ON")
  #11 TikTok in an India workbook / deck (the block itself is in
      ``test_platform_market_blocks.py``; #8 is in
      ``test_usd_plan_abroad_cost_per_hire.py``)

Plans are built headless through ``tests/intl_plan_fixture.py`` (the same
pipeline ``/api/generate`` runs, minus live enrichment).
"""

from __future__ import annotations

import copy
import io
import json
import re
import sys
from pathlib import Path
from typing import Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import openpyxl  # noqa: E402

import excel_v2  # noqa: E402
import gold_standard as gs  # noqa: E402
import ppt_generator  # noqa: E402
import plan_currency  # noqa: E402
import research  # noqa: E402
from tests.intl_plan_fixture import (
    _bundle,
    _sheet_text,
    _workbook_text,
    _row_cells,
)  # noqa: E402


AD_TABLE_NOTE = "US$ (US benchmark)"

NICHE_LABEL = "US benchmark estimate, no local niche board identified"

BRANDING_LABEL = "US benchmark estimate"


@pytest.mark.parametrize("key", ["in", "jp", "uk"])
def test_ad_platform_benchmarks_carry_the_us_dollar_marker_on_a_non_usd_plan(key):
    wb = _bundle(key)["wb"]
    google = _row_cells(wb["Channels & Strategy"], "Google Ads")
    assert google, "Ad Platform table has no Google Ads row"
    for cell in google[2:5]:  # CPC, CPM, CPA
        assert '"US$"' in cell.number_format, (key, cell.coordinate, cell.number_format)
    # the value is the US$ constant, never converted into the plan's currency
    assert google[2].value == 5.81
    text = _sheet_text(wb, "Channels & Strategy")
    assert AD_TABLE_NOTE in text


def test_ad_platform_table_on_a_usd_plan_in_a_non_us_market_is_labelled():
    wb = _bundle("vn")["wb"]
    google = _row_cells(wb["Channels & Strategy"], "Google Ads")
    assert google and google[2].number_format == '"$"#,##0.00'
    assert AD_TABLE_NOTE in _sheet_text(wb, "Channels & Strategy")


def test_us_plan_ad_platform_table_is_unchanged():
    wb = _bundle("us")["wb"]
    google = _row_cells(wb["Channels & Strategy"], "Google Ads")
    assert google and google[2].number_format == '"$"#,##0.00'
    assert AD_TABLE_NOTE not in _sheet_text(wb, "Channels & Strategy")


_LOCAL_SYMBOLS = re.compile(r"[₫₹¥£€]|R\$|MX\$|AED|A\$|C\$|NZ\$|AR\$")


def test_every_country_median_salary_is_printed_in_its_stated_currency():
    """Each non-US row states its basis: a national statistics figure in the
    country's own currency (printed with that currency's symbol and its
    source), or an unsourced US$ estimate (printed as "No local benchmark",
    never as the market's median and never with a local symbol)."""
    checked = sourced = 0
    for country, row in research.COUNTRY_DATA.items():
        if country == "United States":
            continue
        ctx = research.get_labour_market_intelligence("general_entry_level", [country])
        lc = ctx["location_contexts"][0]
        if lc.get("no_country_data"):
            continue
        shown = lc["median_salary"]
        entry = research.median_salary_entry(country)
        if row.get("median_salary_source"):
            assert entry, country
            assert row["median_salary_currency"] == row["currency"] == entry["currency"]
            symbol = plan_currency.symbol_for_code(row["median_salary_currency"])
            assert shown.startswith(symbol), (country, shown)
            assert entry["publisher_short"] in shown, (country, shown)
            assert "US$" not in shown and "(est.)" not in shown, (country, shown)
            assert lc["median_salary_basis"] == research.MEDIAN_SALARY_BASIS_SOURCED
            sourced += 1
        else:
            assert row["median_salary_currency"] == "USD", country
            assert shown == research.NO_LOCAL_SALARY_BENCHMARK, (country, shown)
            assert lc["median_salary_basis"] == research.MEDIAN_SALARY_BASIS
        checked += 1
    assert checked >= 35 and sourced == 16, (checked, sourced)


def test_us_row_keeps_its_dollar_figure():
    lc = research.get_labour_market_intelligence("tech_engineering", ["Chicago"])[
        "location_contexts"
    ][0]
    assert lc["median_salary"].startswith("$") and "USD" in lc["median_salary"]


def _anchors() -> Dict[str, dict]:
    raw = json.loads(
        (PROJECT_ROOT / "data" / "international_benchmarks_2026.json").read_text(
            encoding="utf-8"
        )
    )
    return raw["countries"]


_WAGE_ANCHORS = [
    ("Vietnam", "vietnam", "VND 8.4M/month", 8_400_000 * 12),
    ("Mexico", "mexico", "MXN 18,750/month", 18_750 * 12),
    ("Brazil", "brazil", "R$ 3,560/month", 3_560 * 12),
]


@pytest.mark.parametrize("country,key,needle,annual_local", _WAGE_ANCHORS)
def test_unsourced_median_salary_has_us_dollar_magnitude(
    country, key, needle, annual_local
):
    """An unsourced row is a US$ estimate: it states USD, sits within 2x of the
    sourced national wage converted to US$, and read as local currency it would
    be far below the same source (Vietnam VND 5,000 vs VND 100 million a year)."""
    entry = _anchors()[key]
    anchors = json.dumps(entry["official_wage_anchors"], ensure_ascii=False)
    assert needle in anchors, f"{country}: anchor text moved ({needle})"
    row = research.COUNTRY_DATA[country]
    assert row["median_salary_currency"] == "USD" and not row.get("median_salary_source")
    usd_annual = annual_local * float(entry["usd_rate"])
    figure = float(row["median_salary"])
    assert usd_annual / 2 <= figure <= usd_annual * 2, (country, figure, usd_annual)
    assert figure < annual_local * 0.3, (country, figure, annual_local)


def test_singapore_median_is_the_sourced_local_figure_its_anchor_quotes():
    """Singapore's row is the MOM figure in S$ (the same S$5,775 a month the
    benchmark file's official wage anchor quotes), no longer a US$ estimate."""
    anchors = json.dumps(_anchors()["singapore"]["official_wage_anchors"])
    assert "S$5,775/month" in anchors
    row = research.COUNTRY_DATA["Singapore"]
    assert row["median_salary_currency"] == "SGD"
    assert row["median_salary"] == 5_775 * 12
    assert research.median_salary_display("Singapore").startswith("S$5,775 a month")


@pytest.mark.parametrize(
    "key,shown",
    [
        ("vn", research.NO_LOCAL_SALARY_BENCHMARK),
        ("in", research.NO_LOCAL_SALARY_BENCHMARK),
        ("uk", "£40,911 (ONS 2026, provisional)"),
        ("fr", "€3,693 a month (INSEE 2025, mean, private sector)"),
    ],
)
def test_workbook_median_salary_states_its_basis(key, shown):
    text = _sheet_text(_bundle(key)["wb"], "Market Intelligence")
    assert shown in text
    assert "(est.)" not in text
    assert not re.search(r"[₫₹£€]\s?[\d,]+\s*\((?:VND|INR|GBP|EUR)\)", text)


def test_location_intelligence_income_states_its_basis_too():
    info = research.get_location_info("Hanoi, Vietnam")
    assert info["median_salary_basis"] == "usd_estimate"
    assert info["median_salary_currency"] == "USD"
    assert info["median_salary_display"] == research.NO_LOCAL_SALARY_BENCHMARK
    assert info["currency"] == "VND"  # the country's own currency is untouched
    info = research.get_location_info("Leeds, United Kingdom")
    assert info["median_salary_basis"] == research.MEDIAN_SALARY_BASIS_SOURCED
    assert info["median_salary_currency"] == "GBP"
    assert info["median_salary_display"].startswith("£")


@pytest.mark.parametrize("key", ["vn", "in", "fr", "jp"])
def test_niche_row_and_cpc_are_labelled_a_us_benchmark_estimate(key):
    bundle = _bundle(key)
    wb = bundle["wb"]
    recs = _sheet_text(wb, "Channel Recommendations")
    assert f"CPC: {NICHE_LABEL}." in recs, "Niche row is not labelled"
    assert f"CPC: {BRANDING_LABEL}." in recs
    for sheet in ("Executive Summary", "Channels & Strategy"):
        assert NICHE_LABEL in _sheet_text(wb, sheet), sheet
    # labelling only: the allocation (totals, hires) is the engine's, untouched
    alloc = bundle["data"]["_budget_allocation"]["channel_allocations"]
    niche = [c for c in alloc.values() if c.get("category") == "niche_board"][0]
    assert str(niche["cpc_source"]).startswith("static_benchmark")


def test_us_plan_niche_row_has_no_us_benchmark_label():
    wb = _bundle("us")["wb"]
    assert "US benchmark estimate" not in _workbook_text(wb)


_ORPHAN_RANGE = re.compile(r"\(\d[\d,]*\s*[–-]\s*\d[\d,]*, Executive Summary\)")


@pytest.mark.parametrize("key", ["vn", "fr", "uk"])
def test_no_hires_figure_is_printed_where_the_executive_summary_says_not_projected(key):
    wb = _bundle(key)["wb"]
    es = _sheet_text(wb, "Executive Summary")
    assert "not projected" in es
    ws = wb["Channel Recommendations"]
    total = _row_cells(ws, "TOTAL")
    assert total, "Channel Recommendations has no TOTAL row"
    hires_col = next(
        c.column
        for row in ws.iter_rows()
        for c in row
        if c.value == "Hires" and c.row < total[0].row
    )
    assert ws.cell(row=total[0].row, column=hires_col).value == "n/a"
    assert not _ORPHAN_RANGE.search(
        _workbook_text(wb)
    ), "orphan hires-range cross-reference"
    assert "Hires ranges split the plan's own range" not in _workbook_text(wb)


def test_us_plan_still_projects_hires_everywhere():
    wb = _bundle("us")["wb"]
    ws = wb["Channel Recommendations"]
    total = _row_cells(ws, "TOTAL")
    hires_col = next(
        c.column for row in ws.iter_rows() for c in row if c.value == "Hires"
    )
    assert isinstance(ws.cell(row=total[0].row, column=hires_col).value, int)
    assert "Hires ranges split the plan's own range" in _workbook_text(wb)


def _apply_rate_block(wb) -> str:
    ws = wb["Market Intelligence"]
    lines: List[str] = []
    grab = False
    for row in ws.iter_rows():
        for c in row:
            if c.column == 2 and isinstance(c.value, str):
                if c.value.startswith("Apply Rates:"):
                    grab = True
                elif grab and c.value.startswith("Easy Apply"):
                    grab = False
            if grab and c.value is not None:
                lines.append(str(c.value))
    return "\n".join(lines)


@pytest.mark.parametrize(
    "key,country", [("vn", "Vietnam"), ("ae", "UAE"), ("jp", "Japan")]
)
def test_country_without_a_dataset_row_says_it_is_the_global_average(key, country):
    bundle = _bundle(key, deck=True)
    li = bundle["data"]["_gold_standard"]["linkedin_intelligence"]
    assert li["country_scope"] == "global" and li["country"] == country
    block = _apply_rate_block(bundle["wb"])
    assert "no country-level data; global average" in block
    assert "0.0%" not in block, block
    assert "Median Apply Rate" not in block and "75th Percentile" not in block
    assert "Sample Size (global dataset)" in block
    # the deck row (when the benchmark table has room for it) says global too
    assert ppt_generator._linkedin_apply_rate_row(li) == (
        "LinkedIn Apply Rate (global)",
        f"{li['country_apply_rate']['avg']:.1f}% avg ({li['sample_size']:,} jobs, global)",
    )
    assert f"LinkedIn Apply Rate ({country})" not in bundle["deck_text"]


@pytest.mark.parametrize(
    "key,country", [("uk", "United Kingdom"), ("ca", "Canada"), ("in", "India")]
)
def test_country_with_a_dataset_row_uses_it_and_names_the_country(key, country):
    bundle = _bundle(key)
    li = bundle["data"]["_gold_standard"]["linkedin_intelligence"]
    assert li["country"] == country and li["country_scope"] == "country"
    assert li["country_apply_rate"]["median"] > 0
    block = _apply_rate_block(bundle["wb"])
    assert f"Apply Rates: {country}" in block
    assert "Median Apply Rate" in block
    assert not re.search(r"Apply Rates: (ON|UK)\b", block)
    label, value = ppt_generator._linkedin_apply_rate_row(li)
    assert label == f"LinkedIn Apply Rate ({country})" and "global" not in value


def test_us_plan_linkedin_block_is_untouched():
    li = _bundle("us")["data"]["_gold_standard"]["linkedin_intelligence"]
    assert "country_scope" not in li


def test_india_workbook_and_deck_never_recommend_tiktok():
    bundle = _bundle("in", deck=True)
    wb = bundle["wb"]
    for sheet in ("Channel Recommendations", "Channels & Strategy", "ROI Projections"):
        assert "TikTok" not in _sheet_text(wb, sheet), sheet
    recs = _sheet_text(wb, "Channel Recommendations")
    assert "LinkedIn / Meta Ads" in recs
    assert "TikTok" not in bundle["deck_text"]
    ad = bundle["data"]["_synthesized"]["ad_platform_analysis"]
    assert not [k for k in ad if "tiktok" in k.lower()]


def test_us_and_other_markets_still_get_tiktok():
    assert "LinkedIn / Meta / TikTok Ads" in _sheet_text(
        _bundle("us")["wb"], "Channel Recommendations"
    )
    ad = _bundle("vn")["data"]["_synthesized"]["ad_platform_analysis"]
    assert [k for k in ad if "tiktok" in k.lower()]


def test_knowledge_base_subtree_keyed_by_a_blocked_platform_is_dropped_whole():
    tree = {
        "tiktok": {"platform_stats": "1B users", "algorithm_advantage": "For You feed"},
        "instagram": {"reach": "2B users"},
    }
    cleaned = excel_v2._drop_blocked_platform_keys(tree, ["India"])
    assert cleaned == {"instagram": {"reach": "2B users"}}  # no orphan "For You feed"
    assert excel_v2._drop_blocked_platform_keys(tree, ["United States"]) == tree
    assert (
        excel_v2._drop_blocked_platform_keys(tree, ["India", "United States"]) == tree
    )
    assert excel_v2._drop_blocked_platform_keys(tree, []) == tree


_NON_US_KEYS = ["vn", "in", "jp", "fr", "uk", "ca", "ae"]

_AMOUNT_WITH_CODE = re.compile(r"\S{1,4}\s?\d[\d,]{2,}\s\([A-Z]{3}\)")


@pytest.mark.parametrize("key", _NON_US_KEYS)
def test_market_intelligence_has_no_local_symbol_amount_with_a_currency_code(key):
    text = _sheet_text(_bundle(key)["wb"], "Market Intelligence")
    assert not _AMOUNT_WITH_CODE.search(text), _AMOUNT_WITH_CODE.search(text)


def _median_salary_values(ws) -> List[str]:
    """Every figure printed as a country median: the cells under a "Median
    Salary" table header, and the rest of a "Median Income" label's row."""
    found: List[str] = []
    for row in ws.iter_rows():
        for c in row:
            label = str(c.value or "").strip().lower()
            if label not in ("median salary", "median income"):
                continue
            r = c.row + 1
            while r <= ws.max_row and ws.cell(row=r, column=c.column).value is not None:
                found.append(str(ws.cell(row=r, column=c.column).value))
                r += 1
            found.extend(
                str(x.value) for x in ws[c.row] if x.column > c.column and x.value
            )
    return [v for v in found if re.search(r"\d", v)]


def test_median_figures_in_a_non_us_workbook_carry_their_stated_currency():
    """Every median printed on Market Intelligence is either the plan country's
    sourced label (its own currency, its source) or a US market's US$ figure;
    an unsourced US$-estimate amount never appears, with any symbol."""
    unsourced = {
        str(row["median_salary"])
        for name, row in research.COUNTRY_DATA.items()
        if name != "United States" and not row.get("median_salary_source")
    }
    labels = {
        research.median_salary_display(name)
        for name, row in research.COUNTRY_DATA.items()
        if row.get("median_salary_source")
    }
    checked = 0
    for key in _NON_US_KEYS:
        values = _median_salary_values(_bundle(key)["wb"]["Market Intelligence"])
        for v in values:
            checked += 1
            for amount in re.findall(r"\d[\d,]*", v):
                assert amount.replace(",", "") not in unsourced, (key, v)
            assert v in labels, (key, v)
    assert checked >= 4, checked


@pytest.mark.parametrize("key", ["vn", "in"])
def test_niche_and_branding_rows_in_the_vetted_channel_table_name_the_estimate(key):
    ws = _bundle(key)["wb"]["Channels & Strategy"]
    rationale = {}
    for row in ws.iter_rows():
        for c in row:
            if c.column == 2 and c.value in (
                "Niche / Industry Boards",
                "Employer Branding",
            ):
                text = ws.cell(row=c.row, column=7).value
                if isinstance(text, str) and text.startswith("CPC"):
                    rationale[c.value] = text
    assert f"({NICHE_LABEL})" in rationale["Niche / Industry Boards"]
    assert f"({BRANDING_LABEL})" in rationale["Employer Branding"]


def test_niche_board_matching_says_the_budget_line_is_an_estimate_and_quotes_no_hires():
    text = _sheet_text(_bundle("vn")["wb"], "Niche Board Matching")
    assert "budget line is therefore a US benchmark estimate" in text
    assert "Hires are not projected for this market" in text
    assert "0.0%" not in text and "0 hires" not in text
    us = _sheet_text(_bundle("us")["wb"], "Niche Board Matching")
    assert "US benchmark estimate" not in us
    assert "apply-to-hire rate shown here" in us  # a US plan keeps its own wording


def test_global_fallback_is_flagged_for_an_unresolved_non_us_first_market():
    # "Remote, Europe": non-US, but no country. (This fixture used "Birmingham, UK",
    # which only went unresolved through the Birmingham, AL bug fixed in
    # tests/test_explicit_country_suffix_wins.py; it now resolves to the UK.)
    data = {
        "locations": ["Remote, Europe"],
        "_knowledge_base": {
            "slotops_benchmarks": json.loads(
                (PROJECT_ROOT / "data" / "slotops_benchmarks_summary.json").read_text(
                    encoding="utf-8"
                )
            )
        },
    }
    li = gs.build_linkedin_intelligence(data)
    assert li["country_scope"] == "global"
    assert li["country_apply_rate"]["median"] == 0
    us = dict(data, locations=["Dallas, TX"])
    assert "country_scope" not in gs.build_linkedin_intelligence(us)


def test_a_channel_with_no_application_and_unprojected_hires_both_read_na():
    """The healthcare re-level prints "n/a" for a priced channel the engine
    expects to yield no application; the unprojected-hires total prints "n/a"
    too.  Both must reach the Channel Recommendations sheet (neither may be
    coerced to a live 0)."""
    base = _bundle("uk")["data"]
    data = dict(base)
    data["_budget_allocation"] = copy.deepcopy(base["_budget_allocation"])
    channels = data["_budget_allocation"]["channel_allocations"]
    niche = next(
        name
        for name, ch in channels.items()
        if ch.get("category") == "niche_board" and (ch.get("dollar_amount") or 0) > 0
    )
    channels[niche]["cpa"] = None
    channels[niche]["projected_applications"] = 0
    xlsx = excel_v2.generate_excel_v2(dict(data), research_mod=research)
    xlsx = xlsx.getvalue() if hasattr(xlsx, "getvalue") else xlsx
    ws = openpyxl.load_workbook(io.BytesIO(xlsx))["Channel Recommendations"]
    header = next(
        r for r in ws.iter_rows() if any(c.value == "CPA" for c in r) and r[0].row > 8
    )
    cpa_col = next(c.column for c in header if c.value == "CPA")
    hires_col = next(c.column for c in header if c.value == "Hires")
    niche_row = next(
        r for r in ws.iter_rows() if str(r[1].value or "").startswith("Niche")
    )
    total_row = next(r for r in ws.iter_rows() if r[1].value == "TOTAL")
    assert ws.cell(row=niche_row[0].row, column=cpa_col).value == "n/a"
    assert ws.cell(row=total_row[0].row, column=hires_col).value == "n/a"
    assert isinstance(ws.cell(row=total_row[0].row, column=cpa_col).value, float)
