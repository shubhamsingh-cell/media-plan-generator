"""Provenance of research.COUNTRY_DATA's median salary (2026-11-03).

The column was authored on a mixed basis: some rows were local currency
(UK 42,000 was GBP, Switzerland 82,000 CHF), others US$ (Japan 38,000). Every
row now states its currency; rows whose currency is near parity with US$ carry
a national statistics office figure from data/country_median_earnings.json,
and every surface prints it through research.median_salary_display.

These tests fail when a row is relabelled against its stated currency, when a
sourced figure drifts from its source, when a location table prints a median
the sheet's own salary footnote denies, and when the workforce-trends knowledge
base lets a blocked platform's sub-tree through with the clause scrub off.
"""

from __future__ import annotations

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

import budget_engine  # noqa: E402
import excel_v2  # noqa: E402
import plan_currency  # noqa: E402
import publisher_filter  # noqa: E402
import research  # noqa: E402
from tests.intl_plan_fixture import _bundle  # noqa: E402

REGISTRY_PATH = PROJECT_ROOT / "data" / "country_median_earnings.json"


def _registry() -> dict:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _entries() -> Dict[str, dict]:
    return {e["country"]: e for e in _registry()["entries"]}


def _usd_rates() -> Dict[str, float]:
    raw = json.loads(
        (PROJECT_ROOT / "data" / "international_benchmarks_2026.json").read_text(
            encoding="utf-8"
        )
    )
    return {
        str(c.get("currency")): float(c["usd_rate"])
        for c in raw["countries"].values()
        if isinstance(c, dict) and c.get("usd_rate")
    }


# ---------------------------------------------------------------------------
# the registry
# ---------------------------------------------------------------------------


def test_registry_metadata_keys_come_last():
    raw = _registry()
    assert list(raw)[-1] == "metadata"
    for entry in raw["entries"]:
        assert list(entry)[-1] == "source", entry["id"]


def test_every_registry_entry_quotes_its_figure_from_a_primary_page():
    for entry in _registry()["entries"]:
        src = entry["source"]
        assert src["url"].startswith("https://"), entry["id"]
        assert src["retrieved_on"] == "2026-11-03", entry["id"]
        digits = re.sub(r"\D", "", src["quote"])
        whole = re.sub(r"\D", "", f"{entry['value']}".rstrip("0").rstrip("."))
        assert whole in digits, (entry["id"], whole, src["quote"])
        assert entry["statistic"] in ("median", "mean"), entry["id"]
        assert entry["unit"] in ("year", "month", "week"), entry["id"]
        factor = {"year": 1, "month": 12, "week": 52}[entry["unit"]]
        assert abs(entry["annual_equivalent"] - entry["value"] * factor) <= 1, entry["id"]


def test_every_near_parity_country_is_sourced_or_says_so():
    """Currencies within ~0.6 of parity with US$ (GBP, EUR, CHF, CAD, AUD, NZD,
    SGD) are exactly where a local and a US$ reading are confusable: every such
    row carries a registry entry."""
    rates = _usd_rates()
    for country, row in research.COUNTRY_DATA.items():
        code = row.get("currency")
        if country == "United States" or code not in rates:
            continue
        local_per_usd = 1 / rates[code]
        if abs(local_per_usd - 1) < 0.75:
            assert row.get("median_salary_source"), (country, code)


# ---------------------------------------------------------------------------
# GBP / EUR / CHF: never relabelled against the stated currency
# ---------------------------------------------------------------------------

_CASES = [
    ("United Kingdom", "GBP", "£40,911 (ONS 2026, provisional)", "London, UK"),
    ("Germany", "EUR", "€54,066 (Destatis 2025)", "Berlin, Germany"),
    ("Switzerland", "CHF", "CHF 7,024 a month (FSO 2024)", "Zurich, Switzerland"),
]


@pytest.mark.parametrize("country,code,label,location", _CASES)
def test_sourced_row_is_printed_in_its_own_currency_on_every_surface(
    country, code, label, location
):
    row = research.COUNTRY_DATA[country]
    assert row["median_salary_currency"] == code == row["currency"]
    assert research.median_salary_display(country) == label
    info = research.get_location_info(location)
    assert info["median_salary_currency"] == code
    assert info["median_salary_display"] == label
    lc = research.get_labour_market_intelligence("general_entry_level", [location])[
        "location_contexts"
    ][0]
    assert lc["median_salary"] == label
    assert "US$" not in lc["median_salary"]


@pytest.mark.parametrize("country,code,label,location", _CASES)
def test_relabelling_a_sourced_row_as_us_dollars_is_refused(
    country, code, label, location, monkeypatch
):
    """A row whose stated currency disagrees with its source prints no figure
    at all -- the planted relabel ("US$" on a GBP figure) cannot reach a cell."""
    bad = dict(research.COUNTRY_DATA[country], median_salary_currency="USD")
    monkeypatch.setitem(research.COUNTRY_DATA, country, bad)
    assert research.median_salary_entry(country) is None
    assert research.median_salary_display(country) == research.NO_LOCAL_SALARY_BENCHMARK


@pytest.mark.parametrize("country", sorted(_entries()))
def test_magnitude_guard_local_median_sits_in_a_plausible_band(country):
    """The row's figure is the source's (within 1%), and converted to US$ it is a
    plausible full-time annual wage for a high-income economy (US$15k-130k) --
    a US$ figure mislabelled as local, or a monthly figure stored as annual,
    falls outside."""
    entry = _entries()[country]
    row = research.COUNTRY_DATA[country]
    assert abs(row["median_salary"] - entry["annual_equivalent"]) <= entry[
        "annual_equivalent"
    ] * 0.01, (country, row["median_salary"], entry["annual_equivalent"])
    usd = row["median_salary"] * _usd_rates()[entry["currency"]]
    assert 15_000 <= usd <= 130_000, (country, usd)


def test_unsourced_rows_state_us_dollars_and_print_no_figure():
    for country in ("Japan", "India", "Vietnam", "Sweden", "Poland"):
        row = research.COUNTRY_DATA[country]
        assert row["median_salary_currency"] == "USD"
        assert not row.get("median_salary_source")
        assert research.median_salary_display(country) == research.NO_LOCAL_SALARY_BENCHMARK


# ---------------------------------------------------------------------------
# workbook: one source, agreeing with the sheet's own footnote
# ---------------------------------------------------------------------------


def _table_column(ws, header: str, column_label: str) -> Dict[str, str]:
    """{Location: cell} for the table whose header row holds ``header``."""
    for row in ws.iter_rows():
        vals = [c.value for c in row]
        if "Location" in vals and any(isinstance(v, str) and header in v for v in vals):
            loc_col = vals.index("Location") + 1
            col = next(
                i + 1 for i, v in enumerate(vals) if isinstance(v, str) and column_label in v
            )
            out: Dict[str, str] = {}
            r = row[0].row + 1
            while ws.cell(row=r, column=loc_col).value:
                out[str(ws.cell(row=r, column=loc_col).value)] = str(
                    ws.cell(row=r, column=col).value or ""
                )
                r += 1
            return out
    return {}


def _sheet_text(ws) -> str:
    return "\n".join(
        str(c.value) for row in ws.iter_rows() for c in row if c.value is not None
    )


@pytest.mark.parametrize(
    "key,country,sourced",
    [("uk", "United Kingdom", True), ("fr", "France", True), ("in", "India", False)],
)
def test_location_median_and_salary_footnote_agree(key, country, sourced):
    wb = _bundle(key)["wb"]
    mi = wb["Market Intelligence"]
    label = research.median_salary_display(country)
    economic = _table_column(mi, "Median Salary", "Median Salary")
    assert economic and set(economic.values()) == {label}, economic
    income = _table_column(mi, "Median Income", "Median Income")
    assert income, "Location Intelligence table or its Median Income column not found"
    assert set(income.values()) == {label}, income
    footnote = _sheet_text(wb["Quality Intelligence"])
    denial = re.search(r"Market-level salary estimates are not available for ([^;]+)", footnote)
    if sourced:
        assert label in _sheet_text(mi)
        assert not denial, denial and denial.group(0)
        assert "national median with its statistics-office source" in footnote
    else:
        assert label == research.NO_LOCAL_SALARY_BENCHMARK
        assert denial, footnote[-500:]


# ---------------------------------------------------------------------------
# one US-market test for the benchmark notes and the cost-per-hire rule
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("location", ["Bengaluru, India", "Berlin, Germany"])
def test_us_only_region_default_does_not_make_a_foreign_plan_a_us_plan(
    location, monkeypatch
):
    data = {
        "locations": [location],
        "target_region": "us_only",
        "_budget_allocation": {
            "channel_allocations": {
                "niche_boards": {
                    "dollar_amount": 1000,
                    "category": "niche_board",
                    "cpc_source": "static_benchmark",
                }
            }
        },
    }
    assert excel_v2._plan_has_no_us_market(data) is True
    assert budget_engine._plan_is_non_us_market(data["locations"]) is True
    # a US$ plan: only the market test can make the table a US benchmark
    monkeypatch.setattr(excel_v2, "_get_active_currency", lambda: "USD")
    assert excel_v2._us_benchmark_table(data) is True
    assert excel_v2._us_benchmark_cpc_channels(data) == {
        "niche_boards": excel_v2.NICHE_US_BENCHMARK_CPC_LABEL
    }


def test_a_us_location_still_reads_as_a_us_plan():
    data = {"locations": ["Dallas, TX", "London, UK"], "target_region": "global"}
    assert excel_v2._plan_has_no_us_market(data) is False
    assert budget_engine._plan_is_non_us_market(data["locations"]) is False


# ---------------------------------------------------------------------------
# workforce-trends KB: a blocked platform's sub-tree is dropped at source
# ---------------------------------------------------------------------------


def test_india_workforce_sheet_drops_tiktok_subtree_with_the_clause_scrub_off(
    monkeypatch,
):
    """With publisher_filter.scrub_text disabled, only the knowledge-base
    pruning (excel_v2._drop_blocked_platform_keys inside _wf_source) can keep
    the TikTok sub-tree off an India plan's workforce sheet."""
    import tools_regen_bundles as T

    monkeypatch.setattr(publisher_filter, "scrub_text", lambda text, *a, **k: text)
    data = T.build_plan_data(
        {
            "client_name": "Test Co",
            "budget": "₹8,000,000",
            "campaign_duration": "3 months",
            "industry": "general_entry_level",
            "locations": ["Bengaluru, India"],
            "roles": ["Agent"],
            "target_roles": [{"title": "Agent", "count": 5}],
            "hire_volume": "50-100 hires",
        }
    )
    # the same headless steps tests/intl_plan_fixture._bundle runs
    import data_synthesizer
    import gold_standard as gs

    # with the knowledge base passed, as app.py does: the workforce-trends
    # block (and its TikTok sub-tree) comes from it
    data["_synthesized"] = data_synthesizer.synthesize(
        {}, data.get("_knowledge_base") or {}, data
    )
    data["_gold_standard"] = gs.apply_all_quality_gates(dict(data))
    xlsx = excel_v2.generate_excel_v2(dict(data), research_mod=research)
    xlsx = xlsx.getvalue() if hasattr(xlsx, "getvalue") else xlsx
    wb = openpyxl.load_workbook(io.BytesIO(xlsx))
    text = _sheet_text(wb["Market Intelligence"])
    assert "Social Recruiting" in text or "Supply Partner Trends" in text, (
        "workforce-trends block not rendered; the test would pass vacuously"
    )
    assert "Tiktok:" not in text and "TikTok:" not in text
    assert "Algorithm Advantage" not in text


def test_a_bare_glyph_currency_on_another_currencys_plan_is_written_with_its_code():
    """On a US$ plan a UK median reads "GBP 40,911 ...", never a bare "£" that
    bundle_qa's currency-mixing rule (rightly) flags; letter-prefixed symbols
    (A$, CHF) and the plan's own currency keep their symbol."""
    assert research.median_salary_display("United Kingdom", "USD").startswith(
        "GBP 40,911 (ONS 2026"
    )
    assert research.median_salary_display("Germany", "GBP") == "EUR 54,066 (Destatis 2025)"
    assert research.median_salary_display("United Kingdom", "GBP").startswith("£40,911")
    assert research.median_salary_display("Australia", "USD").startswith("A$1,741 a week")
    assert research.median_salary_display("Switzerland", "EUR").startswith("CHF 7,024")
