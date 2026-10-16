"""The wizard never grades cost per hire against unsourced per-industry quartiles.

Round-5 trust-lens finding: the live preview said "High ~$7K/hire for
Registered Nurse" by comparing against a typed table. The replacement
per-industry quartiles in ``data/recruitment_benchmarks_2026_deep.json`` turned
out to have no reachable source either (provenance audit 2026-10-17: the
"shrm_cph_2024" and "nahs_cna_2024" reports do not exist; SHRM 2025 publishes
only national means). Those rows are now ``status: illustrative_unverified``
and ``cph_benchmark.lookup`` returns None for them; the one verified figure is
the SHRM 2025 national mean, exposed by ``cph_benchmark.national``.
"""

from __future__ import annotations

import json
import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import cph_benchmark  # noqa: E402


def _file():
    with open(os.path.join(ROOT, "data", "recruitment_benchmarks_2026_deep.json"), encoding="utf-8") as fh:
        return json.load(fh)


def _by_industry():
    return _file()["by_industry"]


def _expected_source(source_ids):
    """Publisher + year for every source the row lists, in the row's order."""
    registry = {s["id"]: s for s in _file()["metadata"]["sources"]}
    labels = []
    for sid in source_ids:
        publisher = re.sub(r"\s*\([^)]*\)", "", registry[sid]["publisher"]).strip()
        label = f"{publisher} {registry[sid]['publication_year']}"
        if label not in labels:
            labels.append(label)
    return ", ".join(labels)


_MAPPED_CASES = [
        ("healthcare_medical", ["Registered Nurse"], "nursing"),
        ("healthcare_medical", ["ICU RN", "Staff LPN"], "nursing"),
        ("healthcare_medical", ["Hospital Administrator"], "healthcare"),
        ("healthcare_medical", ["Physician"], "physician"),
        ("tech_engineering", ["Software Engineer"], "tech"),
        ("tech_engineering", ["Software Engineer", "Data Scientist"], "tech"),
        ("retail_consumer", ["Store Associate"], "retail"),
        ("logistics_supply_chain", ["CDL Driver"], "logistics"),
        ("finance_banking", ["Loan Officer"], "finance"),
        ("blue_collar_trades", ["Electrician"], "skilled_trades"),
    ]


@pytest.fixture
def sourced_rows(monkeypatch):
    """The lookup mechanics (role mapping, USD-only, source label) with the
    ``illustrative_unverified`` gate lifted -- a stand-in for a future row that
    really is sourced. The gate itself is pinned by the tests further down."""
    import copy

    store = copy.deepcopy(cph_benchmark._load())
    for row in store["by_industry"].values():
        rng = row.get("cph_range_usd")
        if isinstance(rng, dict):
            rng.pop("status", None)
            rng.pop("note", None)
            rng["source_ids"] = ["appcast_2025", "shrm_cph_2025"]
    monkeypatch.setattr(cph_benchmark, "_cache", store)
    return store


@pytest.mark.parametrize("industry, roles, expected_key", _MAPPED_CASES)
def test_every_industry_row_is_gated_so_no_quartile_grade_is_ever_sent(industry, roles, expected_key):
    """Status gate: the row exists and has numbers, but lookup refuses it."""
    rng = _by_industry()[expected_key]["cph_range_usd"]
    assert rng["status"] == "illustrative_unverified"
    assert cph_benchmark.lookup(industry, roles) is None


def test_every_cph_range_row_is_illustrative_unverified_and_sourceless():
    rows = {k: v["cph_range_usd"] for k, v in _by_industry().items() if "cph_range_usd" in v}
    assert len(rows) == 12
    for key, rng in rows.items():
        assert rng["status"] == "illustrative_unverified", key
        assert rng["source_ids"] == [], key
        assert "No public per-industry cost-per-hire quartiles exist" in rng["note"], key
        assert "not to be shown as a benchmark" in rng["note"], key


def test_status_and_note_are_the_last_keys_of_each_cph_range_object():
    """nova's deep search truncates a matched object to its first keys, so the
    provenance note goes LAST and the figures stay in view."""
    for key, row in _by_industry().items():
        if "cph_range_usd" in row:
            assert list(row["cph_range_usd"])[-2:] == ["status", "note"], key
            assert list(row["cph_range_usd"])[:4] == ["p25", "median", "p75", "source_ids"], key


def test_the_registry_has_no_fabricated_cph_sources_and_no_row_cites_them():
    f = _file()
    ids = {s["id"] for s in f["metadata"]["sources"]}
    assert "shrm_cph_2024" not in ids and "nahs_cna_2024" not in ids
    assert "shrm_cph_2025" in ids
    text = json.dumps(f)
    assert "shrm_cph_2024" not in text and "nahs_cna_2024" not in text


def test_the_source_registry_names_shrm_2025_as_national_means():
    reg = {s["id"]: s for s in _file()["metadata"]["sources"]}
    assert reg["shrm_cph_2025"]["name"] == "SHRM 2025 Benchmarking Reports (Talent Acquisition) \u2014 national cost per hire means"
    assert reg["shrm_ttf_2025"]["url"] == "https://www.shrm.org/topics-tools/research/recruiting-benchmarking"


def test_national_block_sits_between_by_industry_and_by_metro():
    f = _file()
    keys = list(f)
    assert keys.index("by_industry") < keys.index("cph_national_usd") < keys.index("by_metro")
    block = f["cph_national_usd"]
    assert block["non_executive_mean"] == 5475 and block["executive_mean"] == 35879
    assert block["statistic"] == "mean" and block["sample_size"] == 2371
    assert block["source_ids"] == ["shrm_cph_2025"]
    assert block["survey_window"] == "2025-01-09 to 2025-03-03"


def test_national_is_the_shrm_2025_mean_and_usd_only():
    got = cph_benchmark.national()
    assert got == {
        "mean": 5475.0,
        "statistic": "mean",
        "label": "SHRM 2025 US national average cost per hire (non-executive, all industries)",
        "source": "SHRM 2025",
        "source_ids": ["shrm_cph_2025"],
        "url": "https://www.shrm.org/about/press-room/shrm-releases-2025-benchmarking-reports--how-does-your-organizat",
        "year": 2025,
    }
    assert cph_benchmark.national("usd") == got
    for code in ("GBP", "EUR", "INR", "CAD"):
        assert cph_benchmark.national(code) is None


@pytest.mark.parametrize("value", [None, 0, -5, True, "5475"])
def test_national_is_none_when_the_figure_is_missing_or_not_positive(monkeypatch, value):
    store = {"data": {"cph_national_usd": {"non_executive_mean": value}}, "by_industry": {}, "registry": {}}
    monkeypatch.setattr(cph_benchmark, "_cache", store)
    assert cph_benchmark.national() is None


def test_national_is_none_without_the_block(monkeypatch):
    monkeypatch.setattr(cph_benchmark, "_cache", {"data": {}, "by_industry": {}, "registry": {}})
    assert cph_benchmark.national() is None


@pytest.mark.parametrize("industry, roles, expected_key", _MAPPED_CASES)
def test_a_sourced_row_would_still_be_graded_from_the_data(sourced_rows, industry, roles, expected_key):
    got = cph_benchmark.lookup(industry, roles)
    rng = sourced_rows["by_industry"][expected_key]["cph_range_usd"]
    assert got is not None
    assert got["label"] == expected_key.replace("_", " ")
    assert (got["p25"], got["median"], got["p75"]) == (rng["p25"], rng["median"], rng["p75"])
    assert got["source"] == "Appcast 2025, SHRM 2025"


def test_the_source_label_names_every_listed_source_never_one_publisher_alone():
    assert cph_benchmark.source_label(["appcast_2025", "shrm_cph_2025"]) == "Appcast 2025, SHRM 2025"
    assert cph_benchmark.source_label([]) == ""


@pytest.mark.parametrize(
    "industry, roles",
    [
        ("general_entry_level", ["Barista"]),  # no benchmark row fits
        ("energy_utilities", ["Lineman"]),
        ("healthcare_medical", ["Registered Nurse", "Physician"]),  # two yardsticks: none
        ("healthcare_medical", ["Physician Assistant", "Registered Nurse"]),
        ("", ["Anything"]),
    ],
)
def test_no_single_fitting_benchmark_means_no_grade(industry, roles):
    assert cph_benchmark.lookup(industry, roles) is None


def test_a_physician_assistant_is_not_graded_as_a_physician(sourced_rows):
    got = cph_benchmark.lookup("healthcare_medical", ["Physician Assistant"])
    assert got is not None and got["label"] == "healthcare"  # not the $32K physician median


def test_only_usd_plans_are_graded(sourced_rows):
    assert cph_benchmark.lookup("healthcare_medical", ["Registered Nurse"], "USD") is not None
    for code in ("GBP", "EUR", "INR", "CAD"):
        assert cph_benchmark.lookup("healthcare_medical", ["Registered Nurse"], code) is None


@pytest.fixture(scope="module")
def estimate():
    import app

    def run(brief):
        return app._compute_plan_estimate(dict(brief))

    return run


def test_the_estimate_grades_a_nurse_plan_against_nothing_while_the_rows_are_unverified(estimate):
    est = estimate(
        {
            "budget": "$35,000",
            "industry": "healthcare_medical",
            "client_name": "Northwind Health",
            "roles": ["Registered Nurse"],
            "locations": ["Los Angeles, CA"],
        }
    )
    assert est["cph_benchmark"] is None


def test_the_estimate_has_no_benchmark_when_none_fits_or_the_plan_is_not_usd(estimate):
    none_fits = estimate(
        {"budget": "$35,000", "industry": "general_entry_level", "roles": ["Barista"], "locations": ["Austin, TX"]}
    )
    assert none_fits["cph_benchmark"] is None
    london = estimate(
        {
            "budget": "£35,000",
            "industry": "healthcare_medical",
            "roles": ["Registered Nurse"],
            "locations": ["London, UK"],
        }
    )
    assert london["plan_currency"]["code"] != "USD"
    assert london["cph_benchmark"] is None


def test_the_copilot_channel_suggestion_quotes_no_unadjusted_cpa(monkeypatch):
    """Round-5: "Avg CPA $9 for <industry>" was the registry's base CPA, the
    same for every industry, beside an industry-adjusted $17 on screen."""
    import app

    monkeypatch.setattr(
        app.benchmark_registry,
        "get_channel_benchmark",
        lambda channel, industry: {"cpa": 9.4},
    )
    for industry in ("healthcare_medical", "tech_engineering", "retail_consumer"):
        suggestions = app._copilot_suggest_channels(industry, ["Role"], ["Austin, TX"], "", "")
        prog = [s for s in suggestions if s["text"] == "Programmatic/DSP"]
        assert prog, industry
        assert "$" not in prog[0]["reason"] and not any(ch.isdigit() for ch in prog[0]["reason"]), prog[0]


# ---- Round-6 regression: a budget too small for one hire -----------------------


@pytest.mark.parametrize("budget", ["$3,000", "$9,000"])
def test_a_budget_too_small_for_one_hire_states_no_cost_per_hire(estimate, budget):
    """Nurse / Los Angeles at $3K or $9K funds fewer than one hire; the engine's
    cost_per_hire there is budget / max(hires, 1) == the whole budget. The
    estimate must not offer that as a cost per hire (the wizard graded "Efficient
    ~$3K/hire" / "High ~$9K/hire" beside "0 Est. hires")."""
    est = estimate(
        {
            "budget": budget,
            "industry": "healthcare_medical",
            "client_name": "Northwind Health",
            "roles": ["Registered Nurse"],
            "locations": ["Los Angeles, CA"],
        }
    )
    assert est["est_hires"] < 1
    assert est["est_cph"] is None
    assert est["cph_benchmark"] is None


def test_a_funded_plan_still_states_its_cost_per_hire(estimate):
    est = estimate(
        {
            "budget": "$250,000",
            "industry": "healthcare_medical",
            "client_name": "Northwind Health",
            "roles": ["Registered Nurse"],
            "locations": ["Los Angeles, CA"],
        }
    )
    assert est["est_hires"] >= 1
    assert isinstance(est["est_cph"], float) and est["est_cph"] > 0
