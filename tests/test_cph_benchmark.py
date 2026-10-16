"""The wizard grades cost per hire against the data's quartiles, never a typed table.

Round-5 trust-lens finding: the live preview said "High ~$7K/hire for
Registered Nurse" by comparing against a per-role-class table typed into the
browser (``BENCH = {tech: 5000, skilled: 3500, ...}``). The repo's SHRM-sourced
data (``data/recruitment_benchmarks_2026_deep.json`` -> ``by_industry``) has
nursing at median $5,400 / p75 $7,800 and tech at $5,800 / $9,500. The estimate
now carries ``cph_benchmark`` read from that file, and the browser words
"High" / "Efficient" only against it.
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


@pytest.mark.parametrize(
    "industry, roles, expected_key",
    [
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
    ],
)
def test_quartiles_are_the_data_files_own(industry, roles, expected_key):
    got = cph_benchmark.lookup(industry, roles)
    rng = _by_industry()[expected_key]["cph_range_usd"]
    assert got is not None
    assert got["label"] == expected_key.replace("_", " ")
    assert (got["p25"], got["median"], got["p75"]) == (rng["p25"], rng["median"], rng["p75"])
    # the label is the row's OWN source list (every entry, in its order)
    assert got["source"] == _expected_source(_by_industry()[expected_key]["cph_range_usd"]["source_ids"])


def test_the_nursing_label_names_nahs_beside_shrm_never_shrm_alone():
    """Round-6: the nursing quartiles list NAHS (a tier-2 source with no URL)
    beside SHRM, and SHRM's public 2025 release has no nursing quartiles, so the
    on-screen label must mirror the file's own source field."""
    got = cph_benchmark.lookup("healthcare_medical", ["Registered Nurse"])
    assert got is not None
    assert got["source"] == "SHRM 2024, NAHS 2024, SHRM 2025"
    assert "NAHS" in got["source"] and got["source"] != "SHRM"
    assert got["source_ids"] == ["shrm_cph_2024", "nahs_cna_2024", "shrm_cph_2025"]


def test_the_two_figures_the_trust_lens_quoted():
    nurse = cph_benchmark.lookup("healthcare_medical", ["Registered Nurse"])
    tech = cph_benchmark.lookup("tech_engineering", ["Software Engineer"])
    assert nurse is not None and tech is not None
    assert (nurse["median"], nurse["p75"]) == (5400, 7800)
    assert (tech["median"], tech["p75"]) == (5800, 9500)


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


def test_a_physician_assistant_is_not_graded_as_a_physician():
    got = cph_benchmark.lookup("healthcare_medical", ["Physician Assistant"])
    assert got is not None and got["label"] == "healthcare"  # not the $32K physician median


def test_only_usd_plans_are_graded():
    assert cph_benchmark.lookup("healthcare_medical", ["Registered Nurse"], "USD") is not None
    for code in ("GBP", "EUR", "INR", "CAD"):
        assert cph_benchmark.lookup("healthcare_medical", ["Registered Nurse"], code) is None


@pytest.fixture(scope="module")
def estimate():
    import app

    def run(brief):
        return app._compute_plan_estimate(dict(brief))

    return run


def test_the_estimate_carries_the_benchmark_for_a_nurse_plan(estimate):
    est = estimate(
        {
            "budget": "$35,000",
            "industry": "healthcare_medical",
            "client_name": "Northwind Health",
            "roles": ["Registered Nurse"],
            "locations": ["Los Angeles, CA"],
        }
    )
    bm = est["cph_benchmark"]
    assert bm and bm["label"] == "nursing" and bm["p75"] == 7800 and bm["p25"] == 4200
    assert bm["source"].startswith("SHRM")


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
    # the benchmark itself is still there (it is a property of the roles, not the budget)
    assert est["cph_benchmark"] and est["cph_benchmark"]["label"] == "nursing"


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
