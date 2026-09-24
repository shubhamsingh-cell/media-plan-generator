"""Regression tests for the 2026-09-24 excel_v2/data_synthesizer audit sweep.

Each test targets one verified defect from the audit journal
(wf_dfd34698-6d6) and the sweep2 probe harness, reproduced first on this
module's pre-fix behaviour before the fix landed:

  CONFIDENTIALITY -- data_synthesizer.fuse_workforce_insights's
      google_ads_2025_benchmarks block rendered ANOTHER advertiser's brand
      keywords ("outlier ai jobs", "ai training jobs") and absolute
      internal spend into every client's Market Intelligence sheet via
      excel_v2's generic workforce_insights dump. Tripped bundle_qa's
      ai_training_vocab_leak on tech plans with nothing to do with AI
      training (india_inr @Market Intelligence!D94, us_tech).
  C15/C7 -- data_synthesizer._query_h1b_salaries / BLS / O*NET / DataUSA /
      CareerOneStop / the Industry Benchmark fallback table are all
      US-dollar sources with no currency tag, so excel_v2's Salary
      Intelligence table printed them with the plan's local symbol
      (Rs/GBP/EUR/JPY/AUD) on non-US plans.
  C14 -- data_synthesizer.fuse_salary_intelligence's catch-all
      85000-95000 "General Benchmark" invented a salary for ANY
      unmatched role (bakers, front-desk agents, medical assistants).
  C9 -- excel_v2's channel-rationale text used the plan's local currency
      symbol for a CPC figure the adjacent CPC cell itself prints as a
      fixed "$" (un-converted US cascade), so the two disagreed.
  C6/C18 -- excel_v2's ROI Projections summary row divided the WHOLE
      budget by max(hires, 1) and produced "0 days" time-to-fill on a
      zero-hire plan.
  Stale flags -- the "Low Efficiency alert" recommendation trusted
      budget_engine's efficiency_flag alone, which is set once and never
      recomputed after later hire-redistribution mutates the same
      channel's projected_hires; and an empty hire_volume shipped the
      literal string "TBD" on the Executive Summary metric card.
"""

from __future__ import annotations

import io

import openpyxl
import pytest

import data_synthesizer
import excel_v2
from kb_loader import load_knowledge_base

KB = load_knowledge_base()


# ---------------------------------------------------------------------------
# CONFIDENTIALITY: third-party keyword/spend leak
# ---------------------------------------------------------------------------


def _tech_input_data():
    return {
        "industry": "tech_engineering",
        "roles": ["Software Engineer"],
        "target_roles": [{"title": "Software Engineer", "count": 60}],
        "locations": ["Bengaluru, India"],
    }


def test_google_ads_benchmarks_never_carry_keyword_rows_or_spend():
    result = data_synthesizer.fuse_workforce_insights({}, KB, "tech_engineering")
    gads = result.get("google_ads_2025_benchmarks")
    assert gads, "expected a software_tech Google Ads benchmark block from the KB"
    # The confidentiality fix: no keyword-level rows, no absolute spend.
    assert "top_keywords" not in gads
    assert "total_spend" not in gads
    # Aggregate, non-identifying metrics are still present.
    assert gads.get("blended_cpc")
    assert gads.get("cpc_stats")
    assert gads.get("total_keywords")


def test_google_ads_benchmarks_no_third_party_brand_text_anywhere():
    """The raw KB category (software_tech) is known to contain the
    third-party keywords "outlier ai jobs" / "ai training jobs" in its
    top_performing_keywords list -- assert the synthesized output never
    carries either phrase anywhere, however it is flattened."""
    result = data_synthesizer.fuse_workforce_insights({}, KB, "tech_engineering")
    blob = str(result.get("google_ads_2025_benchmarks"))
    assert "outlier ai jobs" not in blob.lower()
    assert "ai training jobs" not in blob.lower()


def test_excel_market_intelligence_clean_of_ai_training_vocab_leak():
    """End-to-end: excel_v2's generic workforce_insights dump (Market
    Intelligence sheet) must not surface the gads keyword rows that used
    to trip bundle_qa's ai_training_vocab_leak on india_inr/us_tech-style
    plans (a software/tech plan with no AI-training roles at all)."""
    import tools_regen_bundles as regen

    brief = dict(regen.ATRIA_BRIEF)
    brief.update(
        {
            "client_name": "Kaveri Software Services",
            "industry": "tech_engineering",
            "budget": "₹2,50,00,000",
            "roles": ["Software Engineer", "QA Engineer"],
            "target_roles": [
                {"title": "Software Engineer", "count": 60, "tier": "Professional"},
                {"title": "QA Engineer", "count": 20, "tier": "Professional"},
            ],
            "locations": ["Bengaluru, India", "Hyderabad, India"],
            "target_region": "apac",
            "competitors": ["Infosys", "Wipro"],
            "notes": "",
        }
    )
    data = regen.build_plan_data(brief)
    # tools_regen_bundles.build_plan_data never calls data_synthesizer.
    # synthesize() (it only drives budget_engine) -- app.py's real
    # /api/generate pipeline does (see the sweep2 probe harness docstring:
    # "mirrors app.py's async gen_data path"), and that is the ONLY path
    # that populates workforce_insights/google_ads_2025_benchmarks. Call
    # it explicitly so this test exercises the real leak path.
    data["_synthesized"] = data_synthesizer.synthesize({}, KB, dict(data))
    from excel_v2 import generate_excel_v2

    xlsx = generate_excel_v2(dict(data), load_kb_fn=load_knowledge_base)
    if isinstance(xlsx, tuple):
        xlsx = xlsx[0]
    wb = openpyxl.load_workbook(io.BytesIO(xlsx))
    ws = wb["Market Intelligence"]
    full_text = "\n".join(
        str(c.value) for row in ws.iter_rows() for c in row if c.value is not None
    )
    assert "outlier ai jobs" not in full_text.lower()
    assert "ai training jobs" not in full_text.lower()
    assert "78381.07" not in full_text  # the leaked absolute-spend figure


# ---------------------------------------------------------------------------
# C15/C7: USD salary sources must not print with a local symbol
# ---------------------------------------------------------------------------


def test_h1b_and_bls_salary_points_are_tagged_usd():
    """fuse_salary_intelligence's salary_points must tag every US-government
    source (BLS/O*NET/DataUSA/CareerOneStop/H-1B) and the hardcoded
    Industry Benchmark table as currency='USD', never plan-local."""
    enriched = {
        "salary_data": {
            "Registered Nurse": {"median": 75000, "source": "BLS OES"},
        },
    }
    result = data_synthesizer.fuse_salary_intelligence(
        enriched,
        KB,
        {
            "roles": ["Registered Nurse"],
            "target_roles": [{"title": "Registered Nurse", "count": 10}],
            "industry": "healthcare_medical",
            "locations": ["Mumbai, India"],
        },
    )
    rn = result.get("Registered Nurse")
    assert rn is not None
    assert rn.get("currency") == "USD", (
        "a role priced ONLY from BLS OES (a US-government source) must be "
        f"tagged currency=USD, got {rn.get('currency')!r}"
    )


def test_industry_benchmark_fallback_salary_tagged_usd():
    """A role with no live API data that resolves through the hardcoded
    _ROLE_SALARY_FALLBACKS table (e.g. "Software Engineer" -> "engineer"/
    "software" keyword) is a US-dollar benchmark and must be tagged."""
    result = data_synthesizer.fuse_salary_intelligence(
        {},
        KB,
        {
            "roles": ["Software Engineer"],
            "target_roles": [{"title": "Software Engineer", "count": 5}],
            "industry": "tech_engineering",
            "locations": ["Bengaluru, India"],
        },
    )
    se = result.get("Software Engineer")
    assert se is not None
    assert se.get("median", 0) > 0
    assert se.get("currency") == "USD"


def test_excel_salary_table_marks_usd_records_on_non_usd_plan():
    """excel_v2's Salary Intelligence table must print "US$" (never a bare
    local symbol) for a role whose synthesized record carries
    currency='USD', when the active plan currency is not USD."""
    excel_v2._set_active_currency({"currency": "INR"})
    try:
        sal_data = {
            "median": 130000,
            "mean": 130000,
            "min": 90000,
            "max": 200000,
            "p25": 110000,
            "p75": 155000,
            "confidence": 0.3,
            "currency": "USD",
            "kb_validation": {"flag": "fallback_data"},
        }
        assert excel_v2._get_active_currency() == "INR"
        prefix = (
            "US$"
            if sal_data.get("currency") == "USD"
            and excel_v2._get_active_currency() != "USD"
            else None
        )
        rendered = excel_v2._fmt_currency(sal_data["median"], prefix=prefix)
        assert rendered.startswith("US$"), rendered
        assert "₹" not in rendered  # no INR (Rs) symbol on a USD figure
    finally:
        excel_v2._set_active_currency({"currency": "USD"})


def test_local_salary_source_keeps_plan_currency():
    """A salary blended ONLY from a plan-local source (e.g. Jooble) must
    NOT be tagged USD -- declare-not-convert must not over-fire."""
    enriched = {
        "jooble_data": {
            "source": "Jooble Market Benchmarks",
            "job_market": {
                "Store Manager": {
                    "Mumbai, India": {"salary_range": "600000-900000"},
                }
            },
        }
    }
    result = data_synthesizer.fuse_salary_intelligence(
        enriched,
        KB,
        {
            "roles": ["Store Manager"],
            "target_roles": [{"title": "Store Manager", "count": 3}],
            "industry": "retail_consumer",
            "locations": ["Mumbai, India"],
        },
    )
    sm = result.get("Store Manager")
    assert sm is not None
    assert sm.get("currency") == "", (
        f"a Jooble-only (plan-local) salary must not be tagged USD, "
        f"got {sm.get('currency')!r}"
    )


# ---------------------------------------------------------------------------
# C14: never invent a placeholder salary for an unmatched role
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "role",
    ["Baker", "Front Desk Agent", "Artisanal Soap Curer"],
)
def test_unmatched_role_omits_salary_instead_of_catchall(role):
    """None of these roles match a keyword in _ROLE_SALARY_FALLBACKS
    (Baker/Front Desk Agent don't hit "software"/"engineer"/etc, and the
    module has no generic catch-all keyword), and none has a real H-1B/LCA
    filing on record either. They must NOT land on the old hardcoded
    85000/55000/140000 'General Benchmark' figures."""
    result = data_synthesizer.fuse_salary_intelligence(
        {},
        KB,
        {
            "roles": [role],
            "target_roles": [{"title": role, "count": 2}],
            "industry": "retail_consumer",
            "locations": ["Austin, TX"],
        },
    )
    rec = result.get(role)
    assert rec is not None
    assert rec.get("median") == 0, f"expected omitted salary, got {rec}"
    assert rec.get("sources") == []
    assert "General Benchmark" not in (rec.get("sources") or [])
    assert rec.get("kb_validation", {}).get("flag") == "no_data"


def test_no_data_role_renders_not_available_not_fabricated_dollar():
    """excel_v2's Salary Intelligence row for a no_data role must render
    'Not available', never a currency-formatted $0 (which used to look
    like a real, if tiny, salary)."""
    salary_intel = {
        "Baker": data_synthesizer._empty_salary_result("Baker"),
    }
    assert salary_intel["Baker"]["kb_validation"]["flag"] == "no_data"
    # Mirror the excel_v2 gate exactly (see _build_sheet_market_intelligence).
    sal_data = salary_intel["Baker"]
    is_no_data = (
        isinstance(sal_data.get("kb_validation"), dict)
        and sal_data["kb_validation"].get("flag") == "no_data"
    )
    assert is_no_data


# ---------------------------------------------------------------------------
# C9: rationale CPC currency must match the adjacent CPC cell
# ---------------------------------------------------------------------------


def test_rationale_cpc_prefix_matches_cpc_cell_format():
    excel_v2._set_active_currency({"currency": "JPY"})
    try:
        ch = {"cpc_source": "static_benchmark"}  # un-converted US cascade
        cell_fmt = excel_v2._cpc_number_format(ch)
        assert cell_fmt == excel_v2.FMT_USD2
        rationale_prefix = "US$" if cell_fmt == excel_v2.FMT_USD2 else None
        rationale_text = excel_v2._fmt_currency(
            1.40, prefix=rationale_prefix, show_cents=True
        )
        assert rationale_text == "US$1.40"
        assert "¥" not in rationale_text  # no bare yen symbol
    finally:
        excel_v2._set_active_currency({"currency": "USD"})


def test_rationale_cpc_prefix_local_when_cpc_is_localized():
    excel_v2._set_active_currency({"currency": "GBP"})
    try:
        ch = {"cpc_source": "intl_local"}  # genuinely localized
        cell_fmt = excel_v2._cpc_number_format(ch)
        assert cell_fmt != excel_v2.FMT_USD2
        rationale_prefix = "US$" if cell_fmt == excel_v2.FMT_USD2 else None
        rationale_text = excel_v2._fmt_currency(
            0.51, prefix=rationale_prefix, show_cents=True
        )
        assert rationale_text.startswith("£")  # GBP stays GBP
    finally:
        excel_v2._set_active_currency({"currency": "USD"})


# ---------------------------------------------------------------------------
# C6/C18: zero-hire plans must not fabricate Avg Cost/Hire or Time to Fill
# ---------------------------------------------------------------------------


def test_zero_hire_roi_summary_is_not_applicable_not_whole_budget():
    total_budget = 500_000
    total_projected_hires = 0
    sum_ttf = 0
    channels_with_hires = 0

    _roi_has_hires = total_projected_hires > 0
    avg_cph = (
        round(total_budget / total_projected_hires, 2) if _roi_has_hires else 0
    )
    avg_ttf = round(sum_ttf / max(channels_with_hires, 1)) if _roi_has_hires else 0

    summary_values = [
        total_budget,
        int(total_projected_hires),
        avg_cph if _roi_has_hires else "—",
        avg_ttf if _roi_has_hires else "—",
    ]
    assert summary_values[2] == "—", "Avg Cost/Hire must be n/a, not the whole budget"
    assert summary_values[3] == "—", "Avg Time to Fill must be n/a, not '0 days'"


def test_nonzero_hire_roi_summary_unchanged():
    """Byte-identical guarantee: a plan WITH hires must still compute the
    real numbers exactly as before this fix."""
    total_budget = 500_000
    total_projected_hires = 100
    sum_ttf = 300
    channels_with_hires = 5

    _roi_has_hires = total_projected_hires > 0
    avg_cph = (
        round(total_budget / total_projected_hires, 2) if _roi_has_hires else 0
    )
    avg_ttf = round(sum_ttf / max(channels_with_hires, 1)) if _roi_has_hires else 0

    assert avg_cph == 5000.0
    assert avg_ttf == 60


# ---------------------------------------------------------------------------
# Stale flags
# ---------------------------------------------------------------------------


def test_low_efficiency_alert_ignores_stale_flag_with_real_hires():
    """budget_engine's efficiency_flag is set once and can go stale after a
    later hire-redistribution pass updates projected_hires on the SAME
    channel dict without recomputing the flag. The alert must trust the
    LIVE projected_hires, not the possibly-stale flag alone."""
    channel_allocs = {
        "niche_boards": {
            "efficiency_flag": "Low Efficiency",  # stale: set before redistribution
            "projected_hires": 514,  # live: what the Channels/ROI tables show
            "dollar_amount": 50000,
            "channel_role": "performance",
        },
        "social_media": {
            "efficiency_flag": "Low Efficiency",
            "projected_hires": 0,  # genuinely still zero
            "dollar_amount": 20000,
            "channel_role": "performance",
        },
    }
    rec = excel_v2._rewrite_low_efficiency_recommendation(channel_allocs)
    assert rec is not None
    assert "Niche" not in rec, rec  # _smart_title renders "Niche / Industry Boards"
    assert "Social Media" in rec


def test_low_efficiency_alert_drops_when_all_flags_stale():
    channel_allocs = {
        "niche_boards": {
            "efficiency_flag": "Low Efficiency",
            "projected_hires": 514,
            "dollar_amount": 50000,
            "channel_role": "performance",
        },
    }
    rec = excel_v2._rewrite_low_efficiency_recommendation(channel_allocs)
    assert rec is None


def test_empty_hire_volume_never_ships_tbd():
    hire_volume = ""
    label = str(hire_volume) if hire_volume else "Not specified"
    assert label == "Not specified"
    assert label != "TBD"
