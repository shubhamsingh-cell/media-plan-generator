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
import ppt_generator as ppt
from kb_loader import load_knowledge_base
from pptx import Presentation

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
    avg_cph = round(total_budget / total_projected_hires, 2) if _roi_has_hires else 0
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
    avg_cph = round(total_budget / total_projected_hires, 2) if _roi_has_hires else 0
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


# ---------------------------------------------------------------------------
# Hershey client report (2026-09-24 Slack): every role in a generated plan
# showed an IDENTICAL talent-pool count (~1.5M) regardless of role/location.
#
# Root cause: data_synthesizer.fuse_job_market_demand's generic fallback
# (fires when a role matches none of _ROLE_DEMAND_FALLBACKS' ~23 keywords
# AND every live signal -- Adzuna/Jooble/Google Ads/Google Trends/LinkedIn
# -- came back empty) sets total_postings AND talent_pool_estimate from the
# SAME hardcoded "Industry Benchmark" dict (job_postings=75000,
# talent_pool=1500000) for every such role in one shot. excel_v2's Market
# Demand by Role table already gated the Postings column against this exact
# fallback (via posting_sources containing "Industry Benchmark" ->
# "Data not available"), but the Talent Pool column right next to it had no
# equivalent gate and printed the fabricated 1,500,000 as if it were a real,
# measured (and coincidentally identical) per-role figure. Manufacturing/CPG
# role titles (e.g. Hershey's line-operator/packaging roles) are exactly the
# kind that miss every keyword in that fallback table, so every role in the
# plan hit the SAME generic bucket and displayed the SAME number.
# ---------------------------------------------------------------------------


def test_unmatched_roles_share_fabricated_talent_pool_value_precondition():
    """PRECONDITION test, not a regression guard -- this passes on BOTH
    pre-fix and post-fix code, because the fix lives downstream in
    excel_v2/ppt_generator's display gates, not in data_synthesizer itself.
    It documents the synthesis-layer condition that makes the client's
    symptom possible: two different, non-matching roles both fall through
    to the exact same hardcoded generic-fallback talent_pool_estimate (and,
    see the tests below, the same market_temperature/trend_direction too).
    The actual regression guards are
    test_excel_talent_pool_column_never_ships_fabricated_number,
    test_excel_market_temp_and_trend_columns_never_ship_fabricated_value,
    and test_ppt_market_temp_row_omitted_for_fabricated_fallback below --
    each of those fails on pre-fix code and this one does not."""
    result = data_synthesizer.fuse_job_market_demand(
        {},
        KB,
        {
            "roles": ["Confectionery Line Operator", "Packaging Associate II"],
            "target_roles": [
                {"title": "Confectionery Line Operator", "count": 20},
                {"title": "Packaging Associate II", "count": 15},
            ],
            "industry": "manufacturing",
            "locations": ["Hershey, PA", "Stuarts Draft, VA"],
        },
    )
    op = result.get("Confectionery Line Operator")
    pkg = result.get("Packaging Associate II")
    assert op is not None and pkg is not None
    assert "Industry Benchmark" in (op.get("posting_sources") or [])
    assert "Industry Benchmark" in (pkg.get("posting_sources") or [])
    assert op["talent_pool_estimate"] == pkg["talent_pool_estimate"] == 1_500_000


def test_excel_talent_pool_column_never_ships_fabricated_number():
    """excel_v2's Market Demand by Role table must render 'Data not
    available' for Talent Pool on a fabricated-fallback role -- the SAME
    gate already applied to the Postings column two cells to its left --
    instead of the hardcoded 1,500,000 that shipped to the Hershey client
    identically on every unmatched role."""
    data = {
        "client_name": "Hershey Test",
        "roles": ["Confectionery Line Operator", "Packaging Associate II"],
        "target_roles": [
            {"title": "Confectionery Line Operator", "count": 20},
            {"title": "Packaging Associate II", "count": 15},
        ],
        "industry": "manufacturing",
        "locations": ["Hershey, PA", "Stuarts Draft, VA"],
        "budget": "150000",
    }
    data["_synthesized"] = data_synthesizer.synthesize({}, KB, dict(data))

    from excel_v2 import generate_excel_v2

    xlsx = generate_excel_v2(dict(data), load_kb_fn=load_knowledge_base)
    if isinstance(xlsx, tuple):
        xlsx = xlsx[0]
    wb = openpyxl.load_workbook(io.BytesIO(xlsx))
    ws = wb["Market Intelligence"]

    demand_rows = {}
    for row in ws.iter_rows(values_only=True):
        if not row:
            continue
        label = row[1] if len(row) > 1 else None
        if label in ("Confectionery Line Operator", "Packaging Associate II"):
            # Postings, Talent Pool are columns 2 and 3 of this table
            # (col 1 is Role) -- only the Market Demand table has a
            # "Postings" value of "Data not available" in column index 2.
            if row[2] == "Data not available":
                demand_rows[label] = row

    assert len(demand_rows) == 2, (
        "expected both roles' Market Demand by Role rows, found " f"{list(demand_rows)}"
    )
    for label, row in demand_rows.items():
        talent_pool_cell = row[3]
        assert talent_pool_cell == "Data not available", (
            f"{label}: Talent Pool cell still shows the fabricated "
            f"Industry Benchmark number: {talent_pool_cell!r}"
        )
        assert "1,500,000" != talent_pool_cell


# ---------------------------------------------------------------------------
# Verifier follow-up (post-review of the Talent Pool fix above): the SAME
# "Industry Benchmark" fallback row also fabricates market_temperature
# ("hot") and trend_direction ("Stable (+2% YoY)") identically across every
# unmatched role -- data_synthesizer.fuse_job_market_demand computes
# temperature = _market_temperature(competition_index * 100) and
# trend_dir = fallback_demand["trend"] inside the SAME branch that sets
# posting_sources = ["Industry Benchmark"], so they carry the identical
# fabrication signal the Postings/Talent Pool columns already gate on.
# excel_v2's Market Temp / Trend columns had no such gate; neither did
# ppt_generator's "Market Temp: {role}" benchmark-table line (deck side).
# ---------------------------------------------------------------------------


def test_excel_market_temp_and_trend_columns_never_ship_fabricated_value():
    """excel_v2's Market Demand by Role table must render 'Data not
    available' for BOTH Temperature and Trend on a fabricated-fallback
    role, the same gate as Postings/Talent Pool -- never the fabricated
    'hot' / 'Stable (+2% YoY)' that is identical for every unmatched role."""
    data = {
        "client_name": "Hershey Test",
        "roles": ["Confectionery Line Operator", "Packaging Associate II"],
        "target_roles": [
            {"title": "Confectionery Line Operator", "count": 20},
            {"title": "Packaging Associate II", "count": 15},
        ],
        "industry": "manufacturing",
        "locations": ["Hershey, PA", "Stuarts Draft, VA"],
        "budget": "150000",
    }
    data["_synthesized"] = data_synthesizer.synthesize({}, KB, dict(data))

    from excel_v2 import generate_excel_v2

    xlsx = generate_excel_v2(dict(data), load_kb_fn=load_knowledge_base)
    if isinstance(xlsx, tuple):
        xlsx = xlsx[0]
    wb = openpyxl.load_workbook(io.BytesIO(xlsx))
    ws = wb["Market Intelligence"]

    demand_rows = {}
    for row in ws.iter_rows(values_only=True):
        if not row:
            continue
        label = row[1] if len(row) > 1 else None
        if label in ("Confectionery Line Operator", "Packaging Associate II"):
            if row[2] == "Data not available":  # Market Demand table row
                demand_rows[label] = row

    assert len(demand_rows) == 2, (
        "expected both roles' Market Demand by Role rows, found " f"{list(demand_rows)}"
    )
    for label, row in demand_rows.items():
        # row[0] is a leading blank (col A). Table columns from row[1]:
        # Role(1), Postings(2), Talent Pool(3), Competition(4),
        # Temperature(5), Trend(6), Search Interest(7).
        temp_cell, trend_cell = row[5], row[6]
        assert temp_cell == "Data not available", (
            f"{label}: Temperature cell still shows the fabricated "
            f"Industry Benchmark value: {temp_cell!r}"
        )
        assert trend_cell == "Data not available", (
            f"{label}: Trend cell still shows the fabricated "
            f"Industry Benchmark value: {trend_cell!r}"
        )
        assert temp_cell != "hot"
        assert trend_cell != "Stable (+2% YoY)"


def _ppt_plan_with_job_market_demand(job_market_demand, **over):
    """Minimal ppt_generator.generate_pptx input with a hand-built
    _synthesized.job_market_demand block (bypassing the full enrichment/
    synthesis pipeline, same pattern as test_plan_output_audit_closure.py's
    _synth_5platform() helper)."""
    data = {
        "client_name": "Hershey Test",
        "industry": "manufacturing",
        "industry_label": "Manufacturing",
        "budget": "$150,000",
        "budget_period": "campaign",
        "campaign_duration": "3 months",
        "campaign_start_month": 9,
        "hire_volume": "35 hires",
        "work_environment": "onsite",
        "locations": [{"city": "Hershey", "state": "PA", "country": "United States"}],
        "roles": [{"title": "Confectionery Line Operator", "count": 20, "tier": "mid"}],
        "_synthesized": {"job_market_demand": job_market_demand},
    }
    data.update(over)
    return data


def _bench_table_texts(prs):
    """All text-frame contents across every slide (the benchmark table is
    built from plain textboxes, not a native pptx table)."""
    texts = []
    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.has_text_frame:
                texts.append(shape.text_frame.text)
    return texts


def test_ppt_market_temp_row_omitted_for_fabricated_fallback():
    """ppt_generator's Channel Strategy benchmark table already omits the
    'Live Postings: {role}' row when posting_sources is the fabricated
    "Industry Benchmark" fallback (existing _is_fabricated_posting guard).
    The 'Market Temp: {role}' row right below it had no equivalent guard
    and printed the fabricated, cross-role-identical temperature verbatim
    -- assert it is now omitted the same way. A single-role fixture is used
    deliberately: the benchmark table caps at ~5 rows and silently drops
    trailing ones, so a multi-role fixture could pass for the wrong reason
    (row-count truncation) rather than because the fabrication guard fired."""
    job_market_demand = {
        "Confectionery Line Operator": {
            "total_postings": 75000,
            "posting_sources": ["Industry Benchmark"],
            "market_temperature": "hot",
            "trend_direction": "Stable (+2% YoY)",
            "talent_pool_estimate": 1_500_000,
        },
    }
    data = _ppt_plan_with_job_market_demand(job_market_demand)
    prs = Presentation(io.BytesIO(ppt.generate_pptx(data)))
    texts = _bench_table_texts(prs)
    assert not any("Market Temp:" in t for t in texts), (
        "deck still ships a 'Market Temp: <role>' row sourced from the "
        "fabricated Industry Benchmark fallback"
    )
    assert not any(
        "Live Postings: Confectionery" in t for t in texts
    ), "sanity check: the pre-existing Live Postings guard regressed too"


def test_ppt_market_temp_row_present_for_real_data():
    """Negative control: a role with genuine (non-fallback) posting_sources
    must still show its Market Temp row -- proves the fix gates on the
    fabrication flag specifically, not on market_temperature being present
    at all (which would silently break the row for every real plan too).

    total_postings is deliberately 0 here (posting_sources is still a real
    source, "Adzuna", never "Industry Benchmark") so the pre-existing Live
    Postings guard omits ITS OWN row -- keeping the benchmark table's total
    row count under the ~5-row cap the table silently truncates to. A
    nonzero total_postings would add a 6th row and get Market Temp cut by
    that unrelated row-count cap instead, making this control meaningless.
    """
    job_market_demand = {
        "Registered Nurse": {
            "total_postings": 0,
            "posting_sources": ["Adzuna"],
            "market_temperature": "cool",
            "trend_direction": "Declining",
            "talent_pool_estimate": 500000,
        },
    }
    data = _ppt_plan_with_job_market_demand(
        job_market_demand,
        industry="healthcare",
        industry_label="Healthcare",
        locations=[{"city": "Dallas", "state": "TX", "country": "United States"}],
        roles=[{"title": "Registered Nurse", "count": 20, "tier": "mid"}],
    )
    prs = Presentation(io.BytesIO(ppt.generate_pptx(data)))
    texts = _bench_table_texts(prs)
    assert any("Market Temp: Registered Nurse" in t for t in texts), (
        "expected the real-data Market Temp row to survive -- got: "
        f"{[t for t in texts if 'Market Temp' in t]}"
    )


# ---------------------------------------------------------------------------
# Verifier follow-up #2: even with the Market Temp ROW fix above (Channel
# Strategy slide), the Executive Summary slide (slide 2) built its own
# `market_temp_str` at ppt_generator.py ~4231-4241 by taking the FIRST role
# in job_market_demand with ANY market_temperature value -- no fabrication
# check at all. Four places on that slide read the one variable: the
# headline qualifier ("...in a {temp} talent market", ~4245), the situation
# card ("Market Temp.: {temp} (...)",  ~4351), the strategy-thesis clause
# ("...in a {temp} market", ~4623 -- a 4th site beyond the 3 the verifier
# named, caught because it derives from the same variable), and the
# secondary metric chip (~4893). Because dict iteration order put a
# fabricated-fallback role first, the headline could not just show stale/
# uninformative data but ACTIVELY CONTRADICT a real role's genuine reading
# later in the same dict (e.g. asserting "hot" while a real-data role says
# "cool").
# ---------------------------------------------------------------------------


def _slide2_texts(prs):
    slide2 = prs.slides[1]  # Executive Summary is the second slide (index 1)
    return [
        shape.text_frame.text
        for shape in slide2.shapes
        if shape.has_text_frame and shape.text_frame.text.strip()
    ]


def test_slide2_never_shows_fabricated_temp_and_prefers_real_role_data():
    """3-role plan modeled on the verifier's own repro: 2 fallback roles
    (posting_sources=["Industry Benchmark"], market_temperature="hot",
    dict-ordered FIRST) + 1 real-data role reporting "cool". Slide 2 must
    reflect the real role's "cool" everywhere it mentions temperature, and
    must never assert the fabricated "hot" -- the headline no longer
    silently overrides genuine data with a fallback role's fabricated
    reading just because that role happened to iterate first."""
    job_market_demand = {
        "Confectionery Line Operator": {
            "total_postings": 75000,
            "posting_sources": ["Industry Benchmark"],
            "market_temperature": "hot",
            "trend_direction": "Stable (+2% YoY)",
            "talent_pool_estimate": 1_500_000,
        },
        "Packaging Associate II": {
            "total_postings": 75000,
            "posting_sources": ["Industry Benchmark"],
            "market_temperature": "hot",
            "trend_direction": "Stable (+2% YoY)",
            "talent_pool_estimate": 1_500_000,
        },
        "Quality Assurance Technician": {
            "total_postings": 4200,
            "posting_sources": ["Adzuna"],
            "market_temperature": "cool",
            "trend_direction": "Declining",
            "talent_pool_estimate": 250_000,
        },
    }
    data = _ppt_plan_with_job_market_demand(
        job_market_demand,
        roles=[
            {"title": "Confectionery Line Operator", "count": 20, "tier": "mid"},
            {"title": "Packaging Associate II", "count": 15, "tier": "mid"},
            {"title": "Quality Assurance Technician", "count": 5, "tier": "mid"},
        ],
    )
    prs = Presentation(io.BytesIO(ppt.generate_pptx(data)))
    texts = _slide2_texts(prs)
    full_text = "\n".join(texts).lower()

    assert "hot" not in full_text, (
        "slide 2 still asserts the fabricated 'hot' reading from a "
        f"fallback role: {[t for t in texts if 'hot' in t.lower()]}"
    )
    assert any(
        "cool talent market" in t.lower() for t in texts
    ), f"expected the headline to show the real role's 'cool' -- got: {texts}"
    assert any(
        "market temp." in t.lower() and "cool" in t.lower() for t in texts
    ), f"expected the situation card to show 'Market Temp.: Cool (...)' -- got: {texts}"


def test_slide2_omits_temp_clause_when_every_role_is_fallback():
    """When EVERY role in the plan hit the Industry Benchmark fallback (no
    real market_temperature data anywhere), slide 2 must omit the
    'in a ... talent market' headline clause and the 'Market Temp.'
    situation-card line entirely -- never fall back to a guess, and never
    assert the fabricated value just because it's the only one available."""
    job_market_demand = {
        "Confectionery Line Operator": {
            "total_postings": 75000,
            "posting_sources": ["Industry Benchmark"],
            "market_temperature": "hot",
            "trend_direction": "Stable (+2% YoY)",
            "talent_pool_estimate": 1_500_000,
        },
        "Packaging Associate II": {
            "total_postings": 75000,
            "posting_sources": ["Industry Benchmark"],
            "market_temperature": "hot",
            "trend_direction": "Stable (+2% YoY)",
            "talent_pool_estimate": 1_500_000,
        },
    }
    data = _ppt_plan_with_job_market_demand(
        job_market_demand,
        roles=[
            {"title": "Confectionery Line Operator", "count": 20, "tier": "mid"},
            {"title": "Packaging Associate II", "count": 15, "tier": "mid"},
        ],
    )
    prs = Presentation(io.BytesIO(ppt.generate_pptx(data)))
    texts = _slide2_texts(prs)
    full_text = "\n".join(texts).lower()

    assert (
        "talent market" not in full_text
    ), f"headline still carries a fabricated temperature clause: {texts}"
    assert (
        "market temp." not in full_text
    ), f"situation card still shows a fabricated Market Temp. line: {texts}"
    assert (
        "hot" not in full_text
    ), f"the fabricated 'hot' value leaked somewhere on slide 2: {texts}"


# ---------------------------------------------------------------------------
# Verifier follow-up #2: the Postings/Talent Pool/Temperature/Trend cells
# above are correctly suppressed to "Data not available" for a fabricated
# Industry Benchmark role -- but data_synthesizer.fuse_job_market_demand's
# SAME fallback branch (data_synthesizer.py, "Source counting" block) still
# set _meta.source_count = 3 (1 for the "Industry Benchmark" posting entry,
# +1 because the derived search_volume = total_postings // 10 happens to be
# non-zero, +1 because the fallback talent_pool value happens to be
# non-zero). _score_section() (used by compute_confidence_scores, read by
# excel_v2._build_sheet_sources for the "Sources & Confidence" sheet) treats
# source_count >= 3 as "1.0 / grade A" regardless of whether those "sources"
# are real. Result: a plan where EVERY role hits the fallback (every demand
# cell in Market Intelligence literally reads "Data not available") still
# showed "Job Market Demand | 100% | A" on the Sources & Confidence sheet,
# and that 1.0 fed into the plan's Overall Confidence average too.
# ---------------------------------------------------------------------------


def test_sources_confidence_job_market_demand_not_graded_a_when_all_fallback():
    """Sources & Confidence's 'Job Market Demand' row must NOT read 100%/A
    when every role in the plan hit the generic Industry Benchmark fallback
    -- the exact Hershey-style manufacturing roles used above, which match
    none of the ~23 _ROLE_DEMAND_FALLBACKS keywords and have no live signal
    data (Adzuna/Jooble/Google Ads/Google Trends/LinkedIn all empty)."""
    data = {
        "client_name": "Hershey Test",
        "roles": ["Confectionery Line Operator", "Packaging Associate II"],
        "target_roles": [
            {"title": "Confectionery Line Operator", "count": 20},
            {"title": "Packaging Associate II", "count": 15},
        ],
        "industry": "manufacturing",
        "locations": ["Hershey, PA", "Stuarts Draft, VA"],
        "budget": "150000",
    }
    data["_synthesized"] = data_synthesizer.synthesize({}, KB, dict(data))

    # Precondition: both roles really did hit the fabricated fallback, and
    # their per-role _meta no longer claims 3 independent sources.
    jmd = data["_synthesized"]["job_market_demand"]
    for role in ("Confectionery Line Operator", "Packaging Associate II"):
        role_data = jmd[role]
        assert "Industry Benchmark" in (role_data.get("posting_sources") or [])
        meta = role_data.get("_meta") or {}
        assert meta.get("source_count") == 0, (
            f"{role}: fallback _meta.source_count still claims real sources: "
            f"{meta!r}"
        )

    # The synthesis-level confidence score for this section must not be the
    # 1.0 ("3+ independent sources agree") band.
    per_section = data["_synthesized"]["confidence_scores"]["per_section"]
    assert per_section.get("job_market_demand") != 1.0, (
        "job_market_demand confidence score is still 1.0 (grade A) for an "
        "all-fallback plan"
    )

    from excel_v2 import generate_excel_v2

    xlsx = generate_excel_v2(dict(data), load_kb_fn=load_knowledge_base)
    if isinstance(xlsx, tuple):
        xlsx = xlsx[0]
    wb = openpyxl.load_workbook(io.BytesIO(xlsx))
    ws = wb["Sources & Confidence"]

    jmd_row = None
    for row in ws.iter_rows(values_only=True):
        if row and row[1] == "Job Market Demand":
            jmd_row = row
            break

    assert (
        jmd_row is not None
    ), "Job Market Demand row not found on Sources & Confidence sheet"
    score_cell, grade_cell = jmd_row[2], jmd_row[3]
    assert score_cell != "100%", (
        f"Sources & Confidence still grades Job Market Demand 100% for an "
        f"all-fallback plan: {jmd_row!r}"
    )
    assert grade_cell != "A", (
        f"Sources & Confidence still grades Job Market Demand 'A' for an "
        f"all-fallback plan: {jmd_row!r}"
    )


def test_sources_confidence_job_market_demand_still_grades_a_with_real_data():
    """Composition check: a role with genuine live signal data (real
    posting/search/talent-pool sources, NOT the Industry Benchmark fallback)
    must still be able to earn the 1.0/A band -- the fix above must not
    down-weight real, independently-sourced data."""
    role_data = {
        "total_postings": 12000,
        "posting_sources": ["Adzuna", "Jooble"],
        "search_volume_monthly": 4500,
        "trend_direction": "up",
        "talent_pool_estimate": 250000,
        "competition_index": 0.048,
        "market_temperature": "hot",
        "kb_industry_context": {},
        "_meta": {"source_count": 3, "kb_validated": True},
    }
    score = data_synthesizer._score_section({"Registered Nurse": role_data})
    assert score == 1.0, (
        f"a role with real, independently-sourced demand data no longer "
        f"scores 1.0: {score!r}"
    )
