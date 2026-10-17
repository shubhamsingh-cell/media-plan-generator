"""Nova's media_plan answer gives each channel a grounded "Why".

The response template had no "why" at all. The budget engine already computes
cpa, roi_score, trend_direction and efficiency_flag per channel; the tool
result now carries a ``why`` built only from those fields, the template tells
the model to copy it, and a channel with no supporting field gets none.
"""

from __future__ import annotations

import budget_engine
import nova


def _entry(**kw):
    base = {
        "dollar_amount": 10000.0,
        "projected_applications": 1000,
        "cpa": 10.0,
        "roi_score": 5,
        "channel_role": "performance",
        "efficiency_flag": "",
    }
    base.update(kw)
    return base


def test_cheap_channel_cites_cpa_vs_plan_average():
    why = budget_engine.channel_why(_entry(cpa=7.0), plan_avg_cpa=10.0)
    assert why == "CPA 30% below the plan average"


def test_expensive_channel_cites_cpa_above_average():
    why = budget_engine.channel_why(_entry(cpa=15.0), plan_avg_cpa=10.0)
    assert why == "CPA 50% above the plan average"


def test_roi_and_trend_are_cited_strongest_first_max_two():
    why = budget_engine.channel_why(
        _entry(cpa=7.0, roi_score=9, trend_direction="rising", trend_pct_yoy=9.2),
        plan_avg_cpa=10.0,
    )
    assert why == "CPA 30% below the plan average; ROI score 9/10"


def test_efficiency_flag_leads():
    why = budget_engine.channel_why(
        _entry(efficiency_flag="Low Efficiency", roi_score=1), plan_avg_cpa=None
    )
    assert why.startswith("Flagged Low Efficiency")


def test_no_supporting_field_means_no_reason():
    # Mid ROI (5 = unknown), CPA near average, stable trend, no flag.
    assert (
        budget_engine.channel_why(
            _entry(cpa=10.5, roi_score=5, trend_direction="stable", trend_pct_yoy=1.0),
            plan_avg_cpa=10.0,
        )
        == ""
    )
    assert budget_engine.channel_why({}, None) == ""
    assert budget_engine.channel_why(None, None) == ""  # type: ignore[arg-type]


def test_brand_channel_uses_the_engines_own_rationale_not_cpa():
    why = budget_engine.channel_why(
        _entry(channel_role="brand", rationale="Brand investment", cpa=99.0, roi_score=1),
        plan_avg_cpa=10.0,
    )
    assert why == "Brand investment"
    assert budget_engine.channel_why(_entry(channel_role="brand"), 10.0) == ""


def test_annotate_adds_why_only_where_supported_and_excludes_brand_from_average():
    allocs = {
        "A": _entry(cpa=5.0, dollar_amount=5000.0, projected_applications=1000),
        "B": _entry(cpa=15.0, dollar_amount=15000.0, projected_applications=1000),
        "C": _entry(cpa=10.0, dollar_amount=10000.0, projected_applications=1000),
        "Brand": _entry(
            channel_role="brand", cpa=500.0, dollar_amount=99999.0,
            projected_applications=10,
        ),
    }
    # weighted average over A, B, C = 30000 / 3000 = 10.0 (brand ignored)
    assert budget_engine.plan_average_cpa(allocs) == 10.0
    budget_engine.annotate_channel_reasons(allocs)
    # the annotated column is the client-safe reason: budget share only here
    # (no comparison with the plan average, no ROI score, no flag)
    assert "why" not in allocs["A"] and "why" not in allocs["B"]  # no share carried
    allocs["A"]["percentage"] = 12.5
    budget_engine.annotate_channel_reasons(allocs)
    assert allocs["A"]["why"] == "12.5% of budget"
    assert "why" not in allocs["Brand"]  # no rationale or share supplied -> nothing invented


def test_media_plan_template_has_a_why_column_and_forbids_inventing():
    template = nova._RESPONSE_TEMPLATES["media_plan"]
    assert "| Projected Hires | Why |" in template
    assert "Never invent a reason" in template


def test_budget_projection_tool_result_carries_why_from_real_engine_fields():
    n = nova.Nova.__new__(nova.Nova)
    n._data_cache = {
        "knowledge_base": {},
        "international_benchmarks": {"countries": {}},
    }
    result = n._query_budget_projection(
        {
            "budget": 100000,
            "roles": ["Registered Nurse"],
            "locations": ["Dallas, TX"],
            "industry": "healthcare",
        }
    )
    allocs = result["channel_allocations"]
    assert allocs
    reasons = {k: v.get("why") for k, v in allocs.items() if v.get("why")}
    assert reasons, "no channel got a grounded reason"
    for name, why in reasons.items():
        entry = allocs[name]
        grounded = why.startswith(f"{entry['percentage']:g}% of budget")
        assert grounded, (name, why)
        assert not any(w in why for w in ("ROI", "Flagged", "plan average")), why
    # a channel whose fields support nothing is left without a why
    for name, entry in allocs.items():
        if "why" not in entry:
            assert budget_engine.channel_reason_client(entry) == ""
