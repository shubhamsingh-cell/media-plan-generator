"""The results screen echoes the budget the plan was generated for, not the
typed text (independent verifier on 2ad8904, 2026-10-01).

Pre-fix: $10,000/month x "6-12 months" generated a $90,000 plan but the
results dashboard's TOTAL BUDGET (and the saved campaign, recent-plans entry
and the "Ask Nova about this plan" context) showed the raw "$10,000" --
buildPlanDashboard fell back to payload.budget_range. The async submit now
returns the resolved budget (app._plan_budget_summary) and every echo reads
it through novaPlanBudgetLabel.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import app
from tests.wizard_js_harness import APP_JS, NODE, inputs_functions_js, js_block, run_node


def _normalized(budget: str, period: str, duration: str, locations=None) -> dict:
    data = {
        "budget_range": budget,
        "budget_period": period,
        "campaign_duration": duration,
        "locations": locations or ["Hershey, PA"],
    }
    app._normalize_request_budget(data, log=False)
    return data


def test_summary_is_the_resolved_total_in_plan_currency():
    s = app._plan_budget_summary(_normalized("$10,000", "monthly", "6-12 months"))
    assert s["total"] == 90000 and s["display"] == "$90,000" and s["currency"] == "USD"
    assert (s["amount"], s["multiplier"], s["months"]) == (10000, 9, 9)
    gbp = app._plan_budget_summary(_normalized("£10,000", "monthly", "6 months", ["London, UK"]))
    assert gbp["display"] == "£60,000" and gbp["currency"] == "GBP"


def test_normalized_budget_is_never_scaled_twice():
    data = _normalized("$10,000", "monthly", "6-12 months")
    assert app._resolve_request_budget(data).total == 90000
    app._normalize_request_budget(data, log=False)  # idempotent
    assert data["budget"] == "$90,000"
    assert data["_budget_input_raw"] == "$10,000"


def test_async_submit_returns_the_resolved_budget():
    src = (Path(__file__).resolve().parent.parent / "app.py").read_text(encoding="utf-8")
    start = src.index('"poll_url": f"/api/jobs/{job_id}",')
    assert '"budget": _plan_budget_summary(data),' in src[start : start + 400]


def _label(payload: dict, currency=None):
    src = APP_JS.read_text(encoding="utf-8")
    script = "\n".join(
        [
            inputs_functions_js(),
            "function novaPlanCurrency() { return " + json.dumps(currency) + "; }",
            js_block(src, "function novaPlanBudgetLabel("),
            "process.stdout.write(JSON.stringify(novaPlanBudgetLabel("
            + json.dumps(payload)
            + ")));",
        ]
    )
    return run_node(script)


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_results_label_prefers_the_server_total_and_never_the_typed_text():
    typed = {"budget_range": "$10,000", "budget_period": "monthly", "campaign_duration": "6-12 months"}
    assert _label(dict(typed, _plan_budget={"display": "$90,000"})) == "$90,000"
    # no server answer: the same reading, computed locally -- still $90,000
    assert _label(typed, {"symbol": "$"}) == "$90,000"
    assert _label(dict(typed, budget_range="1.5 million", budget_period="campaign")) == "$1,500,000"


def test_every_results_echo_uses_the_label():
    src = APP_JS.read_text(encoding="utf-8")
    dash = js_block(src, "function buildPlanDashboard(")
    assert "novaPlanBudgetLabel(payload)" in dash
    assert "payload.budget_range ||" not in src  # no raw-text echo left
    assert src.count("novaPlanBudgetLabel(") >= 5  # def + dashboard, ask-Nova, save, recent
