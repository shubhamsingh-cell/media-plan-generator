"""QA findings say what to do, and warnings reach the results screen.

Design review 2026-11-03: input_role_missing and internal_draft_plan named
the problem but not the action, and the wizard only showed QA findings when
the gate was critical (the overlay), so a warning such as an internal draft
never reached the person downloading the plan.
"""

from __future__ import annotations

import bundle_qa
import plan_validator
from tests.wizard_js_harness import APP_JS, js_block


def test_a_missing_role_says_to_add_it_or_regenerate():
    data = {"input_roles": ["Welder", "Truck Driver"], "target_roles": ["Welder"]}
    found = bundle_qa.input_role_findings(data, deck_text="Welder", workbook_text="Welder")
    assert found
    assert all("Add the missing role or regenerate the plan." in f["message"] for f in found)


def test_an_internal_draft_says_what_to_complete_or_how_to_share():
    data = {"_synthesized": {"confidence_scores": {"overall": 0.31}}}
    (f,) = bundle_qa.internal_draft_findings(data)
    assert f["message"].startswith(plan_validator.internal_draft_stamp(data))
    assert (
        "Complete the salary, demand and location data, or share it as internal only."
        in f["message"]
    )


def test_warnings_are_listed_on_the_results_screen():
    src = APP_JS.read_text(encoding="utf-8")
    # the poll's warn findings travel with the payload to the results screen
    assert "payload._qa_warnings" in src
    dash = js_block(src, "function buildPlanDashboard(payload, serverData)")
    assert "payload._qa_warnings" in dash
    assert "Before you share" in dash
    assert "escapeHtml" in dash.split("_qa_warnings", 1)[1][:600]


# ---------------------------------------------------------------------------
# Re-check 2026-11-03 (blocker): only findings with user-facing copy reach the
# results screen; internal debug text (legacy_key=..., signal=...) never does.
# ---------------------------------------------------------------------------
import json  # noqa: E402

import pytest  # noqa: E402

from tests.wizard_js_harness import NODE, run_node  # noqa: E402

_CONFLICT = {
    "severity": "warn",
    "code": "industry_client_conflict",
    "message": (
        "Plan industry 'Blue Collar / Skilled Trades' (legacy_key=blue_collar_trades) "
        "conflicts with the industry implied by the client name/roles alone: "
        "'Transportation & Logistics' (legacy_key=logistics_supply_chain, "
        "signal='role_title')"
    ),
    "location": "",
}
_NO_COPY = {"severity": "warn", "code": "text_pattern_check_crashed",
            "message": "ValueError('x')", "location": ""}


def test_only_findings_with_user_copy_carry_a_user_message():
    draft = bundle_qa.internal_draft_findings(
        {"_synthesized": {"confidence_scores": {"overall": 0.31}}}
    )[0]
    summary = bundle_qa.summarize_findings([draft, _CONFLICT, _NO_COPY])
    by_code = {f["code"]: f for f in summary["findings"]}
    assert by_code["internal_draft_plan"]["user_message"] == draft["message"]
    assert by_code["industry_client_conflict"]["user_message"] == (
        "The industry you picked differs from what the roles suggest; "
        "check the industry before sharing."
    )
    assert not by_code["text_pattern_check_crashed"].get("user_message")
    for f in summary["findings"]:
        text = f.get("user_message") or ""
        assert "legacy_key" not in text and "signal=" not in text


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_the_results_screen_renders_only_user_copy():
    src = APP_JS.read_text(encoding="utf-8")
    fn = js_block(src, "function novaQaWarnings(pollData)")
    summary = bundle_qa.summarize_findings([_CONFLICT, _NO_COPY])
    shown = run_node(
        fn + "\nprocess.stdout.write(JSON.stringify(novaQaWarnings("
        + json.dumps({"qa_findings": summary["findings"]}) + ")));"
    )
    assert shown == [
        "The industry you picked differs from what the roles suggest; "
        "check the industry before sharing."
    ]
    assert not any("legacy_key" in s or "signal=" in s for s in shown)
