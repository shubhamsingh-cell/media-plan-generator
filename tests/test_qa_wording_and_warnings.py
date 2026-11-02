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
