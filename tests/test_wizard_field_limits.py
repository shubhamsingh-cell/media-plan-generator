"""Field limits are checked where the user types, with the server's own
numbers, and every server 400 names its field (wizard audit D-11,
2026-10-01).

Pre-fix: step 1 "Continue" only checked the budget was non-empty (-5, abc,
2B passed to step 5 and failed at Generate as a transient toast whose
inline error sat on hidden step 1); no client-side limits at all for client
name / use case / roles / competitors / uploads (the UI promised "max 10MB
each" while an 8 MB file then broke the 10 MB request with a 413); and the
server 400s ("client_name exceeds 200 character limit") carried no field.

Now the limits live in wizard_inputs.INPUT_LIMITS, are embedded into the
page, and the server's 400s carry ``field``.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import wizard_inputs
from tests.live_server import live_port, post_json  # noqa: F401 (fixture)
from tests.wizard_js_harness import APP_JS, NODE, inputs_functions_js, js_block, run_node

LIMITS = wizard_inputs.INPUT_LIMITS
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")


def _base_payload(tag: str) -> dict:
    return {
        "requester_name": "QA Bot",
        "requester_email": "qa@joveo.com",
        "client_name": f"Limits Co {tag}",
        "use_case": "Hiring welders",
        "target_roles": ["Welder"],
        "locations": ["Houston, TX"],
        "budget_range": "abc",  # any request that passes the limit check stops here
        "campaign_duration": "3 months",
    }


@pytest.mark.parametrize(
    "tag,override,field",
    [
        ("client", {"client_name": "Acme " + "x" * LIMITS["client_name_max_chars"]}, "client_name"),
        ("usecase", {"use_case": "y" * (LIMITS["use_case_max_chars"] + 1)}, "use_case"),
        ("transcript", {"call_transcript": "z" * (LIMITS["call_transcript_max_chars"] + 1)}, "call_transcript"),
        ("competitors", {"competitors": [f"Comp {i}" for i in range(LIMITS["competitors_max"] + 1)]}, "competitors"),
        ("roles", {"target_roles": [f"Role {i}" for i in range(LIMITS["roles_max"] + 1)]}, "target_roles"),
        ("locations", {"locations": ["Austin, TX"] * (LIMITS["locations_max"] + 1)}, "locations"),
        ("budget", {}, "budget_range"),
    ],
)
def test_generate_400_names_the_field(live_port, tag, override, field):  # noqa: F811
    payload = dict(_base_payload(tag), **override)
    status, body = post_json(live_port, "/api/generate", payload)
    assert status == 400, (status, body)
    assert body.get("field") == field, body
    assert body["error"]


def test_limits_are_one_table_shared_with_the_page():
    import template_composer

    html = template_composer.compose_template("index").decode("utf-8")
    start = html.index('<script type="application/json" id="novaWizardInputs">')
    start = html.index(">", start) + 1
    embedded = json.loads(html[start : html.index("</script>", start)])
    assert embedded["limits"] == LIMITS


def test_upload_hint_matches_the_upload_limit():
    content = (APP_JS.parent / "body_content.html").read_text(encoding="utf-8")
    mb = LIMITS["upload_total_max_bytes"] // (1024 * 1024)
    assert f"up to {mb} MB in total across all uploads" in content
    assert "max 10MB each" not in content
    # base64 (x4/3) of the upload allowance must fit the request cap
    assert LIMITS["upload_total_max_bytes"] * 4 / 3 < LIMITS["request_max_bytes"]


def _limit_js(state_values: dict, files: list, call: str):
    src = APP_JS.read_text(encoding="utf-8")
    names = [
        "function validateBudgetInput(",
        "function novaLimit(",
        "var NOVA_TEXT_LIMITS = {",
        "function novaTextLimitError(",
        "var NOVA_TAG_LIMITS = {",
        "function novaTagLimitReached(",
        "function novaUploadRoomError(",
    ]
    blocks = [js_block(src, n) + (";" if n.startswith("var ") else "") for n in names]
    script = "\n".join(
        [
            inputs_functions_js(),
            "var VALUES = " + json.dumps(state_values) + ";",
            "var shown = [];",
            "var document = { getElementById: function (id) {",
            "  return { id: id, value: VALUES[id] || '' }; } };",
            "function _showFieldError(el, msg) { shown.push([el.id, msg]); }",
            "var briefFiles = " + json.dumps(files) + ", transcriptFiles = [], historicalFiles = [];",
            *blocks,
            "var result = " + call + ";",
            "process.stdout.write(JSON.stringify({ result: result, shown: shown }));",
        ]
    )
    return run_node(script)


@needs_node
@pytest.mark.parametrize(
    "budget,period,duration,valid,error",
    [
        ("-5", "campaign", "3 months", False, "negative"),
        ("0", "campaign", "3 months", False, "zero"),
        ("abc", "campaign", "3 months", False, "not_a_number"),
        ("2B", "campaign", "3 months", False, "too_large"),
        ("1.5", "campaign", "3 months", False, "too_small"),
        ("$200M", "monthly", "12 months", False, "too_large"),
        ("1.5 million", "campaign", "3 months", True, ""),
        ("$10,000", "monthly", "6-12 months", True, ""),
    ],
)
def test_budget_is_validated_at_the_field_with_the_server_rules(budget, period, duration, valid, error):
    got = _limit_js(
        {"budgetPeriod": period, "campaignDuration": duration}, [],
        "validateBudgetInput(" + json.dumps(budget) + ")",
    )["result"]
    assert got["valid"] is valid
    if not valid:
        assert got["error"] == wizard_inputs.BUDGET_ERROR_MESSAGES[error]
        # the server says the same about the same input
        plan = wizard_inputs.resolve_plan_budget(budget, period, duration)
        assert plan.error == error


@needs_node
def test_text_limits_match_the_server_wording():
    n = LIMITS["client_name_max_chars"] + 1
    got = _limit_js({"clientName": "x" * n}, [], "novaTextLimitError('clientName')")
    assert got["result"] == f"Client name is too long ({n} characters; max {LIMITS['client_name_max_chars']})."
    ok = _limit_js({"clientName": "x" * (n - 1)}, [], "novaTextLimitError('clientName')")
    assert ok["result"] == ""


@needs_node
def test_role_and_competitor_counts_stop_at_the_limit():
    roles = [f"r{i}" for i in range(LIMITS["roles_max"])]
    got = _limit_js({}, [], "novaTagLimitReached('rolesContainer', " + json.dumps(roles) + ")")
    assert got["result"] is True
    assert got["shown"] == [["roleInput", f"You can add up to {LIMITS['roles_max']} target roles per plan."]]
    fewer = _limit_js({}, [], "novaTagLimitReached('rolesContainer', " + json.dumps(roles[:-1]) + ")")
    assert fewer["result"] is False
    comps = [f"c{i}" for i in range(LIMITS["competitors_max"])]
    got = _limit_js({}, [], "novaTagLimitReached('competitorsContainer', " + json.dumps(comps) + ")")
    assert got["result"] is True


@needs_node
def test_upload_total_is_checked_before_generate():
    cap = LIMITS["upload_total_max_bytes"]
    already = [{"name": "rfp.pdf", "size": cap - 1024}]
    got = _limit_js({}, already, "novaUploadRoomError({ name: 'brief.docx', size: 4096 })")
    assert got["result"].startswith("brief.docx doesn't fit: attachments must total under 7 MB")
    fits = _limit_js({}, already, "novaUploadRoomError({ name: 'note.txt', size: 512 })")
    assert fits["result"] == ""


def test_step_one_continue_runs_the_full_budget_check():
    src = APP_JS.read_text(encoding="utf-8")
    step = src[src.index("function nextStep()") :]
    step = step[: step.index("if (currentStep === 2 && !selectedIndustry)")]
    assert "validateBudgetInput(" in step
    assert "novaCheckTextLimit(id)" in step
