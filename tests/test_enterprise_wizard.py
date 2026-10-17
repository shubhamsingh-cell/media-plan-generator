"""Enterprise hourly hiring wizard section and its request boundary.

The section is collapsed and off by default; while off the wizard's payload is
exactly what it was before the section existed.  When on, the fields travel in
the /api/generate payload only (the live estimate never reads them) and the
server validates them (400 naming the field) and caps them.
"""

from __future__ import annotations

import re

import pytest

import app
import enterprise_pack as ep
import wizard_inputs
from tests.live_server import live_port, post_json  # noqa: F401 (fixture)
from tests.test_wizard_plan_inputs_parity import _LOCS, _ROLES, _STATE
from tests.wizard_js_harness import NODE, PARTIALS, wizard_payloads

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")
HTML = (PARTIALS / "body_content.html").read_text(encoding="utf-8")


def _section() -> str:
    i = HTML.index('id="enterpriseGroup"')
    return HTML[HTML.rindex("<details", 0, i) : HTML.index("</details>", i)]


def test_section_is_a_collapsed_details_with_the_toggle_off():
    sec = _section()
    open_tag = sec[: sec.index(">") + 1]
    assert " open" not in open_tag
    toggle = re.search(r'<input[^>]*id="enterpriseEnabled"[^>]*>', sec, re.S).group(0)
    assert "checked" not in toggle and 'type="checkbox"' in toggle
    assert re.search(r"<div id=\"enterpriseFields\" hidden>", sec)
    assert 'value="sunday" selected' in sec  # week start defaults to Sunday
    assert "Enterprise hourly hiring" in sec


def test_field_limits_mirror_the_server():
    sec = _section()
    lim = wizard_inputs.INPUT_LIMITS
    assert lim["enterprise_system_of_record_chars"] == ep.MAX_SYSTEM_OF_RECORD_CHARS
    assert lim["enterprise_setup_sla_max_days"] == ep.MAX_SETUP_SLA_DAYS
    assert f'maxlength="{lim["enterprise_system_of_record_chars"]}"' in sec
    assert f'max="{lim["enterprise_setup_sla_max_days"]}"' in sec


@needs_node
def test_payload_is_unchanged_while_the_toggle_is_off():
    kw = dict(selected_industry="blue_collar_trades", locations=_LOCS, roles=_ROLES)
    base = wizard_payloads(dict(_STATE), **kw)["generate"]
    # fields filled in but the toggle never switched on
    state = dict(
        _STATE,
        enterpriseWeekStart={"value": "monday"},
        enterpriseSystemOfRecord={"value": "ATS"},
        enterpriseSetupSla={"value": "5"},
    )
    got = wizard_payloads(state, **kw)
    assert got["generate"] == base
    assert not [k for k in got["generate"] if k.startswith("enterprise_")]
    assert not [k for k in got["estimate"] if k.startswith("enterprise_")]


@needs_node
def test_payload_carries_the_fields_when_on_and_the_estimate_does_not():
    state = dict(
        _STATE,
        enterpriseEnabled={"checked": True},
        enterpriseWeekStart={"value": "monday"},
        enterpriseSystemOfRecord={"value": "  client ATS  "},
        enterpriseLockDay={"value": "wednesday"},
        enterpriseSetupSla={"value": "3"},
    )
    got = wizard_payloads(
        state, selected_industry="blue_collar_trades", locations=_LOCS, roles=_ROLES
    )
    gen = got["generate"]
    assert gen["enterprise_pack"] is True
    assert gen["enterprise_week_start"] == "monday"
    assert gen["enterprise_system_of_record"] == "client ATS"
    assert gen["enterprise_request_lock_day"] == "wednesday"
    assert gen["enterprise_setup_sla_days"] == 3
    assert not [k for k in got["estimate"] if k.startswith("enterprise_")]
    # what the page sends passes the server's own validation
    assert ep.request_problem(gen) == ("", "")


@needs_node
@pytest.mark.parametrize(
    "typed,sent", [("99", 99), ("-4", -4), ("abc", None), ("", None), ("7", 7)]
)
def test_sla_is_sent_as_typed_never_clamped(typed, sent):
    state = dict(
        _STATE, enterpriseEnabled={"checked": True}, enterpriseSetupSla={"value": typed}
    )
    gen = wizard_payloads(
        state, selected_industry="blue_collar_trades", locations=_LOCS, roles=_ROLES
    )["generate"]
    if sent is None:
        assert "enterprise_setup_sla_days" not in gen
    else:
        assert gen["enterprise_setup_sla_days"] == sent  # the server decides


def test_request_sanitizer_keeps_the_boolean_and_strips_markup():
    clean = app._sanitize_generate_request(
        {
            "enterprise_pack": True,
            "enterprise_system_of_record": "<b>ATS</b>",
            "enterprise_setup_sla_days": 4,
        }
    )
    assert clean["enterprise_pack"] is True
    assert clean["enterprise_system_of_record"] == "ATS"
    assert (
        clean["enterprise_setup_sla_days"] == 4
        or clean["enterprise_setup_sla_days"] == "4"
    )


def _generate_payload(**over) -> dict:
    base = {
        "requester_name": "QA Bot",
        "requester_email": "qa@example.com",
        "client_name": "Example Client",
        "use_case": "Hiring associates",
        "target_roles": ["Warehouse Associate"],
        "locations": ["Houston, TX"],
        "budget_range": "abc",  # anything that passes validation stops at the budget
        "campaign_duration": "3 months",
        "enterprise_pack": True,
    }
    base.update(over)
    return base


@pytest.mark.parametrize(
    "over,field",
    [
        ({"enterprise_week_start": "someday"}, "enterprise_week_start"),
        ({"enterprise_request_lock_day": "9"}, "enterprise_request_lock_day"),
        ({"enterprise_setup_sla_days": 31}, "enterprise_setup_sla_days"),
        ({"enterprise_setup_sla_days": "abc"}, "enterprise_setup_sla_days"),
        (
            {"enterprise_system_of_record": "x" * (ep.MAX_SYSTEM_OF_RECORD_CHARS + 1)},
            "enterprise_system_of_record",
        ),
    ],
)
def test_generate_400_names_the_enterprise_field(live_port, over, field):  # noqa: F811
    status, body = post_json(live_port, "/api/generate", _generate_payload(**over))
    assert status == 400 and body.get("field") == field, (status, body)


def test_generate_accepts_valid_enterprise_fields_past_the_check(
    live_port,
):  # noqa: F811
    status, body = post_json(
        live_port,
        "/api/generate",
        _generate_payload(
            enterprise_week_start="Monday",
            enterprise_setup_sla_days="5",
            enterprise_system_of_record="client ATS",
        ),
    )
    # stops at the (deliberately invalid) budget, not at an enterprise field
    assert status == 400 and not str(body.get("field", "")).startswith(
        "enterprise"
    ), body


def test_no_client_names_in_the_wizard_section():
    import hashlib

    from tests.test_enterprise_pack import _BANNED_SHA256

    # One hash is a CSS layout keyword used inline in the section, not a name.
    banned = _BANNED_SHA256 - {
        "222f930b87524636da58b1987eabd52f1285cb0ef7dad72f28fcd026fbb3f73f"
    }
    js = (PARTIALS / "body_app_js.html").read_text(encoding="utf-8")
    start = js.index("// ── Enterprise hourly hiring")
    block = js[start : js.index("document.addEventListener", start + 400)]
    text = (_section() + block).lower()
    hits = [
        w
        for w in set(re.findall(r"[a-z0-9]+", text))
        if hashlib.sha256(w.encode()).hexdigest() in banned
    ]
    assert not hits


# ---------------------------------------------------------------------------
# Demand-first: site goals + the goal-plan callout
# ---------------------------------------------------------------------------
_KW = dict(selected_industry="blue_collar_trades", locations=_LOCS, roles=_ROLES)


def test_site_goal_limits_mirror_the_server():
    import demand_planning as dp

    lim = wizard_inputs.INPUT_LIMITS
    assert lim["enterprise_site_goal_max"] == dp.MAX_GOAL
    assert lim["enterprise_site_goals_max"] == dp.MAX_SITE_GOALS
    js = (PARTIALS / "body_app_js.html").read_text(encoding="utf-8")
    assert f"const NOVA_SITE_GOAL_MAX = {dp.MAX_GOAL};" in js
    assert f"const NOVA_SITE_GOAL_SITES = {dp.MAX_SITE_GOALS};" in js


def test_section_has_a_site_goals_table_and_the_preview_a_callout():
    sec = _section()
    assert 'id="siteGoalsRows"' in sec and 'id="siteGoals-error"' in sec
    assert 'id="npGoalPlan"' in HTML
    prev = (PARTIALS / "body_preview_js.html").read_text(encoding="utf-8")
    assert "est.goal_plan" in prev and "gp.text.callout" in prev


@needs_node
def test_generate_carries_only_valid_goals_for_current_locations():
    state = dict(_STATE, enterpriseEnabled={"checked": True})
    goals = {
        "Zurich, Switzerland": "120",
        "Toronto, ON": "abc",  # never coerced: dropped from the payload
        "Sao Paulo, Brazil": "0",  # no goal
        "Removed Site": "50",  # no longer in the plan
    }
    gen = wizard_payloads(state, site_goals=goals, **_KW)["generate"]
    assert gen["site_goals"] == {"Zurich, Switzerland": 120}
    assert ep.request_problem(gen) == ("", "")


@needs_node
def test_site_goals_travel_only_while_the_toggle_is_on():
    goals = {"Zurich, Switzerland": "120"}
    got = wizard_payloads(dict(_STATE), site_goals=goals, **_KW)
    assert "site_goals" not in got["generate"]
    assert "site_goals" not in got["estimate"]
    assert "enterprise_pack" not in got["estimate"]


@needs_node
def test_estimate_gains_goal_fields_only_when_on_with_a_goal():
    base = wizard_payloads(dict(_STATE), **_KW)["estimate"]
    on_no_goal = wizard_payloads(
        dict(_STATE, enterpriseEnabled={"checked": True}), **_KW
    )["estimate"]
    assert on_no_goal == base  # the pack alone changes nothing
    with_sites = wizard_payloads(
        dict(_STATE, enterpriseEnabled={"checked": True}),
        site_goals={"Toronto, ON": "40"},
        **_KW,
    )["estimate"]
    assert with_sites["enterprise_pack"] is True
    assert with_sites["site_goals"] == {"Toronto, ON": 40}
    assert {k: v for k, v in with_sites.items() if k not in (
        "enterprise_pack", "site_goals")} == base
    with_volume = wizard_payloads(
        dict(
            _STATE,
            enterpriseEnabled={"checked": True},
            hireVolume={"value": "__exact__"},
            exactHires={"value": "250"},
        ),
        **_KW,
    )["estimate"]
    assert with_volume["hire_volume"] == "250 hires"
    assert with_volume["enterprise_pack"] is True
    # what the estimate sends is accepted by the server's own validation
    assert ep.request_problem(with_sites) == ("", "")


@needs_node
@pytest.mark.parametrize(
    "goals,fragment",
    [
        ({"Zurich, Switzerland": "1.5"}, "whole number"),
        ({"Zurich, Switzerland": "100001"}, "cannot exceed"),
        ({"Zurich, Switzerland": "60000", "Toronto, ON": "60000"}, "add up to more"),
        ({"Zurich, Switzerland": "10"}, ""),
    ],
)
def test_site_goal_validation_message(goals, fragment):
    from tests.wizard_js_harness import run_node, js_block, APP_JS

    src = APP_JS.read_text(encoding="utf-8")
    script = "\n".join(
        [
            "var locations = " + __import__("json").dumps(_LOCS) + ";",
            "let novaSiteGoals = " + __import__("json").dumps(goals) + ";",
            "const NOVA_SITE_GOAL_MAX = 100000, NOVA_SITE_GOAL_SITES = 40;",
            js_block(src, "function novaSiteGoalsMessage()"),
            "process.stdout.write(JSON.stringify({m: novaSiteGoalsMessage()}));",
        ]
    )
    msg = run_node(script)["m"]
    assert (fragment in msg) if fragment else msg == ""


def test_generate_400_names_site_goals(live_port):  # noqa: F811
    status, body = post_json(
        live_port,
        "/api/generate",
        _generate_payload(site_goals={"Houston, TX": -3}),
    )
    assert status == 400 and body.get("field") == "site_goals", (status, body)


def test_generate_accepts_valid_site_goals_past_the_check(live_port):  # noqa: F811
    status, body = post_json(
        live_port,
        "/api/generate",
        _generate_payload(site_goals={"Houston, TX": 30}),
    )
    assert status == 400 and body.get("field") != "site_goals", body
