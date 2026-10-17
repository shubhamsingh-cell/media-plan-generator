"""Wizard mechanism fixes (design-judge mechanism lens, prod e900b0f).

W1  Target Region follows the locations unless the user chose one.
W2  The review step never prints the "__exact__" sentinel.
W3  The budget reading never shows a bare, currency-less figure.
W4  The enterprise section's state is visible (review line, "On" badge) and a
    restored out-of-range SLA shows its inline error.

The page's own functions are lifted from the templates and run under node
against a stub DOM, so a regression in the shipped code fails here.
"""

from __future__ import annotations

import http.client
import json
import logging
import re
import time

import pytest

import app
from tests.live_server import live_port  # noqa: F401 (fixture)
from tests.wizard_js_harness import (
    APP_JS,
    NODE,
    inputs_functions_js,
    js_block,
    run_node,
    slice_between,
)

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")
SRC = APP_JS.read_text(encoding="utf-8")


# ── W1: Target Region follows the locations ──────────────────────────────

_REGION_STUB = """
var handlers = [];
var sel = { id: 'regionSelector', value: 'us_only',
  options: ['us_only', 'global', 'emea', 'apac', 'custom'].map(function (v) { return { value: v }; }),
  style: {}, addEventListener: function (t, fn) { if (t === 'change') handlers.push(fn); },
  dispatchEvent: function (e) { handlers.forEach(function (h) { h(e); }); return true; } };
var hint = { id: 'regionSelector-hint', textContent: 'Choose a region to filter benchmarks' };
var els = { regionSelector: sel, 'regionSelector-hint': hint, customCountriesWrap: { style: {} } };
var document = {
  getElementById: function (id) { return els[id] || (els[id] = { id: id, style: {}, checked: false }); },
  querySelectorAll: function () { return []; },
};
function Event(type) { this.type = type; }
"""


def _region_script(steps: str) -> str:
    lifted = slice_between(
        SRC, "var _regionUserPicked = false;", "function getSelectedRegion()"
    )
    return "\n".join(
        [
            _REGION_STUB,
            lifted,
            "initRegionSelector();",
            "function resolved(hints) { return novaApplyDerivedRegion(novaRegionFromMarkets(hints)); }",
            "function userPicks(v) { sel.value = v; sel.dispatchEvent(new Event('change')); }",
            "var log = [];",
            steps,
            "process.stdout.write(JSON.stringify(log));",
        ]
    )


@needs_node
@pytest.mark.parametrize(
    "hints, expected",
    [
        (["emea"], "emea"),  # Berlin, Germany
        (["us"], "us_only"),  # Dallas, TX
        (["apac"], "apac"),
        (["us", "emea"], "global"),  # Dallas + Berlin
        (["emea", "apac"], "global"),
        (["americas"], "global"),  # Toronto: no US-only plan
        (["emea", "none"], "emea"),  # "Remote" names no market
    ],
)
def test_region_follows_the_resolved_locations(hints, expected):
    out = run_node(
        _region_script(f"resolved({json.dumps(hints)}); log.push(sel.value);")
    )
    assert out == [expected]


@needs_node
def test_unplaceable_or_remote_only_locations_leave_the_selector_alone():
    out = run_node(
        _region_script(
            "resolved(['emea']); log.push(sel.value);"
            "resolved(['emea', 'unknown']); log.push(sel.value);"  # a guess never moves it
            "resolved(['none']); log.push(sel.value);"
        )
    )
    assert out == ["emea", "emea", "emea"]


@needs_node
def test_region_rederives_when_the_locations_change_and_resets_when_cleared():
    out = run_node(
        _region_script(
            "resolved(['emea']); log.push(sel.value);"
            "resolved(['us']); log.push(sel.value);"  # Berlin removed, Dallas added
            "resolved(['apac']); log.push(sel.value);"
            "resolved([]); log.push(sel.value);"  # all locations removed
        )
    )
    assert out == ["emea", "us_only", "apac", "us_only"]


@needs_node
def test_a_value_the_user_picked_survives_a_location_change():
    out = run_node(
        _region_script(
            "userPicks('global'); log.push(sel.value);"
            "resolved(['emea']); log.push(sel.value);"
            "resolved(['us']); log.push(sel.value);"
            "resolved([]); log.push(sel.value);"
        )
    )
    assert out == ["global", "global", "global", "global"]


@needs_node
def test_picking_the_default_back_is_still_a_choice():
    out = run_node(
        _region_script(
            "resolved(['emea']); userPicks('us_only');"
            "resolved(['apac']); log.push(sel.value);"
        )
    )
    assert out == ["us_only"]


@needs_node
def test_the_derived_value_is_explained_in_the_hint_and_the_hint_reverts():
    out = run_node(
        _region_script(
            "resolved(['emea']); log.push(hint.textContent);"
            "userPicks('apac'); log.push(hint.textContent);"
        )
    )
    assert out[0] == "Set from your locations. Choose another region to override."
    assert out[1] == "Choose a region to filter benchmarks"


@needs_node
def test_a_derived_change_refires_the_live_estimate_like_any_change():
    # The preview schedules its estimate on a document 'change' event; the
    # derived value must be announced the same way (bubbling change).
    out = run_node(
        _region_script(
            "var fired = 0; sel.addEventListener('change', function () { fired++; });"
            "resolved(['emea']); log.push(fired); resolved(['emea']); log.push(fired);"
        )
    )
    assert out == [1, 1]  # once for the change; none when nothing changed


def test_resolution_response_drives_the_selector_and_saved_state_keeps_the_flag():
    fn = js_block(SRC, "function runLocationResolution(")
    assert "novaApplyDerivedRegion(novaRegionFromMarkets(data.market_regions))" in fn
    assert "data.market_regions.length === sentLocations.length" in fn
    assert "novaApplyDerivedRegion(novaRegionFromMarkets([]))" in fn  # cleared list
    assert "regionPicked: typeof _regionUserPicked" in SRC
    restore = js_block(SRC, "function novaRestoreRegion(")
    assert "s.regionPicked" in restore


def test_location_resolve_endpoint_returns_one_market_hint_per_location(
    live_port,
):  # noqa: F811
    # same double-submit CSRF token shape tests/test_location_integration.py mints
    token = f"mech-test-token.{int(time.time()) + 3600}"
    conn = http.client.HTTPConnection("127.0.0.1", live_port, timeout=30)
    try:
        conn.request(
            "POST",
            "/api/locations/resolve",
            body=json.dumps(
                {
                    "locations": [
                        "Berlin, Germany",
                        "Dallas, TX",
                        "Mumbai, India",
                        "Remote",
                        "Dallas",
                    ]
                }
            ),
            headers={
                "Content-Type": "application/json",
                "Origin": "http://localhost",
                "Cookie": f"csrf_token={token}",
                "X-CSRF-Token": token,
            },
        )
        resp = conn.getresponse()
        raw = resp.read()
    finally:
        conn.close()
    assert resp.status == 200, raw
    assert json.loads(raw)["market_regions"] == [
        "emea",
        "us",
        "apac",
        "none",
        "unknown",
    ]


@pytest.mark.parametrize(
    "entry, hint",
    [
        ("Berlin, Germany", "emea"),
        ("London, UK", "emea"),
        ("Sydney, Australia", "apac"),
        ("Dallas, TX", "us"),
        ("Austin, Texas, USA", "us"),
        ("Toronto, Canada", "americas"),
        ("Worldwide", "unknown"),
        ("Accra, Ghana", "unknown"),
    ],
)
def test_location_market_hint(entry, hint):
    assert app._location_market_hint(entry) == hint


def test_us_only_with_no_us_location_logs_one_warning(caplog):
    brief = {
        "target_region": "us_only",
        "locations": ["Berlin, Germany", "Paris, France"],
    }
    with caplog.at_level(logging.WARNING):
        app._warn_us_only_without_us_market(brief, "/api/estimate")
    msgs = [r.getMessage() for r in caplog.records if "us_only" in r.getMessage()]
    assert len(msgs) == 1 and "Berlin, Germany" in msgs[0] and "emea" in msgs[0]


@pytest.mark.parametrize(
    "brief",
    [
        {"target_region": "us_only", "locations": ["Dallas, TX"]},
        {"target_region": "us_only", "locations": ["Dallas, TX", "Berlin, Germany"]},
        {
            "target_region": "us_only",
            "locations": ["Berlin, Germany", "Dallas"],
        },  # a guess: silent
        {"target_region": "us_only", "locations": []},
        {"target_region": "emea", "locations": ["Berlin, Germany"]},
    ],
)
def test_us_only_warning_stays_quiet_when_a_us_market_exists_or_cannot_be_ruled_out(
    caplog, brief
):
    with caplog.at_level(logging.WARNING):
        app._warn_us_only_without_us_market(brief, "/api/estimate")
    assert not [r for r in caplog.records if "us_only" in r.getMessage()]


# ── W2: "__exact__" never reaches rendered text ───────────────────────────


def _hire_label(volume: str, exact: str):
    script = "\n".join(
        [
            "var ELS = { hireVolume: { value: %s }, exactHires: { value: %s } };"
            % (json.dumps(volume), json.dumps(exact)),
            "var document = { getElementById: function (id) { return ELS[id] || null; } };",
            js_block(SRC, "function novaHireVolumeLabel()"),
            "process.stdout.write(JSON.stringify(novaHireVolumeLabel()));",
        ]
    )
    return run_node(script)


@needs_node
@pytest.mark.parametrize(
    "volume, exact, label",
    [
        ("__exact__", "40", "40 hires"),
        ("__exact__", " 250 ", "250 hires"),
        ("__exact__", "1", "1 hire"),
        ("__exact__", "", ""),
        ("10-50 hires", "", "10-50 hires"),
        ("", "", ""),
    ],
)
def test_review_hire_volume_label(volume, exact, label):
    out = _hire_label(volume, exact)
    assert out == label and "__exact__" not in out


def test_the_sentinel_is_only_ever_compared_or_assigned_never_rendered():
    review = js_block(SRC, "function populateReview(")
    assert "__exact__" not in review
    assert 'getElementById("hireVolume").value || "Not specified"' not in review
    # no line that sets rendered text may also read the sentinel: a leak is
    # `el.textContent = <hireVolume>.value || ...` on the same statement
    for path in sorted(APP_JS.parent.glob("*.html")):
        text = path.read_text(encoding="utf-8")
        for m in re.finditer(r"(textContent|innerText|innerHTML)\s*=[^;]*", text):
            assert "__exact__" not in m.group(0), f"{path.name}: {m.group(0)[:100]}"
        # ...and a rendered read of the raw select value must go through the label
        for m in re.finditer(
            r"(textContent|innerText)\s*=\s*[^;]*getElementById\(\"hireVolume\"\)\.value",
            text,
        ):
            raise AssertionError(f"{path.name}: {m.group(0)[:100]}")


# ── W3: the budget reading always states its currency ────────────────────


def _reading(raw: str, cur):
    script = "\n".join(
        [
            inputs_functions_js(),
            "var cur = %s;" % json.dumps(cur),
            "process.stdout.write(JSON.stringify(novaBudgetReadingText("
            "novaResolvePlanBudget(%s, 'campaign', '3 months'), cur)));"
            % json.dumps(raw),
        ]
    )
    return run_node(script)


@needs_node
def test_budget_reading_uses_the_plan_currency_symbol_when_known():
    assert _reading("80000", {"symbol": "€", "suffix": False, "note": ""}) == (
        "Planning at €80,000"
    )
    assert (
        _reading("120000", {"symbol": "$", "suffix": False, "note": ""})
        == "Planning at $120,000"
    )
    assert (
        _reading("80000", {"symbol": "CHF ", "suffix": False, "note": ""})
        == "Planning at CHF 80,000"
    )


@needs_node
@pytest.mark.parametrize("cur", [None, {"symbol": "", "suffix": False}, {}])
def test_budget_reading_flags_an_unconfirmed_currency_instead_of_a_bare_figure(cur):
    out = _reading("80000", cur)
    assert out == "Planning at 80,000 (currency unconfirmed)"


def test_pending_state_is_what_the_estimate_response_replaces():
    # the currency answer re-renders the field reading and, on the review
    # step, the review (pinned by test_results_screen_budget as well)
    fn = js_block(SRC, "function novaRefreshPlanCurrency(")
    assert "novaRenderBudgetReading(false)" in fn and "populateReview()" in fn


# ── W4: enterprise section state ──────────────────────────────────────────

_ENT_STUB = """
var ELS = {};
function el(id, props) { ELS[id] = Object.assign({ id: id, value: '', checked: false, hidden: false, open: false }, props || {}); return ELS[id]; }
var document = { getElementById: function (id) { return ELS[id] || null; } };
var novaSiteGoals = {};
function novaApplyGoalMessage() { return ''; }
function novaSiteGoalsMessage() { return ''; }
function novaEnterpriseRenderSiteGoals() {}  // the demand-first goals table; not under test here
var errors = [], cleared = [];
function _showFieldError(e, msg) { errors.push({ id: e.id, msg: msg }); }
function _clearFieldError(e) { cleared.push(e.id); }
"""


def _ent_script(setup: str, run: str) -> str:
    lifted = slice_between(
        SRC, "function novaEnterpriseSlaMessage()", "function novaEnterpriseInputs()"
    ) + slice_between(
        SRC, "function novaEnterpriseState()", "function novaEnterpriseReset()"
    )
    return "\n".join(
        [
            _ENT_STUB,
            "['enterpriseEnabled','enterpriseWeekStart','enterpriseSystemOfRecord','enterpriseLockDay',"
            "'enterpriseSetupSla','enterpriseFields','enterpriseGroup','enterpriseOnBadge']"
            ".forEach(function (i) { el(i); });",
            "ELS.enterpriseOnBadge.hidden = true; ELS.enterpriseWeekStart.value = 'sunday';",
            lifted,
            setup,
            "process.stdout.write(JSON.stringify(" + run + "));",
        ]
    )


@needs_node
def test_review_says_off_then_lists_the_fields_when_on():
    off = run_node(_ent_script("", "novaEnterpriseReviewText()"))
    assert off == "Off"
    on = run_node(
        _ent_script(
            "ELS.enterpriseEnabled.checked = true; ELS.enterpriseSystemOfRecord.value = 'Workday';"
            "ELS.enterpriseLockDay.value = 'friday'; ELS.enterpriseSetupSla.value = '3';",
            "novaEnterpriseReviewText()",
        )
    )
    assert on == (
        "On — Fiscal week starts Sunday · System of record: Workday"
        " · Request lock day: Friday · Setup SLA: 3 days"
    )


@needs_node
def test_review_flags_an_invalid_sla_when_on():
    out = run_node(
        _ent_script(
            "ELS.enterpriseEnabled.checked = true; ELS.enterpriseSetupSla.value = '45';",
            "novaEnterpriseReviewText()",
        )
    )
    assert (
        out.startswith("On —")
        and "Setup SLA must be a whole number of days from 0 to 30." in out
    )


@needs_node
def test_badge_follows_the_toggle():
    out = run_node(
        _ent_script(
            "var r = [ELS.enterpriseOnBadge.hidden];"
            "ELS.enterpriseEnabled.checked = true; novaEnterpriseSyncBadge(); r.push(ELS.enterpriseOnBadge.hidden);"
            "ELS.enterpriseEnabled.checked = false; novaEnterpriseSyncBadge(); r.push(ELS.enterpriseOnBadge.hidden);",
            "r",
        )
    )
    assert out == [True, False, True]


@needs_node
def test_restoring_an_enabled_section_shows_the_badge_and_validates_the_sla():
    out = run_node(
        _ent_script(
            "novaEnterpriseRestore({ on: true, weekStart: 'monday', systemOfRecord: '', lockDay: '', setupSla: '45' });",
            "{ badgeHidden: ELS.enterpriseOnBadge.hidden, open: ELS.enterpriseGroup.open,"
            " fieldsHidden: ELS.enterpriseFields.hidden, errors: errors }",
        )
    )
    assert (
        out["badgeHidden"] is False
        and out["open"] is True
        and out["fieldsHidden"] is False
    )
    assert out["errors"] == [
        {
            "id": "enterpriseSetupSla",
            "msg": "Setup SLA must be a whole number of days from 0 to 30.",
        }
    ]


@needs_node
def test_restoring_a_valid_or_disabled_section_shows_no_error():
    ok = run_node(
        _ent_script("novaEnterpriseRestore({ on: true, setupSla: '5' });", "errors")
    )
    off = run_node(
        _ent_script(
            "novaEnterpriseRestore({ on: false, setupSla: '45' });",
            "{ errors: errors, badgeHidden: ELS.enterpriseOnBadge.hidden }",
        )
    )
    assert ok == [] and off == {"errors": [], "badgeHidden": True}


def test_markup_has_the_badge_and_the_review_row_and_the_toggle_handler_syncs_it():
    html = (APP_JS.parent / "body_content.html").read_text(encoding="utf-8")
    assert re.search(
        r'<span class="np-acc-on" id="enterpriseOnBadge" hidden>On</span>', html
    )
    assert 'id="reviewEnterprise"' in html
    assert 'getElementById("reviewEnterprise")' in js_block(
        SRC, "function populateReview("
    )
    assert "novaEnterpriseSyncBadge();" in slice_between(
        SRC,
        'if (e.target.id === "enterpriseEnabled")',
        'else if (e.target.id === "enterpriseSetupSla")',
    )
