"""The budget field's error, reading and currency states (design-judge
panel on mpg-input-parity @f159b3d, 2026-10-01, ITERATE items 1-5).

1. The error showed on the wrong control: #exactBudget's own rule (an id)
   out-specified `.studio .form-group.field-error input`, so the box never
   turned red while the neighbouring period <select> did.
2. At 375 px the inline error sat 226 px below the box (behind the quick
   amounts, the period select and two hint lines) and an error toast covered
   it after the jump.
3. The "Planning at ..." reading looked like help text (.form-hint's muted
   11.5 px) and wrapped to four lines in a 188 px column.
4. The box kept a fixed "$" prefix and "$50K" chips while the note said
   £75,000 / "EUR isn't read".
5. The reading was aria-live AND the field's description (re-announced on
   every keystroke); the field never got aria-invalid; the error had no role.

Computed-style evidence for 1-4 (real Chrome at 1280/768/375) is in the
task report; these tests pin the rules and the behaviour.
"""

from __future__ import annotations

import json
import re

import pytest

import app
from tests.live_server import live_port, post_json  # noqa: F401 (fixture)
from tests.wizard_js_harness import APP_JS, NODE, PARTIALS, inputs_functions_js, js_block, run_node

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")
CSS = (PARTIALS / "head_styles.html").read_text(encoding="utf-8")
CONTENT = (PARTIALS / "body_content.html").read_text(encoding="utf-8")


def _rule_bodies(selector_fragment: str) -> list:
    """Declaration blocks of every CSS rule whose selector contains the
    fragment."""
    return [
        m.group(2)
        for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", CSS)
        if selector_fragment in m.group(1)
    ]


# ── 1. the error is on the control it is about ──────────────────────────


def test_budget_box_error_rule_outranks_the_box_rule():
    bodies = _rule_bodies('.form-group.field-error #exactBudget.budget-amount-input[aria-invalid="true"]')
    assert bodies and all("var(--error" in b and "!important" in b for b in bodies)
    # ... including while focused (after the jump it showed only the purple ring)
    assert any(
        ':focus' in m.group(1) and "var(--error" in m.group(2)
        for m in re.finditer(r"([^{}]+)\{([^{}]*)\}", CSS)
        if '#exactBudget.budget-amount-input[aria-invalid="true"]' in m.group(1)
    )


def test_period_select_stays_neutral_when_the_amount_is_wrong():
    bodies = _rule_bodies('.form-group.field-error #budgetPeriod:not([aria-invalid="true"])')
    assert any("var(--np-hair)" in b for b in bodies)


def _field_error_script(calls: str) -> str:
    src = APP_JS.read_text(encoding="utf-8")
    return "\n".join(
        [
            "function El(id, type) { this.id = id; this.type = type || 'text'; this.attrs = {}; }",
            "El.prototype.setAttribute = function (k, v) { this.attrs[k] = String(v); };",
            "El.prototype.getAttribute = function (k) { return k in this.attrs ? this.attrs[k] : null; };",
            "El.prototype.removeAttribute = function (k) { delete this.attrs[k]; };",
            "var amt = new El('exactBudget'), period = new El('budgetPeriod', 'select-one');",
            "var msg = new El('budgetRange-error'); msg.style = {};",
            "var group = { cls: {}, classList: { add: function (c) { group.cls[c] = 1; },",
            "  remove: function (c) { delete group.cls[c]; } },",
            "  querySelector: function () { return msg; },",
            "  querySelectorAll: function (sel) { return [amt, period].filter(function (e) {",
            "    return e.getAttribute('aria-invalid') === 'true'; }); } };",
            "amt.closest = period.closest = function () { return group; };",
            js_block(src, "function _showFieldError("),
            js_block(src, "function _clearFieldError("),
            calls,
        ]
    )


@needs_node
def test_show_field_error_marks_only_that_control_invalid_and_alerts():
    got = run_node(_field_error_script(
        "var out = [];"
        "_showFieldError(amt, 'Budget must be at least 100');"
        "out.push([amt.getAttribute('aria-invalid'), period.getAttribute('aria-invalid'),"
        "  msg.getAttribute('role'), msg.textContent, !!group.cls['field-error']]);"
        "_showFieldError(period, 'Pick a period');"  # a budget_period 400
        "out.push([amt.getAttribute('aria-invalid'), period.getAttribute('aria-invalid')]);"
        "_clearFieldError(amt);"
        "out.push([amt.getAttribute('aria-invalid'), period.getAttribute('aria-invalid'),"
        "  !!group.cls['field-error'], msg.style.display]);"
        "process.stdout.write(JSON.stringify(out));"
    ))
    assert got[0] == ["true", None, "alert", "Budget must be at least 100", True]
    assert got[1] == [None, "true"]  # the error moved to the select: only it is invalid
    assert got[2] == [None, None, False, "none"]


# ── 2. the error message is directly under the box; no toast over it ────


def test_error_and_reading_sit_directly_under_the_box():
    group = CONTENT[CONTENT.index('class="form-group budget-group"') :]
    group = group[: group.index('<div class="form-group">')]
    order = [
        group.index('id="exactBudgetWrap"'),
        group.index('id="budgetRange-error"'),
        group.index('id="budgetResolved"'),
        group.index('id="budgetQuickfill"'),
        group.index('id="budgetPeriod"'),
        group.index('id="budget-hint"'),
    ]
    assert order == sorted(order), order
    err_tag = group[group.index('id="budgetRange-error"') - 40 : group.index('id="budgetRange-error"') + 40]
    assert 'role="alert"' in err_tag


def test_no_toast_after_the_jump_landed():
    src = APP_JS.read_text(encoding="utf-8")
    gen = js_block(src, "async function generatePlan(")
    budget_block = gen[gen.index("const budgetValidation = validateBudgetInput(") :]
    budget_block = budget_block[: budget_block.index("return;")]
    assert "showToast(" not in budget_block
    catch_block = gen[gen.index("A validation 400 names its field") :]
    catch_block = catch_block[: catch_block.index("btn.disabled = false;")]
    assert "if (!_landedAtField) showToast(userMsg" in catch_block


# ── 3. the reading has its own style and warns ─────────────────────────


def test_reading_has_its_own_ink_style_not_the_hint_style():
    tag = CONTENT[CONTENT.index('id="budgetResolved"') - 60 : CONTENT.index('id="budgetResolved"') + 20]
    assert "form-hint" not in tag
    body = [b for b in _rule_bodies(".studio .budget-resolved") if "font-size" in b][0]
    assert "12.5px" in body and "var(--np-ink)" in body
    assert any("rgba(245, 158, 11" in b for b in _rule_bodies(".studio .budget-resolved.is-warning"))


def test_budget_spans_the_row_above_duration_on_desktop():
    block = CSS[CSS.index("@media (min-width: 769px) {\n    .form-row.form-row-3.form-row-budget") :]
    block = block[: block.index("\n  }\n")]
    assert "grid-column: 1 / -1" in block
    assert 'class="form-row form-row-3 form-row-budget"' in CONTENT


@needs_node
def test_reading_warns_on_a_currency_note_or_a_period_mismatch():
    cases = [
        ("EUR 50,000", "campaign", {"note": "plan currency is USD for your locations"}),
        ("10k/mo", "campaign", {"symbol": "$", "note": ""}),
        ("£75,000", "campaign", {"symbol": "£", "note": ""}),
        ("10k/mo", "monthly", {"symbol": "$", "note": ""}),
        ("75,000", "campaign", {"symbol": "£", "note": "plan currency is GBP for your locations"}),
    ]
    script = "\n".join(
        [
            inputs_functions_js(),
            "var cases = " + json.dumps(cases) + ";",
            "process.stdout.write(JSON.stringify(cases.map(function (c) {",
            "  return novaBudgetReadingWarns(novaResolvePlanBudget(c[0], c[1], '3 months'), c[2]); })));",
        ]
    )
    assert run_node(script) == [True, True, False, False, True]


# ── 4. prefix and chips carry the resolver's currency ──────────────────


def _sync_script(text: str, plan_cur, market_cur) -> str:
    src = APP_JS.read_text(encoding="utf-8")
    return "\n".join(
        [
            inputs_functions_js(),
            "var cls = {}, pre = { textContent: '$', offsetWidth: 12 };",
            "var amt = { value: " + json.dumps(text) + ", style: { props: {},",
            "  setProperty: function (k, v) { this.props[k] = v; },",
            "  removeProperty: function (k) { delete this.props[k]; } } };",
            "var wrap = { classList: { toggle: function (c, on) { if (on) cls[c] = 1; else delete cls[c]; } },",
            "  querySelector: function () { return pre; } };",
            "var chips = [50000, 150000, 375000, 750000, 2000000, 4000000].map(function (a) {",
            "  return { textContent: '', getAttribute: function () { return String(a); } }; });",
            "var document = { getElementById: function (id) {",
            "    return id === 'exactBudgetWrap' ? wrap : id === 'exactBudget' ? amt : null; },",
            "  querySelectorAll: function () { return chips; } };",
            "var _novaCurrency = " + json.dumps(plan_cur) + ", _novaMarketCurrency = " + json.dumps(market_cur) + ";",
            js_block(src, "function novaCompactMoney("),
            js_block(src, "function novaSyncBudgetCurrency("),
            "novaSyncBudgetCurrency();",
            "process.stdout.write(JSON.stringify({ prefix: pre.textContent, typed: !!cls['typed-currency'],",
            "  chips: chips.map(function (c) { return c.textContent; }), pad: amt.style.props['padding-left'] || '' }));",
        ]
    )


@needs_node
@pytest.mark.parametrize(
    "text,plan_cur,market,prefix,typed,chips",
    [
        ("75,000", {"symbol": "£"}, {"symbol": "£"}, "£", False, "£50K £150K £375K £750K £2M £4M"),
        ("", None, {"symbol": "C$"}, "C$", False, "C$50K C$150K C$375K C$750K C$2M C$4M"),
        ("EUR 50,000", {"symbol": "$"}, {"symbol": "$"}, "$", True, "$50K $150K $375K $750K $2M $4M"),
        ("£75,000", {"symbol": "£"}, {"symbol": "£"}, "£", True, "£50K £150K £375K £750K £2M £4M"),
        ("$50,000", {"symbol": "$"}, {"symbol": "£"}, "$", True, "£50K £150K £375K £750K £2M £4M"),
        ("50000", {"symbol": "kr", "suffix": True}, {"symbol": "kr", "suffix": True}, "kr", False,
         "50K kr 150K kr 375K kr 750K kr 2M kr 4M kr"),
    ],
)
def test_prefix_and_quick_amounts_follow_the_resolved_currency(text, plan_cur, market, prefix, typed, chips):
    got = run_node(_sync_script(text, plan_cur, market))
    assert got["prefix"] == prefix and got["typed"] is typed
    assert " ".join(got["chips"]) == chips
    if not typed:
        assert got["pad"] == "34px"  # prefix width + 22: room for "C$" / "kr"


@pytest.mark.parametrize(
    "body,plan_code,market_code",
    [
        ({"locations": ["London, UK"]}, None, "GBP"),
        ({"locations": ["Toronto, ON"]}, None, "CAD"),
        ({"locations": []}, None, "USD"),
        ({"budget_range": "$50,000", "locations": ["London, UK"]}, "USD", "GBP"),
        ({"budget_range": "75,000", "locations": ["London, UK"]}, "GBP", "GBP"),
    ],
)
def test_estimate_returns_the_bare_amount_currency(live_port, body, plan_code, market_code):  # noqa: F811
    status, est = post_json(live_port, "/api/estimate", dict(body, budget_only=True))
    assert status == 200, est
    assert (est["plan_currency"] or {}).get("code") == plan_code
    assert est["market_currency"]["code"] == market_code
    assert est["market_currency"]["symbol"] == app.plan_currency.symbol_for_code(market_code)


# ── 5. accessibility ───────────────────────────────────────────────────


def test_reading_is_not_a_per_keystroke_live_region():
    tag = CONTENT[CONTENT.index('id="budgetResolved"') - 60 : CONTENT.index('id="budgetResolved"') + 60]
    assert "aria-live" not in tag
    live = CONTENT[CONTENT.index('id="budgetResolvedLive"') - 40 : CONTENT.index('id="budgetResolvedLive"') + 120]
    assert 'aria-live="polite"' in live and 'class="sr-only"' in live


@needs_node
def test_live_region_announces_once_typing_settles():
    src = APP_JS.read_text(encoding="utf-8")
    script = "\n".join(
        [
            "var now = 0, timers = [];",
            "function setTimeout(fn, ms) { timers.push({ at: now + ms, fn: fn, live: true }); return timers.length; }",
            "function clearTimeout(id) { if (timers[id - 1]) timers[id - 1].live = false; }",
            "function advance(ms) { now += ms; timers.forEach(function (t) { if (t.live && t.at <= now) { t.live = false; t.fn(); } }); }",
            "var live = { textContent: '' }, writes = [];",
            "Object.defineProperty(live, 'text', { get: function () { return this.textContent; } });",
            "var document = { getElementById: function () { return live; } };",
            "var _novaAnnounceTimer = null;",
            js_block(src, "function novaAnnounceBudgetReading("),
            "['Planning at $1', 'Planning at $15', 'Planning at $150', 'Planning at $150,000'].forEach(function (t) {",
            "  novaAnnounceBudgetReading(t); advance(150); writes.push(live.textContent); });",
            "advance(600); writes.push(live.textContent);",
            "process.stdout.write(JSON.stringify(writes));",
        ]
    )
    # nothing while typing (150 ms between keys); the settled line once
    assert run_node(script) == ["", "", "", "", "Planning at $150,000"]
