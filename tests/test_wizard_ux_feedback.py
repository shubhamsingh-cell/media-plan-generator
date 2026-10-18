"""Wizard usability fixes from the internal feedback form (2026-10-18).

The feedback, in short: in the dark UI it is hard to navigate and unclear which
fields are required; a user wants the simple outcome (expected CPA, application
volume, budget recommendation, list of sources) without the long dashboard;
nothing is saved, so a lost session means starting over.

What is pinned here, each against the REAL template code:

1. Required fields: the "*" markers, aria-required and inline messages agree with
   what Continue / Generate actually block on (a differential run of the real
   nextStep() against novaStepMissing()), and with the API's own 400 branches.
2. Dark UI legibility: contrast ratios computed from the CSS custom properties
   (WCAG 1.4.3 text 4.5:1, 1.4.11 control boundaries 3:1) and the rules that
   apply them.
3. The results screen opens with a Quick answer card of RANGES, from the same
   estimate response and helpers as the live preview.
4. Draft status + a client-side download / restore of the inputs.
5. Nova chat's hand-off URL carries `excluded_publishers`; the wizard prefill
   feeds the excluded-boards box through its normal commit path.

Front-end behaviour is exercised by lifting the real functions out of the
templates and running them under node (tests/wizard_js_harness.py).
"""

from __future__ import annotations

import json
import re

import pytest

from tests.wizard_js_harness import (
    APP_JS,
    NODE,
    PARTIALS,
    PREVIEW_JS,
    inputs_functions_js,
    js_block,
    run_node,
    slice_between,
    wizard_payloads,
)

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")

ROOT = PARTIALS.parent.parent.parent
APP = APP_JS.read_text(encoding="utf-8")
PREVIEW = PREVIEW_JS.read_text(encoding="utf-8")
BODY = (PARTIALS / "body_content.html").read_text(encoding="utf-8")
_RAW_STYLES = (PARTIALS / "head_styles.html").read_text(encoding="utf-8")
STYLES = re.sub(r"/\*.*?\*/", "", _RAW_STYLES, flags=re.S)
# the rules added by this pass (the block's header is a comment, so cut the raw text)
LAST = (
    re.sub(r"/\*.*?\*/", "", _RAW_STYLES.split("WIZARD UX PASS")[1], flags=re.S)
    if "WIZARD UX PASS" in _RAW_STYLES
    else ""
)


# ══════════════════════════════════════════════════════════════════════════
# 1. Required fields
# ══════════════════════════════════════════════════════════════════════════

_STEP_STUBS = """
function El(id, v) { this.id = id; this.value = v || ''; this.style = {}; this.dataset = {};
  this.classList = { add: function () {}, remove: function () {} }; }
El.prototype.focus = function () {};
El.prototype.scrollIntoView = function () {};
El.prototype.setAttribute = function () {};
El.prototype.getAttribute = function () { return null; };
var FIELDS = {};
var document = { getElementById: function (id) { return FIELDS[id] || null; },
  querySelector: function () { return null; } };
var window = { matchMedia: function () { return { matches: true }; }, scrollTo: function () {} };
function setTimeout(fn) { fn(); }
function showToast() {}
function _showFieldError() {} function _clearFieldError() {}
function novaScrollBehavior() { return 'auto'; }
function novaClientNameProblem(v) { return /^[^A-Za-z0-9]+$/.test(v) ? 'chars' : ''; }
function novaCheckTextLimit(id) { return ((FIELDS[id] || {}).value || '').length > 50 ? 'too long' : ''; }
function novaTextLimitError() { return 'too long'; }
// a budget the wizard reads: starts with a digit (the real parser is tested elsewhere)
function validateBudgetInput(t) { return /^\\d/.test(t) ? { valid: true } : { valid: false, error: 'bad' }; }
function addTag(c, i, arr, v) { arr.push(v); } function populateReview() {} function fetchLivePricing() {}
function updateProgress() {} function updateStepCheckmarks() {}
var locations = [], roles = [], selectedIndustry = null, currentStep = 1, totalSteps = 5;
"""

_FULL_STEP1 = {
    "clientName": "Acme Corp",
    "useCase": "Hire engineers",
    "exactBudget": "150,000",
    "budgetRange": "__exact__",
    "campaignDuration": "6 months",
    "locationInput": "",
    "_locations": 1,
}


def _gate_run(step: int, cases: list) -> list:
    """For each case, whether the REAL nextStep() let the wizard advance and
    what novaStepMissing() listed. Both read one stubbed form."""
    script = "\n".join(
        [
            _STEP_STUBS,
            js_block(APP, "var NOVA_STEP_REQUIRED") + ";",
            js_block(APP, "function novaStepMissing("),
            js_block(APP, "function nextStep("),
            "var CASES = " + json.dumps(cases) + ";",
            "var out = CASES.map(function (c) {",
            "  FIELDS = {};",
            "  Object.keys(c).forEach(function (k) { if (k.charAt(0) !== '_') FIELDS[k] = new El(k, c[k]); });",
            "  ['step1','step2','step3'].forEach(function (s) { FIELDS[s] = new El(s); });",
            "  locations = []; for (var i = 0; i < (c._locations || 0); i++) locations.push('Dallas, TX');",
            "  roles = []; for (var j = 0; j < (c._roles || 0); j++) roles.push('Nurse');",
            "  selectedIndustry = c._industry || null;",
            "  currentStep = " + str(step) + ";",
            "  var missing = novaStepMissing("
            + str(step)
            + ").map(function (m) { return m.id; });",
            "  nextStep();",
            "  return { advanced: currentStep === "
            + str(step + 1)
            + ", missing: missing };",
            "});",
            "process.stdout.write(JSON.stringify(out));",
        ]
    )
    return run_node(script)


@needs_node
def test_step_one_missing_list_agrees_with_what_continue_blocks():
    """Every single-field omission (and each way of failing a field): the
    wizard advances exactly when novaStepMissing() is empty. A field Continue
    gates on but the list forgets (or the reverse) is what left Target Roles
    required but unmarked."""
    base = dict(_FULL_STEP1)
    cases = [dict(base)]
    cases += [dict(base, clientName="")]
    cases += [dict(base, clientName="!!")]  # fails the client-name rule
    cases += [dict(base, useCase="")]
    cases += [dict(base, exactBudget="")]
    cases += [dict(base, exactBudget="abc")]  # typed but unreadable
    cases += [dict(base, budgetRange="")]  # the hidden filled-flag is empty
    cases += [dict(base, campaignDuration="")]
    cases += [dict(base, _locations=0)]
    cases += [
        dict(base, _locations=0, locationInput="Dallas, TX")
    ]  # typed, not committed: Continue adds it
    cases += [dict(base, useCase="x" * 60)]  # over the server's length limit
    cases += [
        dict(base, clientWebsite="", campaignStartMonth="", hireVolume="")
    ]  # optional fields blank
    got = _gate_run(1, cases)
    for case, res in zip(cases, got):
        assert res["advanced"] == (res["missing"] == []), (case, res)
    assert got[0] == {"advanced": True, "missing": []}
    # and each required field is individually reported
    assert [r["missing"] for r in got[1:9]] == [
        ["clientName"],
        ["clientName"],
        ["useCase"],
        ["exactBudget"],
        ["exactBudget"],
        ["exactBudget"],
        ["campaignDuration"],
        ["locationInput"],
    ]
    assert got[9]["missing"] == [] and got[9]["advanced"] is True
    assert got[11] == {"advanced": True, "missing": []}


@needs_node
def test_step_two_missing_list_agrees_with_what_continue_blocks():
    cases = [
        {"_industry": "tech_engineering", "_roles": 1},
        {"_industry": None, "_roles": 1},
        {"_industry": "tech_engineering", "_roles": 0},
        {"_industry": None, "_roles": 0},
        {
            "_industry": "tech_engineering",
            "_roles": 0,
            "roleInput": "Nurse",
        },  # typed, not added: still blocks
    ]
    got = _gate_run(2, cases)
    for case, res in zip(cases, got):
        assert res["advanced"] == (res["missing"] == []), (case, res)
    assert [r["missing"] for r in got] == [
        [],
        ["industryGrid"],
        ["roleInput"],
        ["industryGrid", "roleInput"],
        ["roleInput"],
    ]


@needs_node
def test_other_steps_require_nothing_and_step_five_needs_name_and_email():
    script = "\n".join(
        [
            _STEP_STUBS,
            js_block(APP, "var NOVA_STEP_REQUIRED") + ";",
            js_block(APP, "function novaStepMissing("),
            "function ids(step, name, email) {",
            "  FIELDS = { requesterName: new El('requesterName', name), requesterEmail: new El('requesterEmail', email) };",
            "  return novaStepMissing(step).map(function (m) { return m.id; }); }",
            "process.stdout.write(JSON.stringify({",
            "  s3: ids(3, '', ''), s4: ids(4, '', ''),",
            "  none: ids(5, '', ''), nameOnly: ids(5, 'Sam', ''), badEmail: ids(5, 'Sam', 'sam@x'),",
            "  ok: ids(5, 'Sam', 'sam@example.com') }));",
        ]
    )
    got = run_node(script)
    assert got == {
        "s3": [],
        "s4": [],
        "none": ["requesterName", "requesterEmail"],
        "nameOnly": ["requesterEmail"],
        "badEmail": ["requesterEmail"],
        "ok": [],
    }


def _required_ids() -> list:
    out = run_node(
        js_block(APP, "var NOVA_STEP_REQUIRED") + ";\n"
        "process.stdout.write(JSON.stringify(NOVA_STEP_REQUIRED));"
    )
    return [spec["id"] for step in sorted(out) for spec in out[step]]


def test_required_markers_are_on_exactly_the_required_fields():
    required = set(_required_ids())
    assert required == {
        "clientName",
        "useCase",
        "exactBudget",
        "campaignDuration",
        "locationInput",
        "industryGrid",
        "roleInput",
        "requesterName",
        "requesterEmail",
    }
    # labels carrying the visible marker, mapped to the control they name
    marked = set()
    for m in re.finditer(r"<label\b([^>]*)>(.*?)</label\s*>", BODY, flags=re.S):
        if 'class="req-mark"' not in m.group(2):
            continue
        # an accessible name for AT: the marker is aria-hidden, the words are not
        assert 'aria-hidden="true">*</span>' in m.group(2)
        assert '<span class="sr-only"> (required)</span>' in m.group(2)
        target = re.search(r'\bfor="([^"]+)"', m.group(1))
        marked.add(
            target.group(1)
            if target
            else "industryGrid" if 'id="industryLabel"' in m.group(1) else m.group(1)
        )
    assert marked == required, marked ^ required
    # no plain-text asterisk left in a label: the visible mark is the aria-hidden span only
    for m in re.finditer(r"<label\b[^>]*>(.*?)</label\s*>", BODY, flags=re.S):
        text = re.sub(r"<[^>]+>", "", m.group(1))
        assert ("*" in text) == ('class="req-mark"' in m.group(1)), text


def test_required_controls_declare_aria_required_and_a_message_to_describe_them():
    aria_required = set(
        re.findall(
            r'<(?:input|textarea|select)\b[^>]*?id="([^"]+)"[^>]*?aria-required="true"',
            BODY,
            flags=re.S,
        )
    )
    aria_required |= set(
        re.findall(
            r'<(?:input|textarea|select)\b[^>]*?aria-required="true"[^>]*?id="([^"]+)"',
            BODY,
            flags=re.S,
        )
    )
    assert aria_required == set(_required_ids()) - {"industryGrid"}, aria_required
    # a control's aria-describedby must point at elements that exist
    for control in sorted(aria_required) + ["industryGrid"]:
        tag = re.search(
            rf'<[a-z]+\b[^>]*\bid="{control}"[^>]*>', BODY, flags=re.S
        ).group(0)
        described = re.search(r'aria-describedby="([^"]+)"', tag)
        assert described, control
        for ref in described.group(1).split():
            assert f'id="{ref}"' in BODY, (control, ref)
    # the industry card group is a labelled group that carries its own message
    grid = re.search(r'<div\s[^>]*id="industryGrid"[^>]*>', BODY, flags=re.S).group(0)
    assert 'role="group"' in grid and 'aria-labelledby="industryLabel"' in grid
    assert 'id="industry-error"' in BODY


def test_every_step_says_what_is_required_and_has_a_still_needed_line():
    assert BODY.count('data-req-legend="required"') == 3  # steps 1, 2, 5
    assert BODY.count('data-req-legend="none"') == 2  # steps 3, 4
    for step in (1, 2, 5):
        assert (
            f'id="stepNeeds{step}" data-step-needs="{step}" role="status" aria-live="polite"'
            in BODY
        )
    # the Continue / Generate buttons point at their line
    assert 'onclick="nextStep()" aria-describedby="stepNeeds1"' in BODY
    assert 'onclick="nextStep()" aria-describedby="stepNeeds2"' in BODY
    assert 'aria-describedby="stepNeeds5"' in BODY


def test_server_400_fields_are_all_marked_required_in_the_wizard():
    """What /api/generate rejects must be required in the UI too (the reverse
    is not true: the wizard also asks for the brief, duration, location and
    industry because the plan needs them)."""
    app_src = (ROOT / "app.py").read_text(encoding="utf-8")
    start = app_src.index("_missing: list = []")
    gate = app_src[start : app_src.index("if _missing:", start)]
    fields = set(
        re.findall(
            r'_missing\.append\(\s*\(.*?,\s*"([a-z_]+)"\s*\)\s*\)', gate, flags=re.S
        )
    )
    fields |= {"budget_range"}  # _budget_check: field=_request_budget_raw(data)[1]
    assert {
        "client_name",
        "requester_name",
        "requester_email",
        "target_roles",
    } <= fields
    targets = dict(
        re.findall(
            r'^\s+([a-z_]+): "([A-Za-z]+)",$',
            APP[APP.index("var NOVA_FIELD_TARGETS") :].split("};")[0],
            flags=re.M,
        )
    )
    ids = {targets[f] for f in fields}
    assert ids <= set(_required_ids()), ids - set(_required_ids())


@needs_node
def test_inline_error_is_tied_to_its_control_and_cleared_with_it():
    """Pre-fix: a message created by _showFieldError had no id (the control's
    aria-describedby pointed at nothing), a tag box got aria-invalid, and the
    cleared message kept its words for a screen reader to read."""
    script = "\n".join(
        [
            "function El(id, tag) { this.id = id; this.tagName = tag || 'INPUT'; this.attrs = {}; this.style = {}; this.textContent = ''; }",
            "El.prototype.setAttribute = function (k, v) { this.attrs[k] = String(v); if (k === 'id') this.id = v; };",
            "El.prototype.getAttribute = function (k) { return k in this.attrs ? this.attrs[k] : null; };",
            "El.prototype.removeAttribute = function (k) { delete this.attrs[k]; };",
            "var input = new El('roleInput'); var grid = new El('industryGrid', 'DIV');",
            "var made = null;",
            "var group = { cls: {}, classList: { add: function (c) { group.cls[c] = 1; }, remove: function (c) { delete group.cls[c]; } },",
            "  querySelector: function () { return made; },",
            "  querySelectorAll: function () { return [input, grid].filter(function (e) { return e.getAttribute('aria-invalid') === 'true'; }); } };",
            "input.closest = grid.closest = function () { return group; };",
            "var document = { createElement: function () { made = new El(''); made.className = ''; return made; } };",
            "group.appendChild = function (e) { made = e; };",
            "function novaTagNewestErrorToast() {}",
            js_block(APP, "function _showFieldError("),
            js_block(APP, "function novaTieErrorToControl("),
            js_block(APP, "function _clearFieldError("),
            "_showFieldError(input, 'At least one target role is required');",
            "var afterShow = { id: made.id, text: made.textContent, role: made.attrs.role,",
            "  describedby: input.getAttribute('aria-describedby'), invalid: input.getAttribute('aria-invalid') };",
            "_showFieldError(grid, 'Select an industry to continue');",
            "var gridInvalid = grid.getAttribute('aria-invalid');",
            "_clearFieldError(input);",
            "process.stdout.write(JSON.stringify({ afterShow: afterShow, gridInvalid: gridInvalid,",
            "  clearedText: made.textContent, clearedDisplay: made.style.display,",
            "  invalidAfterClear: input.getAttribute('aria-invalid') }));",
        ]
    )
    got = run_node(script)
    assert got["afterShow"] == {
        "id": "roleInput-error",
        "text": "At least one target role is required",
        "role": "alert",
        "describedby": "roleInput-error",
        "invalid": "true",
    }
    assert got["gridInvalid"] is None  # a card group is not a form control
    assert got["clearedText"] == "" and got["clearedDisplay"] == "none"
    assert got["invalidAfterClear"] is None


@needs_node
def test_validate_field_writes_its_message_and_marks_the_control_invalid():
    """Pre-fix: validateField switched the (empty) message box on without
    writing a word and never set aria-invalid, so a blank Your Name turned red
    with nothing said; a typed-but-not-an-email address counted as valid."""
    script = "\n".join(
        [
            "function El(id) { this.id = id; this.value = ''; this.attrs = {}; this.style = {}; this.textContent = ''; }",
            "El.prototype.setAttribute = function (k, v) { this.attrs[k] = String(v); };",
            "El.prototype.getAttribute = function (k) { return k in this.attrs ? this.attrs[k] : null; };",
            "El.prototype.removeAttribute = function (k) { delete this.attrs[k]; };",
            "var cls = {};",
            "var group = { classList: { add: function (c) { cls[c] = 1; }, remove: function (c) { delete cls[c]; } } };",
            "var msg = new El('requesterEmail-error'); var field = new El('requesterEmail');",
            "group.querySelector = function () { return msg; };",
            "field.closest = function () { return group; };",
            "var document = { getElementById: function (id) { return id === 'requesterEmail' ? field : null; } };",
            "function novaTagNewestErrorToast() {}",
            js_block(APP, "function novaTieErrorToControl("),
            "var NOVA_FIELD_MESSAGES = "
            + js_block(APP, "var NOVA_FIELD_MESSAGES").split("=", 1)[1].strip()
            + ";",
            js_block(APP, "function validateField("),
            "var out = [];",
            "function snap(label, ret) { out.push([label, ret, msg.textContent, field.getAttribute('aria-invalid'), !!cls['field-error'], msg.getAttribute('aria-describedby') || field.getAttribute('aria-describedby')]); }",
            "field.value = ''; snap('blank', validateField('requesterEmail'));",
            "field.value = 'sam@x'; snap('notanemail', validateField('requesterEmail'));",
            "field.value = 'sam@example.com'; snap('ok', validateField('requesterEmail'));",
            "process.stdout.write(JSON.stringify(out));",
        ]
    )
    got = run_node(script)
    assert got[0][:5] == ["blank", False, "Enter a valid email address", "true", True]
    assert got[1][:5] == [
        "notanemail",
        False,
        "Enter a valid email address",
        "true",
        True,
    ]
    assert got[2][:5] == ["ok", True, "", None, False]
    assert got[0][5] == "requesterEmail-error"


def test_generate_reports_both_requester_problems_at_once():
    gen = js_block(APP, "async function generatePlan(")
    gate = gen[gen.index("const reqName") : gen.index("const _entMsg")]
    # name and email are both painted before the single return
    assert gate.count("return;") == 1
    assert (
        'validateField("requesterEmail")' in gate
        and 'validateField("requesterName")' in gate
    )
    assert gate.index('validateField("requesterEmail")') < gate.index(
        'validateField("requesterName")'
    )


def test_industry_choice_is_readable_state_and_clears_its_error():
    body = js_block(APP, "function selectIndustry(")
    assert (
        'setAttribute("aria-pressed", "true")' in body
        and 'setAttribute("aria-pressed", "false")' in body
    )
    assert "_clearFieldError(_indGrid)" in body
    step = js_block(APP, "function nextStep(")
    assert '_showFieldError(indGrid, "Select an industry to continue")' in step


# ══════════════════════════════════════════════════════════════════════════
# 2. Dark UI legibility: ratios computed from the CSS custom properties
# ══════════════════════════════════════════════════════════════════════════


def _tokens() -> dict:
    """Custom properties of every :root block, later blocks winning (the
    cascade), with var() references resolved."""
    raw: dict = {}
    for block in re.findall(r":root\s*\{(.*?)\n  \}", STYLES, flags=re.S):
        for name, value in re.findall(r"(--[a-z0-9-]+)\s*:\s*([^;]+);", block):
            raw[name] = value.strip()

    def resolve(v: str, depth: int = 0) -> str:
        m = re.fullmatch(r"var\((--[a-z0-9-]+)\)", v)
        return resolve(raw[m.group(1)], depth + 1) if m and depth < 5 else v

    return {k: resolve(v) for k, v in raw.items()}


def _rgb(hexcolor: str) -> tuple:
    h = hexcolor.strip().lstrip("#")
    assert re.fullmatch(r"[0-9a-fA-F]{6}", h), hexcolor
    return tuple(int(h[i : i + 2], 16) for i in (0, 2, 4))


def _lum(c: tuple) -> float:
    def f(v: int) -> float:
        v = v / 255
        return v / 12.92 if v <= 0.03928 else ((v + 0.055) / 1.055) ** 2.4

    r, g, b = (f(x) for x in c)
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _ratio(a: str, b: str) -> float:
    la, lb = _lum(_rgb(a)), _lum(_rgb(b))
    hi, lo = max(la, lb), min(la, lb)
    return (hi + 0.05) / (lo + 0.05)


# Surfaces the form column's text and controls actually sit on, from the render
# measurement at 1440 and 375 (headless Chrome, text hidden, background pixels
# sampled; ledger/2026-10-18/wizard_ux/contrast_before_after.json):
#   panel            the form card                          --np-panel
#   aurora_brightest the aurora behind labels at 375 px     #343a69
#   worst_outside    the lightest surface a control sits in #191a28
#   wash_darkest     the preview pane's lavender wash       #838193
AURORA_BRIGHTEST = "#343a69"
WORST_OUTSIDE = "#191a28"
WASH_DARKEST = "#838193"


def test_text_tokens_clear_4_5_on_every_dark_surface():
    t = _tokens()
    surfaces = {
        "panel": t["--np-panel"],
        "canvas": t["--np-canvas"],
        "aurora": AURORA_BRIGHTEST,
    }
    too_low = []
    for token in (
        "--np-ink",
        "--np-ink-soft",
        "--np-muted",
        "--np-faint",
        "--text-secondary",
        "--text-muted",
        "--np-teal",
        "--np-req",
    ):
        for name, bg in surfaces.items():
            r = _ratio(t[token], bg)
            if r < 4.5:
                too_low.append((token, name, round(r, 2)))
    assert not too_low, too_low
    # links / section titles in --np-purple-light sit on the cards, never on the bare aurora
    for bg in (t["--np-panel"], t["--np-canvas"]):
        assert _ratio(t["--np-purple-light"], bg) >= 4.5


def test_control_boundary_and_focus_tokens_clear_3_to_1():
    t = _tokens()
    for bg in (t["--np-panel"], t["--np-canvas"], WORST_OUTSIDE):
        assert _ratio(t["--np-field-line"], bg) >= 3.0, (t["--np-field-line"], bg)
        assert (
            _ratio(t["--np-purple-light"], bg) >= 3.0
        )  # focus ring + selected card edge
    # the old boundary token was a 0.10-alpha hairline: it must not be what the controls use
    assert "--np-field-line" in t and t["--np-field-line"].startswith("#")


def test_text_on_the_wash_and_on_paper_clears_4_5():
    t = _tokens()
    assert _ratio(t["--np-wash-ink"], WASH_DARKEST) >= 4.5  # "Live preview"
    for bg in (
        t["--np-lav-50"],
        "#ffffff",
        t["--np-paper"],
    ):  # the Quick answer card's surfaces
        assert _ratio(t["--np-paper-muted"], bg) >= 4.5
        assert _ratio("#b45309", bg) >= 4.5  # its warning text (brand AMBER_TEXT)
    # the empty "$0" ghost (large text, 3:1) is paper-muted at 0.7 opacity over the card
    ghost = tuple(
        round(0.7 * a + 0.3 * b)
        for a, b in zip(_rgb(t["--np-paper-muted"]), _rgb("#f3f3fe"))
    )
    ghost_hex = "#%02x%02x%02x" % ghost
    assert _ratio(ghost_hex, "#f3f3fe") >= 3.0


def test_white_on_gradient_passes_at_both_ends():
    t = _tokens()
    stops = re.findall(r"#[0-9a-fA-F]{6}", t["--np-grad-ink"])
    assert len(stops) == 2
    for stop in stops:
        assert _ratio("#ffffff", stop) >= 4.5, stop
    # the surfaces that print white text on a gradient all use it
    selectors = ""
    for rule in re.finditer(r"([^{}]+)\{([^{}]*)\}", LAST):
        if "background: var(--np-grad-ink) !important" in rule.group(2):
            selectors += rule.group(1)
    for selector in (
        ".studio .btn-primary",
        ".studio .btn-success",
        ".np-apply-mix",
        ".np-logo",
        ".studio-topbar .stb-logo",
        ".studio .li-section-divider .section-number",
        ".studio .plan-rec-icon",  # the results screen's numbered recommendations
    ):
        assert selector in selectors, selector


def test_every_form_control_uses_the_visible_boundary_token():
    last = LAST
    selectors = ""
    for rule in re.finditer(r"([^{}]+)\{([^{}]*)\}", LAST):
        if "border-color: var(--np-field-line) !important" in rule.group(
            2
        ) and 'input[type="text"]' in rule.group(1):
            selectors = rule.group(1)
    assert selectors, "control boundary rule missing"
    for control in (
        'input[type="text"]',
        'input[type="url"]',
        'input[type="email"]',
        "textarea",
        "select",
        ".tags-container",
        "#exactBudget.budget-amount-input",
        "#naicsSearch",
    ):
        assert control in selectors, control
    # error state keeps its red edge: the rule never applies inside .field-error
    assert selectors.count(":not(.field-error)") >= 7
    # cards, the step rail and the neutral period select use it too
    assert ".studio .industry-card:not(.selected)" in last
    assert re.search(r"\.studio \.step-circle\s*\{[^}]*var\(--np-field-line\)", last)
    assert "var(--np-field-line)" in re.search(
        r'#budgetPeriod:not\(\[aria-invalid="true"\]\)\s*\{([^}]*)\}', STYLES
    ).group(1)


def test_step_rail_is_numbered_and_focus_is_visible():
    last = LAST
    assert "counter-increment: nstep" in last and "content: counter(nstep)" in last
    assert (
        ":focus-visible" in last and "outline: 2px solid var(--np-purple-light)" in last
    )
    # a 375px rail: only the current step keeps its badge
    assert ".step-indicator:not(.completed):not(.active) .step-label::before" in last


# ══════════════════════════════════════════════════════════════════════════
# 3. The Quick answer card
# ══════════════════════════════════════════════════════════════════════════

_QA_HELPERS = "\n".join(
    [
        "function sym() { return '$'; }",
        "function trim(x, d) { return Number(x.toFixed(d)).toString(); }",
        "function intc(v) { return Math.round(Math.max(0, v)).toLocaleString('en-US'); }",
        "function hero(v) { v = Math.max(0, v); if (v >= 1e6) return sym() + trim(v / 1e6, 2) + 'M';"
        " if (v >= 1e3) return sym() + Math.round(v / 1e3) + 'K'; return sym() + Math.round(v); }",
    ]
)


def _qa_script(body: str) -> str:
    return "\n".join(
        [
            _QA_HELPERS,
            js_block(PREVIEW, "function hireBasisText("),
            js_block(PREVIEW, "function budgetGap("),
            js_block(PREVIEW, "function goalText("),
            js_block(PREVIEW, "function quickAnswerModel("),
            body,
        ]
    )


_EST = {
    "est_hires": 20.0,
    "projection": {
        "text": {
            "applications": "1,685–10,149",
            "hires": "15–30",
            "cpa": "$14.78–$89",
            "cpa_note": "",
        },
        "hires": {"low": 15, "high": 30},
        "assumptions": {},
    },
    "hire_basis": {"midpoint_cph": 10000, "floor_cph": 5000, "basis": "nova_table"},
}
_M = {"total": 150000, "targetHires": 75, "goalRange": "50–100"}
_NAMES = [
    "Programmatic",
    "Niche",
    "Global boards",
    "Social",
    "Regional",
    "Employer brand",
    "EMEA",
]


@needs_node
def test_quick_answer_model_is_ranges_only_with_the_hire_basis():
    got = run_node(
        _qa_script(
            "process.stdout.write(JSON.stringify(quickAnswerModel("
            + json.dumps(_M)
            + ", "
            + json.dumps(_EST)
            + ", "
            + json.dumps(_NAMES)
            + ")));"
        )
    )
    assert got["ready"] is True
    # the three figures are the server's own low-high strings, never a lone point
    assert (got["cpa"], got["applications"], got["hires"]) == (
        "$14.78–$89",
        "1,685–10,149",
        "15–30",
    )
    # 75 hires needed vs at most 30: the shortfall is stated with the shared helper
    assert "50–100 hire goal (planned at ~75)" in got["supports"]
    assert "$600K" in got["supports"] and got["tone"] == "warn"  # 75 x $10K - $150K
    # ... and the cost per hire the hires rest on
    assert (
        got["basis"]
        == "low end at an assumed $10K all-in cost per hire (Nova table), high end at $5K"
    )
    assert got["budget"] == "$150K"
    assert got["channels"] == _NAMES[:5] and got["more"] == 2


@needs_node
def test_quick_answer_has_no_figures_without_an_estimate_for_these_inputs():
    got = run_node(
        _qa_script(
            "process.stdout.write(JSON.stringify(quickAnswerModel("
            + json.dumps(_M)
            + ", null, "
            + json.dumps(_NAMES[:2])
            + ")));"
        )
    )
    assert got["ready"] is False
    assert got["cpa"] == got["applications"] == got["hires"] == got["supports"] == ""
    assert got["channels"] == _NAMES[:2]  # the plan's own list still stands


@needs_node
def test_quick_answer_projects_no_hires_for_a_market_without_a_local_benchmark():
    est = json.loads(json.dumps(_EST))
    est["projection"]["assumptions"] = {"hires_suppressed": True}
    est["projection"]["text"][
        "hires_note"
    ] = "No local cost-per-hire benchmark for this market."
    got = run_node(
        _qa_script(
            "process.stdout.write(JSON.stringify(quickAnswerModel("
            + json.dumps(_M)
            + ", "
            + json.dumps(est)
            + ", [])));"
        )
    )
    assert got["hires"] == "" and got["basis"] == ""
    assert got["supports"] == "No local cost-per-hire benchmark for this market."
    assert got["cpa"] and got["applications"]  # the other two figures stay


@needs_node
def test_quick_answer_uses_the_goal_plan_line_when_there_is_one():
    est = json.loads(json.dumps(_EST))
    est["goal_plan"] = {
        "ok": True,
        "text": {"callout": "100 hires need about $1M; this budget covers 31."},
    }
    got = run_node(
        _qa_script(
            "process.stdout.write(JSON.stringify(quickAnswerModel("
            + json.dumps(_M)
            + ", "
            + json.dumps(est)
            + ", [])));"
        )
    )
    assert got["supports"] == "100 hires need about $1M; this budget covers 31."


def _render_stubs() -> str:
    return "\n".join(
        [
            "var out = { innerHTML: '' };",
            "var document = { getElementById: function (id) {",
            "  if (id === 'planDashboardContent') return out;",
            "  if (id === 'planDashboardSection') return { classList: { add: function () {} } };",
            "  return null; } };",
            "function novaPlanCurrency() { return null; }",
            "function escapeHtml(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }",
            "function novaFormatMoney() { return ''; }",
            js_block(APP, "function novaPlanBudgetLabel("),
            js_block(APP, "function novaPlanFileExportAvailable("),
            js_block(APP, "function novaQuickAnswerHtml("),
            js_block(APP, "function buildPlanDashboard("),
        ]
    )


_PAYLOAD = {
    "client_name": "Brightwell",
    "budget_range": "$150,000",
    "budget_period": "campaign",
    "campaign_duration": "6 months",
    "locations": ["Dallas, TX"],
    "target_roles": ["Nurse"],
    "_plan_budget": {"display": "$150,000", "total": 150000, "currency": "USD"},
    "_plan_results": {"channels": _NAMES, "channels_funded": len(_NAMES)},
}


def _dashboard(preview_stub: str) -> str:
    script = "\n".join(
        [
            preview_stub,
            _render_stubs(),
            "buildPlanDashboard(" + json.dumps(_PAYLOAD) + ", null);",
            "process.stdout.write(JSON.stringify(out.innerHTML));",
        ]
    )
    return run_node(script)


@needs_node
def test_results_open_with_the_quick_answer_card_above_the_dashboard():
    qa = {
        "ready": True,
        "budget": "$150K",
        "cpa": "$14.78–$89",
        "applications": "1,685–10,149",
        "hires": "15–30",
        "supports": "Supports an estimated 15–30 hires against your goal.",
        "basis": "at an assumed $10K all-in cost per hire (Nova table)",
        "tone": "warn",
        "channels": _NAMES[:5],
        "more": 2,
    }
    html = _dashboard(
        "var NovaPreview = { quickAnswer: function (names) { return "
        + json.dumps(qa)
        + "; } };"
    )
    # first thing after the title, before the metric tiles and the long sections
    assert (
        html.index('class="plan-dashboard-title"')
        < html.index('id="planQuickAnswer"')
        < html.index("plan-metrics-row")
    )
    assert html.index('id="planQuickAnswer"') < html.index("Next steps")
    # the four answers
    assert "Cost per application" in html and "$14.78–$89" in html
    assert "Applications" in html and "1,685–10,149" in html
    assert (
        "What your budget supports" in html
        and "<b>$150,000</b> total campaign budget" in html
    )
    assert "Hires: at an assumed $10K all-in cost per hire (Nova table)." in html
    assert (
        "Recommended channels" in html and html.count("<li>") >= 5 + 3
    )  # 5 chips + the recs list
    assert (
        "+2 more, largest budget first, in the Funded list below" in html
        and "Funded:" in html
    )
    # collapsible and open by default; never a promise
    assert '<details class="plan-quick" id="planQuickAnswer" open>' in html
    assert "not a guarantee" in html
    # no bare point estimate sits beside a range label
    card = html[html.index('id="planQuickAnswer"') : html.index("plan-metrics-row")]
    assert "–" in card and card.count("plan-quick-fig-value") == 3


@needs_node
def test_quick_answer_without_the_preview_still_lists_budget_and_channels_only():
    html = _dashboard("")  # NovaPreview is not defined on the page
    card = html[html.index('id="planQuickAnswer"') : html.index("plan-metrics-row")]
    assert "plan-quick-fig-value" not in card  # no figure without a source
    assert "estimate was not available on screen" in card
    assert "<b>$150,000</b>" in card and "Programmatic" in card and "Social" in card
    assert "Employer brand" not in card  # only the top 5 are chips


@needs_node
def test_quick_answer_escapes_what_it_prints():
    nasty = {
        "ready": True,
        "budget": "",
        "cpa": "<img src=x onerror=alert(1)>",
        "applications": "1–9",
        "hires": "",
        "supports": "<script>x</script>",
        "basis": "",
        "tone": 'info"><b>',
        "channels": ["<b>Bad</b>"],
        "more": 0,
    }
    html = _dashboard(
        "var NovaPreview = { quickAnswer: function () { return "
        + json.dumps(nasty)
        + "; } };"
    )
    card = html[html.index('id="planQuickAnswer"') : html.index("plan-metrics-row")]
    assert "<img" not in card and "<script" not in card and "<b>Bad" not in card
    assert 'info"><b>' not in card


def test_quick_answer_is_375_safe_and_exposed_by_the_preview():
    assert "quickAnswer: quickAnswer" in PREVIEW
    last = LAST
    assert "@media (max-width: 560px)" in last and "grid-template-columns: 1fr" in last
    assert (
        "repeat(auto-fit, minmax(150px, 1fr))" in last
    )  # the figures reflow, never overflow


# ══════════════════════════════════════════════════════════════════════════
# 4. Draft status + download / restore of the inputs
# ══════════════════════════════════════════════════════════════════════════


def _draft_fns() -> str:
    return "\n".join(
        [
            "var NOVA_INPUTS_FILE_APP = 'nova-media-plan-inputs'; var NOVA_INPUTS_FILE_VERSION = 1;"
            " var NOVA_INPUTS_FILE_MAX_BYTES = 2000000;",
            js_block(APP, "function novaDraftStatusText("),
            js_block(APP, "function novaInputsFileName("),
            js_block(APP, "var NOVA_INPUTS_STRING_KEYS") + ";",
            slice_between(APP, "var NOVA_INPUTS_LIST_KEYS", "];") + "];",
            js_block(APP, "function novaCleanJson("),
            js_block(APP, "function novaSanitizeInputsFile("),
        ]
    )


@needs_node
def test_draft_status_says_what_is_true():
    got = run_node(
        _draft_fns() + "\nvar t = 1760790000000;"
        "process.stdout.write(JSON.stringify([novaDraftStatusText(0, false), novaDraftStatusText(t, false),"
        " novaDraftStatusText(0, true)]));"
    )
    assert got[0] == "Your inputs autosave in this browser tab while you work."
    assert re.fullmatch(
        r"Draft saved in this tab( at [0-9:APM\u202f\u00a0 ]+)?\.", got[1]
    ), got[1]
    assert "could not be saved" in got[2] and "Download your inputs" in got[2]
    # honest about scope: tab-only, and accounts are not claimed
    assert "Saved plans for accounts are not turned on yet" in BODY
    assert "a closed tab loses" in BODY
    assert 'id="downloadInputsBtn"' in BODY and 'id="restoreInputsBtn"' in BODY
    assert 'accept="application/json,.json"' in BODY


@needs_node
def test_inputs_file_round_trips_and_keeps_only_what_the_wizard_saves():
    state = {
        "clientName": "Acme",
        "useCase": "Hire nurses",
        "exactBudget": "150,000",
        "budgetRange": "__exact__",
        "campaignDuration": "6 months",
        "selectedIndustry": "healthcare_medical",
        "locations": ["Dallas, TX", "Austin, TX"],
        "roles": ["Registered Nurse"],
        "excludedPublishers": ["indeed.com"],
        "channelsTouched": True,
        "channels": {"chSocial": True, "chProgrammatic": False},
        "sections": {"includeDEI": True},
        "enterprise": {"on": False, "siteGoals": {"Dallas, TX": 12}},
    }
    file = {
        "app": "nova-media-plan-inputs",
        "version": 1,
        "saved_at": "2026-10-18T00:00:00Z",
        "inputs": state,
    }
    got = run_node(
        _draft_fns()
        + "\nprocess.stdout.write(JSON.stringify(novaSanitizeInputsFile("
        + json.dumps(json.dumps(file))
        + ")));"
    )
    assert got["ok"] is True
    assert got["state"] == state  # nothing the wizard saved is lost


@needs_node
def test_inputs_file_rejects_what_is_not_an_inputs_file_and_strips_the_rest():
    def sanitize(text: str) -> dict:
        return run_node(
            _draft_fns()
            + "\nprocess.stdout.write(JSON.stringify(novaSanitizeInputsFile("
            + json.dumps(text)
            + ")));"
        )

    good_inputs = {"clientName": "Acme"}
    wrap = lambda **kw: json.dumps(
        {"app": "nova-media-plan-inputs", "version": 1, "inputs": good_inputs, **kw}
    )  # noqa: E731
    assert sanitize("")["ok"] is False
    assert "not valid JSON" in sanitize("{nope")["error"]
    assert (
        "not a Nova inputs file"
        in sanitize(json.dumps({"inputs": good_inputs}))["error"]
    )
    assert "newer version" in sanitize(wrap(version=2))["error"]
    assert (
        "none of the wizard's inputs"
        in sanitize(
            json.dumps(
                {"app": "nova-media-plan-inputs", "version": 1, "inputs": {"zzz": 1}}
            )
        )["error"]
    )
    assert sanitize("x" * 2_000_001)["ok"] is False
    # unknown keys, prototype keys, wrong types and over-long values never reach the form
    hostile = {
        "app": "nova-media-plan-inputs",
        "version": 1,
        "inputs": {
            "clientName": "A" * 900,
            "useCase": 5,
            "roles": ["ok", 7, "", "x" * 400],
            "evil": "<script>",
            "channels": {
                "chSocial": True,
                "__proto__": True,
                "notach": True,
                "chX": "yes",
            },
            "enterprise": {"siteGoals": {"constructor": 1, "Dallas": 3}},
        },
    }
    got = sanitize(json.dumps(hostile))
    assert got["ok"] is True
    s = got["state"]
    assert len(s["clientName"]) == 500 and "useCase" not in s and "evil" not in s
    assert s["roles"] == ["ok", "x" * 300]
    assert s["channels"] == {"chSocial": True}
    assert s["enterprise"] == {"siteGoals": {"Dallas": 3}}
    # a JSON.parse'd __proto__ key must not have changed any prototype
    assert (
        run_node(
            _draft_fns()
            + '\nnovaSanitizeInputsFile(\'{"app":"nova-media-plan-inputs","version":1,"inputs":{"clientName":"a","__proto__":{"polluted":1}}}\');'
            "process.stdout.write(JSON.stringify({}.polluted === undefined));"
        )
        is True
    )


@needs_node
def test_inputs_file_name_is_safe():
    got = run_node(
        _draft_fns() + "\nvar d = new Date('2026-10-18T09:00:00Z');"
        "process.stdout.write(JSON.stringify([novaInputsFileName('Acme Corp / Q4!', d), novaInputsFileName('', d),"
        " novaInputsFileName('../../etc/passwd', d), novaInputsFileName('x'.repeat(80), d).length]));"
    )
    assert got[0] == "nova-plan-inputs-acme-corp-q4-2026-10-18.json"
    assert got[1] == "nova-plan-inputs-2026-10-18.json"
    assert (
        "/" not in got[2] and ".." not in got[2] and got[2].endswith("2026-10-18.json")
    )
    assert got[3] <= len("nova-plan-inputs-") + 30 + len("-2026-10-18.json")


def test_inputs_file_code_is_client_side_only():
    start = APP.index('// Draft status + "Download your inputs"')
    end = APP.index("// Unsaved changes warning (beforeunload)")
    block = APP[start:end]
    for banned in (
        "fetch(",
        "XMLHttpRequest",
        "sendBeacon",
        "localStorage",
        "WebSocket",
        "navigator.clipboard",
    ):
        assert banned not in block, banned
    assert "FileReader" in block and "URL.createObjectURL" in block


@needs_node
def test_render_tags_persists_the_draft_and_refreshes_the_needs_line():
    """Pre-fix: a tag committed with Enter fired no input/change event, so the
    autosave never saw it (a reload after "add role, Enter" lost the role)."""
    script = "\n".join(
        [
            "var saved = 0, needs = 0;",
            "function saveFormState() { saved++; } function novaRenderStepNeeds() { needs++; }",
            "function mk() { return { children: [], innerHTML: '', dataset: {}, appendChild: function (c) { this.children.push(c); },",
            "  focus: function () {}, classList: { add: function () {} } }; }",
            "var container = mk(), input = mk();",
            "var document = { getElementById: function (id) { return id === 'c' ? container : input; },",
            "  createElement: function () { var e = mk(); e.setAttribute = function () {}; e.appendChild = function () {}; return e; } };",
            "function removeTag() {} function getArrayByContainerId() { return []; }",
            js_block(APP, "function renderTags("),
            "renderTags('c', 'i', ['Nurse']);",
            "process.stdout.write(JSON.stringify({ saved: saved }));",
        ]
    )
    assert run_node(script) == {"saved": 1}


@needs_node
def test_restoring_a_draft_keeps_the_tag_boxes_and_their_inputs():
    """Pre-fix: _restoreDraftState emptied each tag container with
    `innerHTML = ""`, which also deleted the text input the container holds
    (#roleInput / #locationInput ...). addTag then threw on its first call:
    loading a saved draft (and the new "Restore from file") restored no roles
    or locations and left steps 1-2 without their boxes until a reload."""
    script = "\n".join(
        [
            "var locations = [], roles = [], competitors = [], excludedPublishers = [], selectedJobCategories = [];",
            "var selectedIndustry = null, selectedNaicsCode = null, selectedNaicsTitle = null;",
            # a container holds its tags AND its input; innerHTML = '' empties all of it
            "function Box(id, inputId) { this.id = id; this.kids = [{ cls: 'tag', box: this }, { cls: 'input', id: inputId, value: 'x' }]; }",
            "Object.defineProperty(Box.prototype, 'innerHTML', { set: function () { this.kids = []; }, get: function () { return ''; } });",
            "Box.prototype.querySelectorAll = function () {",
            "  var self = this; return this.kids.filter(function (k) { return k.cls === 'tag'; }).map(function (k) {",
            "    return { remove: function () { self.kids = self.kids.filter(function (x) { return x !== k; }); } }; }); };",
            "var BOXES = { locationsContainer: new Box('locationsContainer', 'locationInput'), rolesContainer: new Box('rolesContainer', 'roleInput'),",
            "  competitorsContainer: new Box('competitorsContainer', 'competitorInput'),",
            "  excludedPublishersContainer: new Box('excludedPublishersContainer', 'excludedPublisherInput') };",
            "var document = { getElementById: function (id) {",
            "  if (BOXES[id]) return BOXES[id];",
            "  for (var k in BOXES) { var hit = BOXES[k].kids.filter(function (x) { return x.id === id; })[0]; if (hit) return hit; }",
            "  return null; },",
            "  querySelector: function () { return null; }, querySelectorAll: function () { return []; } };",
            # addTag as the page's does at its core: it clears the input box, then records
            "function addTag(cid, iid, arr, v) { document.getElementById(iid).value = ''; arr.push(v); }",
            "function novaRestoreRegion() {} function setNovaChannelsTouched() {} function novaEnterpriseRestore() {}",
            "function checkBudgetFeasibility() {} function novaSyncJobCategoryCards() {}",
            js_block(APP, "function _restoreDraftState("),
            "_restoreDraftState({ roles: ['Nurse'], locations: ['Dallas, TX'], competitors: ['Acme'], excludedPublishers: ['indeed.com'] });",
            "process.stdout.write(JSON.stringify({ roles: roles, locations: locations, competitors: competitors,",
            "  excluded: excludedPublishers, inputsStillThere: ['roleInput', 'locationInput', 'competitorInput', 'excludedPublisherInput']",
            "    .map(function (id) { return !!document.getElementById(id); }) }));",
        ]
    )
    got = run_node(script)
    assert got["roles"] == ["Nurse"] and got["locations"] == ["Dallas, TX"]
    assert got["competitors"] == ["Acme"] and got["excluded"] == ["indeed.com"]
    assert got["inputsStillThere"] == [True, True, True, True]


# ══════════════════════════════════════════════════════════════════════════
# 5. Nova chat hand-off: excluded_publishers prefill
# ══════════════════════════════════════════════════════════════════════════


def _prefill_run(search: str, existing: list | None = None) -> dict:
    """Run the REAL _applyUrlParamPrefill (+ the real commitExcludedBoards and
    the page's board-name helpers) for a hand-off URL query string."""
    script = "\n".join(
        [
            inputs_functions_js(),
            "var excludedPublishers = "
            + json.dumps(existing or [])
            + ", locations = [], roles = [];",
            "var tags = [], toasts = [], previewUpdates = 0, persisted = 0;",
            "function El(id) { this.id = id; this.value = ''; this.dataset = {}; this.listeners = {};",
            "  this.classList = { add: function () {}, remove: function () {} }; }",
            "El.prototype.dispatchEvent = function () {};",
            "var ELS = {};",
            "var document = { getElementById: function (id) { return ELS[id] || (ELS[id] = new El(id)); },",
            "  querySelector: function () { return null; } };",
            "var window = { location: { search: " + json.dumps(search) + " },",
            "  NovaPreview: { update: function () { previewUpdates++; } } };",
            "function Event(t) { this.type = t; }",
            "function showToast(m) { toasts.push(m); }",
            "function _prefillIndustryKey() { return ''; } function _prefillDurationOption() { return ''; }",
            # the page's addTag pushes the entry and re-renders (which saves the draft)
            "function addTag(container, input, arr, value) { arr.push(value); tags.push([container, value]); persisted++; }",
            js_block(APP, "function commitExcludedBoards("),
            js_block(APP, "function _applyUrlParamPrefill("),
            "_applyUrlParamPrefill();",
            "process.stdout.write(JSON.stringify({ excluded: excludedPublishers, tags: tags, toasts: toasts,",
            "  previewUpdates: previewUpdates, note: (ELS.excludedPublisherNote || {}).textContent || '' }));",
        ]
    )
    return run_node(script)


@needs_node
def test_url_prefill_reads_excluded_publishers_through_the_normal_commit_path():
    """The hand-off URL Nova builds: ?excluded_publishers=indeed.com,Indeed,Zip+Recruiter.
    "Indeed" is the same board as "indeed.com" (one chip); "Zip Recruiter" is kept
    as typed. The chips are added to the excluded-boards container, so they
    persist with the draft and feed both request builders."""
    got = _prefill_run("?excluded_publishers=indeed.com%2CIndeed%2CZip+Recruiter")
    assert got["excluded"] == ["indeed.com", "Zip Recruiter"]
    assert got["tags"] == [
        ["excludedPublishersContainer", "indeed.com"],
        ["excludedPublishersContainer", "Zip Recruiter"],
    ]
    assert "same board as" in got["note"]  # the user is told "Indeed" was folded in
    assert got["previewUpdates"] >= 1  # the live estimate re-reads the exclusions


@needs_node
def test_url_prefill_runs_for_exclusions_alone_and_splits_semicolons():
    got = _prefill_run(
        "?excluded_publishers=ZipRecruiter%3BMonster"
    )  # no other param at all
    assert got["excluded"] == ["ZipRecruiter", "Monster"]
    assert got["toasts"], "the hand-off still tells the user the brief was loaded"


@needs_node
def test_url_prefill_does_not_duplicate_or_accept_generic_words():
    got = _prefill_run(
        "?excluded_publishers=Indeed%2Cjobs%2C+%2Cindeed.com%2CMonster",
        existing=["indeed.com"],
    )
    # "Indeed" / "indeed.com" are already excluded (one board); "jobs" is not a board; Monster is new
    assert got["excluded"] == ["indeed.com", "Monster"]
    assert got["tags"] == [["excludedPublishersContainer", "Monster"]]


@needs_node
def test_prefilled_exclusions_reach_the_estimate_and_generate_requests():
    """Both request builders read the same list the chips hold (one source),
    so a handed-off exclusion goes to /api/estimate and /api/generate exactly
    like a typed one."""
    excluded = _prefill_run("?excluded_publishers=indeed.com%2CZip+Recruiter")[
        "excluded"
    ]
    got = wizard_payloads(
        {
            "budgetRange": {"value": "__exact__"},
            "exactBudget": {"value": "150000"},
            "campaignDuration": {"value": "6 months"},
        },
        selected_industry="healthcare_medical",
        locations=["Dallas, TX"],
        roles=["Nurse"],
        excluded_publishers=excluded,
    )
    assert (
        got["generate"]["excluded_publishers"]
        == excluded
        == ["indeed.com", "Zip Recruiter"]
    )
    assert got["estimate"]["excluded_publishers"] == excluded


def test_prefill_key_list_includes_the_exclusions():
    src = js_block(APP, "function _applyUrlParamPrefill(")
    assert '"excluded_publishers"' in src[: src.index("if (!keys.some")]
