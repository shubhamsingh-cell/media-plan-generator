"""Wizard traps from the 2026-10-01 UI audit (D_wizard_ui.md D-01/D-08/D-09).

D-01: "Clear Form" set ``innerHTML = ""`` on the tag containers, which also
      HOLD the text inputs (#locationInput/#roleInput/#competitorInput) --
      steps 1-2 could never be completed again until a reload. One click,
      no confirm.
D-08: "Cancel Generation" cleared the poll interval but never settled the
      promise generatePlan was awaiting, so the Generate button stayed
      disabled on "Generating..." forever.
D-09: a poll answered 404/410 (job dropped by a deploy/restart) was treated
      as "still processing" -- the overlay spun for up to 8.5 minutes.

The REAL functions are lifted from body_app_js.html and run under node
against a stub DOM with a hand-driven poll clock (no 2 s waits).
"""

from __future__ import annotations

import json
import re

import pytest

from tests.wizard_js_harness import APP_JS, NODE, PARTIALS, js_block, run_node

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")


# ── D-01 ──────────────────────────────────────────────────────────────────

_DOM_JS = r"""
function mkEl(id, tag) {
  var el = { id: id, tagName: tag || 'INPUT', value: '', checked: false,
    defaultChecked: false, textContent: '', children: [], parent: null,
    style: {}, focused: false, options: [], selectedIndex: 0,
    classList: { set: {}, add: function (c) { this.set[c] = 1; },
      remove: function (c) { delete this.set[c]; },
      contains: function (c) { return !!this.set[c]; } },
    focus: function () { focusLog.push(id); },
    appendChild: function (c) { c.parent = this; this.children.push(c); return c; },
    remove: function () { var p = this.parent; if (p) p.children.splice(p.children.indexOf(this), 1); this.parent = null; },
    querySelectorAll: function (sel) {
      return sel === '.tag' ? this.children.filter(function (c) { return c.className === 'tag'; }) : [];
    },
    getBoundingClientRect: function () { return { top: 10, bottom: 40 }; },
  };
  Object.defineProperty(el, 'innerHTML', { set: function (v) {
    this.children.forEach(function (c) { c.parent = null; }); this.children = []; } });
  return el;
}
var focusLog = [], ELS = {};
function attached(el) { var n = el; while (n.parent) n = n.parent; return n === ROOT; }
var ROOT = mkEl('root', 'DIV');
var document = {
  getElementById: function (id) { var el = ELS[id]; return el && attached(el) ? el : null; },
  querySelectorAll: function () { return []; },
};
[['locationsContainer', 'locationInput'], ['rolesContainer', 'roleInput'],
 ['competitorsContainer', 'competitorInput']].forEach(function (p) {
  var box = ELS[p[0]] = ROOT.appendChild(mkEl(p[0], 'DIV'));
  var tag = mkEl('', 'SPAN'); tag.className = 'tag'; box.appendChild(tag);
  ELS[p[1]] = box.appendChild(mkEl(p[1]));
  ELS[p[1]].value = 'half-typed';
});
['clientName', 'locationResolution'].forEach(function (id) { ELS[id] = ROOT.appendChild(mkEl(id)); });
ELS.clientName.value = 'Acme';
var window = { innerHeight: 800 };
var sessionStorage = { removeItem: function () {} };
var _FORM_STORAGE_KEY = 'k', _locResolveTimer = null, _locResolveSeq = 0;
var selectedIndustry = 'x', selectedJobCategories = ['a'];
var locations = ['Austin, TX'], roles = ['Nurse'], competitors = ['Rival'];
var steps = [], toasts = [], timers = [];
function clearNaicsSelection() {}
function goToStep(s) { steps.push(s); }
function showToast(m, t) { toasts.push([m, t]); }
function novaPrefersReducedMotion() { return false; }
function novaScrollBehavior() { return 'smooth'; }
function setTimeout(fn, ms) { timers.push([ms, fn]); return timers.length; }
function clearTimeout() {}
"""


def _clear_form_script(body: str) -> str:
    src = APP_JS.read_text(encoding="utf-8")
    return "\n".join([_DOM_JS, js_block(src, "function clearFormState()"), body])


def test_clear_form_keeps_the_tag_inputs():
    got = run_node(
        _clear_form_script(
            """
clearFormState();
process.stdout.write(JSON.stringify({
  inputs: ['locationInput', 'roleInput', 'competitorInput'].map(function (id) {
    var el = document.getElementById(id); return el ? el.value : null; }),
  tags: ['locationsContainer', 'rolesContainer', 'competitorsContainer'].map(function (id) {
    return ELS[id].querySelectorAll('.tag').length; }),
  arrays: [locations, roles, competitors],
  client: ELS.clientName.value, steps: steps, focus: focusLog, toasts: toasts,
}));
"""
        )
    )
    # every input is still in the page (pre-fix: [null, null, null])
    assert got["inputs"] == ["", "", ""], got
    assert got["tags"] == [0, 0, 0]
    assert got["arrays"] == [[], [], []]
    assert got["client"] == ""
    # the form starts over where it starts: step 1, first field focused
    assert got["steps"] == [1]
    assert "clientName" in got["focus"]
    assert got["toasts"] == [["Form cleared", "success"]]


def test_clear_form_needs_a_second_click():
    src = APP_JS.read_text(encoding="utf-8")
    script = "\n".join(
        [
            "var cleared = 0, timers = [];",
            "function clearFormState() { cleared++; }",
            "function setTimeout(fn, ms) { timers.push([ms, fn]); return timers.length; }",
            "function clearTimeout() {}",
            "var btn = { textContent: 'Clear Form', cls: {}, classList: {",
            "  add: function (c) { btn.cls[c] = 1; }, remove: function (c) { delete btn.cls[c]; },",
            "  contains: function (c) { return !!btn.cls[c]; } } };",
            "var _clearFormArmTimer = null;",
            js_block(src, "function _disarmClearForm("),
            js_block(src, "function requestClearForm("),
            "requestClearForm(btn); var afterOne = [cleared, btn.textContent];",
            "requestClearForm(btn); var afterTwo = [cleared, btn.textContent];",
            # an armed click that times out disarms -- the next click re-arms only
            "requestClearForm(btn); timers[timers.length - 1][1]();",
            "var afterTimeout = [cleared, btn.textContent];",
            "requestClearForm(btn); var afterLate = [cleared];",
            "process.stdout.write(JSON.stringify({ afterOne: afterOne, afterTwo: afterTwo,",
            "  afterTimeout: afterTimeout, afterLate: afterLate,",
            "  delay: timers[0][0] }));",
        ]
    )
    got = run_node(script)
    assert got["afterOne"][0] == 0 and "again" in got["afterOne"][1].lower()
    assert got["afterTwo"] == [1, "Clear Form"]
    assert got["afterTimeout"] == [1, "Clear Form"]
    assert got["afterLate"] == [1]
    assert 2000 <= got["delay"] <= 8000


def test_clear_form_button_uses_the_confirm_step():
    content = (PARTIALS / "body_content.html").read_text(encoding="utf-8")
    tag = re.search(r'<button class="plan-clear-form"[^>]*>', content)
    assert tag, "Clear Form button missing"
    assert 'onclick="requestClearForm(this)"' in tag.group(0)


# ── D-08 / D-09 ───────────────────────────────────────────────────────────


def _generate_run(mode: str, ticks: int, cancel_after: int | None = None) -> dict:
    """Run the real generatePlan + cancelGeneration with /api/jobs answering
    ``mode`` ('processing' | 404 | 410 | 503) for ``ticks`` poll ticks."""
    src = APP_JS.read_text(encoding="utf-8")
    m = re.search(r"GEN_JOB_GONE_POLLS = (\d+)", src)
    script = "\n".join(
        [
            "var MODE = " + json.dumps(mode) + ";",
            "var ELS = {};",
            "function el(id) { if (!ELS[id]) ELS[id] = { id: id, value: '', checked: false,",
            "  disabled: false, innerHTML: '', style: {}, headers: {},",
            "  classList: { add: function () {}, remove: function () {} } }; return ELS[id]; }",
            "var document = { getElementById: el, querySelectorAll: function () { return []; },",
            "  createElement: function () { return el('tmp'); }, body: { appendChild: function () {} } };",
            "el('requesterName').value = 'Test User'; el('requesterEmail').value = 'test@example.com';",
            "el('budgetRange').value = '50000';",
            "var window = { matchMedia: function () { return { matches: false }; } };",
            "var navigator = { onLine: true };",
            "var locations = ['Austin, TX'], roles = ['Nurse'], competitors = [];",
            "var selectedIndustry = null, selectedNaicsCode = '', selectedNaicsTitle = '';",
            "var selectedJobCategories = [], channelsDB = null;",
            "var briefFiles = [], transcriptFiles = [], historicalFiles = [];",
            "var genPollInterval = null, genCurrentJobId = null, _genAbortController = null;",
            "var _genPollReject = null, GEN_JOB_GONE_POLLS = " + (m.group(1) if m else "2") + ";",
            "var GEN_TIMEOUT_SECS = 510;",
            "var toasts = [], hides = 0, jobPolls = 0, intervals = {}, nextId = 1;",
            "function setInterval(fn) { intervals[nextId] = fn; return nextId++; }",
            "function clearInterval(id) { delete intervals[id]; }",
            "function setTimeout(fn) { return 0; }",
            "function validateBudgetInput() { return { valid: true }; }",
            "function validateLocations() { return { valid: true }; }",
            "function validateHireVolume() { return { valid: true }; }",
            "function validateCompetitors() { return { valid: true }; }",
            "function validateField() {} function showFormError() {}",
            "function novaShowFieldErrorAt() { return false; }",
            "function novaPlanCoreInputs() { return {}; }",
            "function getCheckboxGroupValues() { return []; }",
            "function getCsrfToken() { return ''; }",
            "function showGenerationOverlay() {} function hideGenerationOverlay() { hides++; }",
            "function updateGenProgress() {}",
            "function showToast(m, t) { toasts.push([m, t]); }",
            "function waitForQaGateDecision() { return Promise.resolve(true); }",
            "function resp(status, body) { return Promise.resolve({ ok: status >= 200 && status < 300,",
            "  status: status, json: function () { return Promise.resolve(body); } }); }",
            "function fetch(url) {",
            "  if (url === '/api/generate') return resp(202, { job_id: 'j1' });",
            "  jobPolls++;",
            "  if (MODE === 'processing') return resp(200, { status: 'processing', progress_pct: 10 });",
            "  return resp(MODE, { error: 'Job not found or expired' });",
            "}",
            "console.warn = function () {}; console.error = function () {};",
            js_block(src, "function cancelGeneration()"),
            js_block(src, "async function generatePlan()"),
            "function flush() { return new Promise(function (r) { setImmediate(r); }); }",
            "(async function () {",
            "  var settled = false;",
            "  generatePlan().then(function () { settled = true; });",
            "  for (var i = 0; i < 5; i++) await flush();",
            "  for (var t = 0; t < " + str(ticks) + "; t++) {",
            "    for (var k in intervals) await intervals[k]();",
            "    for (var j = 0; j < 5; j++) await flush();",
            "    if (" + json.dumps(cancel_after) + " === t + 1) cancelGeneration();",
            "    for (var j2 = 0; j2 < 5; j2++) await flush();",
            "  }",
            "  var pollsAtEnd = jobPolls;",
            "  for (var k2 in intervals) await intervals[k2]();",
            "  for (var j3 = 0; j3 < 5; j3++) await flush();",
            "  var btn = el('generateBtn');",
            "  process.stdout.write(JSON.stringify({ settled: settled, disabled: btn.disabled,",
            "    btnHtml: btn.innerHTML, toasts: toasts, polls: pollsAtEnd,",
            "    pollsAfterOneMoreTick: jobPolls, liveIntervals: Object.keys(intervals).length,",
            "    hides: hides }));",
            "})();",
        ]
    )
    return run_node(script)


def test_cancel_generation_reenables_generate():
    got = _generate_run("processing", ticks=1, cancel_after=1)
    assert got["settled"], got
    assert got["disabled"] is False, got  # pre-fix: True, "Generating..." forever
    assert "Generate Media Plan" in got["btnHtml"]
    assert got["liveIntervals"] == 0 and got["pollsAfterOneMoreTick"] == got["polls"]
    assert len(got["toasts"]) == 1 and got["toasts"][0][1] == "info"
    assert "cancelled" in got["toasts"][0][0].lower()


@pytest.mark.parametrize("status", [404, 410])
def test_vanished_job_stops_within_two_polls(status):
    got = _generate_run(status, ticks=2)
    assert got["settled"], got  # pre-fix: still polling, never settles
    assert got["polls"] == 2 and got["liveIntervals"] == 0
    assert got["disabled"] is False
    assert len(got["toasts"]) == 1
    msg, kind = got["toasts"][0]
    assert kind == "error"
    assert "interrupted" in msg and "inputs are saved" in msg and "Generate to retry" in msg


def test_transient_server_errors_keep_polling():
    # 5xx is not "job gone" -- a genuinely slow/overloaded job keeps the
    # 8.5-minute ceiling.
    got = _generate_run(503, ticks=4)
    assert got["settled"] is False
    assert got["disabled"] is True
    assert got["liveIntervals"] == 1 and got["toasts"] == []
