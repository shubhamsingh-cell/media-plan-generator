"""Jump-to-field after a server 400 lands on the field; the budget input
takes words on phones and is full width at 375 px (independent verifier on
2ad8904, 2026-10-01).

Pre-fix: novaShowFieldErrorAt focused the field immediately after
goToStep(), whose smooth window.scrollTo(top) then won -- at 1024 px 2 of 4
runs ended at the top of step 1 with the field ~1300 px below. The budget
input was inputmode="numeric" (an iPhone keypad cannot type "M", "k" or
"lakh" the hint advertises) and sat in a 61 px column at 375 px because
`.form-row.form-row-3` out-specified the mobile `.form-row` rule.
"""

from __future__ import annotations

import json
import re

import pytest

from tests.wizard_js_harness import APP_JS, NODE, PARTIALS, js_block, run_node


@pytest.mark.skipif(NODE is None, reason="node not installed")
@pytest.mark.parametrize("current_step,expect_switch", [(5, True), (1, False)])
def test_jump_lands_after_the_step_transition(current_step, expect_switch):
    src = APP_JS.read_text(encoding="utf-8")
    script = "\n".join(
        [
            "var log = [], timers = [], currentStep = " + str(current_step) + ";",
            "var window = { innerHeight: 800, matchMedia: function () { return { matches: false }; } };",
            "function setTimeout(fn, ms) { timers.push([ms, fn]); }",
            "var el = { id: 'exactBudget',",
            "  closest: function () { return { id: 'step1' }; },",
            "  focus: function (o) { log.push(['focus', o && o.preventScroll]); },",
            "  scrollIntoView: function (o) { log.push(['scroll', o.block]); },",
            "  getBoundingClientRect: function () { return { top: 2000, bottom: 2040 }; } };",
            "var document = { getElementById: function (id) { return id === 'exactBudget' ? el : null; } };",
            "function goToStep(s) { log.push(['goToStep', s]); }",
            "function _showFieldError() { log.push(['error']); }",
            "var NOVA_FIELD_TARGETS = { budget_range: 'exactBudget' };",
            js_block(src, "function novaShowFieldErrorAt("),
            "var ok = novaShowFieldErrorAt('budget_range', 'Budget must be at least 100');",
            "var before = log.slice();",
            "timers.sort(function (a, b) { return a[0] - b[0]; }).forEach(function (t) { t[1](); });",
            "process.stdout.write(JSON.stringify({ ok: ok, before: before, after: log,",
            "  delays: timers.map(function (t) { return t[0]; }) }));",
        ]
    )
    got = run_node(script)
    assert got["ok"] is True
    if expect_switch:
        # nothing scrolls/focuses until the 400 ms step transition is over
        assert got["before"] == [["goToStep", 1], ["error"]]
        assert min(got["delays"]) >= 400
        assert ["focus", True] in got["after"] and ["scroll", "center"] in got["after"]
        # a late top-scroll that left the field out of view is corrected
        assert got["after"].count(["scroll", "center"]) == 2
    else:
        assert got["before"] == [["error"], ["focus", True], ["scroll", "center"]]


def test_budget_input_accepts_words_on_phones():
    content = (PARTIALS / "body_content.html").read_text(encoding="utf-8")
    tag = content[content.index('id="exactBudget"') - 200 : content.index('id="exactBudget"') + 400]
    assert 'inputmode="text"' in tag and 'inputmode="numeric"' not in tag
    assert 'autocomplete="off"' in tag


def test_three_column_rows_stack_on_phones():
    css = (PARTIALS / "head_styles.html").read_text(encoding="utf-8")
    block = css[css.index("@media (max-width: 768px) {") :]
    block = block[: block.index("\n  }\n")]
    rule = re.search(r"([^{}]+)\{\s*/\*[^*]*\*+(?:[^/*][^*]*\*+)*/\s*grid-template-columns:\s*1fr;", block)
    assert rule and ".form-row.form-row-3" in rule.group(1), block[:400]
