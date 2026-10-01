"""Programmatic scrolls respect prefers-reduced-motion (design-judge panel on
mpg-input-parity @f159b3d, 2026-10-01, item 6 -- was OPEN).

goToStep() scrolled the window to the top with behavior "smooth" (and ran
the step-enter animation) unconditionally, and the new over-limit field
error in nextStep() smooth-scrolled the field into view with no guard;
CLAUDE.md's quality gate: every animation respects prefers-reduced-motion.
Each test stubs matchMedia the way the page sees it and records the scroll
calls.
"""

from __future__ import annotations

import json

import pytest

from tests.wizard_js_harness import APP_JS, NODE, js_block, run_node

needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")


def _prelude(reduce: bool) -> str:
    return "\n".join(
        [
            "var scrolls = [], animating = [];",
            "var window = { matchMedia: function (q) {",
            "  return { matches: " + json.dumps(reduce) + " && /prefers-reduced-motion: reduce/.test(q) }; },",
            "  scrollTo: function (o) { scrolls.push(['window', o.behavior]); } };",
            "function setTimeout(fn) { }",
        ]
    )


def _helpers() -> str:
    """The page's scroll-preference helpers (absent on pre-fix code, whose
    goToStep/nextStep then run as they were -- the behaviour under test)."""
    src = APP_JS.read_text(encoding="utf-8")
    return "\n".join(
        js_block(src, start)
        for start in ("function novaPrefersReducedMotion(", "function novaScrollBehavior(")
        if start in src
    )


@needs_node
@pytest.mark.parametrize("reduce,behavior,animates", [(True, "auto", False), (False, "smooth", True)])
def test_go_to_step_scroll_and_animation_follow_the_preference(reduce, behavior, animates):
    src = APP_JS.read_text(encoding="utf-8")
    script = "\n".join(
        [
            _prelude(reduce),
            _helpers(),
            "function Step(id) { this.id = id; this.style = {}; var s = this; this.classList = {",
            "  add: function (c) { if (c === 'step-animating') animating.push(s.id); },",
            "  remove: function () {} }; }",
            "var steps = { step1: new Step('step1'), step5: new Step('step5') };",
            "var document = { getElementById: function (id) { return steps[id]; } };",
            "var currentStep = 5, totalSteps = 5;",
            "function updateProgress() {} function updateStepCheckmarks() {}",
            js_block(src, "function goToStep("),
            "goToStep(1);",
            "process.stdout.write(JSON.stringify({ scrolls: scrolls, animating: animating }));",
        ]
    )
    got = run_node(script)
    assert got["scrolls"] == [["window", behavior]]
    assert (got["animating"] == ["step1"]) is animates


@needs_node
@pytest.mark.parametrize("reduce,behavior", [(True, "auto"), (False, "smooth")])
def test_over_limit_field_error_scroll_follows_the_preference(reduce, behavior):
    src = APP_JS.read_text(encoding="utf-8")
    script = "\n".join(
        [
            _prelude(reduce),
            _helpers(),
            "function Field(id, v) { this.id = id; this.value = v; }",
            "Field.prototype.focus = function () {};",
            "Field.prototype.scrollIntoView = function (o) { scrolls.push([this.id, o.behavior]); };",
            "var fields = { clientName: new Field('clientName', 'Acme'),",
            "  useCase: new Field('useCase', 'x'.repeat(10)) };",
            "var document = { getElementById: function (id) { return fields[id] || null; } };",
            "var currentStep = 1, totalSteps = 5, toasts = [];",
            "function showToast(m) { toasts.push(m); }",
            "function _showFieldError() {} function _clearFieldError() {}",
            "function novaClientNameProblem() { return ''; }",
            "function novaCheckTextLimit(id) { return id === 'useCase' ? 'too long' : ''; }",
            "function novaTextLimitError() { return 'Use case / brief is too long'; }",
            js_block(src, "function nextStep("),
            "nextStep();",
            "process.stdout.write(JSON.stringify({ scrolls: scrolls, toasts: toasts }));",
        ]
    )
    got = run_node(script)
    assert got["scrolls"] == [["useCase", behavior]], got


def test_no_unguarded_smooth_scroll_left_in_the_wizard():
    src = APP_JS.read_text(encoding="utf-8")
    assert 'behavior: "smooth"' not in src
    assert src.count("behavior: novaScrollBehavior()") >= 10
