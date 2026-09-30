"""Run the wizard's REAL request builders under node, from one form state.

Shared by the preview-vs-generate parity tests: the /api/generate payload
object literal (generatePlan in body_app_js.html) and the /api/estimate body
the live preview posts (estimatePayload in body_preview_js.html) are lifted
verbatim from the templates and evaluated against a stub DOM, so a field one
builder sends and the other omits shows up as a diff.

Also exposes the budget/duration functions of body_inputs_js.html with the
server's own embedded tables (template_composer.wizard_inputs_json), exactly
as the page receives them.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parent.parent
PARTIALS = ROOT / "templates" / "partials" / "index"
APP_JS = PARTIALS / "body_app_js.html"
PREVIEW_JS = PARTIALS / "body_preview_js.html"
INPUTS_JS = PARTIALS / "body_inputs_js.html"
NODE = shutil.which("node")


def slice_between(src: str, start: str, end: str) -> str:
    i = src.index(start)
    return src[i : src.index(end, i)]


def js_block(src: str, start: str) -> str:
    """Text from ``start`` through the brace that closes the first ``{``
    after it (naive brace count -- the lifted blocks keep braces out of
    strings and regex literals)."""
    i = src.index(start)
    j = src.index("{", i)
    depth = 0
    for k in range(j, len(src)):
        if src[k] == "{":
            depth += 1
        elif src[k] == "}":
            depth -= 1
            if depth == 0:
                return src[i : k + 1]
    raise ValueError(f"unbalanced block after {start!r}")


def inputs_functions_js() -> str:
    """``var NOVA_INPUTS = <server config>;`` + the body_inputs_js functions."""
    import template_composer

    src = INPUTS_JS.read_text(encoding="utf-8")
    funcs = slice_between(
        src, "/* @@wizard-inputs:functions-start */", "/* @@wizard-inputs:functions-end */"
    )
    return "var NOVA_INPUTS = " + template_composer.wizard_inputs_json() + ";\n" + funcs


def run_node(script: str) -> Any:
    if NODE is None:
        raise RuntimeError("node not installed")
    out = subprocess.run(
        [NODE, "-e", script], capture_output=True, text=True, timeout=60
    )
    if out.returncode != 0:
        raise AssertionError(f"node failed:\n{out.stderr}")
    return json.loads(out.stdout)


def _stub_dom(state: dict) -> str:
    return "\n".join(
        [
            "var STATE = " + json.dumps(state) + ";",
            "var listeners = [];",
            "var ELS = {};",
            "var document = {",
            "  getElementById: function (id) {",
            "    if (!(id in ELS)) {",
            "      var s = STATE[id] || {};",
            "      ELS[id] = { id: id, value: s.value || '', checked: !!s.checked,",
            "        closest: function () { return null; } };",
            "    }",
            "    return ELS[id];",
            "  },",
            "  querySelectorAll: function (sel) {",
            "    var m = /name=\"([^\"]+)\"/.exec(sel || '');",
            "    var name = m ? m[1] : '';",
            "    return (STATE.__checked && STATE.__checked[name] || []).map(function (v) {",
            "      return { value: v };",
            "    });",
            "  },",
            "  querySelector: function () { return null; },",
            "  addEventListener: function (t, fn) { if (t === 'change') listeners.push(fn); },",
            "};",
            "var window = {};",
        ]
    )


def wizard_payloads(
    state: dict,
    *,
    selected_industry: Optional[str] = "food_beverage",
    locations: Optional[list] = None,
    roles: Optional[list] = None,
    touch: Optional[dict] = None,
) -> dict:
    """{"generate": <payload>, "estimate": <preview's /api/estimate body>}
    after JSON round-trip, for one wizard form state.

    ``state`` maps element id -> {"value"|"checked"}; ``touch`` applies
    channel-toggle changes as user 'change' events first.
    """
    app_src = APP_JS.read_text(encoding="utf-8")
    prev_src = PREVIEW_JS.read_text(encoding="utf-8")

    parts = [
        slice_between(
            app_src,
            "// ── Channel selection (one rule for generate, estimate and preview)",
            "// ── end channel selection ──",
        ),
        js_block(app_src, "function getSelectedRegion()"),
    ]
    gen_region = app_src[app_src.index("// 28. Updated payload with new fields") :]
    payload_literal = js_block(gen_region, "const payload = {")
    payload_literal = payload_literal[len("const payload = ") :]

    if "function planInputs()" in prev_src:
        parts.append(js_block(prev_src, "function planInputs()"))
        parts.append(js_block(prev_src, "function estimatePayload("))
        estimate_call = "estimatePayload({})"
    else:  # pre-2026-10-01 preview: its own hand-built body
        parts.append(js_block(prev_src, "function estimatePayload("))
        parts.append(
            "function estimateChannelCategories() { return undefined; }"
        )
        estimate_call = (
            "estimatePayload({ total: 0, locs: locations }, roles, selectedIndustry, "
            "document.getElementById('clientName').value, novaChannelCategoriesPayload())"
        )

    script = "\n".join(
        [
            _stub_dom(state),
            "var selectedIndustry = " + json.dumps(selected_industry) + ";",
            "var selectedNaicsCode = null, selectedNaicsTitle = null;",
            "var selectedJobCategories = [], channelsDB = null;",
            "var locations = " + json.dumps(locations or []) + ";",
            "var roles = " + json.dumps(roles or []) + ";",
            "var competitors = [], briefFiles = [], transcriptFiles = [], historicalFiles = [];",
            "function getCheckboxGroupValues() { return []; }",
            "function $(id) { return document.getElementById(id); }",
            "function val(id) { var el = $(id); return el ? (el.value || '').trim() : ''; }",
            "function readTags() { return []; }",
            "function channelOn(id) { var el = $(id); return !!(el && el.checked); }",
            *parts,
            "var touch = " + json.dumps(touch) + ";",
            "if (touch) Object.keys(touch).forEach(function (id) {",
            "  document.getElementById(id).checked = touch[id];",
            "  listeners.forEach(function (fn) { fn({ target: { id: id } }); });",
            "});",
            "var payload = " + payload_literal + ";",
            "var est = " + estimate_call + ";",
            "process.stdout.write(JSON.stringify({",
            "  generate: JSON.parse(JSON.stringify(payload)),",
            "  estimate: JSON.parse(JSON.stringify(est)),",
            "}));",
        ]
    )
    return run_node(script)
