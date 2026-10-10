"""The platform shell must not break the modules it injects.

/platform/<route> loads each product (the media-plan wizard included) as a
fragment: fetch the HTML, set innerHTML, then re-create every <script> so it
runs. Two defects in that re-creation killed the wizard inside the platform
(Brendan Macomber, 2026-10-09: "budget isn't recognized", "locations don't
stick", "none of the buttons work", "can't add competitors"):

1. Every <script> was re-created with NO attributes, so the wizard's data
   block ``<script type="application/json" id="novaWizardInputs">`` ran as
   code (SyntaxError) and lost its id -- NOVA_INPUTS was null, so the
   wizard had no budget rules.
2. Revisiting a module (tab switch, back button, sidebar) re-ran its scripts
   in the same document: ``Identifier 'currentStep' has already been
   declared`` -- the whole wizard script died and the fresh form had no
   handlers at all.

These run the REAL NovaRouter from static/js/platform.js under node against
a stub DOM, so they fail on the pre-fix router.
"""

from __future__ import annotations

import json
import re

import pytest

from tests.wizard_js_harness import NODE, ROOT, js_block, run_node

PLATFORM_JS = ROOT / "static" / "js" / "platform.js"

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")


def _router_js() -> str:
    src = PLATFORM_JS.read_text(encoding="utf-8")
    return js_block(src, "const NovaRouter = (() =>") + ")();"


# A stub browser: just enough DOM for NovaRouter to load a fragment, run its
# scripts, and switch tabs. frame.innerHTML parses <script> tags into stub
# elements; replaceChild records what the router actually re-created.
_STUB = r"""
var reloads = 0, fetches = [];
function Attr(name, value) { this.name = name; this.value = value; }
function El(tag, attrs, text) {
  this.tagName = tag.toUpperCase();
  this._attrs = attrs || {};
  this.textContent = text || "";
  this.parentNode = null;
  var self = this;
  Object.defineProperty(this, "attributes", { get: function () {
    return Object.keys(self._attrs).map(function (k) { return new Attr(k, self._attrs[k]); });
  }});
  Object.defineProperty(this, "src", { get: function () { return self._attrs.src || ""; } });
}
El.prototype.getAttribute = function (n) { return n in this._attrs ? this._attrs[n] : null; };
El.prototype.setAttribute = function (n, v) { this._attrs[n] = String(v); };
var frame = {
  scripts: [], classList: { add: function () {}, remove: function () {} },
  insertAdjacentHTML: function () {}, scrollTop: 0,
  set innerHTML(html) {
    var re = /<script([^>]*)>([\s\S]*?)<\/script>/g, m, out = [];
    while ((m = re.exec(html))) {
      var attrs = {}, am, are = /([\w-]+)(?:="([^"]*)")?/g;
      while ((am = are.exec(m[1]))) attrs[am[1]] = am[2] === undefined ? "" : am[2];
      var el = new El("script", attrs, m[2]);
      el.parentNode = frame;
      out.push(el);
    }
    this.scripts = out;
  },
  querySelectorAll: function () { return this.scripts.slice(); },
  replaceChild: function (n, o) { this.scripts[this.scripts.indexOf(o)] = n; n.parentNode = frame; },
};
var contentArea = { querySelector: function () { return null; }, classList: frame.classList };
var bar = { style: {} };
var document = {
  createElement: function (t) { return new El(t); },
  getElementById: function (id) { return id === "module-frame" ? frame : id === "loading-bar" ? bar : null; },
  querySelector: function () { return contentArea; },
  querySelectorAll: function () { return []; },
};
var location = {
  pathname: "/platform/plan/campaign/full-plan", hash: "",
  reload: function () { reloads++; },
};
var window = { location: location, addEventListener: function () {} };
var history = { pushState: function (s, t, u) { location.pathname = u; }, replaceState: function () {} };
function requestAnimationFrame(f) { f(); }
function setTimeout(f) { f(); }
var FRAGMENTS = {
  "media-plan": '<script type="application/json" id="novaWizardInputs">{"budget":{}}</script>' +
                '<script>const currentStep = 1;</script>' +
                '<script src="/static/sankey-viz.js?v=1.0.0" defer></script>',
  "quick-plan": '<script>const currentStep = 1;</script>',
};
function fetch(url) {
  fetches.push(url);
  var name = url.split("/").pop();
  return Promise.resolve({ ok: true, text: function () { return Promise.resolve(FRAGMENTS[name]); } });
}
var MODULES = [{
  id: "campaign", label: "Campaign", group: "plan", route: "plan/campaign",
  tabs: [{ id: "full-plan", label: "Full Plan", fragment: "media-plan" },
         { id: "quick-plan", label: "Quick Plan", fragment: "quick-plan" }],
}];
var noop = function () {};
var NovaContext = { setNav: noop, emit: noop }, NovaSidebar = { setActive: noop },
    NovaDrawer = { updateSuggestions: noop };
function _buildDashboardHome() { return ""; }
"""


def _run(body: str) -> dict:
    script = (
        _STUB
        + _router_js()
        + "\n(async function () {\n"
        + body
        + "\n})().catch(function (e) { console.log(JSON.stringify({error: String(e)})); });"
    )
    return run_node(script)


def test_data_scripts_keep_id_and_type_and_never_run_as_code():
    out = _run(
        """
        NovaRouter.init();
        await new Promise(function (r) { r(); });
        await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
        console.log(JSON.stringify({
          scripts: frame.scripts.map(function (s) {
            return { attrs: s._attrs, text: s.textContent};
          }),
        }));
        """
    )
    assert "error" not in out, out
    json_block = out["scripts"][0]
    # Still the data block, untouched: id + type survive, so
    # getElementById("novaWizardInputs") finds the budget rules.
    assert json_block["attrs"] == {"type": "application/json", "id": "novaWizardInputs"}
    assert json.loads(json_block["text"]) == {"budget": {}}
    inline, external = out["scripts"][1], out["scripts"][2]
    assert inline["text"] == "const currentStep = 1;"
    # External scripts keep src AND their other attributes (defer).
    assert external["attrs"] == {"src": "/static/sankey-viz.js?v=1.0.0", "defer": ""}


def test_data_script_is_not_recreated_as_a_classic_script():
    """The JSON block must be the ORIGINAL element (never replaced by a
    type-less <script> the browser would execute)."""
    out = _run(
        """
        var seen = [];
        var orig = frame.replaceChild;
        frame.replaceChild = function (n, o) { seen.push(o.getAttribute("type")); return orig.call(frame, n, o); };
        NovaRouter.init();
        await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
        console.log(JSON.stringify({ replacedTypes: seen }));
        """
    )
    assert "error" not in out, out
    assert "application/json" not in out["replacedTypes"]
    assert len(out["replacedTypes"]) == 2  # the inline + the external script


def test_revisiting_a_module_loads_a_fresh_document_instead_of_rerunning_scripts():
    out = _run(
        """
        NovaRouter.init();
        await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
        var afterFirst = { reloads: reloads, fetches: fetches.length };
        NovaRouter.switchTab("quick-plan");
        await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
        console.log(JSON.stringify({
          afterFirst: afterFirst,
          reloads: reloads,
          fetches: fetches.length,
          path: location.pathname,
        }));
        """
    )
    assert "error" not in out, out
    # The first module in a document is injected normally ...
    assert out["afterFirst"] == {"reloads": 0, "fetches": 1}
    # ... a second one would redeclare `const currentStep`, so the router
    # loads the (already pushed) URL as a fresh document instead.
    assert out["path"] == "/platform/plan/campaign/quick-plan"
    assert out["reloads"] == 1
    assert out["fetches"] == 1


def test_dashboard_home_needs_no_reload():
    """Home is built by platform.js itself (no fragment scripts), so leaving a
    module for home stays in-page."""
    out = _run(
        """
        NovaRouter.init();
        await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
        NovaRouter.navigate("");
        await Promise.resolve();
        console.log(JSON.stringify({ reloads: reloads }));
        """
    )
    assert "error" not in out, out
    assert out["reloads"] == 0


def test_router_slice_is_the_real_router():
    # Guard against the harness silently lifting the wrong block.
    js = _router_js()
    assert re.search(r"function _executeScripts\(container\)", js)
    assert "async function handleRoute()" in js
