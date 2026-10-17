"""Behavioural tests for the hub hero card's driver (static/js/hub.js).

The "PREDICTION ENGINE" IIFE is run in node against a tiny DOM stub with the
card's real markup shape, a seeded ``#pe-default`` and a stubbed ``fetch``.
Found by the design-judge mechanism lens (2026-10-17): a scenario missing
from the loaded grid made ``show()`` re-enter itself through the resolved
grid promise forever (a frozen tab). These tests click such a scenario and
require the status line instead, and cover the happy path, the fetch-failure
path and the "grid fetched once" rule.

Runs under pytest, or standalone:
``python3 -m pytest tests/test_hub_prediction_engine_js.py -q``.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tests.wizard_js_harness import NODE, run_node  # noqa: E402

HUB_JS = PROJECT_ROOT / "static" / "js" / "hub.js"

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")


def _iife() -> str:
    js = HUB_JS.read_text(encoding="utf-8")
    start = js.index("PREDICTION ENGINE")
    start = js.index("(function () {", start)
    end = js.index("})();", start) + len("})();")
    return js[start:end]


LABELS = {
    "midpoint_basis": "an assumed {mid} all-in cost per hire (Nova table, unsourced)",
    "best_case": "best case",
}


def _estimate(
    hires: int, cph: float, apps: int, mid_cph: float, mid_hires: int
) -> dict:
    return {
        "ranges": {
            "applications": f"{apps // 4:,}\u2013{apps:,}",
            "cpa": "$9\u2013$58",
        },
        "est_hires": hires,
        "est_cph": cph,
        "est_applications": apps,
        "est_cpa": round(50000 / apps, 2),
        "est_hires_midpoint": mid_hires,
        "industry_midpoint_cph": mid_cph,
        "cph_floor": mid_cph / 2,
        "cph_floor_applied": True,
        "channels": [
            {"key": "programmatic_dsp", "pct": 60.0, "amount": 30000.0},
            {"key": "global_boards", "pct": 40.0, "amount": 20000.0},
        ],
    }


DEFAULT_KEY = "Registered Nurse|Houston, TX|$50,000"
SEED = {
    "brief": {
        "budget": "$50,000",
        "industry": "Healthcare & Medical",
        "roles": ["Registered Nurse"],
        "locations": ["Houston, TX"],
    },
    "estimate": _estimate(9, 5555.56, 3432, 10500.0, 4),
    "labels": LABELS,
}

# A tiny DOM: enough for the IIFE's querySelector(All) / classList / events.
_DOM = r"""
class El {
  constructor(attrs, cls) {
    this.attrs = attrs || {}; this.children = []; this.textContent = "";
    this.hidden = false; this.style = {}; this.dataset = {}; this.listeners = {};
    this._cls = new Set((cls || "").split(" ").filter(Boolean));
    const c = this._cls;
    this.classList = { contains: (x) => c.has(x), add: (x) => c.add(x), remove: (x) => c.delete(x),
      toggle: (x, on) => { if (on) c.add(x); else c.delete(x); } };
  }
  getAttribute(n) { return n in this.attrs ? this.attrs[n] : null; }
  setAttribute(n, v) { this.attrs[n] = String(v); }
  addEventListener(t, f) { (this.listeners[t] = this.listeners[t] || []).push(f); }
  click() { (this.listeners.click || []).forEach((f) => f({ type: "click" })); }
  dispatchEvent(ev) { (this.listeners[ev.type] || []).forEach((f) => f(ev)); }
  all() { return this.children.flatMap((ch) => [ch, ...ch.all()]); }
  matches(sel) {
    return sel.split(/(?=[.\[])/).every((tok) => {
      if (tok.startsWith("[")) {
        const m = /^\[([\w-]+)(?:="([^"]*)")?\]$/.exec(tok);
        return m && m[1] in this.attrs && (m[2] === undefined || this.attrs[m[1]] === m[2]);
      }
      if (tok.startsWith(".")) return this._cls.has(tok.slice(1));
      return true;
    });
  }
  querySelectorAll(sel) { const found = this.all().filter((e) => e.matches(sel)); found.forEach = Array.prototype.forEach; return found; }
  querySelector(sel) { return this.querySelectorAll(sel)[0] || null; }
}
function el(attrs, cls, text) { const e = new El(attrs, cls); if (text) e.textContent = text; return e; }
const card = el({ id: "plan-showcase", "data-pe-grid": "/api/hub/prediction-grid?v=abc123def456abc123de" }, "pe-card pe-card--hero");
const roles = [["Registered Nurse", "Healthcare & Medical"], ["Sales Rep", "Retail"], ["CDL Driver", "Logistics & Supply Chain"]];
roles.forEach(([t, ind], i) => card.children.push(el({ "data-role": String(i), "aria-pressed": i === 0 ? "true" : "false", "data-pe-title": t, "data-pe-industry": ind }, "pe-chip" + (i === 0 ? " is-on" : ""), t)));
["Houston, TX", "Chicago, IL"].forEach((l, i) => card.children.push(el({ "data-loc": String(i), "aria-pressed": i === 0 ? "true" : "false", "data-pe-location": l }, "pe-chip" + (i === 0 ? " is-on" : ""), l)));
const range = el({ "data-pe-range": "", min: "30000" }, "pe-range"); range.value = "50000"; card.children.push(range);
const slots = {};
["budget", "cph", "hires", "apps", "cpa", "basis", "status", "label-basis", "apps-label", "cpa-label"].forEach((k) => { slots[k] = el({ ["data-pe-" + k]: "" }, "pe-" + k); card.children.push(slots[k]); });
slots.status.hidden = true;
slots['apps-label'].textContent = 'Est. applications'; slots['cpa-label'].textContent = 'Cost per application';
["programmatic_dsp", "global_boards", "niche_boards"].forEach((k) => {
  const row = el({ "data-pe-ch": k }, "pe-bar-row");
  row.children.push(el({}, "pe-bar-fill")); row.children.push(el({ "data-pe-amt": "" }, "pe-bar-amt"));
  card.children.push(row);
});
const seedEl = el({ id: "pe-default" }, ""); seedEl.textContent = JSON.stringify(SEED);
const document = { getElementById: (id) => (id === "plan-showcase" ? card : id === "pe-default" ? seedEl : null) };
const window = { matchMedia: () => ({ matches: true }) };  // reduced motion: values land at once
let fetchCalls = 0;
const fetch = async (url) => { fetchCalls++; FETCH_URL = url; return FETCH_IMPL(); };
const requestAnimationFrame = (f) => { f(1e9); return 1; }; const cancelAnimationFrame = () => {};
const performance = { now: () => Date.now() };
let FETCH_URL = null;
"""


def _run(scenario_js: str, grid: dict | None, fetch_ok: bool = True) -> dict:
    if grid is not None:
        grid = {
            "version": "abc123def456abc123de",
            **grid,
        }  # the version the stub page links
    grid_json = json.dumps(grid) if grid is not None else "null"
    script = (
        "const SEED = "
        + json.dumps(SEED)
        + ";\n"
        + "const GRID = "
        + grid_json
        + ";\n"
        + (
            "const FETCH_IMPL = async () => ({ ok: true, json: async () => GRID });\n"
            if fetch_ok
            else "const FETCH_IMPL = async () => ({ ok: false, status: 503, json: async () => ({}) });\n"
        )
        + _DOM
        + "\n"
        + _iife()
        + "\n"
        + "(async () => {\n"
        + scenario_js
        + "\n  const out = { fetchCalls, fetchUrl: FETCH_URL, cph: slots.cph.textContent, hires: slots.hires.textContent,"
        " apps: slots.apps.textContent, cpa: slots.cpa.textContent, appsLabel: slots['apps-label'].textContent,"
        " cpaLabel: slots['cpa-label'].textContent, basis: slots.basis.textContent, status: slots.status.textContent,"
        " statusHidden: slots.status.hidden, computing: card.classList.contains('is-computing'),"
        " labelBasisHidden: slots['label-basis'].hidden,"
        " amounts: card.querySelectorAll('[data-pe-amt]').map((e) => e.textContent),"
        " hiddenRows: card.querySelectorAll('[data-pe-ch]').map((r) => r.hidden),"
        " rolePressed: card.querySelectorAll('[data-role]').map((e) => e.getAttribute('aria-pressed')),"
        " locPressed: card.querySelectorAll('[data-loc]').map((e) => e.getAttribute('aria-pressed')) };\n"
        "  process.stdout.write(JSON.stringify(out));\n"
        "})();\n"
    )
    return run_node(script)  # node's stdout is the JSON the scenario writes


def _settle() -> str:
    return (
        "  for (let i = 0; i < 20; i++) await new Promise((r) => setTimeout(r, 5));\n"
    )


def test_first_paint_is_the_seed_without_a_fetch() -> None:
    out = _run("", {"scenarios": {}})
    assert out["fetchCalls"] == 0
    assert (
        out["cph"] == "$5,556"
        and out["hires"] == "9"
        and out["apps"] == "858\u20133,432"
    )
    assert out["basis"].replace("\u00a0", " ") == (
        "9 whole hires at the engine\u2019s $5,250 floor \u00b7 4 at an assumed "
        "$10.5K all-in cost per hire (Nova table, unsourced)"
    )
    assert out["labelBasisHidden"] is False
    assert out["statusHidden"] is True
    assert out["amounts"][:2] == ["$30,000", "$20,000"] and out["hiddenRows"] == [
        False,
        False,
        True,
    ]


def test_a_chip_click_fetches_the_grid_once_and_paints_the_scenario() -> None:
    grid = {
        "scenarios": {
            "CDL Driver|Houston, TX|$50,000": _estimate(16, 3125.0, 7819, 6250.0, 8)
        }
    }
    js = (
        "  card.querySelector('[data-role=\"2\"]').click();\n"
        + _settle()
        + "  card.querySelector('[data-role=\"0\"]').click();\n"
        + _settle()  # back to the seed: no refetch
        + "  card.querySelector('[data-role=\"2\"]').click();\n"
        + _settle()
    )
    out = _run(js, grid)
    assert out["fetchCalls"] == 1
    assert (
        out["fetchUrl"] == "/api/hub/prediction-grid?v=abc123def456abc123de"
    )  # the hashed URL from the markup
    assert (
        out["cph"] == "$3,125"
        and out["hires"] == "16"
        and out["apps"] == "1,954\u20137,819"
    )
    assert out["basis"].replace("\u00a0", " ") == (
        "16 whole hires at the engine\u2019s $3,125 floor \u00b7 8 at an assumed "
        "$6.25K all-in cost per hire (Nova table, unsourced)"
    )
    assert out["statusHidden"] is True and out["computing"] is False


def test_a_scenario_missing_from_the_grid_shows_a_status_and_never_loops() -> None:
    """The mechanism lens's blocker: with the grid loaded but the key absent,
    show() used to call itself through the resolved promise forever."""
    grid = {
        "scenarios": {
            "CDL Driver|Houston, TX|$50,000": _estimate(16, 3125.0, 7819, 6250.0, 8)
        }
    }
    js = (
        "  card.querySelector('[data-role=\"2\"]').click();\n"
        + _settle()
        + "  card.querySelector('[data-loc=\"1\"]').click();\n"
        + _settle()  # CDL · Chicago: not in the grid
    )
    out = _run(js, grid)
    assert out["fetchCalls"] == 1  # never refetched
    assert out["statusHidden"] is False
    assert out["status"].startswith("No estimate for this combination.")
    assert (
        "CDL Driver · Houston, TX · $50,000" in out["status"]
    )  # the figures still shown
    assert out["cph"] == "$3,125" and out["computing"] is False


def test_a_failed_grid_fetch_keeps_the_figures_and_says_so() -> None:
    js = "  card.querySelector('[data-role=\"1\"]').click();\n" + _settle()
    out = _run(js, None, fetch_ok=False)
    assert out["fetchCalls"] == 1
    assert out["statusHidden"] is False
    assert out["status"].startswith("Couldn’t reach the engine.")
    assert "Registered Nurse · Houston, TX · $50,000" in out["status"]
    assert out["cph"] == "$5,556" and out["computing"] is False


def test_the_iife_under_test_is_the_one_in_hub_js() -> None:
    block = _iife()
    assert block.startswith("(function () {") and block.endswith("})();")
    assert re.search(r'card\.getAttribute\("data-pe-grid"\)', block)
    assert "gridLoaded" in block and "No estimate for this combination." in block


def test_a_grid_of_another_version_is_refused_and_the_figures_stay() -> None:
    """The page links the grid by content hash; a grid of another version (an
    old instance answering mid-deploy) must not replace this page's figures."""
    grid = {
        "version": "0ther",
        "scenarios": {
            "CDL Driver|Houston, TX|$50,000": _estimate(16, 3125.0, 7819, 6250.0, 8)
        },
    }
    js = "  card.querySelector('[data-role=\"2\"]').click();\n" + _settle()
    out = _run(js, grid)
    assert out["fetchCalls"] == 1
    assert out["statusHidden"] is False and out["status"].startswith(
        "Couldn\u2019t reach the engine."
    )
    assert out["cph"] == "$5,556" and out["computing"] is False  # the seed stays


def test_a_scenario_without_the_floor_case_drops_the_plan_efficiency_phrase() -> None:
    est = _estimate(16, 3125.0, 7819, 6250.0, 8)
    est["cph_floor_applied"] = False
    grid = {"scenarios": {"CDL Driver|Houston, TX|$50,000": est}}
    js = "  card.querySelector('[data-role=\"2\"]').click();\n" + _settle()
    out = _run(js, grid)
    assert out["cph"] == "$3,125" and out["basis"] == ""
    assert out["labelBasisHidden"] is True  # "at plan efficiency" is not claimed


def test_tiles_show_the_engine_range_and_a_missing_range_says_best_case() -> None:
    """F2: applications and cost per application show low-high, never only the
    best end; a scenario the engine gave no range keeps its figure and the tile
    label says so."""
    bare = _estimate(16, 3125.0, 7819, 6250.0, 8)
    del bare["ranges"]
    grid = {
        "scenarios": {
            "CDL Driver|Houston, TX|$50,000": _estimate(16, 3125.0, 7819, 6250.0, 8),
            "Sales Rep|Houston, TX|$50,000": bare,
        }
    }
    ranged = _run(
        "  card.querySelector('[data-role=\"2\"]').click();\n" + _settle(), grid
    )
    assert ranged["apps"] == "1,954\u20137,819" and ranged["cpa"] == "$9\u2013$58"
    assert ranged["appsLabel"] == "Est. applications"
    best = _run(
        "  card.querySelector('[data-role=\"1\"]').click();\n" + _settle(), grid
    )
    assert best["apps"] == "7,819" and best["cpa"] == "$6.39"
    assert best["appsLabel"] == "Est. applications, best case"
    assert best["cpaLabel"] == "Cost per application, best case"


def test_no_basis_line_without_the_labels_the_server_sends() -> None:
    """F1: the cost-basis wording comes from the server (seed / grid); with none
    the driver prints no basis line rather than wording it itself."""
    script_seed = dict(SEED)
    script_seed.pop("labels")
    est = _estimate(16, 3125.0, 7819, 6250.0, 8)
    grid = {"scenarios": {"CDL Driver|Houston, TX|$50,000": est}}
    import tests.test_hub_prediction_engine_js as me

    original = me.SEED
    try:
        me.SEED = script_seed
        out = me._run(
            "  card.querySelector('[data-role=\"2\"]').click();\n" + _settle(), grid
        )
    finally:
        me.SEED = original
    assert out["basis"] == "" and out["labelBasisHidden"] is True


def test_m3_chips_keep_aria_pressed_in_sync_with_the_selection() -> None:
    """The selected chip was shown by the ``is-on`` class only; assistive tech
    got no state. Clicking a chip moves aria-pressed with it."""
    grid = {
        "scenarios": {
            "CDL Driver|Houston, TX|$50,000": _estimate(16, 3125.0, 7819, 6250.0, 8),
            "CDL Driver|Chicago, IL|$50,000": _estimate(16, 3125.0, 7000, 6250.0, 8),
        }
    }
    start = _run("", grid)
    assert start["rolePressed"] == ["true", "false", "false"]
    assert start["locPressed"] == ["true", "false"]
    js = (
        "  card.querySelector('[data-role=\"2\"]').click();\n"
        "  card.querySelector('[data-loc=\"1\"]').click();\n" + _settle()
    )
    out = _run(js, grid)
    assert out["rolePressed"] == ["false", "false", "true"]
    assert out["locPressed"] == ["false", "true"]
