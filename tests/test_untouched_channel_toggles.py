"""Untouched channel toggles mean "recommended industry mix" on every surface.

The Step 3 markup pre-checks only Social (body_content.html). Once
_sanitize_request_value kept JSON booleans, an untouched wizard sent
channel_categories = {social_media: True, everything else: False}, so
/api/generate, /api/estimate and the deck selector all funded
{social_media: 100.0} while the live preview (channelsTouched false)
showed "Recommended mix".

Rule: untouched -> channel_categories is omitted from both payloads and the
server uses the recommended industry mix; touched -> the user's exact
selection is honoured everywhere (Brendan's case: Employer Branding off).

The payload code is lifted from the real templates and run under node.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import app  # noqa: E402
import ppt_generator  # noqa: E402

_ROOT = Path(__file__).resolve().parent.parent
_APP_JS = _ROOT / "templates" / "partials" / "index" / "body_app_js.html"
_PREVIEW_JS = _ROOT / "templates" / "partials" / "index" / "body_preview_js.html"
_NODE = shutil.which("node")
needs_node = pytest.mark.skipif(_NODE is None, reason="node not installed")

_TOGGLES = [
    "chRegionalBoards", "chGlobalBoards", "chNicheBoards", "chSocial",
    "chProgrammatic", "chEmployerBrand", "chApac", "chEmea",
]

_BRIEF = {
    "budget": "$90,000",
    "industry": "food_beverage",
    "client_name": "The Hershey Company",
    "roles": ["Machine Operator", "Electrical Technician", "Production Supervisor"],
    "locations": ["Hershey, PA, US"],
}


def _slice(src: str, start: str, end: str) -> str:
    i = src.index(start)
    return src[i : src.index(end, i)]


def _wizard_payloads(touch: dict | None) -> dict:
    """Run the wizard's real channel code under node.

    touch: None for an untouched wizard, else {toggle_id: checked} applied
    as user changes (each fires a bubbling 'change' event).
    Returns the channel_categories each payload would carry after
    JSON.stringify ("<absent>" when the key is dropped).
    """
    app_src = _APP_JS.read_text()
    prev_src = _PREVIEW_JS.read_text()
    parts = []
    marker = "// ── Channel selection (one rule for generate, estimate and preview)"
    if marker in app_src:
        parts.append(_slice(app_src, marker, "// ── end channel selection ──"))
    gen_prop = _slice(
        app_src[app_src.index("// 28. Updated payload with new fields") :],
        "channel_categories:",
        "include_educational:",
    )
    parts.append(_slice(prev_src, "// True once the user toggles any channel", "// ── Channel model"))
    parts.append(_slice(prev_src, "function estimateChannelCategories", "function estimatePayload"))
    script = "\n".join(
        [
            "var state = " + json.dumps({t: t == "chSocial" for t in _TOGGLES}) + ";",
            "var listeners = [];",
            "var document = {",
            "  getElementById: function (id) { return id in state ? {",
            "    get checked() { return state[id]; }, id: id } : null; },",
            "  addEventListener: function (t, fn) { if (t === 'change') listeners.push(fn); },",
            "};",
            "function $(id) { return document.getElementById(id); }",
            "function channelOn(id) { var el = $(id); return !!(el && el.checked); }",
            *parts,
            "var touch = " + json.dumps(touch) + ";",
            "if (touch) Object.keys(touch).forEach(function (id) {",
            "  state[id] = touch[id];",
            "  listeners.forEach(function (fn) { fn({ target: { id: id } }); });",
            "  if (typeof channelsTouched === 'boolean') channelsTouched = true;",
            "});",
            "var gen = JSON.parse(JSON.stringify({" + gen_prop + "}));",
            "var est = JSON.parse(JSON.stringify({ channel_categories: estimateChannelCategories() }));",
            "function out(p) { return 'channel_categories' in p ? p.channel_categories : '<absent>'; }",
            "process.stdout.write(JSON.stringify({ generate: out(gen), estimate: out(est) }));",
        ]
    )
    res = subprocess.run(
        [_NODE, "-e", script], capture_output=True, text=True, timeout=30, check=True
    )
    return json.loads(res.stdout)


def _brief_with(cats) -> dict:
    brief = dict(_BRIEF)
    if cats != "<absent>":
        brief["channel_categories"] = cats
    return brief


@pytest.fixture
def estimate_pcts(monkeypatch):
    seen: dict = {}
    real = app.calculate_budget_allocation

    def _spy(*args, **kwargs):
        seen["pcts"] = dict(kwargs.get("channel_percentages") or {})
        return real(*args, **kwargs)

    monkeypatch.setattr(app, "calculate_budget_allocation", _spy)

    def run(brief: dict) -> dict:
        app._compute_plan_estimate(dict(brief))
        return seen["pcts"]

    return run


@needs_node
def test_untouched_wizard_sends_no_channel_selection():
    got = _wizard_payloads(None)
    assert got == {"generate": "<absent>", "estimate": "<absent>"}


@needs_node
def test_untouched_wizard_funds_recommended_mix_everywhere(estimate_pcts):
    got = _wizard_payloads(None)
    recommended = estimate_pcts(dict(_BRIEF))
    # /api/estimate and /api/generate share _apply_channel_selection.
    for surface in ("generate", "estimate"):
        brief = _brief_with(got[surface])
        pcts = estimate_pcts(brief)
        assert pcts == recommended, surface
        assert app._apply_channel_selection(recommended, brief) == recommended
        assert pcts != {"social_media": 100.0}
        assert len(pcts) >= 5
    deck = ppt_generator._selected_channels(_brief_with(got["generate"]))
    assert len(deck) >= 5
    assert set(deck) != {"social_media"}


@needs_node
def test_touched_selection_is_honoured_brendan_employer_branding_off(estimate_pcts):
    touch = {
        "chProgrammatic": True,
        "chGlobalBoards": True,
        "chNicheBoards": True,
        "chRegionalBoards": True,
        "chSocial": True,
        "chEmployerBrand": False,
    }
    got = _wizard_payloads(touch)
    assert got["generate"] == got["estimate"]
    cats = got["generate"]
    assert cats["employer_branding"] is False
    assert cats["programmatic_dsp"] is True and cats["social_media"] is True
    pcts = estimate_pcts(_brief_with(cats))
    assert (pcts.get("employer_branding") or 0) == 0
    assert abs(sum(pcts.values()) - 100) < 1e-6
    deck = ppt_generator._selected_channels(_brief_with(cats))
    assert "employer_branding" not in deck
    assert {"programmatic_dsp", "global_boards", "social_media"} <= set(deck)


@needs_node
def test_touching_only_social_off_then_on_is_an_explicit_social_only_choice():
    got = _wizard_payloads({"chSocial": True})
    assert got["generate"]["social_media"] is True
    assert got["generate"]["employer_branding"] is False
