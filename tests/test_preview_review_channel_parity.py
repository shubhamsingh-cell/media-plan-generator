"""Live preview and review step list the channels the plan will fund.

Hershey plan, 2026-09-25 (client: "the tool shows me X but the output
says Y"): with untouched toggles the live preview showed a fixed 4-channel
STARTER mix (Programmatic, Global, Social, Niche) and the review step --
which read each toggle's ``checked`` -- listed only "Social Media Channels",
while /api/estimate, /api/generate and the deck all funded the recommended
food_beverage mix (6 channels incl. Regional and Employer Branding).

Rule: preview rows and review chips come from the channels /api/estimate
funded (``channels`` in its response). Untouched toggles, and touched but
every channel unchecked, both mean the recommended mix -- the same fallback
app._apply_channel_selection applies when every channel is off.

The template JS is lifted from the real files and run under node.
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

_KEY_TO_ID = {
    "programmatic_dsp": "chProgrammatic",
    "global_boards": "chGlobalBoards",
    "niche_boards": "chNicheBoards",
    "social_media": "chSocial",
    "regional_boards": "chRegionalBoards",
    "employer_branding": "chEmployerBrand",
    "apac_regional": "chApac",
    "emea_regional": "chEmea",
}
_TOGGLES = list(_KEY_TO_ID.values())

_BRIEF = {
    "budget": "90000",
    "industry": "food_beverage",
    "client_name": "The Hershey Company",
    "roles": ["Machine Operator", "Production Worker", "Sanitation Worker"],
    "locations": ["Hershey, PA, US"],
}
_BRENDAN = {  # touched: everything on except Employer Branding
    "chProgrammatic": True,
    "chGlobalBoards": True,
    "chNicheBoards": True,
    "chRegionalBoards": True,
    "chSocial": True,
    "chEmployerBrand": False,
}
_ALL_OFF = {t: False for t in _TOGGLES}


def _slice(src: str, start: str, end: str) -> str:
    i = src.index(start)
    return src[i : src.index(end, i)]


def _selection(touch: dict | None) -> dict | None:
    if touch is None:
        return None
    state = {t: t == "chSocial" for t in _TOGGLES}
    state.update(touch)
    return {k: state[i] for k, i in _KEY_TO_ID.items()}


def _estimate(touch: dict | None) -> dict:
    brief = dict(_BRIEF)
    cats = _selection(touch)
    if cats is not None:
        brief["channel_categories"] = cats
    return app._compute_plan_estimate(brief)


def _run_wizard(touch: dict | None, estimate: dict | None) -> dict:
    """Run the real preview gather() + review list under node.

    touch: None (untouched) or {toggle_id: checked} applied as user changes.
    estimate: the /api/estimate JSON the preview received (None = pending).
    Returns {"preview": [toggle ids of the mix rows], "recommended": bool,
    "review": {...} | None}.
    """
    app_src = _APP_JS.read_text()
    prev_src = _PREVIEW_JS.read_text()
    parts = [
        _slice(
            app_src,
            "// ── Channel selection (one rule for generate, estimate and preview)",
            "// ── end channel selection ──",
        ),
        _slice(prev_src, "// True once the user toggles any channel", "// ── Money formatters"),
        _slice(prev_src, "function gather()", "// ── Live plan estimate"),
    ]
    if "function setFunded(" in prev_src:
        parts.append(_slice(prev_src, "function setFunded(", "function resolveIndustry()"))
    state = {t: t == "chSocial" for t in _TOGGLES}
    values = {"exactBudget": _BRIEF["budget"], "budgetPeriod": "campaign"}
    script = "\n".join(
        [
            "var window = {};",
            "var state = " + json.dumps(state) + ";",
            "var values = " + json.dumps(values) + ";",
            "var listeners = [];",
            "var document = {",
            "  getElementById: function (id) {",
            "    if (id in state) return { id: id, get checked() { return state[id]; } };",
            "    if (id in values) return { id: id, value: values[id] };",
            "    return null;",
            "  },",
            "  querySelector: function () { return null; },",
            "  querySelectorAll: function () { return []; },",
            "  addEventListener: function (t, fn) { if (t === 'change') listeners.push(fn); },",
            "};",
            *parts,
            "var touch = " + json.dumps(touch) + ";",
            "if (touch) Object.keys(touch).forEach(function (id) {",
            "  state[id] = touch[id];",
            "  listeners.forEach(function (fn) { fn({ target: { id: id } }); });",
            "});",
            "var est = " + json.dumps(estimate) + ";",
            "if (est && typeof setFunded === 'function') setFunded(est, 'current');",
            "var m = gather();",
            "var review = typeof novaReviewChannelList === 'function' ? (function () {",
            "  window.NovaPreview = { fundedChannels: function () {",
            "    return typeof fundedChannels !== 'undefined' && fundedChannels",
            "      ? fundedChannels.map(function (f) { return f.id; }) : null; } };",
            "  return novaReviewChannelList(); })() : null;",
            "process.stdout.write(JSON.stringify({",
            "  preview: m.rows.map(function (r) { return r.id; }),",
            "  shares: m.rows.map(function (r) { return r.share; }),",
            "  recommended: !!(m.recommended || m.usingStarter),",
            "  review: review,",
            "}));",
        ]
    )
    res = subprocess.run(
        [_NODE, "-e", script], capture_output=True, text=True, timeout=30, check=True
    )
    return json.loads(res.stdout)


def _funded_ids(estimate: dict) -> list[str]:
    return [_KEY_TO_ID[c["key"]] for c in estimate["channels"]]


_LABELS = {
    "chRegionalBoards": "Regional & Local Job Boards",
    "chGlobalBoards": "Global Reach Job Boards",
    "chNicheBoards": "Niche & Industry-Specific Boards",
    "chSocial": "Social Media Channels",
    "chProgrammatic": "Programmatic & DSP Channels",
    "chEmployerBrand": "Employer Branding Platforms",
    "chApac": "APAC Regional Channels",
    "chEmea": "EMEA Regional Channels",
}


# ── server: /api/estimate reports the channels it funded ──────────────


def test_estimate_reports_funded_channels_recommended_mix():
    est = _estimate(None)
    keys = [c["key"] for c in est["channels"]]
    assert set(keys) == {
        "programmatic_dsp",
        "global_boards",
        "niche_boards",
        "social_media",
        "regional_boards",
        "employer_branding",
    }
    assert abs(sum(c["amount"] for c in est["channels"]) - 90_000) < 1
    amounts = [c["amount"] for c in est["channels"]]
    assert amounts == sorted(amounts, reverse=True)


def test_estimate_channels_match_the_deck_selection():
    for touch in (None, _BRENDAN, _ALL_OFF):
        brief = dict(_BRIEF)
        cats = _selection(touch)
        if cats is not None:
            brief["channel_categories"] = cats
        est_keys = {c["key"] for c in app._compute_plan_estimate(dict(brief))["channels"]}
        assert est_keys == set(ppt_generator._selected_channels(dict(brief))), touch


def test_all_unchecked_is_the_recommended_mix_server_side():
    assert {c["key"] for c in _estimate(_ALL_OFF)["channels"]} == {
        c["key"] for c in _estimate(None)["channels"]
    }


def test_brendan_selection_drops_employer_branding():
    keys = {c["key"] for c in _estimate(_BRENDAN)["channels"]}
    assert "employer_branding" not in keys
    assert {"programmatic_dsp", "regional_boards", "social_media"} <= keys


# ── client: preview rows and review chips == what the estimate funded ──


@needs_node
@pytest.mark.parametrize(
    "touch,recommended",
    [(None, True), (_ALL_OFF, True), (_BRENDAN, False)],
    ids=["untouched", "touched-all-unchecked", "brendan-employer-branding-off"],
)
def test_preview_and_review_list_the_funded_channels(touch, recommended):
    est = _estimate(touch)
    got = _run_wizard(touch, est)
    funded = _funded_ids(est)
    assert got["preview"] == funded
    total = sum(c["amount"] for c in est["channels"])
    for share, c in zip(got["shares"], est["channels"]):
        assert abs(share - c["amount"] / total) < 1e-9
    assert got["recommended"] is recommended
    assert got["review"] is not None, "review step has no funded-channel list"
    assert got["review"]["recommended"] is recommended
    assert got["review"]["labels"] == [_LABELS[i] for i in funded]


@needs_node
def test_untouched_preview_never_shows_the_old_starter_mix_while_pending():
    got = _run_wizard(None, None)
    assert got["preview"] == []
    assert got["recommended"] is True
    assert got["review"]["labels"] == []
    assert got["review"]["recommended"] is True


@needs_node
def test_touched_selection_pending_shows_the_selected_channels():
    got = _run_wizard(_BRENDAN, None)
    assert set(got["preview"]) == {i for i, on in _BRENDAN.items() if on}
    assert set(got["review"]["labels"]) == {
        _LABELS[i] for i, on in _BRENDAN.items() if on
    }
