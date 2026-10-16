"""The platform shell must not ship a real person's identity as the default.

NovaContext.user defaulted to a hardcoded person's name, email and an
``admin`` role: every visitor was greeted by that name, and the identity was
persisted to localStorage and restored for 24h. These run the REAL NovaContext
block from static/js/platform.js under node against a stub DOM/localStorage.
"""

from __future__ import annotations

import json
import re

import pytest

from tests.wizard_js_harness import NODE, ROOT, js_block, run_node

PLATFORM_JS = ROOT / "static" / "js" / "platform.js"
PLATFORM_HTML = ROOT / "templates" / "platform.html"

pytestmark = pytest.mark.skipif(NODE is None, reason="node not installed")

_STUB = r"""
var store = {};
var localStorage = {
  getItem: function (k) { return k in store ? store[k] : null; },
  setItem: function (k, v) { store[k] = String(v); },
  removeItem: function (k) { delete store[k]; },
};
var window = { addEventListener: function () {} };
var document = { getElementById: function () { return null; } };
function _updateCampaignUI() {}
"""


def _context_js() -> str:
    src = PLATFORM_JS.read_text(encoding="utf-8")
    return js_block(src, "const NovaContext = (() =>") + ")();"


def _run(body: str):
    return run_node(_STUB + _context_js() + "\n" + body)


def test_default_user_is_neutral():
    out = _run(
        "console.log(JSON.stringify({user: NovaContext.user,"
        " greeting: (NovaContext.user.name || '')}));"
    )
    assert out["user"]["name"] == ""
    assert out["user"]["email"] == ""
    assert out["user"]["role"] != "admin"
    assert out["greeting"] == ""


def test_stale_stored_identity_is_not_restored_or_rewritten():
    out = _run(
        """
        store.nova_context_v2 = JSON.stringify({
          campaign: {name: "Q4 push"},
          user: {name: "Old Person", email: "old.person@example.com", role: "admin"},
          _ts: Date.now(),
        });
        NovaContext.init();
        NovaContext.setCampaign({budget: 5});
        // flush the debounced save the way pagehide does
        var u = NovaContext.user;
        console.log(JSON.stringify({user: u, campaign: NovaContext.campaign.name}));
        """
    )
    assert out["user"]["name"] == ""
    assert out["user"]["email"] == ""
    assert out["user"]["role"] == ""
    assert out["campaign"] == "Q4 push"


def test_persisted_payload_carries_no_identity():
    out = _run(
        """
        var timers = [];
        setTimeout = function (f) { timers.push(f); return 1; };
        NovaContext.user.name = "Typed Name";
        NovaContext.setCampaign({budget: 5});
        timers.forEach(function (f) { f(); });
        console.log(JSON.stringify(JSON.parse(store.nova_context_v2)));
        """
    )
    assert out["user"] == {"role": ""}


def test_source_literal_has_no_person_identity():
    """The shipped source literal itself (not only the evaluated object)."""
    src = PLATFORM_JS.read_text(encoding="utf-8")
    block = js_block(src, "    user: {")
    assert re.search(r'name:\s*""', block)
    assert re.search(r'email:\s*""', block)
    assert "@" not in block
