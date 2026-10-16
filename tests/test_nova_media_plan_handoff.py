"""A plan-shaped Nova answer offers "Build the full media plan".

Nova answers "create a media plan" with an overview; the wizard at
/media-plan builds the real deliverable and already prefills from a query
string (roles, locations, budget, hires, timeline, industry, client -- the
same URL Quick Brief builds). The server now emits that URL as
``media_plan_handoff`` on plan-shaped answers and the three chat surfaces
render it as a link.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pytest

import nova
import nova_handoff
import nova_memory
from tests.wizard_js_harness import NODE, ROOT, js_block, run_node

PLAN_TABLE = (
    "### Media Plan\n\n| Channel | Budget | % | Est. CPA | Projected Hires | Why |\n"
    "|---|---|---|---|---|---|\n| Programmatic | $40,000 | 40% | $12 | 20 | -- |\n"
) + ("Details. " * 20)


def _params(url: str) -> dict:
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query, keep_blank_values=True).items()}


# ---------------------------------------------------------------------------
# Server: brief parsing + URL
# ---------------------------------------------------------------------------
def test_brief_parses_every_field_and_encodes_lists_with_semicolons():
    msg = (
        "I need a $1.5M media plan to hire 120 warehouse workers in Dallas, TX "
        "and Memphis, TN over a 6-month campaign for healthcare, client is Acme Health"
    )
    brief = nova_handoff.extract_brief(msg)
    assert brief == {
        "roles": ["Warehouse Worker"],
        "locations": ["Dallas, TX", "Memphis, TN"],
        "budget": 1_500_000,
        "hires": 120,
        "timeline": "6 months",
        "industry": "healthcare_medical",
        "client": "Acme Health",
    }
    url = nova_handoff.build_handoff_url(brief)
    assert url.startswith("/media-plan?")
    # URL-encoded (no raw spaces / semicolons / commas in values) ...
    assert " " not in url and ";" not in url
    # ... and decodes to the wizard's "; "-joined lists
    decoded = _params(url)
    assert decoded["locations"] == "Dallas, TX; Memphis, TN"
    assert decoded["roles"] == "Warehouse Worker"
    assert decoded["budget"] == "1500000" and decoded["hires"] == "120"
    assert decoded["timeline"] == "6 months"
    assert decoded["client"] == "Acme Health"


def test_only_params_that_parsed_are_emitted():
    brief = nova_handoff.extract_brief("Create a media plan for 50 software engineers in Austin")
    assert set(brief) == {"roles", "locations", "hires"}
    url = nova_handoff.build_handoff_url(brief)
    assert set(_params(url)) == {"roles", "locations", "hires"}
    # nothing parsed -> bare wizard URL, no empty params
    assert nova_handoff.build_handoff_url(nova_handoff.extract_brief("plan something")) == "/media-plan"


def test_media_word_in_media_plan_is_not_mistaken_for_the_media_industry():
    brief = nova_handoff.extract_brief("Give me a media plan for 10 nurses in Boston")
    assert "industry" not in brief


def test_industry_value_is_a_real_wizard_card_key():
    body = (ROOT / "templates" / "partials" / "index" / "body_content.html").read_text(
        encoding="utf-8"
    )
    keys = set(nova_handoff._INDUSTRY_TO_WIZARD_KEY.values())
    for key in keys:
        assert f'data-industry="{key}"' in body, key


def test_earlier_turns_and_context_fill_what_the_last_message_lacks():
    history = [
        {"role": "user", "content": "We are hiring 30 registered nurses in Boston"},
        {"role": "assistant", "content": "Sure."},
    ]
    brief = nova_handoff.extract_brief(
        "what budget would you suggest?",
        history,
        {"campaign_budget": 90000, "campaign_industry": "Healthcare", "campaign_client": "Acme Health"},
    )
    assert brief["roles"] == ["Registered Nurse"]
    assert brief["locations"] == ["Boston"]
    assert brief["hires"] == 30
    assert brief["budget"] == 90000
    assert brief["industry"] == "healthcare_medical"
    assert brief["client"] == "Acme Health"


def test_budget_tool_inputs_are_a_fallback_for_budget_hires_industry():
    tool = {"query_budget_projection": '{"total_budget": 60000, "target_hires": 12, "industry": "retail"}'}
    brief = nova_handoff.extract_brief("plan it", None, None, tool)
    assert brief["budget"] == 60000 and brief["hires"] == 12
    assert brief["industry"] == "retail_consumer"


def test_plan_shape_detection():
    assert nova_handoff.is_plan_shaped("media_plan", "anything")
    assert nova_handoff.is_plan_shaped("general", PLAN_TABLE)  # allocation table alone
    assert not nova_handoff.is_plan_shaped("salary", "The median salary is $120,000.")
    assert not nova_handoff.is_plan_shaped("general", "| Metric | Value |\n|---|---|\n| Median | $1 |")


def test_handoff_absent_for_non_plan_error_and_empty_answers():
    ok = {"response": "The median salary is $120,000. " * 5}
    assert nova_handoff.build_media_plan_handoff("salary for a data scientist", [], None, ok, "salary") is None
    err = {"response": PLAN_TABLE, "error": "internal_error"}
    assert nova_handoff.build_media_plan_handoff("media plan", [], None, err, "media_plan") is None
    assert nova_handoff.build_media_plan_handoff("media plan", [], None, {"response": " "}, "media_plan") is None
    plan = {"response": PLAN_TABLE}
    h = nova_handoff.build_media_plan_handoff("media plan for 5 nurses in Boston", [], None, plan, "media_plan")
    assert h["label"] == "Build the full media plan" and h["url"].startswith("/media-plan?")


# ---------------------------------------------------------------------------
# Server: the real request handler + stream
# ---------------------------------------------------------------------------
class _StubIQ:
    def __init__(self, response):
        self._response = response

    def chat(self, user_message, **kwargs):
        return {"response": self._response, "sources": [], "confidence": 0.9, "tools_used": []}


def _handle(message, response, **extra):
    patches = [
        patch.object(nova, "_get_iq", lambda: _StubIQ(response)),
        patch.object(nova_memory.NovaMemory, "load", lambda self: None),
        patch.object(nova_memory.NovaMemory, "_persist_entry", lambda s, e: None),
        patch.object(nova_memory, "_conversation_memories", OrderedDict()),
    ]
    for p in patches:
        p.start()
    try:
        return nova.handle_chat_request({"message": message, "conversation_id": "conv-hhhhhhhhhhhhhhhh", **extra})
    finally:
        for p in reversed(patches):
            p.stop()


def test_handle_chat_request_attaches_handoff_for_a_plan_answer():
    result = _handle("Create a media plan for 50 software engineers in Austin", PLAN_TABLE)
    handoff = result["media_plan_handoff"]
    assert handoff["label"] == "Build the full media plan"
    assert _params(handoff["url"]) == {
        "roles": "Software Engineer",
        "locations": "Austin",
        "hires": "50",
    }
    assert handoff["params"]["hires"] == 50


def test_handle_chat_request_has_no_handoff_for_a_non_plan_answer():
    result = _handle(
        "What salary should I offer for a Data Scientist in NYC?",
        "The median Data Scientist salary in New York is about $140,000 per year. " * 3,
    )
    assert "media_plan_handoff" not in result


def test_stream_done_chunk_carries_the_handoff():
    plan_result = {
        "response": PLAN_TABLE,
        "sources": [],
        "confidence": 0.9,
        "tools_used": [],
        "media_plan_handoff": {
            "url": "/media-plan?roles=Nurse",
            "label": "Build the full media plan",
            "params": {"roles": ["Nurse"]},
        },
    }
    with patch.object(nova, "handle_chat_request", lambda *a, **k: dict(plan_result)):
        chunks = list(nova.handle_chat_request_stream({"message": "media plan for nurses"}))
    done = [c for c in chunks if c.get("done")][-1]
    assert done["media_plan_handoff"]["url"] == "/media-plan?roles=Nurse"


def test_sse_and_ws_final_events_forward_the_handoff():
    src = (ROOT / "app.py").read_text(encoding="utf-8")
    assert src.count('"media_plan_handoff": _sse_media_plan_handoff') == 1
    assert src.count('"media_plan_handoff": _ws_media_plan_handoff') == 1
    assert src.count('chunk.get("media_plan_handoff")') == 2


# ---------------------------------------------------------------------------
# Client rendering (real shipped functions under node)
# ---------------------------------------------------------------------------
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")

_DOM = r"""
function El(tag) { this.tagName = tag; this.children = []; this.attrs = {}; this.className = "";
  this.textContent = ""; this.listeners = {}; }
El.prototype.appendChild = function (c) { this.children.push(c); return c; };
El.prototype.setAttribute = function (k, v) { this.attrs[k] = v; };
El.prototype.addEventListener = function (t, f) { this.listeners[t] = f; };
var document = { createElement: function (t) { return new El(t); } };
var window = { location: { pathname: "/x" } };
var GOOD = {url: "/media-plan?roles=Nurse&locations=Austin%3B%20Dallas", label: "Build the full media plan"};
var BAD = [null, {}, {url: "https://evil.example/media-plan"}, {url: "//evil.example"},
  {url: "javascript:alert(1)"}, {url: "/media-planner?x=1"}, {url: "/media-plan?a=\"><script>"},
  {url: 42}];
"""


@needs_node
def test_widget_renders_only_safe_handoff_links():
    src = (ROOT / "static" / "nova-chat.js").read_text(encoding="utf-8")
    out = run_node(
        _DOM
        + "var HANDOFF_HREF_RE = "
        + re.search(r"var HANDOFF_HREF_RE = (/.*/);", src).group(1)
        + ";\n"
        + js_block(src, "function sanitizeHandoff(")
        + js_block(src, "function addHandoffToElement(")
        + r"""
var host = new El("div");
var link = addHandoffToElement(host, GOOD);
var rejected = BAD.map(function (b) { var h = new El("div"); return addHandoffToElement(h, b) === null && h.children.length === 0; });
console.log(JSON.stringify({href: link.href, text: link.textContent, target: link.target,
  rel: link.rel, aria: link.attrs["aria-label"], cls: link.className, kids: host.children.length,
  rejected: rejected}));
"""
    )
    assert out["href"] == "/media-plan?roles=Nurse&locations=Austin%3B%20Dallas"
    assert out["text"].startswith("Build the full media plan")
    assert out["target"] == "_blank" and out["rel"] == "noopener"
    assert "media plan" in out["aria"] and out["cls"] == "nova-handoff-btn"
    assert out["kids"] == 1
    assert all(out["rejected"])


@needs_node
def test_nova_page_renders_only_safe_handoff_links():
    src = (ROOT / "static" / "js" / "nova.js").read_text(encoding="utf-8")
    out = run_node(
        _DOM
        + "var HANDOFF_HREF_RE = "
        + re.search(r"var HANDOFF_HREF_RE = (/.*/);", src).group(1)
        + ";\n"
        + js_block(src, "function buildHandoffLink(")
        + r"""
var link = buildHandoffLink(GOOD);
console.log(JSON.stringify({href: link.href, cls: link.className, rel: link.rel,
  rejected: BAD.map(function (b) { return buildHandoffLink(b) === null; })}));
"""
    )
    assert out["href"].startswith("/media-plan?roles=Nurse") and out["cls"] == "handoff-btn"
    assert out["rel"] == "noopener" and all(out["rejected"])


@needs_node
def test_drawer_renders_only_safe_handoff_links():
    src = (ROOT / "static" / "js" / "platform.js").read_text(encoding="utf-8")
    out = run_node(
        _DOM
        + "function _escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')"
        ".replace(/>/g,'&gt;').replace(/\"/g,'&quot;'); }\n"
        + "const HANDOFF_HREF_RE = "
        + re.search(r"const HANDOFF_HREF_RE = (/.*/);", src).group(1)
        + ";\n"
        + js_block(src, "function _handoffHtml(")
        + r"""
console.log(JSON.stringify({good: _handoffHtml(GOOD),
  bad: BAD.map(function (b) { return _handoffHtml(b); })}));
"""
    )
    assert 'href="/media-plan?roles=Nurse&amp;locations=Austin%3B%20Dallas"' in out["good"]
    assert 'rel="noopener"' in out["good"] and "Build the full media plan" in out["good"]
    assert out["bad"] == [""] * len(out["bad"])


def test_every_surface_persists_and_forwards_the_handoff_and_drawer_sends_client():
    widget = (ROOT / "static" / "nova-chat.js").read_text(encoding="utf-8")
    assert widget.count("media_plan_handoff") >= 8  # ws + sse + 2 fallbacks + render
    assert "addHandoffToElement(msgEl, msg.media_plan_handoff)" in widget
    nova_js = (ROOT / "static" / "js" / "nova.js").read_text(encoding="utf-8")
    assert "media_plan_handoff: metadata.media_plan_handoff || null" in nova_js
    plat = (ROOT / "static" / "js" / "platform.js").read_text(encoding="utf-8")
    assert "handoff: data.media_plan_handoff || null" in plat
    assert plat.count('campaign_client: campaign.client || ""') == 2
