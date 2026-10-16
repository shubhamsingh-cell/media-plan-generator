"""Nova must never show its internal ``<!-- t3-* -->`` bookkeeping markers.

The T3 post-processors tag response text with HTML-comment markers so a second
pass short-circuits (``<!-- t3-numver -->`` from the number verifier,
``<!-- t3-citations -->`` from the citations block). They crossed the engine
boundary and every chat client HTML-escapes model text, so users saw the literal
characters ``<!-- t3-numver -->`` at the top of a reply.

Contract pinned here:
  * ``Nova.chat`` (the one place text leaves the engine: web sync + SSE via
    ``handle_chat_request``, and the Slack bot calling it directly) returns text
    with no marker;
  * inside the engine the markers still work: verification still runs, the
    unverified count is still reported, and a second pass does not duplicate the
    Sources block;
  * the /nova page renderer drops any residual HTML comment instead of printing it.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
import types
from pathlib import Path

import pytest

for _name in (
    "anthropic",
    "openai",
    "supabase",
    "redis",
    "qdrant_client",
    "sentence_transformers",
    "posthog",
):
    if _name not in sys.modules:
        sys.modules[_name] = types.ModuleType(_name)

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import nova  # noqa: E402

NODE = shutil.which("node")

# Two numbers in the "model output": 14.0% is in no tool result and not on the
# verifier's whitelist, so the verifier must flag it.
CANNED = "Nurse CPA in London is around 145 GBP and the apply rate is 14.0% on Indeed UK."
MESSAGE = "tell me about nurse recruiting costs in london"


def _canned_result() -> dict:
    return {
        "response": CANNED,
        "sources": ["BLS"],
        "confidence": 0.8,
        "tools_used": [],
    }


@pytest.fixture
def engine(monkeypatch):
    """A real ``Nova`` whose pipeline is cut to: canned model output -> real T3."""
    monkeypatch.setenv("NOVA_NUMBER_VERIFIER_ENABLED", "1")
    monkeypatch.setenv("NOVA_CITATIONS_ENABLED", "1")
    iq = nova.Nova.__new__(nova.Nova)

    monkeypatch.setattr(nova, "_check_learned_answers", lambda *_a, **_k: None)
    monkeypatch.setattr(nova, "_intelligent_cache_available", False)
    monkeypatch.setattr(nova, "_get_response_cache", lambda *_a, **_k: None)
    for name in (
        "_try_quick_answer",
        "_clarification_check",
        "_fast_path_benchmark_lookup",
        "_fast_path_supply_listing",
    ):
        monkeypatch.setattr(nova.Nova, name, lambda *_a, **_k: None)
    # The "LLM": hands back the canned answer; Nova.chat then runs the REAL
    # _apply_t3_postprocessing on it (nova.py: ``return self._apply_t3_...``).
    monkeypatch.setattr(
        nova.Nova, "_try_direct_tool_dispatch", lambda *_a, **_k: _canned_result()
    )
    return iq


# --------------------------------------------------------------------------
# Server: the markers are consumed at the engine boundary
# --------------------------------------------------------------------------


class TestEngineBoundary:
    def test_chat_reply_has_no_html_comment(self, engine):
        result = engine.chat(MESSAGE)
        assert "<!--" not in result["response"], result["response"]
        assert "t3-numver" not in result["response"]
        assert "t3-citations" not in result["response"]
        assert result["response"].startswith("Nurse CPA in London")

    def test_verification_and_citations_still_ran(self, engine):
        """Stripping the marker must not turn the verifier or citations off."""
        result = engine.chat(MESSAGE)
        assert result["unverified_numbers"] >= 1  # 14.0% was flagged
        assert result["response"].count("**Sources**") == 1
        assert "U.S. BLS" in result["response"]
        # the blank-line gap the citations marker owned goes with it
        assert "\n\n\n" not in result["response"]

    def test_handle_chat_request_reply_is_clean(self, engine, monkeypatch):
        monkeypatch.setattr(nova, "_get_iq", lambda: engine)
        out = nova.handle_chat_request({"message": MESSAGE})
        assert "<!--" not in out["response"], out["response"]
        assert out["unverified_numbers"] >= 1

    def test_cache_hit_text_is_clean_too(self, engine, monkeypatch):
        """A cached reply that still carries a marker is scrubbed on the way out."""
        stale = {
            "response": nova._T3_NUMVER_MARKER + CANNED,
            "sources": [],
            "confidence": 0.9,
            "tools_used": [],
        }
        monkeypatch.setattr(nova, "_check_learned_answers", lambda *_a, **_k: stale)
        out = engine.chat(MESSAGE)
        assert "<!--" not in out["response"]
        # the cached object is not rewritten behind the cache's back
        assert stale["response"].startswith(nova._T3_NUMVER_MARKER)


class TestMarkersStillDoTheirJob:
    """Inside the engine the markers keep the T3 passes idempotent."""

    def test_second_pass_does_not_duplicate_sources_or_tags(self, engine):
        once = engine._apply_t3_postprocessing(_canned_result(), MESSAGE)
        assert nova._T3_NUMVER_MARKER in once["response"]
        assert nova._T3_CITATIONS_MARKER in once["response"]
        snapshot = once["response"]
        twice = engine._apply_t3_postprocessing(once, MESSAGE)
        assert twice["response"] == snapshot
        assert twice["response"].count("**Sources**") == 1

    def test_chat_does_not_mutate_the_engine_side_result(self, monkeypatch):
        """The wrapper returns a copy: a cache holding the original keeps its
        markers, so a later T3 pass over it still short-circuits."""
        held = {"response": nova._T3_NUMVER_MARKER + CANNED}

        class Stub:
            @nova._consume_internal_markers
            def chat(self, *_a, **_k):
                return held

        out = Stub().chat("x")
        assert "<!--" not in out["response"]
        assert out is not held
        assert held["response"].startswith(nova._T3_NUMVER_MARKER)


class TestStripInternalMarkers:
    def test_numver_prefix(self):
        assert nova.strip_internal_markers("<!-- t3-numver -->Hello") == "Hello"

    def test_citations_marker_in_the_middle(self):
        text = "body\n\n<!-- t3-citations -->\n\n---\n**Sources**\n- [1] X"
        assert nova.strip_internal_markers(text) == "body\n\n---\n**Sources**\n- [1] X"

    def test_every_t3_marker_constant_is_covered(self):
        """A future ``_T3_*_MARKER`` that the stripper misses fails here."""
        names = [n for n in dir(nova) if re.fullmatch(r"_T3_\w*MARKER", n)]
        assert {"_T3_NUMVER_MARKER", "_T3_CITATIONS_MARKER"} <= set(names)
        for name in names:
            marker = getattr(nova, name)
            assert nova.strip_internal_markers(f"a{marker}b") == "ab", name

    def test_idempotent_and_case_insensitive(self):
        once = nova.strip_internal_markers("<!-- T3-NumVer -->x<!--t3-citations-->")
        assert once == "x"
        assert nova.strip_internal_markers(once) == once

    def test_non_marker_text_untouched(self):
        for text in ("plain", "", "<!-- keep me -->", "5 < 6 --> 4"):
            assert nova.strip_internal_markers(text) == text
        assert nova.strip_internal_markers(None) is None


# --------------------------------------------------------------------------
# Client: the /nova page renderer drops residual HTML comments
# --------------------------------------------------------------------------

# The /nova page renderer. The floating widget (static/nova-chat.js) has the same
# escape-first renderMarkdown, but a change to it must bump ?v= in static/js/hub.js
# and, because hub.js is itself ?v=-guarded, in templates/hub.html (see
# test_static_asset_cache_versions.py); it is covered by the server-side strip.
RENDERERS = {
    "nova_page": REPO_ROOT / "static" / "js" / "nova.js",
}


def _lift(src: str, name: str) -> str:
    start = src.index(f"  function {name}(")
    return src[start : src.index("\n  }\n", start) + 4]


def _render(path: Path, text: str) -> str:
    """Run the renderer's real ``renderMarkdown`` under a stub DOM."""
    src = path.read_text(encoding="utf-8")
    program = (
        "var document = { createElement: function () { var t = '';"
        " return { appendChild: function (n) { t = n.v; },"
        " get innerHTML() { return t.replace(/&/g,'&amp;').replace(/</g,'&lt;')"
        ".replace(/>/g,'&gt;'); } }; },"
        " createTextNode: function (v) { return { v: v }; } };\n"
        "function highlightSyntax(c) { return c; }\n"
        "function escapeHtml(s) { if (s == null) return ''; var d = document"
        ".createElement('div'); d.appendChild(document.createTextNode(String(s)));"
        " return d.innerHTML; }\n"
        + (_lift(src, "stripHtmlComments") if "function stripHtmlComments(" in src else "")
        + _lift(src, "renderMarkdown")
        + f"\nprocess.stdout.write(renderMarkdown({json.dumps(text)}));"
    )
    proc = subprocess.run(
        [NODE, "-e", program], capture_output=True, text=True, timeout=30
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


@pytest.mark.skipif(NODE is None, reason="node not installed")
@pytest.mark.parametrize("surface", sorted(RENDERERS))
class TestRendererDropsResidualComments:
    def test_marker_not_printed(self, surface):
        html = _render(
            RENDERERS[surface],
            "<!-- t3-numver -->Nurse CPA is 145 GBP.\n\n<!-- t3-citations -->\n\n"
            "---\n**Sources**",
        )
        assert "t3-" not in html and "&lt;!--" not in html and "<!--" not in html
        assert "Nurse CPA is 145 GBP." in html
        assert "<strong>Sources</strong>" in html

    def test_comment_inside_code_is_kept(self, surface):
        html = _render(
            RENDERERS[surface],
            "Use:\n```html\n<!-- keep -->\n<p>x</p>\n```\nand `<!-- inline -->`.",
        )
        assert html.count("&lt;!-- keep --&gt;") == 1
        assert "&lt;!-- inline --&gt;" in html

    def test_plain_text_unchanged(self, surface):
        assert "<p>Hello world</p>" == _render(RENDERERS[surface], "Hello world")
