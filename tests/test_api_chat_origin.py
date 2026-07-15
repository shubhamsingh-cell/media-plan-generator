"""Regression tests for the same-origin auth gate on POST /api/chat and
POST /api/chat/stream.

Bug fixed: like /api/estimate and /api/generate before commit 6a694a8, the
S48 gate on /api/chat and /api/chat/stream tested substring containment
(``"localhost" in (Origin or Referer)``), which is trivially bypassable --
``Referer: https://evil.com/?x=localhost`` and
``Origin: https://localhost.evil.com`` both CONTAIN an allowed string
without BEING an allowed host. 6a694a8 fixed /api/estimate and
/api/generate by introducing ``MediaPlanHandler._check_same_origin_auth``
(parsed-host equality against ``app._SAME_ORIGIN_ALLOWED_HOSTS``) but
deliberately left /api/chat and /api/chat/stream on the old pattern
(documented as out of scope in that commit's message). This file closes
that gap: both chat gates now call the SAME shared helper -- no new
helper, no edit to the existing one.

CRITICAL constraint verified here: /api/chat's auth is deliberately MORE
permissive than /api/generate's (S48 comment: "Chat auth is more
permissive... Allow: @joveo.com users, admin key, OR same-origin
requests"). Its ``or``-chain has two non-origin auth paths
(``_check_joveo_auth``, ``_check_admin_auth``) that must survive
untouched -- only the three origin/referer substring clauses are replaced
by the one helper call. Same shape, same constraint, for
/api/chat/stream.

This file covers:
    1. TestSameOriginGateUnit -- unit-level coverage of the gate logic
       itself (legit localhost passes; the evil.com/?x=localhost Referer
       trick and the localhost.evil.com host-suffix trick are both
       rejected; a flat evil.com Origin is rejected) PLUS source-inspection
       proof that both chat gates still wire in the two preserved
       non-origin auth paths and no longer contain the bypassable
       substring pattern.
    2. TestOriginGateLive -- real HTTP against an in-process
       ``app.ThreadedHTTPServer`` on an ephemeral port (always runs, no
       external server needed -- matches test_generate_concurrency.py's
       fixture pattern and test_api_estimate.py's TestOriginGateLive).
       Covers the same bypass vectors end-to-end against BOTH routes (401),
       legit origins passing the gate (fast 4xx past it, never 401), and a
       live functional proof that the admin-key non-origin auth path still
       admits a request even with a hostile Origin header.

       /api/chat/stream is SSE, which could in principle hang a naive
       ``resp.read()`` if a request ever reached the real streaming
       completion loop. Every live test here is deliberately built to
       short-circuit BEFORE that loop: rejected requests 401 immediately
       at the auth gate, and passing requests use an empty body so they
       400 ("Empty request body") right after the gate/rate-limit checks
       -- neither path ever opens the SSE stream, so a plain ``.read()``
       on a bounded, Content-Length'd JSON error response is safe (backed
       by the same 30s connection timeout test_api_estimate.py uses for
       its own /api/generate live checks).

Runs under pytest, or standalone: ``python3 tests/test_api_chat_origin.py``.
"""

from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import time
import types
from pathlib import Path
from typing import Iterator, Optional

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import app  # noqa: E402


# ---------------------------------------------------------------------------
# 1. Unit-level gate coverage + wiring/source-inspection.
# ---------------------------------------------------------------------------


def _gate_verdict(headers: dict[str, str]) -> bool:
    """Run _check_same_origin_auth against a fake handler with the given
    headers (dict.get matches http.client.HTTPMessage.get for our use).
    Same technique as test_api_estimate.py's _gate_verdict -- this is the
    ONE shared helper /api/chat and /api/chat/stream now call too."""
    fake_handler = types.SimpleNamespace(headers=headers)
    return app.MediaPlanHandler._check_same_origin_auth(fake_handler)


class TestSameOriginGateUnit:
    """Parsed-host equality, never substring containment -- and proof the
    two non-origin /api/chat auth paths were left untouched by the fix."""

    def test_legit_localhost_origin_passes(self) -> None:
        assert _gate_verdict({"Origin": "http://localhost:5001"}) is True

    def test_referer_query_string_trick_rejected(self) -> None:
        """The verified bypass: 'localhost' as a query-string VALUE. The
        old substring gate passed this; the parsed hostname is evil.com."""
        assert _gate_verdict({"Referer": "https://evil.com/?x=localhost"}) is False

    def test_evil_origin_rejected(self) -> None:
        """A flat disallowed Origin, no trick needed -- the floor case."""
        assert _gate_verdict({"Origin": "https://evil.com"}) is False

    def test_host_suffix_trick_rejected(self) -> None:
        """localhost as a subdomain label of an attacker's domain."""
        assert _gate_verdict({"Origin": "https://localhost.evil.com"}) is False

    def test_chat_permissiveness_comment_preserved(self, app_source: str) -> None:
        """The S48 comment documenting /api/chat's deliberately-more-
        permissive posture (vs /api/generate) must survive the origin-gate
        hardening verbatim -- this fix must not tighten or reinterpret
        that policy, only close the substring-bypass hole."""
        idx = app_source.index("# ── Nova Chat Endpoint ──")
        snippet = app_source[idx : idx + 400]
        assert "S48: Chat auth is more permissive than /api/generate." in snippet
        assert (
            "Allow: @joveo.com users, admin key, OR same-origin requests"
            in snippet
        )

    def test_chat_gates_preserve_non_origin_auth_paths(self, app_source: str) -> None:
        """CRITICAL constraint: both /api/chat's and /api/chat/stream's
        ``or``-chains must still call _check_joveo_auth() and
        _check_admin_auth() unchanged -- the fix may swap ONLY the
        localhost-substring origin/referer clauses for the shared helper.
        Anchored on each route's own comment (matches
        test_api_estimate.py's anchoring rationale -- '"/api/chat"' also
        appears in route tables and dispatch tuples elsewhere in app.py)."""
        for anchor, label in (
            ("# ── Nova Chat Endpoint ──", "/api/chat"),
            ("# ── Nova Chat SSE Streaming Endpoint ──", "/api/chat/stream"),
        ):
            idx = app_source.index(anchor)
            snippet = app_source[idx : idx + 700]
            assert "_check_joveo_auth" in snippet, (
                f"{label} gate must still call _check_joveo_auth()"
            )
            assert "_check_admin_auth" in snippet, (
                f"{label} gate must still call _check_admin_auth()"
            )
            assert "_check_same_origin_auth" in snippet, (
                f"{label} gate must call the shared parsed-host helper"
            )
            assert 'in (self.headers.get("Origin")' not in snippet, (
                f"{label} gate must not use the bypassable substring check"
            )

    def test_chat_or_chain_is_exactly_three_clauses(self, app_source: str) -> None:
        """Locks in the exact shape post-fix: joveo OR admin OR
        same-origin -- no clause added, none dropped, none duplicated."""
        for anchor, varname in (
            ("# ── Nova Chat Endpoint ──", "_chat_auth_ok"),
            ("# ── Nova Chat SSE Streaming Endpoint ──", "_stream_auth_ok"),
        ):
            idx = app_source.index(anchor)
            assign_idx = app_source.index(f"{varname} = (", idx)
            end_marker = "self._check_same_origin_auth()"
            end_idx = app_source.index(end_marker, assign_idx) + len(end_marker)
            end_idx = app_source.index(")", end_idx)
            clause = app_source[assign_idx : end_idx + 1]
            assert clause.count(" or ") == 2, (
                f"{varname} must be exactly a 3-way or-chain, got: {clause!r}"
            )
            assert "self._check_joveo_auth()" in clause
            assert "self._check_admin_auth()" in clause
            assert "self._check_same_origin_auth()" in clause


# ---------------------------------------------------------------------------
# 2. Live E2E over an in-process server (always runs -- no external server
#    needed, matches tests/test_generate_concurrency.py's fixture pattern
#    and test_api_estimate.py's TestOriginGateLive).
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def gate_server() -> Iterator[int]:
    """Real app.ThreadedHTTPServer on an ephemeral port (in-process daemon
    thread) -- same fixture pattern as test_api_estimate.py's gate_server /
    test_generate_concurrency.py's live_server."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    server = app.ThreadedHTTPServer(("127.0.0.1", port), app.MediaPlanHandler)
    thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="test-chat-origin-gate-server"
    )
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("gate_server did not start accepting connections in time")
    yield port
    server.shutdown()
    server.server_close()


def _post_to(
    port: int,
    path: str,
    body: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
) -> http.client.HTTPResponse:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    hdrs = dict(headers or {})
    if body and "Content-Type" not in hdrs:
        hdrs["Content-Type"] = "application/json"
    conn.request("POST", path, body=body, headers=hdrs)
    return conn.getresponse()


class TestOriginGateLive:
    """End-to-end over real HTTP: the bypass vectors get 401 on BOTH
    /api/chat and /api/chat/stream; legit origins get past the gate (never
    401); and the admin-key non-origin auth path still admits a request
    even carrying a hostile Origin -- proving the preserved ``or``-chain
    clauses are not just present in source but functionally live.

    Every request here uses an EMPTY body on purpose: past the gate, both
    routes fast-400 on ``content_len <= 0`` ("Empty request body") before
    ever reaching the real chat-completion / SSE-streaming code, so these
    tests can distinguish "gate admitted the request" (never 401) from
    "gate rejected it" (401) without waiting on an LLM call or an actual
    SSE stream.
    """

    # -- Bypass vectors: 401 on both routes --

    def test_chat_evil_referer_query_trick_401(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat",
            body=b"",
            headers={"Referer": "https://evil.com/?x=localhost"},
        )
        assert resp.status == 401
        resp.read()

    def test_chat_stream_evil_referer_query_trick_401(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat/stream",
            body=b"",
            headers={"Referer": "https://evil.com/?x=localhost"},
        )
        assert resp.status == 401
        resp.read()

    def test_chat_host_suffix_trick_401(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat",
            body=b"",
            headers={"Origin": "https://localhost.evil.com"},
        )
        assert resp.status == 401
        resp.read()

    def test_chat_stream_host_suffix_trick_401(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat/stream",
            body=b"",
            headers={"Origin": "https://localhost.evil.com"},
        )
        assert resp.status == 401
        resp.read()

    def test_chat_evil_origin_401(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat",
            body=b"",
            headers={"Origin": "https://evil.com"},
        )
        assert resp.status == 401
        resp.read()

    def test_chat_no_auth_at_all_401(self, gate_server: int) -> None:
        resp = _post_to(gate_server, "/api/chat", body=b"")
        assert resp.status == 401
        resp.read()

    # -- Legit origins pass the gate (never 401) --

    def test_chat_localhost_origin_passes_gate(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat",
            body=b"",
            headers={"Origin": f"http://localhost:{gate_server}"},
        )
        assert resp.status != 401
        assert resp.status == 400  # "Empty request body"
        resp.read()

    def test_chat_stream_localhost_origin_passes_gate(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat/stream",
            body=b"",
            headers={"Origin": f"http://localhost:{gate_server}"},
        )
        assert resp.status != 401
        assert resp.status == 400
        resp.read()

    def test_chat_nova_origin_passes_gate(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat",
            body=b"",
            headers={"Origin": "https://nova.joveo.com"},
        )
        assert resp.status != 401
        assert resp.status == 400
        resp.read()

    def test_chat_stream_nova_origin_passes_gate(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat/stream",
            body=b"",
            headers={"Origin": "https://nova.joveo.com"},
        )
        assert resp.status != 401
        assert resp.status == 400
        resp.read()

    def test_chat_onrender_origin_passes_gate(self, gate_server: int) -> None:
        resp = _post_to(
            gate_server,
            "/api/chat",
            body=b"",
            headers={"Origin": "https://media-plan-generator.onrender.com"},
        )
        assert resp.status != 401
        assert resp.status == 400
        resp.read()

    # -- Preserved non-origin auth path, proven functionally: admin key
    #    still admits the request even with a hostile Origin header. --

    def test_chat_admin_key_bypasses_hostile_origin(
        self, gate_server: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_check_admin_auth is one of the two non-origin auth paths in
        /api/chat's or-chain that the fix must leave untouched. Patch
        app.ADMIN_API_KEY (module global _check_admin_auth reads at call
        time) so a valid X-Admin-Key header authenticates the request
        despite an Origin that would otherwise 401 -- proving the
        preserved clause is not just present in source but live."""
        monkeypatch.setattr(app, "ADMIN_API_KEY", "test-only-admin-key-chat-origin")
        resp = _post_to(
            gate_server,
            "/api/chat",
            body=b"",
            headers={
                "Origin": "https://evil.com",
                "X-Admin-Key": "test-only-admin-key-chat-origin",
            },
        )
        assert resp.status != 401, (
            "admin-key auth path must admit the request even with a "
            "hostile Origin -- the fix must not have folded this "
            "non-origin path into the same-origin check"
        )
        assert resp.status == 400
        resp.read()

    def test_chat_stream_admin_key_bypasses_hostile_origin(
        self, gate_server: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(app, "ADMIN_API_KEY", "test-only-admin-key-chat-origin")
        resp = _post_to(
            gate_server,
            "/api/chat/stream",
            body=b"",
            headers={
                "Origin": "https://evil.com",
                "X-Admin-Key": "test-only-admin-key-chat-origin",
            },
        )
        assert resp.status != 401
        assert resp.status == 400
        resp.read()

    def test_chat_wrong_admin_key_still_401_with_hostile_origin(
        self, gate_server: int, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sanity check on the sanity check: an INCORRECT admin key must
        not accidentally satisfy hmac.compare_digest, and the hostile
        Origin must still 401 -- otherwise the test above would be
        vacuous (passing for reasons unrelated to the admin-key path)."""
        monkeypatch.setattr(app, "ADMIN_API_KEY", "test-only-admin-key-chat-origin")
        resp = _post_to(
            gate_server,
            "/api/chat",
            body=b"",
            headers={
                "Origin": "https://evil.com",
                "X-Admin-Key": "definitely-the-wrong-key",
            },
        )
        assert resp.status == 401
        resp.read()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
