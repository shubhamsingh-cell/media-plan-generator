"""Integration-lite threading tests for the "one generation stalls the
whole server" fix.

Two independent things are verified:

1. The stdlib server is already a ThreadingHTTPServer (one thread per
   request, daemon threads) -- confirmed by source inspection, since that's
   a class-level property, not something worth spinning up a real server to
   check.

2. The synchronous /api/generate path is capped at ``_MAX_CONCURRENT_GENERATE``
   concurrent requests via a module-level ``threading.BoundedSemaphore``:
   the 3rd+ concurrent caller gets an immediate HTTP 429 with a
   ``Retry-After`` header instead of queueing, and /api/health -- which
   never touches this semaphore -- stays fast regardless of how many
   generate slots are held.

These run against a REAL ``app.ThreadedHTTPServer`` instance bound to an
ephemeral port in a background daemon thread (per the task's "integration-
lite" allowance), plus one real end-to-end /api/generate call to prove the
acquire/release wiring in do_POST's ``finally`` block actually fires for a
live request (not just when the semaphore is manipulated directly in-
process). Concurrency-saturation scenarios acquire the real module-level
semaphore directly instead of running two real ~5-15s generation pipelines
-- this is the "slow handler stub" the task allows, applied to the actual
bottleneck primitive rather than a fake handler.
"""

from __future__ import annotations

import http.client
import json
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, Iterator

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import app as app_module  # noqa: E402

_GEN_PAYLOAD = {
    "client_name": "Concurrency Test Co",
    "requester_name": "QA Bot",
    "requester_email": "qa@joveo.com",
    "target_roles": ["Warehouse Associate"],
    "locations": ["Columbus, OH"],
    "budget": 20000,
    "duration_weeks": 4,
}

# Headers that satisfy the /api/generate same-origin auth check
# (self._check_joveo_auth() OR ... OR "localhost" in Origin/Referer).
_AUTH_HEADERS = {"Content-Type": "application/json", "Origin": "http://localhost"}


def _gen_payload(tag: str) -> dict:
    """Distinct payload per real-generate test.

    app.py has a separate, pre-existing 60s request-deduplication guard
    (_is_duplicate_request, keyed on a hash of the whole payload) that is
    unrelated to the concurrency semaphore under test here -- two real
    /api/generate calls with an identical body within that window get a
    429 from *that* guard, which would be mistaken for a semaphore
    rejection. Varying client_name sidesteps it.
    """
    payload = dict(_GEN_PAYLOAD)
    payload["client_name"] = f"{_GEN_PAYLOAD['client_name']} ({tag})"
    return payload


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_server() -> Iterator[int]:
    """Start the real ThreadedHTTPServer on an ephemeral port."""
    port = _free_port()
    server = app_module.ThreadedHTTPServer(("127.0.0.1", port), app_module.MediaPlanHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True, name="test-http-server")
    thread.start()
    # Wait for the socket to actually accept connections.
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("live_server did not start accepting connections in time")
    # Warm up: the first-ever /api/health call pays a one-time cost
    # self-healing optional module imports (nova.py etc, unrelated to this
    # fix) that later calls don't pay. Absorb that here so timed
    # assertions below measure steady-state latency, not cold-start.
    _http_get(port, "/api/health")
    yield port
    server.shutdown()
    server.server_close()


def _wait_for_slots_available(count: int, timeout: float = 5.0) -> bool:
    """Poll for ``count`` generate slots to become acquirable, releasing
    them again once confirmed. Real requests do trailing post-response
    work (metrics, Slack notify, Supabase writes) in do_POST's `finally`
    *after* the client has already read the HTTP response, so slot release
    is not synchronous with the client seeing a 200 -- callers must poll,
    not check once immediately after the response."""
    deadline = time.time() + timeout
    acquired = 0
    try:
        while acquired < count and time.time() < deadline:
            if app_module._generate_slots.acquire(blocking=False):
                acquired += 1
            else:
                time.sleep(0.05)
        return acquired == count
    finally:
        for _ in range(acquired):
            app_module._generate_slots.release()


def _http_get(port: int, path: str, timeout: float = 5.0) -> tuple[int, dict, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", path)
        resp = conn.getresponse()
        body = resp.read()
        return resp.status, dict(resp.getheaders()), body
    finally:
        conn.close()


def _http_post_generate(
    port: int, timeout: float = 60.0, payload: dict | None = None
) -> tuple[int, dict, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        body = json.dumps(payload if payload is not None else _GEN_PAYLOAD).encode(
            "utf-8"
        )
        conn.request("POST", "/api/generate", body=body, headers=_AUTH_HEADERS)
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, dict(resp.getheaders()), data
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 1. Serving model
# ---------------------------------------------------------------------------


def test_server_is_threading_http_server_with_daemon_threads() -> None:
    """app.py's stdlib dev server must handle each request on its own
    thread (not block serially), and those threads must not prevent
    process shutdown."""
    import socketserver

    assert issubclass(app_module.ThreadedHTTPServer, socketserver.ThreadingMixIn)
    assert app_module.ThreadedHTTPServer.daemon_threads is True


# ---------------------------------------------------------------------------
# 2. Concurrency cap on /api/generate
# ---------------------------------------------------------------------------


def test_generate_slots_default_capacity_is_two() -> None:
    assert app_module._MAX_CONCURRENT_GENERATE == 2


def test_health_stays_fast_while_generate_slots_saturated(live_server: int) -> None:
    """Simulate 2 concurrent synchronous /api/generate calls in flight by
    holding the real semaphore directly (the "slow handler stub"), then
    confirm /api/health -- which never touches this semaphore -- is
    unaffected."""
    port = live_server
    for _ in range(app_module._MAX_CONCURRENT_GENERATE):
        assert app_module._generate_slots.acquire(blocking=False), (
            "test setup: could not saturate the generate semaphore"
        )
    try:
        t0 = time.time()
        status, _headers, _body = _http_get(port, "/api/health")
        elapsed = time.time() - t0
        assert status == 200
        assert elapsed < 1.0, (
            f"/api/health took {elapsed:.2f}s while generate slots were "
            "saturated -- health must stay trivial and non-blocking"
        )
    finally:
        for _ in range(app_module._MAX_CONCURRENT_GENERATE):
            app_module._generate_slots.release()


def test_third_concurrent_generate_returns_429_with_retry_after(live_server: int) -> None:
    """With both slots held, a new /api/generate call must be rejected
    immediately (429 + Retry-After), never queued and never allowed to
    pile onto the box."""
    port = live_server
    for _ in range(app_module._MAX_CONCURRENT_GENERATE):
        assert app_module._generate_slots.acquire(blocking=False)
    try:
        t0 = time.time()
        status, headers, body = _http_post_generate(port, timeout=10.0)
        elapsed = time.time() - t0

        assert status == 429
        assert elapsed < 2.0, (
            f"overflow /api/generate took {elapsed:.2f}s -- must reject "
            "immediately, not queue"
        )
        assert headers.get("Retry-After") == "5"

        payload: dict[str, Any] = json.loads(body)
        assert payload["success"] is False
        assert payload["code"] == "TOO_MANY_CONCURRENT_GENERATIONS"
        assert "busy" in payload["error"].lower()
    finally:
        for _ in range(app_module._MAX_CONCURRENT_GENERATE):
            app_module._generate_slots.release()


def test_generate_slot_available_when_not_saturated(live_server: int) -> None:
    """Sanity check the inverse: with 0 slots held, acquiring one directly
    must succeed (i.e. the semaphore isn't permanently exhausted by a
    leak from an earlier test)."""
    assert app_module._generate_slots.acquire(blocking=False)
    app_module._generate_slots.release()


# ---------------------------------------------------------------------------
# 3. Real end-to-end request: acquire/release wiring in do_POST's `finally`
# ---------------------------------------------------------------------------


def test_real_generate_request_releases_its_slot_on_completion(live_server: int) -> None:
    """Fire one real (not simulated) /api/generate call and confirm all
    _MAX_CONCURRENT_GENERATE slots are acquirable again shortly after it
    completes -- proving do_POST's `finally` release fires for a live
    request, not just when the semaphore is poked directly.

    Polls rather than checking once: the client sees the HTTP response as
    soon as the bytes hit the socket, but the server thread still runs
    trailing post-response work (metrics, Slack notify, Supabase writes)
    inside _handle_POST *before* do_POST's `finally` (and thus the slot
    release) executes -- so release is not synchronous with the response.
    """
    port = live_server
    status, _headers, _body = _http_post_generate(
        port, timeout=90.0, payload=_gen_payload("slot-release")
    )
    assert status == 200, f"real /api/generate call failed with status {status}"

    assert _wait_for_slots_available(app_module._MAX_CONCURRENT_GENERATE, timeout=10.0), (
        "generate slots were not released within 10s of the request "
        "completing -- do_POST's finally-release is leaking"
    )


def test_health_stays_fast_during_a_real_concurrent_generate(live_server: int) -> None:
    """The literal symptom from the task: while a real synchronous
    /api/generate runs, /api/health must respond quickly. Runs the real
    generation pipeline in a background thread (not the semaphore stub) to
    prove the ThreadingHTTPServer + circuit-breaker-mesh RLock fix combine
    correctly for genuine concurrent traffic."""
    port = live_server
    gen_result: dict[str, Any] = {}

    def _run_generate() -> None:
        status, _headers, _body = _http_post_generate(
            port, timeout=90.0, payload=_gen_payload("health-concurrent")
        )
        gen_result["status"] = status

    gen_thread = threading.Thread(target=_run_generate, daemon=True)
    gen_thread.start()
    time.sleep(0.3)  # let the generate request be admitted and start running

    try:
        health_latencies = []
        for _ in range(3):
            t0 = time.time()
            status, _headers, _body = _http_get(port, "/api/health")
            health_latencies.append(time.time() - t0)
            assert status == 200
            time.sleep(0.2)
    finally:
        gen_thread.join(timeout=90.0)

    assert not gen_thread.is_alive(), "background /api/generate never completed"
    assert gen_result.get("status") == 200
    assert max(health_latencies) < 2.0, (
        f"/api/health latencies during a real concurrent generation: "
        f"{health_latencies} -- expected all well under 1-2s"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
