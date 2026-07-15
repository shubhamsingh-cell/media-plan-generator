"""Integration-lite threading tests for the "one generation stalls the
whole server" fix.

Three independent things are verified:

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

3. The cap spans ASYNC (``X-Async: true``) generations too -- the primary
   web-UI path. The submit request returns a job_id in milliseconds, but
   slot ownership transfers to the background worker thread: the slot
   stays held while the job reports "processing" and is released exactly
   once when the pipeline reaches a terminal status (completed OR failed).
   A saturated server rejects async submits with the same immediate 429 +
   ``Retry-After`` and creates no job.

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


def _http_get(
    port: int, path: str, timeout: float = 5.0, headers: dict | None = None
) -> tuple[int, dict, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("GET", path, headers=headers or {})
        resp = conn.getresponse()
        body = resp.read()
        return resp.status, dict(resp.getheaders()), body
    finally:
        conn.close()


def _http_post_generate(
    port: int,
    timeout: float = 60.0,
    payload: dict | None = None,
    extra_headers: dict | None = None,
) -> tuple[int, dict, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        body = json.dumps(payload if payload is not None else _GEN_PAYLOAD).encode(
            "utf-8"
        )
        headers = dict(_AUTH_HEADERS)
        if extra_headers:
            headers.update(extra_headers)
        conn.request("POST", "/api/generate", body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        return resp.status, dict(resp.getheaders()), data
    finally:
        conn.close()


def _count_free_slots() -> int:
    """Non-destructively count currently-acquirable generate slots
    (acquire non-blocking up to the cap, release everything acquired)."""
    acquired = 0
    for _ in range(app_module._MAX_CONCURRENT_GENERATE):
        if app_module._generate_slots.acquire(blocking=False):
            acquired += 1
    for _ in range(acquired):
        app_module._generate_slots.release()
    return acquired


def _poll_job(port: int, job_id: str) -> dict:
    """Poll /api/jobs/{job_id} in JSON mode. The Accept header matters:
    without ``application/json`` a completed job returns the binary ZIP
    (and marks it downloaded) instead of a status document."""
    status, _headers, body = _http_get(
        port,
        f"/api/jobs/{job_id}",
        timeout=10.0,
        headers={"Accept": "application/json"},
    )
    assert status == 200, f"/api/jobs/{job_id} returned HTTP {status}"
    return json.loads(body)


def _wait_for_terminal_job(port: int, job_id: str, timeout: float) -> dict:
    """Poll a job until it reaches a terminal status (completed/failed)."""
    deadline = time.time() + timeout
    last: dict = {}
    while time.time() < deadline:
        last = _poll_job(port, job_id)
        if last.get("status") in ("completed", "failed"):
            return last
        time.sleep(0.5)
    pytest.fail(
        f"job {job_id} did not reach a terminal status within {timeout}s "
        f"(last poll: {last})"
    )


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


# ---------------------------------------------------------------------------
# 4. Async (X-Async: true) generations -- the cap must span the worker thread
# ---------------------------------------------------------------------------


def test_async_submit_rejected_when_slots_saturated(live_server: int) -> None:
    """With both slots held, an async submit must get the same immediate
    429 + Retry-After as a sync call -- rejected before any job is
    created, so the response carries no job_id to poll."""
    port = live_server
    # Barrier: a previous test's /api/generate releases its slot in
    # do_POST's finally AFTER the client saw the response -- wait for all
    # slots to be back before saturating them.
    assert _wait_for_slots_available(
        app_module._MAX_CONCURRENT_GENERATE, timeout=10.0
    ), "test setup: slots from a previous test's request not yet released"
    for _ in range(app_module._MAX_CONCURRENT_GENERATE):
        assert app_module._generate_slots.acquire(blocking=False), (
            "test setup: could not saturate the generate semaphore"
        )
    try:
        status, headers, body = _http_post_generate(
            port,
            timeout=10.0,
            payload=_gen_payload("async-reject"),
            extra_headers={"X-Async": "true"},
        )
        assert status == 429
        assert headers.get("Retry-After") == "5"
        payload: dict[str, Any] = json.loads(body)
        assert payload["code"] == "TOO_MANY_CONCURRENT_GENERATIONS"
        assert "job_id" not in payload, (
            "a saturated async submit must not create a job"
        )
    finally:
        for _ in range(app_module._MAX_CONCURRENT_GENERATE):
            app_module._generate_slots.release()


def test_async_generate_holds_slot_until_worker_finishes(live_server: int) -> None:
    """Regression pin for the async cap bypass: an X-Async submit returns
    a job_id in milliseconds, but its concurrency slot must stay owned by
    the background worker for the full pipeline -- not be released by
    do_POST's finally when the handler thread returns. Before the fix the
    slot came back within milliseconds of the 200 while the job was still
    processing, so real (web-UI) async traffic was effectively uncapped.
    """
    port = live_server
    # Barrier: wait out any trailing slot release from a previous test's
    # request so the free-slot arithmetic below starts from a full pool.
    assert _wait_for_slots_available(
        app_module._MAX_CONCURRENT_GENERATE, timeout=10.0
    ), "test setup: slots from a previous test's request not yet released"
    status, _headers, body = _http_post_generate(
        port,
        timeout=30.0,
        payload=_gen_payload("async-slot-hold"),
        extra_headers={"X-Async": "true"},
    )
    assert status == 200, f"async /api/generate submit failed with status {status}"
    submit: dict[str, Any] = json.loads(body)
    job_id = submit["job_id"]
    assert submit["status"] == "processing"

    observed_processing = False
    try:
        # Observation window: while the job reports "processing", at most
        # cap-1 slots may be acquirable (the worker owns one). Sample
        # repeatedly rather than once -- do_POST's finally runs *after*
        # the response bytes hit the socket, so a buggy handler-side
        # release lands a beat after the 200, not instantly.
        deadline = time.time() + 3.0
        while time.time() < deadline:
            if _poll_job(port, job_id).get("status") != "processing":
                break
            free = _count_free_slots()
            # Re-read status after the probe so a job that reached a
            # terminal state mid-probe can't be mistaken for a leak.
            if _poll_job(port, job_id).get("status") == "processing":
                observed_processing = True
                assert free <= app_module._MAX_CONCURRENT_GENERATE - 1, (
                    f"{free} generate slots free while async job {job_id} "
                    "is still processing -- the submit handler released "
                    "the worker's slot (async cap bypass)"
                )
            time.sleep(0.2)
        assert observed_processing, (
            "async job left 'processing' before the slot hold could be "
            "observed -- cannot validate the cap"
        )
    finally:
        # Drain to terminal even on assertion failure so the worker is not
        # still running (and, post-fix, holding a slot) in later tests.
        final = _wait_for_terminal_job(port, job_id, timeout=180.0)

    assert final.get("status") in ("completed", "failed")
    assert _wait_for_slots_available(
        app_module._MAX_CONCURRENT_GENERATE, timeout=10.0
    ), (
        "generate slots were not all released after the async job reached "
        f"terminal status {final.get('status')!r} -- the worker's "
        "finally-release is leaking"
    )


def test_async_generate_releases_slot_when_pipeline_fails(
    live_server: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure-path release: if the async pipeline blows up, the job must
    land in "failed" AND the worker's slot must still come back. Seam:
    ``load_knowledge_base`` is a module-level function the worker calls
    early and unconditionally, outside any inner try/except, so patching
    it to raise propagates to the closure's outer failure handler."""
    port = live_server
    # Barrier: wait out any trailing slot release from a previous test's
    # request so the all-slots-released check below is meaningful.
    assert _wait_for_slots_available(
        app_module._MAX_CONCURRENT_GENERATE, timeout=10.0
    ), "test setup: slots from a previous test's request not yet released"

    def _boom() -> dict:
        raise RuntimeError("injected knowledge-base failure (test seam)")

    monkeypatch.setattr(app_module, "load_knowledge_base", _boom)

    status, _headers, body = _http_post_generate(
        port,
        timeout=30.0,
        payload=_gen_payload("async-fail-release"),
        extra_headers={"X-Async": "true"},
    )
    assert status == 200, f"async /api/generate submit failed with status {status}"
    job_id = json.loads(body)["job_id"]

    final = _wait_for_terminal_job(port, job_id, timeout=120.0)
    assert final.get("status") == "failed"
    assert "injected knowledge-base failure" in (final.get("error") or "")

    assert _wait_for_slots_available(
        app_module._MAX_CONCURRENT_GENERATE, timeout=10.0
    ), (
        "generate slots were not all released after the async job failed "
        "-- the worker's finally-release must fire on the failure path too"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
