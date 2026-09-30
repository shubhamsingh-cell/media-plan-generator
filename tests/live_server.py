"""In-process ThreadedHTTPServer on an ephemeral port, for request-level
tests of /api/estimate and /api/generate validation (same pattern as
tests/test_generate_concurrency.py). Only fast paths (validation 400s,
estimates) should go through it -- a request that passes validation runs a
full plan generation."""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from typing import Any, Iterator

import pytest

import app as app_module

AUTH_HEADERS = {"Content-Type": "application/json", "Origin": "http://localhost"}


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def live_port() -> Iterator[int]:
    port = _free_port()
    server = app_module.ThreadedHTTPServer(
        ("127.0.0.1", port), app_module.MediaPlanHandler
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    deadline = time.time() + 5.0
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.05)
    else:
        pytest.fail("live server did not start")
    yield port
    server.shutdown()
    server.server_close()


def post_json(port: int, path: str, payload: Any, timeout: float = 60.0) -> tuple:
    """(status, parsed JSON body or raw bytes). Clears the per-IP rate-limit
    buckets first (the handler's 30/min store and the 10/min generate / 60/min
    estimate limiters) so a long test session never turns a validation check
    into a 429."""
    app_module._rate_limit_store.clear()
    for limiter in (app_module._rl_generate, app_module._rl_estimate):
        with limiter._lock:
            limiter._requests.clear()
    body = json.dumps(payload).encode("utf-8")
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request("POST", path, body=body, headers=AUTH_HEADERS)
        resp = conn.getresponse()
        raw = resp.read()
    finally:
        conn.close()
    try:
        return resp.status, json.loads(raw)
    except ValueError:
        return resp.status, raw
