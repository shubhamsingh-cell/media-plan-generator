"""Unit tests for ``langfuse_integration``.

These tests deliberately avoid hitting the network. We monkey-patch
``urllib.request.urlopen`` so we can assert what would have been sent
without any real HTTP traffic.
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import threading
import time
import urllib.error
from typing import Any, Dict, List, Optional, Tuple

import pytest

# Force a clean import each time the module is imported, so env-var
# changes per-test are honoured by ``_get_config``.
import langfuse_integration  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────


def _set_env(monkeypatch: pytest.MonkeyPatch, **kwargs: Optional[str]) -> None:
    """Set / unset LANGFUSE_* env vars and reset the cached config."""
    keys = (
        "LANGFUSE_HOST",
        "LANGFUSE_PUBLIC_KEY",
        "LANGFUSE_SECRET_KEY",
        "LANGFUSE_SAMPLE_RATE",
        "LANGFUSE_TIMEOUT_S",
        "LANGFUSE_ENABLED",
    )
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    for key, value in kwargs.items():
        if value is not None:
            monkeypatch.setenv(key, value)
    langfuse_integration.reload_config()


def _wait_for_threads(
    name_prefix: str = "langfuse-trace", timeout_s: float = 3.0
) -> None:
    """Block until all daemon threads matching ``name_prefix`` finish."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        live = [
            t
            for t in threading.enumerate()
            if t.name.startswith(name_prefix) and t.is_alive()
        ]
        if not live:
            return
        time.sleep(0.02)


class _FakeResponse:
    """Minimal stand-in for ``http.client.HTTPResponse`` returned by urlopen."""

    def __init__(
        self, status: int = 207, body: bytes = b'{"successes":[],"errors":[]}'
    ) -> None:
        self.status = status
        self._body = body

    def read(self, n: int = -1) -> bytes:
        return self._body if n == -1 else self._body[:n]

    def getcode(self) -> int:
        return self.status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *exc: Any) -> None:  # noqa: ANN401
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Test #1 -- silent no-op when env vars are missing
# ──────────────────────────────────────────────────────────────────────────────


def test_trace_silent_noop_when_env_missing(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """No env vars -> trace_llm_call returns None and never spawns a thread."""
    _set_env(monkeypatch)  # all unset
    assert langfuse_integration.is_enabled() is False

    # Track whether any thread was started.
    started: List[threading.Thread] = []
    original_start = threading.Thread.start

    def _spy_start(self: threading.Thread) -> None:
        if self.name.startswith("langfuse-trace"):
            started.append(self)
        original_start(self)

    monkeypatch.setattr(threading.Thread, "start", _spy_start)

    # Also spy urlopen to make sure we never reach it.
    calls: List[Any] = []

    def _spy_urlopen(*_a: Any, **_kw: Any) -> _FakeResponse:
        calls.append((_a, _kw))
        return _FakeResponse()

    monkeypatch.setattr(langfuse_integration.urllib.request, "urlopen", _spy_urlopen)

    with caplog.at_level(logging.WARNING):
        result = langfuse_integration.trace_llm_call(
            model="test-model",
            input_messages=[{"role": "user", "content": "hi"}],
            output="ok",
            latency_ms=12.3,
        )

    assert result is None
    assert started == []
    assert calls == []
    # No warnings -- a missing config is a silent no-op, not an error.
    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings == []


# ──────────────────────────────────────────────────────────────────────────────
# Test #2 -- network failure must not raise
# ──────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raised",
    [
        urllib.error.URLError("connection refused"),
        urllib.error.HTTPError(
            url="http://localhost:3000/api/public/ingestion",
            code=500,
            msg="boom",
            hdrs=None,  # type: ignore[arg-type]
            fp=io.BytesIO(b"server exploded"),
        ),
        TimeoutError("read timed out"),
        OSError("socket reset"),
    ],
    ids=["URLError", "HTTPError", "TimeoutError", "OSError"],
)
def test_trace_swallows_network_errors(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    raised: BaseException,
) -> None:
    """Network errors are logged at WARNING but never propagated."""
    _set_env(
        monkeypatch,
        LANGFUSE_HOST="http://localhost:3000",
        LANGFUSE_PUBLIC_KEY="pk-lf-test",
        LANGFUSE_SECRET_KEY="sk-lf-test",
        LANGFUSE_SAMPLE_RATE="1.0",
    )
    assert langfuse_integration.is_enabled() is True

    def _explode(*_a: Any, **_kw: Any) -> _FakeResponse:
        raise raised

    monkeypatch.setattr(langfuse_integration.urllib.request, "urlopen", _explode)

    with caplog.at_level(logging.WARNING):
        # Must not raise.
        langfuse_integration.trace_llm_call(
            model="claude-haiku-4-5",
            input_messages=[{"role": "user", "content": "hello"}],
            output="hi",
            latency_ms=42.0,
            metadata={"provider": "claude"},
        )
        _wait_for_threads()

    # We expect a WARNING with "Langfuse:" prefix.
    msgs = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any(m.startswith("Langfuse:") for m in msgs), msgs


# ──────────────────────────────────────────────────────────────────────────────
# Test #3 -- LANGFUSE_SAMPLE_RATE=0.0 skips all traces
# ──────────────────────────────────────────────────────────────────────────────


def test_sample_rate_zero_skips_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    """sample_rate=0.0 must skip 100 % of traces (no thread, no urlopen)."""
    _set_env(
        monkeypatch,
        LANGFUSE_HOST="http://localhost:3000",
        LANGFUSE_PUBLIC_KEY="pk-lf-test",
        LANGFUSE_SECRET_KEY="sk-lf-test",
        LANGFUSE_SAMPLE_RATE="0.0",
    )
    assert langfuse_integration.is_enabled() is True

    started: List[threading.Thread] = []
    original_start = threading.Thread.start

    def _spy_start(self: threading.Thread) -> None:
        if self.name.startswith("langfuse-trace"):
            started.append(self)
        original_start(self)

    monkeypatch.setattr(threading.Thread, "start", _spy_start)

    calls: List[Any] = []

    def _spy_urlopen(*_a: Any, **_kw: Any) -> _FakeResponse:
        calls.append((_a, _kw))
        return _FakeResponse()

    monkeypatch.setattr(langfuse_integration.urllib.request, "urlopen", _spy_urlopen)

    for _ in range(50):
        langfuse_integration.trace_llm_call(
            model="m",
            input_messages=[{"role": "user", "content": "x"}],
            output="y",
            latency_ms=1.0,
        )

    assert started == []
    assert calls == []


# ──────────────────────────────────────────────────────────────────────────────
# Test #4 -- POST hits the right URL with Basic auth + correct payload
# ──────────────────────────────────────────────────────────────────────────────


def test_posts_to_correct_url_with_basic_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify URL, Basic-auth header, and the JSON batch shape."""
    _set_env(
        monkeypatch,
        LANGFUSE_HOST="https://obs.joveo.com",
        LANGFUSE_PUBLIC_KEY="pk-lf-public",
        LANGFUSE_SECRET_KEY="sk-lf-secret",
        LANGFUSE_SAMPLE_RATE="1.0",
    )

    captured: Dict[str, Any] = {}
    done = threading.Event()

    def _capture_urlopen(
        request: Any, timeout: float = 5.0
    ) -> _FakeResponse:  # noqa: ARG001
        # ``request`` is a urllib.request.Request -- capture URL, headers, body.
        try:
            captured["url"] = request.full_url
            captured["method"] = request.get_method()
            captured["headers"] = {k.lower(): v for k, v in request.header_items()}
            data = request.data or b""
            captured["body_raw"] = data
            captured["body_json"] = json.loads(data.decode("utf-8"))
        finally:
            done.set()
        return _FakeResponse(status=207)

    monkeypatch.setattr(
        langfuse_integration.urllib.request, "urlopen", _capture_urlopen
    )

    langfuse_integration.trace_llm_call(
        model="claude-haiku-4-5",
        input_messages=[{"role": "user", "content": "ping"}],
        output="pong",
        latency_ms=123.4,
        cost_usd=0.000123,
        metadata={
            "provider": "claude",
            "task_type": "conversational",
            "input_tokens": 7,
            "output_tokens": 3,
            "cache_hit": False,
            "fallback_used": False,
        },
        user_id="shubham@joveo.com",
        session_id="conv-abc-123",
    )

    assert done.wait(timeout=3.0), "POST thread did not run within 3 s"

    # 1) URL is host + ingestion path.
    assert captured["url"] == "https://obs.joveo.com/api/public/ingestion"
    assert captured["method"] == "POST"

    # 2) Basic-auth header is base64("pk-lf-public:sk-lf-secret").
    expected_header = "Basic " + base64.b64encode(b"pk-lf-public:sk-lf-secret").decode(
        "ascii"
    )
    # urllib lower-cases header names in header_items(); value preserved.
    assert captured["headers"].get("authorization") == expected_header
    assert captured["headers"].get("content-type") == "application/json"

    # 3) Payload shape: {"batch": [trace-create, generation-create]}.
    body = captured["body_json"]
    assert "batch" in body
    types = [event.get("type") for event in body["batch"]]
    assert "trace-create" in types
    assert "generation-create" in types

    # 4) Generation body carries the model + output + token usage.
    gen_event = next(e for e in body["batch"] if e["type"] == "generation-create")
    gen_body = gen_event["body"]
    assert gen_body["model"] == "claude-haiku-4-5"
    assert gen_body["output"] == "pong"
    assert gen_body["usage"]["input"] == 7
    assert gen_body["usage"]["output"] == 3
    assert gen_body["usage"]["unit"] == "TOKENS"

    # 5) Trace body carries user / session ids and tags.
    trace_event = next(e for e in body["batch"] if e["type"] == "trace-create")
    trace_body = trace_event["body"]
    assert trace_body["userId"] == "shubham@joveo.com"
    assert trace_body["sessionId"] == "conv-abc-123"
    assert "nova-ai-suite" in trace_body["tags"]


# ──────────────────────────────────────────────────────────────────────────────
# Bonus -- LANGFUSE_ENABLED=false hard-disables even with keys set
# ──────────────────────────────────────────────────────────────────────────────


def test_kill_switch_overrides_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    """Setting LANGFUSE_ENABLED=false must hard-disable the tracer."""
    _set_env(
        monkeypatch,
        LANGFUSE_HOST="https://obs.joveo.com",
        LANGFUSE_PUBLIC_KEY="pk-lf-public",
        LANGFUSE_SECRET_KEY="sk-lf-secret",
        LANGFUSE_ENABLED="false",
    )
    assert langfuse_integration.is_enabled() is False

    calls: List[Any] = []
    monkeypatch.setattr(
        langfuse_integration.urllib.request,
        "urlopen",
        lambda *a, **kw: calls.append((a, kw)) or _FakeResponse(),
    )

    langfuse_integration.trace_llm_call(
        model="m",
        input_messages=[{"role": "user", "content": "x"}],
        output="y",
        latency_ms=1.0,
    )
    assert calls == []
