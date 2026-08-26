"""Regression tests for PostHog dead-letter classification (posthog_integration.py).

Pre-fix, PostHogClient._flush()'s except urllib.error.HTTPError handler
dead-lettered the whole batch on ANY HTTP error code, unconditionally.
With an invalid/expired POSTHOG_API_KEY every flush 401s, so every batch
-- forever -- got requeued into _dead_letter_queue, retried by the next
flush cycle (_FLUSH_INTERVAL_S, every 5s), 401'd again, and got
requeued again: a permanent-failure retry storm that resends doomed
requests indefinitely.

The fix distinguishes permanent client/auth failures (400/401/403, which
can never succeed on retry -- log once, drop the batch) from transient
ones (429/5xx/network, which might succeed later -- keep the existing
dead-letter-and-retry behavior unchanged).
"""

from __future__ import annotations

import io
import json
import urllib.error
from email.message import Message
from unittest import mock

import pytest

import posthog_integration as ph


class _FakeResponse:
    """Minimal stand-in for the context-managed object urlopen() returns."""

    def __init__(self, code: int = 200) -> None:
        self._code = code

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False

    def getcode(self) -> int:
        return self._code

    def read(self, n: int = -1) -> bytes:
        return b""


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="https://us.i.posthog.com/batch/",
        code=code,
        msg="error",
        hdrs=Message(),
        fp=io.BytesIO(b"error body"),
    )


def _make_client() -> ph.PostHogClient:
    """Construct a client with the background flush timer disabled.

    ``_enabled`` gates ``_start_flush_thread()`` in ``__init__``; POSTHOG_API_KEY
    is unset in the test environment so ``_enabled`` is False and no real
    timer thread starts -- each test drives ``_flush()`` directly instead.
    """
    client = ph.PostHogClient()
    assert client._enabled is False  # sanity: confirms no background thread is running
    return client


@pytest.fixture(autouse=True)
def _clear_dead_letter_queue() -> None:
    """_dead_letter_queue is module-level (shared across clients) -- isolate
    each test from whatever a previous test left behind."""
    ph._dead_letter_queue.clear()
    yield
    ph._dead_letter_queue.clear()


@pytest.mark.parametrize("code", [400, 401, 403])
def test_permanent_http_errors_are_dropped_not_dead_lettered(code: int) -> None:
    """400/401/403 can never succeed on retry: the batch must be dropped,
    not pushed to the dead-letter queue. This is the actual storm fix --
    pre-fix, every one of these codes was unconditionally dead-lettered."""
    client = _make_client()
    client._queue.append({"event": "test.event"})

    with mock.patch.object(ph.urllib.request, "urlopen", side_effect=_http_error(code)):
        client._flush()

    assert len(ph._dead_letter_queue) == 0
    assert client._queue_size() == 0  # drained, not silently left queued either


@pytest.mark.parametrize("code", [429, 500, 502, 503])
def test_transient_http_errors_still_retry_via_dead_letter(code: int) -> None:
    """429/5xx are transient -- the existing dead-letter-and-retry behavior
    must be unchanged by this fix."""
    client = _make_client()
    client._queue.append({"event": "test.event"})

    with mock.patch.object(ph.urllib.request, "urlopen", side_effect=_http_error(code)):
        client._flush()

    assert len(ph._dead_letter_queue) == 1


def test_dead_lettered_batch_is_retried_on_next_flush() -> None:
    """Sanity check for the retry path this fix must not break: an event
    dead-lettered by an earlier (transient) failure is included in, and
    cleared by, the next successful flush."""
    ph._dead_letter_queue.append({"event": "stuck.event"})
    client = _make_client()

    sent_batches: list[list[dict]] = []

    def _fake_urlopen(req: object, timeout: float | None = None) -> _FakeResponse:
        payload = json.loads(req.data.decode("utf-8"))  # type: ignore[attr-defined]
        sent_batches.append(payload["batch"])
        return _FakeResponse(200)

    with mock.patch.object(ph.urllib.request, "urlopen", side_effect=_fake_urlopen):
        client._flush()

    assert sent_batches == [[{"event": "stuck.event"}]]
    assert len(ph._dead_letter_queue) == 0
