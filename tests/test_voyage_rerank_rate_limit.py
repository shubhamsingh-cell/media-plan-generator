"""Tests that the Voyage rerank path is throttled by the shared rate window.

Background: ``_rerank_with_voyage`` (Voyage ``rerank-2.5-lite``) used to POST to
the Voyage API with NO rate accounting at all -- it never touched the
``_voyage_request_times`` / ``_VOYAGE_RPM_LIMIT`` window that ``_embed_uncached_voyage``
uses. So the rerank path could burst the shared Voyage account with no ceiling,
independently risking 429s. This routes rerank through
``_voyage_try_reserve_slot()`` immediately before its ``urlopen`` so embed and
rerank draw on ONE per-process budget.

Covers:
  * ``_voyage_try_reserve_slot`` is atomic: records on a free window, declines
    on a full one, and prunes entries older than 60s;
  * a rerank send consumes exactly one slot from the *same* window embed reads;
  * when the window is already full, rerank fires NO API call and falls back
    (returns None) instead of sending and risking a 429;
  * embed and rerank share the one window -- a window filled to the cap (as the
    embed path would fill it) blocks the rerank send.

All network is mocked at ``urllib.request.urlopen`` -- no live API calls. The
rate window itself (``_voyage_rate_lock`` / ``_voyage_request_times``) is the
code under test and is never stubbed.

Runs under pytest, or standalone:
``python3 tests/test_voyage_rerank_rate_limit.py``.
"""

import contextlib
import json
import sys
import threading
import time
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import vector_search as vs  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────────────


class _FakeResp:
    """Minimal context-manager stand-in for urllib.request.urlopen()."""

    def __init__(self, payload: dict):
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def read(self):
        return self._body


class _CountingUrlopen:
    """urlopen stand-in that counts calls, so "did rerank fire?" is testable."""

    def __init__(self):
        self.calls = 0
        self._lock = threading.Lock()

    def __call__(self, req, timeout=None, context=None):
        with self._lock:
            self.calls += 1
        # One reranked entry pointing back at the first candidate document.
        return _FakeResp({"data": [{"index": 0, "relevance_score": 0.9}]})


@contextlib.contextmanager
def _voyage_isolated():
    """Stub the network + API key, then restore the shared rate window.

    ``_voyage_request_times`` is mutated in place (slice assignment / append) by
    both the limiter and these tests, so it is snapshotted and restored to keep
    a seeded window from leaking across tests.

    Yields:
        The ``_CountingUrlopen`` instance standing in for the network.
    """
    times = list(vs._voyage_request_times)
    last = vs._voyage_last_request

    fake = _CountingUrlopen()
    patches = [
        mock.patch.object(vs, "_VOYAGE_API_KEY", "k"),
        mock.patch.object(vs.urllib.request, "urlopen", fake),
    ]
    for p in patches:
        p.start()
    try:
        yield fake
    finally:
        for p in reversed(patches):
            p.stop()
        vs._voyage_request_times[:] = times
        vs._voyage_last_request = last


def _candidate_docs() -> list[dict]:
    return [{"id": "d1", "text": "hello world", "metadata": {}}]


# ── _voyage_try_reserve_slot unit behavior ──────────────────────────────────


def test_try_reserve_slot_records_on_free_window():
    with _voyage_isolated():
        vs._voyage_request_times[:] = []
        assert vs._voyage_try_reserve_slot() is True
        assert len(vs._voyage_request_times) == 1


def test_try_reserve_slot_declines_full_window_without_recording():
    with _voyage_isolated():
        now = time.monotonic()
        vs._voyage_request_times[:] = [now] * vs._VOYAGE_RPM_LIMIT
        assert vs._voyage_try_reserve_slot() is False
        # Declined -- must NOT have appended an (N+1)th entry.
        assert len(vs._voyage_request_times) == vs._VOYAGE_RPM_LIMIT


def test_try_reserve_slot_prunes_stale_entries():
    with _voyage_isolated():
        now = time.monotonic()
        # A full window of entries older than 60s should all age out, leaving
        # room for a fresh slot.
        vs._voyage_request_times[:] = [now - 120.0] * vs._VOYAGE_RPM_LIMIT
        assert vs._voyage_try_reserve_slot() is True
        assert len(vs._voyage_request_times) == 1


# ── The rerank path goes through the shared window ───────────────────────────


def test_rerank_consumes_one_shared_slot():
    """A successful rerank records exactly one request into the shared window."""
    with _voyage_isolated() as fake:
        vs._voyage_request_times[:] = []
        out = vs._rerank_with_voyage(_candidate_docs(), "some query", top_k=1)
        assert fake.calls == 1, "rerank should have fired exactly one API call"
        assert out and out[0]["rerank_method"] == "voyage_rerank_2_5_lite"
        assert (
            len(vs._voyage_request_times) == 1
        ), "rerank must record into the shared _voyage_request_times window"


def test_rerank_blocked_when_window_full_fires_no_call():
    """A full window makes rerank fall back BEFORE any urlopen -- no 429 risk."""
    with _voyage_isolated() as fake:
        now = time.monotonic()
        vs._voyage_request_times[:] = [now] * vs._VOYAGE_RPM_LIMIT
        out = vs._rerank_with_voyage(_candidate_docs(), "some query", top_k=1)
        assert fake.calls == 0, "rerank must not POST when the window is full"
        assert out is None, "rerank should fall back (None) when rate-limited"
        # Window unchanged: the declined send recorded nothing.
        assert len(vs._voyage_request_times) == vs._VOYAGE_RPM_LIMIT


def test_embed_and_rerank_share_one_window():
    """Slots the embed path would fill count against the rerank path too."""
    with _voyage_isolated() as fake:
        now = time.monotonic()
        # Simulate the embed path having spent all but one slot.
        vs._voyage_request_times[:] = [now] * (vs._VOYAGE_RPM_LIMIT - 1)

        first = vs._rerank_with_voyage(_candidate_docs(), "q1", top_k=1)
        assert first is not None, "the last free slot should let rerank through"
        assert fake.calls == 1
        assert len(vs._voyage_request_times) == vs._VOYAGE_RPM_LIMIT

        # Budget now exhausted -- the next rerank must be blocked.
        second = vs._rerank_with_voyage(_candidate_docs(), "q2", top_k=1)
        assert second is None, "rerank must respect the now-full shared window"
        assert fake.calls == 1, "no second POST once the shared window is full"


# ── Standalone runner ────────────────────────────────────────────────────────

if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except AssertionError as exc:
                failures += 1
                print(f"FAIL {name}: {exc}")
    sys.exit(1 if failures else 0)
