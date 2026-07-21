"""Tests that the Voyage rerank path is throttled by its own cross-process window.

Background: ``_rerank_with_voyage`` (Voyage ``rerank-2.5-lite``) used to POST to
the Voyage API with NO rate accounting at all. It now reserves through
``_voyage_try_reserve_slot()`` -- the non-blocking wrapper over the unified
``_voyage_reserve_slot`` primitive -- immediately before its ``urlopen``.

The window is PER-MODEL and INSTANCE-GLOBAL: Voyage meters each model separately
(per-model, organization-wide RPM), so rerank-2.5-lite has its own window file,
independent of voyage-3-lite's embedding window, and both windows are shared
across gunicorn workers via flock over NOVA_SLOT_DIR (not per-process globals).

Covers:
  * a rerank send records exactly one reservation in the RERANK window file --
    proof the accounting is cross-process (file), not per-process (globals);
  * a full rerank window makes rerank fire NO API call and fall back (None),
    without recording anything;
  * stale (>60s) reservations age out;
  * per-model independence, exercised through the REAL paths on both sides: a
    saturated embed window does not block rerank, and a saturated rerank window
    does not block embed -- each asserted by actually calling the other path;
  * non-blocking means the lock too: rerank declines when another process holds
    the window's flock, rather than stalling the search hot path.

All network is mocked at ``urllib.request.urlopen`` -- no live API calls, and
the embed test stubs the disk cache (the tracked ~30MB file) per the convention
in tests/test_gemini_embeddings.py. The reservation windows themselves are the
code under test and are never stubbed.

Runs under pytest, or standalone:
``python3 tests/test_voyage_rerank_rate_limit.py``.
"""

import contextlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
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
    """urlopen stand-in that counts calls, so "did it fire?" is testable.

    Serves both shapes: a rerank response for the rerank endpoint, an embeddings
    response otherwise, so one instance can back tests that drive both paths.
    """

    def __init__(self):
        self.calls = 0

    def __call__(self, req, timeout=None, context=None):
        self.calls += 1
        url = getattr(req, "full_url", "")
        if "rerank" in url:
            return _FakeResp({"data": [{"index": 0, "relevance_score": 0.9}]})
        return _FakeResp({"data": [{"index": 0, "embedding": [0.1] * 512}]})


@contextlib.contextmanager
def _voyage_isolated():
    """Private NOVA_SLOT_DIR (own window files) + stubbed network/key/disk-cache.

    The cross-process windows live in files under NOVA_SLOT_DIR, so pointing that
    at a per-test tmpdir gives each test pristine embed AND rerank windows with no
    cross-test leakage. The in-process fallback globals are snapshotted/restored
    too, for the same anti-pollution reason as ever (ship-gate hang, 4cac3116).

    Disk-cache stubs follow tests/test_gemini_embeddings.py::_no_cache: the embed
    path otherwise lazily reads the tracked ~30MB cache file and writes stub
    vectors back into it.

    Yields:
        The ``_CountingUrlopen`` instance standing in for the network.
    """
    times = list(vs._voyage_request_times)
    last = vs._voyage_last_request

    slot_dir = tempfile.mkdtemp(prefix="nova_voyage_rerank_test_")
    fake = _CountingUrlopen()
    patches = [
        mock.patch.dict("os.environ", {"NOVA_SLOT_DIR": slot_dir}, clear=False),
        mock.patch.object(vs, "_VOYAGE_API_KEY", "k"),
        mock.patch.object(vs.urllib.request, "urlopen", fake),
        mock.patch.object(vs, "_load_embedding_cache", mock.Mock()),
        mock.patch.object(vs, "_cache_get_locked", mock.Mock(return_value=None)),
        mock.patch.object(vs, "_cache_put_locked", mock.Mock()),
        mock.patch.object(vs, "_ensure_flush_thread", mock.Mock()),
    ]
    for p in patches:
        p.start()
    try:
        yield fake
    finally:
        for p in reversed(patches):
            p.stop()
        shutil.rmtree(slot_dir, ignore_errors=True)
        vs._voyage_request_times[:] = times
        vs._voyage_last_request = last


def _seed_window(window: str, reservations) -> None:
    """Write wall-clock reservation times into one per-model window file."""
    _, path = vs._voyage_window_paths(window)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(list(reservations), fh)


def _read_window(window: str) -> list:
    _, path = vs._voyage_window_paths(window)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _candidate_docs() -> list[dict]:
    return [{"id": "d1", "text": "hello world", "metadata": {}}]


# ── _voyage_try_reserve_slot unit behavior (rerank window) ───────────────────


def test_try_reserve_slot_records_in_rerank_window_file():
    with _voyage_isolated():
        assert vs._voyage_try_reserve_slot() is True
        assert len(_read_window(vs._VOYAGE_WINDOW_RERANK)) == 1
        # And NOT into the embed window -- the windows are per-model.
        assert _read_window(vs._VOYAGE_WINDOW_EMBED) == []


def test_try_reserve_slot_declines_full_window_without_recording():
    with _voyage_isolated():
        now = time.time()
        _seed_window(vs._VOYAGE_WINDOW_RERANK, [now] * vs._VOYAGE_RPM_LIMIT)
        assert vs._voyage_try_reserve_slot() is False
        # Declined -- must NOT have appended an (N+1)th entry.
        assert len(_read_window(vs._VOYAGE_WINDOW_RERANK)) == vs._VOYAGE_RPM_LIMIT


def test_try_reserve_slot_prunes_stale_entries():
    with _voyage_isolated():
        now = time.time()
        # A full window of entries older than 60s should all age out, leaving
        # room for a fresh slot.
        _seed_window(vs._VOYAGE_WINDOW_RERANK, [now - 120.0] * vs._VOYAGE_RPM_LIMIT)
        assert vs._voyage_try_reserve_slot() is True
        assert len(_read_window(vs._VOYAGE_WINDOW_RERANK)) == 1


def test_try_reserve_slot_declines_when_lock_contended():
    """Non-blocking includes the flock, not just the window: if another process
    holds the rerank window's lock, rerank declines instead of stalling."""
    holder = None
    with _voyage_isolated():
        lock_path, _ = vs._voyage_window_paths(vs._VOYAGE_WINDOW_RERANK)
        # A real second process holds the flock (flock does not conflict between
        # fds of the same process on some platforms, so in-process won't do).
        holder = subprocess.Popen(
            [
                sys.executable,
                "-c",
                (
                    "import fcntl,sys,time\n"
                    f"f=open({lock_path!r},'a+b')\n"
                    "fcntl.flock(f,fcntl.LOCK_EX)\n"
                    "print('locked',flush=True)\n"
                    "time.sleep(30)\n"
                ),
            ],
            stdout=subprocess.PIPE,
        )
        try:
            assert holder.stdout.readline().strip() == b"locked"
            started = time.monotonic()
            assert vs._voyage_try_reserve_slot() is False
            elapsed = time.monotonic() - started
            assert elapsed < 1.0, f"declined but only after {elapsed:.2f}s stall"
            assert _read_window(vs._VOYAGE_WINDOW_RERANK) == []
        finally:
            holder.kill()
            holder.wait()


# ── The rerank path goes through its window ──────────────────────────────────


def test_rerank_consumes_one_slot_in_its_window():
    """A successful rerank records exactly one reservation, in the FILE window."""
    with _voyage_isolated() as fake:
        out = vs._rerank_with_voyage(_candidate_docs(), "some query", top_k=1)
        assert fake.calls == 1, "rerank should have fired exactly one API call"
        assert out and out[0]["rerank_method"] == "voyage_rerank_2_5_lite"
        assert (
            len(_read_window(vs._VOYAGE_WINDOW_RERANK)) == 1
        ), "rerank must record into the cross-process rerank window file"


def test_rerank_blocked_when_window_full_fires_no_call():
    """A full window makes rerank fall back BEFORE any urlopen -- no 429 risk."""
    with _voyage_isolated() as fake:
        now = time.time()
        _seed_window(vs._VOYAGE_WINDOW_RERANK, [now] * vs._VOYAGE_RPM_LIMIT)
        out = vs._rerank_with_voyage(_candidate_docs(), "some query", top_k=1)
        assert fake.calls == 0, "rerank must not POST when the window is full"
        assert out is None, "rerank should fall back (None) when rate-limited"
        # Window unchanged: the declined send recorded nothing.
        assert len(_read_window(vs._VOYAGE_WINDOW_RERANK)) == vs._VOYAGE_RPM_LIMIT


# ── Per-model independence, exercised through the REAL paths on both sides ────
# Voyage meters voyage-3-lite (embed) and rerank-2.5-lite (rerank) separately,
# so the app keeps separate windows: pressure on one model must never starve the
# other. Unlike a seeded-globals simulation, these call the actual other path.


def test_saturated_embed_window_does_not_block_rerank():
    with _voyage_isolated() as fake:
        _seed_window(vs._VOYAGE_WINDOW_EMBED, [time.time()] * vs._VOYAGE_RPM_LIMIT)
        out = vs._rerank_with_voyage(_candidate_docs(), "q", top_k=1)
        assert out is not None, "embed-window pressure must not starve rerank"
        assert fake.calls == 1


def test_saturated_rerank_window_does_not_block_embed():
    with _voyage_isolated() as fake:
        _seed_window(vs._VOYAGE_WINDOW_RERANK, [time.time()] * vs._VOYAGE_RPM_LIMIT)
        result = vs._embed_uncached_voyage(["hello"])
        assert (
            result is not None and len(result) == 1
        ), "rerank-window pressure must not starve the embedding path"
        assert fake.calls == 1
        # And embed recorded into ITS window, not rerank's.
        assert len(_read_window(vs._VOYAGE_WINDOW_EMBED)) == 1
        assert len(_read_window(vs._VOYAGE_WINDOW_RERANK)) == vs._VOYAGE_RPM_LIMIT


# ── Standalone runner ────────────────────────────────────────────────────────

if __name__ == "__main__":
    _failures = 0
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            try:
                _fn()
                print(f"PASS {_name}")
            except AssertionError as exc:
                _failures += 1
                print(f"FAIL {_name}: {exc}")
            except Exception as exc:  # noqa: BLE001
                _failures += 1
                print(f"ERROR {_name}: {exc}")
    sys.exit(1 if _failures else 0)
