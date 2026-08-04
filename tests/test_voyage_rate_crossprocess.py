"""Tests for the cross-process Voyage rate limiter (vector_search).

Production runs ``gunicorn --workers 2 --preload`` (render.yaml), so the old
per-PROCESS rate window let each worker independently enforce _VOYAGE_RPM_LIMIT
and the instance as a whole could send N x the cap to Voyage -- the exact 429s
the limiter was written to prevent. The limiter now backs its window with an
flock over a shared file (mirroring app._CrossProcessSlots) so the cap is
INSTANCE-GLOBAL.

These tests exercise the flock-backed window directly: they never touch the
real Voyage API (urllib.request.urlopen is stubbed) and never touch the real
~30MB tracked embedding cache (_load_embedding_cache / _cache_put_locked and
friends are stubbed, matching tests/test_gemini_embeddings.py::_no_cache). The
window's shared dir is pointed at a private tmpdir per test via NOVA_SLOT_DIR.

Runs under pytest, or standalone: ``python3 tests/test_voyage_rate_crossprocess.py``.
"""

import json
import os
import shutil
import sys
import tempfile
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


def _voyage_payload(vectors):
    """Shape a fake Voyage /embeddings response (order-preserving with index)."""
    return {"data": [{"index": i, "embedding": v} for i, v in enumerate(vectors)]}


def _no_cache():
    """Force every embed_batch lookup to miss and skip ALL disk I/O.

    Mirrors tests/test_gemini_embeddings.py::_no_cache -- critically stubs the
    real cache load/put so the tracked ~30MB data/.embedding_cache.json is never
    read or written by the test.
    """
    return mock.patch.multiple(
        vs,
        _load_embedding_cache=mock.Mock(),
        _cache_get_locked=mock.Mock(return_value=None),
        _cache_put_locked=mock.Mock(),
        _ensure_flush_thread=mock.Mock(),
    )


class _SlotDir:
    """Context manager: private NOVA_SLOT_DIR + clean rate globals per test."""

    def __enter__(self):
        self._dir = tempfile.mkdtemp(prefix="nova_voyage_rate_test_")
        self._env = mock.patch.dict(
            "os.environ", {"NOVA_SLOT_DIR": self._dir}, clear=False
        )
        self._env.start()
        # Reset the in-process fallback window so tests are order-independent.
        vs._voyage_request_times.clear()
        vs._voyage_last_request = 0.0
        return self

    def __exit__(self, *exc):
        self._env.stop()
        shutil.rmtree(self._dir, ignore_errors=True)
        return False

    @property
    def window_path(self):
        """Path to the EMBED model's window file (rerank has its own)."""
        return vs._voyage_window_paths(vs._VOYAGE_WINDOW_EMBED)[1]


def _read_window(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ── The shared window is persisted across independent calls ───────────────────


def test_first_send_reserves_immediately_without_sleeping():
    # Empty window -> the very first request sends now, no throttle sleep.
    with _SlotDir() as slot, mock.patch.object(
        vs.time, "sleep"
    ) as slept, mock.patch.object(vs.time, "time", return_value=1000.0):
        vs._voyage_acquire_send_slot()

        slept.assert_not_called()
        reserved = _read_window(slot.window_path)
    assert reserved == [1000.0]


def test_second_send_is_spaced_by_min_delay():
    # A prior reservation at t=1000 forces the next send to 1000 + MIN_DELAY.
    with _SlotDir() as slot, mock.patch.object(vs.time, "sleep") as slept:
        with mock.patch.object(vs.time, "time", return_value=1000.0):
            vs._voyage_acquire_send_slot()
        # Second call one second later: must wait out the remaining spacing.
        with mock.patch.object(vs.time, "time", return_value=1001.0):
            vs._voyage_acquire_send_slot()

        # The second call slept for (1000 + MIN_DELAY) - 1001.
        expected_wait = (1000.0 + vs._VOYAGE_MIN_DELAY) - 1001.0
        assert slept.call_count == 1
        assert abs(slept.call_args.args[0] - expected_wait) < 1e-6

        reserved = _read_window(slot.window_path)
    assert reserved[0] == 1000.0
    assert abs(reserved[1] - (1000.0 + vs._VOYAGE_MIN_DELAY)) < 1e-6


def test_window_reservations_survive_a_fresh_module_read():
    # Simulate "another worker" by reserving, then reading the file back the way
    # a second process would -- the reservation must be visible (that is the
    # whole point: state lives in the file, not a per-process global).
    with _SlotDir() as slot, mock.patch.object(vs.time, "sleep"), mock.patch.object(
        vs.time, "time", return_value=5000.0
    ):
        vs._voyage_acquire_send_slot()
        # _read_voyage_window is exactly what a second worker would call.
        seen = vs._read_voyage_window(slot.window_path)
    assert seen == [5000.0]


def test_full_window_pushes_next_send_past_the_oldest():
    # Pre-load the window with _VOYAGE_RPM_LIMIT reservations spaced so they all
    # fall inside a trailing 60s window; the next send must wait until the
    # oldest ages out (oldest + 60 + 0.5 buffer), enforcing the RPM cap.
    limit = vs._VOYAGE_RPM_LIMIT
    base = 10_000.0
    # Reservations at base, base+0.1, base+0.2, ... (all within ~1s => all in
    # the same 60s window). now = base + 1 so all are still "recent".
    existing = [base + 0.1 * i for i in range(limit)]
    with _SlotDir() as slot, mock.patch.object(vs.time, "sleep") as slept:
        with open(slot.window_path, "w", encoding="utf-8") as fh:
            json.dump(existing, fh)
        now = base + 1.0
        with mock.patch.object(vs.time, "time", return_value=now):
            vs._voyage_acquire_send_slot()

        reserved = _read_window(slot.window_path)
        new_reservation = reserved[-1]
        # Oldest reservation is `base`; ours must land at base + 60 + 0.5.
        assert abs(new_reservation - (base + 60.0 + 0.5)) < 1e-6
        # And it slept for (that time) - now.
        assert slept.call_count == 1
        assert abs(slept.call_args.args[0] - ((base + 60.5) - now)) < 1e-6


def test_aged_out_reservations_are_pruned():
    # Reservations older than 60s must be dropped, not counted toward the cap.
    with _SlotDir() as slot, mock.patch.object(vs.time, "sleep"):
        stale = [100.0, 200.0]  # far older than now-60
        with open(slot.window_path, "w", encoding="utf-8") as fh:
            json.dump(stale, fh)
        with mock.patch.object(vs.time, "time", return_value=10_000.0):
            vs._voyage_acquire_send_slot()

        reserved = _read_window(slot.window_path)
    # Stale entries gone; only the fresh reservation remains.
    assert reserved == [10_000.0]


def test_corrupt_window_file_is_tolerated():
    # A torn/foreign window file must fail open to an empty window, never raise.
    with _SlotDir() as slot, mock.patch.object(vs.time, "sleep"):
        with open(slot.window_path, "w", encoding="utf-8") as fh:
            fh.write("{not json at all")
        with mock.patch.object(vs.time, "time", return_value=42.0):
            vs._voyage_acquire_send_slot()  # must not raise
        reserved = _read_window(slot.window_path)
    assert reserved == [42.0]


# ── End-to-end: embed_batch drives the shared limiter, never the API twice ────


def test_embed_batch_routes_through_shared_limiter():
    # embed_batch (voyage default) must call _voyage_acquire_send_slot before
    # hitting urlopen -- proving the limiter is on the live request path.
    # Pinned to the legacy voyage-3-lite model (512-dim) rather than widening
    # the stub to 1024: this test is about the rate limiter, not the model
    # succession, and the fixed-512 stub would otherwise trip the
    # voyage-4-lite-default response-dim guard.
    vecs = [[0.5] * 512]
    with _SlotDir(), _no_cache(), mock.patch.dict(
        # Explicit since the 2026-07-21 cutover made gemini the default:
        # this test proves the VOYAGE limiter sits on the live request path.
        "os.environ",
        {"EMBEDDING_PROVIDER": "voyage"},
        clear=False,
    ), mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-3-lite"), mock.patch.object(
        vs, "_VOYAGE_API_KEY", "k"
    ), mock.patch.object(
        vs.time, "sleep"
    ), mock.patch.object(
        vs, "_voyage_acquire_send_slot", wraps=vs._voyage_acquire_send_slot
    ) as slot, mock.patch.object(
        vs.urllib.request, "urlopen", return_value=_FakeResp(_voyage_payload(vecs))
    ):
        out = vs.embed_batch(["hello"])

    assert out == vecs
    slot.assert_called_once()


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
