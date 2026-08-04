"""Tests for caller-deadline propagation into the embedding layer.

Background: ``search_bounded``/``nova._bounded_vector_search`` run ``search()``
in a worker thread and abandon it when ``join(timeout)`` expires. ``join()``
cannot cancel the worker, so before this fix the abandoned thread slept out the
full rate-limiter wait and then fired a real Voyage request for a caller that
had already given up -- spending one of the 10 requests/minute budget on a
result nobody could read, while holding ``_voyage_rate_lock`` throughout.

Covers:
  * the worker abandons before both in-lock sleeps once the caller's deadline
    cannot be met, and fires no API call;
  * callers with no deadline (build_index / startup indexing) keep their
    previous unbounded behavior exactly;
  * ``search_bounded`` and ``nova._bounded_vector_search`` leave no orphan;
  * the in-lock hold stays bounded by ``_VOYAGE_MIN_DELAY`` instead of growing
    to ``_VOYAGE_MIN_DELAY + queue_wait`` (the stale-``now`` amplification);
  * ``embed_deadline`` restores the prior value, so it cannot leak across tasks
    if a thread is ever pooled;
  * the load-bearing invariant -- an expired deadline never spends quota at the
    POST -- holds under BOTH ``EMBEDDING_PROVIDER=voyage`` and ``=gemini``, so
    the queued provider switch (SESSION_HANDOFF task #11) cannot silently drop
    it on the live path; and the Gemini retry path bails before its backoff
    sleep, the same way the Voyage path bails before its rate-limiter sleeps.

All network is mocked at ``urllib.request.urlopen`` -- no live API calls. The
rate limiter itself (``with _voyage_rate_lock:`` and its ``time.sleep`` calls)
is the code under test and is never stubbed.

Runs under pytest, or standalone: ``python3 tests/test_voyage_embed_deadline.py``.
"""

import contextlib
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path
from unittest import mock

import pytest

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
    """urlopen stand-in that counts calls, so "did the worker fire?" is testable."""

    def __init__(self):
        self.calls = 0
        self._lock = threading.Lock()

    def __call__(self, req, timeout=None, context=None):
        with self._lock:
            self.calls += 1
        return _FakeResp({"data": [{"index": 0, "embedding": [0.1] * 512}]})


def _bounded_workers() -> list:
    return [
        t
        for t in threading.enumerate()
        if t.name in ("vs-bounded", "bounded-vs") and t.is_alive()
    ]


def _drain_bounded_workers(timeout: float = 30.0) -> None:
    """Join any leaked worker so it cannot hold the lock into the next test."""
    for t in _bounded_workers():
        t.join(timeout=timeout)


def _unique_query() -> str:
    """Embedding results are disk-cached by text, so each call needs a new one."""
    return f"deadline probe {time.monotonic_ns()}"


@contextlib.contextmanager
def _voyage_isolated(index=None, min_delay=None):
    """Own the whole Voyage fixture: stub the network, then restore all state.

    Teardown order is load-bearing. An abandoned worker -- the very defect this
    module tests -- outlives the test body, so it must be drained while the
    urlopen stub is STILL installed. Draining after the stub is removed sends a
    real request to api.voyageai.com; running these tests against the unfixed
    code did exactly that and logged "HTTP Error 401: Unauthorized".

    ``mock.patch.object`` also restores rebound attributes but NOT the contents
    of ``_voyage_request_times``, which the limiter mutates in place via slice
    assignment. Leaving a seeded window or a stale ``_voyage_last_request``
    behind is the pollution class that caused a 30-minute ship-gate hang
    (commit 4cac3116), so both are saved and restored here.

    The EMBED path's rate limiter is now the flock-backed cross-process window
    (``_voyage_acquire_send_slot``), which persists reservations to a file under
    ``NOVA_SLOT_DIR``. This fixture points ``NOVA_SLOT_DIR`` at a private tmpdir
    per test (and removes it on teardown) so one test's reservations can't arm
    the next test's wait. Tests arm a wait by seeding that file via
    ``_seed_voyage_window`` rather than by seeding ``_voyage_request_times``.

    The disk cache is stubbed out at both ends, which the timing assertions
    depend on. ``_cache_put_locked``: ``embed_batch`` persists whatever it
    computes into the tracked ``data/.embedding_cache.json`` (a 2,000-entry
    LRU), so a stubbed embedding would evict two real vectors and dirty the
    working tree -- observed exactly that before this guard existed.
    ``_load_embedding_cache``: it is a lazy one-shot read of that same ~30MB
    file (~0.77s cold, ~0.12s warm), nothing else in the suite primes it
    (tests/test_gemini_embeddings.py:58 stubs it rather than running it), and
    the first test here to reach embed_batch would otherwise pay that cost
    inside its 0.5s worker budget -- passing only on a machine whose page cache
    is already warm, and failing the ship gate on a cold CI container. Stubbing
    matches the convention that test file already set.

    Args:
        index: Optional value for ``vs._index``. ``search()`` only embeds when
            a vector tier exists, so tests that go through ``search`` must pass
            one or they silently exercise nothing.
        min_delay: Optional override for ``_VOYAGE_MIN_DELAY``, to keep tests
            that must actually sleep sub-second.

    Yields:
        The ``_CountingUrlopen`` instance standing in for the network.
    """
    times = list(vs._voyage_request_times)
    last = vs._voyage_last_request
    startup = vs._is_startup_indexing

    slot_dir = tempfile.mkdtemp(prefix="nova_voyage_deadline_")

    fake = _CountingUrlopen()
    patches = [
        mock.patch.dict(
            "os.environ",
            # Pin the provider: this fixture exists to exercise the VOYAGE
            # paths, and since the 2026-07-21 cutover the module default is
            # gemini -- relying on the default would silently route
            # embed_batch to the wrong provider.
            {"NOVA_SLOT_DIR": slot_dir, "EMBEDDING_PROVIDER": "voyage"},
            clear=False,
        ),
        # Pin the LEGACY voyage-3-lite model (512-dim) rather than widening
        # every fake response below to 1024: this module is about deadline
        # propagation, not the model succession, and _CountingUrlopen's
        # fixed 512-dim stub would otherwise trip the voyage-4-lite-default
        # response-dim guard added in the same change that introduced it.
        mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-3-lite"),
        mock.patch.object(vs, "_VOYAGE_API_KEY", "k"),
        mock.patch.object(vs.urllib.request, "urlopen", fake),
        mock.patch.object(vs, "_cache_put_locked", lambda key, value: None),
        mock.patch.object(vs, "_load_embedding_cache", lambda: None),
    ]
    if index is not None:
        patches.append(mock.patch.object(vs, "_index", index))
    if min_delay is not None:
        patches.append(mock.patch.object(vs, "_VOYAGE_MIN_DELAY", min_delay))

    for p in patches:
        p.start()
    try:
        yield fake
    finally:
        _drain_bounded_workers()  # while urlopen is still stubbed
        for p in reversed(patches):
            p.stop()
        shutil.rmtree(slot_dir, ignore_errors=True)
        vs._voyage_request_times[:] = times
        vs._voyage_last_request = last
        vs._is_startup_indexing = startup


def _seed_voyage_window(reservations) -> None:
    """Seed the cross-process EMBED rate window with wall-clock reservation times
    so ``_voyage_acquire_send_slot`` computes a real wait. Must run inside
    ``_voyage_isolated`` (which points NOVA_SLOT_DIR at a private dir).

    One recent reservation (``[time.time()]``) arms a ``_VOYAGE_MIN_DELAY``
    spacing wait; ``_VOYAGE_RPM_LIMIT`` recent reservations arm the ~60s
    sliding-window wait.
    """
    _, path = vs._voyage_window_paths(vs._VOYAGE_WINDOW_EMBED)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(list(reservations), fh)


def _fake_index() -> list:
    return [
        {"id": "d1", "text": "hello world", "embedding": [0.1] * 512, "metadata": {}}
    ]


# ── The deadline is honored before each in-lock sleep ────────────────────────


def test_abandons_before_spacing_sleep_when_deadline_unreachable():
    """_VOYAGE_MIN_DELAY (6.5s) > the callers' 3s timeout, so this is the norm."""
    with _voyage_isolated() as fake:
        # Arm the spacing guard: a request just went out, so the next one owes
        # a full _VOYAGE_MIN_DELAY wait.
        _seed_voyage_window([time.time()])

        started = time.monotonic()
        with vs.embed_deadline(time.monotonic() + 0.1):
            result = vs._embed_uncached_voyage(["hello"])
        elapsed = time.monotonic() - started
        calls = fake.calls

    assert result is None, "should abandon rather than return partial embeddings"
    assert elapsed < 1.0, f"should bail before the 6.5s sleep, took {elapsed:.2f}s"
    assert calls == 0, "must not spend Voyage quota for a caller that has left"


def test_abandons_before_rate_limit_window_sleep():
    """The other in-lock sleep: the 60s sliding-window wait."""
    with _voyage_isolated() as fake:
        # Saturate the window with _VOYAGE_RPM_LIMIT recent reservations so the
        # sliding-window branch (not the spacing branch) is the one that fires.
        _seed_voyage_window([time.time() - 5.0] * vs._VOYAGE_RPM_LIMIT)

        started = time.monotonic()
        with vs.embed_deadline(time.monotonic() + 0.1):
            result = vs._embed_uncached_voyage(["hello"])
        elapsed = time.monotonic() - started
        calls = fake.calls

    assert result is None
    assert (
        elapsed < 1.0
    ), f"should bail before the ~55s window wait, took {elapsed:.2f}s"
    assert calls == 0


def test_expired_deadline_skips_the_post_even_with_no_sleep_pending():
    """The quota is charged at the POST, not at the sleeps.

    A worker can reach the request with its deadline already spent and no wait
    pending -- nothing was slow enough to trip a sleep guard, the caller was
    just already gone (e.g. a slow embedding-cache load ate the budget). Firing
    the POST here would spend one of the 10 requests/minute on a result nobody
    can read, which is exactly what the deadline exists to prevent.
    """
    with _voyage_isolated() as fake:
        # Empty window (fresh per-test dir) => no wait owed, so no spacing/window
        # sleep guard fires: the already-expired deadline (wait==0 case) is the
        # only thing that can bail before the POST.
        with vs.embed_deadline(time.monotonic() - 1.0):  # already expired
            result = vs._embed_uncached_voyage(["hello"])
        calls = fake.calls

    assert result is None
    assert calls == 0, "fired a Voyage request for a caller that had already left"


def test_deadline_far_enough_away_still_sleeps_and_succeeds():
    """A deadline the wait fits inside must not trigger a spurious abandon."""
    with _voyage_isolated(min_delay=0.2) as fake:
        _seed_voyage_window([time.time()])  # arms a 0.2s spacing wait

        with vs.embed_deadline(time.monotonic() + 10.0):
            result = vs._embed_uncached_voyage(["hello"])
        calls = fake.calls

    assert result is not None and len(result) == 1
    assert calls == 1


# ── Un-deadlined callers keep their existing behavior ────────────────────────


def test_no_deadline_preserves_unbounded_sleep_behavior():
    """build_index/startup indexing sets no deadline and must be unaffected."""
    with _voyage_isolated(min_delay=0.2) as fake:
        _seed_voyage_window([time.time()])  # arms a 0.2s spacing wait

        # No embed_deadline() wrapper at all -- the default.
        result = vs._embed_uncached_voyage(["hello"])
        calls = fake.calls

    assert result is not None, "an un-deadlined caller must still wait and succeed"
    assert calls == 1


def test_deadline_exceeded_by_is_false_without_a_deadline():
    assert getattr(vs._embed_deadline, "value", None) is None
    assert vs._deadline_exceeded_by(9999.0) is False


# ── End-to-end: no orphan, no wasted quota ───────────────────────────────────


def test_search_bounded_leaves_no_orphan_burning_quota():
    with _voyage_isolated(index=_fake_index()) as fake:
        assert vs._index, "fixture must populate _index or search() never embeds"
        _seed_voyage_window([time.time()])  # arms the 6.5s spacing wait

        started = time.monotonic()
        results = vs.search_bounded(_unique_query(), top_k=3, timeout_s=0.5)
        elapsed = time.monotonic() - started

        # The worker should have bailed on its own, not been abandoned alive.
        leaked = [t.name for t in _bounded_workers()]
        calls_at_return = fake.calls

    assert isinstance(results, list)
    assert elapsed < 0.5, (
        f"worker should bail before the spacing sleep rather than burn the "
        f"full timeout; took {elapsed:.2f}s"
    )
    assert leaked == [], f"worker outlived its caller: {leaked}"
    assert calls_at_return == 0, "abandoned worker must not spend Voyage quota"


def test_bounded_vector_search_in_nova_also_propagates_deadline():
    """nova.py has its own copy of the wrapper; it must honor the deadline too."""
    import nova

    with _voyage_isolated(index=_fake_index()) as fake:
        _seed_voyage_window([time.time()])  # arms the 6.5s spacing wait

        started = time.monotonic()
        nova._bounded_vector_search(_unique_query(), top_k=3, timeout_s=0.5)
        elapsed = time.monotonic() - started
        leaked = [t.name for t in _bounded_workers()]
        calls_at_return = fake.calls

    assert elapsed < 0.5, f"took {elapsed:.2f}s"
    assert leaked == [], f"worker outlived its caller: {leaked}"
    assert calls_at_return == 0


# ── The stale-`now` amplification is gone ────────────────────────────────────


def test_in_lock_hold_does_not_grow_with_queue_wait():
    """Exercises the in-process fallback (_voyage_reserve_slot_inprocess), which
    still uses _voyage_rate_lock; both live paths now use the flock-backed
    cross-process windows and no longer touch that lock.

    Reading `now` outside the lock made `elapsed` negative while queued.

    wait_time = _VOYAGE_MIN_DELAY - elapsed then became
    _VOYAGE_MIN_DELAY + queue_wait, so each thread queued behind the lock slept
    LONGER than the one before it -- positive feedback rather than a bounded
    cap. With `now` read inside the lock the wait can never exceed
    _VOYAGE_MIN_DELAY however long acquiring took.
    """
    queue_wait = 1.0
    min_delay = 0.5
    released = threading.Event()

    def _hold_lock():
        # Stand in for a real in-flight embed call: hold the lock, then stamp
        # _voyage_last_request the way the live path does after its own wait.
        # Stamping inside the block guarantees the victim sees the new value,
        # which is what drives its stale `now` negative.
        with vs._voyage_rate_lock:
            released.set()
            time.sleep(queue_wait)
            vs._voyage_last_request = time.monotonic()

    with _voyage_isolated(min_delay=min_delay):
        vs._voyage_request_times[:] = []
        vs._voyage_last_request = time.monotonic()

        holder = threading.Thread(target=_hold_lock, name="lock-holder", daemon=True)
        holder.start()
        assert released.wait(timeout=5.0), "holder never acquired the lock"

        started = time.monotonic()
        vs._voyage_reserve_slot_inprocess(  # samples `now`, queues behind holder
            blocking=True, min_delay=min_delay, rpm_limit=vs._VOYAGE_RPM_LIMIT
        )
        elapsed = time.monotonic() - started
        holder.join(timeout=5.0)

    # Pre-fix `now` is sampled before the ~1.0s queue wait, so by the time the
    # lock is acquired _voyage_last_request is NEWER than `now`: elapsed goes to
    # -queue_wait and wait_time becomes min_delay + queue_wait.
    #   pre-fix : queue_wait + (min_delay + queue_wait) = 2.5s
    #   post-fix: queue_wait + at most min_delay        = 1.5s
    assert elapsed < queue_wait + min_delay + 0.3, (
        f"in-lock wait still scales with queue time ({elapsed:.2f}s) -- the "
        f"stale-`now` amplification is back"
    )


# ── The deadline cannot leak across tasks ────────────────────────────────────


def test_embed_deadline_restores_previous_value():
    assert getattr(vs._embed_deadline, "value", None) is None

    with vs.embed_deadline(123.0):
        assert vs._embed_deadline.value == 123.0
        with vs.embed_deadline(456.0):
            assert vs._embed_deadline.value == 456.0
        assert vs._embed_deadline.value == 123.0

    assert getattr(vs._embed_deadline, "value", None) is None


def test_embed_deadline_restores_on_exception():
    try:
        with vs.embed_deadline(123.0):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert getattr(vs._embed_deadline, "value", None) is None


# ── The invariant holds for whichever provider EMBEDDING_PROVIDER selects ─────
#
# SESSION_HANDOFF.md task #11 queues ``EMBEDDING_PROVIDER=gemini`` as a
# production change. ``embed_batch`` routes to ``_embed_batch_gemini`` under that
# flag, and that path has its own POST and its own retry-backoff sleeps -- so the
# deadline guard has to live on BOTH low-level paths or flipping the env var
# silently turns the mechanism into a no-op on the live path, with no failing
# test to signal the loss. These tests pin the invariant to both providers.
#
# The Gemini path has no rate limiter, lock, or sliding window (Google's free
# tier is far more generous), so only the quota-waste half of the Voyage fix
# applies: the abandoned worker must not fire the POST, and must not sleep out a
# retry backoff, once the caller's deadline has passed.


class _CountingUrlopenGemini:
    """urlopen stand-in for Gemini's batchEmbedContents endpoint (768-dim)."""

    def __init__(self):
        self.calls = 0
        self._lock = threading.Lock()

    def __call__(self, req, timeout=None, context=None):
        with self._lock:
            self.calls += 1
        return _FakeResp({"embeddings": [{"values": [0.1] * 768}]})


@contextlib.contextmanager
def _gemini_isolated():
    """Gemini analogue of ``_voyage_isolated``: stub the network and API key.

    The Gemini path keeps none of the Voyage rate-limiter state
    (``_voyage_request_times``/``_voyage_last_request``), so there is nothing of
    that kind to save and restore. The disk-cache stubs are kept for parity with
    ``_voyage_isolated`` and to hold the convention that no test in this module
    touches the tracked ~30MB ``data/.embedding_cache.json`` -- although
    ``_embed_batch_gemini`` is called directly here, below ``embed_batch``, so it
    never reaches the cache regardless.

    Yields:
        The ``_CountingUrlopenGemini`` instance standing in for the network.
    """
    fake = _CountingUrlopenGemini()
    patches = [
        mock.patch.object(vs, "_get_gemini_api_key", lambda: "k"),
        mock.patch.object(vs.urllib.request, "urlopen", fake),
        mock.patch.object(vs, "_cache_put_locked", lambda key, value: None),
        mock.patch.object(vs, "_load_embedding_cache", lambda: None),
    ]
    for p in patches:
        p.start()
    try:
        yield fake
    finally:
        for p in reversed(patches):
            p.stop()


def _arm_voyage_quiet() -> None:
    """No pending spacing or window wait, so only the pre-POST guard can bail."""
    vs._voyage_request_times[:] = []
    vs._voyage_last_request = 0.0


# (isolation cm, arm fn, embed call) per provider. The embed call goes straight
# to the low-level backend so the assertion is about that backend, not routing.
_PROVIDERS = {
    "voyage": (
        _voyage_isolated,
        _arm_voyage_quiet,
        lambda: vs._embed_uncached_voyage(["hello"]),
    ),
    "gemini": (
        _gemini_isolated,
        lambda: None,
        lambda: vs._embed_batch_gemini(["hello"]),
    ),
}


@pytest.mark.parametrize("provider", list(_PROVIDERS))
def test_expired_deadline_skips_post_for_either_provider(provider):
    """Load-bearing invariant: quota is charged at the POST, so an already-spent
    deadline must skip the request on whichever backend is selected -- otherwise
    a Voyage->Gemini switch drops deadline enforcement with no test to catch it.
    """
    isolated, arm, embed = _PROVIDERS[provider]
    with isolated() as fake:
        arm()
        with vs.embed_deadline(time.monotonic() - 1.0):  # already expired
            result = embed()
        calls = fake.calls

    assert result is None, f"{provider}: should abandon, not return embeddings"
    assert calls == 0, f"{provider}: fired a request for a caller that had left"


@pytest.mark.parametrize("provider", list(_PROVIDERS))
def test_far_deadline_still_posts_for_either_provider(provider):
    """A deadline the request comfortably beats must NOT trip a spurious bail;
    without this the guard above could pass by simply always returning None.
    """
    isolated, arm, embed = _PROVIDERS[provider]
    with isolated() as fake:
        arm()
        with vs.embed_deadline(time.monotonic() + 30.0):
            result = embed()
        calls = fake.calls

    assert result is not None and len(result) == 1, f"{provider}: dropped a result"
    assert calls == 1, f"{provider}: expected exactly one request"


def test_gemini_abandons_before_retry_backoff_sleep():
    """The Gemini analogue of the Voyage pre-sleep guards: after a 429 the worker
    must not sleep out its backoff once that sleep would overrun the deadline.
    """

    class _Raise429(_CountingUrlopenGemini):
        def __call__(self, req, timeout=None, context=None):
            with self._lock:
                self.calls += 1
            raise vs.urllib.error.HTTPError(
                url="http://x",
                code=429,
                msg="Too Many Requests",
                hdrs=None,
                fp=None,
            )

    fake = _Raise429()
    # A 30s backoff makes "did it sleep?" unambiguous; a deadline ~2s out is far
    # enough that the attempt-0 pre-POST guard passes (the POST fires and gets the
    # 429) yet close enough that the backoff would overrun it, so the retry guard
    # is the one under test.
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(vs, "_get_gemini_api_key", lambda: "k"))
        stack.enter_context(mock.patch.object(vs.urllib.request, "urlopen", fake))
        stack.enter_context(mock.patch.object(vs, "_GEMINI_BASE_BACKOFF", 30.0))
        started = time.monotonic()
        with vs.embed_deadline(time.monotonic() + 2.0):
            result = vs._embed_batch_gemini(["hello"])
        elapsed = time.monotonic() - started
        calls = fake.calls

    assert result is None
    assert calls == 1, "should POST once, hit 429, then bail before retrying"
    assert elapsed < 2.0, f"should bail before the 30s backoff, took {elapsed:.2f}s"


# ── The rerank leg: quota is spent at TWO API sites, not one ─────────────────
#
# ``search()`` calls ``_rerank_results`` -> ``_rerank_with_voyage`` -> a Voyage
# POST drawing on the SAME org-wide RPM budget as embeddings. The embed guards
# alone cannot enforce the invariant, because search() reaches rerank on paths
# they never see:
#   * a cache-hit embed (the common case in prod, 30MB disk cache) skips every
#     embed guard entirely, then reranks on the vector-only tier;
#   * when the embed guard DOES bail, search() falls back to BM25/TF-IDF and
#     still reranks -- re-spending the very request the embed guard just saved.
# On top of the POST, ``_voyage_try_reserve_slot`` consumes one of the shared
# rate-window slots BEFORE the request, so an unguarded abandoned worker also
# steals a slot from live callers. These tests pin all of it.


def _rerank_candidates() -> list[dict]:
    return [{"text": "hello world", "score": 1.0, "id": "d1", "metadata": {}}]


def _rerank_window_len() -> int:
    """Reservations recorded in the rerank model's cross-worker flock window.

    The rerank throttle stopped using the in-process ``_voyage_request_times``
    list when the rate windows were unified per-model (6e9e621) -- these tests
    must observe the store production actually reserves into, or the slot-theft
    assertions go vacuously green against the wrong structure.
    """
    _, path = vs._voyage_window_paths(vs._VOYAGE_WINDOW_RERANK)
    return len(vs._read_voyage_window(path))


def _embed_window_len() -> int:
    _, path = vs._voyage_window_paths(vs._VOYAGE_WINDOW_EMBED)
    return len(vs._read_voyage_window(path))


def test_rerank_expired_deadline_fires_no_post_and_reserves_no_slot():
    # _voyage_isolated points NOVA_SLOT_DIR at a private tmpdir, so the rerank
    # window starts empty; any entry after the call is a reservation we made.
    with _voyage_isolated() as fake:
        with vs.embed_deadline(time.monotonic() - 1.0):  # already expired
            result = vs._rerank_with_voyage(_rerank_candidates(), "hello", top_k=1)
        calls = fake.calls
        slots = _rerank_window_len()

    assert result is None, "should bail to keyword fallback, not rerank"
    assert calls == 0, "fired a rerank POST for a caller that had already left"
    assert slots == 0, "reserved a rerank-window slot for a discarded result"


def test_rerank_without_deadline_still_posts_and_reserves():
    """Non-bounded callers must keep today's behavior: reserve, POST, rank."""
    with _voyage_isolated() as fake:
        vs._rerank_with_voyage(_rerank_candidates(), "hello", top_k=1)
        calls = fake.calls
        slots = _rerank_window_len()

    assert calls == 1, "un-deadlined rerank should still issue its POST"
    assert slots == 1, "un-deadlined rerank should still reserve its rate slot"


def test_rerank_far_deadline_still_posts():
    """A live deadline must not trip a spurious bail -- the caller is waiting."""
    with _voyage_isolated() as fake:
        with vs.embed_deadline(time.monotonic() + 30.0):
            vs._rerank_with_voyage(_rerank_candidates(), "hello", top_k=1)
        calls = fake.calls

    assert calls == 1


def test_voyage_abandons_before_retry_backoff_sleep():
    """Voyage twin of the Gemini backoff test: after a 429 the worker must not
    sleep out its backoff once that sleep would overrun the deadline. The next
    acquire re-checks the deadline before reserving, so the sleep could never
    lead to a POST anyway -- it only delays the orphan's exit (pre-guard, by up
    to ~14s across the three attempts).
    """

    class _Raise429(_CountingUrlopen):
        def __call__(self, req, timeout=None, context=None):
            with self._lock:
                self.calls += 1
            raise vs.urllib.error.HTTPError(
                url="http://x",
                code=429,
                msg="Too Many Requests",
                hdrs=None,
                fp=None,
            )

    raiser = _Raise429()
    with _voyage_isolated():
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(vs.urllib.request, "urlopen", raiser))
            stack.enter_context(mock.patch.object(vs, "_VOYAGE_BASE_BACKOFF", 30.0))
            started = time.monotonic()
            # +2s: far enough that attempt 0's acquire and POST proceed (the
            # 429 comes from the stub), close enough that the 30s backoff
            # would overrun it -- so the backoff guard is the one under test.
            with vs.embed_deadline(time.monotonic() + 2.0):
                result = vs._embed_uncached_voyage(["hello"])
            elapsed = time.monotonic() - started

    assert result is None
    assert raiser.calls == 1, "should POST once, hit 429, then bail before retrying"
    assert elapsed < 2.0, f"should bail before the 30s backoff, took {elapsed:.2f}s"


def test_cached_embed_path_spends_no_rerank_quota_after_deadline():
    """The common prod case: embed hits the disk cache, so NO embed guard ever
    runs -- the vector-only tier then reranks. With the deadline already spent,
    the rerank guard is the only thing standing between an abandoned worker and
    a wasted request, and search() must still return results (keyword-overlap
    fallback), preserving the graceful-degradation contract.
    """
    with _voyage_isolated(index=_fake_index()) as fake:
        with contextlib.ExitStack() as stack:
            # Every lookup is a cache hit -> embed_batch never touches the API.
            stack.enter_context(
                mock.patch.object(vs, "_cache_get_locked", lambda key: [0.1] * 512)
            )
            # Force the vector-only tier deterministically (rerank site #1):
            # a fresh unbuilt BM25 index regardless of what earlier tests built.
            stack.enter_context(mock.patch.object(vs, "_bm25_index", vs.BM25Index()))
            with vs.embed_deadline(time.monotonic() - 1.0):  # already expired
                results = vs.search("hello world", top_k=3)
        calls = fake.calls
        slots = _rerank_window_len()

    assert calls == 0, "cache-hit path burned a rerank request past the deadline"
    assert slots == 0, "cache-hit path stole a rerank-window slot past the deadline"
    assert results, "guard must degrade to keyword fallback, not drop results"
    assert results[0].get("rerank_method") == "keyword_overlap_fallback"


@contextlib.contextmanager
def _tfidf_corpus(documents):
    """Build the real TF-IDF tier over ``documents``, restoring ALL its module
    state afterwards. ``_build_tfidf_index`` mutates six module globals in
    place (lists via .clear()+append, plus ``_tfidf_built``); leaving a fake
    corpus behind would poison every later test that falls through to the
    TF-IDF tier -- the exact cross-test pollution class behind the 4cac3116
    ship-gate hang.
    """
    idx = list(vs._tfidf_index)
    idf = dict(vs._tfidf_idf)
    texts = list(vs._tfidf_doc_texts)
    ids = list(vs._tfidf_doc_ids)
    meta = list(vs._tfidf_doc_meta)
    built = vs._tfidf_built
    try:
        vs._build_tfidf_index(documents)
        yield
    finally:
        with vs._tfidf_lock:
            vs._tfidf_index[:] = idx
            vs._tfidf_idf.clear()
            vs._tfidf_idf.update(idf)
            vs._tfidf_doc_texts[:] = texts
            vs._tfidf_doc_ids[:] = ids
            vs._tfidf_doc_meta[:] = meta
            vs._tfidf_built = built


def test_abandoned_bounded_search_spends_no_quota_on_any_tier():
    """End-to-end across BOTH API sites: with the deadline already spent, the
    worker must exit through embed guard -> TF-IDF fallback -> rerank guard
    (rerank site #2) with ZERO requests. Before the rerank guard existed, this
    path re-spent the request the embed guard saved. Counted after the worker
    is fully drained, so a late POST cannot hide behind the caller's return.

    The TF-IDF tier is built over the fake corpus so it genuinely yields
    results and the worker genuinely reaches rerank -- with an empty tier the
    test would pass vacuously without ever exercising the guard (caught by the
    red-proof run against unguarded code).
    """
    with _voyage_isolated(index=_fake_index()) as fake:
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(vs, "_bm25_index", vs.BM25Index()))
            stack.enter_context(_tfidf_corpus(_fake_index()))
            # timeout_s=0.0: the deadline is spent on arrival -- the abandoned-
            # worker end state, without racing real clocks in the test.
            vs.search_bounded(f"hello world {time.monotonic_ns()}", timeout_s=0.0)
            _drain_bounded_workers()  # let the orphan run to completion
            calls = fake.calls
            # Both per-model flock windows must stay empty: neither the embed
            # tier nor the rerank tier may reserve for an expired caller.
            slots = _embed_window_len() + _rerank_window_len()

    assert calls == 0, "abandoned worker spent Voyage quota on some tier"
    assert slots == 0, "abandoned worker reserved a rate slot on some tier"


# ── Ratchet: a NEW provider cannot ship without deadline coverage ────────────


def test_deadline_matrix_covers_every_registered_provider():
    """If a third embedding provider is ever added (EMBEDDING_PROVIDER_OPENAI,
    ...), this fails until it gets an entry in _PROVIDERS above -- forcing the
    new backend under the same expired-deadline/far-deadline tests instead of
    silently shipping without quota-guard coverage. That is the exact failure
    mode the voyage->gemini switch nearly had.
    """
    registered = {
        value
        for name, value in vars(vs).items()
        if name.startswith("EMBEDDING_PROVIDER_") and isinstance(value, str)
    }
    assert registered == set(_PROVIDERS), (
        f"providers registered in vector_search {sorted(registered)} != "
        f"providers under deadline test {sorted(_PROVIDERS)} -- add the missing "
        f"provider to _PROVIDERS with an isolation fixture, arm fn, and embed "
        f"call so the deadline invariant is enforced on its backend too"
    )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
