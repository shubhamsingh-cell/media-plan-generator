"""Tests for the lazy per-process Qdrant attach in vector_search.search().

gunicorn --preload forks workers from the master AFTER wsgi.py's startup
sequence has already run there; module globals (including _qdrant_available)
do not cross fork(), and under gevent monkey-patching the preload background
thread does not simply die at fork either -- it re-runs redundantly in the
master and every worker (see wsgi.py's NOVA_BUILD_LOCAL_INDEX gate). Either
way, a request-serving worker's _index/BM25/TF-IDF are effectively never
populated in production. Before this fix, search() had no path to Qdrant's
already-populated, process-shared collection: _get_vector_results discarded
Qdrant's own payload and _resolve_doc had nowhere to look a Qdrant-sourced
doc_id up, so search() returned [] even with _qdrant_available flipped True
by hand -- a silent, invisible outage of the entire retrieval layer.

This file was the first test coverage of vector_search's startup/retrieval
path in production shape; no prior test imported wsgi or called
build_index()/index_knowledge_base().

No live network calls: _qdrant_request/_qdrant_search/embed_text are stubbed
in every test. tests/conftest.py's external-network guard would block a real
attempt anyway (non-loopback connect raises ConnectionRefusedError).

Runs under pytest, or standalone: ``python3 tests/test_worker_retrieval.py``.
"""

import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import vector_search as vs  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────────────


class _CapturingHandler:
    """Minimal handler stand-in capturing _send_json_response payloads."""

    def __init__(self):
        self.payload = None
        self.status = None


def _empty_local_tiers():
    """Patch _index/BM25/TF-IDF to a fresh, unbuilt, empty state.

    Mirrors a forked gunicorn worker that never ran build_index(): the
    exact state this fix's Qdrant-attach path has to work without.
    """
    return mock.patch.multiple(
        vs,
        _index=[],
        _bm25_index=vs.BM25Index(),
        _tfidf_built=False,
    )


# ── THE regression pin ────────────────────────────────────────────────────────


def test_search_resolves_qdrant_hits_with_all_local_indexes_empty():
    """search() must return real, grounded results when Qdrant already holds
    the answer, even though _index/BM25/TF-IDF are all empty -- exactly a
    forked worker's shape. _qdrant_available is set directly (True) to
    isolate this test from the attach mechanism itself (covered separately
    below); embed_text and _qdrant_search are stubbed to return a
    payload-bearing hit.

    Confirmed by hand: this test FAILS (returns []) against the pre-fix
    _get_vector_results/_resolve_doc (which discarded Qdrant's payload and
    had no fourth tier to resolve a Qdrant-sourced doc_id against), and
    PASSES after the payload_sink/extra plumbing in this change.
    """
    fake_hits = [
        {
            "id": "channels_db.json:42",
            "text": "LinkedIn CPC benchmarks for recruitment average $3.50.",
            "metadata": {"source": "channels_db.json"},
            "score": 0.91,
        }
    ]
    with _empty_local_tiers(), mock.patch.object(
        vs, "_qdrant_available", True
    ), mock.patch.object(vs, "embed_text", return_value=[0.1] * 512), mock.patch.object(
        vs, "_qdrant_search", return_value=fake_hits
    ):
        results = vs.search("linkedin cpc benchmarks", top_k=5)

    assert results, "search() must not return [] when Qdrant already holds the answer"
    assert results[0]["id"] == "channels_db.json:42"
    assert "LinkedIn CPC" in results[0]["text"]
    assert results[0]["search_method"] == "vector"


# ── _qdrant_attach() ─────────────────────────────────────────────────────────


def test_qdrant_attach_is_read_only_and_idempotent():
    """_qdrant_attach() must issue exactly one GET -- never a PUT/DDL call --
    and become a no-op once armed. A serving worker must never create a
    collection, only verify one already exists (unlike
    _qdrant_ensure_collection, which this function deliberately avoids).
    """
    calls = []

    def _fake_request(method, path, body=None, timeout=vs._QDRANT_TIMEOUT):
        calls.append(method)
        return {"result": {"status": "green"}}

    with mock.patch.object(vs, "_qdrant_available", False), mock.patch.object(
        vs, "_qdrant_attach_last_attempt", 0.0
    ), mock.patch.object(vs, "_QDRANT_URL", "https://fake.qdrant"), mock.patch.object(
        vs, "_QDRANT_API_KEY", "fake-key"
    ), mock.patch.object(
        vs, "_qdrant_request", side_effect=_fake_request
    ):
        first = vs._qdrant_attach()
        second = vs._qdrant_attach()

    assert first is True
    assert second is True
    assert calls == ["GET"]


def test_qdrant_attach_cooldown_bounds_retries():
    """A persistently-down Qdrant must not be hammered: one HTTP attempt per
    _QDRANT_ATTACH_COOLDOWN_S window, not one per search() call.
    """
    calls = []

    def _fake_request(method, path, body=None, timeout=vs._QDRANT_TIMEOUT):
        calls.append(method)
        return None  # simulated failure

    with mock.patch.object(vs, "_qdrant_available", False), mock.patch.object(
        vs, "_qdrant_attach_last_attempt", 0.0
    ), mock.patch.object(vs, "_QDRANT_URL", "https://fake.qdrant"), mock.patch.object(
        vs, "_QDRANT_API_KEY", "fake-key"
    ), mock.patch.object(
        vs, "_qdrant_request", side_effect=_fake_request
    ), mock.patch.object(
        vs.time, "monotonic", return_value=1000.0
    ):
        for _ in range(50):
            assert vs._qdrant_attach() is False

    assert len(calls) == 1


# ── Duplicate-point dedupe ────────────────────────────────────────────────────


def test_qdrant_duplicates_are_deduped():
    """Qdrant currently holds ~86x duplicate points per doc_id (unseeded
    hash-based point ids in build_index -- see the sharedstore investigation).
    _get_vector_results must over-fetch and dedupe so a caller doesn't get N
    copies of the same chunk instead of top_k distinct documents.
    """
    # 20 raw hits over 2 distinct doc_ids, best-scoring occurrence first per
    # id -- mirrors real Qdrant nearest-neighbor ordering.
    raw_hits = [
        {
            "id": "doc:1",
            "text": f"chunk one copy {i}",
            "metadata": {},
            "score": 0.9 - i * 0.001,
        }
        for i in range(10)
    ] + [
        {
            "id": "doc:2",
            "text": f"chunk two copy {i}",
            "metadata": {},
            "score": 0.8 - i * 0.001,
        }
        for i in range(10)
    ]

    with mock.patch.object(vs, "_qdrant_available", True), mock.patch.object(
        vs, "_qdrant_search", return_value=raw_hits
    ):
        sink: dict = {}
        results = vs._get_vector_results(
            "query", [0.1] * 512, fetch_k=5, payload_sink=sink
        )

    ids = [doc_id for doc_id, _ in results]
    assert len(ids) == len(set(ids)), "must not return duplicate doc_ids"
    assert len(ids) <= 5
    assert ids[0] == "doc:1"  # highest-scoring occurrence kept, not last-seen
    assert sink["doc:1"]["text"] == "chunk one copy 0"


# ── /api/deploy/ready observability ──────────────────────────────────────────


def test_deploy_ready_reports_retrieval_without_gating_status():
    """The new `retrieval` block on /api/deploy/ready must be purely
    additive: `ready`, `checks`, and the HTTP status must be identical
    whether the retrieval layer is fully dead or fully attached -- driven
    only by warmup+KB state, never by whether search() would currently
    return grounding. Gating readiness on a third party (Qdrant) risks
    failing every deploy while this very bug is live.
    """
    import routes.health as rh

    def _run(qdrant_available):
        captured = _CapturingHandler()

        def _fake_send(handler, result, status_code=200):
            captured.payload = result
            captured.status = status_code

        with mock.patch.object(
            rh, "_send_json_response", _fake_send
        ), _empty_local_tiers(), mock.patch.object(
            vs, "_qdrant_available", qdrant_available
        ):
            rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)
        return captured

    dead = _run(qdrant_available=False)
    alive = _run(qdrant_available=True)

    assert dead.payload["retrieval"]["can_retrieve"] is False
    assert alive.payload["retrieval"]["can_retrieve"] is True
    assert dead.payload["retrieval"]["qdrant_attached"] is False
    assert alive.payload["retrieval"]["qdrant_attached"] is True

    # Readiness/status must not move with retrieval health.
    assert dead.payload["ready"] == alive.payload["ready"]
    assert dead.payload["checks"] == alive.payload["checks"]
    assert dead.status == alive.status
    assert "retrieval" not in dead.payload["checks"]


# ── Once-per-process dead-retrieval log ──────────────────────────────────────


def test_retrieval_dead_logs_once_not_per_query():
    """_warn_retrieval_dead() must fire exactly once per process, not once
    per query -- this is the signal that was missing for the ~4 months this
    bug was live; it must not become log spam on a hot, fully-degraded path.
    """
    with _empty_local_tiers(), mock.patch.object(
        vs, "_qdrant_available", False
    ), mock.patch.object(vs, "_QDRANT_URL", ""), mock.patch.object(
        vs, "_retrieval_dead_logged", False
    ), mock.patch.object(
        vs, "embed_text", return_value=None
    ), mock.patch.object(
        vs.logger, "error"
    ) as mock_error:
        for _ in range(5):
            assert vs.search("anything", top_k=5) == []

    assert mock_error.call_count == 1
    assert "RETRIEVAL LAYER EMPTY" in mock_error.call_args[0][0]


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
