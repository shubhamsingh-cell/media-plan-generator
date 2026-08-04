"""Tests that rag_pipeline._Reranker.rerank shares vector_search's rerank window.

Background: rag_pipeline.py's ``_Reranker.rerank`` (Voyage ``rerank-2.5-lite``)
used to POST to the Voyage API with NO rate accounting at all -- the identical
bug already fixed in vector_search.py's ``_rerank_with_voyage`` (see
tests/test_voyage_rerank_rate_limit.py). Both modules call the SAME Voyage
model, and Voyage meters rerank-2.5-lite per account, not per caller, so the
two call sites must reserve from the ONE shared, cross-process, per-model
window (``vector_search._VOYAGE_WINDOW_RERANK``) or they can jointly over-send
against a budget that looks correct from either caller's own point of view.

Covers:
  * a rag_pipeline rerank call records exactly one reservation in the REAL
    shared rerank window file vector_search._voyage_window_paths() writes --
    proof this reserves from the same cross-process window vector_search.py
    uses, not an independent (or nonexistent) one;
  * a pre-seeded FULL rerank window makes rag_pipeline's reranker decline and
    fire ZERO Voyage API calls, falling back to the exact same value
    (``candidates[:top_k]``) the method already returns today when rerank is
    disabled/unavailable -- no new fallback shape invented;
  * the happy path (room in the window) is unaffected: rerank still POSTs
    and still reorders candidates by the mocked relevance scores.

All network is mocked at ``rag_pipeline._http_post_json`` -- no live API
calls. The reservation window file itself is the real vector_search code
under test and is never stubbed. NOVA_SLOT_DIR is pointed at a private
per-test tmpdir (the same isolation convention as
tests/test_voyage_rerank_rate_limit.py) so this suite never shares a window
with a concurrent pytest run or the live app.

Runs under pytest, or standalone:
``python3 tests/test_rag_pipeline_rerank_rate_limit.py``.
"""

import contextlib
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import rag_pipeline  # noqa: E402
import vector_search as vs  # noqa: E402


# ── Helpers ──────────────────────────────────────────────────────────────────


def _candidates(n: int = 2) -> list["rag_pipeline.RetrievalHit"]:
    """A handful of distinct RRF hits -- >= 2 so _Reranker's early-return
    guard (``len(candidates) <= 1``) never short-circuits before the rate
    check runs."""
    return [
        rag_pipeline.RetrievalHit(
            doc_id=f"d{i}",
            text=f"document body {i}",
            metadata={},
            score=1.0 / (i + 1),
            source_file="test.json",
            search_method="hybrid_rrf",
        )
        for i in range(n)
    ]


@contextlib.contextmanager
def _rerank_isolated(*, api_key: str = "test-key"):
    """Private NOVA_SLOT_DIR (own rerank window file) + a counting stub for
    rag_pipeline's own HTTP helper, so no real network call is possible and
    the shared window file is never polluted by/for another test or process.

    Yields:
        The ``mock.Mock`` standing in for ``rag_pipeline._http_post_json``.
    """
    slot_dir = tempfile.mkdtemp(prefix="nova_rag_rerank_test_")
    fake_post = mock.Mock(
        return_value={"data": [{"index": i, "relevance_score": 1.0 - i * 0.1}
                                for i in range(8)]}
    )
    env = {"NOVA_SLOT_DIR": slot_dir}
    if api_key:
        env["VOYAGE_API_KEY"] = api_key
    patches = [
        mock.patch.dict(os.environ, env, clear=False),
        mock.patch.object(rag_pipeline, "_http_post_json", fake_post),
    ]
    for p in patches:
        p.start()
    try:
        yield fake_post
    finally:
        for p in reversed(patches):
            p.stop()
        shutil.rmtree(slot_dir, ignore_errors=True)


def _seed_rerank_window(reservations) -> None:
    """Write wall-clock reservation times into the REAL shared rerank window
    file -- the same file vector_search._voyage_try_reserve_slot() reads."""
    _, path = vs._voyage_window_paths(vs._VOYAGE_WINDOW_RERANK)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(list(reservations), fh)


def _read_rerank_window() -> list:
    _, path = vs._voyage_window_paths(vs._VOYAGE_WINDOW_RERANK)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ── rag_pipeline's rerank reserves from vector_search's REAL shared window ──


def test_rerank_reserves_slot_in_vector_search_shared_window():
    """A rag_pipeline rerank call must record into the SAME cross-process
    rerank window file vector_search.py's own rerank caller uses."""
    with _rerank_isolated() as fake_post:
        reranker = rag_pipeline._Reranker(enabled=True)
        out = reranker.rerank("some query", _candidates(3), top_k=3)

        assert fake_post.call_count == 1, "rerank should have POSTed exactly once"
        assert out[0].search_method == "rerank"
        assert (
            len(_read_rerank_window()) == 1
        ), "rag_pipeline's rerank must reserve in vector_search's shared window file"


def test_rerank_declines_when_shared_window_full_fires_zero_calls():
    """A rerank window pre-saturated by ANY caller (here simulating
    vector_search.py having used up the budget) must make rag_pipeline's
    reranker decline before ever POSTing, falling back exactly as it already
    does for a disabled/unavailable reranker."""
    with _rerank_isolated() as fake_post:
        now = time.time()
        _seed_rerank_window([now] * vs._VOYAGE_RPM_LIMIT)

        candidates = _candidates(3)
        reranker = rag_pipeline._Reranker(enabled=True)
        out = reranker.rerank("some query", candidates, top_k=3)

        assert fake_post.call_count == 0, "must not POST when the shared window is full"
        # Identical fallback shape to the pre-existing "rerank unavailable"
        # path: compare directly against a disabled reranker's own output,
        # rather than re-deriving candidates[:top_k] a second time.
        disabled = rag_pipeline._Reranker(enabled=False)
        expected = disabled.rerank("some query", candidates, top_k=3)
        assert out == expected
        # Declined -- must NOT have appended an (N+1)th entry.
        assert len(_read_rerank_window()) == vs._VOYAGE_RPM_LIMIT


def test_rerank_still_works_when_window_has_room():
    """Sanity: the new gate must not break the happy path -- with room in the
    window, rerank still POSTs once and still reorders by relevance score."""
    with _rerank_isolated() as fake_post:
        candidates = _candidates(2)
        reranker = rag_pipeline._Reranker(enabled=True)
        out = reranker.rerank("some query", candidates, top_k=2)

        assert fake_post.call_count == 1
        assert [c.doc_id for c in out] == ["d0", "d1"]
        assert all(c.search_method == "rerank" for c in out)


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
