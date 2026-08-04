"""Tests that build_index makes BM25/TF-IDF queryable BEFORE embedding runs.

Background (F5, 2026-08-04): build_index used to build the BM25 + TF-IDF
keyword indexes only at the END of the function (or in the
all-embeddings-failed branch), AFTER the full embedding loop. On a
cache-cold startup -- e.g. the voyage-4-lite model succession, where the
disk cache holds zero entries for the new model+input_type namespace -- that
embedding loop can run for several minutes. Until it finished, search() had
no vector index (empty), no BM25 (not yet built), and no TF-IDF (also not
yet built) to answer from, so it returned [] -- a TOTAL retrieval blackout
on a live product, not a degraded one.

The fix builds BM25 + TF-IDF from flat_docs (text only, no vectors needed)
BEFORE the embedding loop starts. This test proves it by installing a fake
embed_batch that -- while build_index is still "inside" its embedding call,
i.e. embeddings are still pending -- asserts the BM25 index is already built
and search() already returns real keyword hits, not [].

No live network: embed_batch is replaced with a synchronous fake, so no
urlopen ever happens, and the tracked ~30MB data/.embedding_cache.json is
never touched (build_index's embedding path is short-circuited by the fake
before it would reach the cache).
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import vector_search as vs  # noqa: E402


def _reset_indexes():
    """Clear every index build_index touches, so this test starts from a
    known-empty state regardless of what an earlier test in the run built."""
    with vs._index_lock:
        vs._index.clear()
    vs._index_built = False
    vs._bm25_index.__init__()  # cheapest full reset of all instance state
    with vs._tfidf_lock:
        vs._tfidf_doc_texts.clear()
        vs._tfidf_doc_ids.clear()
        vs._tfidf_doc_meta.clear()
        vs._tfidf_index.clear()
        vs._tfidf_idf.clear()
    vs._tfidf_built = False


def test_bm25_is_queryable_while_embeddings_are_still_pending():
    """The core F5 assertion: from INSIDE the (still-running) embed_batch
    call build_index makes, BM25 must already be built and search() must
    already return a real keyword hit -- not an empty list."""
    _reset_indexes()

    doc_text = "The zorblatt quarterly benchmark covers CPC by channel."
    documents = [{"id": "kb_doc:1", "text": doc_text, "metadata": {"source": "kb"}}]

    checked_while_pending = {"ran": False}

    def _fake_embed_batch(texts, input_type=None):
        # This runs DURING build_index, before it has returned -- i.e.
        # embeddings are still "pending" from build_index's own point of
        # view. At this point BM25 must already be queryable.
        assert vs._bm25_index.is_built, (
            "BM25 index must be built BEFORE embed_batch is ever called, "
            "not after -- that ordering is the whole fix."
        )
        results = vs.search("zorblatt benchmark", top_k=5)
        assert results, "search() returned [] while embeddings were pending"
        assert any(r["id"] == "kb_doc:1" for r in results)
        assert any(r.get("search_method") == "bm25" for r in results)
        checked_while_pending["ran"] = True

        # Let build_index finish normally afterwards.
        return [[0.1] * 8 for _ in texts]

    with mock.patch.object(
        vs, "embed_batch", side_effect=_fake_embed_batch
    ), mock.patch.object(vs, "_qdrant_is_configured", return_value=False):
        vs.build_index(documents)

    assert checked_while_pending["ran"], (
        "the pending-state assertion never ran -- build_index didn't call "
        "embed_batch, so this test would vacuously pass without it"
    )

    # And after build_index finishes normally, search() must still work.
    final_results = vs.search("zorblatt benchmark", top_k=5)
    assert final_results


def test_bm25_stays_queryable_when_embedding_fails_entirely():
    """The all-embeddings-failed branch must not regress: BM25/TF-IDF must
    still be queryable (it was already built up front), and build_index must
    not attempt to rebuild it a second time."""
    _reset_indexes()

    doc_text = "The zorblatt quarterly benchmark covers CPC by channel."
    documents = [{"id": "kb_doc:1", "text": doc_text, "metadata": {"source": "kb"}}]

    with mock.patch.object(vs, "embed_batch", return_value=None), mock.patch.object(
        vs, "_qdrant_is_configured", return_value=False
    ):
        vs.build_index(documents)

    assert vs._bm25_index.is_built
    results = vs.search("zorblatt benchmark", top_k=5)
    assert results
    assert any(r["id"] == "kb_doc:1" for r in results)


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
