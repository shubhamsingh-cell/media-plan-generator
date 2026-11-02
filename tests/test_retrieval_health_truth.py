"""Pins what /api/deploy/ready says about retrieval, and that it is true.

Covers the boot-time retrieval warm-up (wsgi -> vector_search.warm_retrieval):
BM25/TF-IDF built pre-fork, the Qdrant tier armed by one read-only GET that also
runs the embedding-dimension guard, the per-worker attach bookkeeping and its
after-fork reset, the Qdrant-payload-first doc resolution and the id-drift guard
that make hybrid RRF safe now that BM25 exists in prod, the RAG v2 store's own
dim guard, and the embedding-space consistency between vector_search and
rag_pipeline.  No network: every Qdrant/Voyage boundary is faked.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import rag_pipeline as rp  # noqa: E402
import vector_search as vs  # noqa: E402


def _collection_info(size: int, distance: str = "Cosine") -> Dict[str, Any]:
    return {
        "result": {
            "points_count": 6227,
            "config": {"params": {"vectors": {"size": size, "distance": distance}}},
        }
    }


class _FakeQdrant:
    """Records (method, path, timeout); GET answers with a collection config."""

    def __init__(self, size: Optional[int] = None, down: bool = False) -> None:
        self.size = vs._active_vector_dim() if size is None else size
        self.down = down
        self.calls: List[tuple] = []

    def __call__(self, method, path, body=None, timeout=vs._QDRANT_TIMEOUT):
        self.calls.append((method, path, timeout))
        if self.down:
            return None
        if method == "GET":
            return _collection_info(self.size)
        if path.endswith("/points/count"):
            return {"result": {"count": 6227}}
        return None

    def gets(self) -> List[tuple]:
        return [c for c in self.calls if c[0] == "GET"]


@pytest.fixture
def clean_retrieval_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fresh, unbuilt local tiers and an unattached, configured Qdrant tier."""
    monkeypatch.setattr(vs, "_index", [])
    monkeypatch.setattr(vs, "_bm25_index", vs.BM25Index())
    for name in ("_tfidf_index", "_tfidf_doc_texts", "_tfidf_doc_ids", "_tfidf_doc_meta"):
        monkeypatch.setattr(vs, name, [])
    monkeypatch.setattr(vs, "_tfidf_idf", {})
    monkeypatch.setattr(vs, "_tfidf_built", False)
    monkeypatch.setattr(vs, "_keyword_indexes_built_by", None)
    monkeypatch.setattr(vs, "_keyword_indexes_error", None)
    monkeypatch.setattr(vs, "_hybrid_id_text_mismatches", 0)
    monkeypatch.setattr(vs, "_qdrant_available", False)
    monkeypatch.setattr(vs, "_qdrant_attach_last_attempt", 0.0)
    monkeypatch.setattr(vs, "_qdrant_attach_attempts", 0)
    monkeypatch.setattr(vs, "_qdrant_attach_pid", None)
    monkeypatch.setattr(vs, "_qdrant_attach_source", None)
    monkeypatch.setattr(vs, "_qdrant_dim_guard", None)
    monkeypatch.setattr(vs, "_last_qdrant_error", None)
    monkeypatch.setattr(vs, "_QDRANT_URL", "https://qdrant.invalid")
    monkeypatch.setattr(vs, "_QDRANT_API_KEY", "test-key")


# ---------------------------------------------------------------------------
# Boot-time warm-up
# ---------------------------------------------------------------------------


def test_warm_retrieval_builds_keyword_indexes_and_attaches_read_only(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeQdrant()
    monkeypatch.setattr(vs, "_qdrant_request", fake)

    summary = vs.warm_retrieval(attach_timeout=5)

    expected_docs = len(vs._collect_kb_documents())
    assert expected_docs > 1000
    assert summary["bm25_documents"] == expected_docs == vs._bm25_index.N
    assert vs._bm25_index.is_built and vs._tfidf_built
    assert summary["qdrant_attached"] is True and vs._qdrant_available is True
    # Exactly one GET of the active collection, with the short boot timeout.
    assert fake.calls == [("GET", f"/collections/{vs._active_collection()}", 5)]
    state = vs.qdrant_attach_state()
    assert state["attach_source"] == "warmup"
    assert state["attached_by_pid"] == os.getpid()
    assert vs.get_status()["search_mode"] == "hybrid_rrf"
    assert vs.get_status()["keyword_indexes_built_by"] == "warmup"


def test_warm_retrieval_survives_qdrant_down(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(vs, "_qdrant_request", _FakeQdrant(down=True))
    summary = vs.warm_retrieval()
    assert summary["qdrant_attached"] is False
    assert summary["bm25_documents"] > 0  # keyword fallback still built
    assert vs.get_status()["search_mode"] == "bm25"
    assert vs.qdrant_attach_state()["attach_attempted_in_this_worker"] is True


def test_warm_retrieval_dim_guard_refuses_at_boot_and_search_falls_back(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(vs, "_qdrant_request", _FakeQdrant(size=vs._active_vector_dim() // 2))
    summary = vs.warm_retrieval()
    assert summary["qdrant_attached"] is False
    guard = summary["dim_guard"]
    assert guard and guard["expected_dim"] == vs._active_vector_dim()
    assert guard["actual_dim"] == vs._active_vector_dim() // 2
    assert vs.qdrant_attach_state()["dim_guard_refused"] is True

    embed = mock.Mock(side_effect=AssertionError("must not embed for a refused tier"))
    monkeypatch.setattr(vs, "embed_text", embed)
    results = vs.search("LinkedIn sponsored jobs CPC", top_k=3)
    assert results and results[0]["search_method"] == "bm25"


def test_keyword_build_failure_is_reported_not_raised(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(vs, "_collect_kb_documents", mock.Mock(side_effect=OSError("disk")))
    assert vs.build_keyword_indexes() == 0
    assert "disk" in vs.get_status()["keyword_indexes_error"]
    assert vs._bm25_index.is_built is False


def test_wsgi_runs_retrieval_warm_up_before_gc_freeze() -> None:
    src = (PROJECT_ROOT / "wsgi.py").read_text("utf-8")
    body = src[src.index("def _run_deferred_startup") :]
    warm = body.index("warm_retrieval(")
    assert warm < body.index("_gc.freeze()"), "warm-up must run pre-fork, before freeze"


# ---------------------------------------------------------------------------
# Per-worker attach truth
# ---------------------------------------------------------------------------


def test_after_fork_resets_counters_and_keeps_inherited_attach(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(vs, "_qdrant_available", True)
    monkeypatch.setattr(vs, "_qdrant_attach_pid", os.getpid() + 1)  # "the master"
    monkeypatch.setattr(vs, "_qdrant_attach_source", "warmup")
    monkeypatch.setattr(vs, "_qdrant_attach_attempts", 3)
    monkeypatch.setattr(vs, "_qdrant_attach_last_attempt", 123.0)

    vs._after_fork_in_child()

    state = vs.qdrant_attach_state()
    assert vs._qdrant_attach_last_attempt == 0.0
    assert state["attempts_in_this_worker"] == 0
    assert state["attach_attempted_in_this_worker"] is False
    assert state["attached_in_this_worker"] is True
    assert state["inherited_from_parent"] is True
    assert state["attach_source"] == "warmup"


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork only")
def test_fork_hook_is_registered_for_real_forks(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(vs, "_qdrant_available", True)
    monkeypatch.setattr(vs, "_qdrant_attach_pid", os.getpid())
    monkeypatch.setattr(vs, "_qdrant_attach_attempts", 2)
    monkeypatch.setattr(vs, "_qdrant_attach_last_attempt", 99.0)
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:  # child: report and leave without running pytest teardown
        try:
            st = vs.qdrant_attach_state()
            payload = {
                "attempts": st["attempts_in_this_worker"],
                "last": vs._qdrant_attach_last_attempt,
                "inherited": st["inherited_from_parent"],
                "attached": st["attached_in_this_worker"],
            }
            os.write(w, json.dumps(payload).encode())
        finally:
            os._exit(0)
    os.close(w)
    data = b""
    while True:
        chunk = os.read(r, 4096)
        if not chunk:
            break
        data += chunk
    os.close(r)
    os.waitpid(pid, 0)
    child = json.loads(data)
    assert child == {"attempts": 0, "last": 0.0, "inherited": True, "attached": True}
    assert vs._qdrant_attach_attempts == 2  # parent untouched


class _Captured:
    payload: Dict[str, Any] = {}
    status: int = 0


def _probe(monkeypatch: pytest.MonkeyPatch) -> _Captured:
    import routes.health as rh

    cap = _Captured()

    def _send(handler, result, status_code=200):
        cap.payload = result
        cap.status = status_code

    monkeypatch.setattr(rh, "_send_json_response", _send)
    rh._handle_deploy_ready(handler=None, path="/api/deploy/ready", parsed=None)
    return cap


def test_deploy_ready_on_a_warmed_worker_reports_inherited_hybrid(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = _FakeQdrant()
    monkeypatch.setattr(vs, "_qdrant_request", fake)
    vs.warm_retrieval()
    # Pretend this request is served by a worker forked after that warm-up.
    monkeypatch.setattr(vs, "_qdrant_attach_pid", os.getpid() + 1)
    vs._after_fork_in_child()
    fake.calls.clear()

    retrieval = _probe(monkeypatch).payload["retrieval"]

    assert retrieval["qdrant_attached"] is True
    assert retrieval["bm25_built"] is True and retrieval["tfidf_built"] is True
    assert retrieval["search_mode"] == "hybrid_rrf"
    assert retrieval["can_retrieve"] is True
    attach = retrieval["qdrant_attach"]
    assert attach["attached_in_this_worker"] is True
    assert attach["inherited_from_parent"] is True
    assert attach["attach_source"] == "warmup"
    assert attach["attach_attempted_in_this_worker"] is False
    assert retrieval["keyword_indexes"]["built_by"] == "warmup"
    assert retrieval["keyword_indexes"]["error"] is None
    # The probe did not need its own attach GET on a warmed worker.
    assert fake.gets() == []


def test_deploy_ready_on_a_cold_worker_says_the_probe_attached_it(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(vs, "_qdrant_request", _FakeQdrant())
    retrieval = _probe(monkeypatch).payload["retrieval"]
    attach = retrieval["qdrant_attach"]
    assert attach["attach_source"] == "probe"
    assert attach["inherited_from_parent"] is False
    assert attach["attempts_in_this_worker"] == 1
    assert retrieval["bm25_built"] is False  # not warmed: reported as such
    assert retrieval["keyword_indexes"]["built_by"] is None


def test_deploy_ready_names_both_embedding_spaces(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(vs, "_qdrant_request", _FakeQdrant())
    emb = _probe(monkeypatch).payload["embedding"]
    assert emb["model"] == vs.get_active_embedding_model()
    assert emb["dim_guard"] is None
    rag = emb["rag_v2"]
    assert rag["model"] == rp._VOYAGE_EMBED_MODEL
    assert rag["collection"] == rp._QDRANT_COLLECTION != emb["collection"]
    assert rag["dim_guard"] is None


# ---------------------------------------------------------------------------
# Hybrid correctness once BM25 exists alongside Qdrant
# ---------------------------------------------------------------------------


def _bm25_with(docs: List[Dict[str, Any]]) -> vs.BM25Index:
    idx = vs.BM25Index()
    idx.index(docs)
    return idx


def test_resolve_doc_prefers_the_qdrant_payload(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        vs, "_bm25_index", _bm25_with([{"id": "f.json:1", "text": "LOCAL chunk text here", "metadata": {}}])
    )
    extra = {"f.json:1": {"id": "f.json:1", "text": "QDRANT chunk text here", "metadata": {}}}
    assert vs._resolve_doc("f.json:1", extra=extra)["text"].startswith("QDRANT")
    assert vs._resolve_doc("f.json:1")["text"].startswith("LOCAL")


def _hybrid_search(monkeypatch, local_text: str, qdrant_text: str) -> List[dict]:
    monkeypatch.setattr(
        vs,
        "_bm25_index",
        _bm25_with(
            [
                {"id": "f.json:1", "text": local_text, "metadata": {}},
                {"id": "f.json:2", "text": "unrelated filler about payroll", "metadata": {}},
            ]
        ),
    )
    monkeypatch.setattr(vs, "_qdrant_available", True)
    monkeypatch.setattr(vs, "embed_text", lambda q: [0.1] * vs._active_vector_dim())
    hit = {"id": "f.json:1", "text": qdrant_text, "metadata": {}, "score": 0.9}
    monkeypatch.setattr(vs, "_qdrant_search", lambda vec, top_k=5: [hit])
    return vs.search("nursing cpc benchmark", top_k=5)


def test_hybrid_fuses_ids_that_name_the_same_chunk(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    text = "[f.json] nursing\nnote: nursing cpc benchmark is high"
    results = _hybrid_search(monkeypatch, text, text)
    assert [r["id"] for r in results] == ["f.json:1"]
    assert results[0]["search_method"] == "hybrid_rrf"
    assert vs._hybrid_id_text_mismatches == 0


def test_hybrid_never_fuses_drifted_ids_naming_different_chunks(
    clean_retrieval_state: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    local = "[f.json] nursing\nnote: nursing cpc benchmark is high"
    remote = "[f.json] retail\nnote: retail seasonal hiring peaks in autumn"
    results = _hybrid_search(monkeypatch, local, remote)
    texts = [r["text"] for r in results]
    assert remote in texts and local in texts  # both kept, each with its own text
    assert vs._hybrid_id_text_mismatches == 1
    assert vs.get_status()["hybrid_id_text_mismatches"] == 1


# ---------------------------------------------------------------------------
# Embedding-space consistency: query vectors must be comparable to doc vectors
# ---------------------------------------------------------------------------

#: The model each collection's DOCUMENT vectors were built with. Changing a
#: pipeline's query-side model constant without re-embedding its collection
#: (a data migration) makes every query vector incomparable -- silently, at an
#: equal dimension. nova_knowledge_v2 was backfilled with voyage-3.5-lite
#: (docs/RAG_Phase2_Backfill_Report.md).
COLLECTION_MODEL = {"nova_knowledge_v2": "voyage-3.5-lite"}


def test_rag_pipeline_queries_its_collection_with_the_model_that_built_it() -> None:
    assert rp._VOYAGE_EMBED_MODEL == COLLECTION_MODEL[rp._QDRANT_COLLECTION]
    assert rp.VoyageRESTEmbedder.dim == rp._VOYAGE_EMBED_DIM


def test_rag_pipeline_documents_and_queries_use_one_model_constant() -> None:
    sent: List[str] = []

    def _post(url, payload, headers, **_kw):
        sent.append(payload["model"])
        return {"data": [{"index": 0, "embedding": [0.0] * rp._VOYAGE_EMBED_DIM}]}

    emb = rp.VoyageRESTEmbedder(api_key="test")
    with mock.patch.object(rp, "_http_post_json", _post):
        emb.embed_batch(["doc"])
        emb.embed_query("query")
    assert sent == [rp._VOYAGE_EMBED_MODEL, rp._VOYAGE_EMBED_MODEL]


def test_vector_search_collection_is_scoped_to_its_model_and_dim() -> None:
    model = vs.get_active_embedding_model()
    coll = vs._active_collection()
    if model != "voyage-3-lite":  # the legacy bare-name rollback space
        assert coll.endswith(f"__{model}_{vs._active_vector_dim()}")


def test_the_two_pipelines_share_a_collection_only_with_the_same_model() -> None:
    """vector_search and rag_pipeline resolve DIFFERENT model constants today
    (voyage-4-lite vs voyage-3.5-lite); that is safe only because they read
    different collections.  If they ever point at one collection, the models
    must be identical or one side's queries are incomparable to the vectors.
    """
    same_collection = rp._QDRANT_COLLECTION == vs._active_collection()
    if same_collection:
        assert rp._VOYAGE_EMBED_MODEL == vs.get_active_embedding_model()
    else:
        assert rp._QDRANT_COLLECTION != vs._active_collection()
    # Pin today's state so a change to either side is a deliberate edit here.
    assert not same_collection
    assert vs.get_active_embedding_model() == "voyage-4-lite"


# ---------------------------------------------------------------------------
# RAG v2 store dim guard
# ---------------------------------------------------------------------------


@pytest.fixture
def rag_guard_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rp, "_rag_dim_guard", None)


def test_rag_store_refuses_a_wrong_dimension_collection(
    rag_guard_reset: None,
) -> None:
    store = rp.QdrantRESTStore(url="https://qdrant.invalid", api_key="k", dim=128)
    gets: List[str] = []

    def _get(url, headers, method="GET", **_kw):
        gets.append(method)
        return _collection_info(1024)

    post = mock.Mock(side_effect=AssertionError("must not search a refused collection"))
    with mock.patch.object(rp, "_http_json", _get), mock.patch.object(rp, "_http_post_json", post):
        for _ in range(2):
            with pytest.raises(rp._RestError, match="dim guard"):
                store.search([0.0] * 128, 5)
        with pytest.raises(rp._RestError, match="dim guard"):
            store.search_payload([0.0] * 128, 5)
    assert gets == ["GET"]  # verified once, refusal is sticky
    assert rp._rag_dim_guard["expected_dim"] == 128
    assert rp._rag_dim_guard["actual_dim"] == 1024


def test_rag_store_searches_a_matching_collection(rag_guard_reset: None) -> None:
    store = rp.QdrantRESTStore(url="https://qdrant.invalid", api_key="k", dim=1024)
    with mock.patch.object(rp, "_http_json", return_value=_collection_info(1024)), mock.patch.object(
        rp, "_http_post_json", return_value={"result": {"points": [{"id": "a", "score": 0.5}]}}
    ):
        assert store.search([0.0] * 1024, 5) == [("a", 0.5)]
    assert rp._rag_dim_guard is None


def test_rag_retrieve_degrades_to_bm25_when_guard_refuses(rag_guard_reset: None) -> None:
    store = rp.QdrantRESTStore(url="https://qdrant.invalid", api_key="k", dim=128)
    pipe = rp.NovaRAGPipeline(
        embedder=rp.HashEmbedder(), store=store, rerank_enabled=False
    )
    doc = rp.Document(doc_id="d1", text="nursing cost per application benchmark", metadata={})
    pipe._docs_by_id[doc.doc_id] = doc
    pipe.bm25.index([doc])
    with mock.patch.object(rp, "_http_json", return_value=_collection_info(1024)):
        hits = pipe.retrieve("nursing cost per application", top_k=3)
    assert [h.doc_id for h in hits] == ["d1"]
    assert rp._rag_dim_guard is not None
