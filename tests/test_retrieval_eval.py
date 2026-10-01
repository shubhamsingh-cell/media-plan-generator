"""Tests for evals/retrieval_eval.py -- the offline retrieval-quality gate.

Covers: corpus parity with the production indexer, golden-set integrity, the
metric maths, the regression gate (3-point rule), the explicit SKIPPED rows for
tiers that need embeddings, the injected-embedder path (dense + hybrid tiers),
the real-Voyage embedder against a faked HTTP boundary, and floors for the
offline BM25 / TF-IDF tiers over the committed golden set.  No network, no keys.
"""

from __future__ import annotations

import io
import json
import sys
import time
import urllib.error
import zlib
from pathlib import Path
from typing import Any, List, Sequence
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "evals"))

import retrieval_eval as rev  # noqa: E402
import vector_search as vs  # noqa: E402

# Floors for the committed golden set (baseline minus ~8-10 points of slack so a
# KB refresh does not turn the suite red; the tight 3-point rule lives in
# test_committed_baseline_has_no_regression and in `--check`).
OFFLINE_FLOORS = {
    "bm25": {"recall@5": 70.0, "recall@10": 78.0, "mrr": 55.0},
    "tfidf": {"recall@5": 62.0, "recall@10": 68.0, "mrr": 45.0},
}
RUNTIME_BUDGET_S = 20.0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def corpus() -> List[rev.Chunk]:
    return rev.build_corpus()


@pytest.fixture(scope="module")
def offline_report() -> dict:
    """One full offline run (BM25 + TF-IDF + explicit dense SKIPs), timed."""
    started = time.monotonic()
    report = rev.run_eval(
        repo_cache_path=PROJECT_ROOT / "does_not_exist" / ".embedding_cache.json"
    )
    report["_wall_seconds"] = time.monotonic() - started
    return report


class HashingEmbedder(rev.Embedder):
    """Deterministic offline embedder: hashed bag-of-words (no model, no net)."""

    name = "test-hashing"

    def __init__(self, dim: int = 192) -> None:
        self._dim = dim
        self.calls: List[tuple] = []

    def embed(self, texts: Sequence[str], input_type: str) -> List[List[float]]:
        self.calls.append((len(texts), input_type))
        out: List[List[float]] = []
        for text in texts:
            vec = [0.0] * self._dim
            for tok in vs._tokenize(text):
                h = zlib.crc32(tok.encode("utf-8"))
                vec[h % self._dim] += 1.0 if (h >> 16) & 1 else -1.0
            out.append(vec)
        return out


def _write_kb(tmp_path: Path) -> Path:
    """A tiny KB dir with three well-separated chunks."""
    data = tmp_path / "data"
    data.mkdir()
    (data / "mini_kb.json").write_text(
        json.dumps(
            {
                "alpha_topic": {
                    "summary": "Welding apprenticeship pipelines for fabrication shops"
                },
                "beta_topic": {
                    "summary": "Nursing recruitment advertising on healthcare boards"
                },
                "gamma_topic": {
                    "summary": "Seasonal retail holiday hiring surge planning guide"
                },
            }
        ),
        encoding="utf-8",
    )
    return data


def _write_golden(path: Path, rows: List[dict]) -> Path:
    path.write_text(json.dumps({"rows": rows}), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Corpus
# ---------------------------------------------------------------------------


def test_stable_id_for_dict_and_leaf_chunks():
    dict_chunk = "[f.json] a.b\nkey: some long enough value here"
    leaf_chunk = "[f.json] a.b[3]: a leaf string that is definitely long"
    root_chunk = "[f.json] \nkey: some long enough value here"
    assert rev.stable_id_for(dict_chunk) == "f.json#a.b"
    # The leaf's VALUE must not leak into the id.
    assert rev.stable_id_for(leaf_chunk) == "f.json#a.b[3]"
    assert rev.stable_id_for(root_chunk) == "f.json#"
    assert rev.stable_id_for("no header at all") is None


def test_corpus_matches_production_indexer(corpus):
    """The harness must index exactly what index_knowledge_base() builds."""
    captured: List[dict] = []
    # index_knowledge_base() also flips the module's startup flag; patch it so
    # the real value is restored afterwards.
    with mock.patch.object(
        vs, "build_index", side_effect=lambda docs: captured.extend(docs)
    ), mock.patch.object(vs, "_is_startup_indexing", True):
        n = vs.index_knowledge_base()
    assert n == len(corpus) == len(captured)
    assert [c.doc_id for c in corpus] == [d["id"] for d in captured]
    assert [c.text for c in corpus] == [d["text"] for d in captured]


def test_stable_ids_are_nearly_unique(corpus):
    """A stable id that maps to several chunks would make recall ambiguous."""
    seen: dict = {}
    for c in corpus:
        seen[c.stable_id] = seen.get(c.stable_id, 0) + 1
    dupes = {k: v for k, v in seen.items() if v > 1}
    assert len(dupes) <= 5, f"too many ambiguous stable ids: {list(dupes)[:5]}"


# ---------------------------------------------------------------------------
# Golden set
# ---------------------------------------------------------------------------


def test_golden_set_integrity(corpus):
    rows = rev.load_golden()
    assert len(rows) >= 60
    assert len({r.row_id for r in rows}) == len(rows)
    assert len({r.query.lower() for r in rows}) == len(rows), "duplicate queries"
    for r in rows:
        assert r.expected_ids, r.row_id
        assert r.notes, f"{r.row_id}: notes are required"
    domains = {r.domain for r in rows}
    for needed in ("cpc_cpa", "salary", "channel", "seasonality"):
        assert needed in domains
    scored = rev.resolve_golden(rows, corpus)
    dead = [s.row.row_id for s in scored if not s.resolved_ids]
    assert not dead, f"golden rows with no resolvable expected id: {dead}"
    assert len([s for s in scored if s.resolved_ids]) >= 60


def test_load_golden_rejects_bad_rows(tmp_path):
    for bad in (
        {"rows": []},
        {"rows": [{"id": "x", "query": "", "expected_ids": ["a#b"]}]},
        {"rows": [{"id": "x", "query": "q", "expected_ids": []}]},
        {
            "rows": [
                {"id": "x", "query": "q", "expected_ids": ["a#b"]},
                {"id": "x", "query": "q2", "expected_ids": ["a#b"]},
            ]
        },
    ):
        with pytest.raises(ValueError):
            rev.load_golden(_write_golden(tmp_path / "g.json", bad["rows"]))


def test_stale_expected_ids_are_reported_not_silently_scored(tmp_path):
    data = _write_kb(tmp_path)
    golden = _write_golden(
        tmp_path / "g.json",
        [
            {
                "id": "a",
                "query": "welding apprenticeship",
                "expected_ids": ["mini_kb.json#alpha_topic", "mini_kb.json#gone"],
                "notes": "n",
            }
        ],
    )
    report = rev.run_eval(
        golden, data_dir=data, kb_files=["mini_kb.json"], tiers=["bm25"]
    )
    assert report["corpus"]["stale_expected_ids"] == ["mini_kb.json#gone"]
    assert report["tiers"]["bm25"]["metrics"]["recall@1"] == 100.0
    assert "WARNING" in rev.format_table(report)


# ---------------------------------------------------------------------------
# Metric maths
# ---------------------------------------------------------------------------


def test_recall_and_reciprocal_rank():
    ranked = ["x", "y", "z", "w"]
    assert rev.first_hit_rank(ranked, ["z", "q"]) == 3
    assert rev.first_hit_rank(ranked, ["nope"]) is None
    assert rev.recall_at_k(ranked, ["z"], 1) == 0.0
    assert rev.recall_at_k(ranked, ["z"], 3) == 1.0
    assert rev.reciprocal_rank(ranked, ["y"]) == 0.5
    assert rev.reciprocal_rank(ranked, ["w"], depth=3) == 0.0
    assert rev.reciprocal_rank([], ["w"]) == 0.0


def test_aggregate_metrics_are_percentage_points():
    recs = [
        {
            "recall@1": 1.0,
            "recall@3": 1.0,
            "recall@5": 1.0,
            "recall@10": 1.0,
            "rr": 1.0,
        },
        {
            "recall@1": 0.0,
            "recall@3": 0.0,
            "recall@5": 1.0,
            "recall@10": 1.0,
            "rr": 0.2,
        },
    ]
    agg = rev.aggregate_metrics(recs)
    assert agg == {
        "recall@1": 50.0,
        "recall@3": 50.0,
        "recall@5": 100.0,
        "recall@10": 100.0,
        "mrr": 60.0,
    }
    assert rev.aggregate_metrics([])["mrr"] == 0.0


# ---------------------------------------------------------------------------
# Regression gate (3-point rule)
# ---------------------------------------------------------------------------


def _rep(metrics: dict, status: str = "ok") -> dict:
    return {"tiers": {"bm25": {"status": status, "metrics": metrics}}}


def test_regression_exactly_three_points_passes_more_fails():
    base = _rep({"recall@5": 80.0, "mrr": 60.0})
    assert rev.check_regression(_rep({"recall@5": 77.0, "mrr": 60.0}), base) == []
    out = rev.check_regression(_rep({"recall@5": 76.99, "mrr": 60.0}), base)
    assert len(out) == 1 and out[0]["metric"] == "recall@5" and out[0]["drop"] == 3.01
    # An improvement never fails.
    assert rev.check_regression(_rep({"recall@5": 95.0, "mrr": 99.0}), base) == []


def test_regression_ignores_skipped_new_and_missing_baseline():
    base = {
        "tiers": {
            "bm25": {"status": "ok", "metrics": {"mrr": 60.0}},
            "vector": {"status": "ok", "metrics": {"mrr": 90.0}},
        }
    }
    cur = {
        "tiers": {
            "bm25": {"status": "ok", "metrics": {"mrr": 60.0}},
            "vector": {"status": "skipped", "reason": "SKIPPED: no key"},
            "tfidf": {"status": "ok", "metrics": {"mrr": 1.0}},  # not in baseline
        }
    }
    assert rev.check_regression(cur, base) == []
    assert rev.check_regression(cur, None) == []
    assert rev.check_regression(cur, {}) == []


def test_committed_baseline_has_no_regression(offline_report):
    """The real gate: current offline numbers vs the committed baseline."""
    baseline = json.loads(rev.DEFAULT_BASELINE_PATH.read_text(encoding="utf-8"))
    assert baseline["schema_version"] == rev.SCHEMA_VERSION
    violations = rev.check_regression(offline_report, baseline)
    assert not violations, (
        "retrieval regressed vs evals/retrieval_baseline.json -- if intentional, "
        "re-run `python3 evals/retrieval_eval.py --update-baseline`: "
        + "; ".join(v["message"] for v in violations)
    )


# ---------------------------------------------------------------------------
# Offline tiers over the committed golden set: floors + runtime
# ---------------------------------------------------------------------------


def test_offline_tiers_clear_floors(offline_report):
    for tier, floors in OFFLINE_FLOORS.items():
        result = offline_report["tiers"][tier]
        assert result["status"] == "ok", tier
        for metric, floor in floors.items():
            got = result["metrics"][metric]
            assert got >= floor, f"{tier} {metric}={got} below floor {floor}"
    # Recall must be monotone in k for every tier that ran.
    for tier in ("bm25", "tfidf"):
        m = offline_report["tiers"][tier]["metrics"]
        assert m["recall@1"] <= m["recall@3"] <= m["recall@5"] <= m["recall@10"]


def test_offline_run_is_fast(offline_report):
    assert offline_report["_wall_seconds"] < RUNTIME_BUDGET_S


def test_dense_tiers_are_skipped_explicitly_without_embedder(offline_report):
    for name in ("vector", "hybrid_rrf"):
        tier = offline_report["tiers"][name]
        assert tier["status"] == "skipped"
        assert tier["reason"].startswith(rev.SKIP_NO_KEY)
    table = rev.format_table(offline_report)
    assert table.count("SKIPPED: no key") == 2


def test_tfidf_tier_restores_module_state(corpus):
    sentinel_ids = ["keep-me"]
    with mock.patch.object(vs, "_tfidf_doc_ids", list(sentinel_ids)), mock.patch.object(
        vs, "_tfidf_built", False
    ):
        before = (list(vs._tfidf_doc_ids), vs._tfidf_built, len(vs._tfidf_idf))
        with rev.TfidfTier(corpus[:50]) as tier:
            assert vs._tfidf_built is True
            assert tier.search("welding", 3) is not None
        assert (list(vs._tfidf_doc_ids), vs._tfidf_built, len(vs._tfidf_idf)) == before


def test_worst_queries_carry_a_diagnosis(offline_report, corpus):
    scored = {s.row.row_id: s for s in rev.resolve_golden(rev.load_golden(), corpus)}
    worst = rev.worst_queries(offline_report, "bm25", scored, n=5)
    assert len(worst) == 5
    ranks = [w["first_hit_rank"] or 99 for w in worst]
    assert ranks == sorted(ranks, reverse=True)
    for w in worst:
        assert w["why"] and w["expected"]


# ---------------------------------------------------------------------------
# Dense + hybrid tiers via an injected embedder
# ---------------------------------------------------------------------------


def test_injected_embedder_runs_dense_and_hybrid_tiers():
    emb = HashingEmbedder()
    report = rev.run_eval(embedder=emb, tiers=["bm25", "vector", "hybrid_rrf"])
    assert report["embedder"] == "test-hashing"
    for name in ("vector", "hybrid_rrf"):
        assert report["tiers"][name]["status"] == "ok", name
        assert report["tiers"][name]["metrics"]["recall@10"] > 20.0
    # Documents are embedded as 'document', queries as 'query', each exactly once.
    kinds = [k for _, k in emb.calls]
    assert kinds.count("document") == 1 and kinds.count("query") == 1


def test_dense_index_python_fallback_matches_numpy():
    pytest.importorskip("numpy")
    vecs = HashingEmbedder(32).embed(
        ["welding trades", "nursing healthcare", "retail holiday", "tech hiring"],
        "document",
    )
    query = HashingEmbedder(32).embed(["healthcare nursing boards"], "query")[0]
    fast = rev._DenseIndex(vecs).top_k(query, 4)
    with mock.patch.dict(sys.modules, {"numpy": None}):
        slow_index = rev._DenseIndex(vecs)
        assert slow_index._np is None
        slow = slow_index.top_k(query, 4)
    assert fast == slow and fast[0] == 1


def test_embedder_failure_becomes_explicit_skip(tmp_path):
    class Boom(rev.Embedder):
        name = "boom"

        def embed(self, texts, input_type):
            raise rev.EmbedderUnavailable("SKIPPED: no key -- boom")

    data = _write_kb(tmp_path)
    golden = _write_golden(
        tmp_path / "g.json",
        [
            {
                "id": "a",
                "query": "welding",
                "expected_ids": ["mini_kb.json#alpha_topic"],
                "notes": "n",
            }
        ],
    )
    report = rev.run_eval(
        golden, data_dir=data, kb_files=["mini_kb.json"], embedder=Boom()
    )
    assert report["tiers"]["vector"]["status"] == "skipped"
    assert "boom" in report["tiers"]["vector"]["reason"]
    assert report["tiers"]["bm25"]["status"] == "ok"


# ---------------------------------------------------------------------------
# Repo-cache embedder: used ONLY with full coverage
# ---------------------------------------------------------------------------


def _cache_for(texts: List[str], model: str, path: Path, skip: int = -1) -> None:
    emb = HashingEmbedder(64)
    cache = {}
    for i, text in enumerate(texts):
        if i == skip:
            continue
        cache[rev._repo_cache_key(model, text[:8000], None)] = emb.embed([text], "x")[0]
    path.write_text(json.dumps(cache), encoding="utf-8")


def test_repo_cache_is_used_only_when_it_covers_everything(tmp_path):
    data = _write_kb(tmp_path)
    corpus_ = rev.build_corpus(data, ["mini_kb.json"])
    queries = ["welding apprenticeship", "nursing recruitment", "retail holiday surge"]
    golden = _write_golden(
        tmp_path / "g.json",
        [
            {
                "id": f"q{i}",
                "query": q,
                "expected_ids": [f"mini_kb.json#{t}"],
                "notes": "n",
            }
            for i, (q, t) in enumerate(
                zip(queries, ("alpha_topic", "beta_topic", "gamma_topic"))
            )
        ],
    )
    texts = [c.text for c in corpus_] + queries

    full = tmp_path / "full.json"
    _cache_for(texts, "voyage-3-lite", full)
    ok = rev.run_eval(
        golden, data_dir=data, kb_files=["mini_kb.json"], repo_cache_path=full
    )
    assert ok["embedder"] == "repo-cache:voyage-3-lite"
    assert ok["tiers"]["vector"]["status"] == "ok"
    assert ok["tiers"]["vector"]["metrics"]["recall@1"] == 100.0

    partial = tmp_path / "partial.json"
    _cache_for(texts, "voyage-3-lite", partial, skip=len(texts) - 1)  # drop 1 query
    skipped = rev.run_eval(
        golden, data_dir=data, kb_files=["mini_kb.json"], repo_cache_path=partial
    )
    reason = skipped["tiers"]["vector"]["reason"]
    assert skipped["tiers"]["vector"]["status"] == "skipped"
    assert reason.startswith(rev.SKIP_NO_KEY)
    assert "2/3 golden queries" in reason and "3/3 chunks" in reason


def test_repo_cache_key_matches_vector_search_key():
    """The harness's key formula must stay identical to the production one."""
    with mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-3-lite"), mock.patch.dict(
        "os.environ", {"EMBEDDING_PROVIDER": "voyage"}
    ):
        assert vs._text_cache_key("hello") == rev._repo_cache_key(
            "voyage-3-lite", "hello", None
        )
    with mock.patch.object(vs, "_VOYAGE_MODEL", "voyage-4-lite"), mock.patch.dict(
        "os.environ", {"EMBEDDING_PROVIDER": "voyage"}
    ):
        assert vs._text_cache_key("hello", "document") == rev._repo_cache_key(
            "voyage-4-lite", "hello", "document"
        )


# ---------------------------------------------------------------------------
# VoyageEmbedder against a faked HTTP boundary
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, payload: dict) -> None:
        self._b = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._b

    def __enter__(self) -> "_Resp":
        return self

    def __exit__(self, *a: Any) -> None:
        return None


class _FakeVoyage:
    """Fake urlopen: echoes one deterministic vector per input text."""

    def __init__(self, fail_429_times: int = 0) -> None:
        self.requests: List[dict] = []
        self.headers: List[dict] = []
        self.fail_429_times = fail_429_times

    def __call__(self, req, timeout=None, context=None):
        if self.fail_429_times > 0:
            self.fail_429_times -= 1
            raise urllib.error.HTTPError(
                req.full_url,
                429,
                "Too Many Requests",
                {"Retry-After": "1"},
                io.BytesIO(b""),
            )
        body = json.loads(req.data.decode("utf-8"))
        self.requests.append(body)
        self.headers.append(dict(req.header_items()))
        data = [
            {"index": i, "embedding": [float(len(t)), float(i), 1.0]}
            for i, t in enumerate(body["input"])
        ]
        # Return out of order to prove the index sort.
        return _Resp({"data": list(reversed(data))})


def _voyage(tmp_path: Path, fake: _FakeVoyage, **kw: Any) -> rev.VoyageEmbedder:
    sleeps: List[float] = []
    clock = {"t": 0.0}

    def _sleep(s: float) -> None:
        sleeps.append(s)
        clock["t"] += s

    emb = rev.VoyageEmbedder(
        model=kw.pop("model", "voyage-4-lite"),
        api_key=kw.pop("api_key", "test-key"),
        batch_size=kw.pop("batch_size", 2),
        rpm=kw.pop("rpm", 10),
        cache_path=tmp_path / "embed_cache.json",
        opener=fake,
        sleeper=_sleep,
        clock=lambda: clock["t"],
        **kw,
    )
    emb._sleeps = sleeps  # type: ignore[attr-defined]
    return emb


def test_voyage_embedder_payload_batching_pacing_and_cache(tmp_path):
    fake = _FakeVoyage()
    emb = _voyage(tmp_path, fake)
    texts = ["aaa", "bb", "c", "dddd", "ee"]
    vecs = emb.embed(texts, "document")
    assert [v[0] for v in vecs] == [3.0, 2.0, 1.0, 4.0, 2.0]  # order preserved
    # 5 texts, batch 2 -> 3 requests; payload shape matches the Voyage API.
    assert [len(r["input"]) for r in fake.requests] == [2, 2, 1]
    assert all(r["model"] == "voyage-4-lite" for r in fake.requests)
    assert all(r["input_type"] == "document" for r in fake.requests)
    assert fake.headers[0]["Authorization"] == "Bearer test-key"
    # 10 RPM -> >= 6s between requests (sleeps recorded by the fake clock).
    assert len(emb._sleeps) == 2 and all(s >= 6.0 - 1e-6 for s in emb._sleeps)
    # Second call is served entirely from the on-disk cache: no new requests.
    fake.requests.clear()
    again = _voyage(tmp_path, fake).embed(texts, "document")
    assert again == vecs and fake.requests == []
    # A different input_type is a different embedding space -> cache miss.
    _voyage(tmp_path, fake).embed(["aaa"], "query")
    assert fake.requests and fake.requests[-1]["input_type"] == "query"


def test_voyage_legacy_model_is_untyped_and_base_url_is_overridable(tmp_path):
    fake = _FakeVoyage()
    emb = _voyage(tmp_path, fake, model="voyage-3-lite")
    emb.embed(["x"], "query")
    assert "input_type" not in fake.requests[0]
    atlas = rev.VoyageEmbedder(
        api_key="k", base_url="https://ai.mongodb.com/v1/", cache_path=None
    )
    assert atlas._url == "https://ai.mongodb.com/v1/embeddings"


def test_voyage_embedder_retries_429_and_surfaces_missing_key(tmp_path):
    fake = _FakeVoyage(fail_429_times=2)
    emb = _voyage(tmp_path, fake)
    assert emb.embed(["x"], "query")  # succeeds on the 3rd attempt
    assert 1.0 in emb._sleeps  # the Retry-After header was honoured
    with mock.patch.dict("os.environ", {"VOYAGE_API_KEY": ""}):
        no_key = rev.VoyageEmbedder(cache_path=None)
        with pytest.raises(rev.EmbedderUnavailable, match="SKIPPED: no key"):
            no_key.embed(["x"], "query")


def test_voyage_embedder_http_error_is_unavailable_not_crash(tmp_path):
    def _deny(req, timeout=None, context=None):
        raise urllib.error.HTTPError(
            req.full_url, 403, "Forbidden", {}, io.BytesIO(b'{"detail":"nope"}')
        )

    emb = _voyage(tmp_path, _deny)  # type: ignore[arg-type]
    with pytest.raises(rev.EmbedderUnavailable, match="HTTP 403"):
        emb.embed(["x"], "document")


# ---------------------------------------------------------------------------
# CLI contract
# ---------------------------------------------------------------------------


def test_cli_update_then_check_roundtrip(tmp_path, capsys):
    base = tmp_path / "baseline.json"
    assert (
        rev.main(["--tiers", "bm25", "--update-baseline", "--baseline", str(base)]) == 0
    )
    written = json.loads(base.read_text(encoding="utf-8"))
    assert written["tiers"]["bm25"]["status"] == "ok"
    assert "per_query" not in written
    assert rev.main(["--tiers", "bm25", "--check", "--baseline", str(base)]) == 0

    # Make the committed baseline 4 points better than reality -> gate fails.
    for metric in written["tiers"]["bm25"]["metrics"]:
        written["tiers"]["bm25"]["metrics"][metric] += 4.0
    base.write_text(json.dumps(written), encoding="utf-8")
    assert rev.main(["--tiers", "bm25", "--check", "--baseline", str(base)]) == 1
    assert "RETRIEVAL GATE: FAIL" in capsys.readouterr().out


def test_cli_check_without_baseline_is_an_error(tmp_path):
    assert (
        rev.main(
            ["--tiers", "bm25", "--check", "--baseline", str(tmp_path / "nope.json")]
        )
        == rev.EXIT_ERROR
    )


def test_cli_audit_golden_passes_on_committed_set(capsys):
    assert rev.main(["--audit-golden"]) == 0
    assert "0 stale expected id(s)" in capsys.readouterr().out
