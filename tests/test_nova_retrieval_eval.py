"""Tests for evals/nova_retrieval_eval.py -- the Nova retrieval regression gate.

Offline and fast: the local BM25 / TF-IDF tiers run over the real KB; the live
Qdrant tiers run against a faked ``vector_search._qdrant_request`` boundary (the
suite's socket guard forbids real network).  The ``live`` test at the bottom
probes the real collection and auto-skips without keys.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "evals"))

import nova_retrieval_eval as nre  # noqa: E402
import retrieval_eval as rev  # noqa: E402
import vector_search as vs  # noqa: E402
from tests.timing_budget import CpuTimer  # noqa: E402

# Committed baseline minus slack, so a KB refresh does not turn the suite red;
# the tight 3-point recall@10 rule lives in the --check gate and in
# test_committed_baseline_has_no_regression.
OFFLINE_FLOORS = {
    "local_bm25": {"recall@5": 82.0, "recall@10": 90.0, "mrr": 65.0},
    "local_tfidf": {"recall@5": 82.0, "recall@10": 88.0, "mrr": 62.0},
}
#: CPU seconds for one offline run (~1 s idle); catches a 10x regression.
RUNTIME_BUDGET_S = 15.0

REQUIRED_DOMAINS = {
    "cpc_cpa_country",
    "cpc_cpa_industry",
    "job_boards_country",
    "report_facts",
    "plan_figures",
    "out_of_scope",
}


@pytest.fixture(scope="module")
def corpus() -> List[rev.Chunk]:
    return rev.build_corpus()


@pytest.fixture(scope="module")
def offline_report() -> Dict[str, Any]:
    with CpuTimer() as timer:
        report = nre.run_eval(qdrant="off")
    report["_cpu"] = timer.cpu
    report["_timer"] = timer.describe()
    return report


# ---------------------------------------------------------------------------
# Query set integrity
# ---------------------------------------------------------------------------


def test_query_set_shape() -> None:
    queries = nre.load_queries()
    assert 60 <= len(queries) <= 100
    assert REQUIRED_DOMAINS <= {q.domain for q in queries}
    oos = [q for q in queries if q.out_of_scope]
    assert len(oos) >= 5
    assert all(not q.expected_ids and not q.expected_prefixes for q in oos)
    assert all(q.expected_ids or q.expected_prefixes for q in queries if not q.out_of_scope)


def test_every_label_resolves_in_the_current_kb(corpus: List[rev.Chunk]) -> None:
    stale = nre.stale_labels(nre.load_queries(), [c.stable_id for c in corpus])
    assert stale == [], f"labels no longer in the KB: {stale}"


def test_query_set_names_no_client() -> None:
    """The repo is public: no client identifier may appear in the query set.

    Client identifiers are derived from the KB's own plan keys (never typed
    here, which would commit them).
    """
    plans = json.loads(
        (PROJECT_ROOT / "data" / "client_media_plans_kb.json").read_text("utf-8")
    )["plans"]
    tokens = {k.split("_")[0].lower() for k in plans if len(k.split("_")[0]) >= 3}
    assert tokens, "expected client plan keys to derive identifiers from"
    text = nre.DEFAULT_QUERIES_PATH.read_text("utf-8").lower()
    leaked = sorted(t for t in tokens if re.search(rf"\b{re.escape(t)}\b", text))
    assert leaked == []
    assert "client_media_plans_kb.json" not in text


@pytest.mark.parametrize(
    "bad,match",
    [
        ({"rows": [{"id": "a", "query": "q"}]}, "needs expected"),
        (
            {"rows": [{"id": "a", "query": "q", "out_of_scope": True, "expected_ids": ["x"]}]},
            "no labels",
        ),
        ({"rows": [{"id": "a", "query": "", "expected_ids": ["x"]}]}, "empty query"),
        (
            {"rows": [{"id": "a", "query": "q", "expected_ids": ["x"]}] * 2},
            "duplicate",
        ),
        ({"rows": []}, "no rows"),
    ],
)
def test_load_queries_rejects_bad_rows(tmp_path: Path, bad: dict, match: str) -> None:
    path = tmp_path / "q.json"
    path.write_text(json.dumps(bad), encoding="utf-8")
    with pytest.raises(ValueError, match=match):
        nre.load_queries(path)


# ---------------------------------------------------------------------------
# Metric maths
# ---------------------------------------------------------------------------


def _q(ids: Sequence[str] = (), prefixes: Sequence[str] = (), oos: bool = False):
    return nre.Query("t", "d", "query", tuple(ids), tuple(prefixes), oos, "kb")


def test_prefix_matching_is_segment_aware() -> None:
    assert nre.matches_prefix("f.json#a.b", "f.json#a.b")
    assert nre.matches_prefix("f.json#a.b.c", "f.json#a.b")
    assert nre.matches_prefix("f.json#a.b[2]", "f.json#a.b")
    assert not nre.matches_prefix("f.json#a.bc", "f.json#a.b")


def test_score_query_rank_recall_and_mrr() -> None:
    q = _q(prefixes=["f#hit"])
    ranked = [(f"f#miss{i}", 1.0) for i in range(6)] + [("f#hit.x", 0.5)]
    rec = nre.score_query(q, ranked)
    assert rec["first_hit_rank"] == 7
    assert rec["recall@5"] == 0.0 and rec["recall@10"] == 1.0
    assert rec["rr"] == pytest.approx(1 / 7)
    assert nre.score_query(q, [("f#nope", 1.0)])["first_hit_rank"] is None


def test_aggregate_excludes_out_of_scope() -> None:
    hit = nre.score_query(_q(ids=["a"]), [("a", 1.0)])
    oos = nre.score_query(_q(oos=True), [("a", 1.0)])
    assert nre.aggregate([hit, oos]) == {"recall@5": 100.0, "recall@10": 100.0, "mrr": 100.0}


def _tiers(**recall10: Optional[float]) -> Dict[str, Any]:
    return {
        "tiers": {
            name: (
                {"status": "ok", "metrics": {"recall@10": val}}
                if val is not None
                else {"status": "skipped"}
            )
            for name, val in recall10.items()
        }
    }


def test_gate_allows_exactly_three_points_and_fails_beyond() -> None:
    base = _tiers(local_bm25=90.0)
    assert nre.check_regression(_tiers(local_bm25=87.0), base) == []
    failed = nre.check_regression(_tiers(local_bm25=86.99), base)
    assert len(failed) == 1 and "local_bm25 recall@10" in failed[0]


def test_gate_ignores_tiers_not_ok_on_both_sides() -> None:
    base = _tiers(local_bm25=90.0, qdrant_vector=None)
    cur = _tiers(local_bm25=None, qdrant_vector=10.0)
    assert nre.check_regression(cur, base) == []
    assert nre.check_regression(cur, None) == []


# ---------------------------------------------------------------------------
# Offline run over the real KB
# ---------------------------------------------------------------------------


def test_offline_run_is_fast(offline_report: Dict[str, Any]) -> None:
    assert offline_report["_cpu"] < RUNTIME_BUDGET_S, offline_report["_timer"]


@pytest.mark.parametrize("tier", sorted(OFFLINE_FLOORS))
def test_offline_tiers_clear_floors(offline_report: Dict[str, Any], tier: str) -> None:
    result = offline_report["tiers"][tier]
    assert result["status"] == "ok"
    for metric, floor in OFFLINE_FLOORS[tier].items():
        assert result["metrics"][metric] >= floor, (tier, metric, result["metrics"])


def test_qdrant_tiers_are_explicitly_skipped_offline(
    offline_report: Dict[str, Any],
) -> None:
    for name in nre.QDRANT_TIERS:
        assert offline_report["tiers"][name]["status"] == "skipped"


def test_qdrant_auto_mode_needs_every_env_var() -> None:
    full = {v: "x" for v in nre.QDRANT_ENV_VARS}
    assert nre.qdrant_env_present(full)
    for missing in nre.QDRANT_ENV_VARS:
        assert not nre.qdrant_env_present({**full, missing: " "})


def test_committed_baseline_has_no_regression(offline_report: Dict[str, Any]) -> None:
    baseline = json.loads(nre.DEFAULT_BASELINE_PATH.read_text("utf-8"))
    assert baseline["gate"] == {"metric": "recall@10", "max_drop_points": 3.0}
    assert nre.check_regression(offline_report, baseline) == []


def test_tfidf_module_state_is_restored() -> None:
    # The TF-IDF tier builds vector_search's process-wide fallback index over
    # the eval corpus; the run must put the previous state back.
    before = (list(vs._tfidf_doc_ids), vs._tfidf_built)
    nre.run_eval(qdrant="off")
    assert (list(vs._tfidf_doc_ids), vs._tfidf_built) == before


def test_main_check_fails_on_regression(tmp_path: Path) -> None:
    baseline = tmp_path / "baseline.json"
    report = tmp_path / "report.json"
    baseline.write_text(json.dumps(_tiers(local_bm25=100.1)), encoding="utf-8")
    code = nre.main(
        ["--check", "--qdrant", "off", "--baseline", str(baseline), "--report", str(report)]
    )
    assert code == nre.EXIT_FAIL
    written = json.loads(report.read_text("utf-8"))
    assert written["tiers"]["local_bm25"]["status"] == "ok"
    assert "per_query" in written


def test_main_check_errors_without_baseline(tmp_path: Path) -> None:
    code = nre.main(
        [
            "--check",
            "--qdrant",
            "off",
            "--baseline",
            str(tmp_path / "missing.json"),
            "--report",
            str(tmp_path / "r.json"),
        ]
    )
    assert code == nre.EXIT_ERROR


# ---------------------------------------------------------------------------
# Live Qdrant tiers against a faked HTTP boundary
# ---------------------------------------------------------------------------


_QUERY_TEXTS = [q.query for q in nre.load_queries()]


class _FakeEmbedder:
    """One-hot query vectors so the fake store can tell which query it got."""

    name = "fake"

    def embed(self, texts: Sequence[str], input_type: str) -> List[List[float]]:
        assert input_type == "query"
        dim = vs._active_vector_dim()
        out = []
        for t in texts:
            vec = [0.0] * dim
            vec[_QUERY_TEXTS.index(t)] = 1.0
            out.append(vec)
        return out


class _FakeQdrant:
    """Records every request; answers each query with its labelled chunk."""

    def __init__(self, corpus: Sequence[rev.Chunk], size: int, distance: str = "Cosine"):
        self.calls: List[tuple] = []
        self._size = size
        self._distance = distance
        self._answer: Dict[int, str] = {}
        for idx, q in enumerate(nre.load_queries()):
            if q.out_of_scope:
                continue
            for c in corpus:
                if nre.is_relevant(c.stable_id, q):
                    self._answer[idx] = c.text
                    break

    def __call__(self, method: str, path: str, body: Optional[dict] = None, **_kw):
        self.calls.append((method, path))
        if method == "GET":
            return {
                "result": {
                    "points_count": 6227,
                    "config": {
                        "params": {
                            "vectors": {"size": self._size, "distance": self._distance}
                        }
                    },
                }
            }
        assert method == "POST" and path.endswith("/points/search")
        idx = body["vector"].index(1.0)
        text = self._answer.get(idx, "[x.json] unrelated\nnothing: here")
        # Duplicate points per doc (as prod holds) must be deduped by the tier.
        return {
            "result": [
                {"id": 1, "score": 0.9, "payload": {"doc_id": "x:1", "text": text}},
                {"id": 2, "score": 0.8, "payload": {"doc_id": "x:1", "text": text}},
            ]
        }


@pytest.fixture
def fake_qdrant_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(vs, "_QDRANT_URL", "https://qdrant.invalid")
    monkeypatch.setattr(vs, "_QDRANT_API_KEY", "test-key")
    monkeypatch.setattr(vs, "_qdrant_dim_guard", None)
    monkeypatch.setattr(vs, "_last_qdrant_error", None)
    monkeypatch.setattr(vs, "_qdrant_available", False)


def test_qdrant_tiers_run_read_only(
    fake_qdrant_env: None,
    monkeypatch: pytest.MonkeyPatch,
    corpus: List[rev.Chunk],
) -> None:
    fake = _FakeQdrant(corpus, size=vs._active_vector_dim())
    monkeypatch.setattr(vs, "_qdrant_request", fake)
    report = nre.run_eval(qdrant="on", embedder=_FakeEmbedder())

    vec = report["tiers"]["qdrant_vector"]
    assert vec["status"] == "ok", vec
    # Each in-scope query is answered with its labelled chunk at rank 1.
    assert vec["metrics"]["recall@5"] == 100.0
    assert report["tiers"]["qdrant_hybrid"]["status"] == "ok"
    assert report["qdrant"]["collection"] == vs._active_collection()
    # Read-only: one collection GET, then only points/search POSTs.
    assert fake.calls[0] == ("GET", f"/collections/{vs._active_collection()}")
    assert all(m == "POST" and p.endswith("/points/search") for m, p in fake.calls[1:])
    # The process's attach flag is restored after the run.
    assert vs._qdrant_available is False


def test_qdrant_tier_refuses_wrong_dimension_collection(
    fake_qdrant_env: None,
    monkeypatch: pytest.MonkeyPatch,
    corpus: List[rev.Chunk],
) -> None:
    fake = _FakeQdrant(corpus, size=vs._active_vector_dim() // 2)
    monkeypatch.setattr(vs, "_qdrant_request", fake)
    report = nre.run_eval(qdrant="on", embedder=_FakeEmbedder())
    for name in nre.QDRANT_TIERS:
        assert report["tiers"][name]["status"] == "error"
        assert "dim guard" in report["tiers"][name]["reason"]
    assert [m for m, _ in fake.calls] == ["GET"]  # never searched


@pytest.mark.live
def test_live_qdrant_tiers_when_keys_present() -> None:
    if not nre.qdrant_env_present():
        pytest.skip("needs " + ", ".join(nre.QDRANT_ENV_VARS))
    report = nre.run_eval(qdrant="auto")
    assert report["tiers"]["qdrant_vector"]["status"] == "ok", report["tiers"]
