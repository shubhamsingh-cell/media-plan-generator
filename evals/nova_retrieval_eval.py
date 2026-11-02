#!/usr/bin/env python3
"""nova_retrieval_eval.py -- recall@5 / recall@10 / MRR for Nova's retrieval paths.

What it measures
----------------
A labelled set of client-neutral Nova questions (``evals/nova_retrieval_queries.json``:
CPC/CPA by country and industry, best job boards by country, benchmark/report
facts, plan figures, out-of-scope) is replayed against:

``local_bm25``     the in-process BM25 index (``vector_search.BM25Index``)   offline
``local_tfidf``    the in-process TF-IDF fallback (``_tfidf_search``)        offline
``qdrant_vector``  the LIVE Qdrant collection prod searches, read-only     network
``qdrant_hybrid``  Qdrant + local BM25 fused with RRF k=60 -- the path
                   ``vector_search.search()`` serves once warm-up has built
                   BM25 in the worker                                       network

The local tiers run offline everywhere (CI included).  The Qdrant tiers run
only when ``QDRANT_URL``, ``QDRANT_API_KEY`` and ``VOYAGE_API_KEY`` are all set
(or ``--qdrant on``); they send one ``GET /collections/{name}`` (to run the
embedding-dimension guard), then one ``POST .../points/search`` per query, plus
one Voyage query embedding per query.  Nothing is written to Qdrant.  Without
the env vars they are reported as an explicit ``skipped`` row, never silently.

Labels
------
Chunks are addressed by STABLE id (``"file.json#key.path"``, the header every
chunk carries; see ``evals/retrieval_eval.py``) because the production doc id
(``"file.json:N"``) is a positional counter that shifts on every KB edit.
``expected_ids`` match exactly; ``expected_id_prefixes`` match the id or any
descendant (``prefix.child`` / ``prefix[3]``).  Any one match counts.  A label
that resolves to no chunk is reported as stale and fails ``--check``.

Metrics
-------
recall@k = % of in-scope queries with a labelled chunk in the top k (hit-rate);
mrr = mean reciprocal rank of the first labelled chunk within the top 10, x100.
Out-of-scope rows are never in recall; their mean top-1 score is reported next
to the in-scope mean so a future abstain threshold can be chosen from data.

Gate
----
``--check`` exits 1 when any tier's recall@10 is more than 3 points below the
committed baseline (``evals/nova_retrieval_baseline.json``).  Only tiers that
ran (``ok``) in BOTH the baseline and this run are compared.

Commands
--------
    python3 evals/nova_retrieval_eval.py                    # offline table
    python3 evals/nova_retrieval_eval.py --check            # CI gate
    python3 evals/nova_retrieval_eval.py --update-baseline  # after a verified-good change
    QDRANT_URL=... QDRANT_API_KEY=... VOYAGE_API_KEY=... \\
        python3 evals/nova_retrieval_eval.py --check        # + live Qdrant tiers

The JSON report is written to ``--report`` (default
``evals/.cache/nova_retrieval_report.json``, git-ignored).

Exit codes: 0 pass, 1 regression or stale label, 2 error.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

EVALS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = EVALS_DIR.parent
for _p in (str(PROJECT_ROOT), str(EVALS_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import retrieval_eval as rev  # noqa: E402  (corpus, stable ids, embedder)
import vector_search as vs  # noqa: E402

logger = logging.getLogger("nova_retrieval_eval")

DEFAULT_QUERIES_PATH = EVALS_DIR / "nova_retrieval_queries.json"
DEFAULT_BASELINE_PATH = EVALS_DIR / "nova_retrieval_baseline.json"
DEFAULT_REPORT_PATH = EVALS_DIR / ".cache" / "nova_retrieval_report.json"
DEFAULT_EMBED_CACHE_PATH = EVALS_DIR / ".cache" / "nova_retrieval_query_embeddings.json"

K_VALUES: Tuple[int, ...] = (5, 10)
MAX_DEPTH = max(K_VALUES)
METRIC_NAMES: Tuple[str, ...] = ("recall@5", "recall@10", "mrr")
GATE_METRIC = "recall@10"
MAX_REGRESSION = 3.0
RRF_K = 60
#: Per-source fetch depth for the hybrid tier (prod search() uses 2 x top_k).
HYBRID_FETCH_K = 2 * MAX_DEPTH
SCHEMA_VERSION = 1

LOCAL_TIERS: Tuple[str, ...] = ("local_bm25", "local_tfidf")
QDRANT_TIERS: Tuple[str, ...] = ("qdrant_vector", "qdrant_hybrid")
TIER_NAMES: Tuple[str, ...] = LOCAL_TIERS + QDRANT_TIERS

QDRANT_ENV_VARS: Tuple[str, ...] = ("QDRANT_URL", "QDRANT_API_KEY", "VOYAGE_API_KEY")

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_ERROR = 2


# ---------------------------------------------------------------------------
# Query set
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Query:
    """One labelled eval question."""

    row_id: str
    domain: str
    query: str
    expected_ids: Tuple[str, ...]
    expected_prefixes: Tuple[str, ...]
    out_of_scope: bool
    answer_source: str


def load_queries(path: Path = DEFAULT_QUERIES_PATH) -> List[Query]:
    """Load and validate the query set; raise ``ValueError`` on a bad row."""
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"cannot read query set {path}: {exc}") from exc
    items = raw.get("rows") if isinstance(raw, dict) else None
    if not isinstance(items, list) or not items:
        raise ValueError(f"query set {path} has no rows")
    out: List[Query] = []
    seen: set = set()
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"row #{idx} is not an object")
        row_id = str(item.get("id") or "").strip()
        query = str(item.get("query") or "").strip()
        if not row_id or row_id in seen:
            raise ValueError(f"row #{idx}: missing or duplicate id {row_id!r}")
        seen.add(row_id)
        if not query:
            raise ValueError(f"row {row_id}: empty query")
        ids = tuple(str(x) for x in (item.get("expected_ids") or []))
        prefixes = tuple(str(x) for x in (item.get("expected_id_prefixes") or []))
        oos = bool(item.get("out_of_scope"))
        if oos and (ids or prefixes):
            raise ValueError(f"row {row_id}: out_of_scope rows must carry no labels")
        if not oos and not (ids or prefixes):
            raise ValueError(f"row {row_id}: in-scope row needs expected ids/prefixes")
        out.append(
            Query(
                row_id=row_id,
                domain=str(item.get("domain") or "general"),
                query=query,
                expected_ids=ids,
                expected_prefixes=prefixes,
                out_of_scope=oos,
                answer_source=str(item.get("answer_source") or "kb"),
            )
        )
    return out


def matches_prefix(stable_id: str, prefix: str) -> bool:
    """True if ``stable_id`` is ``prefix`` or one of its descendants."""
    return stable_id == prefix or stable_id.startswith((prefix + ".", prefix + "["))


def is_relevant(stable_id: str, q: Query) -> bool:
    """True if ``stable_id`` satisfies any of ``q``'s labels."""
    if stable_id in q.expected_ids:
        return True
    return any(matches_prefix(stable_id, p) for p in q.expected_prefixes)


def stale_labels(queries: Sequence[Query], corpus_ids: Sequence[str]) -> List[str]:
    """Labels that match no chunk in the corpus (``row_id: label``)."""
    known = set(corpus_ids)
    stale: List[str] = []
    for q in queries:
        for e in q.expected_ids:
            if e not in known:
                stale.append(f"{q.row_id}: {e}")
        for p in q.expected_prefixes:
            if not any(matches_prefix(sid, p) for sid in known):
                stale.append(f"{q.row_id}: {p}*")
    return stale


# ---------------------------------------------------------------------------
# Metrics (pure)
# ---------------------------------------------------------------------------


def first_relevant_rank(ranked: Sequence[str], q: Query) -> Optional[int]:
    """1-based rank of the first relevant id within ``MAX_DEPTH``, else None."""
    for idx, sid in enumerate(ranked[:MAX_DEPTH], start=1):
        if is_relevant(sid, q):
            return idx
    return None


def score_query(q: Query, ranked: Sequence[Tuple[str, float]]) -> Dict[str, Any]:
    """Per-query record for one tier's ranked ``(stable_id, score)`` list."""
    ids = [sid for sid, _ in ranked]
    rank = first_relevant_rank(ids, q)
    rec: Dict[str, Any] = {
        "id": q.row_id,
        "domain": q.domain,
        "query": q.query,
        "out_of_scope": q.out_of_scope,
        "first_hit_rank": rank,
        "rr": (1.0 / rank) if rank else 0.0,
        "top1_score": round(float(ranked[0][1]), 4) if ranked else None,
        "top3": ids[:3],
    }
    for k in K_VALUES:
        rec[f"recall@{k}"] = 1.0 if (rank is not None and rank <= k) else 0.0
    return rec


def aggregate(records: Sequence[Dict[str, Any]]) -> Dict[str, float]:
    """recall@k and MRR over in-scope records, as percentage points."""
    scoped = [r for r in records if not r["out_of_scope"]]
    n = len(scoped)
    if n == 0:
        return {m: 0.0 for m in METRIC_NAMES}
    out = {
        f"recall@{k}": round(100.0 * sum(r[f"recall@{k}"] for r in scoped) / n, 2)
        for k in K_VALUES
    }
    out["mrr"] = round(100.0 * sum(r["rr"] for r in scoped) / n, 2)
    return out


def by_domain(records: Sequence[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Per-domain metrics (informational; never gated)."""
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in records:
        if not r["out_of_scope"]:
            groups.setdefault(r["domain"], []).append(r)
    return {
        d: {"queries": len(rs), **aggregate(rs)} for d, rs in sorted(groups.items())
    }


def _mean(values: Sequence[Optional[float]]) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return round(sum(vals) / len(vals), 4) if vals else None


def out_of_scope_view(records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Top-1 score separation between in-scope and out-of-scope questions."""
    oos = [r["top1_score"] for r in records if r["out_of_scope"]]
    ins = [r["top1_score"] for r in records if not r["out_of_scope"]]
    return {
        "queries": len(oos),
        "oos_top1_score_mean": _mean(oos),
        "in_scope_top1_score_mean": _mean(ins),
        "note": "informational: retrieval never abstains today; use the "
        "separation to pick an abstain threshold",
    }


def check_regression(
    current: Dict[str, Any],
    baseline: Optional[Dict[str, Any]],
    metric: str = GATE_METRIC,
    max_drop: float = MAX_REGRESSION,
) -> List[str]:
    """Messages for tiers whose ``metric`` fell more than ``max_drop`` points.

    Only tiers ``ok`` in both reports are compared; exactly ``max_drop`` passes.
    """
    if not baseline:
        return []
    out: List[str] = []
    for name, base in sorted((baseline.get("tiers") or {}).items()):
        cur = (current.get("tiers") or {}).get(name) or {}
        if base.get("status") != "ok" or cur.get("status") != "ok":
            continue
        b = (base.get("metrics") or {}).get(metric)
        c = (cur.get("metrics") or {}).get(metric)
        if b is None or c is None:
            continue
        drop = round(float(b) - float(c), 2)
        if drop > max_drop:
            out.append(
                f"{name} {metric} dropped {drop:.2f} pts ({float(b):.2f} -> "
                f"{float(c):.2f}); allowed {max_drop:.1f}"
            )
    return out


# ---------------------------------------------------------------------------
# Tiers: search(query, depth) -> [(stable_id, score)], best first, deduped
# ---------------------------------------------------------------------------


def _dedupe(pairs: Sequence[Tuple[Optional[str], float]], depth: int):
    out: List[Tuple[str, float]] = []
    seen: set = set()
    for sid, score in pairs:
        if not sid or sid in seen:
            continue
        seen.add(sid)
        out.append((sid, float(score)))
        if len(out) >= depth:
            break
    return out


class LocalBM25:
    """The production BM25 class over the eval corpus (own instance)."""

    name = "local_bm25"

    def __init__(self, corpus: Sequence[rev.Chunk]) -> None:
        self._index = vs.BM25Index()
        self._index.index(rev._docs_for_index(corpus))

    def search(self, query: str, depth: int) -> List[Tuple[str, float]]:
        return _dedupe(self._index.search(query, top_k=depth), depth)


class LocalTfidf:
    """The production TF-IDF fallback; module state restored on exit."""

    name = "local_tfidf"

    def __init__(self, corpus: Sequence[rev.Chunk]) -> None:
        self._corpus = corpus
        self._guard: Any = None

    def __enter__(self) -> "LocalTfidf":
        self._guard = rev._tfidf_state_guard()
        self._guard.__enter__()
        vs._build_tfidf_index(rev._docs_for_index(self._corpus))
        return self

    def __exit__(self, *exc_info: Any) -> None:
        if self._guard is not None:
            self._guard.__exit__(*exc_info)
            self._guard = None

    def search(self, query: str, depth: int) -> List[Tuple[str, float]]:
        hits = vs._tfidf_search(query, top_k=depth)
        return _dedupe([(h["id"], h.get("score", 0.0)) for h in hits], depth)


class QdrantUnavailable(RuntimeError):
    """The live Qdrant tier cannot run (no env, dim guard, request failure)."""


class QdrantVector:
    """Read-only search of the live collection ``vector_search`` serves from.

    Query vectors come from ``embedder`` (Voyage, the ACTIVE model) and the
    search goes through ``vector_search._qdrant_search`` -- the exact request
    prod sends.  Hits are mapped to stable ids from the payload text header.
    """

    name = "qdrant_vector"

    def __init__(self, embedder: Any, overfetch: int = vs._QDRANT_OVERFETCH) -> None:
        if not vs._qdrant_is_configured():
            raise QdrantUnavailable("QDRANT_URL / QDRANT_API_KEY not configured")
        self.collection = vs._active_collection()
        info = vs._qdrant_request("GET", f"/collections/{self.collection}")
        if not info or not info.get("result"):
            raise QdrantUnavailable(f"collection {self.collection!r} not readable")
        if vs._qdrant_dim_guard_check(self.collection, info["result"]):
            reason = (vs._qdrant_dim_guard or {}).get("reason") or "dim guard"
            raise QdrantUnavailable(f"dim guard: {reason}")
        # Read-only arm of THIS process's tier (what _qdrant_attach does).
        vs._qdrant_available = True
        self.points = (info["result"] or {}).get("points_count")
        self._embedder = embedder
        self._overfetch = max(1, int(overfetch))
        self._qvec: Dict[str, List[float]] = {}

    def prepare_queries(self, queries: Sequence[str]) -> None:
        todo = [q for q in dict.fromkeys(queries) if q not in self._qvec]
        if not todo:
            return
        try:
            vecs = self._embedder.embed(todo, "query")
        except rev.EmbedderUnavailable as exc:
            raise QdrantUnavailable(f"query embedding failed: {exc}") from exc
        if len(vecs) != len(todo):
            raise QdrantUnavailable("embedder returned the wrong number of vectors")
        self._qvec.update(zip(todo, vecs))

    def search(self, query: str, depth: int) -> List[Tuple[str, float]]:
        self.prepare_queries([query])
        hits = vs._qdrant_search(
            self._qvec[query], top_k=min(depth * self._overfetch, 100)
        )
        if hits is None:
            raise QdrantUnavailable("points/search request failed")
        pairs = [(rev.stable_id_for(h.get("text") or ""), h["score"]) for h in hits]
        return _dedupe(pairs, depth)


class QdrantHybrid:
    """Qdrant + local BM25 via RRF k=60: what ``search()`` serves when warm."""

    name = "qdrant_hybrid"

    def __init__(self, vector: QdrantVector, bm25: LocalBM25) -> None:
        self._vector = vector
        self._bm25 = bm25

    def search(self, query: str, depth: int) -> List[Tuple[str, float]]:
        v = self._vector.search(query, HYBRID_FETCH_K)
        b = self._bm25.search(query, HYBRID_FETCH_K)
        fused = vs.reciprocal_rank_fusion(v, b, k=RRF_K)
        return _dedupe(fused, depth)


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def qdrant_env_present(environ: Optional[Dict[str, str]] = None) -> bool:
    """True when every env var the live Qdrant tiers need is non-empty."""
    env = os.environ if environ is None else environ
    return all((env.get(v) or "").strip() for v in QDRANT_ENV_VARS)


def _run_tier(tier: Any, queries: Sequence[Query]) -> Tuple[Dict[str, Any], list]:
    records = [score_query(q, tier.search(q.query, MAX_DEPTH)) for q in queries]
    result = {
        "status": "ok",
        "metrics": aggregate(records),
        "by_domain": by_domain(records),
        "out_of_scope": out_of_scope_view(records),
    }
    return result, records


def run_eval(
    queries_path: Path = DEFAULT_QUERIES_PATH,
    qdrant: str = "auto",
    embedder: Any = None,
    data_dir: Optional[Path] = None,
    voyage_rpm: float = 10.0,
) -> Dict[str, Any]:
    """Run every tier that can run and return the full report.

    Args:
        queries_path: Labelled query set.
        qdrant: ``"auto"`` (run if the env vars are present), ``"on"`` (try
            even without them; reported as an error row if it cannot) or
            ``"off"``.
        embedder: Query embedder for the Qdrant tiers (tests inject a fake);
            defaults to Voyage with the ACTIVE model.
        data_dir: KB directory override (tests).
        voyage_rpm: Voyage request pacing for the default embedder.
    """
    started = time.monotonic()
    corpus = rev.build_corpus(data_dir)
    queries = load_queries(queries_path)
    stale = stale_labels(queries, [c.stable_id for c in corpus])
    domains: Dict[str, int] = {}
    for q in queries:
        domains[q.domain] = domains.get(q.domain, 0) + 1

    report: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": date.today().isoformat(),
        "metric_units": "percentage points (0-100); mrr = MRR@10 x 100",
        "gate": {"metric": GATE_METRIC, "max_drop_points": MAX_REGRESSION},
        "corpus": {
            "chunks": len(corpus),
            "kb_files": len({c.source for c in corpus}),
            "queries": len(queries),
            "in_scope_queries": sum(1 for q in queries if not q.out_of_scope),
            "out_of_scope_queries": sum(1 for q in queries if q.out_of_scope),
            "queries_by_domain": dict(sorted(domains.items())),
            "stale_labels": stale,
        },
        "tiers": {},
        "per_query": {},
    }

    def record(name: str, tier: Any) -> None:
        result, records = _run_tier(tier, queries)
        report["tiers"][name] = result
        report["per_query"][name] = records

    bm25 = LocalBM25(corpus)
    record("local_bm25", bm25)
    with LocalTfidf(corpus) as tfidf:
        record("local_tfidf", tfidf)

    run_qdrant = qdrant == "on" or (qdrant == "auto" and qdrant_env_present())
    if not run_qdrant:
        reason = (
            "disabled (--qdrant off)"
            if qdrant == "off"
            else "SKIPPED: needs " + ", ".join(QDRANT_ENV_VARS)
        )
        for name in QDRANT_TIERS:
            report["tiers"][name] = {"status": "skipped", "reason": reason}
    else:
        saved_available = vs._qdrant_available
        try:
            if embedder is None:
                embedder = rev.VoyageEmbedder(
                    model=vs.get_active_embedding_model(),
                    rpm=voyage_rpm,
                    cache_path=DEFAULT_EMBED_CACHE_PATH,
                )
            vector = QdrantVector(embedder)
            vector.prepare_queries([q.query for q in queries])
            report["qdrant"] = {
                "collection": vector.collection,
                "points_count": vector.points,
                "embedding_model": vs.get_active_embedding_model(),
                "embedding_dim": vs._active_vector_dim(),
            }
            record("qdrant_vector", vector)
            record("qdrant_hybrid", QdrantHybrid(vector, bm25))
        except QdrantUnavailable as exc:
            for name in QDRANT_TIERS:
                report["tiers"].setdefault(name, {"status": "error", "reason": str(exc)})
        finally:
            vs._qdrant_available = saved_available

    report["elapsed_seconds"] = round(time.monotonic() - started, 2)
    return report


def baseline_view(report: Dict[str, Any]) -> Dict[str, Any]:
    """Committed-baseline shape: metrics only, no per-query records."""
    return {k: v for k, v in report.items() if k not in ("per_query", "elapsed_seconds")}


def format_table(report: Dict[str, Any]) -> str:
    """Human-readable summary (skipped / error rows included)."""
    c = report["corpus"]
    lines = [
        f"Nova retrieval eval -- {c['in_scope_queries']} in-scope + "
        f"{c['out_of_scope_queries']} out-of-scope queries over {c['chunks']} chunks",
        "",
        f"{'tier':<15}" + "".join(f"{m:>11}" for m in METRIC_NAMES) + "  oos/in top1",
        "-" * 64,
    ]
    for name in TIER_NAMES:
        t = report["tiers"].get(name)
        if not t:
            continue
        if t.get("status") != "ok":
            lines.append(f"{name:<15}{t.get('status')}: {t.get('reason')}")
            continue
        m, o = t["metrics"], t["out_of_scope"]
        lines.append(
            f"{name:<15}"
            + "".join(f"{m[x]:>11.2f}" for x in METRIC_NAMES)
            + f"  {o['oos_top1_score_mean']}/{o['in_scope_top1_score_mean']}"
        )
    if c["stale_labels"]:
        lines += ["", f"STALE LABELS ({len(c['stale_labels'])}):"]
        lines += [f"  {s}" for s in c["stale_labels"]]
    return "\n".join(lines)


def _load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return raw if isinstance(raw, dict) else None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--queries", type=Path, default=DEFAULT_QUERIES_PATH)
    p.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE_PATH)
    p.add_argument("--report", type=Path, default=DEFAULT_REPORT_PATH)
    p.add_argument("--check", action="store_true", help="gate on recall@10")
    p.add_argument("--update-baseline", action="store_true")
    p.add_argument("--qdrant", choices=("auto", "on", "off"), default="auto")
    p.add_argument("--voyage-rpm", type=float, default=10.0)
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    try:
        report = run_eval(args.queries, qdrant=args.qdrant, voyage_rpm=args.voyage_rpm)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_ERROR

    print(format_table(report))
    try:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"\nreport: {args.report}")
    except OSError as exc:
        print(f"ERROR: cannot write report {args.report}: {exc}", file=sys.stderr)
        return EXIT_ERROR

    if args.update_baseline:
        args.baseline.write_text(
            json.dumps(baseline_view(report), indent=2) + "\n", encoding="utf-8"
        )
        print(f"baseline written: {args.baseline}")
        return EXIT_PASS
    if not args.check:
        return EXIT_PASS

    baseline = _load_json(args.baseline)
    if baseline is None:
        print(f"ERROR: no readable baseline at {args.baseline}", file=sys.stderr)
        return EXIT_ERROR
    failures = check_regression(report, baseline)
    if report["corpus"]["stale_labels"]:
        failures.append(
            f"{len(report['corpus']['stale_labels'])} label(s) no longer resolve "
            "in the KB (fix the query set)"
        )
    for name, base in sorted((baseline.get("tiers") or {}).items()):
        cur = report["tiers"].get(name) or {}
        if base.get("status") == "ok" and cur.get("status") != "ok":
            print(f"NOTE: {name} not compared ({cur.get('reason') or 'not run'})")
    if failures:
        print("\nNOVA RETRIEVAL GATE: FAIL")
        for f in failures:
            print(f"  - {f}")
        return EXIT_FAIL
    print("\nNOVA RETRIEVAL GATE: PASS")
    return EXIT_PASS


if __name__ == "__main__":
    sys.exit(main())
