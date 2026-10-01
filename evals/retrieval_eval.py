#!/usr/bin/env python3
"""retrieval_eval.py -- offline retrieval-quality eval for the Nova knowledge base.

Why this exists
---------------
Every retrieval change (embedding-model swap, reranker, chunking, hybrid
weights) used to ship unmeasured.  This harness is the gate: it replays a
golden set of real Nova questions against the SAME corpus the production
indexer builds (``vector_search._KB_INDEX_FILES`` chunked by
``vector_search._extract_text_chunks``) and reports recall@1/3/5/10 and MRR@10
for every retrieval tier that can run, then fails ``--check`` when any tier
regresses more than 3 points below the committed baseline
(``evals/retrieval_baseline.json``), mirroring ``scripts/eval_gate.py`` /
``evals/baseline_scores.json``.

Definitions
-----------
* A *document* is one chunk of ``data/*.json`` as the indexer builds it.  The
  production id (``"file.json:N"``) is a positional counter and shifts when any
  KB file changes, so golden rows reference a STABLE id instead:
  ``"file.json#key.path"`` -- the key path printed in the chunk's first line
  (``[file.json] key.path``).  ``--audit-golden`` shows every expected id with
  its chunk text and flags ids that no longer resolve.
* ``expected_ids`` are ALTERNATIVE valid answers (any one counts), because the
  KB repeats facts across files.
* ``recall@k`` = % of queries with at least one expected id in the top k
  (hit-rate convention); ``mrr`` = mean reciprocal rank of the first expected
  id within the top 10, times 100.  All metrics are percentage points (0-100)
  so the 3-point regression rule is uniform.

Tiers
-----
``bm25``        Okapi BM25 (``vector_search.BM25Index``)          -- offline
``tfidf``       TF-IDF cosine (``vector_search._tfidf_search``)   -- offline
``vector``      dense cosine over chunk embeddings                -- needs an embedder
``hybrid_rrf``  vector + BM25 fused with RRF k=60 (prod's search) -- needs an embedder

``vector`` / ``hybrid_rrf`` need embeddings.  With no embedder they print an
explicit ``SKIPPED: no key ...`` row (never silently).  An embedder is any
object with ``name: str`` and ``embed(texts, input_type) -> list[list[float]]``
(raise ``EmbedderUnavailable`` when it cannot).  ``RepoCacheEmbedder`` serves
vectors from ``data/.embedding_cache.json`` and is used automatically ONLY when
the cache holds every golden query AND every chunk; ``VoyageEmbedder`` calls the
real API.  Inject your own via ``run_eval(embedder=...)``.

Commands
--------
    # Offline (CI): BM25 + TF-IDF, print table, no files written
    python3 evals/retrieval_eval.py

    # CI gate: exit 1 if any tier is >3 points below evals/retrieval_baseline.json
    python3 evals/retrieval_eval.py --check

    # Refresh the committed baseline after a verified-good change
    python3 evals/retrieval_eval.py --update-baseline

    # Check the golden set still resolves against the current KB files
    python3 evals/retrieval_eval.py --audit-golden

    # OWNER, with a real key: add the dense + hybrid tiers (voyage-4-lite = prod model).
    # ~6.2k chunk embeddings; at 10 RPM / 32 per request that is ~20 minutes the
    # first time, cached under evals/.cache/ (git-ignored) for every later run.
    VOYAGE_API_KEY=... python3 evals/retrieval_eval.py --voyage --voyage-rpm 10 \\
        --update-baseline          # first keyed run: record vector tiers in the baseline
    VOYAGE_API_KEY=... python3 evals/retrieval_eval.py --voyage --check

    # Plan step 6 A/B (other model, same golden set, nothing else changes):
    VOYAGE_API_KEY=... python3 evals/retrieval_eval.py --voyage \\
        --voyage-model voyage-3-large --report /tmp/voyage3large.json

Exit codes: 0 pass, 1 regression, 2 error (missing baseline/golden, bad input).

Python stdlib only (numpy is used for the dense tier when importable).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import operator
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import vector_search as vs  # noqa: E402

logger = logging.getLogger("retrieval_eval")

EVALS_DIR = PROJECT_ROOT / "evals"
DEFAULT_GOLDEN_PATH = EVALS_DIR / "retrieval_golden.json"
DEFAULT_BASELINE_PATH = EVALS_DIR / "retrieval_baseline.json"
DEFAULT_EMBED_CACHE_PATH = EVALS_DIR / ".cache" / "retrieval_embeddings.json"
DEFAULT_REPO_CACHE_PATH = PROJECT_ROOT / "data" / ".embedding_cache.json"

K_VALUES: Tuple[int, ...] = (1, 3, 5, 10)
MAX_DEPTH = max(K_VALUES)
#: Max allowed drop (percentage points) below the committed baseline.
MAX_REGRESSION = 3.0
#: Fetch depth per source for the hybrid tier (prod uses 2 x top_k per source).
HYBRID_FETCH_K = 2 * MAX_DEPTH
SCHEMA_VERSION = 1

EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_ERROR = 2

TIER_NAMES: Tuple[str, ...] = ("bm25", "tfidf", "vector", "hybrid_rrf")
METRIC_NAMES: Tuple[str, ...] = tuple(f"recall@{k}" for k in K_VALUES) + ("mrr",)

SKIP_NO_KEY = "SKIPPED: no key"


class EmbedderUnavailable(RuntimeError):
    """An embedder cannot produce vectors (no key, cache miss, API failure)."""


# ---------------------------------------------------------------------------
# Corpus: the exact chunks the production indexer builds
# ---------------------------------------------------------------------------

_HEADER_RE = re.compile(r"\[([^\]]+)\] ([^\n]*)")
_LEAF_RE = re.compile(r"^(.*?\[\d+\]): ")


@dataclass(frozen=True)
class Chunk:
    """One indexed KB chunk."""

    doc_id: str  # production id, "file.json:N" (positional, shifts with KB edits)
    stable_id: str  # "file.json#key.path" (what golden rows reference)
    source: str
    text: str  # as indexed (<= 2000 chars)


def stable_id_for(chunk_text: str) -> Optional[str]:
    """Return ``"file#key.path"`` parsed from a chunk's first line, else None.

    Dict chunks start ``[file] key.path`` and list-of-string leaf chunks start
    ``[file] key.path[3]: value``; the leaf form must not leak its value into
    the id.
    """
    match = _HEADER_RE.match(chunk_text or "")
    if not match:
        return None
    source, rest = match.group(1), match.group(2)
    leaf = _LEAF_RE.match(rest)
    prefix = leaf.group(1) if leaf else rest
    return f"{source}#{prefix}"


#: Indexed KB files that are gitignored runtime artifacts (written by the
#: enrichment jobs, the server and the test suite; absent on a fresh checkout).
#: The eval excludes them so its corpus -- and therefore the committed baseline --
#: is the same on every machine instead of drifting with whatever a prior run
#: left in data/.  Pass ``include_runtime=True`` to mirror production exactly.
RUNTIME_GENERATED_KB_FILES = frozenset(
    {
        "channel_benchmarks_live.json",
        "competitor_careers.json",
        "google_trends.json",
        "job_posting_volumes.json",
    }
)


def build_corpus(
    data_dir: Optional[Path] = None,
    kb_files: Optional[Sequence[str]] = None,
    include_runtime: bool = False,
) -> List[Chunk]:
    """Rebuild the production chunk list (mirrors ``index_knowledge_base``).

    ``tests/test_retrieval_eval.py`` pins parity with the real indexer (with
    ``include_runtime=True``), so a change to chunking cannot silently
    desynchronise this harness.  By default the gitignored runtime artifacts in
    ``RUNTIME_GENERATED_KB_FILES`` are skipped (see that constant).
    """
    data_dir = Path(data_dir) if data_dir else PROJECT_ROOT / "data"
    if kb_files is not None:
        files = list(kb_files)
    else:
        files = [
            f
            for f in vs._KB_INDEX_FILES
            if include_runtime or f not in RUNTIME_GENERATED_KB_FILES
        ]
    chunks: List[Chunk] = []
    counter = 0
    for filename in files:
        path = data_dir / filename
        if not path.exists():
            continue
        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
        except (json.JSONDecodeError, OSError) as exc:
            logger.error("Cannot load %s: %s", filename, exc, exc_info=True)
            continue
        for chunk_text in vs._extract_text_chunks(raw, source=filename):
            if len(chunk_text.strip()) < 20:
                continue
            counter += 1
            text = chunk_text[:2000]
            sid = stable_id_for(text) or f"{filename}#?{counter}"
            chunks.append(
                Chunk(
                    doc_id=f"{filename}:{counter}",
                    stable_id=sid,
                    source=filename,
                    text=text,
                )
            )
    return chunks


# ---------------------------------------------------------------------------
# Golden set
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class GoldenRow:
    """One golden query."""

    row_id: str
    domain: str
    query: str
    expected_ids: Tuple[str, ...]
    notes: str


@dataclass
class ScoredRow:
    """A golden row resolved against the current corpus."""

    row: GoldenRow
    resolved_ids: Tuple[str, ...]
    stale_ids: Tuple[str, ...]


def load_golden(path: Path = DEFAULT_GOLDEN_PATH) -> List[GoldenRow]:
    """Load and validate the golden set; raise ``ValueError`` on bad rows."""
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ValueError(f"cannot read golden set {path}: {exc}") from exc
    items = raw.get("rows") if isinstance(raw, dict) else raw
    if not isinstance(items, list) or not items:
        raise ValueError(f"golden set {path} has no rows")
    rows: List[GoldenRow] = []
    seen: set = set()
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            raise ValueError(f"golden row #{idx} is not an object")
        row_id = str(item.get("id") or f"g{idx + 1:03d}")
        query = str(item.get("query") or "").strip()
        expected = item.get("expected_ids")
        if not query:
            raise ValueError(f"golden row {row_id}: empty query")
        if not isinstance(expected, list) or not expected:
            raise ValueError(f"golden row {row_id}: expected_ids must be non-empty")
        if row_id in seen:
            raise ValueError(f"golden row {row_id}: duplicate id")
        seen.add(row_id)
        rows.append(
            GoldenRow(
                row_id=row_id,
                domain=str(item.get("domain") or "general"),
                query=query,
                expected_ids=tuple(str(e) for e in expected),
                notes=str(item.get("notes") or ""),
            )
        )
    return rows


def resolve_golden(
    rows: Sequence[GoldenRow], corpus: Sequence[Chunk]
) -> List[ScoredRow]:
    """Resolve each row's expected ids against the corpus (stale ids dropped)."""
    known = {c.stable_id for c in corpus}
    scored: List[ScoredRow] = []
    for row in rows:
        ok = tuple(e for e in row.expected_ids if e in known)
        stale = tuple(e for e in row.expected_ids if e not in known)
        scored.append(ScoredRow(row=row, resolved_ids=ok, stale_ids=stale))
    return scored


# ---------------------------------------------------------------------------
# Metrics (pure)
# ---------------------------------------------------------------------------


def first_hit_rank(ranked: Sequence[str], expected: Sequence[str]) -> Optional[int]:
    """1-based rank of the first expected id in ``ranked``, or None."""
    wanted = set(expected)
    for idx, doc in enumerate(ranked, start=1):
        if doc in wanted:
            return idx
    return None


def recall_at_k(ranked: Sequence[str], expected: Sequence[str], k: int) -> float:
    """1.0 if any expected id is in the top ``k``, else 0.0."""
    rank = first_hit_rank(ranked[:k], expected)
    return 1.0 if rank is not None else 0.0


def reciprocal_rank(
    ranked: Sequence[str], expected: Sequence[str], depth: int = MAX_DEPTH
) -> float:
    """1 / rank of the first expected id within ``depth``, else 0.0."""
    rank = first_hit_rank(ranked[:depth], expected)
    return 1.0 / rank if rank is not None else 0.0


def aggregate_metrics(
    per_query: Sequence[Dict[str, Any]],
) -> Dict[str, float]:
    """Mean recall@k and MRR over per-query records, as percentage points."""
    n = len(per_query)
    if n == 0:
        return {name: 0.0 for name in METRIC_NAMES}
    out: Dict[str, float] = {}
    for k in K_VALUES:
        out[f"recall@{k}"] = round(
            100.0 * sum(float(q[f"recall@{k}"]) for q in per_query) / n, 2
        )
    out["mrr"] = round(100.0 * sum(float(q["rr"]) for q in per_query) / n, 2)
    return out


def score_rankings(
    scored: Sequence[ScoredRow], rankings: Dict[str, List[str]]
) -> List[Dict[str, Any]]:
    """Per-query metric records for one tier."""
    records: List[Dict[str, Any]] = []
    for item in scored:
        ranked = rankings.get(item.row.row_id) or []
        rec: Dict[str, Any] = {
            "id": item.row.row_id,
            "domain": item.row.domain,
            "query": item.row.query,
            "first_hit_rank": first_hit_rank(ranked[:MAX_DEPTH], item.resolved_ids),
            "rr": reciprocal_rank(ranked, item.resolved_ids),
            "top3": list(ranked[:3]),
        }
        for k in K_VALUES:
            rec[f"recall@{k}"] = recall_at_k(ranked, item.resolved_ids, k)
        records.append(rec)
    return records


def metrics_by_domain(
    per_query: Sequence[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """recall@5 / mrr per domain (informational; never gated)."""
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for rec in per_query:
        groups.setdefault(str(rec["domain"]), []).append(rec)
    out: Dict[str, Dict[str, Any]] = {}
    for domain, recs in sorted(groups.items()):
        agg = aggregate_metrics(recs)
        out[domain] = {
            "queries": len(recs),
            "recall@5": agg["recall@5"],
            "mrr": agg["mrr"],
        }
    return out


# ---------------------------------------------------------------------------
# Regression check (pure -- mirrors scripts/eval_gate._check_regression)
# ---------------------------------------------------------------------------


def check_regression(
    current: Dict[str, Any],
    baseline: Optional[Dict[str, Any]],
    max_drop: float = MAX_REGRESSION,
) -> List[Dict[str, Any]]:
    """Violations where an ``ok`` tier fell more than ``max_drop`` points.

    Only tiers/metrics present as ``ok`` in BOTH reports are compared, so a
    tier that is skipped here (no key in CI) or newly added never fails the
    gate.  Exactly ``max_drop`` is allowed (strictly greater fails).
    """
    if not baseline:
        return []
    violations: List[Dict[str, Any]] = []
    cur_tiers = current.get("tiers") or {}
    base_tiers = baseline.get("tiers") or {}
    for tier_name, base in sorted(base_tiers.items()):
        cur = cur_tiers.get(tier_name)
        if not cur or cur.get("status") != "ok" or base.get("status") != "ok":
            continue
        for metric, base_val in sorted((base.get("metrics") or {}).items()):
            cur_val = (cur.get("metrics") or {}).get(metric)
            if cur_val is None:
                continue
            drop = round(float(base_val) - float(cur_val), 2)
            if drop > max_drop:
                violations.append(
                    {
                        "tier": tier_name,
                        "metric": metric,
                        "baseline": float(base_val),
                        "current": float(cur_val),
                        "drop": drop,
                        "max_drop": max_drop,
                        "message": (
                            f"{tier_name} {metric} dropped {drop:.2f} pts "
                            f"({float(base_val):.2f} -> {float(cur_val):.2f}), "
                            f"exceeding the allowed {max_drop:.1f} pt regression."
                        ),
                    }
                )
    return violations


# ---------------------------------------------------------------------------
# Tiers
# ---------------------------------------------------------------------------


class Tier:
    """A retrieval tier: ``search`` returns ranked STABLE ids, best first."""

    name = "tier"

    def prepare_queries(self, queries: Sequence[str]) -> None:
        """Hook to batch query-side work (embedding) before searching."""

    def search(self, query: str, depth: int) -> List[str]:  # pragma: no cover
        raise NotImplementedError

    def __enter__(self) -> "Tier":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        return None


def _docs_for_index(corpus: Sequence[Chunk]) -> List[Dict[str, Any]]:
    return [
        {"id": c.stable_id, "text": c.text, "metadata": {"source": c.source}}
        for c in corpus
    ]


class BM25Tier(Tier):
    """Production BM25 over the corpus (own ``BM25Index`` instance)."""

    name = "bm25"

    def __init__(self, corpus: Sequence[Chunk]) -> None:
        self._index = vs.BM25Index()
        self._index.index(_docs_for_index(corpus))

    def search(self, query: str, depth: int) -> List[str]:
        return [doc_id for doc_id, _ in self._index.search(query, top_k=depth)]


_TFIDF_LISTS = (
    "_tfidf_index",
    "_tfidf_doc_texts",
    "_tfidf_doc_ids",
    "_tfidf_doc_meta",
)


@contextmanager
def _tfidf_state_guard() -> Iterator[None]:
    """Snapshot/restore ``vector_search``'s module-level TF-IDF state.

    ``_build_tfidf_index`` mutates module globals in place; the eval must never
    leave the eval corpus behind for other code (or tests) in the process.
    """
    saved_lists = {n: list(getattr(vs, n)) for n in _TFIDF_LISTS}
    saved_idf = dict(vs._tfidf_idf)
    saved_built = vs._tfidf_built
    try:
        yield
    finally:
        for name, saved in saved_lists.items():
            getattr(vs, name)[:] = saved
        vs._tfidf_idf.clear()
        vs._tfidf_idf.update(saved_idf)
        vs._tfidf_built = saved_built


class TfidfTier(Tier):
    """Production TF-IDF fallback.  Use as a context manager (state guard)."""

    name = "tfidf"

    def __init__(self, corpus: Sequence[Chunk]) -> None:
        self._corpus = corpus
        self._guard: Any = None

    def __enter__(self) -> "TfidfTier":
        self._guard = _tfidf_state_guard()
        self._guard.__enter__()
        vs._build_tfidf_index(_docs_for_index(self._corpus))
        return self

    def __exit__(self, *exc_info: Any) -> None:
        if self._guard is not None:
            self._guard.__exit__(*exc_info)
            self._guard = None

    def search(self, query: str, depth: int) -> List[str]:
        return [r["id"] for r in vs._tfidf_search(query, top_k=depth)]


class _DenseIndex:
    """Normalised dense vectors with cosine top-k (numpy when available)."""

    def __init__(self, vectors: Sequence[Sequence[float]]) -> None:
        self._np: Any = None
        try:
            import numpy as np  # noqa: WPS433 -- optional accelerator

            self._np = np
            mat = np.asarray(vectors, dtype="float64")
            norms = np.linalg.norm(mat, axis=1, keepdims=True)
            norms[norms == 0] = 1.0
            self._mat = mat / norms
        except ImportError:
            self._mat = [self._unit(v) for v in vectors]

    @staticmethod
    def _unit(vec: Sequence[float]) -> List[float]:
        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        return [x / norm for x in vec]

    def top_k(self, query_vec: Sequence[float], k: int) -> List[int]:
        if self._np is not None:
            q = self._np.asarray(query_vec, dtype="float64")
            q = q / (self._np.linalg.norm(q) or 1.0)
            # .dot, not `@`: numpy 2.0.x on macOS (Accelerate) raises spurious
            # divide-by-zero/overflow RuntimeWarnings from matmul on valid input.
            sims = self._mat.dot(q)
            order = self._np.argsort(-sims, kind="stable")[:k]
            return [int(i) for i in order]
        q = self._unit(query_vec)
        sims = [sum(map(operator.mul, row, q)) for row in self._mat]
        order = sorted(range(len(sims)), key=lambda i: -sims[i])[:k]
        return order


class VectorTier(Tier):
    """Dense cosine retrieval over chunk embeddings from an injected embedder."""

    name = "vector"

    def __init__(self, corpus: Sequence[Chunk], embedder: "Embedder") -> None:
        self._ids = [c.stable_id for c in corpus]
        doc_vecs = embedder.embed([c.text for c in corpus], "document")
        if len(doc_vecs) != len(corpus):
            raise EmbedderUnavailable(
                f"embedder returned {len(doc_vecs)} vectors for {len(corpus)} chunks"
            )
        self._embedder = embedder
        self._index = _DenseIndex(doc_vecs)
        self._qvec: Dict[str, List[float]] = {}

    def prepare_queries(self, queries: Sequence[str]) -> None:
        todo = [q for q in dict.fromkeys(queries) if q not in self._qvec]
        if not todo:
            return
        vecs = self._embedder.embed(todo, "query")
        if len(vecs) != len(todo):
            raise EmbedderUnavailable(
                f"embedder returned {len(vecs)} vectors for {len(todo)} queries"
            )
        self._qvec.update(zip(todo, vecs))

    def search(self, query: str, depth: int) -> List[str]:
        qv = self._qvec.get(query)
        if qv is None:
            self.prepare_queries([query])
            qv = self._qvec[query]
        return [self._ids[i] for i in self._index.top_k(qv, depth)]


class HybridTier(Tier):
    """Vector + BM25 fused with RRF k=60, as ``vector_search.search`` does."""

    name = "hybrid_rrf"

    def __init__(self, vector: VectorTier, bm25: BM25Tier) -> None:
        self._vector = vector
        self._bm25 = bm25

    def prepare_queries(self, queries: Sequence[str]) -> None:
        self._vector.prepare_queries(queries)

    def search(self, query: str, depth: int) -> List[str]:
        v = [(d, 0.0) for d in self._vector.search(query, HYBRID_FETCH_K)]
        b = [(d, 0.0) for d in self._bm25.search(query, HYBRID_FETCH_K)]
        fused = vs.reciprocal_rank_fusion(v, b, k=60)
        return [doc_id for doc_id, _ in fused[:depth]]


# ---------------------------------------------------------------------------
# Embedders
# ---------------------------------------------------------------------------


class Embedder:
    """Protocol-ish base: ``name`` + ``embed(texts, input_type)``."""

    name = "embedder"

    def embed(
        self, texts: Sequence[str], input_type: str
    ) -> List[List[float]]:  # pragma: no cover
        raise NotImplementedError


def _repo_cache_key(model: str, text: str, input_type: Optional[str]) -> str:
    """Replicates ``vector_search._text_cache_key`` for an explicit model."""
    segment = f"{input_type}:" if input_type else ""
    content = f"{model}:{segment}{text}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


class RepoCacheEmbedder(Embedder):
    """Serve vectors from the repo's ``data/.embedding_cache.json``.

    All-or-nothing: if any requested text is absent the embedder raises
    ``EmbedderUnavailable`` (a half-covered corpus would report misleading
    recall).  ``voyage-3-lite`` entries are untyped (see
    ``vector_search._voyage_input_type``); every other model is keyed with its
    input_type.
    """

    def __init__(
        self,
        model: str = "voyage-3-lite",
        path: Path = DEFAULT_REPO_CACHE_PATH,
    ) -> None:
        self.name = f"repo-cache:{model}"
        self._model = model
        self._path = Path(path)
        self._cache: Optional[Dict[str, List[float]]] = None

    def _load(self) -> Dict[str, List[float]]:
        if self._cache is None:
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                data = {}
            self._cache = data if isinstance(data, dict) else {}
        return self._cache

    def _typed(self, input_type: str) -> Optional[str]:
        return None if self._model == "voyage-3-lite" else input_type

    def coverage(self, texts: Sequence[str], input_type: str) -> Tuple[int, int]:
        """(found, total) for ``texts`` -- used to explain a SKIPPED tier."""
        cache = self._load()
        typed = self._typed(input_type)
        found = sum(
            1 for t in texts if _repo_cache_key(self._model, t[:8000], typed) in cache
        )
        return found, len(texts)

    def embed(self, texts: Sequence[str], input_type: str) -> List[List[float]]:
        cache = self._load()
        typed = self._typed(input_type)
        out: List[List[float]] = []
        for text in texts:
            vec = cache.get(_repo_cache_key(self._model, text[:8000], typed))
            if vec is None:
                found, total = self.coverage(texts, input_type)
                raise EmbedderUnavailable(
                    f"{found}/{total} {input_type} texts present in "
                    f"{self._path.name} for {self._model}"
                )
            out.append(vec)
        return out


class VoyageEmbedder(Embedder):
    """Real Voyage API embedder with pacing, 429 retry and a resumable cache.

    Independent of ``vector_search``'s rate limiter and its tracked
    ``data/.embedding_cache.json`` on purpose: an eval run must not write
    experimental vectors into the production cache, and it needs an explicit
    ``model`` so plan step 6 can A/B models on the same golden set.
    """

    API_URL = "https://api.voyageai.com/v1/embeddings"

    def __init__(
        self,
        model: str = "voyage-4-lite",
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        batch_size: int = 32,
        rpm: float = 10.0,
        cache_path: Optional[Path] = DEFAULT_EMBED_CACHE_PATH,
        flush_every: int = 20,
        max_retries: int = 3,
        opener: Callable[..., Any] = urllib.request.urlopen,
        sleeper: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = f"voyage:{model}"
        self._model = model
        self._api_key = api_key or os.environ.get("VOYAGE_API_KEY") or ""
        self._url = (base_url or os.environ.get("VOYAGE_BASE_URL") or "").rstrip("/")
        self._url = f"{self._url}/embeddings" if self._url else self.API_URL
        self._batch = max(1, int(batch_size))
        self._min_interval = 60.0 / rpm if rpm and rpm > 0 else 0.0
        self._cache_path = Path(cache_path) if cache_path else None
        self._flush_every = max(1, int(flush_every))
        self._max_retries = max_retries
        self._opener = opener
        self._sleep = sleeper
        self._clock = clock
        self._last_request = -1e18
        self._cache: Optional[Dict[str, List[float]]] = None
        self._dirty = 0
        self.requests_sent = 0

    # -- cache ------------------------------------------------------------
    def _key(self, text: str, input_type: Optional[str]) -> str:
        raw = f"{self._model}|{input_type or ''}|{text}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    def _load(self) -> Dict[str, List[float]]:
        if self._cache is None:
            self._cache = {}
            if self._cache_path is not None and self._cache_path.exists():
                try:
                    data = json.loads(self._cache_path.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        self._cache = data
                except (OSError, ValueError):
                    logger.error(
                        "ignoring unreadable embed cache %s",
                        self._cache_path,
                        exc_info=True,
                    )
        return self._cache

    def _flush(self) -> None:
        if self._cache_path is None or self._cache is None or not self._dirty:
            return
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=str(self._cache_path.parent), suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self._cache, fh)
            os.replace(tmp, self._cache_path)
            self._dirty = 0
        except OSError:
            logger.error("could not write embed cache", exc_info=True)
            try:
                os.unlink(tmp)
            except OSError:
                pass

    # -- network ----------------------------------------------------------
    def _pace(self) -> None:
        wait = self._min_interval - (self._clock() - self._last_request)
        if wait > 0:
            self._sleep(wait)
        self._last_request = self._clock()

    def _post(self, batch: List[str], input_type: Optional[str]) -> List[List[float]]:
        payload: Dict[str, Any] = {"input": batch, "model": self._model}
        if input_type:
            payload["input_type"] = input_type
        body = json.dumps(payload).encode("utf-8")
        for attempt in range(self._max_retries + 1):
            self._pace()
            req = urllib.request.Request(
                self._url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self._api_key}",
                },
                method="POST",
            )
            try:
                with self._opener(req, timeout=30) as resp:
                    parsed = json.loads(resp.read().decode("utf-8"))
                self.requests_sent += 1
                items = sorted(
                    parsed.get("data") or [], key=lambda d: d.get("index", 0)
                )
                vecs = [d.get("embedding") or [] for d in items]
                if len(vecs) != len(batch) or any(not v for v in vecs):
                    raise EmbedderUnavailable(
                        f"voyage returned {len(vecs)} vectors for {len(batch)} inputs"
                    )
                return vecs
            except urllib.error.HTTPError as exc:
                if exc.code == 429 and attempt < self._max_retries:
                    retry_after = (
                        exc.headers.get("Retry-After") if exc.headers else None
                    )
                    try:
                        delay = float(retry_after) if retry_after else 2.0 * 2**attempt
                    except ValueError:
                        delay = 2.0 * 2**attempt
                    self._sleep(delay)
                    continue
                detail = ""
                try:
                    detail = exc.read().decode("utf-8", errors="replace")[:200]
                except (OSError, ValueError):
                    pass
                raise EmbedderUnavailable(f"voyage HTTP {exc.code}: {detail}") from exc
            except (urllib.error.URLError, OSError, ValueError) as exc:
                raise EmbedderUnavailable(f"voyage request failed: {exc}") from exc
        raise EmbedderUnavailable("voyage: retries exhausted")  # pragma: no cover

    def embed(self, texts: Sequence[str], input_type: str) -> List[List[float]]:
        if not self._api_key:
            raise EmbedderUnavailable(
                f"{SKIP_NO_KEY} -- VOYAGE_API_KEY is not set "
                "(export it, or pass api_key=)"
            )
        # voyage-3-lite is the frozen untyped legacy space (see vector_search).
        typed: Optional[str] = None if self._model == "voyage-3-lite" else input_type
        cache = self._load()
        clipped = [t[:8000] for t in texts]
        keys = [self._key(t, typed) for t in clipped]
        missing = [i for i, k in enumerate(keys) if k not in cache]
        try:
            for start in range(0, len(missing), self._batch):
                idxs = missing[start : start + self._batch]
                vecs = self._post([clipped[i] for i in idxs], typed)
                for i, vec in zip(idxs, vecs):
                    cache[keys[i]] = vec
                self._dirty += len(idxs)
                if (start // self._batch + 1) % self._flush_every == 0:
                    self._flush()
        finally:
            self._flush()
        return [cache[k] for k in keys]


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def _skip(reason: str) -> Dict[str, Any]:
    return {"status": "skipped", "reason": reason}


def _run_tier(
    tier: Tier, scored: Sequence[ScoredRow]
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    queries = [s.row.query for s in scored]
    tier.prepare_queries(queries)
    rankings = {s.row.row_id: tier.search(s.row.query, MAX_DEPTH) for s in scored}
    per_query = score_rankings(scored, rankings)
    result = {
        "status": "ok",
        "metrics": aggregate_metrics(per_query),
        "by_domain": metrics_by_domain(per_query),
    }
    return result, per_query


def _no_embedder_reason(
    corpus: Sequence[Chunk], scored: Sequence[ScoredRow], cache_path: Path
) -> str:
    """Explain a skipped dense tier, with real numbers about the repo cache."""
    probe = RepoCacheEmbedder("voyage-3-lite", cache_path)
    q_found, q_total = probe.coverage([s.row.query for s in scored], "query")
    d_found, d_total = probe.coverage([c.text for c in corpus], "document")
    return (
        f"{SKIP_NO_KEY} -- no embedder provided and {cache_path.name} (voyage-3-lite, "
        f"512-d) holds {q_found}/{q_total} golden queries and {d_found}/{d_total} "
        "chunks; run with --voyage and VOYAGE_API_KEY (see module docstring)"
    )


def select_embedder(
    corpus: Sequence[Chunk],
    scored: Sequence[ScoredRow],
    embedder: Optional[Embedder] = None,
    repo_cache_path: Path = DEFAULT_REPO_CACHE_PATH,
) -> Tuple[Optional[Embedder], Optional[str]]:
    """Return ``(embedder, None)`` or ``(None, skip_reason)``.

    An injected embedder always wins.  Otherwise the repo cache is used only if
    it covers every golden query and every chunk.
    """
    if embedder is not None:
        return embedder, None
    probe = RepoCacheEmbedder("voyage-3-lite", repo_cache_path)
    q_found, q_total = probe.coverage([s.row.query for s in scored], "query")
    d_found, d_total = probe.coverage([c.text for c in corpus], "document")
    if q_found == q_total and d_found == d_total and q_total and d_total:
        return probe, None
    return None, _no_embedder_reason(corpus, scored, repo_cache_path)


def run_eval(
    golden_path: Path = DEFAULT_GOLDEN_PATH,
    data_dir: Optional[Path] = None,
    embedder: Optional[Embedder] = None,
    tiers: Sequence[str] = TIER_NAMES,
    repo_cache_path: Path = DEFAULT_REPO_CACHE_PATH,
    kb_files: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Run the eval and return the full report (including per-query records)."""
    started = time.monotonic()
    corpus = build_corpus(data_dir, kb_files)
    rows = load_golden(golden_path)
    scored = resolve_golden(rows, corpus)
    scorable = [s for s in scored if s.resolved_ids]
    stale = sorted({e for s in scored for e in s.stale_ids})

    report: Dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": date.today().isoformat(),
        "metric_units": "percentage points (0-100); mrr = MRR@10 x 100",
        "k_values": list(K_VALUES),
        "corpus": {
            "chunks": len(corpus),
            "kb_files": len({c.source for c in corpus}),
            "golden_rows": len(rows),
            "scorable_rows": len(scorable),
            "stale_expected_ids": stale,
        },
        "tiers": {},
        "per_query": {},
    }
    unknown = [t for t in tiers if t not in TIER_NAMES]
    if unknown:
        raise ValueError(f"unknown tier(s): {', '.join(unknown)}")
    if not scorable:
        raise ValueError("no golden row resolves against the current corpus")

    def record(name: str, tier_cm: Callable[[], Tier]) -> None:
        with tier_cm() as tier:
            result, per_query = _run_tier(tier, scorable)
        report["tiers"][name] = result
        report["per_query"][name] = per_query

    bm25_cache: List[BM25Tier] = []

    def bm25() -> BM25Tier:
        if not bm25_cache:
            bm25_cache.append(BM25Tier(corpus))
        return bm25_cache[0]

    if "bm25" in tiers:
        record("bm25", bm25)
    if "tfidf" in tiers:
        record("tfidf", lambda: TfidfTier(corpus))

    wants_dense = [t for t in ("vector", "hybrid_rrf") if t in tiers]
    if wants_dense:
        chosen, reason = select_embedder(corpus, scorable, embedder, repo_cache_path)
        if chosen is None:
            for name in wants_dense:
                report["tiers"][name] = _skip(str(reason))
        else:
            try:
                vector = VectorTier(corpus, chosen)
                vector.prepare_queries([s.row.query for s in scorable])
                report["embedder"] = chosen.name
                if "vector" in tiers:
                    record("vector", lambda: vector)
                if "hybrid_rrf" in tiers:
                    record("hybrid_rrf", lambda: HybridTier(vector, bm25()))
            except EmbedderUnavailable as exc:
                for name in wants_dense:
                    report["tiers"].setdefault(name, _skip(str(exc)))

    report["elapsed_seconds"] = round(time.monotonic() - started, 2)
    return report


def baseline_view(report: Dict[str, Any]) -> Dict[str, Any]:
    """The committed-baseline shape: metrics only (no per-query records)."""
    out = {k: v for k, v in report.items() if k not in ("per_query", "elapsed_seconds")}
    return out


# ---------------------------------------------------------------------------
# Worst-query diagnosis + presentation
# ---------------------------------------------------------------------------


def worst_queries(
    report: Dict[str, Any], tier: str, scored_ids: Dict[str, ScoredRow], n: int = 5
) -> List[Dict[str, Any]]:
    """The ``n`` worst-recalled queries for ``tier`` with a diagnosis string."""
    records = list((report.get("per_query") or {}).get(tier) or [])
    if not records:
        return []
    corpus_text: Dict[str, str] = {}
    for chunk in build_corpus():
        corpus_text.setdefault(chunk.stable_id, chunk.text)

    def badness(rec: Dict[str, Any]) -> Tuple[float, str]:
        rank = rec["first_hit_rank"]
        return (float("inf") if rank is None else float(rank), rec["id"])

    out: List[Dict[str, Any]] = []
    for rec in sorted(records, key=badness, reverse=True)[:n]:
        item = scored_ids[rec["id"]]
        q_terms = sorted(set(vs._tokenize(rec["query"])))
        best_hits = 0
        for sid in item.resolved_ids:
            body = set(vs._tokenize(corpus_text.get(sid, "")))
            best_hits = max(best_hits, sum(1 for t in q_terms if t in body))
        rank = rec["first_hit_rank"]
        if best_hits == 0:
            why = "vocabulary gap: no query term appears in any expected chunk"
        elif rank is None:
            why = (
                f"expected chunk shares {best_hits}/{len(q_terms)} query terms but "
                "is outranked by >10 chunks"
            )
        else:
            why = (
                f"expected chunk shares {best_hits}/{len(q_terms)} query terms; "
                f"first hit at rank {rank}"
            )
        out.append(
            {
                "id": rec["id"],
                "query": rec["query"],
                "first_hit_rank": rank,
                "expected": list(item.resolved_ids),
                "top3": rec["top3"],
                "why": why,
            }
        )
    return out


def format_table(report: Dict[str, Any]) -> str:
    """Human-readable metrics table (explicit SKIPPED rows included)."""
    corpus = report["corpus"]
    lines = [
        f"Retrieval eval -- {corpus['scorable_rows']}/{corpus['golden_rows']} "
        f"golden rows scorable over {corpus['chunks']} chunks "
        f"({corpus['kb_files']} KB files)",
        "",
        f"{'tier':<12} " + " ".join(f"{m:>9}" for m in METRIC_NAMES),
        "-" * (13 + 10 * len(METRIC_NAMES)),
    ]
    for name in TIER_NAMES:
        tier = report["tiers"].get(name)
        if tier is None:
            continue
        if tier.get("status") != "ok":
            lines.append(f"{name:<12} {tier.get('reason', 'SKIPPED')}")
            continue
        m = tier["metrics"]
        lines.append(f"{name:<12} " + " ".join(f"{m[x]:>9.2f}" for x in METRIC_NAMES))
    if corpus.get("stale_expected_ids"):
        lines.append("")
        lines.append(
            f"WARNING: {len(corpus['stale_expected_ids'])} expected id(s) no longer "
            "resolve in the KB (run --audit-golden)"
        )
    lines.append("")
    lines.append("metrics are percentage points; mrr = MRR@10 x 100")
    return "\n".join(lines)


def audit_golden(
    golden_path: Path = DEFAULT_GOLDEN_PATH, data_dir: Optional[Path] = None
) -> Tuple[str, int]:
    """Return (text, stale_count): each expected id with its chunk head."""
    corpus = build_corpus(data_dir)
    by_id: Dict[str, Chunk] = {}
    for c in corpus:
        by_id.setdefault(c.stable_id, c)
    out: List[str] = []
    stale = 0
    for row in load_golden(golden_path):
        out.append(f"[{row.row_id}] ({row.domain}) {row.query}")
        for sid in row.expected_ids:
            chunk = by_id.get(sid)
            if chunk is None:
                stale += 1
                out.append(f"    STALE  {sid}")
                continue
            body = chunk.text.split("\n", 1)
            head = (body[1] if len(body) > 1 else body[0])[:110].replace("\n", " | ")
            out.append(f"    ok     {sid}\n           {head}")
    return "\n".join(out), stale


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _load_baseline(path: Path) -> Optional[Dict[str, Any]]:
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return raw if isinstance(raw, dict) else None


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Offline retrieval eval (recall@k / MRR) for the Nova KB."
    )
    p.add_argument("--golden", type=Path, default=DEFAULT_GOLDEN_PATH)
    p.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE_PATH)
    p.add_argument("--check", action="store_true", help="fail on >3pt regression")
    p.add_argument(
        "--update-baseline",
        action="store_true",
        help="rewrite the committed baseline from this run (no gating)",
    )
    p.add_argument("--report", type=Path, help="write the full JSON report here")
    p.add_argument("--worst", type=int, default=5, help="show N worst queries")
    p.add_argument(
        "--tiers", default=",".join(TIER_NAMES), help="comma list of tiers to run"
    )
    p.add_argument("--audit-golden", action="store_true")
    p.add_argument("--voyage", action="store_true", help="embed with the Voyage API")
    p.add_argument("--voyage-model", default="voyage-4-lite")
    p.add_argument("--voyage-rpm", type=float, default=10.0)
    p.add_argument("--voyage-batch", type=int, default=32)
    p.add_argument("--embed-cache", type=Path, default=DEFAULT_EMBED_CACHE_PATH)
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

    if args.audit_golden:
        try:
            text, stale = audit_golden(args.golden)
        except ValueError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return EXIT_ERROR
        print(text)
        print(f"\n{stale} stale expected id(s)")
        return EXIT_FAIL if stale else EXIT_PASS

    embedder: Optional[Embedder] = None
    if args.voyage:
        embedder = VoyageEmbedder(
            model=args.voyage_model,
            rpm=args.voyage_rpm,
            batch_size=args.voyage_batch,
            cache_path=args.embed_cache,
        )

    tiers = [t.strip() for t in args.tiers.split(",") if t.strip()]
    try:
        report = run_eval(args.golden, embedder=embedder, tiers=tiers)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return EXIT_ERROR

    print(format_table(report))
    scorable = {
        s.row.row_id: s
        for s in resolve_golden(load_golden(args.golden), build_corpus())
    }
    for tier_name in ("bm25", "tfidf"):
        worst = worst_queries(report, tier_name, scorable, args.worst)
        if worst:
            print(f"\nWorst {len(worst)} queries ({tier_name}):")
            for w in worst:
                rank = w["first_hit_rank"] or ">10"
                print(f"  [{w['id']}] rank={rank}  {w['query']}\n      {w['why']}")

    if args.report:
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if args.update_baseline:
        args.baseline.write_text(
            json.dumps(baseline_view(report), indent=2) + "\n", encoding="utf-8"
        )
        print(f"\nbaseline written: {args.baseline}")
        return EXIT_PASS
    if args.check:
        baseline = _load_baseline(args.baseline)
        if baseline is None:
            print(
                f"ERROR: no readable baseline at {args.baseline}; "
                "run with --update-baseline first",
                file=sys.stderr,
            )
            return EXIT_ERROR
        violations = check_regression(report, baseline)
        for tier_name, base in sorted((baseline.get("tiers") or {}).items()):
            cur = report["tiers"].get(tier_name) or {}
            if base.get("status") == "ok" and cur.get("status") != "ok":
                print(
                    f"NOTE: {tier_name} not compared ({cur.get('reason') or 'not run'})"
                )
        if violations:
            print("\nRETRIEVAL GATE: FAIL")
            for v in violations:
                print(f"  - {v['message']}")
            return EXIT_FAIL
        print("\nRETRIEVAL GATE: PASS")
    return EXIT_PASS


if __name__ == "__main__":
    sys.exit(main())
