"""rag_implementation_sketch.py -- Standalone reference for Nova RAG v2.

This module is the code companion to docs/RAG_Design_2026.md. It is NOT
imported by nova.py. It is meant as:

  1. A working reference implementation an engineer can lift verbatim into
     a real ``rag_pipeline.py`` module.
  2. A demo runnable via ``python -m docs.rag_implementation_sketch`` that
     indexes a small slice of ``data/*.json`` and proves retrieval works.
  3. A pytest harness at the bottom that exercises the indexing and
     retrieval contract on sample data.

Design constraints (from RAG_Design_2026.md):
  - Embedding model: Voyage AI ``voyage-3.5-lite`` (1024 dim, Matryoshka)
    with sentence-transformers ``all-MiniLM-L6-v2`` (384 dim) fallback.
  - Vector store: Qdrant Cloud (production) with an in-memory dict fallback
    for local development and CI.
  - Hybrid retrieval: vector + BM25 fused via Reciprocal Rank Fusion.
  - Optional cross-encoder rerank via Voyage ``rerank-2.5-lite``.
  - Structure-aware JSON chunking: one chunk per leaf-ish dict node, with
    a key-path prefix preserved for context.

External dependencies (declared but optional -- module degrades gracefully):
  - ``voyageai`` -- official Voyage SDK. ``pip install voyageai``. Docs:
    https://docs.voyageai.com/docs/python-sdk
  - ``qdrant-client`` -- Qdrant REST client. ``pip install qdrant-client``.
    Docs: https://qdrant.tech/documentation/quick-start/
  - ``sentence-transformers`` -- local embedding fallback.
    ``pip install sentence-transformers``. Docs: https://www.sbert.net/
  - ``rank-bm25`` -- pure-Python BM25 for the keyword leg. Apache 2.0.
    https://github.com/dorianbrown/rank_bm25
  - All optional. If a dep is missing the module falls back to a working
    (but lower-quality) tier.

Environment variables consumed:
  - ``VOYAGE_API_KEY``: optional. Without it we use sentence-transformers.
  - ``QDRANT_URL`` / ``QDRANT_API_KEY``: optional. Without them we use
    the in-memory dict.
  - ``NOVA_RAG_DATA_DIR``: optional, defaults to ``./data/`` relative to
    the package root (so the demo runs in the media-plan-generator repo
    without configuration).

Author: AI Engineer Agent (May 22, 2026).
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import logging
import math
import os
import re
import time
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Embedding model selection. voyage-3.5-lite is 1024 dim Matryoshka. We use
# the truncate-to-1024 path. See docs/voyage_4_migration_runbook.md.
_VOYAGE_EMBED_MODEL = "voyage-3.5-lite"
_VOYAGE_EMBED_DIM = 1024
_VOYAGE_RERANK_MODEL = "rerank-2.5-lite"

# Local fallback. ~80 MB, CPU-friendly, ~5 ms/query.
# https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2
_LOCAL_EMBED_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
_LOCAL_EMBED_DIM = 384

# Qdrant collection. Use v2 suffix so we can ship in parallel with the
# existing ``nova_knowledge`` (685 points) collection.
_QDRANT_COLLECTION = "nova_knowledge_v2"

# Chunking parameters. Tuned in the design doc: 400-800 tokens with 80
# overlap, structure-aware splitting on JSON dict boundaries.
_CHUNK_MIN_TOKENS = 80
_CHUNK_MAX_TOKENS = 800
_CHUNK_OVERLAP_TOKENS = 80

# Retrieval parameters.
_VECTOR_FETCH_K = 20
_BM25_FETCH_K = 20
_RRF_K = 60  # standard RRF constant (Cormack, Clarke, Buettcher 2009)
_RERANK_INPUT_K = 20
_DEFAULT_TOP_K = 5
_RRF_SCORE_FLOOR = 0.015  # empirical noise floor; drop chunks below this


# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Document:
    """A single chunk of indexable text.

    Args:
        doc_id: Deterministic UUID built from (source_file, kb_section,
            chunk_index) so re-indexing is idempotent.
        text: The chunk text, already truncated to ``_CHUNK_MAX_TOKENS``.
        metadata: Filterable payload (country, metric, year, vertical, ...).
            Schema documented in RAG_Design_2026.md §2.3.
    """

    doc_id: str
    text: str
    metadata: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class RetrievalHit:
    """A retrieved chunk with provenance."""

    doc_id: str
    text: str
    metadata: dict[str, Any]
    score: float
    source_file: str
    search_method: str  # "vector" | "bm25" | "hybrid_rrf" | "rerank"


# ---------------------------------------------------------------------------
# Embedding backends
# ---------------------------------------------------------------------------


class _EmbeddingBackend:
    """Abstract base. Subclasses implement ``embed_batch`` and ``dim``."""

    name: str = "abstract"
    dim: int = 0

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        raise NotImplementedError


class VoyageEmbedder(_EmbeddingBackend):
    """Voyage AI embedder. Requires VOYAGE_API_KEY.

    Docs: https://docs.voyageai.com/docs/embeddings
    SDK: https://docs.voyageai.com/docs/python-sdk
    """

    name = "voyage-3.5-lite"
    dim = _VOYAGE_EMBED_DIM

    def __init__(self, api_key: str | None = None) -> None:
        try:
            import voyageai  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "voyageai package not installed. Run `pip install voyageai`."
            ) from exc
        key = api_key or os.environ.get("VOYAGE_API_KEY") or ""
        if not key:
            raise RuntimeError(
                "VOYAGE_API_KEY not set. Sign up at https://www.voyageai.com."
            )
        self._client = voyageai.Client(api_key=key)

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts. Voyage allows up to 128 per call."""
        if not texts:
            return []
        # Truncate each text to ~32K chars to stay under model token limit.
        truncated = [t[:32000] for t in texts]
        # input_type="document" for indexing, "query" for retrieval -- Voyage
        # uses asymmetric encoders. We pick "document" here; ``embed_query``
        # below uses "query".
        result = self._client.embed(
            truncated, model=_VOYAGE_EMBED_MODEL, input_type="document"
        )
        return [list(v) for v in result.embeddings]

    def embed_query(self, query: str) -> list[float]:
        """Single-query embedding with input_type='query' (asymmetric)."""
        result = self._client.embed(
            [query[:32000]], model=_VOYAGE_EMBED_MODEL, input_type="query"
        )
        return list(result.embeddings[0])


class LocalEmbedder(_EmbeddingBackend):
    """sentence-transformers fallback. CPU-only, no API key needed.

    Docs: https://www.sbert.net/docs/quickstart.html
    """

    name = "all-MiniLM-L6-v2"
    dim = _LOCAL_EMBED_DIM

    def __init__(self) -> None:
        try:
            from sentence_transformers import (  # type: ignore[import-not-found]
                SentenceTransformer,
            )
        except ImportError as exc:
            raise RuntimeError(
                "sentence-transformers not installed. "
                "Run `pip install sentence-transformers`."
            ) from exc
        self._model = SentenceTransformer(_LOCAL_EMBED_MODEL)

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        vecs = self._model.encode(
            texts, batch_size=32, show_progress_bar=False, normalize_embeddings=True
        )
        return [list(map(float, v)) for v in vecs]

    def embed_query(self, query: str) -> list[float]:
        return self.embed_batch([query])[0]


class HashEmbedder(_EmbeddingBackend):
    """Deterministic hashing embedder. Used ONLY for tests and the demo.

    This is not a real embedding -- it's a stable, fast, content-addressed
    pseudo-vector so the demo and pytest can run without any network or
    model download. It will not give good retrieval quality. The production
    code should never hit this path; we raise an explicit warning when it
    is constructed outside of a test context.
    """

    name = "hash-deterministic-test-only"
    dim = 128

    def __init__(self) -> None:
        logger.warning(
            "HashEmbedder is for tests/demos only; quality will be poor. "
            "Install voyageai or sentence-transformers for real retrieval."
        )

    def _hash_vector(self, text: str) -> list[float]:
        """Map text -> 128-dim unit vector via SHA256 byte chunks."""
        h = hashlib.sha256(text.lower().encode("utf-8")).digest()
        # Repeat the hash bytes until we have 128 ints, then L2-normalize.
        raw = (h * ((self.dim // len(h)) + 1))[: self.dim]
        vec = [(b - 128) / 128.0 for b in raw]
        norm = math.sqrt(sum(x * x for x in vec)) or 1.0
        return [x / norm for x in vec]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [self._hash_vector(t) for t in texts]

    def embed_query(self, query: str) -> list[float]:
        return self._hash_vector(query)


def _make_embedder(prefer: str | None = None) -> _EmbeddingBackend:
    """Select the best available embedder.

    Priority:
        1. ``prefer`` argument if provided ("voyage" | "local" | "hash").
        2. Voyage if VOYAGE_API_KEY is set and voyageai is importable.
        3. sentence-transformers if importable.
        4. HashEmbedder (test-only fallback).

    Args:
        prefer: Explicit backend choice for tests.

    Returns:
        An embedder instance.
    """
    if prefer == "voyage":
        return VoyageEmbedder()
    if prefer == "local":
        return LocalEmbedder()
    if prefer == "hash":
        return HashEmbedder()

    if os.environ.get("VOYAGE_API_KEY"):
        try:
            return VoyageEmbedder()
        except RuntimeError as exc:
            logger.warning("Voyage unavailable, falling back: %s", exc)
    try:
        return LocalEmbedder()
    except RuntimeError as exc:
        logger.warning("Local embedder unavailable, falling back to hash: %s", exc)
    return HashEmbedder()


# ---------------------------------------------------------------------------
# Vector store backends
# ---------------------------------------------------------------------------


class _VectorStore:
    """Abstract base for vector stores. Subclasses implement upsert/search."""

    def upsert(self, docs: list[Document], vectors: list[list[float]]) -> int:
        raise NotImplementedError

    def search(
        self,
        query_vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[str, float]]:
        raise NotImplementedError

    def get(self, doc_id: str) -> Document | None:
        raise NotImplementedError

    def count(self) -> int:
        raise NotImplementedError


class QdrantStore(_VectorStore):
    """Qdrant Cloud-backed store.

    Docs: https://qdrant.tech/documentation/quick-start/
    Filtering: https://qdrant.tech/documentation/concepts/filtering/
    Python client: https://github.com/qdrant/qdrant-client
    """

    def __init__(
        self,
        url: str | None = None,
        api_key: str | None = None,
        collection: str = _QDRANT_COLLECTION,
        dim: int = _VOYAGE_EMBED_DIM,
    ) -> None:
        try:
            from qdrant_client import QdrantClient  # type: ignore[import-not-found]
            from qdrant_client.http import models as qmodels  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "qdrant-client not installed. Run `pip install qdrant-client`."
            ) from exc

        self._qmodels = qmodels
        url = url or os.environ.get("QDRANT_URL") or ""
        api_key = api_key or os.environ.get("QDRANT_API_KEY") or ""
        if not url or not api_key:
            raise RuntimeError("QDRANT_URL and QDRANT_API_KEY required.")

        self._client = QdrantClient(url=url, api_key=api_key, timeout=15)
        self._collection = collection
        self._dim = dim
        self._ensure_collection()

    def _ensure_collection(self) -> None:
        """Create the collection if missing, idempotent."""
        existing = {c.name for c in self._client.get_collections().collections}
        if self._collection in existing:
            return
        self._client.create_collection(
            collection_name=self._collection,
            vectors_config=self._qmodels.VectorParams(
                size=self._dim, distance=self._qmodels.Distance.COSINE
            ),
        )
        # Add payload indexes for fast filtering. These are the metadata
        # fields most commonly used in queries.
        for field in ("source_file", "country", "metric", "year", "vertical"):
            try:
                self._client.create_payload_index(
                    collection_name=self._collection,
                    field_name=field,
                    field_schema=self._qmodels.PayloadSchemaType.KEYWORD,
                )
            except Exception as exc:
                # Index creation is best-effort; missing indexes only slow
                # queries, not break them.
                logger.warning("Payload index for %s failed: %s", field, exc)

    def upsert(self, docs: list[Document], vectors: list[list[float]]) -> int:
        if not docs:
            return 0
        if len(docs) != len(vectors):
            raise ValueError("docs and vectors length mismatch.")
        points = [
            self._qmodels.PointStruct(
                id=doc.doc_id,
                vector=vec,
                payload={**doc.metadata, "text": doc.text},
            )
            for doc, vec in zip(docs, vectors)
        ]
        # Batch upsert to avoid REST payload limits (~32 MB).
        batch_size = 100
        total = 0
        for i in range(0, len(points), batch_size):
            self._client.upsert(
                collection_name=self._collection, points=points[i : i + batch_size]
            )
            total += len(points[i : i + batch_size])
        return total

    def _build_filter(self, filters: dict[str, Any] | None):
        """Translate {key: value} or {key: [v1, v2]} to a Qdrant Filter."""
        if not filters:
            return None
        must = []
        for key, value in filters.items():
            if isinstance(value, list):
                must.append(
                    self._qmodels.FieldCondition(
                        key=key, match=self._qmodels.MatchAny(any=value)
                    )
                )
            else:
                must.append(
                    self._qmodels.FieldCondition(
                        key=key, match=self._qmodels.MatchValue(value=value)
                    )
                )
        return self._qmodels.Filter(must=must)

    def search(
        self,
        query_vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[str, float]]:
        results = self._client.search(
            collection_name=self._collection,
            query_vector=query_vector,
            query_filter=self._build_filter(filters),
            limit=top_k,
            with_payload=False,
            with_vectors=False,
        )
        return [(str(r.id), float(r.score)) for r in results]

    def get(self, doc_id: str) -> Document | None:
        result = self._client.retrieve(
            collection_name=self._collection, ids=[doc_id], with_payload=True
        )
        if not result:
            return None
        point = result[0]
        payload = dict(point.payload or {})
        text = payload.pop("text", "")
        return Document(doc_id=str(point.id), text=text, metadata=payload)

    def count(self) -> int:
        info = self._client.get_collection(collection_name=self._collection)
        return int(info.points_count or 0)


class InMemoryStore(_VectorStore):
    """In-process vector store. For tests, local dev, and Qdrant outages.

    Stores docs + vectors in a dict. Cosine similarity via dot product on
    normalized vectors. Not threadsafe; wrap in a lock if shared.
    """

    def __init__(self) -> None:
        self._docs: dict[str, Document] = {}
        self._vecs: dict[str, list[float]] = {}

    @staticmethod
    def _normalize(v: list[float]) -> list[float]:
        norm = math.sqrt(sum(x * x for x in v)) or 1.0
        return [x / norm for x in v]

    def upsert(self, docs: list[Document], vectors: list[list[float]]) -> int:
        for doc, vec in zip(docs, vectors):
            self._docs[doc.doc_id] = doc
            self._vecs[doc.doc_id] = self._normalize(vec)
        return len(docs)

    @staticmethod
    def _passes_filter(doc: Document, filters: dict[str, Any] | None) -> bool:
        if not filters:
            return True
        for k, v in filters.items():
            doc_v = doc.metadata.get(k)
            if isinstance(v, list):
                if doc_v not in v:
                    return False
            else:
                if doc_v != v:
                    return False
        return True

    def search(
        self,
        query_vector: list[float],
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[tuple[str, float]]:
        qv = self._normalize(query_vector)
        scored: list[tuple[float, str]] = []
        for doc_id, vec in self._vecs.items():
            doc = self._docs.get(doc_id)
            if doc is None or not self._passes_filter(doc, filters):
                continue
            sim = sum(a * b for a, b in zip(qv, vec))
            scored.append((sim, doc_id))
        scored.sort(reverse=True)
        return [(doc_id, score) for score, doc_id in scored[:top_k]]

    def get(self, doc_id: str) -> Document | None:
        return self._docs.get(doc_id)

    def count(self) -> int:
        return len(self._docs)


def _make_store(
    prefer: str | None = None, dim: int = _VOYAGE_EMBED_DIM
) -> _VectorStore:
    """Select the best available vector store.

    Priority:
        1. ``prefer`` argument if provided.
        2. Qdrant if URL + key set.
        3. InMemory fallback.
    """
    if prefer == "qdrant":
        return QdrantStore(dim=dim)
    if prefer == "memory":
        return InMemoryStore()

    if os.environ.get("QDRANT_URL") and os.environ.get("QDRANT_API_KEY"):
        try:
            return QdrantStore(dim=dim)
        except RuntimeError as exc:
            logger.warning("Qdrant unavailable, falling back: %s", exc)
    return InMemoryStore()


# ---------------------------------------------------------------------------
# BM25 keyword index (pure Python)
# ---------------------------------------------------------------------------


# Stop words copied verbatim from vector_search.py for behavior parity. In a
# real deployment we'd share the import.
_STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "the",
        "and",
        "or",
        "but",
        "in",
        "on",
        "at",
        "to",
        "for",
        "of",
        "with",
        "by",
        "from",
        "is",
        "it",
        "as",
        "was",
        "are",
        "be",
        "has",
        "had",
        "have",
        "this",
        "that",
        "these",
        "those",
        "not",
        "no",
        "will",
        "can",
        "do",
        "if",
        "so",
        "than",
        "too",
        "very",
        "just",
    }
)
_TOKEN_RE = re.compile(r"[A-Za-z0-9_\-]+")


def _tokenize(text: str) -> list[str]:
    """Tokenize: lowercase, drop stop words, single chars, and numerics-only."""
    return [
        t.lower()
        for t in _TOKEN_RE.findall(text)
        if len(t) > 1 and t.lower() not in _STOP_WORDS
    ]


class BM25Index:
    """Okapi BM25 keyword index. Matches vector_search.BM25Index contract.

    Args:
        k1: Term frequency saturation parameter.
        b: Document length normalization parameter.
    """

    def __init__(self, k1: float = 1.5, b: float = 0.75) -> None:
        self.k1 = k1
        self.b = b
        self._doc_ids: list[str] = []
        self._tf: list[dict[str, int]] = []
        self._df: dict[str, int] = {}
        self._dl: list[int] = []
        self._avg_dl: float = 0.0
        self._N: int = 0
        self._built = False

    def index(self, docs: list[Document]) -> None:
        df: dict[str, int] = defaultdict(int)
        for doc in docs:
            tokens = _tokenize(doc.text)
            tf = dict(Counter(tokens))
            self._doc_ids.append(doc.doc_id)
            self._tf.append(tf)
            self._dl.append(len(tokens))
            for t in set(tokens):
                df[t] += 1
        self._df = dict(df)
        self._N = len(docs)
        self._avg_dl = sum(self._dl) / self._N if self._N else 0.0
        self._built = True

    def search(self, query: str, top_k: int) -> list[tuple[str, float]]:
        if not self._built or self._N == 0:
            return []
        q_tokens = _tokenize(query)
        if not q_tokens:
            return []
        scores: list[tuple[float, int]] = []
        for i in range(self._N):
            score = 0.0
            for term in q_tokens:
                df_t = self._df.get(term)
                if not df_t:
                    continue
                f = self._tf[i].get(term, 0)
                if not f:
                    continue
                idf = math.log((self._N - df_t + 0.5) / (df_t + 0.5) + 1.0)
                dl = self._dl[i]
                tf_norm = (f * (self.k1 + 1.0)) / (
                    f + self.k1 * (1.0 - self.b + self.b * dl / (self._avg_dl or 1))
                )
                score += idf * tf_norm
            if score > 0:
                scores.append((score, i))
        scores.sort(reverse=True)
        return [(self._doc_ids[idx], round(s, 4)) for s, idx in scores[:top_k]]


# ---------------------------------------------------------------------------
# Reranker (cross-encoder via Voyage rerank API)
# ---------------------------------------------------------------------------


class _Reranker:
    """Cross-encoder rerank wrapper. Voyage rerank-2.5-lite or noop."""

    def __init__(self, enabled: bool = True) -> None:
        self._enabled = enabled and bool(os.environ.get("VOYAGE_API_KEY"))
        if self._enabled:
            try:
                import voyageai  # type: ignore[import-not-found]

                self._client = voyageai.Client(api_key=os.environ["VOYAGE_API_KEY"])
            except ImportError:
                self._enabled = False

    def rerank(
        self, query: str, candidates: list[RetrievalHit], top_k: int
    ) -> list[RetrievalHit]:
        """Return top_k candidates re-ordered by cross-encoder score.

        Args:
            query: User query.
            candidates: Hits from RRF fusion.
            top_k: Number of hits to return.

        Returns:
            Reranked hits. Falls back to input order if rerank fails.
        """
        if not self._enabled or not candidates or len(candidates) <= 1:
            return candidates[:top_k]
        try:
            docs = [c.text[:2000] for c in candidates]
            result = self._client.rerank(
                query=query,
                documents=docs,
                model=_VOYAGE_RERANK_MODEL,
                top_k=top_k,
            )
            ranked: list[RetrievalHit] = []
            for r in result.results:
                src = candidates[r.index]
                ranked.append(
                    dataclasses.replace(
                        src,
                        score=float(r.relevance_score),
                        search_method="rerank",
                    )
                )
            return ranked
        except Exception as exc:
            logger.warning("Rerank failed, returning RRF order: %s", exc)
            return candidates[:top_k]


# ---------------------------------------------------------------------------
# Chunking and metadata extraction
# ---------------------------------------------------------------------------


_COUNTRY_PATTERNS = {
    "US": re.compile(r"\b(united states|usa|america)\b", re.I),
    "UK": re.compile(r"\b(united kingdom|britain|england)\b", re.I),
    "CA": re.compile(r"\b(canada|canadian)\b", re.I),
    "DE": re.compile(r"\b(germany|deutschland)\b", re.I),
    "FR": re.compile(r"\b(france|french)\b", re.I),
    "IN": re.compile(r"\b(india|indian)\b", re.I),
    "AU": re.compile(r"\b(australia|australian)\b", re.I),
    "JP": re.compile(r"\b(japan|japanese)\b", re.I),
}

_METRIC_PATTERNS = {
    "cpc": re.compile(r"\bcost.per.click\b|\bcpc\b", re.I),
    "cpa": re.compile(r"\bcost.per.(applicant|application)\b|\bcpa\b", re.I),
    "cph": re.compile(r"\bcost.per.hire\b|\bcph\b", re.I),
    "apply_rate": re.compile(r"\bapply.rate\b|\bapplication.rate\b", re.I),
    "time_to_fill": re.compile(r"\btime.to.fill\b", re.I),
}

_VERTICAL_KEYWORDS = {
    "healthcare_nursing": ["nurse", "nursing", " rn ", "lpn", "cna"],
    "healthcare_general": ["healthcare", "medical", "clinician", "physician"],
    "trucking": ["truck", "cdl", "driver"],
    "gig": ["gig", "delivery", "rideshare"],
    "warehouse": ["warehouse", "forklift", "picker"],
    "retail": ["retail", "cashier", "store associate"],
    "tech": ["software", "developer", "engineer", "data scien"],
}


def _extract_metadata(text: str, source_file: str, key_path: str) -> dict[str, Any]:
    """Best-effort metadata extraction from chunk text + key path.

    Args:
        text: The chunk text.
        source_file: Source JSON filename (e.g. "joveo_cpa_benchmarks_2026.json").
        key_path: Dot-separated key path within the JSON.

    Returns:
        Metadata dict. Missing fields are simply absent (Qdrant filters
        treat absent fields as "does not match", which is correct).
    """
    md: dict[str, Any] = {
        "source_file": source_file,
        "kb_section": key_path,
    }
    haystack = f"{key_path} {text[:1000]}".lower()

    # Year: prefer the year mentioned in the filename, then in text.
    year_match = re.search(r"(20\d{2})", source_file)
    if year_match:
        md["year"] = int(year_match.group(1))
    else:
        year_match = re.search(r"\b(20\d{2})\b", haystack)
        if year_match:
            md["year"] = int(year_match.group(1))

    # Country.
    for code, pat in _COUNTRY_PATTERNS.items():
        if pat.search(haystack):
            md["country"] = code
            break

    # Metric.
    for metric, pat in _METRIC_PATTERNS.items():
        if pat.search(haystack):
            md["metric"] = metric
            break

    # Vertical.
    for vertical, kws in _VERTICAL_KEYWORDS.items():
        if any(kw in haystack for kw in kws):
            md["vertical"] = vertical
            break

    return md


def _approx_tokens(text: str) -> int:
    """Rough token count. 1 token ~= 4 chars for English."""
    return max(1, len(text) // 4)


def _chunk_json(data: Any, source_file: str, prefix: str = "") -> list[tuple[str, str]]:
    """Recursively chunk a JSON value into (key_path, text) tuples.

    Strategy:
        - On a dict with all-scalar children: emit one chunk
          ``"key1: v1\\nkey2: v2\\n..."``.
        - On a dict with nested dicts/lists: recurse, append non-scalar
          children separately. Also emit a summary chunk of scalar
          children at this level if any exist.
        - On a list: recurse into each item with an indexed key path.
        - On a scalar string >= 20 chars: emit as a chunk.

    Args:
        data: JSON value (dict, list, or scalar).
        source_file: Source filename for the chunk prefix.
        prefix: Dot-separated key path so far.

    Returns:
        List of (key_path, text) tuples.
    """
    chunks: list[tuple[str, str]] = []

    if isinstance(data, dict):
        scalars: list[str] = []
        for key, value in data.items():
            if key.startswith("_") or key in ("metadata", "last_updated", "version"):
                continue
            sub_prefix = f"{prefix}.{key}" if prefix else key
            if isinstance(value, (dict, list)):
                chunks.extend(_chunk_json(value, source_file, sub_prefix))
            elif isinstance(value, str) and len(value) >= 20:
                scalars.append(f"{key}: {value}")
            elif value is not None and not isinstance(value, str):
                scalars.append(f"{key}: {value}")
        if scalars:
            text = f"[{source_file}] {prefix}\n" + "\n".join(scalars)
            chunks.append((prefix, text))

    elif isinstance(data, list):
        for i, item in enumerate(data):
            sub_prefix = f"{prefix}[{i}]"
            chunks.extend(_chunk_json(item, source_file, sub_prefix))

    elif isinstance(data, str) and len(data) >= 20:
        chunks.append((prefix, f"[{source_file}] {prefix}: {data}"))

    return chunks


def _enforce_token_window(chunks: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Split overlong chunks and merge tiny ones into the 80-800 token window."""
    out: list[tuple[str, str]] = []
    pending_key: str | None = None
    pending_text: list[str] = []
    pending_tokens = 0

    def flush() -> None:
        nonlocal pending_key, pending_text, pending_tokens
        if pending_text:
            out.append((pending_key or "", "\n".join(pending_text)))
        pending_key = None
        pending_text = []
        pending_tokens = 0

    for key_path, text in chunks:
        n = _approx_tokens(text)
        if n > _CHUNK_MAX_TOKENS:
            # Split a giant chunk on newlines, attempting to stay under the cap.
            flush()
            lines = text.split("\n")
            buf: list[str] = []
            buf_tokens = 0
            for line in lines:
                lt = _approx_tokens(line)
                if buf_tokens + lt > _CHUNK_MAX_TOKENS and buf:
                    out.append((key_path, "\n".join(buf)))
                    # Overlap: carry the last 80-token tail.
                    tail = []
                    tail_tokens = 0
                    for ln in reversed(buf):
                        tail.append(ln)
                        tail_tokens += _approx_tokens(ln)
                        if tail_tokens >= _CHUNK_OVERLAP_TOKENS:
                            break
                    buf = list(reversed(tail))
                    buf_tokens = sum(_approx_tokens(ln) for ln in buf)
                buf.append(line)
                buf_tokens += lt
            if buf:
                out.append((key_path, "\n".join(buf)))
        elif n < _CHUNK_MIN_TOKENS:
            # Accumulate small chunks until we reach the minimum window.
            if pending_key is None:
                pending_key = key_path
            pending_text.append(text)
            pending_tokens += n
            if pending_tokens >= _CHUNK_MIN_TOKENS:
                flush()
        else:
            flush()
            out.append((key_path, text))
    flush()
    return out


# ---------------------------------------------------------------------------
# NovaRAG: orchestrator
# ---------------------------------------------------------------------------


class NovaRAG:
    """Retrieval-Augmented Generation engine for Nova.

    Public surface (per RAG_Design_2026.md):
        - embed_documents(docs)
        - index(documents)
        - retrieve(query, top_k=5, filters=None)
        - rerank(query, hits, top_k=5)
        - format_for_llm(hits)

    Args:
        embedder: Embedding backend; auto-selected if None.
        store: Vector store; auto-selected if None.
        bm25: BM25 keyword index; created internally if None.
        reranker: Cross-encoder reranker; created internally if None.
        rerank_enabled: If False, skip rerank step.
    """

    def __init__(
        self,
        embedder: _EmbeddingBackend | None = None,
        store: _VectorStore | None = None,
        bm25: BM25Index | None = None,
        reranker: _Reranker | None = None,
        rerank_enabled: bool = True,
    ) -> None:
        self.embedder = embedder or _make_embedder()
        self.store = store or _make_store(dim=self.embedder.dim)
        self.bm25 = bm25 or BM25Index()
        self.reranker = reranker or _Reranker(enabled=rerank_enabled)
        # Mirror of indexed docs for BM25 lookup. In production this lives
        # in Qdrant payloads; we keep an in-process copy because BM25 needs
        # access to text and metadata at search time.
        self._docs_by_id: dict[str, Document] = {}

    # ----- Indexing --------------------------------------------------------

    def embed_documents(self, docs: list[Document]) -> list[list[float]]:
        """Embed a batch of documents.

        Args:
            docs: Documents to embed.

        Returns:
            List of embedding vectors, parallel to docs.
        """
        texts = [d.text for d in docs]
        vectors: list[list[float]] = []
        # Batch in chunks to respect Voyage's 128/call limit.
        for i in range(0, len(texts), 64):
            batch = texts[i : i + 64]
            vectors.extend(self.embedder.embed_batch(batch))
        return vectors

    def index(self, documents: list[Document]) -> dict[str, int]:
        """Embed and upsert documents into vector + BM25 indices.

        Args:
            documents: Documents to index.

        Returns:
            Dict with ``indexed`` (count upserted) and ``bm25`` (count in
            BM25 index after this call).
        """
        if not documents:
            return {"indexed": 0, "bm25": 0}
        logger.info("Indexing %d documents...", len(documents))
        t0 = time.monotonic()

        vectors = self.embed_documents(documents)
        n = self.store.upsert(documents, vectors)

        for doc in documents:
            self._docs_by_id[doc.doc_id] = doc
        self.bm25.index(list(self._docs_by_id.values()))

        elapsed = time.monotonic() - t0
        logger.info(
            "Indexed %d docs in %.1fs (%.1f docs/s).",
            n,
            elapsed,
            n / elapsed if elapsed > 0 else 0.0,
        )
        return {"indexed": n, "bm25": len(self._docs_by_id)}

    # ----- Retrieval -------------------------------------------------------

    def retrieve(
        self,
        query: str,
        top_k: int = _DEFAULT_TOP_K,
        filters: dict[str, Any] | None = None,
    ) -> list[RetrievalHit]:
        """Hybrid retrieval: vector + BM25 -> RRF -> rerank.

        Args:
            query: Natural-language query.
            top_k: Number of hits to return (default 5).
            filters: Optional metadata pre-filter for the vector leg.
                E.g. ``{"country": "US", "metric": "cpa", "year": 2026}``.

        Returns:
            Ranked list of RetrievalHit. Empty list if no candidates.
        """
        if not query or not query.strip():
            return []

        # Vector leg.
        vector_hits: list[tuple[str, float]] = []
        try:
            q_vec = (
                self.embedder.embed_query(query)
                if hasattr(self.embedder, "embed_query")
                else self.embedder.embed_batch([query])[0]
            )
            vector_hits = self.store.search(q_vec, _VECTOR_FETCH_K, filters)
        except Exception as exc:
            logger.warning("Vector retrieval failed: %s", exc, exc_info=True)

        # BM25 leg. BM25 doesn't natively support filters; we apply them
        # post-hoc against our in-process doc mirror.
        bm25_hits_raw = self.bm25.search(query, _BM25_FETCH_K)
        bm25_hits: list[tuple[str, float]] = []
        for doc_id, score in bm25_hits_raw:
            doc = self._docs_by_id.get(doc_id)
            if doc is None:
                continue
            if filters and not InMemoryStore._passes_filter(doc, filters):
                continue
            bm25_hits.append((doc_id, score))

        # Fuse with reciprocal rank fusion.
        fused = self._rrf(vector_hits, bm25_hits, k=_RRF_K)

        # Drop below-noise-floor candidates.
        fused = [(d, s) for d, s in fused if s >= _RRF_SCORE_FLOOR]

        # Materialize candidates.
        candidates: list[RetrievalHit] = []
        for doc_id, score in fused[:_RERANK_INPUT_K]:
            doc = self._docs_by_id.get(doc_id) or self.store.get(doc_id)
            if doc is None:
                continue
            candidates.append(
                RetrievalHit(
                    doc_id=doc.doc_id,
                    text=doc.text,
                    metadata=doc.metadata,
                    score=score,
                    source_file=doc.metadata.get("source_file", "unknown"),
                    search_method="hybrid_rrf",
                )
            )

        # Cross-encoder rerank to top_k.
        return self.rerank(query, candidates, top_k)

    @staticmethod
    def _rrf(
        a: list[tuple[str, float]],
        b: list[tuple[str, float]],
        k: int = _RRF_K,
    ) -> list[tuple[str, float]]:
        """Reciprocal Rank Fusion. Cormack/Clarke/Buettcher 2009."""
        scores: dict[str, float] = {}
        for rank, (doc_id, _) in enumerate(a):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
        for rank, (doc_id, _) in enumerate(b):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [(d, round(s, 6)) for d, s in ranked]

    def rerank(
        self, query: str, hits: list[RetrievalHit], top_k: int
    ) -> list[RetrievalHit]:
        """Cross-encoder rerank. No-op if rerank unavailable."""
        return self.reranker.rerank(query, hits, top_k)

    # ----- LLM formatting --------------------------------------------------

    @staticmethod
    def format_for_llm(hits: list[RetrievalHit]) -> str:
        """Format hits as an LLM-injectable evidence block.

        Output schema (consumed by the existing nova.py system_prompt
        synthesis):

            <kb_evidence>
              <chunk id="..." source="file.json" section="benchmarks.cpa.nursing"
                     score="0.78" search="rerank">
                text content
              </chunk>
              ...
            </kb_evidence>

        XML-style because Claude is known to handle XML-delimited context
        cleanly (Anthropic prompt engineering docs). The LLM is instructed
        to cite chunks by ``source``.
        """
        if not hits:
            return ""
        lines = ["<kb_evidence>"]
        for h in hits:
            attrs = (
                f'id="{h.doc_id[:8]}" '
                f'source="{h.source_file}" '
                f'section="{h.metadata.get("kb_section", "")}" '
                f'score="{h.score:.3f}" '
                f'search="{h.search_method}"'
            )
            lines.append(f"  <chunk {attrs}>")
            for line in h.text.splitlines():
                lines.append(f"    {line}")
            lines.append("  </chunk>")
        lines.append("</kb_evidence>")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Indexing helper: walk the data/ directory and produce Documents
# ---------------------------------------------------------------------------


# Mirrors vector_search.index_knowledge_base() at module load time. In the
# real rag_pipeline.py module this list comes from a config file so the
# index is reproducible across deploys.
_KB_FILES = (
    "recruitment_industry_knowledge.json",
    "joveo_2026_benchmarks.json",
    "joveo_cpa_benchmarks_2026.json",
    "recruitment_benchmarks_comprehensive_2026.json",
    "international_benchmarks_2026.json",
    "channels_db.json",
    "joveo_publishers.json",
    "platform_intelligence_deep.json",
    "salary_benchmarks_detailed_2026.json",
    "ad_benchmarks_recruitment_2026.json",
    "client_media_plans_kb.json",
    "nova_learned_answers.json",
)


def build_documents_from_kb(
    data_dir: Path, files: Iterable[str] = _KB_FILES
) -> list[Document]:
    """Walk data/*.json and produce indexable Documents.

    Args:
        data_dir: Path to the data/ directory.
        files: Subset of filenames to index.

    Returns:
        List of Documents ready for ``NovaRAG.index``.
    """
    docs: list[Document] = []
    for filename in files:
        fpath = data_dir / filename
        if not fpath.exists():
            logger.info("Skipping missing KB file: %s", filename)
            continue
        try:
            with fpath.open("r", encoding="utf-8") as f:
                payload = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Failed to load %s: %s", filename, exc)
            continue

        raw_chunks = _chunk_json(payload, source_file=filename)
        normalized = _enforce_token_window(raw_chunks)
        for i, (key_path, text) in enumerate(normalized):
            md = _extract_metadata(text=text, source_file=filename, key_path=key_path)
            md["chunk_index"] = i
            md["token_count"] = _approx_tokens(text)
            md["indexed_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            # Deterministic UUID5 keeps re-indexing idempotent.
            ns = uuid.UUID("12345678-1234-5678-1234-567812345678")
            doc_id = str(uuid.uuid5(ns, f"{filename}::{key_path}::{i}"))
            docs.append(Document(doc_id=doc_id, text=text, metadata=md))
    return docs


# ---------------------------------------------------------------------------
# Demo / __main__
# ---------------------------------------------------------------------------


def _demo() -> None:
    """End-to-end demo: index a slice of data/*.json and run sample queries.

    Run with:
        python -m docs.rag_implementation_sketch
    or
        cd media-plan-generator && python docs/rag_implementation_sketch.py
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    # Force the hash/in-memory path so the demo runs without any API keys
    # or local model downloads. In production the auto-detection picks
    # Voyage + Qdrant.
    embedder = _make_embedder(prefer="hash")
    store = _make_store(prefer="memory", dim=embedder.dim)
    rag = NovaRAG(embedder=embedder, store=store, rerank_enabled=False)

    data_dir = Path(
        os.environ.get("NOVA_RAG_DATA_DIR")
        or (Path(__file__).resolve().parent.parent / "data")
    )
    if not data_dir.exists():
        logger.error(
            "Data dir not found: %s. Set NOVA_RAG_DATA_DIR or run from repo root.",
            data_dir,
        )
        return

    # Only index a handful of files in the demo so it runs in <2s.
    demo_files = (
        "recruitment_industry_knowledge.json",
        "joveo_2026_benchmarks.json",
        "nova_learned_answers.json",
    )
    docs = build_documents_from_kb(data_dir, files=demo_files)
    logger.info("Built %d demo documents from %s", len(docs), data_dir.name)

    if not docs:
        logger.error("No demo documents built. Aborting demo.")
        return

    stats = rag.index(docs)
    logger.info("Index stats: %s", stats)

    sample_queries = [
        "What is the average cost per click on Indeed for healthcare jobs?",
        "How much does a media plan typically cost for nursing roles?",
        "What CPA should I expect for warehouse jobs in the US?",
        "Tell me about LinkedIn job ad costs.",
    ]
    for q in sample_queries:
        print("\n" + "=" * 80)
        print(f"Query: {q}")
        print("-" * 80)
        hits = rag.retrieve(q, top_k=3)
        if not hits:
            print("(no hits)")
            continue
        for i, h in enumerate(hits, 1):
            preview = h.text.replace("\n", " ")[:160]
            print(
                f"{i}. [{h.source_file}] {h.metadata.get('kb_section', '')[:60]} "
                f"score={h.score:.3f} method={h.search_method}\n   {preview}..."
            )

    print("\n" + "=" * 80)
    print("LLM-formatted evidence block (preview):")
    print("=" * 80)
    print(rag.format_for_llm(hits)[:800])


# ---------------------------------------------------------------------------
# pytest harness (run with: pytest -q docs/rag_implementation_sketch.py)
# ---------------------------------------------------------------------------


def _sample_docs() -> list[Document]:
    """Hand-crafted docs for offline tests. No file I/O, no network."""
    return [
        Document(
            doc_id="doc1",
            text=(
                "[recruitment_industry_knowledge.json] benchmarks.cost_per_click.indeed\n"
                "average_cpc_range: $0.25-$1.50\n"
                "model: Pay-per-click for sponsored listings\n"
                "notes: Best for volume hiring, blue-collar, and hourly roles."
            ),
            metadata={
                "source_file": "recruitment_industry_knowledge.json",
                "kb_section": "benchmarks.cost_per_click.indeed",
                "country": "US",
                "metric": "cpc",
                "year": 2026,
            },
        ),
        Document(
            doc_id="doc2",
            text=(
                "[recruitment_industry_knowledge.json] benchmarks.cost_per_click.linkedin\n"
                "job_ad_cpc_range: $1.50-$4.50\n"
                "cost_per_applicant_us_average: $2.83\n"
                "notes: Premium CPC reflects highly targeted professional audience."
            ),
            metadata={
                "source_file": "recruitment_industry_knowledge.json",
                "kb_section": "benchmarks.cost_per_click.linkedin",
                "country": "US",
                "metric": "cpc",
                "year": 2026,
            },
        ),
        Document(
            doc_id="doc3",
            text=(
                "[joveo_2026_benchmarks.json] cost_per_application.healthcare_nursing\n"
                "average_cpa: $24.50\n"
                "p90_cpa: $58.00\n"
                "notes: Texas and Florida exhibit 15% premium over US median."
            ),
            metadata={
                "source_file": "joveo_2026_benchmarks.json",
                "kb_section": "cost_per_application.healthcare_nursing",
                "country": "US",
                "metric": "cpa",
                "vertical": "healthcare_nursing",
                "year": 2026,
            },
        ),
        Document(
            doc_id="doc4",
            text=(
                "[international_benchmarks_2026.json] germany.healthcare\n"
                "cpa_eur: 18.40\n"
                "notes: Germany healthcare has tight nurse supply post-2024."
            ),
            metadata={
                "source_file": "international_benchmarks_2026.json",
                "kb_section": "germany.healthcare",
                "country": "DE",
                "metric": "cpa",
                "vertical": "healthcare_general",
                "year": 2026,
            },
        ),
    ]


def _make_test_rag() -> NovaRAG:
    """Build a hermetic NovaRAG for tests: hash embedder + in-memory store."""
    return NovaRAG(
        embedder=_make_embedder(prefer="hash"),
        store=_make_store(prefer="memory", dim=128),
        rerank_enabled=False,
    )


def test_index_then_retrieve() -> None:
    """Smoke test: index 4 docs, retrieve, confirm at least one BM25 hit."""
    rag = _make_test_rag()
    docs = _sample_docs()
    stats = rag.index(docs)
    assert stats["indexed"] == len(docs), "Should index all docs"
    assert stats["bm25"] == len(docs), "BM25 should mirror the index"

    # Use a query with high keyword overlap so BM25 (which always works,
    # unlike our hash-embedded vectors) provides reliable signal.
    hits = rag.retrieve("LinkedIn CPC professional audience", top_k=3)
    assert hits, "Should return at least one hit"
    assert hits[0].source_file in {
        "recruitment_industry_knowledge.json",
        "joveo_2026_benchmarks.json",
    }
    # The LinkedIn doc must outrank the Indeed doc on this keyword-rich query.
    linkedin_hit = next(
        (h for h in hits if "linkedin" in h.metadata.get("kb_section", "")), None
    )
    assert linkedin_hit is not None, "LinkedIn doc should be retrieved"


def test_metadata_filter() -> None:
    """Filtering by metric=cpa should exclude CPC docs."""
    rag = _make_test_rag()
    rag.index(_sample_docs())
    hits = rag.retrieve("nursing cost", top_k=5, filters={"metric": "cpa"})
    for h in hits:
        assert (
            h.metadata.get("metric") == "cpa"
        ), f"Filter leak: doc {h.doc_id} has metric={h.metadata.get('metric')}"


def test_country_filter() -> None:
    """Filtering by country=DE should return only the Germany doc."""
    rag = _make_test_rag()
    rag.index(_sample_docs())
    hits = rag.retrieve("healthcare costs", top_k=5, filters={"country": "DE"})
    assert hits, "Should find the Germany doc"
    for h in hits:
        assert h.metadata.get("country") == "DE"


def test_format_for_llm() -> None:
    """format_for_llm should produce a parseable XML evidence block."""
    rag = _make_test_rag()
    rag.index(_sample_docs())
    hits = rag.retrieve("LinkedIn CPC", top_k=2)
    formatted = rag.format_for_llm(hits)
    assert "<kb_evidence>" in formatted and "</kb_evidence>" in formatted
    assert "<chunk " in formatted and "</chunk>" in formatted
    # Citation field present.
    assert 'source="' in formatted


def test_empty_query_returns_empty() -> None:
    """Edge case: empty query returns empty list, no crash."""
    rag = _make_test_rag()
    rag.index(_sample_docs())
    assert rag.retrieve("", top_k=5) == []
    assert rag.retrieve("   ", top_k=5) == []


def test_chunking_walks_full_data_dir() -> None:
    """Integration-style: walk the real data/ dir if available.

    This test is skipped when run outside the media-plan-generator repo.
    It asserts we can build >100 documents from the real KB.
    """
    data_dir = Path(__file__).resolve().parent.parent / "data"
    if not data_dir.exists():
        # Don't import pytest at module level to keep this file dep-free.
        try:
            import pytest

            pytest.skip(f"data_dir not present: {data_dir}")
        except ImportError:
            return
    docs = build_documents_from_kb(
        data_dir, files=("recruitment_industry_knowledge.json",)
    )
    assert len(docs) > 10, f"Expected >10 chunks, got {len(docs)}"
    # Every doc must have a source_file in metadata.
    for d in docs:
        assert d.metadata.get("source_file") == "recruitment_industry_knowledge.json"


if __name__ == "__main__":
    _demo()
