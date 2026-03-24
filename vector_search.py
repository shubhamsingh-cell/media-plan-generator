#!/usr/bin/env python3
"""Semantic vector search using Voyage AI embeddings + in-memory vector store.

Uses Voyage AI HTTP API for embeddings (no chromadb/voyageai pip packages).
Implements a pure-Python in-memory vector store with cosine similarity.

Fallback tiers when Voyage AI is unavailable:
    Tier 1: Voyage AI embeddings (primary, high-quality)
    Tier 2: TF-IDF keyword matching (pure Python, no external calls)

This keeps our stdlib-only approach while enabling semantic search across
the Nova knowledge base.

API: POST https://api.voyageai.com/v1/embeddings
Env var: VOYAGE_API_KEY (sign up at https://www.voyageai.com -- 200M free tokens)

All functions:
    - Return empty/None on failure (never raise)
    - Log errors with exc_info=True
    - Use type hints on all signatures
"""

from __future__ import annotations

import collections
import json
import logging
import math
import os
import re
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
_VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"
_VOYAGE_MODEL = "voyage-3-lite"  # Good balance of quality/speed/cost
_VOYAGE_TIMEOUT = 20  # seconds
_VOYAGE_API_KEY: str | None = None
_VOYAGE_MAX_BATCH = 128  # Voyage API max batch size
_VOYAGE_RPM_LIMIT = 10  # Voyage free tier: ~10 req/min
_voyage_request_times: list[float] = []
_voyage_rate_lock = threading.Lock()


def _is_voyage_rate_limited() -> bool:
    """Check if we've exceeded Voyage AI rate limit."""
    now = time.monotonic()
    with _voyage_rate_lock:
        # Prune requests older than 60s
        _voyage_request_times[:] = [t for t in _voyage_request_times if now - t < 60]
        return len(_voyage_request_times) >= _VOYAGE_RPM_LIMIT


def _record_voyage_request() -> None:
    """Record a Voyage API request timestamp."""
    with _voyage_rate_lock:
        _voyage_request_times.append(time.monotonic())


def _get_api_key() -> str | None:
    """Load Voyage API key from environment (cached after first load)."""
    global _VOYAGE_API_KEY
    if _VOYAGE_API_KEY is None:
        _VOYAGE_API_KEY = os.environ.get("VOYAGE_API_KEY") or ""
    return _VOYAGE_API_KEY if _VOYAGE_API_KEY else None


# ── In-memory vector index ───────────────────────────────────────────────────
# Each entry: {"id": str, "text": str, "embedding": list[float], "metadata": dict}
_index: list[dict] = []
_index_lock = threading.Lock()
_index_built = False


# ── Embedding API ────────────────────────────────────────────────────────────


def embed_text(text: str) -> list[float] | None:
    """Get embedding vector for a single text string via Voyage AI API.

    Args:
        text: Text to embed (max ~32K tokens for voyage-3-lite).

    Returns:
        List of floats (embedding vector), or None on failure.
    """
    result = embed_batch([text])
    if result and len(result) > 0:
        return result[0]
    return None


def embed_batch(texts: list[str]) -> list[list[float]] | None:
    """Get embedding vectors for a batch of texts via Voyage AI API.

    Args:
        texts: List of texts to embed (max 128 per call).

    Returns:
        List of embedding vectors (list[list[float]]), or None on failure.
    """
    api_key = _get_api_key()
    if not api_key:
        logger.warning(
            "VOYAGE_API_KEY not set. Sign up at https://www.voyageai.com "
            "and set VOYAGE_API_KEY environment variable."
        )
        return None

    if not texts:
        return []

    # Rate limit check
    if _is_voyage_rate_limited():
        logger.warning(
            "Voyage AI rate limited (>%d req/min), skipping embed_batch",
            _VOYAGE_RPM_LIMIT,
        )
        return None

    # Chunk into batches of _VOYAGE_MAX_BATCH
    all_embeddings: list[list[float]] = []
    for i in range(0, len(texts), _VOYAGE_MAX_BATCH):
        batch = texts[i : i + _VOYAGE_MAX_BATCH]
        # Truncate each text to avoid exceeding token limits
        batch = [t[:8000] for t in batch]

        payload = {
            "input": batch,
            "model": _VOYAGE_MODEL,
        }

        try:
            _record_voyage_request()
            data = json.dumps(payload).encode("utf-8")
            req = urllib.request.Request(
                _VOYAGE_API_URL,
                data=data,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {api_key}",
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=_VOYAGE_TIMEOUT) as resp:
                body = resp.read().decode("utf-8")
                result = json.loads(body)

            embeddings_data = result.get("data") or []
            # Sort by index to preserve order
            embeddings_data.sort(key=lambda x: x.get("index", 0))
            batch_embeddings = [e.get("embedding") or [] for e in embeddings_data]
            all_embeddings.extend(batch_embeddings)

        except urllib.error.HTTPError as e:
            logger.error(
                "Voyage AI HTTP error %d: %s",
                e.code,
                e.reason,
                exc_info=True,
            )
            return None
        except urllib.error.URLError as e:
            logger.error(
                "Voyage AI URL error: %s",
                e.reason,
                exc_info=True,
            )
            return None
        except (json.JSONDecodeError, OSError, ValueError, TypeError) as e:
            logger.error("Voyage AI error: %s", e, exc_info=True)
            return None

    if len(all_embeddings) != len(texts):
        logger.warning(
            "Voyage AI returned %d embeddings for %d texts",
            len(all_embeddings),
            len(texts),
        )
        return None

    return all_embeddings


# ── Pure-Python cosine similarity ────────────────────────────────────────────


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Compute cosine similarity between two vectors (pure Python, no numpy).

    Args:
        a: First vector.
        b: Second vector.

    Returns:
        Cosine similarity in range [-1, 1].
    """
    if len(a) != len(b) or not a:
        return 0.0

    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))

    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0

    return dot / (norm_a * norm_b)


# ── Index management ─────────────────────────────────────────────────────────


def build_index(documents: list[dict]) -> None:
    """Build in-memory vector index from a list of documents.

    Each document should have at minimum: {"id": str, "text": str}.
    Optional "metadata" dict is preserved for retrieval.

    Falls back to TF-IDF index if Voyage AI embedding fails.

    Args:
        documents: List of dicts with at least "id" and "text" keys.
    """
    global _index_built

    if not documents:
        logger.warning("build_index called with empty document list")
        return

    texts = [d.get("text") or "" for d in documents]
    # Filter out empty texts
    valid_docs = [(d, t) for d, t in zip(documents, texts) if t.strip()]
    if not valid_docs:
        logger.warning("build_index: no valid documents with text content")
        return

    valid_texts = [t for _, t in valid_docs]
    embeddings = embed_batch(valid_texts)

    if embeddings is None:
        logger.warning(
            "build_index: Voyage AI embedding failed, building TF-IDF fallback index"
        )
        # Build TF-IDF fallback index from the valid documents
        _build_tfidf_index(
            [
                {
                    "id": doc.get("id") or "",
                    "text": text,
                    "metadata": doc.get("metadata") or {},
                }
                for doc, text in valid_docs
            ]
        )
        return

    with _index_lock:
        new_entries = []
        for (doc, text), embedding in zip(valid_docs, embeddings):
            if not embedding:
                continue
            new_entries.append(
                {
                    "id": doc.get("id") or "",
                    "text": text,
                    "embedding": embedding,
                    "metadata": doc.get("metadata") or {},
                }
            )
        _index.extend(new_entries)
        _index_built = True

    # Also build TF-IDF index as a warm standby for runtime fallback
    _build_tfidf_index(
        [
            {
                "id": doc.get("id") or "",
                "text": text,
                "metadata": doc.get("metadata") or {},
            }
            for doc, text in valid_docs
        ]
    )

    logger.info(
        "Vector index built: %d documents indexed (total: %d)",
        len(new_entries),
        len(_index),
    )


def search(query: str, top_k: int = 5) -> list[dict]:
    """Semantic search across indexed documents.

    Tries Voyage AI embeddings first; falls back to TF-IDF keyword search.

    Args:
        query: Natural language search query.
        top_k: Number of top results to return.

    Returns:
        List of dicts with keys: id, text, metadata, score.
        Returns empty list on failure.
    """
    # Tier 1: Voyage AI vector search
    if _index:
        query_embedding = embed_text(query)
        if query_embedding is not None:
            scored: list[tuple[float, dict]] = []
            with _index_lock:
                for entry in _index:
                    sim = _cosine_similarity(query_embedding, entry["embedding"])
                    scored.append((sim, entry))

            scored.sort(key=lambda x: x[0], reverse=True)

            results: list[dict] = []
            for score, entry in scored[:top_k]:
                results.append(
                    {
                        "id": entry["id"],
                        "text": entry["text"][:500],
                        "metadata": entry["metadata"],
                        "score": round(score, 4),
                    }
                )
            return results

        # Voyage AI failed at query time -- fall through to TF-IDF
        logger.warning("Voyage AI embedding failed for query, falling back to TF-IDF")

    # Tier 2: TF-IDF keyword search fallback
    tfidf_results = _tfidf_search(query, top_k=top_k)
    if tfidf_results:
        logger.info(
            "TF-IDF fallback returned %d results for query=%s",
            len(tfidf_results),
            query[:50],
        )
        return tfidf_results

    return []


def index_knowledge_base() -> int:
    """Index all knowledge base JSON files from the data/ directory at startup.

    Reads each JSON file, extracts text-bearing entries, and indexes them.
    Handles various KB JSON structures (nested dicts, lists of dicts, etc.).

    Returns:
        Number of documents indexed, or 0 on failure.
    """
    data_dir = Path(__file__).resolve().parent / "data"
    if not data_dir.exists():
        logger.warning("Data directory not found: %s", data_dir)
        return 0

    # KB files to index (skip api_cache, backups, etc.)
    kb_files = [
        "recruitment_industry_knowledge.json",
        "platform_intelligence_deep.json",
        "recruitment_benchmarks_deep.json",
        "recruitment_strategy_intelligence.json",
        "regional_hiring_intelligence.json",
        "supply_ecosystem_intelligence.json",
        "workforce_trends_intelligence.json",
        "industry_white_papers.json",
        "joveo_2026_benchmarks.json",
        "google_ads_2025_benchmarks.json",
        "external_benchmarks_2025.json",
        "client_media_plans_kb.json",
        "channels_db.json",
        "joveo_publishers.json",
    ]

    documents: list[dict] = []
    doc_id = 0

    for filename in kb_files:
        fpath = data_dir / filename
        if not fpath.exists():
            continue

        try:
            with open(fpath, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load %s for indexing: %s", filename, e)
            continue

        # Extract text chunks from various JSON structures
        chunks = _extract_text_chunks(raw, source=filename)
        for chunk_text in chunks:
            if len(chunk_text.strip()) < 20:
                continue
            doc_id += 1
            documents.append(
                {
                    "id": f"{filename}:{doc_id}",
                    "text": chunk_text[:2000],  # Limit chunk size
                    "metadata": {"source": filename, "chunk_id": doc_id},
                }
            )

    if not documents:
        logger.warning("No documents extracted from knowledge base for indexing")
        return 0

    logger.info(
        "Extracted %d text chunks from %d KB files for vector indexing",
        len(documents),
        len(kb_files),
    )
    build_index(documents)
    return len(documents)


def _extract_text_chunks(data: Any, source: str = "", prefix: str = "") -> list[str]:
    """Recursively extract text chunks from nested JSON structures.

    Produces one chunk per leaf string value or per dict that has
    meaningful text content.

    Args:
        data: JSON data (dict, list, or scalar).
        source: Source filename for context.
        prefix: Key path prefix for context.

    Returns:
        List of text strings suitable for embedding.
    """
    chunks: list[str] = []

    if isinstance(data, dict):
        # If this dict has text-like values, combine them into one chunk
        text_parts: list[str] = []
        for key, value in data.items():
            if key.startswith("_") or key in ("metadata", "last_updated", "version"):
                continue
            if isinstance(value, str) and len(value) > 20:
                text_parts.append(f"{key}: {value}")
            elif isinstance(value, (dict, list)):
                sub_chunks = _extract_text_chunks(
                    value, source=source, prefix=f"{prefix}.{key}" if prefix else key
                )
                chunks.extend(sub_chunks)

        if text_parts:
            combined = f"[{source}] {prefix}\n" + "\n".join(text_parts)
            chunks.append(combined)

    elif isinstance(data, list):
        for i, item in enumerate(data):
            sub_chunks = _extract_text_chunks(
                item, source=source, prefix=f"{prefix}[{i}]"
            )
            chunks.extend(sub_chunks)

    elif isinstance(data, str) and len(data) > 20:
        chunks.append(f"[{source}] {prefix}: {data}")

    return chunks


# ── TF-IDF Fallback Engine (Tier 2: pure Python, no external calls) ─────────

# Stop words for TF-IDF tokenizer
_STOP_WORDS: frozenset[str] = frozenset(
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
        "about",
        "above",
        "after",
        "again",
        "all",
        "also",
        "am",
        "any",
        "because",
        "been",
        "before",
        "being",
        "between",
        "both",
        "could",
        "did",
        "does",
        "doing",
        "down",
        "during",
        "each",
        "few",
        "get",
        "got",
        "he",
        "her",
        "here",
        "him",
        "his",
        "how",
        "i",
        "into",
        "its",
        "let",
        "me",
        "more",
        "most",
        "my",
        "nor",
        "now",
        "only",
        "other",
        "our",
        "out",
        "over",
        "own",
        "same",
        "she",
        "should",
        "some",
        "such",
        "tell",
        "their",
        "them",
        "then",
        "there",
        "they",
        "through",
        "under",
        "until",
        "up",
        "us",
        "we",
        "what",
        "when",
        "where",
        "which",
        "while",
        "who",
        "whom",
        "why",
        "would",
        "you",
        "your",
    }
)

# TF-IDF index state
_tfidf_index: list[dict[str, float]] = []
_tfidf_idf: dict[str, float] = {}
_tfidf_doc_texts: list[str] = []
_tfidf_doc_ids: list[str] = []
_tfidf_doc_meta: list[dict] = []
_tfidf_lock = threading.Lock()
_tfidf_built = False


def _tokenize(text: str) -> list[str]:
    """Tokenize text into lowercase words, removing stop words.

    Args:
        text: Input text string.

    Returns:
        List of cleaned token strings.
    """
    tokens = re.findall(r"[a-z0-9]+", text.lower())
    return [t for t in tokens if t not in _STOP_WORDS and len(t) > 1]


def _compute_tf(tokens: list[str]) -> dict[str, float]:
    """Compute log-normalized term frequency for a token list.

    Args:
        tokens: List of tokens from a document.

    Returns:
        Dict mapping each token to its TF score.
    """
    counts = collections.Counter(tokens)
    tf: dict[str, float] = {}
    for term, count in counts.items():
        tf[term] = 1.0 + math.log(count) if count > 0 else 0.0
    return tf


def _build_tfidf_index(documents: list[dict]) -> None:
    """Build a TF-IDF index from documents as a fallback for vector search.

    Args:
        documents: List of dicts with "id", "text", and optional "metadata".
    """
    global _tfidf_built

    if not documents:
        return

    with _tfidf_lock:
        all_tokens: list[list[str]] = []
        doc_count = len(documents)

        _tfidf_doc_texts.clear()
        _tfidf_doc_ids.clear()
        _tfidf_doc_meta.clear()
        _tfidf_index.clear()
        _tfidf_idf.clear()

        df: dict[str, int] = collections.defaultdict(int)

        for doc in documents:
            text = doc.get("text") or ""
            tokens = _tokenize(text)
            all_tokens.append(tokens)
            _tfidf_doc_texts.append(text[:500])
            _tfidf_doc_ids.append(doc.get("id") or "")
            _tfidf_doc_meta.append(doc.get("metadata") or {})

            for term in set(tokens):
                df[term] += 1

        for term, freq in df.items():
            _tfidf_idf[term] = math.log((doc_count + 1) / (freq + 1)) + 1.0

        for tokens in all_tokens:
            tf = _compute_tf(tokens)
            tfidf_vec: dict[str, float] = {
                term: tf_val * _tfidf_idf.get(term, 1.0) for term, tf_val in tf.items()
            }
            _tfidf_index.append(tfidf_vec)

        _tfidf_built = True

    logger.info(
        "TF-IDF fallback index built: %d documents, %d unique terms",
        doc_count,
        len(_tfidf_idf),
    )


def _tfidf_cosine_similarity(
    vec_a: dict[str, float],
    vec_b: dict[str, float],
) -> float:
    """Compute cosine similarity between two sparse TF-IDF vectors.

    Args:
        vec_a: First sparse vector (term -> weight).
        vec_b: Second sparse vector (term -> weight).

    Returns:
        Cosine similarity in range [0, 1].
    """
    common_terms = set(vec_a.keys()) & set(vec_b.keys())
    if not common_terms:
        return 0.0

    dot = sum(vec_a[t] * vec_b[t] for t in common_terms)
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(v * v for v in vec_b.values()))

    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0

    return dot / (norm_a * norm_b)


def _tfidf_search(query: str, top_k: int = 5) -> list[dict]:
    """Search the TF-IDF index with a text query (Tier 2 fallback).

    Args:
        query: Natural language search query.
        top_k: Number of top results to return.

    Returns:
        List of result dicts with id, text, metadata, score.
    """
    if not _tfidf_built:
        return []

    query_tokens = _tokenize(query)
    if not query_tokens:
        return []

    query_tf = _compute_tf(query_tokens)
    query_vec: dict[str, float] = {
        term: tf_val * _tfidf_idf.get(term, 1.0) for term, tf_val in query_tf.items()
    }

    scored: list[tuple[float, int]] = []
    with _tfidf_lock:
        for idx, doc_vec in enumerate(_tfidf_index):
            sim = _tfidf_cosine_similarity(query_vec, doc_vec)
            if sim > 0.0:
                scored.append((sim, idx))

    scored.sort(key=lambda x: x[0], reverse=True)

    results: list[dict] = []
    for score, idx in scored[:top_k]:
        results.append(
            {
                "id": _tfidf_doc_ids[idx],
                "text": _tfidf_doc_texts[idx],
                "metadata": _tfidf_doc_meta[idx],
                "score": round(score, 4),
            }
        )

    return results


# ── Status ───────────────────────────────────────────────────────────────────


def get_status() -> dict:
    """Return status dict for health/diagnostics endpoints."""
    has_key = bool(_get_api_key())
    return {
        "voyage_configured": has_key,
        "index_size": len(_index),
        "index_built": _index_built,
        "tfidf_index_size": len(_tfidf_index),
        "tfidf_built": _tfidf_built,
        "model": _VOYAGE_MODEL,
    }
