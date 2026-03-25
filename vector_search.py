#!/usr/bin/env python3
"""Semantic vector search using Voyage AI embeddings + Qdrant vector store.

Uses Voyage AI HTTP API for embeddings (no chromadb/voyageai pip packages).
Stores vectors in Qdrant Cloud for production persistence, with in-memory
fallback and TF-IDF as last resort.

Search tiers (in order):
    Tier 1: Qdrant Cloud vector store (production, QDRANT_URL + QDRANT_API_KEY)
    Tier 2: In-memory vector store with cosine similarity (warm standby)
    Tier 3: TF-IDF keyword matching (pure Python, no external calls)

Storage tiers for build_index:
    - Qdrant Cloud: persistent, shared across deploys
    - In-memory dict: fast, ephemeral, per-process
    - TF-IDF index: always built as warm standby

This keeps our stdlib-only approach while enabling semantic search across
the Nova knowledge base.

APIs:
    Voyage AI: POST https://api.voyageai.com/v1/embeddings
    Qdrant:    REST API at QDRANT_URL (collection: nova_knowledge, 1024-dim cosine)

Env vars:
    VOYAGE_API_KEY  -- Voyage AI embeddings (200M free tokens)
    QDRANT_URL      -- Qdrant Cloud cluster URL
    QDRANT_API_KEY  -- Qdrant Cloud API key

All functions:
    - Return empty/None on failure (never raise)
    - Log errors with exc_info=True
    - Use type hints on all signatures
"""

from __future__ import annotations

import collections
import hashlib
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
_VOYAGE_MIN_DELAY = 6.5  # Seconds between requests (10 RPM = 6s + 0.5s buffer)
_voyage_request_times: list[float] = []
_voyage_rate_lock = threading.Lock()
_voyage_last_request: float = 0.0  # monotonic timestamp of last API call

# ── Embedding disk cache ─────────────────────────────────────────────────────
# Caches Voyage AI embeddings to disk so server restarts don't re-compute them.
# Cache is keyed by a hash of the text content, stored as JSON.
_EMBEDDING_CACHE_FILE = (
    Path(__file__).resolve().parent / "data" / ".embedding_cache.json"
)
_embedding_cache: dict[str, list[float]] = {}
_embedding_cache_lock = threading.Lock()
_embedding_cache_loaded = False

# ── Qdrant Configuration ────────────────────────────────────────────────────
_QDRANT_URL: str = os.environ.get("QDRANT_URL") or ""
_QDRANT_API_KEY: str = os.environ.get("QDRANT_API_KEY") or ""
_QDRANT_COLLECTION = "nova_knowledge"
_QDRANT_VECTOR_DIM = 1024  # Voyage AI voyage-3-lite default dimension
_QDRANT_TIMEOUT = 15  # seconds
_qdrant_available: bool = False  # set True after successful collection create/verify
_qdrant_lock = threading.Lock()


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


# ── Embedding disk cache helpers ─────────────────────────────────────────────


def _text_cache_key(text: str) -> str:
    """Generate a stable cache key for a text string.

    Uses SHA-256 of the text content combined with the model name
    so cache invalidates if the model changes.

    Args:
        text: The text to compute a cache key for.

    Returns:
        Hex digest string suitable as a dict key.
    """
    content = f"{_VOYAGE_MODEL}:{text}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]


def _load_embedding_cache() -> None:
    """Load the embedding cache from disk (thread-safe, called once).

    Reads .embedding_cache.json from the data/ directory. If the file
    does not exist or is corrupt, starts with an empty cache.
    """
    global _embedding_cache, _embedding_cache_loaded

    if _embedding_cache_loaded:
        return

    with _embedding_cache_lock:
        if _embedding_cache_loaded:
            return

        if _EMBEDDING_CACHE_FILE.exists():
            try:
                raw = _EMBEDDING_CACHE_FILE.read_text(encoding="utf-8")
                loaded = json.loads(raw)
                if isinstance(loaded, dict):
                    _embedding_cache = loaded
                    logger.info(
                        "Loaded %d cached embeddings from disk", len(_embedding_cache)
                    )
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning(
                    "Failed to load embedding cache, starting fresh: %s", exc
                )
                _embedding_cache = {}
        else:
            logger.debug("No embedding cache file found, starting fresh")

        _embedding_cache_loaded = True


def _save_embedding_cache() -> None:
    """Persist the embedding cache to disk (thread-safe).

    Writes atomically by writing to a temp file then renaming.
    """
    with _embedding_cache_lock:
        cache_snapshot = dict(_embedding_cache)

    try:
        tmp_path = _EMBEDDING_CACHE_FILE.with_suffix(".tmp")
        tmp_path.write_text(
            json.dumps(cache_snapshot, separators=(",", ":")),
            encoding="utf-8",
        )
        tmp_path.replace(_EMBEDDING_CACHE_FILE)
        logger.info("Saved %d embeddings to disk cache", len(cache_snapshot))
    except OSError as exc:
        logger.error("Failed to save embedding cache: %s", exc, exc_info=True)


# ── Qdrant REST API helpers (stdlib-only, no pip packages) ───────────────────


def _qdrant_is_configured() -> bool:
    """Check if Qdrant credentials are present in environment."""
    return bool(_QDRANT_URL and _QDRANT_API_KEY)


def _qdrant_request(
    method: str,
    path: str,
    body: dict | None = None,
    timeout: int = _QDRANT_TIMEOUT,
) -> dict | None:
    """Send an HTTP request to the Qdrant REST API.

    Args:
        method: HTTP method (GET, PUT, POST, DELETE).
        path: API path (e.g., /collections/nova_knowledge).
        body: Optional JSON body.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    if not _qdrant_is_configured():
        return None

    # Strip trailing slash from URL, ensure path starts with /
    base = _QDRANT_URL.rstrip("/")
    if not path.startswith("/"):
        path = f"/{path}"

    url = f"{base}{path}"
    headers = {
        "api-key": _QDRANT_API_KEY,
        "Content-Type": "application/json",
    }

    data = json.dumps(body).encode("utf-8") if body else None

    try:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp_body = resp.read().decode("utf-8")
            if resp_body:
                return json.loads(resp_body)
            return {"status": "ok"}
    except urllib.error.HTTPError as exc:
        resp_text = ""
        try:
            resp_text = exc.read().decode("utf-8")[:500]
        except Exception:
            pass
        logger.error(
            "Qdrant API error %d %s for %s %s: %s",
            exc.code,
            exc.reason,
            method,
            path,
            resp_text,
            exc_info=True,
        )
        return None
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as exc:
        logger.error(
            "Qdrant request error for %s %s: %s", method, path, exc, exc_info=True
        )
        return None


def _qdrant_ensure_collection() -> bool:
    """Create the Qdrant collection if it does not already exist.

    Uses PUT with on_existing=skip to be idempotent. Sets up cosine
    distance with the correct vector dimension for Voyage AI embeddings.

    Returns:
        True if collection exists or was created, False on failure.
    """
    global _qdrant_available

    if not _qdrant_is_configured():
        return False

    # Check if collection already exists
    check = _qdrant_request("GET", f"/collections/{_QDRANT_COLLECTION}")
    if check and check.get("result"):
        _qdrant_available = True
        logger.info("Qdrant collection '%s' already exists", _QDRANT_COLLECTION)
        return True

    # Create collection
    result = _qdrant_request(
        "PUT",
        f"/collections/{_QDRANT_COLLECTION}",
        body={
            "vectors": {
                "size": _QDRANT_VECTOR_DIM,
                "distance": "Cosine",
            }
        },
    )
    if result is not None:
        _qdrant_available = True
        logger.info("Qdrant collection '%s' created successfully", _QDRANT_COLLECTION)
        return True

    logger.warning("Failed to create Qdrant collection '%s'", _QDRANT_COLLECTION)
    return False


def _qdrant_upsert_points(
    points: list[dict],
) -> bool:
    """Upsert points (vectors + payload) into the Qdrant collection.

    Points are batched into chunks of 100 for the REST API.

    Args:
        points: List of dicts with keys: id (int), vector (list[float]),
                payload (dict with text, doc_id, metadata).

    Returns:
        True if all batches succeeded, False if any failed.
    """
    if not _qdrant_available or not points:
        return False

    batch_size = 100
    success = True

    for i in range(0, len(points), batch_size):
        batch = points[i : i + batch_size]
        result = _qdrant_request(
            "PUT",
            f"/collections/{_QDRANT_COLLECTION}/points",
            body={"points": batch},
            timeout=30,  # larger batches need more time
        )
        if result is None:
            success = False
            logger.warning("Qdrant upsert batch %d-%d failed", i, i + len(batch))

    return success


def _qdrant_search(
    query_vector: list[float],
    top_k: int = 5,
) -> list[dict] | None:
    """Search Qdrant collection for nearest neighbors.

    Args:
        query_vector: Query embedding vector (1024-dim for voyage-3-lite).
        top_k: Number of results to return.

    Returns:
        List of result dicts with keys: id, text, metadata, score.
        Returns None on failure (so caller can fall back to in-memory).
    """
    if not _qdrant_available:
        return None

    result = _qdrant_request(
        "POST",
        f"/collections/{_QDRANT_COLLECTION}/points/search",
        body={
            "vector": query_vector,
            "limit": top_k,
            "with_payload": True,
        },
    )

    if result is None or "result" not in result:
        return None

    hits = result.get("result") or []
    results: list[dict] = []
    for hit in hits:
        payload = hit.get("payload") or {}
        results.append(
            {
                "id": payload.get("doc_id") or str(hit.get("id", "")),
                "text": (payload.get("text") or "")[:500],
                "metadata": payload.get("metadata") or {},
                "score": round(hit.get("score", 0.0), 4),
            }
        )

    return results


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

    Uses a disk cache to avoid re-computing embeddings on server restart.
    Only texts missing from the cache are sent to the Voyage API, with
    rate-limited batching to avoid 429 errors.

    Args:
        texts: List of texts to embed (max 128 per call).

    Returns:
        List of embedding vectors (list[list[float]]), or None on failure.
    """
    global _voyage_last_request

    api_key = _get_api_key()
    if not api_key:
        logger.warning(
            "VOYAGE_API_KEY not set. Sign up at https://www.voyageai.com "
            "and set VOYAGE_API_KEY environment variable."
        )
        return None

    if not texts:
        return []

    # Ensure disk cache is loaded
    _load_embedding_cache()

    # Truncate texts to match what we'd send to the API
    truncated: list[str] = [t[:8000] for t in texts]

    # Split into cached vs uncached
    result_embeddings: list[list[float] | None] = [None] * len(truncated)
    uncached_indices: list[int] = []

    with _embedding_cache_lock:
        for idx, text in enumerate(truncated):
            key = _text_cache_key(text)
            cached = _embedding_cache.get(key)
            if cached is not None:
                result_embeddings[idx] = cached
            else:
                uncached_indices.append(idx)

    cache_hits = len(truncated) - len(uncached_indices)
    if cache_hits > 0:
        logger.info(
            "Embedding cache: %d/%d hits, %d to compute via API",
            cache_hits,
            len(truncated),
            len(uncached_indices),
        )

    if not uncached_indices:
        # All embeddings were cached -- no API calls needed
        return [e for e in result_embeddings if e is not None]

    # Collect uncached texts and compute embeddings via API with rate limiting
    uncached_texts = [truncated[i] for i in uncached_indices]
    new_embeddings: list[list[float]] = []

    for batch_start in range(0, len(uncached_texts), _VOYAGE_MAX_BATCH):
        batch = uncached_texts[batch_start : batch_start + _VOYAGE_MAX_BATCH]

        payload = {
            "input": batch,
            "model": _VOYAGE_MODEL,
        }

        try:
            # Rate limiting: enforce BOTH minimum inter-request delay AND sliding window
            now = time.monotonic()
            with _voyage_rate_lock:
                # Minimum delay between consecutive requests to prevent bursts
                elapsed = now - _voyage_last_request
                if elapsed < _VOYAGE_MIN_DELAY and _voyage_last_request > 0:
                    wait_time = _VOYAGE_MIN_DELAY - elapsed
                    logger.debug(
                        "Voyage AI: spacing requests, waiting %.1fs", wait_time
                    )
                    time.sleep(wait_time)
                    now = time.monotonic()

                # Sliding window: prune requests older than 60s
                _voyage_request_times[:] = [
                    t for t in _voyage_request_times if now - t < 60
                ]

                # If we have 10+ requests in the last 60s, wait until oldest ages out
                if len(_voyage_request_times) >= _VOYAGE_RPM_LIMIT:
                    oldest_request = min(_voyage_request_times)
                    wait_time = 60.0 - (now - oldest_request) + 0.5
                    if wait_time > 0.001:
                        logger.info(
                            "Voyage AI rate limiting: waiting %.1fs for window to clear",
                            wait_time,
                        )
                        time.sleep(wait_time)

            _record_voyage_request()
            _voyage_last_request = time.monotonic()

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
                api_result = json.loads(body)

            embeddings_data = api_result.get("data") or []
            # Sort by index to preserve order
            embeddings_data.sort(key=lambda x: x.get("index", 0))
            batch_embeddings = [e.get("embedding") or [] for e in embeddings_data]
            new_embeddings.extend(batch_embeddings)

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

    if len(new_embeddings) != len(uncached_indices):
        logger.warning(
            "Voyage AI returned %d embeddings for %d texts",
            len(new_embeddings),
            len(uncached_indices),
        )
        return None

    # Merge new embeddings into result array and update cache
    cache_updated = False
    with _embedding_cache_lock:
        for local_idx, original_idx in enumerate(uncached_indices):
            embedding = new_embeddings[local_idx]
            result_embeddings[original_idx] = embedding
            # Save to cache
            key = _text_cache_key(truncated[original_idx])
            _embedding_cache[key] = embedding
            cache_updated = True

    # Persist cache to disk in background if we computed new embeddings
    if cache_updated:
        threading.Thread(
            target=_save_embedding_cache, daemon=True, name="save-embed-cache"
        ).start()

    # Verify all slots are filled
    final: list[list[float]] = []
    for emb in result_embeddings:
        if emb is None:
            logger.warning("Embedding result has unfilled slot, returning None")
            return None
        final.append(emb)

    return final


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

    # Upsert into Qdrant production vector store (if configured)
    _qdrant_index_count = 0
    if _qdrant_is_configured():
        try:
            if _qdrant_ensure_collection():
                qdrant_points: list[dict] = []
                for idx, entry in enumerate(new_entries):
                    qdrant_points.append(
                        {
                            "id": abs(hash(entry["id"])) % (2**63),  # int64 ID
                            "vector": entry["embedding"],
                            "payload": {
                                "doc_id": entry["id"],
                                "text": entry["text"][:2000],
                                "metadata": entry["metadata"],
                            },
                        }
                    )
                if _qdrant_upsert_points(qdrant_points):
                    _qdrant_index_count = len(qdrant_points)
                    logger.info(
                        "Qdrant: upserted %d vectors into '%s'",
                        _qdrant_index_count,
                        _QDRANT_COLLECTION,
                    )
                else:
                    logger.warning("Qdrant: partial or full upsert failure")
        except (OSError, ValueError, TypeError) as exc:
            logger.error("Qdrant indexing error: %s", exc, exc_info=True)

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

    qdrant_msg = f", Qdrant: {_qdrant_index_count}" if _qdrant_index_count else ""
    logger.info(
        "Vector index built: %d documents indexed (total: %d%s)",
        len(new_entries),
        len(_index),
        qdrant_msg,
    )


def _rerank_results(results: list[dict], query: str, top_k: int = 3) -> list[dict]:
    """Rerank search results using keyword overlap scoring.

    Simple but effective: combines vector similarity with keyword overlap
    for hybrid-like search without a separate sparse index.

    Args:
        results: List of search result dicts (must have text/content and score/similarity).
        query: Original search query string.
        top_k: Number of top results to return after reranking.

    Returns:
        Reranked and truncated list of result dicts.
    """
    if not results:
        return results

    query_terms = set(query.lower().split())

    for result in results:
        text = (result.get("text", "") or result.get("content", "")).lower()
        text_terms = set(text.split())

        # Keyword overlap score (Jaccard-like)
        overlap = len(query_terms & text_terms)
        keyword_score = overlap / max(len(query_terms), 1)

        # Combine with existing score (if any)
        vector_score = result.get("score", result.get("similarity", 0.5))

        # Weighted combination: 60% vector + 40% keyword
        result["combined_score"] = round(vector_score * 0.6 + keyword_score * 0.4, 4)
        result["keyword_overlap"] = overlap

    # Sort by combined score
    results.sort(key=lambda x: x.get("combined_score", 0), reverse=True)

    return results[:top_k]


def search(query: str, top_k: int = 5) -> list[dict]:
    """Semantic search across indexed documents.

    Tries in order:
        1. Qdrant vector store (production, if configured)
        2. In-memory vector search (Voyage AI embeddings)
        3. TF-IDF keyword search (pure Python fallback)

    Results are reranked using hybrid scoring (vector + keyword overlap)
    before being returned.

    Args:
        query: Natural language search query.
        top_k: Number of top results to return.

    Returns:
        List of dicts with keys: id, text, metadata, score.
        Returns empty list on failure.
    """
    query_embedding: list[float] | None = None

    # Get embedding once (shared by Qdrant and in-memory tiers)
    if _qdrant_available or _index:
        query_embedding = embed_text(query)

    # Tier 1: Qdrant production vector store
    if _qdrant_available and query_embedding is not None:
        try:
            qdrant_results = _qdrant_search(query_embedding, top_k=top_k)
            if qdrant_results:
                logger.debug(
                    "Qdrant search returned %d results for query=%s",
                    len(qdrant_results),
                    query[:50],
                )
                results = _rerank_results(qdrant_results, query, top_k)
                return results
        except (OSError, ValueError, TypeError) as exc:
            logger.error("Qdrant search error, falling back: %s", exc, exc_info=True)

    # Tier 2: In-memory Voyage AI vector search
    if _index and query_embedding is not None:
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
        results = _rerank_results(results, query, top_k)
        return results

    if query_embedding is None and (_qdrant_available or _index):
        # Voyage AI failed at query time -- fall through to TF-IDF
        logger.warning("Voyage AI embedding failed for query, falling back to TF-IDF")

    # Tier 3: TF-IDF keyword search fallback
    tfidf_results = _tfidf_search(query, top_k=top_k)
    if tfidf_results:
        logger.info(
            "TF-IDF fallback returned %d results for query=%s",
            len(tfidf_results),
            query[:50],
        )
        results = _rerank_results(tfidf_results, query, top_k)
        return results

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
        "embedding_cache_size": len(_embedding_cache),
        "embedding_cache_loaded": _embedding_cache_loaded,
        "model": _VOYAGE_MODEL,
    }
