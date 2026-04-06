#!/usr/bin/env python3
"""Hybrid search using Voyage AI vector embeddings + BM25 keyword matching.

Uses Voyage AI HTTP API for embeddings (no chromadb/voyageai pip packages).
Stores vectors in Qdrant Cloud for production persistence, with in-memory
fallback and TF-IDF as last resort.

Search strategy:
    Hybrid = Vector similarity (semantic) + BM25 (exact keyword matching)
    Combined via Reciprocal Rank Fusion (RRF, k=60).

    Vector tiers (in order):
        Tier 1: Qdrant Cloud vector store (production, QDRANT_URL + QDRANT_API_KEY)
        Tier 2: In-memory vector store with cosine similarity (warm standby)

    BM25 tier:
        Always available once index is built. Pure Python, no external deps.

    Fallback:
        TF-IDF keyword matching if both vector and BM25 fail.

Storage tiers for build_index:
    - Qdrant Cloud: persistent, shared across deploys
    - In-memory dict: fast, ephemeral, per-process
    - BM25 index: always built alongside vector index
    - TF-IDF index: always built as warm standby

This keeps our stdlib-only approach while enabling hybrid search across
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
import ssl
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# SSL context for Voyage AI API -- Python 3.14+ has stricter TLS defaults
# that cause WRONG_VERSION_NUMBER errors with some API endpoints.
_VOYAGE_SSL_CTX: ssl.SSLContext = ssl.create_default_context()
_VOYAGE_SSL_CTX.minimum_version = ssl.TLSVersion.TLSv1_2

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
_VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"
_VOYAGE_MODEL = "voyage-3-lite"  # Good balance of quality/speed/cost
_VOYAGE_TIMEOUT = 20  # seconds
_VOYAGE_API_KEY: str | None = None
_VOYAGE_MAX_BATCH = 32  # Reduced from 128 to avoid 429s on startup bursts
_VOYAGE_STARTUP_BATCH = 16  # Even smaller batches during initial indexing
_VOYAGE_RPM_LIMIT = 10  # Voyage free tier: ~10 req/min
_VOYAGE_MIN_DELAY = 6.5  # Seconds between requests (10 RPM = 6s + 0.5s buffer)
_VOYAGE_MAX_RETRIES = 3  # Max retries on 429 errors
_VOYAGE_BASE_BACKOFF = 2.0  # Base backoff in seconds for exponential retry
_is_startup_indexing = True  # Flag to use smaller batches during startup
_voyage_request_times: list[float] = []
_voyage_rate_lock = threading.Lock()
_voyage_last_request: float = 0.0  # monotonic timestamp of last API call

# ── Embedding disk cache ─────────────────────────────────────────────────────
# Caches Voyage AI embeddings to disk so server restarts don't re-compute them.
# Cache is keyed by a hash of the text content, stored as JSON.
_PERSISTENT_DISK = Path("/data/persistent")
_EMBEDDING_CACHE_FILE = (
    _PERSISTENT_DISK / ".embedding_cache.json"
    if _PERSISTENT_DISK.exists()
    else Path(__file__).resolve().parent / "data" / ".embedding_cache.json"
)
_embedding_cache: dict[str, list[float]] = {}
_embedding_cache_lock = threading.Lock()
_embedding_cache_loaded = False

# ── Qdrant Configuration ────────────────────────────────────────────────────
_QDRANT_URL: str = os.environ.get("QDRANT_URL") or ""
_QDRANT_API_KEY: str = os.environ.get("QDRANT_API_KEY") or ""
_QDRANT_COLLECTION = "nova_knowledge"
_QDRANT_VECTOR_DIM = 512  # Voyage AI voyage-3-lite produces 512-dim vectors
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
                "id": payload.get("doc_id") or str(hit.get("id") or ""),
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

    # Use smaller batch size during startup to reduce 429 risk
    effective_batch_size = (
        _VOYAGE_STARTUP_BATCH if _is_startup_indexing else _VOYAGE_MAX_BATCH
    )

    for batch_start in range(0, len(uncached_texts), effective_batch_size):
        batch = uncached_texts[batch_start : batch_start + effective_batch_size]

        payload = {
            "input": batch,
            "model": _VOYAGE_MODEL,
        }

        # Retry loop with exponential backoff for 429 errors
        for attempt in range(_VOYAGE_MAX_RETRIES + 1):
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
                with urllib.request.urlopen(
                    req, timeout=_VOYAGE_TIMEOUT, context=_VOYAGE_SSL_CTX
                ) as resp:
                    body = resp.read().decode("utf-8")
                    api_result = json.loads(body)

                embeddings_data = api_result.get("data") or []
                # Sort by index to preserve order
                embeddings_data.sort(key=lambda x: x.get("index", 0))
                batch_embeddings = [e.get("embedding") or [] for e in embeddings_data]
                new_embeddings.extend(batch_embeddings)
                break  # Success -- exit retry loop

            except urllib.error.HTTPError as e:
                if e.code == 429 and attempt < _VOYAGE_MAX_RETRIES:
                    # Parse Retry-After header if present
                    retry_after_header = (
                        e.headers.get("Retry-After") if e.headers else None
                    )
                    if retry_after_header:
                        try:
                            backoff_time = float(retry_after_header)
                        except ValueError:
                            backoff_time = _VOYAGE_BASE_BACKOFF * (2**attempt)
                    else:
                        backoff_time = _VOYAGE_BASE_BACKOFF * (2**attempt)

                    logger.info(
                        "Voyage AI 429 rate limited (attempt %d/%d), "
                        "backing off %.1fs before retry",
                        attempt + 1,
                        _VOYAGE_MAX_RETRIES,
                        backoff_time,
                    )
                    time.sleep(backoff_time)
                    continue  # Retry this batch
                elif e.code == 429:
                    # Exhausted retries on 429 -- fall back to TF-IDF gracefully
                    logger.warning(
                        "Voyage AI 429 rate limit persists after %d retries, "
                        "falling back to TF-IDF for remaining embeddings",
                        _VOYAGE_MAX_RETRIES,
                    )
                    return None
                else:
                    logger.error(
                        "Voyage AI HTTP error %d: %s",
                        e.code,
                        e.reason,
                        exc_info=True,
                    )
                    return None
            except urllib.error.URLError as e:
                reason_str = str(e.reason) if e.reason else ""
                if "SSL" in reason_str and attempt < _VOYAGE_MAX_RETRIES:
                    logger.warning(
                        "Voyage AI SSL error (attempt %d/%d), retrying: %s",
                        attempt + 1,
                        _VOYAGE_MAX_RETRIES,
                        reason_str[:100],
                    )
                    time.sleep(_VOYAGE_BASE_BACKOFF * (2**attempt))
                    continue
                logger.error(
                    "Voyage AI URL error: %s",
                    e.reason,
                    exc_info=True,
                )
                return None
            except OSError as e:
                err_str = str(e)
                if "SSL" in err_str and attempt < _VOYAGE_MAX_RETRIES:
                    logger.warning(
                        "Voyage AI SSL/OS error (attempt %d/%d), retrying: %s",
                        attempt + 1,
                        _VOYAGE_MAX_RETRIES,
                        err_str[:100],
                    )
                    time.sleep(_VOYAGE_BASE_BACKOFF * (2**attempt))
                    continue
                logger.error("Voyage AI OS error: %s", e, exc_info=True)
                return None
            except (json.JSONDecodeError, ValueError, TypeError) as e:
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

    # Stagger embedding in smaller chunks to avoid overwhelming Voyage AI on startup
    _STAGGER_CHUNK_SIZE = 64  # Process 64 docs at a time with delays between chunks
    _STAGGER_DELAY = 2.0  # Seconds between staggered chunks

    all_embeddings: list[list[float]] = []
    embedding_failed = False

    for chunk_start in range(0, len(valid_texts), _STAGGER_CHUNK_SIZE):
        chunk_texts = valid_texts[chunk_start : chunk_start + _STAGGER_CHUNK_SIZE]

        # Add delay between chunks (not before the first one)
        if chunk_start > 0:
            logger.debug(
                "Staggering startup embedding: chunk %d/%d, waiting %.1fs",
                chunk_start // _STAGGER_CHUNK_SIZE + 1,
                (len(valid_texts) + _STAGGER_CHUNK_SIZE - 1) // _STAGGER_CHUNK_SIZE,
                _STAGGER_DELAY,
            )
            time.sleep(_STAGGER_DELAY)

        chunk_embeddings = embed_batch(chunk_texts)
        if chunk_embeddings is None:
            embedding_failed = True
            break
        all_embeddings.extend(chunk_embeddings)

    embeddings = all_embeddings if not embedding_failed else None

    # Prepare flat doc list for BM25 + TF-IDF indexing (shared by both paths)
    flat_docs = [
        {
            "id": doc.get("id") or "",
            "text": text,
            "metadata": doc.get("metadata") or {},
        }
        for doc, text in valid_docs
    ]

    if embeddings is None:
        logger.info(
            "build_index: Voyage AI embedding unavailable, building BM25 + TF-IDF fallback"
        )
        # Build BM25 index (always, for hybrid search)
        _bm25_index.index(flat_docs)
        # Build TF-IDF fallback index from the valid documents
        _build_tfidf_index(flat_docs)
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

    # Build BM25 index for hybrid search (always alongside vector index)
    _bm25_index.index(flat_docs)

    # Also build TF-IDF index as a warm standby for runtime fallback
    _build_tfidf_index(flat_docs)

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
        text = (result.get("text") or result.get("content") or "").lower()
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


# ── BM25 Index (Okapi BM25 keyword scoring) ─────────────────────────────────


class BM25Index:
    """Okapi BM25 keyword index for knowledge base documents.

    Pure Python implementation with no external dependencies.
    Built alongside the vector index at startup for hybrid search.

    BM25 excels at exact keyword matches (e.g., "LinkedIn CPC benchmarks")
    while vector search excels at semantic similarity (e.g., "social media
    cost metrics"). Combining both via RRF gives best-of-both-worlds retrieval.

    Args:
        k1: Term frequency saturation parameter (default 1.5).
        b: Document length normalization parameter (default 0.75).
    """

    def __init__(self, k1: float = 1.5, b: float = 0.75) -> None:
        self.k1 = k1
        self.b = b
        self.doc_lengths: list[int] = []
        self.avg_dl: float = 0.0
        self.doc_freqs: dict[str, int] = {}  # term -> number of docs containing term
        self.term_freqs: list[dict[str, int]] = []  # doc_idx -> {term: raw_count}
        self.doc_ids: list[str] = []
        self.doc_texts: list[str] = []
        self.doc_meta: list[dict] = []
        self.N: int = 0
        self._built = False
        self._lock = threading.Lock()

    @property
    def is_built(self) -> bool:
        """Whether the BM25 index has been built."""
        return self._built

    def index(self, documents: list[dict]) -> None:
        """Build BM25 index from documents.

        Each document should have: {"id": str, "text": str, "metadata": dict}.
        Tokenizes using the shared _tokenize() function (lowercase, stop-word removal).

        Args:
            documents: List of document dicts to index.
        """
        if not documents:
            return

        with self._lock:
            self.doc_lengths.clear()
            self.doc_freqs.clear()
            self.term_freqs.clear()
            self.doc_ids.clear()
            self.doc_texts.clear()
            self.doc_meta.clear()

            df: dict[str, int] = collections.defaultdict(int)

            for doc in documents:
                text = doc.get("text") or ""
                tokens = _tokenize(text)
                tf = dict(collections.Counter(tokens))

                self.term_freqs.append(tf)
                self.doc_lengths.append(len(tokens))
                self.doc_ids.append(doc.get("id") or "")
                self.doc_texts.append(text[:500])
                self.doc_meta.append(doc.get("metadata") or {})

                for term in set(tokens):
                    df[term] += 1

            self.doc_freqs = dict(df)
            self.N = len(documents)
            self.avg_dl = sum(self.doc_lengths) / self.N if self.N > 0 else 0.0
            self._built = True

        logger.info(
            "BM25 index built: %d documents, %d unique terms, avg_dl=%.1f",
            self.N,
            len(self.doc_freqs),
            self.avg_dl,
        )

    def search(self, query: str, top_k: int = 10) -> list[tuple[str, float]]:
        """Search the BM25 index and return ranked (doc_id, score) tuples.

        Uses Okapi BM25 scoring formula:
            score(D,Q) = sum_over_terms( IDF(qi) * (f(qi,D) * (k1+1)) /
                                          (f(qi,D) + k1 * (1 - b + b * |D|/avgdl)) )

        Args:
            query: Natural language query string.
            top_k: Number of top results to return.

        Returns:
            List of (doc_id, bm25_score) tuples sorted by score descending.
        """
        if not self._built or self.N == 0:
            return []

        query_tokens = _tokenize(query)
        if not query_tokens:
            return []

        scores: list[tuple[float, int]] = []

        with self._lock:
            for doc_idx in range(self.N):
                score = 0.0
                dl = self.doc_lengths[doc_idx]
                tf_map = self.term_freqs[doc_idx]

                for term in query_tokens:
                    if term not in self.doc_freqs:
                        continue

                    # IDF: log((N - df + 0.5) / (df + 0.5) + 1)
                    df = self.doc_freqs[term]
                    idf = math.log((self.N - df + 0.5) / (df + 0.5) + 1.0)

                    # TF component with length normalization
                    f = tf_map.get(term, 0)
                    if f == 0:
                        continue

                    tf_norm = (f * (self.k1 + 1.0)) / (
                        f + self.k1 * (1.0 - self.b + self.b * dl / self.avg_dl)
                    )

                    score += idf * tf_norm

                if score > 0.0:
                    scores.append((score, doc_idx))

        scores.sort(key=lambda x: x[0], reverse=True)

        results: list[tuple[str, float]] = []
        for bm25_score, doc_idx in scores[:top_k]:
            results.append((self.doc_ids[doc_idx], round(bm25_score, 4)))

        return results

    def get_doc(self, doc_id: str) -> dict | None:
        """Retrieve a document by its ID from the BM25 index.

        Args:
            doc_id: The document ID to look up.

        Returns:
            Dict with id, text, metadata -- or None if not found.
        """
        try:
            idx = self.doc_ids.index(doc_id)
            return {
                "id": self.doc_ids[idx],
                "text": self.doc_texts[idx],
                "metadata": self.doc_meta[idx],
            }
        except ValueError:
            return None


# ── BM25 index instance (module-level singleton) ────────────────────────────
_bm25_index = BM25Index()


def reciprocal_rank_fusion(
    vector_results: list[tuple[str, float]],
    bm25_results: list[tuple[str, float]],
    k: int = 60,
) -> list[tuple[str, float]]:
    """Combine vector and BM25 rankings using Reciprocal Rank Fusion.

    RRF merges two ranked lists by summing reciprocal ranks. Documents
    that appear in both lists get boosted, while documents appearing in
    only one list still contribute. The parameter k controls how much
    weight is given to lower-ranked results (higher k = more uniform).

    Reference: Cormack, Clarke, Buettcher (2009) - "Reciprocal Rank Fusion
    outperforms Condorcet and individual Rank Learning Methods"

    Args:
        vector_results: List of (doc_id, score) from vector search.
        bm25_results: List of (doc_id, score) from BM25 search.
        k: RRF constant (default 60, per original paper).

    Returns:
        Merged list of (doc_id, rrf_score) sorted by RRF score descending.
    """
    scores: dict[str, float] = {}

    for rank, (doc_id, _) in enumerate(vector_results):
        scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)

    for rank, (doc_id, _) in enumerate(bm25_results):
        scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    return [(doc_id, round(score, 6)) for doc_id, score in ranked]


def _get_vector_results(
    query: str,
    query_embedding: list[float] | None,
    fetch_k: int,
) -> list[tuple[str, float]]:
    """Get vector search results as (doc_id, score) tuples.

    Tries Qdrant first, then falls back to in-memory vector index.

    Args:
        query: The search query (for logging).
        query_embedding: Pre-computed query embedding vector.
        fetch_k: Number of results to fetch.

    Returns:
        List of (doc_id, score) tuples sorted by score descending.
    """
    if query_embedding is None:
        return []

    # Tier 1: Qdrant production vector store
    if _qdrant_available:
        try:
            qdrant_results = _qdrant_search(query_embedding, top_k=fetch_k)
            if qdrant_results:
                logger.debug(
                    "Qdrant search returned %d results for query=%s",
                    len(qdrant_results),
                    query[:50],
                )
                return [(r["id"], r.get("score", 0.0)) for r in qdrant_results]
        except (OSError, ValueError, TypeError) as exc:
            logger.error("Qdrant search error, falling back: %s", exc, exc_info=True)

    # Tier 2: In-memory vector search
    if _index:
        scored: list[tuple[float, str]] = []
        with _index_lock:
            for entry in _index:
                sim = _cosine_similarity(query_embedding, entry["embedding"])
                scored.append((sim, entry["id"]))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [(doc_id, score) for score, doc_id in scored[:fetch_k]]

    return []


def _resolve_doc(doc_id: str) -> dict | None:
    """Look up full document data by doc_id from any available index.

    Checks in-memory vector index, BM25 index, then TF-IDF index.

    Args:
        doc_id: The document identifier.

    Returns:
        Dict with id, text, metadata -- or None if not found.
    """
    # Check in-memory vector index
    with _index_lock:
        for entry in _index:
            if entry["id"] == doc_id:
                return {
                    "id": entry["id"],
                    "text": entry["text"][:500],
                    "metadata": entry["metadata"],
                }

    # Check BM25 index
    bm25_doc = _bm25_index.get_doc(doc_id)
    if bm25_doc:
        return bm25_doc

    # Check TF-IDF index
    if _tfidf_built:
        try:
            idx = _tfidf_doc_ids.index(doc_id)
            return {
                "id": _tfidf_doc_ids[idx],
                "text": _tfidf_doc_texts[idx],
                "metadata": _tfidf_doc_meta[idx],
            }
        except ValueError:
            pass

    return None


def search(query: str, top_k: int = 5) -> list[dict]:
    """Hybrid search across indexed documents (vector + BM25 via RRF).

    Combines semantic vector similarity with BM25 keyword matching using
    Reciprocal Rank Fusion for best-of-both-worlds retrieval:
    - Vector search catches semantic matches ("talent acquisition" ~ "hiring")
    - BM25 catches exact keyword matches ("LinkedIn CPC benchmarks")
    - RRF merges both rankings without needing score normalization

    Fallback order when hybrid is unavailable:
        1. Vector-only with keyword reranking (if BM25 not built)
        2. BM25-only (if embeddings unavailable)
        3. TF-IDF keyword search (if both above fail)

    Args:
        query: Natural language search query.
        top_k: Number of top results to return.

    Returns:
        List of dicts with keys: id, text, metadata, score.
        Returns empty list on failure.
    """
    fetch_k = top_k * 2  # Fetch 2x from each source for better RRF fusion

    query_embedding: list[float] | None = None

    # Get embedding once (shared by Qdrant and in-memory tiers)
    if _qdrant_available or _index:
        query_embedding = embed_text(query)

    if query_embedding is None and (_qdrant_available or _index):
        logger.warning(
            "Voyage AI embedding failed for query, falling back to BM25/TF-IDF"
        )

    # ── Hybrid path: Vector + BM25 via RRF ──────────────────────────────
    vector_results = _get_vector_results(query, query_embedding, fetch_k)
    bm25_results = (
        _bm25_index.search(query, top_k=fetch_k) if _bm25_index.is_built else []
    )

    if vector_results and bm25_results:
        # Full hybrid: merge both via RRF
        fused = reciprocal_rank_fusion(vector_results, bm25_results, k=60)
        logger.debug(
            "Hybrid search: %d vector + %d BM25 -> %d fused for query=%s",
            len(vector_results),
            len(bm25_results),
            len(fused),
            query[:50],
        )

        results: list[dict] = []
        for doc_id, rrf_score in fused[:top_k]:
            doc = _resolve_doc(doc_id)
            if doc:
                doc["score"] = rrf_score
                doc["search_method"] = "hybrid_rrf"
                results.append(doc)

        if results:
            return results

    # ── Vector-only path (BM25 not available) ───────────────────────────
    if vector_results:
        logger.debug(
            "Vector-only search: %d results for query=%s",
            len(vector_results),
            query[:50],
        )
        results = []
        for doc_id, score in vector_results[:top_k]:
            doc = _resolve_doc(doc_id)
            if doc:
                doc["score"] = round(score, 4)
                doc["search_method"] = "vector"
                results.append(doc)

        if results:
            results = _rerank_results(results, query, top_k)
            return results

    # ── BM25-only path (embeddings unavailable) ─────────────────────────
    if bm25_results:
        logger.info(
            "BM25-only search: %d results for query=%s",
            len(bm25_results),
            query[:50],
        )
        results = []
        for doc_id, score in bm25_results[:top_k]:
            doc = _resolve_doc(doc_id)
            if doc:
                doc["score"] = round(score, 4)
                doc["search_method"] = "bm25"
                results.append(doc)

        if results:
            return results

    # ── TF-IDF fallback (last resort) ───────────────────────────────────
    tfidf_results = _tfidf_search(query, top_k=top_k)
    if tfidf_results:
        logger.info(
            "TF-IDF fallback returned %d results for query=%s",
            len(tfidf_results),
            query[:50],
        )
        for r in tfidf_results:
            r["search_method"] = "tfidf"
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
        "international_sources.json",
        # 2026 research files (added to close data flow gap)
        "hr_tech_landscape_2026.json",
        "publisher_benchmarks_2026.json",
        "recruitment_marketing_trends_2026.json",
        "labor_market_outlook_2026.json",
        "salary_benchmarks_detailed_2026.json",
        "ad_benchmarks_recruitment_2026.json",
        "industry_hiring_patterns_2026.json",
        "top_employers_by_city_2026.json",
        "compliance_regulations_2026.json",
        "agency_rpo_market_2026.json",
        # H-1B salary intelligence (rich city-level wage data)
        "h1b_salary_intelligence.json",
        # Cross-product recruitment benchmarks (S45 deep research -- 28 sources)
        "recruitment_benchmarks_comprehensive_2026.json",
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

    # Clear startup flag so subsequent embed_batch calls use normal batch sizes
    global _is_startup_indexing
    _is_startup_indexing = False

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
        "bm25_index_size": _bm25_index.N,
        "bm25_built": _bm25_index.is_built,
        "bm25_vocab_size": len(_bm25_index.doc_freqs),
        "search_mode": (
            "hybrid_rrf"
            if (_index_built or _qdrant_available) and _bm25_index.is_built
            else (
                "vector"
                if _index_built or _qdrant_available
                else ("bm25" if _bm25_index.is_built else "tfidf")
            )
        ),
        "tfidf_index_size": len(_tfidf_index),
        "tfidf_built": _tfidf_built,
        "embedding_cache_size": len(_embedding_cache),
        "embedding_cache_loaded": _embedding_cache_loaded,
        "model": _VOYAGE_MODEL,
    }
