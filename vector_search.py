#!/usr/bin/env python3
"""Semantic vector search using Voyage AI embeddings + in-memory vector store.

Uses Voyage AI HTTP API for embeddings (no chromadb/voyageai pip packages).
Implements a pure-Python in-memory vector store with cosine similarity.

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

import json
import logging
import math
import os
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
        logger.error("build_index: embedding generation failed")
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

    logger.info(
        "Vector index built: %d documents indexed (total: %d)",
        len(new_entries),
        len(_index),
    )


def search(query: str, top_k: int = 5) -> list[dict]:
    """Semantic search across indexed documents.

    Args:
        query: Natural language search query.
        top_k: Number of top results to return.

    Returns:
        List of dicts with keys: id, text, metadata, score.
        Returns empty list on failure.
    """
    if not _index:
        return []

    query_embedding = embed_text(query)
    if query_embedding is None:
        return []

    # Compute similarities
    scored: list[tuple[float, dict]] = []
    with _index_lock:
        for entry in _index:
            sim = _cosine_similarity(query_embedding, entry["embedding"])
            scored.append((sim, entry))

    # Sort by similarity descending
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


# ── Status ───────────────────────────────────────────────────────────────────


def get_status() -> dict:
    """Return status dict for health/diagnostics endpoints."""
    has_key = bool(_get_api_key())
    return {
        "voyage_configured": has_key,
        "index_size": len(_index),
        "index_built": _index_built,
        "model": _VOYAGE_MODEL,
    }
