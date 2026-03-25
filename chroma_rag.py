"""chroma_rag.py -- ChromaDB vector store for Nova RAG (Retrieval-Augmented Generation).

Provides document indexing and semantic search using ChromaDB as a persistent
vector store. Enhances Nova's knowledge base search with embedding-based
retrieval as a complement/fallback to the existing Voyage AI + Qdrant pipeline.

Functions:
    index_document(doc_id, text, metadata)  -- Add/update a document
    search_similar(query, top_k)            -- Semantic similarity search
    get_collection_stats()                  -- Collection size and health info
    bulk_index(documents)                   -- Batch index multiple documents
    initialize()                            -- Initialize ChromaDB client + collection

Architecture:
    - Uses ChromaDB's built-in embedding function (default: all-MiniLM-L6-v2)
    - Persistent storage at ./chroma_data/ for survival across restarts
    - Falls back gracefully if chromadb is not installed
    - Thread-safe via ChromaDB's internal locking

Configuration:
    CHROMA_PERSIST_DIR    -- Storage directory (default: ./chroma_data)
    CHROMA_COLLECTION     -- Collection name (default: nova_knowledge)

All functions return empty/None on failure (never raise).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# -- Configuration -----------------------------------------------------------

_PERSIST_DIR: str = os.environ.get("CHROMA_PERSIST_DIR") or str(
    Path(__file__).resolve().parent / "chroma_data"
)
_COLLECTION_NAME: str = os.environ.get("CHROMA_COLLECTION") or "nova_knowledge"

# -- Module state ------------------------------------------------------------

_client: Any = None
_collection: Any = None
_initialized: bool = False
_available: bool = False
_init_lock = threading.Lock()
_init_error: Optional[str] = None

# -- Chunk size limits -------------------------------------------------------

_MAX_DOCUMENT_LENGTH: int = 8000  # characters per document chunk
_MAX_BATCH_SIZE: int = 100  # max documents per bulk insert


def _ensure_initialized() -> bool:
    """Ensure ChromaDB client and collection are initialized.

    Thread-safe lazy initialization. Called automatically by all public functions.

    Returns:
        True if ChromaDB is available and initialized, False otherwise.
    """
    global _client, _collection, _initialized, _available, _init_error

    if _initialized:
        return _available

    with _init_lock:
        if _initialized:
            return _available

        try:
            import chromadb

            _client = chromadb.PersistentClient(path=_PERSIST_DIR)
            _collection = _client.get_or_create_collection(
                name=_COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )
            _available = True
            _initialized = True
            logger.info(
                "chroma_rag: initialized (persist_dir=%s, collection=%s, count=%d)",
                _PERSIST_DIR,
                _COLLECTION_NAME,
                _collection.count(),
            )
            return True

        except ImportError:
            _init_error = "chromadb package not installed"
            _available = False
            _initialized = True
            logger.warning(
                "chroma_rag: chromadb not installed; "
                "install with 'pip install chromadb' to enable RAG"
            )
            return False

        except (OSError, RuntimeError, ValueError) as exc:
            _init_error = str(exc)
            _available = False
            _initialized = True
            logger.error("chroma_rag: initialization failed: %s", exc, exc_info=True)
            return False


def initialize() -> bool:
    """Explicitly initialize ChromaDB client and collection.

    Can be called at startup to fail fast rather than on first query.

    Returns:
        True if ChromaDB is available, False otherwise.
    """
    return _ensure_initialized()


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════


def index_document(
    doc_id: str,
    text: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    """Index a single document into the ChromaDB collection.

    If a document with the same ID already exists, it will be updated
    (upsert semantics).

    Args:
        doc_id: Unique document identifier.
        text: Document text content (will be truncated to 8000 chars).
        metadata: Optional metadata dict (must contain only str/int/float/bool values).

    Returns:
        True if the document was indexed successfully, False otherwise.
    """
    if not _ensure_initialized():
        return False

    if not doc_id or not text:
        logger.warning("chroma_rag: doc_id and text are required for indexing")
        return False

    # Truncate text to avoid embedding failures
    text = text[:_MAX_DOCUMENT_LENGTH]

    # Sanitize metadata -- ChromaDB only accepts str/int/float/bool values
    clean_meta = _sanitize_metadata(metadata)

    try:
        _collection.upsert(
            ids=[doc_id],
            documents=[text],
            metadatas=[clean_meta] if clean_meta else None,
        )
        logger.debug("chroma_rag: indexed document '%s' (%d chars)", doc_id, len(text))
        return True

    except (RuntimeError, ValueError, TypeError) as exc:
        logger.error(
            "chroma_rag: failed to index document '%s': %s",
            doc_id,
            exc,
            exc_info=True,
        )
        return False


def bulk_index(
    documents: List[Dict[str, Any]],
) -> int:
    """Batch index multiple documents into ChromaDB.

    Each document dict should have keys: 'id', 'text', and optionally 'metadata'.
    Documents are processed in batches of 100 for efficiency.

    Args:
        documents: List of document dicts with 'id' and 'text' keys.

    Returns:
        Number of documents successfully indexed.
    """
    if not _ensure_initialized():
        return 0

    if not documents:
        return 0

    indexed = 0

    for i in range(0, len(documents), _MAX_BATCH_SIZE):
        batch = documents[i : i + _MAX_BATCH_SIZE]
        ids: List[str] = []
        texts: List[str] = []
        metadatas: List[Dict[str, Any]] = []

        for doc in batch:
            doc_id = doc.get("id") or ""
            text = doc.get("text") or ""
            if not doc_id or not text:
                continue
            ids.append(doc_id)
            texts.append(text[:_MAX_DOCUMENT_LENGTH])
            metadatas.append(_sanitize_metadata(doc.get("metadata")))

        if not ids:
            continue

        try:
            _collection.upsert(
                ids=ids,
                documents=texts,
                metadatas=metadatas,
            )
            indexed += len(ids)
            logger.debug(
                "chroma_rag: bulk indexed batch %d-%d (%d docs)",
                i,
                i + len(ids),
                len(ids),
            )
        except (RuntimeError, ValueError, TypeError) as exc:
            logger.error(
                "chroma_rag: bulk index batch %d failed: %s",
                i,
                exc,
                exc_info=True,
            )

    logger.info("chroma_rag: bulk indexed %d/%d documents", indexed, len(documents))
    return indexed


def search_similar(
    query: str,
    top_k: int = 5,
    where: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Search for documents similar to the query using ChromaDB embeddings.

    Args:
        query: Natural language search query.
        top_k: Number of top results to return (default 5).
        where: Optional ChromaDB where filter for metadata filtering.

    Returns:
        List of result dicts with keys: id, text, metadata, score.
        Returns empty list on failure.
    """
    if not _ensure_initialized():
        return []

    if not query:
        return []

    try:
        query_params: Dict[str, Any] = {
            "query_texts": [query],
            "n_results": min(top_k, _collection.count() or 1),
        }
        if where:
            query_params["where"] = where

        results = _collection.query(**query_params)

        if not results or not results.get("ids"):
            return []

        ids = results["ids"][0] if results["ids"] else []
        documents = results["documents"][0] if results.get("documents") else []
        metadatas = results["metadatas"][0] if results.get("metadatas") else []
        distances = results["distances"][0] if results.get("distances") else []

        output: List[Dict[str, Any]] = []
        for idx, doc_id in enumerate(ids):
            # ChromaDB returns distances (lower = more similar for cosine)
            # Convert to similarity score: similarity = 1 - distance
            distance = distances[idx] if idx < len(distances) else 1.0
            similarity = max(0.0, 1.0 - distance)

            output.append(
                {
                    "id": doc_id,
                    "text": (documents[idx] if idx < len(documents) else "")[:500],
                    "metadata": metadatas[idx] if idx < len(metadatas) else {},
                    "score": round(similarity, 4),
                }
            )

        logger.debug(
            "chroma_rag: search returned %d results for query='%s'",
            len(output),
            query[:50],
        )
        return output

    except (RuntimeError, ValueError, TypeError) as exc:
        logger.error(
            "chroma_rag: search failed for query='%s': %s",
            query[:50],
            exc,
            exc_info=True,
        )
        return []


def get_collection_stats() -> Dict[str, Any]:
    """Return statistics about the ChromaDB collection.

    Returns:
        Dictionary with collection name, document count, persist directory,
        and availability status.
    """
    if not _ensure_initialized():
        return {
            "available": False,
            "error": _init_error or "not initialized",
            "collection": _COLLECTION_NAME,
            "persist_dir": _PERSIST_DIR,
            "count": 0,
        }

    try:
        count = _collection.count()
    except (RuntimeError, ValueError) as exc:
        logger.error("chroma_rag: failed to get count: %s", exc, exc_info=True)
        count = -1

    return {
        "available": True,
        "collection": _COLLECTION_NAME,
        "persist_dir": _PERSIST_DIR,
        "count": count,
    }


def get_status() -> Dict[str, Any]:
    """Return health/diagnostic status for the Chroma RAG module.

    Alias for get_collection_stats() for consistency with other modules.

    Returns:
        Dictionary with availability and collection stats.
    """
    return get_collection_stats()


# ═══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _sanitize_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Sanitize metadata to contain only ChromaDB-compatible value types.

    ChromaDB metadata values must be str, int, float, or bool.
    Nested dicts/lists are serialized to JSON strings.

    Args:
        metadata: Raw metadata dict (may contain any types).

    Returns:
        Cleaned metadata dict with only compatible value types.
    """
    if not metadata:
        return {}

    clean: Dict[str, Any] = {}
    for key, value in metadata.items():
        if isinstance(value, (str, int, float, bool)):
            clean[key] = value
        elif isinstance(value, (dict, list)):
            try:
                clean[key] = json.dumps(value)
            except (TypeError, ValueError):
                clean[key] = str(value)
        elif value is not None:
            clean[key] = str(value)
    return clean


def index_knowledge_base_chroma() -> int:
    """Index all knowledge base JSON files into ChromaDB.

    Reads from the data/ directory (same as vector_search.py) and indexes
    text chunks into ChromaDB for complementary RAG search.

    Returns:
        Number of documents indexed, or 0 on failure.
    """
    if not _ensure_initialized():
        return 0

    data_dir = Path(__file__).resolve().parent / "data"
    if not data_dir.exists():
        logger.warning("chroma_rag: data directory not found: %s", data_dir)
        return 0

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

    documents: List[Dict[str, Any]] = []
    doc_id = 0

    for filename in kb_files:
        fpath = data_dir / filename
        if not fpath.exists():
            continue

        try:
            with open(fpath, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("chroma_rag: failed to load %s: %s", filename, exc)
            continue

        chunks = _extract_text_chunks(raw, source=filename)
        for chunk_text in chunks:
            if len(chunk_text.strip()) < 20:
                continue
            doc_id += 1
            documents.append(
                {
                    "id": f"kb:{filename}:{doc_id}",
                    "text": chunk_text[:_MAX_DOCUMENT_LENGTH],
                    "metadata": {"source": filename, "chunk_id": doc_id},
                }
            )

    if not documents:
        logger.warning("chroma_rag: no documents extracted from knowledge base")
        return 0

    logger.info(
        "chroma_rag: extracted %d chunks from KB files for indexing",
        len(documents),
    )
    return bulk_index(documents)


def _extract_text_chunks(data: Any, source: str = "", prefix: str = "") -> List[str]:
    """Recursively extract text chunks from nested JSON structures.

    Mirrors the logic in vector_search.py for consistency.

    Args:
        data: JSON data (dict, list, or scalar).
        source: Source filename for context.
        prefix: Key path prefix for context.

    Returns:
        List of text strings suitable for embedding.
    """
    chunks: List[str] = []

    if isinstance(data, dict):
        text_parts: List[str] = []
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
