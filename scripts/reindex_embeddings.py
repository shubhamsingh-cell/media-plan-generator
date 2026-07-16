#!/usr/bin/env python3
"""Re-embed the Nova knowledge base into Qdrant with the selected provider.

Layer-3 #11: this script (re)builds the Qdrant ``nova_knowledge`` collection
using whichever embedding provider EMBEDDING_PROVIDER selects:

    EMBEDDING_PROVIDER=voyage  (default)  -> voyage-3-lite, 512-dim
    EMBEDDING_PROVIDER=gemini             -> text-embedding-004, 768-dim

Because Voyage and Gemini live in different embedding spaces *and* produce
vectors of different dimension, switching provider requires recreating the
Qdrant collection at the matching dimension and re-embedding every chunk --
you cannot mix vectors from the two providers in one collection. This script
does that end to end:

    1. Extract KB text chunks (reuses vector_search.index_knowledge_base's
       file list + chunker so the corpus matches what the app indexes).
    2. (Re)create the Qdrant collection at the active provider's dimension.
       By default the collection is recreated (DELETE + PUT) so a leftover
       collection at the wrong dimension can't reject upserts; pass
       --no-recreate to keep an existing same-dim collection in place.
    3. Embed chunks in batches via vector_search.embed_batch (which already
       routes to the active provider, caches to disk, and rate-limits Voyage).
    4. Upsert vectors + payload into Qdrant with deterministic point IDs, so
       re-running the script is idempotent (same chunk -> same point ID).

Idempotency: deterministic point IDs (md5 of doc_id) mean a re-run overwrites
points in place rather than duplicating them. With --no-recreate the script is
fully incremental.

Env vars required at RUN TIME:
    QDRANT_URL        -- Qdrant Cloud cluster URL          (WRITE access needed)
    QDRANT_API_KEY    -- Qdrant Cloud API key              (WRITE access needed)
    EMBEDDING_PROVIDER-- "voyage" (default) or "gemini"
    VOYAGE_API_KEY    -- required when provider == voyage
    GEMINI_API_KEY    -- required when provider == gemini

Usage:
    EMBEDDING_PROVIDER=gemini python3 scripts/reindex_embeddings.py
    python3 scripts/reindex_embeddings.py --no-recreate
    python3 scripts/reindex_embeddings.py --batch-size 64 --limit 500
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
import time
from pathlib import Path

# Make the project root importable so we can reuse vector_search's provider
# switch, embedding functions, Qdrant helpers, and KB chunker -- no duplicated
# logic, so the reindexed corpus always matches what the live app indexes.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import vector_search as vs  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("reindex_embeddings")


def _deterministic_point_id(doc_id: str) -> int:
    """Stable int64 Qdrant point ID derived from a document id.

    Using a content-addressed ID (not a random/sequential one) makes re-runs
    idempotent: the same chunk always maps to the same point, so an upsert
    overwrites in place instead of creating duplicates.
    """
    digest = hashlib.md5(doc_id.encode("utf-8")).hexdigest()
    return int(digest[:15], 16)  # 60 bits -- safely within int64


def _collect_documents(limit: int | None = None) -> list[dict]:
    """Extract KB chunks using vector_search's own file list + chunker.

    Returns the same {"id", "text", "metadata"} document dicts that
    index_knowledge_base() builds, so the reindexed corpus is identical to the
    app's. Reads the KB file list out of index_knowledge_base via a light
    re-implementation that calls the module's private extractor -- we mirror
    the public function's loop without triggering an in-process build_index().
    """
    data_dir = PROJECT_ROOT / "data"
    if not data_dir.exists():
        logger.error("Data directory not found: %s", data_dir)
        return []

    documents: list[dict] = []
    doc_id = 0
    files_seen = 0

    for filename in vs._KB_INDEX_FILES:  # shared canonical KB file list
        fpath = data_dir / filename
        if not fpath.exists():
            continue
        files_seen += 1
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Skipping %s (load error): %s", filename, exc)
            continue

        for chunk_text in vs._extract_text_chunks(raw, source=filename):
            if len(chunk_text.strip()) < 20:
                continue
            doc_id += 1
            documents.append(
                {
                    "id": f"{filename}:{doc_id}",
                    "text": chunk_text[:2000],
                    "metadata": {"source": filename, "chunk_id": doc_id},
                }
            )
            if limit is not None and len(documents) >= limit:
                logger.info("Reached --limit %d, stopping extraction", limit)
                return documents

    logger.info("Extracted %d chunks from %d KB files", len(documents), files_seen)
    return documents


def _recreate_collection(dim: int, recreate: bool) -> bool:
    """Ensure the Qdrant collection exists at ``dim`` (cosine).

    When ``recreate`` is True (default) the collection is dropped first so a
    leftover collection at the wrong dimension can't reject upserts. When
    False, an existing collection is reused as-is (incremental reindex).

    Returns True on success.
    """
    if not vs._qdrant_is_configured():
        logger.error(
            "QDRANT_URL / QDRANT_API_KEY not set -- cannot write to Qdrant. "
            "These are REQUIRED at run time."
        )
        return False

    collection = vs._QDRANT_COLLECTION

    if recreate:
        logger.info("Deleting collection '%s' (recreate mode)", collection)
        # DELETE is idempotent in Qdrant -- 200 even if it doesn't exist.
        vs._qdrant_request("DELETE", f"/collections/{collection}")
        result = vs._qdrant_request(
            "PUT",
            f"/collections/{collection}",
            body={"vectors": {"size": dim, "distance": "Cosine"}},
        )
        if result is None:
            logger.error("Failed to create collection '%s' at dim %d", collection, dim)
            return False
        logger.info("Created collection '%s' at dim %d (cosine)", collection, dim)
        # Mark available so vs._qdrant_upsert_points() will run.
        vs._qdrant_available = True
        return True

    # Incremental: reuse vector_search's idempotent ensure (creates at the
    # active provider's dim if missing, otherwise leaves it untouched).
    if not vs._qdrant_ensure_collection():
        logger.error("Failed to ensure collection '%s'", collection)
        return False
    logger.info("Reusing existing collection '%s' (no-recreate mode)", collection)
    return True


def _embed_and_upsert(documents: list[dict], batch_size: int) -> tuple[int, int]:
    """Embed documents in batches and upsert them into Qdrant.

    Returns (embedded_count, upserted_count). Batches that fail to embed are
    skipped (logged) so one bad batch doesn't abort the whole reindex.
    """
    total = len(documents)
    embedded = 0
    upserted = 0

    for start in range(0, total, batch_size):
        batch = documents[start : start + batch_size]
        texts = [d["text"] for d in batch]

        t0 = time.monotonic()
        vectors = vs.embed_batch(texts)
        elapsed = time.monotonic() - t0

        if not vectors or len(vectors) != len(batch):
            logger.warning(
                "Batch %d-%d: embedding failed/short (got %s), skipping",
                start,
                start + len(batch),
                None if not vectors else len(vectors),
            )
            continue

        embedded += len(vectors)
        points = [
            {
                "id": _deterministic_point_id(doc["id"]),
                "vector": vec,
                "payload": {
                    "doc_id": doc["id"],
                    "text": doc["text"][:2000],
                    "metadata": doc["metadata"],
                },
            }
            for doc, vec in zip(batch, vectors)
        ]

        if vs._qdrant_upsert_points(points):
            upserted += len(points)
        else:
            logger.warning(
                "Batch %d-%d: Qdrant upsert failed", start, start + len(batch)
            )

        logger.info(
            "Progress: %d/%d embedded (%.1f%%), %d upserted, last batch %.1fs",
            embedded,
            total,
            100.0 * embedded / total if total else 0.0,
            upserted,
            elapsed,
        )

    return embedded, upserted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Embedding/upsert batch size (default 32).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max chunks to (re)index -- for smoke tests.",
    )
    parser.add_argument(
        "--no-recreate",
        action="store_true",
        help="Do not drop the collection first (incremental reindex).",
    )
    args = parser.parse_args(argv)

    provider = vs.get_embedding_provider()
    model = vs.get_active_embedding_model()
    dim = vs._active_vector_dim()

    logger.info("=" * 64)
    logger.info("Reindex: provider=%s model=%s dim=%d", provider, model, dim)
    logger.info(
        "Collection=%s recreate=%s", vs._QDRANT_COLLECTION, not args.no_recreate
    )
    logger.info("=" * 64)

    # Fail fast on the provider's required embedding key.
    if provider == vs.EMBEDDING_PROVIDER_GEMINI and not vs._get_gemini_api_key():
        logger.error("EMBEDDING_PROVIDER=gemini but GEMINI_API_KEY is not set.")
        return 2
    if provider == vs.EMBEDDING_PROVIDER_VOYAGE and not vs._get_api_key():
        logger.error("EMBEDDING_PROVIDER=voyage but VOYAGE_API_KEY is not set.")
        return 2

    documents = _collect_documents(limit=args.limit)
    if not documents:
        logger.error("No documents extracted -- nothing to index.")
        return 1

    if not _recreate_collection(dim, recreate=not args.no_recreate):
        return 1

    embedded, upserted = _embed_and_upsert(documents, batch_size=args.batch_size)

    logger.info("=" * 64)
    logger.info(
        "Done: %d/%d embedded, %d upserted into '%s' (provider=%s, dim=%d)",
        embedded,
        len(documents),
        upserted,
        vs._QDRANT_COLLECTION,
        provider,
        dim,
    )
    logger.info("=" * 64)

    return 0 if upserted > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
