"""rag_backfill.py -- Phase 2 backfill for Nova RAG v2.

Indexes the curated Nova knowledge-base JSON files into the Qdrant
``nova_knowledge_v2`` collection (defined in ``rag_pipeline.py``) so that
``_query_kb_semantic`` returns real retrieval results once
``RAG_V2_ENABLED`` is flipped on in production.

This is a one-shot CLI runner. It is **idempotent**: ``Document.doc_id``
is a UUID5 of ``(source_file, key_path, chunk_index)`` so re-running this
script overwrites existing points instead of duplicating them. That makes
it safe to re-run after KB file edits.

Phase per ``docs/RAG_Design_2026.md`` §6.1:
    - Index `data/*.json` curated files (~12 default + extras).
    - Embed via Voyage ``voyage-3.5-lite`` (1024-dim, $0.02/M tokens).
    - Fall back to ``sentence-transformers/all-MiniLM-L6-v2`` if Voyage
      is unavailable.
    - Upsert into Qdrant Cloud (``QDRANT_URL`` + ``QDRANT_API_KEY`` env).
    - Report total chunks, total documents, time elapsed, cost estimate.

Usage::

    # Dry run (no Qdrant writes; uses in-memory store)
    python3 scripts/rag_backfill.py --dry-run

    # Real backfill (writes to Qdrant; respects existing point IDs)
    python3 scripts/rag_backfill.py

    # Override the data directory or file list
    python3 scripts/rag_backfill.py --data-dir ./data \\
        --files recruitment_industry_knowledge.json,joveo_publishers.json

Cost guardrail:
    The script aborts if the estimated embedding cost exceeds ``--max-cost``
    (default $2.00). Per the design doc, the full backfill is ~$0.13.

Environment:
    VOYAGE_API_KEY     Optional. Without it we fall back to
                       sentence-transformers (must be `pip install`-ed).
    QDRANT_URL,        Required for real backfill (omitted in --dry-run).
    QDRANT_API_KEY
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

# Ensure the project root is importable when run as `python3 scripts/...`.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from rag_pipeline import (  # noqa: E402
    BM25Index,
    InMemoryStore,
    NovaRAGPipeline,
    QdrantStore,
    VoyageEmbedder,
    _LOCAL_EMBED_DIM,
    _VOYAGE_EMBED_DIM,
    _make_embedder,
    build_documents_from_kb,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("rag_backfill")


# Default file set: the 12 curated files from ``rag_pipeline._KB_FILES``
# plus the additional KBs Nova actively cites in chat responses. Mirrors
# (a subset of) ``kb_loader.KB_FILES`` so production retrieval has the
# same surface as the existing tool-handler reads.
#
# IMPORTANT: ``ta_leaders_curated_2026.json`` is included because the
# Phase 2 smoke test ("tell me about talent acquisition trends from
# industry experts") expects Hung Lee / Madeline Mann citations sourced
# from this file. Without it the smoke test cannot pass.
#
# Files NOT included intentionally (per design doc §2.2):
#   * ephemeral API caches (``api_cache/``, ``cache/``)
#   * embedding cache (``.embedding_cache.json``)
#   * large operational data (``slotops_baseline_data.json``)
#   * compressed archives, backups, logs, errors, analytics
DEFAULT_BACKFILL_FILES: tuple[str, ...] = (
    # From rag_pipeline._KB_FILES (12 baseline files):
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
    # Additional curated files (required for smoke test + Nova chat surface):
    "ta_leaders_curated_2026.json",  # REQUIRED for smoke test (TA experts)
    "joveo_media_plan_deck_2026.json",  # REQUIRED: deck KB for semantic search (Nova-F2)
    "industry_reports_2026.json",
    "recruitment_benchmarks_deep.json",
    "recruitment_strategy_intelligence.json",
    "workforce_trends_intelligence.json",
    "industry_white_papers.json",
    "regional_hiring_intelligence.json",
    "hr_tech_landscape_2026.json",
    "recruitment_marketing_trends_2026.json",
    "labor_market_outlook_2026.json",
    "industry_hiring_patterns_2026.json",
    "compliance_regulations_2026.json",
    "agency_rpo_market_2026.json",
    "recruitment_benchmarks_2026_deep.json",
    "employer_career_intelligence_2026.json",
    "healthcare_specialty_pay_2026.json",
    "external_benchmarks_2025.json",
    "publisher_benchmarks_2026.json",
    "top_employers_by_city_2026.json",
    "h1b_salary_intelligence.json",
    "linkedin_performance_benchmarks.json",
    "craigslist_performance_benchmarks.json",
    "healthcare_supply_map_us.json",
    "partner_specialty_crosswalk.json",
    "category_to_partners.json",
    "seasonal_hiring_trends.json",
)


def _estimate_cost(total_tokens: int, model: str) -> float:
    """Return a USD cost estimate for embedding ``total_tokens``.

    Args:
        total_tokens: Sum of approximate tokens across all chunks.
        model: Embedding model name (``voyage-3.5-lite`` or local).

    Returns:
        Cost in USD. ``0.0`` for local sentence-transformers.
    """
    # Voyage AI pricing (May 2026): voyage-3.5-lite = $0.02 per million tokens.
    # Sources: https://www.voyageai.com/pricing
    if model.startswith("voyage"):
        return total_tokens / 1_000_000 * 0.02
    # sentence-transformers runs locally on CPU -- no API cost.
    return 0.0


def _approx_tokens_total(docs: list) -> int:
    """Sum approximate tokens across all chunks. 1 token ~= 4 chars (English)."""
    return sum(len(d.text) // 4 for d in docs)


def run_backfill(
    data_dir: Path,
    files: tuple[str, ...],
    dry_run: bool = False,
    max_cost_usd: float = 2.0,
    batch_size: int = 64,
    prefer_embedder: str | None = None,
) -> dict:
    """Execute the backfill: build docs, embed, upsert to Qdrant.

    Args:
        data_dir: Path to the ``data/`` directory containing source JSON.
        files: Filenames (within data_dir) to index.
        dry_run: If True, use the in-memory store instead of Qdrant.
        max_cost_usd: Hard cost ceiling; the script aborts if the
            pre-flight estimate exceeds this value. Default $2.00.
        batch_size: Documents per embedding API call. Voyage allows
            up to 128/call; we use 64 to keep failure blast radius low.
        prefer_embedder: Force a specific embedder ("voyage" | "local").
            Default: auto-select (Voyage first, then local).

    Returns:
        Summary dict::

            {
              "files_attempted": int,
              "files_loaded": int,
              "documents_built": int,
              "documents_indexed": int,
              "tokens_estimated": int,
              "cost_usd_estimate": float,
              "elapsed_seconds": float,
              "embedder": str,
              "store": str,
              "skipped_files": list[str],
              "dry_run": bool,
            }
    """
    start = time.monotonic()
    files = tuple(files)

    # Step 1: load and chunk every file.
    logger.info("Building documents from %d KB files in %s ...", len(files), data_dir)
    docs = build_documents_from_kb(data_dir, files)
    skipped = [f for f in files if not (data_dir / f).exists()]
    loaded = len(files) - len(skipped)

    if not docs:
        logger.error(
            "No documents built. Check --data-dir and --files. " "Skipped files: %s",
            skipped,
        )
        return {
            "files_attempted": len(files),
            "files_loaded": loaded,
            "documents_built": 0,
            "documents_indexed": 0,
            "tokens_estimated": 0,
            "cost_usd_estimate": 0.0,
            "elapsed_seconds": time.monotonic() - start,
            "embedder": "n/a",
            "store": "n/a",
            "skipped_files": skipped,
            "dry_run": dry_run,
        }

    # Step 2: pre-flight cost estimate. Abort if over budget.
    tokens_est = _approx_tokens_total(docs)
    embedder = _make_embedder(prefer=prefer_embedder)
    embedder_name = embedder.name
    cost_est = _estimate_cost(tokens_est, embedder_name)
    logger.info(
        "Built %d chunks from %d files (skipped %d). Est tokens: %s. "
        "Embedder: %s. Cost est: $%.4f.",
        len(docs),
        loaded,
        len(skipped),
        f"{tokens_est:,}",
        embedder_name,
        cost_est,
    )
    if cost_est > max_cost_usd:
        logger.error(
            "Estimated cost $%.2f exceeds --max-cost $%.2f. Aborting.",
            cost_est,
            max_cost_usd,
        )
        raise SystemExit(2)

    # Step 3: select the vector store.
    if dry_run:
        store = InMemoryStore()
        store_name = "InMemoryStore (dry-run)"
    else:
        if not (os.environ.get("QDRANT_URL") and os.environ.get("QDRANT_API_KEY")):
            logger.error(
                "QDRANT_URL/QDRANT_API_KEY not set. Use --dry-run or set them."
            )
            raise SystemExit(3)
        store = QdrantStore(dim=embedder.dim)
        store_name = f"QdrantStore (dim={embedder.dim})"
    logger.info("Vector store: %s", store_name)

    # Step 4: instantiate the pipeline with our chosen backends and index.
    # Disable rerank during indexing (only used on retrieve).
    pipeline = NovaRAGPipeline(
        embedder=embedder,
        store=store,
        bm25=BM25Index(),
        rerank_enabled=False,
    )

    # NovaRAGPipeline.index() already batches in chunks of 64 internally
    # (see ``embed_documents``), so we just hand the full doc list off and
    # let it stream them in. We chunk again here only so we can log
    # progress and recover gracefully from a mid-run failure.
    indexed = 0
    failures = 0
    for i in range(0, len(docs), batch_size):
        batch = docs[i : i + batch_size]
        try:
            result = pipeline.index(batch)
            indexed += result["indexed"]
            logger.info(
                "Batch %d/%d: indexed %d docs (total %d/%d).",
                i // batch_size + 1,
                (len(docs) + batch_size - 1) // batch_size,
                result["indexed"],
                indexed,
                len(docs),
            )
        except Exception as exc:  # noqa: BLE001 -- best effort; report at end
            failures += 1
            logger.error(
                "Batch %d failed: %s. Continuing with next batch.",
                i // batch_size + 1,
                exc,
                exc_info=True,
            )

    elapsed = time.monotonic() - start
    summary = {
        "files_attempted": len(files),
        "files_loaded": loaded,
        "documents_built": len(docs),
        "documents_indexed": indexed,
        "tokens_estimated": tokens_est,
        "cost_usd_estimate": round(cost_est, 4),
        "elapsed_seconds": round(elapsed, 2),
        "embedder": embedder_name,
        "store": store_name,
        "skipped_files": skipped,
        "failed_batches": failures,
        "dry_run": dry_run,
        "qdrant_collection": "nova_knowledge_v2" if not dry_run else None,
    }
    return summary


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill embeddings into Qdrant for Nova RAG v2."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=_PROJECT_ROOT / "data",
        help="Directory containing the source JSON files (default: ./data).",
    )
    parser.add_argument(
        "--files",
        type=str,
        default=None,
        help=(
            "Comma-separated list of JSON filenames to index. "
            "Default: the curated DEFAULT_BACKFILL_FILES tuple."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Qdrant writes; use in-memory store for validation only.",
    )
    parser.add_argument(
        "--max-cost",
        type=float,
        default=2.0,
        help="Abort if estimated embedding cost exceeds this USD value.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        help="Documents per embedding API call (default 64).",
    )
    parser.add_argument(
        "--embedder",
        type=str,
        choices=("voyage", "local"),
        default=None,
        help="Force a specific embedder. Default: auto-select.",
    )
    args = parser.parse_args()

    if args.files:
        files = tuple(f.strip() for f in args.files.split(",") if f.strip())
    else:
        files = DEFAULT_BACKFILL_FILES

    if not args.data_dir.exists():
        logger.error("Data directory does not exist: %s", args.data_dir)
        return 1

    try:
        summary = run_backfill(
            data_dir=args.data_dir,
            files=files,
            dry_run=args.dry_run,
            max_cost_usd=args.max_cost,
            batch_size=args.batch_size,
            prefer_embedder=args.embedder,
        )
    except SystemExit:
        raise
    except Exception as exc:
        logger.error("Backfill failed: %s", exc, exc_info=True)
        return 1

    # Final report -- structured + human-readable.
    print("\n" + "=" * 72)
    print("RAG Backfill Summary")
    print("=" * 72)
    for k, v in summary.items():
        print(f"  {k:24s}: {v}")
    print("=" * 72)
    if summary["documents_indexed"] == 0:
        logger.warning("No documents were indexed. Check logs above.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
