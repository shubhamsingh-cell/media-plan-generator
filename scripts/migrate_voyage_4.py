"""
Migrate Qdrant vector index from voyage-3-lite to voyage-4-{lite,large} (S50 -- May 2026).

USAGE:
  # Step 1: dry run + backup (safe, no changes)
  python scripts/migrate_voyage_4.py --dry-run --backup

  # Step 2: execute (will backup automatically before changes)
  python scripts/migrate_voyage_4.py --execute

  # Rollback if needed
  python scripts/migrate_voyage_4.py --rollback --rollback-from data/qdrant_backup_pre_voyage4_2026-05-02T10-00-00.json

TIME ESTIMATE: 685 points x 6.5s/batch / 32 per batch ~= 2.5 minutes (free-tier rate-limited).
COST ESTIMATE: ~3.4M tokens x $0.02/M (voyage-4-lite) = ~$0.07.

DESIGN NOTES:
  - Idempotent: re-running re-embeds + upserts, safe.
  - Voyage 4 uses Matryoshka truncation via output_dimension; we keep 512-dim
    to avoid Qdrant collection schema changes (cosine, 512-dim).
  - Voyage 4 lives in a different embedding space than Voyage 3, so all 685
    points must be re-embedded -- partial migration is invalid.
  - --dry-run is the DEFAULT for safety. Use --execute to actually mutate.
  - --execute always forces a backup first (cannot be skipped).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("migrate_voyage_4")

# ── Constants ────────────────────────────────────────────────────────────────

QDRANT_COLLECTION = "nova_knowledge"
VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"
VOYAGE_MAX_BATCH = 32  # Voyage API max documents per request
VOYAGE_RATE_DELAY_SEC = 6.5  # 10 RPM free tier => 6s between requests + 0.5s safety
VOYAGE_TIMEOUT_SEC = 60
QDRANT_TIMEOUT_SEC = 30
QDRANT_SCROLL_PAGE_SIZE = 200
HTTP_OK = 200

# Voyage 4 family pricing (USD per 1M tokens) -- as of 2026-05
VOYAGE_4_PRICING = {
    "voyage-4-lite": 0.02,
    "voyage-4-large": 0.12,
}

# Voyage 4 output_dimension valid values via Matryoshka truncation
VOYAGE_4_VALID_DIMS = {256, 512, 1024, 2048}

# Sample queries for recall@5 sanity check after migration
SANITY_CHECK_QUERIES = [
    "recruitment marketing benchmarks 2026",
    "Indeed cost per click programmatic",
    "LinkedIn applicant rate Easy Apply versus ATS",
]

DEFAULT_BASELINE_PATH = "data/recall_baseline_voyage3.json"
BACKUP_DIR = "data"
BACKUP_PREFIX = "qdrant_backup_pre_voyage4"

_SSL_CTX = ssl.create_default_context()


# ── Environment validation ───────────────────────────────────────────────────


def _validate_env() -> tuple[str, str, str]:
    """Validate required environment variables and return (voyage_key, qdrant_url, qdrant_key).

    Exits with code 1 and clear instructions if any are missing.
    """
    voyage_key = os.environ.get("VOYAGE_API_KEY") or ""
    qdrant_url = os.environ.get("QDRANT_URL") or ""
    qdrant_key = os.environ.get("QDRANT_API_KEY") or ""

    missing: list[str] = []
    if not voyage_key:
        missing.append("VOYAGE_API_KEY")
    if not qdrant_url:
        missing.append("QDRANT_URL")
    if not qdrant_key:
        missing.append("QDRANT_API_KEY")

    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        logger.error("")
        logger.error("Set them via:")
        logger.error("  export VOYAGE_API_KEY=<get from https://www.voyageai.com>")
        logger.error("  export QDRANT_URL=<your Qdrant cluster URL>")
        logger.error("  export QDRANT_API_KEY=<your Qdrant API key>")
        logger.error("")
        logger.error("Or source ~/.zshrc if they are configured locally.")
        sys.exit(1)

    return voyage_key, qdrant_url.rstrip("/"), qdrant_key


# ── Qdrant REST helpers (stdlib only) ────────────────────────────────────────


def _qdrant_request(
    method: str,
    qdrant_url: str,
    qdrant_key: str,
    path: str,
    body: dict | None = None,
    timeout: int = QDRANT_TIMEOUT_SEC,
) -> dict | None:
    """Send an HTTP request to Qdrant and return parsed JSON, or None on failure."""
    if not path.startswith("/"):
        path = f"/{path}"
    url = f"{qdrant_url}{path}"

    headers = {
        "api-key": qdrant_key,
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode("utf-8") if body is not None else None

    try:
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
            resp_body = resp.read().decode("utf-8")
            if resp_body:
                return json.loads(resp_body)
            return {"status": "ok"}
    except urllib.error.HTTPError as exc:
        resp_text = ""
        try:
            resp_text = exc.read().decode("utf-8")[:500]
        except (UnicodeDecodeError, OSError):
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


def _qdrant_collection_info(qdrant_url: str, qdrant_key: str) -> dict | None:
    """Fetch Qdrant collection info; return None if it does not exist or call failed."""
    return _qdrant_request(
        "GET", qdrant_url, qdrant_key, f"/collections/{QDRANT_COLLECTION}"
    )


def _qdrant_scroll_all(qdrant_url: str, qdrant_key: str) -> list[dict]:
    """Scroll the entire nova_knowledge collection. Returns list of raw point dicts.

    Each point dict has: {"id": ..., "payload": {...}, "vector": [...]}.
    """
    all_points: list[dict] = []
    next_offset: str | int | None = None
    page = 0

    while True:
        body: dict = {
            "limit": QDRANT_SCROLL_PAGE_SIZE,
            "with_payload": True,
            "with_vector": True,
        }
        if next_offset is not None:
            body["offset"] = next_offset

        result = _qdrant_request(
            "POST",
            qdrant_url,
            qdrant_key,
            f"/collections/{QDRANT_COLLECTION}/points/scroll",
            body=body,
        )
        if result is None:
            logger.error("Qdrant scroll failed at page %d", page)
            break

        payload = result.get("result") or {}
        page_points = payload.get("points") or []
        all_points.extend(page_points)

        next_offset = payload.get("next_page_offset")
        page += 1
        logger.info(
            "Scrolled page %d (%d points; total=%d)",
            page,
            len(page_points),
            len(all_points),
        )

        if next_offset is None or not page_points:
            break

    return all_points


def _qdrant_upsert_batch(
    qdrant_url: str,
    qdrant_key: str,
    batch: list[dict],
    dry_run: bool,
) -> bool:
    """Upsert a batch of {id, vector, payload} dicts to Qdrant."""
    if dry_run:
        logger.info("[DRY RUN] Would upsert %d points to Qdrant", len(batch))
        return True

    result = _qdrant_request(
        "PUT",
        qdrant_url,
        qdrant_key,
        f"/collections/{QDRANT_COLLECTION}/points",
        body={"points": batch},
        timeout=60,
    )
    return result is not None


# ── Voyage AI embedding helpers ──────────────────────────────────────────────


def _voyage_embed(
    texts: list[str],
    model: str,
    output_dim: int,
    voyage_key: str,
    input_type: str = "document",
) -> list[list[float]]:
    """Call Voyage AI embeddings API for a batch of texts.

    Returns embeddings in the same order as input. Raises RuntimeError on failure.
    """
    if not texts:
        return []

    payload = {
        "input": texts,
        "model": model,
        "output_dimension": output_dim,
        "input_type": input_type,
    }
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        VOYAGE_API_URL,
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {voyage_key}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(
            req, timeout=VOYAGE_TIMEOUT_SEC, context=_SSL_CTX
        ) as resp:
            body = resp.read().decode("utf-8")
            api_result = json.loads(body)
    except urllib.error.HTTPError as exc:
        resp_text = ""
        try:
            resp_text = exc.read().decode("utf-8")[:500]
        except (UnicodeDecodeError, OSError):
            pass
        raise RuntimeError(
            f"Voyage API HTTPError {exc.code} {exc.reason}: {resp_text}"
        ) from exc
    except (urllib.error.URLError, OSError, json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError(f"Voyage API call failed: {exc}") from exc

    embeddings_data = api_result.get("data") or []
    embeddings_data.sort(key=lambda x: x.get("index", 0))
    embeddings = [e.get("embedding") or [] for e in embeddings_data]

    if len(embeddings) != len(texts):
        raise RuntimeError(
            f"Voyage returned {len(embeddings)} embeddings for {len(texts)} inputs"
        )
    return embeddings


# ── Backup / restore ─────────────────────────────────────────────────────────


def _backup_path(timestamp: str) -> Path:
    """Return absolute path for a backup file given an ISO-ish timestamp."""
    project_root = Path(__file__).resolve().parent.parent
    backup_dir = project_root / BACKUP_DIR
    backup_dir.mkdir(parents=True, exist_ok=True)
    return backup_dir / f"{BACKUP_PREFIX}_{timestamp}.json"


def _write_backup(points: list[dict], path: Path) -> None:
    """Write points list to a JSON backup file with metadata."""
    backup_blob = {
        "version": 1,
        "collection": QDRANT_COLLECTION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "point_count": len(points),
        "source_model": "voyage-3-lite",
        "points": points,
    }
    with path.open("w", encoding="utf-8") as fh:
        json.dump(backup_blob, fh, ensure_ascii=False)
    logger.info("Backup written: %s (%d points)", path, len(points))


def _read_backup(path: Path) -> list[dict]:
    """Load backup file and return points list."""
    if not path.is_file():
        raise FileNotFoundError(f"Backup file not found: {path}")
    with path.open("r", encoding="utf-8") as fh:
        blob = json.load(fh)
    points = blob.get("points") or []
    if not isinstance(points, list):
        raise ValueError(f"Backup at {path} has invalid 'points' field")
    logger.info(
        "Loaded backup: %s (%d points, source=%s)",
        path,
        len(points),
        blob.get("source_model"),
    )
    return points


# ── Recall@5 sanity check ────────────────────────────────────────────────────


def _recall_sanity_check(
    qdrant_url: str,
    qdrant_key: str,
    voyage_key: str,
    target_model: str,
    output_dim: int,
    baseline_path: Path,
) -> dict:
    """Embed sanity-check queries with the new model, search Qdrant, return top-5 results.

    Compares against baseline file (creates one if missing).
    """
    logger.info(
        "Running recall@5 sanity check with %d queries...", len(SANITY_CHECK_QUERIES)
    )
    query_embeddings = _voyage_embed(
        SANITY_CHECK_QUERIES, target_model, output_dim, voyage_key, input_type="query"
    )

    new_results: dict[str, list[dict]] = {}
    for query, qvec in zip(SANITY_CHECK_QUERIES, query_embeddings):
        result = _qdrant_request(
            "POST",
            qdrant_url,
            qdrant_key,
            f"/collections/{QDRANT_COLLECTION}/points/search",
            body={"vector": qvec, "limit": 5, "with_payload": True},
        )
        if result is None or "result" not in result:
            new_results[query] = []
            continue
        hits = []
        for hit in result["result"]:
            payload = hit.get("payload") or {}
            hits.append(
                {
                    "id": hit.get("id"),
                    "score": round(float(hit.get("score") or 0.0), 4),
                    "text_preview": (
                        payload.get("text") or payload.get("content") or ""
                    )[:80],
                }
            )
        new_results[query] = hits

    logger.info("Top-5 results per query (new model):")
    for query, hits in new_results.items():
        logger.info("  Q: %s", query)
        for i, hit in enumerate(hits, 1):
            logger.info(
                "    %d. id=%s score=%.4f preview=%s",
                i,
                hit["id"],
                hit["score"],
                hit["text_preview"],
            )

    # Baseline comparison
    if baseline_path.is_file():
        try:
            with baseline_path.open("r", encoding="utf-8") as fh:
                baseline = json.load(fh)
            logger.info("Comparing against baseline %s ...", baseline_path)
            for query in SANITY_CHECK_QUERIES:
                baseline_ids = {h.get("id") for h in (baseline.get(query) or [])}
                new_ids = {h["id"] for h in new_results.get(query, [])}
                overlap = len(baseline_ids & new_ids)
                logger.info(
                    "  Q: %s -- recall@5 overlap with baseline: %d/5",
                    query,
                    overlap,
                )
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read baseline %s: %s", baseline_path, exc)
    else:
        logger.info(
            "Baseline file %s not found; writing current results as new baseline.",
            baseline_path,
        )
        baseline_path.parent.mkdir(parents=True, exist_ok=True)
        with baseline_path.open("w", encoding="utf-8") as fh:
            json.dump(new_results, fh, ensure_ascii=False, indent=2)

    return new_results


# ── Migration workflow ───────────────────────────────────────────────────────


def _extract_text(payload: dict) -> str:
    """Pull the embeddable text from a point's payload. Tries 'text' then 'content'."""
    text = payload.get("text") or payload.get("content") or ""
    if not isinstance(text, str):
        text = str(text)
    return text


def _migrate(
    target_model: str,
    output_dim: int,
    do_backup: bool,
    execute: bool,
    voyage_key: str,
    qdrant_url: str,
    qdrant_key: str,
    baseline_path: Path,
) -> int:
    """Execute the migration workflow. Returns process exit code."""
    dry_run = not execute
    mode = "EXECUTE" if execute else "DRY RUN"
    logger.info("=" * 70)
    logger.info("Voyage 4 Migration -- mode=%s", mode)
    logger.info("  target_model=%s  output_dim=%d", target_model, output_dim)
    logger.info("=" * 70)

    # Step 3: Connect + verify collection
    info = _qdrant_collection_info(qdrant_url, qdrant_key)
    if info is None or "result" not in info:
        logger.error(
            "Could not fetch Qdrant collection '%s'. Check QDRANT_URL/QDRANT_API_KEY "
            "and that the collection exists.",
            QDRANT_COLLECTION,
        )
        return 1

    coll = info["result"]
    point_count = coll.get("points_count") or coll.get("vectors_count") or 0
    config = coll.get("config") or {}
    vec_cfg = ((config.get("params") or {}).get("vectors")) or {}
    current_dim = vec_cfg.get("size") if isinstance(vec_cfg, dict) else None
    distance = vec_cfg.get("distance") if isinstance(vec_cfg, dict) else None
    logger.info(
        "Qdrant collection '%s': points=%d  dim=%s  distance=%s",
        QDRANT_COLLECTION,
        point_count,
        current_dim,
        distance,
    )

    if current_dim is not None and current_dim != output_dim:
        logger.error(
            "Collection vector dim is %s but --output-dim is %d. They must match. "
            "Either change --output-dim or recreate the Qdrant collection.",
            current_dim,
            output_dim,
        )
        return 1

    # Step 4: Backup (always backup on --execute, optional on --dry-run)
    backup_required = do_backup or execute
    points: list[dict] = []
    if backup_required:
        logger.info("Scrolling all points for backup...")
        points = _qdrant_scroll_all(qdrant_url, qdrant_key)
        if not points:
            logger.error("Scroll returned 0 points. Aborting (nothing to migrate).")
            return 1
        timestamp = (
            datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace(":", "-")
            .replace("+00-00", "Z")
        )
        backup_file = _backup_path(timestamp)
        if dry_run:
            logger.info(
                "[DRY RUN] Would write backup to %s with %d points",
                backup_file,
                len(points),
            )
            # Still write the backup on dry-run when --backup is explicitly requested,
            # so users can inspect contents before --execute.
            if do_backup:
                _write_backup(points, backup_file)
        else:
            _write_backup(points, backup_file)
    else:
        logger.info("Scrolling all points (no backup requested)...")
        points = _qdrant_scroll_all(qdrant_url, qdrant_key)
        if not points:
            logger.error("Scroll returned 0 points. Aborting (nothing to migrate).")
            return 1

    # Step 5: Sample test -- embed 5 samples and verify dimension
    logger.info("Embedding 5 sample texts to verify model + output_dim...")
    sample_texts = [
        _extract_text(p.get("payload") or {}) or "sample text"
        for p in points[: min(5, len(points))]
    ]
    try:
        sample_embeddings = _voyage_embed(
            sample_texts, target_model, output_dim, voyage_key, input_type="document"
        )
    except RuntimeError as exc:
        logger.error("Sample embedding failed: %s", exc)
        return 1

    if not sample_embeddings or len(sample_embeddings[0]) != output_dim:
        actual = len(sample_embeddings[0]) if sample_embeddings else 0
        logger.error(
            "Sample embedding dim mismatch: expected %d, got %d. Aborting.",
            output_dim,
            actual,
        )
        return 1
    logger.info(
        "Sample test OK: returned %d embeddings of dim %d.",
        len(sample_embeddings),
        len(sample_embeddings[0]),
    )

    # Step 6 + 7: Re-embed and upsert in batches
    start_time = time.time()
    total_tokens_estimate = 0
    migrated = 0
    failed_batches = 0

    total_batches = (len(points) + VOYAGE_MAX_BATCH - 1) // VOYAGE_MAX_BATCH
    logger.info(
        "Re-embedding %d points in %d batches (size=%d, ~%.1fs/batch rate-limited)...",
        len(points),
        total_batches,
        VOYAGE_MAX_BATCH,
        VOYAGE_RATE_DELAY_SEC,
    )

    last_request_time = 0.0
    for batch_idx in range(total_batches):
        batch_points = points[
            batch_idx * VOYAGE_MAX_BATCH : (batch_idx + 1) * VOYAGE_MAX_BATCH
        ]
        batch_texts: list[str] = []
        valid_batch_points: list[dict] = []
        for p in batch_points:
            text = _extract_text(p.get("payload") or {})
            if not text:
                logger.warning(
                    "Point id=%s has no payload.text or payload.content; skipping",
                    p.get("id"),
                )
                continue
            batch_texts.append(text)
            valid_batch_points.append(p)

        if not batch_texts:
            continue

        # Rate limiting between batches
        elapsed = time.time() - last_request_time
        if last_request_time > 0 and elapsed < VOYAGE_RATE_DELAY_SEC:
            wait = VOYAGE_RATE_DELAY_SEC - elapsed
            logger.debug("Rate-limit sleep %.2fs", wait)
            time.sleep(wait)

        if dry_run:
            logger.info(
                "[DRY RUN] Batch %d/%d: would embed %d texts and upsert with same IDs",
                batch_idx + 1,
                total_batches,
                len(batch_texts),
            )
            last_request_time = time.time()
            migrated += len(valid_batch_points)
            total_tokens_estimate += sum(len(t.split()) for t in batch_texts) * 4 // 3
            continue

        try:
            new_vectors = _voyage_embed(
                batch_texts, target_model, output_dim, voyage_key, input_type="document"
            )
        except RuntimeError as exc:
            logger.error("Batch %d embed failed: %s", batch_idx + 1, exc)
            failed_batches += 1
            last_request_time = time.time()
            continue

        last_request_time = time.time()
        total_tokens_estimate += sum(len(t.split()) for t in batch_texts) * 4 // 3

        upsert_batch = []
        for p, vec in zip(valid_batch_points, new_vectors):
            upsert_batch.append(
                {
                    "id": p.get("id"),
                    "vector": vec,
                    "payload": p.get("payload") or {},
                }
            )

        ok = _qdrant_upsert_batch(qdrant_url, qdrant_key, upsert_batch, dry_run=False)
        if ok:
            migrated += len(upsert_batch)
            logger.info(
                "Batch %d/%d: upserted %d points (running total=%d)",
                batch_idx + 1,
                total_batches,
                len(upsert_batch),
                migrated,
            )
        else:
            failed_batches += 1
            logger.error("Batch %d upsert failed", batch_idx + 1)

    elapsed_total = time.time() - start_time

    # Step 8: Verify -- recall@5 sanity check (skipped on dry-run)
    if dry_run:
        logger.info("[DRY RUN] Would run recall@5 sanity check after migration.")
    else:
        try:
            _recall_sanity_check(
                qdrant_url,
                qdrant_key,
                voyage_key,
                target_model,
                output_dim,
                baseline_path,
            )
        except RuntimeError as exc:
            logger.error("Recall sanity check failed: %s", exc)

    # Step 9: Summary
    cost_per_m = VOYAGE_4_PRICING.get(target_model, 0.02)
    estimated_cost_usd = (total_tokens_estimate / 1_000_000.0) * cost_per_m
    logger.info("=" * 70)
    logger.info("MIGRATION SUMMARY (%s)", mode)
    logger.info("  Target model:        %s", target_model)
    logger.info("  Output dim:          %d", output_dim)
    logger.info("  Points migrated:     %d / %d", migrated, len(points))
    logger.info("  Failed batches:      %d", failed_batches)
    logger.info(
        "  Elapsed:             %.1fs (%.1f min)", elapsed_total, elapsed_total / 60.0
    )
    logger.info("  Tokens (est):        ~%d", total_tokens_estimate)
    logger.info(
        "  Cost (est):          $%.4f USD ($%.4f/M tokens)",
        estimated_cost_usd,
        cost_per_m,
    )
    logger.info("=" * 70)

    if dry_run:
        logger.info("Next steps:")
        logger.info("  1. Inspect any backup file under data/.")
        logger.info("  2. When ready, re-run with --execute to perform the migration.")
        logger.info("  3. After --execute, update vector_search.py:")
        logger.info('       _VOYAGE_MODEL = "%s"', target_model)
        logger.info(
            "       (output_dimension=%d already matches Qdrant schema)", output_dim
        )
    else:
        logger.info("Next steps:")
        logger.info('  1. Update vector_search.py: _VOYAGE_MODEL = "%s"', target_model)
        logger.info("  2. Add output_dimension=%d to Voyage embed payload.", output_dim)
        logger.info("  3. Run nova-test golden eval and verify recall.")
        logger.info("  4. Deploy. If issues arise, --rollback restores from backup.")

    if failed_batches > 0:
        logger.warning(
            "%d batches failed -- migration is INCOMPLETE. Investigate, then re-run "
            "(idempotent: already-migrated points will be re-embedded harmlessly).",
            failed_batches,
        )
        return 2
    return 0


# ── Rollback workflow ────────────────────────────────────────────────────────


def _rollback(
    backup_file: Path,
    execute: bool,
    qdrant_url: str,
    qdrant_key: str,
) -> int:
    """Restore Qdrant collection from a backup JSON file."""
    dry_run = not execute
    mode = "EXECUTE" if execute else "DRY RUN"
    logger.info("=" * 70)
    logger.info("Voyage 4 Migration ROLLBACK -- mode=%s", mode)
    logger.info("  backup_file=%s", backup_file)
    logger.info("=" * 70)

    try:
        points = _read_backup(backup_file)
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        logger.error("Could not load backup: %s", exc)
        return 1

    if not points:
        logger.error("Backup contains 0 points; nothing to restore.")
        return 1

    info = _qdrant_collection_info(qdrant_url, qdrant_key)
    if info is None:
        logger.error("Cannot reach Qdrant; aborting rollback.")
        return 1

    start_time = time.time()
    upserted = 0
    failed_batches = 0
    qdrant_batch_size = 100  # Qdrant batch size, NOT bound by Voyage rate-limits
    total_batches = (len(points) + qdrant_batch_size - 1) // qdrant_batch_size

    for batch_idx in range(total_batches):
        batch = points[
            batch_idx * qdrant_batch_size : (batch_idx + 1) * qdrant_batch_size
        ]
        upsert_batch = []
        for p in batch:
            if "id" not in p or "vector" not in p:
                logger.warning("Skipping malformed backup point: %s", p.get("id"))
                continue
            upsert_batch.append(
                {
                    "id": p.get("id"),
                    "vector": p.get("vector"),
                    "payload": p.get("payload") or {},
                }
            )
        if not upsert_batch:
            continue

        ok = _qdrant_upsert_batch(qdrant_url, qdrant_key, upsert_batch, dry_run=dry_run)
        if ok:
            upserted += len(upsert_batch)
            logger.info(
                "Rollback batch %d/%d: %s %d points (total=%d)",
                batch_idx + 1,
                total_batches,
                "would restore" if dry_run else "restored",
                len(upsert_batch),
                upserted,
            )
        else:
            failed_batches += 1
            logger.error("Rollback batch %d failed", batch_idx + 1)

    elapsed = time.time() - start_time
    logger.info("=" * 70)
    logger.info("ROLLBACK SUMMARY (%s)", mode)
    logger.info("  Points restored:     %d / %d", upserted, len(points))
    logger.info("  Failed batches:      %d", failed_batches)
    logger.info("  Elapsed:             %.1fs", elapsed)
    logger.info("=" * 70)

    if failed_batches > 0:
        return 2
    if dry_run:
        logger.info("Re-run with --execute to actually restore.")
    else:
        logger.info(
            "Rollback complete. Revert vector_search.py to voyage-3-lite as well."
        )
    return 0


# ── CLI entry point ──────────────────────────────────────────────────────────


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="migrate_voyage_4.py",
        description=(
            "Migrate Qdrant 'nova_knowledge' index from voyage-3-lite to "
            "voyage-4-{lite,large}. Safe by default (dry-run). Use --execute "
            "to actually mutate. Use --rollback to restore from a backup."
        ),
        epilog=(
            "Examples:\n"
            "  python scripts/migrate_voyage_4.py --dry-run --backup\n"
            "  python scripts/migrate_voyage_4.py --execute\n"
            "  python scripts/migrate_voyage_4.py --execute --target-model voyage-4-large\n"
            "  python scripts/migrate_voyage_4.py --rollback --rollback-from data/qdrant_backup_pre_voyage4_TS.json --execute\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Log every action without changing Qdrant. DEFAULT.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        default=False,
        help="Actually run the migration (auto-backup first). Overrides --dry-run.",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        default=False,
        help="Force a backup file to be written (always on with --execute).",
    )
    parser.add_argument(
        "--rollback",
        action="store_true",
        default=False,
        help="Restore points from --rollback-from backup file (instead of migrating).",
    )
    parser.add_argument(
        "--rollback-from",
        type=str,
        default="",
        help="Path to backup JSON file (required with --rollback).",
    )
    parser.add_argument(
        "--target-model",
        type=str,
        default="voyage-4-lite",
        choices=sorted(VOYAGE_4_PRICING.keys()),
        help="Voyage 4 model to migrate to. Default: voyage-4-lite.",
    )
    parser.add_argument(
        "--output-dim",
        type=int,
        default=512,
        help=(
            "Output embedding dimension via Matryoshka truncation. "
            "Must match Qdrant collection vector size. Default: 512. "
            f"Valid values: {sorted(VOYAGE_4_VALID_DIMS)}."
        ),
    )
    parser.add_argument(
        "--baseline-path",
        type=str,
        default=DEFAULT_BASELINE_PATH,
        help=(
            "Path to recall@5 baseline JSON (created on first run). "
            f"Default: {DEFAULT_BASELINE_PATH}"
        ),
    )
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    # Validate output_dim is a Voyage 4 supported truncation
    if args.output_dim not in VOYAGE_4_VALID_DIMS:
        logger.error(
            "Invalid --output-dim %d. Must be one of %s.",
            args.output_dim,
            sorted(VOYAGE_4_VALID_DIMS),
        )
        return 1

    voyage_key, qdrant_url, qdrant_key = _validate_env()

    project_root = Path(__file__).resolve().parent.parent
    baseline_path = Path(args.baseline_path)
    if not baseline_path.is_absolute():
        baseline_path = project_root / baseline_path

    # Rollback path
    if args.rollback:
        if not args.rollback_from:
            logger.error("--rollback requires --rollback-from <path>")
            return 1
        backup_file = Path(args.rollback_from)
        if not backup_file.is_absolute():
            backup_file = project_root / backup_file
        return _rollback(
            backup_file=backup_file,
            execute=args.execute,
            qdrant_url=qdrant_url,
            qdrant_key=qdrant_key,
        )

    # Migration path
    return _migrate(
        target_model=args.target_model,
        output_dim=args.output_dim,
        do_backup=args.backup,
        execute=args.execute,
        voyage_key=voyage_key,
        qdrant_url=qdrant_url,
        qdrant_key=qdrant_key,
        baseline_path=baseline_path,
    )


if __name__ == "__main__":
    sys.exit(main())
