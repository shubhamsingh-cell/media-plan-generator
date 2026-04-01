#!/usr/bin/env python3
"""Populate Qdrant Cloud with Nova AI knowledge base embeddings.

Reads all KB JSON files from data/, chunks them into segments, generates
embeddings via Voyage AI (voyage-3-lite, 1024 dims), and upserts into
the Qdrant nova_knowledge collection.

Idempotent: safe to re-run. Uses PUT for collection creation and
deterministic point IDs based on content hash.

Env vars required:
    QDRANT_URL       -- Qdrant Cloud cluster URL
    QDRANT_API_KEY   -- Qdrant Cloud API key
    VOYAGE_API_KEY   -- Voyage AI API key

Usage:
    python scripts/populate_qdrant.py
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
QDRANT_URL = os.environ.get("QDRANT_URL") or ""
QDRANT_API_KEY = os.environ.get("QDRANT_API_KEY") or ""
VOYAGE_API_KEY = os.environ.get("VOYAGE_API_KEY") or ""

COLLECTION_NAME = "nova_knowledge"
VECTOR_DIM = 1024
VOYAGE_MODEL = "voyage-3-lite"
VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"

# Chunking
MAX_CHUNK_CHARS = 2000  # ~500 tokens
MIN_CHUNK_CHARS = 100  # skip tiny chunks

# Rate limiting for Voyage AI (free tier: ~10 RPM)
VOYAGE_BATCH_SIZE = 8  # Keep small for 10K TPM limit
VOYAGE_DELAY = 22.0  # 3 RPM free tier = 20s + buffer

# Qdrant upsert batch size
QDRANT_BATCH_SIZE = 100

# Files to skip (not KB content)
SKIP_FILES = {
    "auto_qc_dynamic_tests.json",
    "auto_qc_results.json",
    "audit_log.jsonl",
    "benchmark_drift_results.json",
    "enrichment_state.json",
    "live_market_data.json",
    "nova_memory_default.json",
    "nova_memory_stress-test.json",
    "nova_memory_test.json",
    "nova_learned_answers.json",
    "request_log.json",
    "request_log.json.lock",
}

# Very large files to handle specially (chunk more aggressively)
LARGE_FILE_THRESHOLD = 500_000  # 500KB

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


# ── Qdrant API ───────────────────────────────────────────────────────────────


def qdrant_request(
    method: str,
    path: str,
    body: dict | None = None,
    timeout: int = 30,
) -> dict | None:
    """Send HTTP request to Qdrant REST API."""
    base = QDRANT_URL.rstrip("/")
    if not path.startswith("/"):
        path = f"/{path}"

    url = f"{base}{path}"
    headers = {
        "api-key": QDRANT_API_KEY,
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
        logger.error("Qdrant %s %s -> %d: %s", method, path, exc.code, resp_text)
        return None
    except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
        logger.error("Qdrant %s %s error: %s", method, path, exc)
        return None


def ensure_collection() -> bool:
    """Create nova_knowledge collection if it doesn't exist."""
    check = qdrant_request("GET", f"/collections/{COLLECTION_NAME}")
    if check and check.get("result"):
        logger.info("Collection '%s' already exists", COLLECTION_NAME)
        # Get current point count
        info = check.get("result", {})
        points_count = info.get("points_count", 0)
        logger.info("Current point count: %d", points_count)
        return True

    logger.info(
        "Creating collection '%s' (dim=%d, cosine)", COLLECTION_NAME, VECTOR_DIM
    )
    result = qdrant_request(
        "PUT",
        f"/collections/{COLLECTION_NAME}",
        body={
            "vectors": {
                "size": VECTOR_DIM,
                "distance": "Cosine",
            }
        },
    )
    if result is not None:
        logger.info("Collection '%s' created successfully", COLLECTION_NAME)
        return True

    logger.error("Failed to create collection '%s'", COLLECTION_NAME)
    return False


def upsert_points(points: list[dict]) -> int:
    """Upsert points into Qdrant in batches. Returns count of successfully upserted."""
    total = 0
    for i in range(0, len(points), QDRANT_BATCH_SIZE):
        batch = points[i : i + QDRANT_BATCH_SIZE]
        result = qdrant_request(
            "PUT",
            f"/collections/{COLLECTION_NAME}/points",
            body={"points": batch},
            timeout=60,
        )
        if result is not None:
            total += len(batch)
        else:
            logger.warning("Upsert batch %d-%d failed", i, i + len(batch))
    return total


# ── Voyage AI Embeddings ─────────────────────────────────────────────────────


def get_embeddings(texts: list[str], max_retries: int = 5) -> list[list[float] | None]:
    """Get embeddings from Voyage AI for a batch of texts, with retry on 429."""
    if not texts:
        return []

    headers = {
        "Authorization": f"Bearer {VOYAGE_API_KEY}",
        "Content-Type": "application/json",
    }

    for attempt in range(max_retries):
        body = json.dumps(
            {
                "input": texts,
                "model": VOYAGE_MODEL,
            }
        ).encode("utf-8")

        try:
            req = urllib.request.Request(
                VOYAGE_API_URL, data=body, headers=headers, method="POST"
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read().decode("utf-8"))

            embeddings: list[list[float] | None] = [None] * len(texts)
            for item in result.get("data", []):
                idx = item.get("index", 0)
                if idx < len(embeddings):
                    embeddings[idx] = item.get("embedding")
            return embeddings

        except urllib.error.HTTPError as exc:
            resp_text = ""
            try:
                resp_text = exc.read().decode("utf-8")[:500]
            except Exception:
                pass

            if exc.code == 429:
                backoff = 25 * (attempt + 1)
                logger.warning(
                    "Rate limited (attempt %d/%d), waiting %ds...",
                    attempt + 1,
                    max_retries,
                    backoff,
                )
                time.sleep(backoff)
                continue

            logger.error("Voyage API error %d: %s", exc.code, resp_text)
            return [None] * len(texts)
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            logger.error("Voyage API error: %s", exc)
            return [None] * len(texts)

    logger.error("Exhausted retries for Voyage API")
    return [None] * len(texts)


# ── Chunking ─────────────────────────────────────────────────────────────────


def chunk_text(text: str, max_chars: int = MAX_CHUNK_CHARS) -> list[str]:
    """Split text into chunks by double newline or heading boundaries."""
    if len(text) <= max_chars:
        return [text.strip()] if text.strip() else []

    # Split by double newlines first
    paragraphs = re.split(r"\n\n+", text)

    chunks: list[str] = []
    current = ""

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        if len(current) + len(para) + 2 <= max_chars:
            current = f"{current}\n\n{para}" if current else para
        else:
            if current and len(current) >= MIN_CHUNK_CHARS:
                chunks.append(current.strip())
            # If a single paragraph is too large, split by sentences
            if len(para) > max_chars:
                sentences = re.split(r"(?<=[.!?])\s+", para)
                current = ""
                for sent in sentences:
                    if len(current) + len(sent) + 1 <= max_chars:
                        current = f"{current} {sent}" if current else sent
                    else:
                        if current and len(current) >= MIN_CHUNK_CHARS:
                            chunks.append(current.strip())
                        current = sent
            else:
                current = para

    if current and len(current) >= MIN_CHUNK_CHARS:
        chunks.append(current.strip())

    return chunks


def json_to_text(data: Any, prefix: str = "", depth: int = 0) -> str:
    """Convert JSON structure to readable text for embedding."""
    if depth > 5:
        return str(data)[:500]

    if isinstance(data, str):
        return f"{prefix}: {data}" if prefix else data

    if isinstance(data, (int, float, bool)):
        return f"{prefix}: {data}" if prefix else str(data)

    if isinstance(data, list):
        parts = []
        for i, item in enumerate(data):
            if isinstance(item, dict):
                parts.append(json_to_text(item, prefix=prefix, depth=depth + 1))
            elif isinstance(item, str):
                parts.append(item)
            else:
                parts.append(str(item))
        return "\n".join(parts)

    if isinstance(data, dict):
        parts = []
        for key, value in data.items():
            label = f"{prefix} > {key}" if prefix else key
            if isinstance(value, str) and len(value) < 500:
                parts.append(f"{label}: {value}")
            elif isinstance(value, (int, float, bool)):
                parts.append(f"{label}: {value}")
            elif isinstance(value, dict):
                parts.append(json_to_text(value, prefix=label, depth=depth + 1))
            elif isinstance(value, list):
                if all(isinstance(v, str) for v in value):
                    parts.append(f"{label}: {', '.join(value)}")
                else:
                    parts.append(json_to_text(value, prefix=label, depth=depth + 1))
        return "\n".join(parts)

    return str(data)


def chunk_json_file(filepath: Path) -> list[dict]:
    """Read a JSON file and return chunks with metadata.

    Returns list of dicts: {text: str, filename: str, chunk_index: int}
    """
    filename = filepath.name
    file_size = filepath.stat().st_size

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to read %s: %s", filename, exc)
        return []

    chunks: list[dict] = []

    # For very large files (e.g., joveo_global_supply_repository), chunk at top-level items
    if file_size > LARGE_FILE_THRESHOLD and isinstance(data, (list, dict)):
        if isinstance(data, list):
            # Process items individually or in small groups
            batch_text = ""
            for item in data:
                item_text = json_to_text(item, prefix=filename.replace(".json", ""))
                if len(batch_text) + len(item_text) > MAX_CHUNK_CHARS:
                    if batch_text.strip():
                        chunks.append(
                            {
                                "text": batch_text.strip()[:2000],
                                "filename": filename,
                                "chunk_index": len(chunks),
                            }
                        )
                    batch_text = item_text
                else:
                    batch_text = (
                        f"{batch_text}\n{item_text}" if batch_text else item_text
                    )

            if batch_text.strip():
                chunks.append(
                    {
                        "text": batch_text.strip()[:2000],
                        "filename": filename,
                        "chunk_index": len(chunks),
                    }
                )

        elif isinstance(data, dict):
            # Chunk by top-level keys
            for key, value in data.items():
                section_text = json_to_text(value, prefix=key)
                section_chunks = chunk_text(section_text)
                for sc in section_chunks:
                    chunks.append(
                        {
                            "text": sc[:2000],
                            "filename": filename,
                            "chunk_index": len(chunks),
                        }
                    )
    else:
        # Normal file: convert to text and chunk
        full_text = json_to_text(data, prefix=filename.replace(".json", ""))
        text_chunks = chunk_text(full_text)
        for i, tc in enumerate(text_chunks):
            chunks.append(
                {
                    "text": tc[:2000],
                    "filename": filename,
                    "chunk_index": i,
                }
            )

    return chunks


def chunk_csv_file(filepath: Path) -> list[dict]:
    """Read a CSV file and return chunks with metadata."""
    filename = filepath.name

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError as exc:
        logger.error("Failed to read %s: %s", filename, exc)
        return []

    chunks: list[dict] = []
    # Skip the first header row (generic column names), use row 2 as header
    if len(lines) < 3:
        return []

    header = lines[1].strip()
    batch = header + "\n"
    for line in lines[2:]:
        line = line.strip()
        if not line:
            continue
        if len(batch) + len(line) > MAX_CHUNK_CHARS:
            chunks.append(
                {
                    "text": batch.strip()[:2000],
                    "filename": filename,
                    "chunk_index": len(chunks),
                }
            )
            batch = header + "\n" + line + "\n"
        else:
            batch += line + "\n"

    if batch.strip() and len(batch.strip()) > MIN_CHUNK_CHARS:
        chunks.append(
            {
                "text": batch.strip()[:2000],
                "filename": filename,
                "chunk_index": len(chunks),
            }
        )

    return chunks


def make_point_id(filename: str, chunk_index: int) -> int:
    """Generate a deterministic int64 point ID from filename + chunk index."""
    key = f"{filename}::{chunk_index}"
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(h[:16], 16) % (2**63)


# ── Main ─────────────────────────────────────────────────────────────────────


def populate() -> None:
    """Main function: read KB, chunk, embed, upsert into Qdrant."""
    # Validate env vars
    missing = []
    if not QDRANT_URL:
        missing.append("QDRANT_URL")
    if not QDRANT_API_KEY:
        missing.append("QDRANT_API_KEY")
    if not VOYAGE_API_KEY:
        missing.append("VOYAGE_API_KEY")
    if missing:
        logger.error("Missing env vars: %s", ", ".join(missing))
        sys.exit(1)

    logger.info("Qdrant URL: %s", QDRANT_URL)
    logger.info("Data dir: %s", DATA_DIR)

    # Step 1: Ensure collection exists
    if not ensure_collection():
        logger.error("Cannot create/verify Qdrant collection. Aborting.")
        sys.exit(1)

    # Step 2: Collect all KB files
    json_files = sorted(DATA_DIR.glob("*.json"))
    csv_files = sorted(DATA_DIR.glob("*.csv"))

    # Step 3: Chunk all files
    all_chunks: list[dict] = []

    for fpath in json_files:
        if fpath.name in SKIP_FILES or fpath.name.startswith("."):
            logger.info("Skipping %s", fpath.name)
            continue

        file_chunks = chunk_json_file(fpath)
        logger.info("Chunked %s -> %d chunks", fpath.name, len(file_chunks))
        all_chunks.extend(file_chunks)

    for fpath in csv_files:
        file_chunks = chunk_csv_file(fpath)
        logger.info("Chunked %s -> %d chunks", fpath.name, len(file_chunks))
        all_chunks.extend(file_chunks)

    logger.info("Total chunks to embed: %d", len(all_chunks))

    if not all_chunks:
        logger.warning("No chunks found. Nothing to do.")
        return

    # Step 4: Generate embeddings in batches
    all_points: list[dict] = []
    total_embedded = 0
    total_batches = (len(all_chunks) + VOYAGE_BATCH_SIZE - 1) // VOYAGE_BATCH_SIZE

    for batch_idx in range(0, len(all_chunks), VOYAGE_BATCH_SIZE):
        batch_chunks = all_chunks[batch_idx : batch_idx + VOYAGE_BATCH_SIZE]
        batch_texts = [c["text"] for c in batch_chunks]
        batch_num = batch_idx // VOYAGE_BATCH_SIZE + 1

        logger.info(
            "Embedding batch %d/%d (%d texts)...",
            batch_num,
            total_batches,
            len(batch_texts),
        )

        embeddings = get_embeddings(batch_texts)

        for chunk, embedding in zip(batch_chunks, embeddings):
            if embedding is None:
                logger.warning(
                    "No embedding for %s chunk %d, skipping",
                    chunk["filename"],
                    chunk["chunk_index"],
                )
                continue

            point_id = make_point_id(chunk["filename"], chunk["chunk_index"])
            all_points.append(
                {
                    "id": point_id,
                    "vector": embedding,
                    "payload": {
                        "doc_id": f"{chunk['filename']}::chunk_{chunk['chunk_index']}",
                        "text": chunk["text"],
                        "metadata": {
                            "filename": chunk["filename"],
                            "chunk_index": chunk["chunk_index"],
                            "source": "knowledge_base",
                        },
                    },
                }
            )
            total_embedded += 1

        # Rate limit: wait between batches
        if batch_num < total_batches:
            logger.info("Rate limit pause (%.1fs)...", VOYAGE_DELAY)
            time.sleep(VOYAGE_DELAY)

    logger.info("Successfully embedded %d / %d chunks", total_embedded, len(all_chunks))

    # Step 5: Upsert into Qdrant
    if all_points:
        logger.info("Upserting %d points into Qdrant...", len(all_points))
        upserted = upsert_points(all_points)
        logger.info("Successfully upserted %d / %d points", upserted, len(all_points))
    else:
        logger.warning("No points to upsert.")

    # Step 6: Verify
    check = qdrant_request("GET", f"/collections/{COLLECTION_NAME}")
    if check and check.get("result"):
        info = check["result"]
        logger.info(
            "Verification: collection '%s' has %d points",
            COLLECTION_NAME,
            info.get("points_count", 0),
        )
    else:
        logger.warning("Could not verify collection after upsert")

    logger.info("Done!")


if __name__ == "__main__":
    populate()
