"""Migrate local JSON knowledge base files to Supabase tables.

Reads all JSON files from data/ and inserts them into the appropriate
Supabase tables using the REST API (PostgREST). Uses upsert to avoid
duplicates on re-run.

Usage:
    python scripts/migrate_to_supabase.py [--dry-run]

Environment variables required:
    SUPABASE_URL      -- e.g. https://trpynqjatlhatxpzrvgt.supabase.co
    SUPABASE_ANON_KEY -- anon/service key
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"

SUPABASE_URL = os.environ.get("SUPABASE_URL") or ""
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY") or ""

_SSL_CTX = ssl.create_default_context()
_HTTP_TIMEOUT = 10  # longer timeout for bulk inserts

# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------
_stats: dict[str, int] = {
    "files_processed": 0,
    "files_skipped": 0,
    "files_errored": 0,
    "rows_upserted": 0,
    "rows_failed": 0,
}


# ---------------------------------------------------------------------------
# HTTP helpers (mirrors supabase_cache.py patterns)
# ---------------------------------------------------------------------------


def _build_headers(extra: Optional[dict[str, str]] = None) -> dict[str, str]:
    """Build standard Supabase REST API headers."""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if extra:
        headers.update(extra)
    return headers


def _rest_url(table: str) -> str:
    """Build full REST URL for the given table."""
    base = SUPABASE_URL.rstrip("/")
    return f"{base}/rest/v1/{table}"


def _supabase_upsert(
    table: str, rows: list[dict[str, Any]], dry_run: bool = False
) -> int:
    """Upsert rows into a Supabase table via REST API.

    Uses Prefer: resolution=merge-duplicates for idempotent inserts.
    Batches rows in chunks of 500 to stay within PostgREST limits.

    Args:
        table: Target table name.
        rows: List of row dicts to upsert.
        dry_run: If True, skip the actual HTTP call.

    Returns:
        Number of rows successfully upserted.
    """
    if not rows:
        return 0

    if dry_run:
        logger.info(f"  [DRY RUN] Would upsert {len(rows)} rows into '{table}'")
        return len(rows)

    url = _rest_url(table)
    headers = _build_headers(
        {
            "Prefer": "resolution=merge-duplicates,return=minimal",
        }
    )

    total_upserted = 0
    batch_size = 500

    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        try:
            body = json.dumps(batch, default=str).encode("utf-8")
            req = urllib.request.Request(url, data=body, method="POST", headers=headers)
            with urllib.request.urlopen(
                req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
            ) as resp:
                resp.read()
                if 200 <= resp.status < 300:
                    total_upserted += len(batch)
                    logger.info(
                        f"  Upserted batch {i // batch_size + 1}: {len(batch)} rows into '{table}'"
                    )
                else:
                    logger.error(
                        f"  Unexpected status {resp.status} upserting into '{table}'"
                    )
                    _stats["rows_failed"] += len(batch)
        except urllib.error.HTTPError as exc:
            error_body = ""
            try:
                error_body = exc.read().decode("utf-8", errors="replace")[:500]
            except Exception:
                pass
            logger.error(
                f"  HTTP {exc.code} upserting into '{table}': {error_body}",
                exc_info=True,
            )
            _stats["rows_failed"] += len(batch)
        except urllib.error.URLError as exc:
            logger.error(
                f"  URLError upserting into '{table}': {exc.reason}", exc_info=True
            )
            _stats["rows_failed"] += len(batch)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
            logger.error(f"  Error upserting into '{table}': {exc}", exc_info=True)
            _stats["rows_failed"] += len(batch)

    return total_upserted


def _log_enrichment(
    table_name: str,
    action: str,
    records_affected: int,
    source: str,
    details: dict[str, Any],
    dry_run: bool = False,
) -> None:
    """Log a migration action to the enrichment_log table."""
    row = {
        "table_name": table_name,
        "action": action,
        "records_affected": records_affected,
        "source": source,
        "details": details,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    _supabase_upsert("enrichment_log", [row], dry_run=dry_run)


# ---------------------------------------------------------------------------
# JSON file loaders
# ---------------------------------------------------------------------------


def _load_json(filename: str) -> Optional[dict[str, Any]]:
    """Load a JSON file from the data directory.

    Args:
        filename: Name of the JSON file in data/.

    Returns:
        Parsed JSON dict, or None if the file does not exist or fails to parse.
    """
    filepath = DATA_DIR / filename
    if not filepath.exists():
        logger.warning(f"File not found: {filepath}")
        return None
    try:
        return json.loads(filepath.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.error(f"Failed to load {filepath}: {exc}", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Migration functions per file -> table mapping
# ---------------------------------------------------------------------------

# Files that map directly into knowledge_base with a category
_KB_FILE_MAP: dict[str, str] = {
    "recruitment_industry_knowledge.json": "industry_insights",
    "platform_intelligence_deep.json": "platform_data",
    "recruitment_benchmarks_deep.json": "benchmarks",
    "recruitment_strategy_intelligence.json": "strategy",
    "regional_hiring_intelligence.json": "regional",
    "supply_ecosystem_intelligence.json": "supply",
    "workforce_trends_intelligence.json": "trends",
    "industry_white_papers.json": "white_papers",
    "joveo_2026_benchmarks.json": "joveo_benchmarks",
    "google_ads_2025_benchmarks.json": "google_ads_benchmarks",
    "external_benchmarks_2025.json": "external_benchmarks",
    "client_media_plans_kb.json": "client_plans",
}


def _migrate_knowledge_base_file(
    filename: str, category: str, dry_run: bool = False
) -> int:
    """Migrate a single JSON file into the knowledge_base table.

    Each top-level key in the JSON becomes a row with (category, key, data).

    Args:
        filename: JSON file name in data/.
        category: Category tag for knowledge_base rows.
        dry_run: If True, skip actual writes.

    Returns:
        Number of rows upserted.
    """
    data = _load_json(filename)
    if data is None:
        _stats["files_skipped"] += 1
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []

    if isinstance(data, dict):
        for key, value in data.items():
            rows.append(
                {
                    "category": category,
                    "key": key,
                    "data": (
                        value if isinstance(value, (dict, list)) else {"value": value}
                    ),
                    "source": "migration",
                    "version": 1,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            )
    else:
        # Entire file as a single row
        rows.append(
            {
                "category": category,
                "key": "_all",
                "data": data,
                "source": "migration",
                "version": 1,
                "created_at": now_iso,
                "updated_at": now_iso,
            }
        )

    count = _supabase_upsert("knowledge_base", rows, dry_run=dry_run)
    _stats["files_processed"] += 1
    _stats["rows_upserted"] += count
    _log_enrichment(
        "knowledge_base",
        "migrate",
        count,
        f"file:{filename}",
        {"category": category},
        dry_run=dry_run,
    )
    logger.info(f"  {filename} -> knowledge_base ({category}): {count} rows")
    return count


def _migrate_channels_db(dry_run: bool = False) -> int:
    """Migrate channels_db.json into supply_repository table.

    Extracts individual channel names from traditional_channels and
    non_traditional_channels lists and stores each as a publisher row.

    Args:
        dry_run: If True, skip actual writes.

    Returns:
        Number of rows upserted.
    """
    data = _load_json("channels_db.json")
    if data is None:
        _stats["files_skipped"] += 1
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    # Extract channels from traditional_channels (each sub-key has a list of names)
    for subcategory, channel_list in (data.get("traditional_channels") or {}).items():
        if isinstance(channel_list, list):
            for item in channel_list:
                name = (
                    item
                    if isinstance(item, str)
                    else (item.get("name") or item.get("publisher_name") or str(item))
                )
                if name and name not in seen_names:
                    seen_names.add(name)
                    rows.append(
                        {
                            "publisher_name": name,
                            "category": f"traditional_{subcategory}",
                            "countries": [],
                            "industries": [],
                            "performance": {},
                            "metadata": {
                                "source_subcategory": subcategory,
                                "source_file": "channels_db.json",
                            },
                            "updated_at": now_iso,
                        }
                    )

    # Extract channels from non_traditional_channels
    for subcategory, channel_list in (
        data.get("non_traditional_channels") or {}
    ).items():
        if isinstance(channel_list, list):
            for item in channel_list:
                name = (
                    item
                    if isinstance(item, str)
                    else (item.get("name") or item.get("publisher_name") or str(item))
                )
                if name and name not in seen_names:
                    seen_names.add(name)
                    rows.append(
                        {
                            "publisher_name": name,
                            "category": f"nontraditional_{subcategory}",
                            "countries": [],
                            "industries": [],
                            "performance": {},
                            "metadata": {
                                "source_subcategory": subcategory,
                                "source_file": "channels_db.json",
                            },
                            "updated_at": now_iso,
                        }
                    )

    # Also store the full file as knowledge_base for strategy/lookup data
    strategy_rows: list[dict[str, Any]] = []
    for key in (
        "channel_strategies",
        "market_trend_factors",
        "competitor_categories",
        "industries",
        "job_categories",
        "cpa_rate_benchmarks",
    ):
        value = data.get(key)
        if value is not None:
            strategy_rows.append(
                {
                    "category": "channels_db",
                    "key": key,
                    "data": (
                        value if isinstance(value, (dict, list)) else {"value": value}
                    ),
                    "source": "migration",
                    "version": 1,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            )

    count_supply = _supabase_upsert("supply_repository", rows, dry_run=dry_run)
    count_kb = _supabase_upsert("knowledge_base", strategy_rows, dry_run=dry_run)
    total = count_supply + count_kb
    _stats["files_processed"] += 1
    _stats["rows_upserted"] += total
    _log_enrichment(
        "supply_repository",
        "migrate",
        count_supply,
        "file:channels_db.json",
        {"publishers": len(rows)},
        dry_run=dry_run,
    )
    logger.info(
        f"  channels_db.json -> supply_repository: {count_supply} rows, knowledge_base: {count_kb} rows"
    )
    return total


def _migrate_global_supply(dry_run: bool = False) -> int:
    """Migrate global_supply.json into supply_repository table.

    Extracts publisher data from country_job_boards and stores each
    board as a supply_repository row with country associations.

    Args:
        dry_run: If True, skip actual writes.

    Returns:
        Number of rows upserted.
    """
    data = _load_json("global_supply.json")
    if data is None:
        _stats["files_skipped"] += 1
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    # country_job_boards: {country: {boards: [{name, billing, category, tier, ...}]}}
    for country, country_data in (data.get("country_job_boards") or {}).items():
        boards = []
        if isinstance(country_data, dict):
            boards = country_data.get("boards") or []
        elif isinstance(country_data, list):
            boards = country_data

        for board in boards:
            if not isinstance(board, dict):
                continue
            name = board.get("name") or ""
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            rows.append(
                {
                    "publisher_name": name,
                    "category": board.get("category") or "general",
                    "countries": [country],
                    "industries": [],
                    "performance": {},
                    "metadata": {
                        "billing": board.get("billing") or "",
                        "tier": board.get("tier") or "",
                        "source_file": "global_supply.json",
                    },
                    "updated_at": now_iso,
                }
            )

    # Store ancillary data (dei_boards, commission_tiers, etc.) in knowledge_base
    kb_rows: list[dict[str, Any]] = []
    for key in (
        "dei_boards_by_country",
        "women_specific_boards",
        "commission_tiers",
        "billing_models",
    ):
        value = data.get(key)
        if value is not None:
            kb_rows.append(
                {
                    "category": "global_supply",
                    "key": key,
                    "data": (
                        value if isinstance(value, (dict, list)) else {"value": value}
                    ),
                    "source": "migration",
                    "version": 1,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            )

    count_supply = _supabase_upsert("supply_repository", rows, dry_run=dry_run)
    count_kb = _supabase_upsert("knowledge_base", kb_rows, dry_run=dry_run)
    total = count_supply + count_kb
    _stats["files_processed"] += 1
    _stats["rows_upserted"] += total
    _log_enrichment(
        "supply_repository",
        "migrate",
        count_supply,
        "file:global_supply.json",
        {
            "publishers": len(rows),
            "countries": len(data.get("country_job_boards") or {}),
        },
        dry_run=dry_run,
    )
    logger.info(
        f"  global_supply.json -> supply_repository: {count_supply} rows, knowledge_base: {count_kb} rows"
    )
    return total


def _migrate_joveo_publishers(dry_run: bool = False) -> int:
    """Migrate joveo_publishers.json into supply_repository table.

    Extracts publisher data from by_category and by_country structures.

    Args:
        dry_run: If True, skip actual writes.

    Returns:
        Number of rows upserted.
    """
    data = _load_json("joveo_publishers.json")
    if data is None:
        _stats["files_skipped"] += 1
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    # by_category: {category_name: [publisher_names or publisher_dicts]}
    for category, publishers in (data.get("by_category") or {}).items():
        if not isinstance(publishers, list):
            continue
        for pub in publishers:
            name = ""
            pub_meta: dict[str, Any] = {}
            if isinstance(pub, str):
                name = pub
            elif isinstance(pub, dict):
                name = pub.get("name") or pub.get("publisher_name") or ""
                pub_meta = {
                    k: v for k, v in pub.items() if k not in ("name", "publisher_name")
                }

            if not name or name in seen_names:
                continue
            seen_names.add(name)
            rows.append(
                {
                    "publisher_name": name,
                    "publisher_id": pub.get("id") if isinstance(pub, dict) else None,
                    "category": category,
                    "countries": [],
                    "industries": [],
                    "performance": {},
                    "metadata": {**pub_meta, "source_file": "joveo_publishers.json"},
                    "updated_at": now_iso,
                }
            )

    # by_country: {country_name: [publisher_names]}
    country_map: dict[str, list[str]] = {}
    for country, publishers in (data.get("by_country") or {}).items():
        if isinstance(publishers, list):
            for pub in publishers:
                name = (
                    pub
                    if isinstance(pub, str)
                    else (pub.get("name") or "") if isinstance(pub, dict) else ""
                )
                if name:
                    country_map.setdefault(name, []).append(country)

    # Update country lists for existing rows
    for row in rows:
        name = row["publisher_name"]
        if name in country_map:
            row["countries"] = country_map[name]

    # Add publishers from by_country that were not in by_category
    for country, publishers in (data.get("by_country") or {}).items():
        if isinstance(publishers, list):
            for pub in publishers:
                name = (
                    pub
                    if isinstance(pub, str)
                    else (pub.get("name") or "") if isinstance(pub, dict) else ""
                )
                if name and name not in seen_names:
                    seen_names.add(name)
                    rows.append(
                        {
                            "publisher_name": name,
                            "category": "joveo_publisher",
                            "countries": [country],
                            "industries": [],
                            "performance": {},
                            "metadata": {"source_file": "joveo_publishers.json"},
                            "updated_at": now_iso,
                        }
                    )

    # global_publishers
    for pub in data.get("global_publishers") or []:
        name = (
            pub
            if isinstance(pub, str)
            else (pub.get("name") or "") if isinstance(pub, dict) else ""
        )
        if name and name not in seen_names:
            seen_names.add(name)
            rows.append(
                {
                    "publisher_name": name,
                    "category": "global_publisher",
                    "countries": ["global"],
                    "industries": [],
                    "performance": {},
                    "metadata": {"source_file": "joveo_publishers.json"},
                    "updated_at": now_iso,
                }
            )

    count = _supabase_upsert("supply_repository", rows, dry_run=dry_run)
    _stats["files_processed"] += 1
    _stats["rows_upserted"] += count
    _log_enrichment(
        "supply_repository",
        "migrate",
        count,
        "file:joveo_publishers.json",
        {"publishers": len(rows)},
        dry_run=dry_run,
    )
    logger.info(f"  joveo_publishers.json -> supply_repository: {count} rows")
    return count


def _migrate_live_market_data(dry_run: bool = False) -> int:
    """Migrate live_market_data.json into channel_benchmarks and market_trends.

    Extracts per-channel CPC/CPA data into channel_benchmarks and
    overall market data into market_trends.

    Args:
        dry_run: If True, skip actual writes.

    Returns:
        Number of rows upserted.
    """
    data = _load_json("live_market_data.json")
    if data is None:
        _stats["files_skipped"] += 1
        return 0

    now_iso = datetime.now(timezone.utc).isoformat()
    scraped_at = data.get("scraped_at") or now_iso

    # -- Channel benchmarks from job_boards --
    benchmark_rows: list[dict[str, Any]] = []
    for channel_name, channel_data in (data.get("job_boards") or {}).items():
        if not isinstance(channel_data, dict):
            continue
        benchmark_rows.append(
            {
                "channel": channel_name,
                "industry": "overall",
                "cpc": channel_data.get("avg_cpc_typical"),
                "cpa": channel_data.get("avg_cpa_min"),
                "apply_rate": None,
                "quality_score": None,
                "monthly_reach": None,
                "pricing_model": channel_data.get("pricing_model") or "",
                "data_source": "live_firecrawl",
                "metadata": {
                    k: v
                    for k, v in channel_data.items()
                    if k not in ("avg_cpc_typical", "avg_cpa_min", "pricing_model")
                },
                "updated_at": now_iso,
            }
        )

    # -- Market trends from sources --
    trend_rows: list[dict[str, Any]] = []
    for source_info in data.get("sources") or []:
        if not isinstance(source_info, dict):
            continue
        url = source_info.get("url") or source_info.get("source") or ""
        if not url:
            # Generate a unique key from source name + date
            url = f"live_market:{source_info.get('name', 'unknown')}:{scraped_at}"
        trend_rows.append(
            {
                "title": source_info.get("name")
                or source_info.get("title")
                or "Market data source",
                "summary": source_info.get("description")
                or source_info.get("summary")
                or "",
                "source": source_info.get("name") or "live_market_data",
                "url": url,
                "category": "cpc_trends",
                "published_date": None,
                "scraped_at": scraped_at,
                "metadata": source_info,
            }
        )

    # -- Also store the full pricing_models_overview and data_freshness in knowledge_base --
    kb_rows: list[dict[str, Any]] = []
    for key in ("pricing_models_overview", "data_freshness", "sources"):
        value = data.get(key)
        if value is not None:
            kb_rows.append(
                {
                    "category": "live_market",
                    "key": key,
                    "data": (
                        value if isinstance(value, (dict, list)) else {"value": value}
                    ),
                    "source": "migration",
                    "version": 1,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }
            )

    count_bench = _supabase_upsert(
        "channel_benchmarks", benchmark_rows, dry_run=dry_run
    )
    count_trends = _supabase_upsert("market_trends", trend_rows, dry_run=dry_run)
    count_kb = _supabase_upsert("knowledge_base", kb_rows, dry_run=dry_run)
    total = count_bench + count_trends + count_kb
    _stats["files_processed"] += 1
    _stats["rows_upserted"] += total
    _log_enrichment(
        "channel_benchmarks",
        "migrate",
        count_bench,
        "file:live_market_data.json",
        {"channels": len(benchmark_rows)},
        dry_run=dry_run,
    )
    _log_enrichment(
        "market_trends",
        "migrate",
        count_trends,
        "file:live_market_data.json",
        {"trends": len(trend_rows)},
        dry_run=dry_run,
    )
    logger.info(
        f"  live_market_data.json -> channel_benchmarks: {count_bench}, market_trends: {count_trends}, knowledge_base: {count_kb}"
    )
    return total


# ---------------------------------------------------------------------------
# Main migration orchestrator
# ---------------------------------------------------------------------------


def run_migration(dry_run: bool = False) -> dict[str, int]:
    """Run the full migration from local JSON files to Supabase.

    Args:
        dry_run: If True, show what would be migrated without writing.

    Returns:
        Dict of migration statistics.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        logger.error(
            "SUPABASE_URL and SUPABASE_ANON_KEY environment variables must be set"
        )
        sys.exit(1)

    mode = "DRY RUN" if dry_run else "LIVE"
    logger.info(f"=== Nova AI Suite Migration ({mode}) ===")
    logger.info(f"Supabase URL: {SUPABASE_URL[:50]}...")
    logger.info(f"Data directory: {DATA_DIR}")
    logger.info("")

    # 1. Migrate knowledge_base files
    logger.info("--- Phase 1: Knowledge Base files ---")
    for filename, category in _KB_FILE_MAP.items():
        try:
            _migrate_knowledge_base_file(filename, category, dry_run=dry_run)
        except (OSError, json.JSONDecodeError, urllib.error.URLError) as exc:
            logger.error(f"Failed to migrate {filename}: {exc}", exc_info=True)
            _stats["files_errored"] += 1

    # 2. Migrate supply repository files
    logger.info("")
    logger.info("--- Phase 2: Supply Repository files ---")
    for migrate_fn, label in [
        (_migrate_channels_db, "channels_db"),
        (_migrate_global_supply, "global_supply"),
        (_migrate_joveo_publishers, "joveo_publishers"),
    ]:
        try:
            migrate_fn(dry_run=dry_run)
        except (OSError, json.JSONDecodeError, urllib.error.URLError) as exc:
            logger.error(f"Failed to migrate {label}: {exc}", exc_info=True)
            _stats["files_errored"] += 1

    # 3. Migrate live market data
    logger.info("")
    logger.info("--- Phase 3: Live Market Data ---")
    try:
        _migrate_live_market_data(dry_run=dry_run)
    except (OSError, json.JSONDecodeError, urllib.error.URLError) as exc:
        logger.error(f"Failed to migrate live_market_data: {exc}", exc_info=True)
        _stats["files_errored"] += 1

    # Summary
    logger.info("")
    logger.info("=== Migration Summary ===")
    logger.info(f"Files processed:  {_stats['files_processed']}")
    logger.info(f"Files skipped:    {_stats['files_skipped']}")
    logger.info(f"Files errored:    {_stats['files_errored']}")
    logger.info(f"Rows upserted:    {_stats['rows_upserted']}")
    logger.info(f"Rows failed:      {_stats['rows_failed']}")

    if _stats["files_errored"] > 0:
        logger.warning("Some files failed -- check logs above for details")

    return dict(_stats)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    run_migration(dry_run=dry_run)
