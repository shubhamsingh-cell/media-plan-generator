#!/usr/bin/env python3
"""Seed Supabase tables with initial data from JSON files.

Reads seed JSON files from data/ and inserts them into Supabase using
the Python SDK (supabase-py). Supports --dry-run for preview and
--table to seed specific tables.

Usage:
    python scripts/seed_supabase.py                          # seed all tables
    python scripts/seed_supabase.py --dry-run                # preview without writing
    python scripts/seed_supabase.py --table salary_data      # seed one table
    python scripts/seed_supabase.py --table nova_memory      # create sample memory entries

Requires:
    SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("seed_supabase")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"

# Add project root to path so we can import supabase_client
sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Supabase client
# ---------------------------------------------------------------------------

BATCH_SIZE = 50


def _get_client() -> Any:
    """Get the Supabase client via the shared singleton.

    Returns:
        Supabase client instance.

    Raises:
        SystemExit: If client is unavailable.
    """
    try:
        from supabase_client import get_client

        client = get_client()
        if client is None:
            logger.error(
                "Supabase client unavailable. Check SUPABASE_URL and "
                "SUPABASE_SERVICE_ROLE_KEY environment variables."
            )
            sys.exit(1)
        return client
    except ImportError:
        logger.error(
            "Could not import supabase_client. Make sure supabase-py is installed."
        )
        sys.exit(1)


def _load_seed_file(filename: str) -> list[dict[str, Any]]:
    """Load a seed JSON file from the data directory.

    Args:
        filename: JSON filename in data/.

    Returns:
        List of row dicts.
    """
    filepath = DATA_DIR / filename
    if not filepath.exists():
        logger.error(f"Seed file not found: {filepath}")
        return []
    try:
        data = json.loads(filepath.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return data
        logger.error(f"Expected list in {filepath}, got {type(data).__name__}")
        return []
    except (json.JSONDecodeError, OSError) as exc:
        logger.error(f"Failed to load {filepath}: {exc}", exc_info=True)
        return []


def _upsert_rows(
    client: Any,
    table: str,
    rows: list[dict[str, Any]],
    *,
    dry_run: bool = False,
    on_conflict: str = "",
) -> int:
    """Insert or upsert rows into a Supabase table in batches.

    Args:
        client: Supabase client instance.
        table: Target table name.
        rows: List of row dicts to insert.
        dry_run: If True, skip actual inserts.
        on_conflict: Comma-separated conflict columns for upsert.

    Returns:
        Number of rows inserted/upserted.
    """
    if not rows:
        return 0

    if dry_run:
        logger.info(f"  [DRY-RUN] Would insert {len(rows)} rows into {table}")
        for row in rows[:3]:
            logger.info(f"    Sample: {json.dumps(row, default=str)[:200]}")
        if len(rows) > 3:
            logger.info(f"    ... and {len(rows) - 3} more")
        return len(rows)

    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        try:
            if on_conflict:
                result = (
                    client.table(table).upsert(chunk, on_conflict=on_conflict).execute()
                )
            else:
                result = client.table(table).insert(chunk).execute()
            count = len(result.data) if result and result.data else len(chunk)
            total += count
            logger.info(
                f"  Inserted batch {i // BATCH_SIZE + 1}: " f"{count} rows into {table}"
            )
        except Exception as exc:
            logger.error(f"  Failed to insert batch into {table}: {exc}", exc_info=True)
    return total


# ---------------------------------------------------------------------------
# Table seeders
# ---------------------------------------------------------------------------


def seed_salary_data(client: Any, *, dry_run: bool = False) -> int:
    """Seed salary_data table from seed_salary_data.json.

    Args:
        client: Supabase client.
        dry_run: Preview mode.

    Returns:
        Number of rows seeded.
    """
    rows = _load_seed_file("seed_salary_data.json")
    if not rows:
        return 0
    logger.info(f"Seeding salary_data with {len(rows)} rows...")
    return _upsert_rows(client, "salary_data", rows, dry_run=dry_run)


def seed_compliance_rules(client: Any, *, dry_run: bool = False) -> int:
    """Seed compliance_rules table from seed_compliance_rules.json.

    Args:
        client: Supabase client.
        dry_run: Preview mode.

    Returns:
        Number of rows seeded.
    """
    rows = _load_seed_file("seed_compliance_rules.json")
    if not rows:
        return 0
    logger.info(f"Seeding compliance_rules with {len(rows)} rows...")
    return _upsert_rows(
        client, "compliance_rules", rows, dry_run=dry_run, on_conflict="id"
    )


def seed_market_trends(client: Any, *, dry_run: bool = False) -> int:
    """Seed market_trends table from seed_market_trends.json.

    Args:
        client: Supabase client.
        dry_run: Preview mode.

    Returns:
        Number of rows seeded.
    """
    rows = _load_seed_file("seed_market_trends.json")
    if not rows:
        return 0
    logger.info(f"Seeding market_trends with {len(rows)} rows...")
    return _upsert_rows(
        client, "market_trends", rows, dry_run=dry_run, on_conflict="id"
    )


def seed_vendor_profiles(client: Any, *, dry_run: bool = False) -> int:
    """Seed vendor_profiles table from seed_vendor_profiles.json.

    Args:
        client: Supabase client.
        dry_run: Preview mode.

    Returns:
        Number of rows seeded.
    """
    rows = _load_seed_file("seed_vendor_profiles.json")
    if not rows:
        return 0
    logger.info(f"Seeding vendor_profiles with {len(rows)} rows...")
    return _upsert_rows(
        client, "vendor_profiles", rows, dry_run=dry_run, on_conflict="name"
    )


def seed_supply_repository(client: Any, *, dry_run: bool = False) -> int:
    """Seed supply_repository table from seed_supply_repository.json.

    Args:
        client: Supabase client.
        dry_run: Preview mode.

    Returns:
        Number of rows seeded.
    """
    rows = _load_seed_file("seed_supply_repository.json")
    if not rows:
        return 0
    logger.info(f"Seeding supply_repository with {len(rows)} rows...")
    return _upsert_rows(
        client, "supply_repository", rows, dry_run=dry_run, on_conflict="name"
    )


def seed_nova_memory(client: Any, *, dry_run: bool = False) -> int:
    """Seed nova_memory table with sample entries to verify the table works.

    Args:
        client: Supabase client.
        dry_run: Preview mode.

    Returns:
        Number of rows seeded.
    """
    rows = [
        {
            "user_id": "system",
            "memory_type": "long_term",
            "key": "system_init",
            "value": {"event": "table_created", "version": "1.0"},
            "content": "Nova memory table initialized with seed data",
            "metadata": {"source": "seed_supabase.py"},
        },
        {
            "user_id": "system",
            "memory_type": "preference",
            "key": "default_model",
            "value": {"model": "claude-sonnet-4-20250514"},
            "content": "claude-sonnet-4-20250514",
            "metadata": {"source": "seed_supabase.py", "key": "default_model"},
        },
    ]
    logger.info(f"Seeding nova_memory with {len(rows)} sample rows...")
    return _upsert_rows(client, "nova_memory", rows, dry_run=dry_run)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

VALID_TABLES = [
    "salary_data",
    "compliance_rules",
    "market_trends",
    "vendor_profiles",
    "supply_repository",
    "nova_memory",
]

TABLE_SEEDERS: dict[str, Any] = {
    "salary_data": seed_salary_data,
    "compliance_rules": seed_compliance_rules,
    "market_trends": seed_market_trends,
    "vendor_profiles": seed_vendor_profiles,
    "supply_repository": seed_supply_repository,
    "nova_memory": seed_nova_memory,
}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Optional argument list.

    Returns:
        Parsed namespace.
    """
    parser = argparse.ArgumentParser(
        description="Seed Supabase tables with initial data from JSON files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/seed_supabase.py                        # seed all\n"
            "  python scripts/seed_supabase.py --dry-run               # preview\n"
            "  python scripts/seed_supabase.py --table salary_data     # one table\n"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be inserted without actually writing.",
    )
    parser.add_argument(
        "--table",
        choices=VALID_TABLES,
        default=None,
        help="Seed a single table instead of all tables.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the seeder for all (or a single) table.

    Args:
        argv: Optional CLI arguments.

    Returns:
        Exit code: 0 on success.
    """
    args = parse_args(argv)

    logger.info(f"Data directory: {DATA_DIR}")
    logger.info(f"Dry run: {args.dry_run}")
    if args.table:
        logger.info(f"Target table: {args.table}")
    logger.info("-" * 60)

    # Only initialize client if not dry-run
    client = None
    if not args.dry_run:
        client = _get_client()

    summary: dict[str, int] = {}
    tables_to_run = [args.table] if args.table else VALID_TABLES

    for table in tables_to_run:
        seeder = TABLE_SEEDERS[table]
        if args.dry_run:
            count = seeder(None, dry_run=True)
        else:
            count = seeder(client, dry_run=False)
        summary[table] = count

    # Summary
    logger.info("-" * 60)
    logger.info("Seed summary:")
    grand_total = 0
    for table, count in summary.items():
        logger.info(f"  {table}: {count} rows")
        grand_total += count
    logger.info(f"  TOTAL: {grand_total} rows")

    if args.dry_run:
        logger.info("(Dry run -- no data was actually written)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
