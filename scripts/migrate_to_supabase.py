#!/usr/bin/env python3
"""Standalone migration script: JSON data files -> Supabase tables via PostgREST.

Reads all knowledge-base JSON files from data/ and upserts them into the
corresponding Supabase tables.  Uses stdlib only (urllib, json, pathlib).

Usage:
    python migrate_to_supabase.py                     # migrate everything
    python migrate_to_supabase.py --dry-run            # preview without writing
    python migrate_to_supabase.py --table knowledge_base   # single table
    python migrate_to_supabase.py --table channel_benchmarks --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
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
logger = logging.getLogger("migrate_to_supabase")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SUPABASE_URL: str = (
    os.environ.get("SUPABASE_URL") or "https://trpynqjatlhatxpzrvgt.supabase.co"
)
SUPABASE_KEY: str = (
    os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    or os.environ.get("SUPABASE_ANON_KEY")
    or ""
)
DATA_DIR: Path = Path(__file__).parent / "data"
BATCH_SIZE: int = 100
HTTP_TIMEOUT: int = 15
SSL_CTX: ssl.SSLContext = ssl.create_default_context()

# ---------------------------------------------------------------------------
# Knowledge-base category -> filename map (mirrors supabase_data.py)
# ---------------------------------------------------------------------------

KB_CATEGORY_FILE_MAP: dict[str, str] = {
    "industry_insights": "recruitment_industry_knowledge.json",
    "benchmarks": "recruitment_benchmarks_deep.json",
    "platform_data": "platform_intelligence_deep.json",
    "strategy": "recruitment_strategy_intelligence.json",
    "regional": "regional_hiring_intelligence.json",
    "white_papers": "industry_white_papers.json",
    "google_ads_benchmarks": "google_ads_2025_benchmarks.json",
    "joveo_benchmarks": "joveo_2026_benchmarks.json",
    "external_benchmarks": "external_benchmarks_2025.json",
    "client_plans": "client_media_plans_kb.json",
    "supply": "supply_ecosystem_intelligence.json",
}

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _build_headers(*, prefer: str = "") -> dict[str, str]:
    """Build standard Supabase REST API headers.

    Args:
        prefer: Optional Prefer header value for upsert semantics.

    Returns:
        Dict of HTTP headers.
    """
    headers: dict[str, str] = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if prefer:
        headers["Prefer"] = prefer
    return headers


def _post_rows(table: str, rows: list[dict[str, Any]], *, dry_run: bool = False) -> int:
    """Upsert a batch of rows into a Supabase table via PostgREST.

    Uses the ``Prefer: resolution=merge-duplicates`` header so the call is
    idempotent (upsert rather than insert).

    Args:
        table: Target Supabase table name.
        rows: List of row dicts to upsert.
        dry_run: If True, skip the actual HTTP call.

    Returns:
        Number of rows sent (or that would be sent in dry-run mode).
    """
    if not rows:
        return 0
    if dry_run:
        logger.info(f"  [DRY-RUN] Would upsert {len(rows)} rows into {table}")
        return len(rows)

    # Map tables to their unique constraint columns for upsert
    _on_conflict_map: dict[str, str] = {
        "knowledge_base": "category,key",
        "channel_benchmarks": "channel,industry",
        "vendor_profiles": "name",
        "supply_repository": "name",
        "salary_data": "role,location",
        "compliance_rules": "rule_type,jurisdiction",
        "market_trends": "category,title,source",
    }
    base = SUPABASE_URL.rstrip("/")
    on_conflict = _on_conflict_map.get(table) or ""
    url = f"{base}/rest/v1/{table}"
    if on_conflict:
        # URL-encode the on_conflict parameter to handle commas correctly
        encoded_conflict = urllib.parse.quote(on_conflict, safe="")
        url += f"?on_conflict={encoded_conflict}"
    payload = json.dumps(rows, ensure_ascii=False).encode("utf-8")
    headers = _build_headers(prefer="resolution=merge-duplicates")

    try:
        req = urllib.request.Request(url, data=payload, method="POST", headers=headers)
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT, context=SSL_CTX) as resp:
            status = resp.getcode()
            if status and status >= 400:
                body = resp.read().decode("utf-8", errors="replace")[:300]
                logger.error(f"  HTTP {status} upserting to {table}: {body}")
                return 0
        return len(rows)
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        logger.error(
            f"  HTTP {exc.code} upserting to {table}: {error_body}", exc_info=True
        )
        return 0
    except urllib.error.URLError as exc:
        logger.error(f"  URLError upserting to {table}: {exc.reason}", exc_info=True)
        return 0
    except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error(f"  Error upserting to {table}: {exc}", exc_info=True)
        return 0


def _upsert_batched(
    table: str, rows: list[dict[str, Any]], *, dry_run: bool = False
) -> int:
    """Upsert rows in chunks of BATCH_SIZE to avoid payload limits.

    Args:
        table: Target Supabase table name.
        rows: Full list of row dicts.
        dry_run: If True, skip actual HTTP calls.

    Returns:
        Total number of rows successfully upserted.
    """
    total = 0
    for i in range(0, len(rows), BATCH_SIZE):
        chunk = rows[i : i + BATCH_SIZE]
        total += _post_rows(table, chunk, dry_run=dry_run)
    return total


# ---------------------------------------------------------------------------
# JSON loaders
# ---------------------------------------------------------------------------


def _load_json(filename: str) -> dict[str, Any] | list[Any] | None:
    """Load a JSON file from the data directory.

    Args:
        filename: Name of the file inside DATA_DIR.

    Returns:
        Parsed JSON (dict or list), or None on failure.
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
# Transformers: JSON -> row dicts for each target table
# ---------------------------------------------------------------------------


def _transform_knowledge_base(category: str, filename: str) -> list[dict[str, Any]]:
    """Transform a knowledge-base JSON file into rows for the knowledge_base table.

    Each top-level key in the JSON becomes a row with:
      - category: the knowledge-base category
      - key: the top-level JSON key
      - data: the value (stored as JSONB)

    Args:
        category: Knowledge-base category string.
        filename: Source JSON filename.

    Returns:
        List of row dicts ready for upsert.
    """
    raw = _load_json(filename)
    if raw is None or not isinstance(raw, dict):
        return []

    rows: list[dict[str, Any]] = []
    for key, value in raw.items():
        rows.append(
            {
                "category": category,
                "key": key,
                "data": value,
            }
        )
    return rows


def _transform_channel_benchmarks() -> list[dict[str, Any]]:
    """Transform live_market_data.json into rows for the channel_benchmarks table.

    Extracts per-board data from job_boards, plus industry/seniority/company-size
    benchmark sections.

    Returns:
        List of row dicts ready for upsert.
    """
    raw = _load_json("live_market_data.json")
    if raw is None or not isinstance(raw, dict):
        return []

    rows: list[dict[str, Any]] = []

    # Per-board benchmarks from job_boards
    job_boards: dict[str, Any] = raw.get("job_boards") or {}
    for channel_name, board_data in job_boards.items():
        if not isinstance(board_data, dict):
            continue
        rows.append(
            {
                "channel": channel_name,
                "industry": "overall",
                "cpc": board_data.get("avg_cpc_typical"),
                "cpa": board_data.get("avg_cpa_min"),
                "pricing_model": board_data.get("pricing_model") or "",
                "metadata": board_data,
            }
        )

    # Industry benchmarks
    industry_benchmarks: dict[str, Any] = raw.get("industry_benchmarks") or {}
    for industry_name, ind_data in industry_benchmarks.items():
        if not isinstance(ind_data, dict):
            continue
        rows.append(
            {
                "channel": "_industry_benchmark",
                "industry": industry_name,
                "cpc": ind_data.get("cpc_range_low") or ind_data.get("cpc"),
                "cpa": ind_data.get("cpa_range_low") or ind_data.get("cpa"),
                "pricing_model": "benchmark",
                "metadata": ind_data,
            }
        )

    # Seniority benchmarks
    seniority_benchmarks: dict[str, Any] = raw.get("seniority_benchmarks") or {}
    for seniority_name, sen_data in seniority_benchmarks.items():
        if not isinstance(sen_data, dict):
            continue
        rows.append(
            {
                "channel": "_seniority_benchmark",
                "industry": seniority_name,
                "cpc": sen_data.get("cpc_range_low") or sen_data.get("cpc"),
                "cpa": sen_data.get("cpa_range_low") or sen_data.get("cpa"),
                "pricing_model": "benchmark",
                "metadata": sen_data,
            }
        )

    # Company-size benchmarks
    company_size_benchmarks: dict[str, Any] = raw.get("company_size_benchmarks") or {}
    for size_name, size_data in company_size_benchmarks.items():
        if not isinstance(size_data, dict):
            continue
        rows.append(
            {
                "channel": "_company_size_benchmark",
                "industry": size_name,
                "cpc": size_data.get("cpc_range_low") or size_data.get("cpc"),
                "cpa": size_data.get("cpa_range_low") or size_data.get("cpa"),
                "pricing_model": "benchmark",
                "metadata": size_data,
            }
        )

    return rows


def _transform_vendor_profiles() -> list[dict[str, Any]]:
    """Transform channels_db.json into rows for the vendor_profiles table.

    Extracts channel names and categories from the nested structure:
      traditional_channels, non_traditional_channels, and niche sub-dicts.

    Returns:
        List of row dicts ready for upsert.
    """
    raw = _load_json("channels_db.json")
    if raw is None or not isinstance(raw, dict):
        return []

    rows: list[dict[str, Any]] = []
    seen_names: set[str] = set()

    def _add_channels(
        channels: list[str] | list[dict[str, Any]],
        category: str,
        description: str = "",
    ) -> None:
        """Add channel entries, deduplicating by name."""
        for item in channels:
            if isinstance(item, str):
                name = item.strip()
                if not name or name in seen_names:
                    continue
                seen_names.add(name)
                rows.append(
                    {
                        "name": name,
                        "category": category,
                        "description": description,
                        "website_url": "",
                        "pricing_info": {},
                        "features": [],
                    }
                )
            elif isinstance(item, dict):
                name = (item.get("name") or "").strip()
                if not name or name in seen_names:
                    continue
                seen_names.add(name)
                rows.append(
                    {
                        "name": name,
                        "category": category,
                        "description": item.get("description") or description,
                        "website_url": item.get("website_url") or item.get("url") or "",
                        "pricing_info": {
                            k: v
                            for k, v in item.items()
                            if k not in ("name", "description", "website_url", "url")
                        },
                        "features": item.get("features") or [],
                    }
                )

    # traditional_channels: regional_local, global_reach (flat lists of strings)
    traditional: dict[str, Any] = raw.get("traditional_channels") or {}
    _add_channels(
        traditional.get("regional_local") or [],
        "regional_local",
        "Regional/local job board",
    )
    _add_channels(
        traditional.get("global_reach") or [], "global_reach", "Global job board"
    )

    # traditional_channels: niche_by_industry (dict of industry -> list[str])
    niche: dict[str, list[str]] = traditional.get("niche_by_industry") or {}
    for industry, names in niche.items():
        if isinstance(names, list):
            _add_channels(names, f"niche_{industry}", f"Niche board - {industry}")

    # traditional_channels: APAC regional (dict of country -> list[str])
    apac: dict[str, list[str]] = traditional.get("apac_regional") or {}
    for country, names in apac.items():
        if isinstance(names, list):
            _add_channels(names, f"apac_{country}", f"APAC - {country}")

    # traditional_channels: DACH regional (flat list)
    _add_channels(traditional.get("dach_regional") or [], "dach", "DACH regional")

    # traditional_channels: EMEA regional (dict of country -> list[str])
    emea: dict[str, list[str]] = traditional.get("emea_regional") or {}
    for country, names in emea.items():
        if isinstance(names, list):
            _add_channels(names, f"emea_{country}", f"EMEA - {country}")

    # non_traditional_channels
    non_trad: dict[str, Any] = raw.get("non_traditional_channels") or {}
    for sub_category, entries in non_trad.items():
        if isinstance(entries, list):
            _add_channels(
                entries, f"non_trad_{sub_category}", f"Non-traditional - {sub_category}"
            )
        elif isinstance(entries, dict):
            for sub_key, sub_list in entries.items():
                if isinstance(sub_list, list):
                    _add_channels(
                        sub_list,
                        f"non_trad_{sub_category}_{sub_key}",
                        f"Non-traditional - {sub_category}/{sub_key}",
                    )

    # CPA rate benchmarks (store as special vendor entries for reference)
    cpa_benchmarks: dict[str, Any] = raw.get("cpa_rate_benchmarks") or {}
    for region, region_data in cpa_benchmarks.items():
        if isinstance(region_data, dict):
            rows.append(
                {
                    "name": f"_cpa_benchmark_{region}",
                    "category": "cpa_benchmark",
                    "description": f"CPA rate benchmarks for {region}",
                    "website_url": "",
                    "pricing_info": region_data,
                    "features": [],
                }
            )

    return rows


def _transform_supply_global() -> list[dict[str, Any]]:
    """Transform global_supply.json into rows for the supply_repository table.

    Extracts boards per country with their metadata (name, billing, category,
    tier), plus DEI boards, innovative channels, and niche industry boards.

    Returns:
        List of row dicts ready for upsert.
    """
    raw = _load_json("global_supply.json")
    if raw is None or not isinstance(raw, dict):
        return []

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _key(name: str, source: str) -> str:
        return f"{name}|{source}"

    # country_job_boards: dict of country -> {boards: [...], monthly_spend, key_metros}
    country_boards: dict[str, Any] = raw.get("country_job_boards") or {}
    for country, country_data in country_boards.items():
        if not isinstance(country_data, dict):
            continue
        boards: list[dict[str, Any]] = country_data.get("boards") or []
        for board in boards:
            if not isinstance(board, dict):
                continue
            name = (board.get("name") or "").strip()
            if not name:
                continue
            k = _key(name, "global_supply")
            if k in seen:
                continue
            seen.add(k)
            rows.append(
                {
                    "name": name,
                    "source_file": "global_supply.json",
                    "category": board.get("category") or "",
                    "countries": [country],
                    "description": board.get("_verification_notes") or "",
                    "performance": {
                        "billing": board.get("billing") or "",
                        "tier": board.get("tier") or "",
                        "last_verified": board.get("_last_verified") or "",
                    },
                }
            )

    # DEI boards by country
    dei_boards: dict[str, Any] = raw.get("dei_boards_by_country") or {}
    for country, boards_list in dei_boards.items():
        if not isinstance(boards_list, list):
            continue
        for entry in boards_list:
            name = ""
            if isinstance(entry, str):
                name = entry.strip()
            elif isinstance(entry, dict):
                name = (entry.get("name") or "").strip()
            if not name:
                continue
            k = _key(name, "global_supply_dei")
            if k in seen:
                continue
            seen.add(k)
            rows.append(
                {
                    "name": name,
                    "source_file": "global_supply.json",
                    "category": "DEI",
                    "countries": [country],
                    "description": f"DEI board - {country}",
                    "performance": {},
                }
            )

    # Innovative channels 2025
    innovative: dict[str, Any] = raw.get("innovative_channels_2025") or {}
    for channel_type, channel_data in innovative.items():
        if not isinstance(channel_data, dict):
            continue
        name = channel_type.replace("_", " ").title()
        k = _key(name, "global_supply_innovative")
        if k in seen:
            continue
        seen.add(k)
        rows.append(
            {
                "name": name,
                "source_file": "global_supply.json",
                "category": "innovative",
                "countries": ["Global"],
                "description": channel_data.get("description") or "",
                "performance": channel_data,
            }
        )

    # Niche industry boards
    niche: dict[str, Any] = raw.get("niche_industry_boards") or {}
    for industry, board_list in niche.items():
        if not isinstance(board_list, list):
            continue
        for entry in board_list:
            if isinstance(entry, str):
                name = entry.strip()
            elif isinstance(entry, dict):
                name = (entry.get("name") or "").strip()
            else:
                continue
            if not name:
                continue
            k = _key(name, "global_supply_niche")
            if k in seen:
                continue
            seen.add(k)
            rows.append(
                {
                    "name": name,
                    "source_file": "global_supply.json",
                    "category": f"niche_{industry}",
                    "countries": ["Global"],
                    "description": f"Niche - {industry}",
                    "performance": {},
                }
            )

    return rows


def _transform_supply_joveo() -> list[dict[str, Any]]:
    """Transform joveo_publishers.json into rows for the supply_repository table.

    Extracts publishers organized by_category and by_country.

    Returns:
        List of row dicts ready for upsert.
    """
    raw = _load_json("joveo_publishers.json")
    if raw is None or not isinstance(raw, dict):
        return []

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _key(name: str) -> str:
        return f"{name}|joveo"

    # by_category: dict of category -> list[str]
    by_category: dict[str, list[str]] = raw.get("by_category") or {}
    for category, names in by_category.items():
        if not isinstance(names, list):
            continue
        for name in names:
            if not isinstance(name, str):
                continue
            name = name.strip()
            if not name:
                continue
            k = _key(name)
            if k in seen:
                continue
            seen.add(k)
            rows.append(
                {
                    "name": name,
                    "source_file": "joveo_publishers.json",
                    "category": category,
                    "countries": [],
                    "description": f"Joveo publisher - {category}",
                    "performance": {},
                }
            )

    # by_country: dict of country -> list[str]
    by_country: dict[str, list[str]] = raw.get("by_country") or {}
    for country, names in by_country.items():
        if not isinstance(names, list):
            continue
        for name in names:
            if not isinstance(name, str):
                continue
            name = name.strip()
            if not name:
                continue
            k = _key(name)
            if k in seen:
                # Update countries for existing entry
                for row in rows:
                    if (
                        row["name"] == name
                        and row["source_file"] == "joveo_publishers.json"
                    ):
                        if country not in row["countries"]:
                            row["countries"].append(country)
                        break
                continue
            seen.add(k)
            rows.append(
                {
                    "name": name,
                    "source_file": "joveo_publishers.json",
                    "category": "regional",
                    "countries": [country],
                    "description": f"Joveo publisher - {country}",
                    "performance": {},
                }
            )

    # global_publishers: list[str]
    global_pubs: list[str] = raw.get("global_publishers") or []
    for name in global_pubs:
        if not isinstance(name, str):
            continue
        name = name.strip()
        if not name:
            continue
        k = _key(name)
        if k in seen:
            for row in rows:
                if (
                    row["name"] == name
                    and row["source_file"] == "joveo_publishers.json"
                ):
                    if "Global" not in row["countries"]:
                        row["countries"].append("Global")
                    break
            continue
        seen.add(k)
        rows.append(
            {
                "name": name,
                "source_file": "joveo_publishers.json",
                "category": "global",
                "countries": ["Global"],
                "description": "Joveo global publisher",
                "performance": {},
            }
        )

    # publisher_verification: dict of name -> verification_data
    verification: dict[str, Any] = raw.get("publisher_verification") or {}
    for pub_name, ver_data in verification.items():
        if not isinstance(ver_data, dict):
            continue
        # Enrich existing rows with verification data
        for row in rows:
            if (
                row["name"] == pub_name
                and row["source_file"] == "joveo_publishers.json"
            ):
                row["performance"] = {
                    **row["performance"],
                    "verified": True,
                    "verification": ver_data,
                }
                break

    return rows


def _transform_supply_ecosystem() -> list[dict[str, Any]]:
    """Transform supply_ecosystem_intelligence.json into rows for supply_repository.

    Extracts publisher tiers, competitive landscape entries, and top publishers.

    Returns:
        List of row dicts ready for upsert.
    """
    raw = _load_json("supply_ecosystem_intelligence.json")
    if raw is None or not isinstance(raw, dict):
        return []

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _key(name: str) -> str:
        return f"{name}|ecosystem"

    # publisher_tiers: usually dict of tier -> data
    tiers: dict[str, Any] = raw.get("publisher_tiers") or {}
    for tier_name, tier_data in tiers.items():
        if not isinstance(tier_data, dict):
            continue
        publishers: list[Any] = (
            tier_data.get("publishers") or tier_data.get("examples") or []
        )
        for pub in publishers:
            name = ""
            if isinstance(pub, str):
                name = pub.strip()
            elif isinstance(pub, dict):
                name = (pub.get("name") or "").strip()
            if not name:
                continue
            k = _key(name)
            if k in seen:
                continue
            seen.add(k)
            perf: dict[str, Any] = {"tier": tier_name}
            if isinstance(pub, dict):
                perf.update({k2: v2 for k2, v2 in pub.items() if k2 != "name"})
            rows.append(
                {
                    "name": name,
                    "source_file": "supply_ecosystem_intelligence.json",
                    "category": f"tier_{tier_name}",
                    "countries": ["Global"],
                    "description": tier_data.get("description")
                    or f"Publisher tier: {tier_name}",
                    "performance": perf,
                }
            )

    # competitive_landscape: dict of competitor -> data
    competitors: dict[str, Any] = raw.get("competitive_landscape") or {}
    for comp_name, comp_data in competitors.items():
        if not isinstance(comp_data, dict):
            continue
        name = comp_data.get("name") or comp_name
        name = name.strip()
        k = _key(name)
        if k in seen:
            continue
        seen.add(k)
        rows.append(
            {
                "name": name,
                "source_file": "supply_ecosystem_intelligence.json",
                "category": "competitor",
                "countries": ["Global"],
                "description": comp_data.get("description")
                or comp_data.get("overview")
                or "",
                "performance": comp_data,
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Migration orchestrator
# ---------------------------------------------------------------------------


def migrate_knowledge_base(*, dry_run: bool = False) -> int:
    """Migrate all knowledge-base JSON files into the knowledge_base table.

    Args:
        dry_run: If True, preview without writing.

    Returns:
        Total rows upserted.
    """
    total = 0
    for category, filename in KB_CATEGORY_FILE_MAP.items():
        rows = _transform_knowledge_base(category, filename)
        if not rows:
            logger.warning(f"  No rows for knowledge_base/{category} from {filename}")
            continue
        logger.info(
            f"Migrating {filename} -> knowledge_base (category={category})... {len(rows)} rows"
        )
        total += _upsert_batched("knowledge_base", rows, dry_run=dry_run)
    return total


def migrate_channel_benchmarks(*, dry_run: bool = False) -> int:
    """Migrate live_market_data.json into the channel_benchmarks table.

    Args:
        dry_run: If True, preview without writing.

    Returns:
        Total rows upserted.
    """
    rows = _transform_channel_benchmarks()
    if not rows:
        logger.warning("  No rows for channel_benchmarks")
        return 0
    logger.info(
        f"Migrating live_market_data.json -> channel_benchmarks... {len(rows)} rows"
    )
    return _upsert_batched("channel_benchmarks", rows, dry_run=dry_run)


def migrate_vendor_profiles(*, dry_run: bool = False) -> int:
    """Migrate channels_db.json into the vendor_profiles table.

    Args:
        dry_run: If True, preview without writing.

    Returns:
        Total rows upserted.
    """
    rows = _transform_vendor_profiles()
    if not rows:
        logger.warning("  No rows for vendor_profiles")
        return 0
    logger.info(f"Migrating channels_db.json -> vendor_profiles... {len(rows)} rows")
    return _upsert_batched("vendor_profiles", rows, dry_run=dry_run)


def migrate_supply_repository(*, dry_run: bool = False) -> int:
    """Migrate supply files into the supply_repository table.

    Combines data from global_supply.json, joveo_publishers.json, and
    supply_ecosystem_intelligence.json.

    Args:
        dry_run: If True, preview without writing.

    Returns:
        Total rows upserted.
    """
    total = 0

    # global_supply.json
    rows = _transform_supply_global()
    if rows:
        logger.info(
            f"Migrating global_supply.json -> supply_repository... {len(rows)} rows"
        )
        total += _upsert_batched("supply_repository", rows, dry_run=dry_run)

    # joveo_publishers.json
    rows = _transform_supply_joveo()
    if rows:
        logger.info(
            f"Migrating joveo_publishers.json -> supply_repository... {len(rows)} rows"
        )
        total += _upsert_batched("supply_repository", rows, dry_run=dry_run)

    # supply_ecosystem_intelligence.json
    rows = _transform_supply_ecosystem()
    if rows:
        logger.info(
            f"Migrating supply_ecosystem_intelligence.json -> supply_repository... {len(rows)} rows"
        )
        total += _upsert_batched("supply_repository", rows, dry_run=dry_run)

    return total


# ---------------------------------------------------------------------------
# CLI & main
# ---------------------------------------------------------------------------

VALID_TABLES: list[str] = [
    "knowledge_base",
    "channel_benchmarks",
    "vendor_profiles",
    "supply_repository",
]

TABLE_MIGRATORS: dict[str, Any] = {
    "knowledge_base": migrate_knowledge_base,
    "channel_benchmarks": migrate_channel_benchmarks,
    "vendor_profiles": migrate_vendor_profiles,
    "supply_repository": migrate_supply_repository,
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Optional argument list (defaults to sys.argv[1:]).

    Returns:
        Parsed argparse.Namespace.
    """
    parser = argparse.ArgumentParser(
        description="Migrate local JSON data files into Supabase tables.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python migrate_to_supabase.py                        # full migration\n"
            "  python migrate_to_supabase.py --dry-run               # preview\n"
            "  python migrate_to_supabase.py --table knowledge_base  # single table\n"
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
        help="Migrate a single table instead of all tables.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the migration for all (or a single) table.

    Args:
        argv: Optional CLI arguments (for testing).

    Returns:
        Exit code: 0 on success, 1 on configuration error.
    """
    args = parse_args(argv)

    if not SUPABASE_KEY:
        logger.error(
            "SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_ANON_KEY) environment variable not set. "
            "Export it and try again."
        )
        return 1

    logger.info(f"Supabase URL: {SUPABASE_URL}")
    logger.info(f"Data directory: {DATA_DIR}")
    logger.info(f"Dry run: {args.dry_run}")
    logger.info(f"Batch size: {BATCH_SIZE}")
    if args.table:
        logger.info(f"Target table: {args.table}")
    logger.info("-" * 60)

    summary: dict[str, int] = {}

    tables_to_run = [args.table] if args.table else VALID_TABLES
    for table in tables_to_run:
        migrator = TABLE_MIGRATORS[table]
        count = migrator(dry_run=args.dry_run)
        summary[table] = count

    # Summary
    logger.info("-" * 60)
    logger.info("Migration summary:")
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
