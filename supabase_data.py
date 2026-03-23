"""Supabase data access layer for Nova AI Suite.

Provides typed query functions for all Supabase tables with automatic
fallback to local JSON files if Supabase is unavailable. Includes a
simple in-memory cache with 5-minute TTL.

Usage:
    from supabase_data import get_knowledge, get_channel_benchmarks

    # Tries Supabase first, falls back to local JSON
    data = get_knowledge("industry_insights", "benchmarks")
    benchmarks = get_channel_benchmarks(channel="indeed")

Thread-safe, stdlib-only (no third-party dependencies).
"""

from __future__ import annotations

import json
import logging
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SUPABASE_URL = os.environ.get("SUPABASE_URL") or ""
SUPABASE_KEY = os.environ.get("SUPABASE_ANON_KEY") or ""
_DATA_DIR = Path(__file__).parent / "data"
_ENABLED = bool(SUPABASE_URL and SUPABASE_KEY)
_HTTP_TIMEOUT = 3  # seconds -- cache should never slow the pipeline
_SSL_CTX = ssl.create_default_context()

# ---------------------------------------------------------------------------
# In-memory cache (5-minute TTL)
# ---------------------------------------------------------------------------

_CACHE_TTL = 300  # 5 minutes in seconds
_cache: dict[str, tuple[float, Any]] = {}
_cache_lock = threading.Lock()


def _cache_get(key: str) -> Optional[Any]:
    """Get a value from the in-memory cache if not expired.

    Args:
        key: Cache key string.

    Returns:
        Cached value if present and fresh, else None.
    """
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        ts, value = entry
        if time.monotonic() - ts > _CACHE_TTL:
            del _cache[key]
            return None
        return value


def _cache_set(key: str, value: Any) -> None:
    """Store a value in the in-memory cache with current timestamp.

    Args:
        key: Cache key string.
        value: Value to cache.
    """
    with _cache_lock:
        _cache[key] = (time.monotonic(), value)


# ---------------------------------------------------------------------------
# HTTP transport (mirrors supabase_cache.py patterns)
# ---------------------------------------------------------------------------


def _build_headers() -> dict[str, str]:
    """Build standard Supabase REST API headers."""
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _query_supabase(table: str, params: str = "") -> list[dict[str, Any]]:
    """Query Supabase REST API and return list of rows.

    Uses PostgREST query syntax. Returns empty list on any failure.

    Args:
        table: Table name to query.
        params: PostgREST query parameters (e.g., 'category=eq.benchmarks&limit=10').

    Returns:
        List of row dicts from Supabase, or empty list on failure.
    """
    if not _ENABLED:
        return []

    base = SUPABASE_URL.rstrip("/")
    url = f"{base}/rest/v1/{table}"
    if params:
        url = f"{url}?{params}"

    try:
        req = urllib.request.Request(url, method="GET", headers=_build_headers())
        with urllib.request.urlopen(
            req, timeout=_HTTP_TIMEOUT, context=_SSL_CTX
        ) as resp:
            raw = resp.read().decode("utf-8")
            if not raw or not raw.strip():
                return []
            result = json.loads(raw)
            if isinstance(result, list):
                return result
            return []
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8", errors="replace")[:200]
        except Exception:
            pass
        logger.error(f"HTTP {exc.code} querying {table}: {error_body}", exc_info=True)
        return []
    except urllib.error.URLError as exc:
        logger.error(f"URLError querying {table}: {exc.reason}", exc_info=True)
        return []
    except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
        logger.error(f"Error querying {table}: {exc}", exc_info=True)
        return []


# ---------------------------------------------------------------------------
# Local JSON fallback
# ---------------------------------------------------------------------------

# Maps knowledge_base categories to their source JSON files
_KB_CATEGORY_FILE_MAP: dict[str, str] = {
    "industry_insights": "recruitment_industry_knowledge.json",
    "platform_data": "platform_intelligence_deep.json",
    "benchmarks": "recruitment_benchmarks_deep.json",
    "strategy": "recruitment_strategy_intelligence.json",
    "regional": "regional_hiring_intelligence.json",
    "supply": "supply_ecosystem_intelligence.json",
    "trends": "workforce_trends_intelligence.json",
    "white_papers": "industry_white_papers.json",
    "joveo_benchmarks": "joveo_2026_benchmarks.json",
    "google_ads_benchmarks": "google_ads_2025_benchmarks.json",
    "external_benchmarks": "external_benchmarks_2025.json",
    "client_plans": "client_media_plans_kb.json",
}


def _load_local_json(filename: str) -> Optional[dict[str, Any]]:
    """Load a JSON file from the data directory.

    Args:
        filename: JSON filename in data/.

    Returns:
        Parsed dict, or None on failure.
    """
    filepath = _DATA_DIR / filename
    if not filepath.exists():
        return None
    try:
        return json.loads(filepath.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        logger.error(f"Failed to load local file {filepath}: {exc}", exc_info=True)
        return None


def _fallback_knowledge(category: str, key: str = "") -> dict[str, Any]:
    """Fall back to local JSON for knowledge_base queries.

    Args:
        category: Knowledge base category.
        key: Optional top-level key within the JSON file.

    Returns:
        Matching data dict, or empty dict.
    """
    filename = _KB_CATEGORY_FILE_MAP.get(category)
    if not filename:
        return {}
    data = _load_local_json(filename)
    if data is None:
        return {}
    if key:
        value = data.get(key)
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        return {"value": value}
    return data


def _fallback_channel_benchmarks(
    channel: str = "", industry: str = ""
) -> list[dict[str, Any]]:
    """Fall back to local JSON for channel benchmark queries.

    Args:
        channel: Optional channel name filter.
        industry: Optional industry filter.

    Returns:
        List of benchmark dicts.
    """
    data = _load_local_json("live_market_data.json")
    if data is None:
        return []

    results: list[dict[str, Any]] = []
    for ch_name, ch_data in (data.get("job_boards") or {}).items():
        if not isinstance(ch_data, dict):
            continue
        if channel and ch_name.lower() != channel.lower():
            continue
        results.append(
            {
                "channel": ch_name,
                "industry": "overall",
                "cpc": ch_data.get("avg_cpc_typical"),
                "cpa": ch_data.get("avg_cpa_min"),
                "pricing_model": ch_data.get("pricing_model") or "",
                "metadata": ch_data,
            }
        )
    return results


def _fallback_salary_data(role: str = "", location: str = "") -> list[dict[str, Any]]:
    """Fall back to local JSON for salary data queries.

    No local salary JSON exists yet, so this returns empty.

    Args:
        role: Optional role filter.
        location: Optional location filter.

    Returns:
        Empty list (no local salary data).
    """
    return []


def _fallback_compliance_rules(
    rule_type: str = "", jurisdiction: str = ""
) -> list[dict[str, Any]]:
    """Fall back to local JSON for compliance rule queries.

    No local compliance JSON exists yet, so this returns empty.

    Args:
        rule_type: Optional rule type filter.
        jurisdiction: Optional jurisdiction filter.

    Returns:
        Empty list (no local compliance data).
    """
    return []


def _fallback_market_trends(
    category: str = "", limit: int = 10
) -> list[dict[str, Any]]:
    """Fall back to local JSON for market trend queries.

    Args:
        category: Optional trend category filter.
        limit: Max results.

    Returns:
        List of trend dicts.
    """
    data = _load_local_json("live_market_data.json")
    if data is None:
        return []

    results: list[dict[str, Any]] = []
    for source in data.get("sources") or []:
        if not isinstance(source, dict):
            continue
        results.append(
            {
                "title": source.get("name") or "Market data source",
                "source": source.get("name") or "",
                "url": source.get("url") or "",
                "category": "cpc_trends",
                "metadata": source,
            }
        )
        if len(results) >= limit:
            break
    return results


def _fallback_vendor_profiles(category: str = "") -> list[dict[str, Any]]:
    """Fall back to local JSON for vendor profile queries.

    No separate vendor JSON exists, returns empty.

    Args:
        category: Optional category filter.

    Returns:
        Empty list.
    """
    return []


# ---------------------------------------------------------------------------
# Public query functions
# ---------------------------------------------------------------------------


def get_knowledge(category: str, key: str = "") -> dict[str, Any]:
    """Get knowledge base entry by category and optional key.

    Tries Supabase first (3s timeout), falls back to local JSON,
    caches results in memory for 5 minutes.

    Args:
        category: Knowledge base category (e.g., 'industry_insights', 'benchmarks').
        key: Optional lookup key within the category.

    Returns:
        Data dict matching the query, or empty dict on failure.
    """
    cache_key = f"kb:{category}:{key}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # Try Supabase
    params_parts = [f"category=eq.{urllib.parse.quote(category, safe='')}"]
    if key:
        params_parts.append(f"key=eq.{urllib.parse.quote(key, safe='')}")
    params_parts.append("select=key,data")
    params_parts.append("limit=100")

    rows = _query_supabase("knowledge_base", "&".join(params_parts))
    if rows:
        if key:
            # Single key lookup: return the data blob
            result = rows[0].get("data") or {}
            if isinstance(result, dict):
                _cache_set(cache_key, result)
                return result
            _cache_set(cache_key, {"value": result})
            return {"value": result}
        else:
            # All keys in category: return merged dict
            merged: dict[str, Any] = {}
            for row in rows:
                row_key = row.get("key") or "_unknown"
                merged[row_key] = row.get("data")
            _cache_set(cache_key, merged)
            return merged

    # Fallback to local JSON
    result = _fallback_knowledge(category, key)
    if result:
        _cache_set(cache_key, result)
    return result


def get_channel_benchmarks(
    channel: str = "", industry: str = ""
) -> list[dict[str, Any]]:
    """Get channel benchmark data, optionally filtered.

    Tries Supabase first, falls back to local JSON.

    Args:
        channel: Optional channel name (e.g., 'indeed', 'linkedin').
        industry: Optional industry (e.g., 'technology', 'healthcare').

    Returns:
        List of benchmark dicts, or empty list.
    """
    cache_key = f"bench:{channel}:{industry}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    params_parts = ["select=*"]
    if channel:
        params_parts.append(f"channel=eq.{urllib.parse.quote(channel, safe='')}")
    if industry:
        params_parts.append(f"industry=eq.{urllib.parse.quote(industry, safe='')}")
    params_parts.append("limit=100")

    rows = _query_supabase("channel_benchmarks", "&".join(params_parts))
    if rows:
        _cache_set(cache_key, rows)
        return rows

    result = _fallback_channel_benchmarks(channel, industry)
    if result:
        _cache_set(cache_key, result)
    return result


def get_salary_data(role: str = "", location: str = "") -> list[dict[str, Any]]:
    """Get salary data, optionally filtered by role and location.

    Tries Supabase first, falls back to local JSON.

    Args:
        role: Optional role/job title filter.
        location: Optional location filter.

    Returns:
        List of salary data dicts, or empty list.
    """
    cache_key = f"salary:{role}:{location}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    params_parts = ["select=*"]
    if role:
        params_parts.append(f"role=ilike.*{urllib.parse.quote(role, safe='')}*")
    if location:
        params_parts.append(f"location=ilike.*{urllib.parse.quote(location, safe='')}*")
    params_parts.append("limit=50")

    rows = _query_supabase("salary_data", "&".join(params_parts))
    if rows:
        _cache_set(cache_key, rows)
        return rows

    result = _fallback_salary_data(role, location)
    if result:
        _cache_set(cache_key, result)
    return result


def get_compliance_rules(
    rule_type: str = "", jurisdiction: str = ""
) -> list[dict[str, Any]]:
    """Get compliance rules, optionally filtered.

    Tries Supabase first, falls back to local JSON.

    Args:
        rule_type: Optional rule type (e.g., 'pay_transparency', 'bias_language').
        jurisdiction: Optional jurisdiction (e.g., 'california', 'eu').

    Returns:
        List of compliance rule dicts, or empty list.
    """
    cache_key = f"compliance:{rule_type}:{jurisdiction}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    params_parts = ["select=*"]
    if rule_type:
        params_parts.append(f"rule_type=eq.{urllib.parse.quote(rule_type, safe='')}")
    if jurisdiction:
        params_parts.append(
            f"jurisdiction=eq.{urllib.parse.quote(jurisdiction, safe='')}"
        )
    params_parts.append("status=eq.active")
    params_parts.append("limit=100")

    rows = _query_supabase("compliance_rules", "&".join(params_parts))
    if rows:
        _cache_set(cache_key, rows)
        return rows

    result = _fallback_compliance_rules(rule_type, jurisdiction)
    if result:
        _cache_set(cache_key, result)
    return result


def get_market_trends(category: str = "", limit: int = 10) -> list[dict[str, Any]]:
    """Get recent market trends.

    Tries Supabase first, falls back to local JSON.

    Args:
        category: Optional trend category (e.g., 'cpc_trends', 'hiring_volume').
        limit: Maximum number of trends to return (default 10).

    Returns:
        List of market trend dicts, or empty list.
    """
    cache_key = f"trends:{category}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    params_parts = ["select=*", "order=scraped_at.desc"]
    if category:
        params_parts.append(f"category=eq.{urllib.parse.quote(category, safe='')}")
    params_parts.append(f"limit={limit}")

    rows = _query_supabase("market_trends", "&".join(params_parts))
    if rows:
        _cache_set(cache_key, rows)
        return rows

    result = _fallback_market_trends(category, limit)
    if result:
        _cache_set(cache_key, result)
    return result


def get_vendor_profiles(category: str = "") -> list[dict[str, Any]]:
    """Get vendor profiles, optionally filtered by category.

    Tries Supabase first, falls back to local JSON.

    Args:
        category: Optional vendor category (e.g., 'major_job_board', 'niche').

    Returns:
        List of vendor profile dicts, or empty list.
    """
    cache_key = f"vendor:{category}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    params_parts = ["select=*"]
    if category:
        params_parts.append(f"category=eq.{urllib.parse.quote(category, safe='')}")
    params_parts.append("limit=100")

    rows = _query_supabase("vendor_profiles", "&".join(params_parts))
    if rows:
        _cache_set(cache_key, rows)
        return rows

    result = _fallback_vendor_profiles(category)
    if result:
        _cache_set(cache_key, result)
    return result


def get_supply_repository(
    category: str = "", country: str = ""
) -> list[dict[str, Any]]:
    """Get supply repository publishers, optionally filtered.

    Tries Supabase first, falls back to local JSON.

    Args:
        category: Optional publisher category filter.
        country: Optional country filter (checks JSONB countries array).

    Returns:
        List of publisher dicts, or empty list.
    """
    cache_key = f"supply:{category}:{country}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    params_parts = ["select=*"]
    if category:
        params_parts.append(f"category=ilike.*{urllib.parse.quote(category, safe='')}*")
    if country:
        params_parts.append(
            f"countries=cs.[\"{urllib.parse.quote(country, safe='')}\"]"
        )
    params_parts.append("limit=200")

    rows = _query_supabase("supply_repository", "&".join(params_parts))
    if rows:
        _cache_set(cache_key, rows)
        return rows

    # Fallback: load from local files
    result: list[dict[str, Any]] = []
    for filename in ("joveo_publishers.json", "global_supply.json", "channels_db.json"):
        data = _load_local_json(filename)
        if data and isinstance(data, dict):
            result.append({"source_file": filename, "data": data})

    if result:
        _cache_set(cache_key, result)
    return result


def clear_cache() -> int:
    """Clear all entries from the in-memory cache.

    Returns:
        Number of entries cleared.
    """
    with _cache_lock:
        count = len(_cache)
        _cache.clear()
        return count


def cache_info() -> dict[str, Any]:
    """Return cache statistics for monitoring.

    Returns:
        Dict with keys: size, enabled, supabase_url_prefix.
    """
    with _cache_lock:
        size = len(_cache)
    return {
        "size": size,
        "enabled": _ENABLED,
        "supabase_url_prefix": (
            SUPABASE_URL[:40] + "..." if len(SUPABASE_URL) > 40 else SUPABASE_URL
        ),
        "cache_ttl_seconds": _CACHE_TTL,
        "http_timeout_seconds": _HTTP_TIMEOUT,
    }


# ---------------------------------------------------------------------------
# Module initialization
# ---------------------------------------------------------------------------

if _ENABLED:
    logger.info(
        f"[supabase_data] Enabled: {SUPABASE_URL[:40]}..."
        if len(SUPABASE_URL) > 40
        else f"[supabase_data] Enabled: {SUPABASE_URL}"
    )
else:
    logger.info(
        "[supabase_data] Disabled (SUPABASE_URL or SUPABASE_ANON_KEY not set), using local JSON fallback"
    )
