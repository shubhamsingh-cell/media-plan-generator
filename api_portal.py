#!/usr/bin/env python3
"""
api_portal.py -- Self-Service API Portal Backend

Provides REST API access to the Media Plan Generator with:
  - API key management (generate, revoke, list)
  - Tiered rate limiting (Free=100/day, Pro=1000/day, Enterprise=unlimited)
  - Usage tracking and analytics per key
  - OpenAPI 3.0 / Swagger spec generation
  - Request/response logging with timing

Thread-safe storage using JSON file with file locking.
All operations gracefully degrade when dependencies are unavailable.

Depends on (lazy-imported):
  - research (INDUSTRY_LABEL_MAP variants)
  - shared_utils (INDUSTRY_LABEL_MAP)
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import secrets
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# Lazy Imports -- graceful fallback when modules are unavailable
# ═══════════════════════════════════════════════════════════════════════════════

_shared_utils = None
_HAS_SHARED_UTILS = False


def _lazy_shared_utils():
    global _shared_utils, _HAS_SHARED_UTILS
    if _shared_utils is not None:
        return _shared_utils
    try:
        import shared_utils as _mod

        _shared_utils = _mod
        _HAS_SHARED_UTILS = True
        return _mod
    except ImportError:
        logger.warning("shared_utils not available; using fallback industry labels")
        _HAS_SHARED_UTILS = False
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Industry label map (import from shared_utils or define fallback)
# ═══════════════════════════════════════════════════════════════════════════════

try:
    from shared_utils import INDUSTRY_LABEL_MAP
except ImportError:
    INDUSTRY_LABEL_MAP = {
        "healthcare_medical": "Healthcare & Medical",
        "blue_collar_trades": "Blue Collar / Skilled Trades",
        "tech_engineering": "Technology & Engineering",
        "general_entry_level": "General / Entry-Level",
        "finance_banking": "Finance & Banking",
        "retail_consumer": "Retail & Consumer",
        "logistics_supply_chain": "Logistics & Supply Chain",
        "hospitality_travel": "Hospitality & Travel",
        "construction_real_estate": "Construction & Real Estate",
        "education": "Education",
        "aerospace_defense": "Aerospace & Defense",
        "pharma_biotech": "Pharma & Biotech",
        "energy_utilities": "Energy & Utilities",
        "insurance": "Insurance",
        "telecommunications": "Telecommunications",
        "automotive": "Automotive & Manufacturing",
        "food_beverage": "Food & Beverage",
        "media_entertainment": "Media & Entertainment",
        "legal_services": "Legal Services",
        "mental_health": "Mental Health & Behavioral",
        "maritime_marine": "Maritime & Marine",
        "military_recruitment": "Military Recruitment",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════════

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
API_KEYS_FILE = os.path.join(DATA_DIR, "api_keys.json")

# Rate limit tiers
RATE_LIMIT_TIERS = {
    "free": {
        "requests_per_day": 100,
        "label": "Free",
        "price": "$0/month",
        "features": [
            "100 requests/day",
            "Basic media plan generation",
            "JSON responses",
            "Community support",
        ],
    },
    "pro": {
        "requests_per_day": 1000,
        "label": "Pro",
        "price": "$49/month",
        "features": [
            "1,000 requests/day",
            "Full media plan generation",
            "Excel & PPT exports",
            "Priority support",
            "Webhook notifications",
            "Batch processing",
        ],
    },
    "enterprise": {
        "requests_per_day": -1,
        "label": "Enterprise",
        "price": "Custom",
        "features": [
            "Unlimited requests",
            "Full media plan generation",
            "Excel & PPT exports",
            "Dedicated support",
            "Custom integrations",
            "SLA guarantee",
            "On-premise deployment option",
            "SSO/SAML",
        ],
    },
}

# API key prefix for identification
API_KEY_PREFIX = "mpg_"
API_KEY_LENGTH = 40  # Total length including prefix

# File lock for thread-safe JSON operations
_file_lock = threading.Lock()

# In-memory cache for rate limiting (reset daily)
_rate_limit_cache: Dict[str, Dict[str, Any]] = {}
_rate_limit_lock = threading.Lock()

# Usage log buffer (flushed periodically)
_usage_buffer: List[Dict[str, Any]] = []
_usage_buffer_lock = threading.Lock()
_USAGE_FLUSH_INTERVAL = 30  # seconds
_last_flush_time = time.time()

# Max workers for concurrent operations
_MAX_WORKERS = 6


# ═══════════════════════════════════════════════════════════════════════════════
# 1. STORAGE -- Thread-safe JSON File Operations
# ═══════════════════════════════════════════════════════════════════════════════


def _ensure_data_dir():
    """Ensure data directory exists."""
    os.makedirs(DATA_DIR, exist_ok=True)


def _load_api_keys() -> Dict[str, Any]:
    """Load API keys from JSON file. Thread-safe."""
    _ensure_data_dir()
    if not os.path.exists(API_KEYS_FILE):
        return {
            "keys": {},
            "usage_log": [],
            "metadata": {"created": datetime.utcnow().isoformat() + "Z"},
        }
    try:
        with open(API_KEYS_FILE, "r") as f:
            data = json.load(f)
        # Ensure required keys exist
        if "keys" not in data:
            data["keys"] = {}
        if "usage_log" not in data:
            data["usage_log"] = []
        if "metadata" not in data:
            data["metadata"] = {}
        return data
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to load API keys file: %s", exc)
        return {
            "keys": {},
            "usage_log": [],
            "metadata": {"created": datetime.utcnow().isoformat() + "Z"},
        }


def _save_api_keys(data: Dict[str, Any]) -> bool:
    """Save API keys to JSON file. Thread-safe."""
    _ensure_data_dir()
    try:
        # Trim usage log to last 10,000 entries to prevent unbounded growth
        if len(data.get("usage_log") or []) > 10000:
            data["usage_log"] = data["usage_log"][-10000:]
        tmp_path = API_KEYS_FILE + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        os.replace(tmp_path, API_KEYS_FILE)
        return True
    except OSError as exc:
        logger.error("Failed to save API keys file: %s", exc)
        return False


# ═══════════════════════════════════════════════════════════════════════════════
# 2. API KEY MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════


def generate_api_key(tier: str, owner: str) -> Dict[str, Any]:
    """Create a new API key for the given tier and owner.

    Args:
        tier: One of 'free', 'pro', 'enterprise'
        owner: Name or email of the key owner

    Returns:
        Dict with key, tier, owner, created_at, and tier details.
        The full API key is only returned at creation time.
    """
    if not owner or not owner.strip():
        return {"error": "Owner name is required", "success": False}

    owner = owner.strip()
    tier = tier.lower().strip() if tier else "free"
    if tier not in RATE_LIMIT_TIERS:
        return {
            "error": f"Invalid tier: {tier}. Must be one of: {', '.join(RATE_LIMIT_TIERS.keys())}",
            "success": False,
        }

    # Generate a secure random key
    raw_key = secrets.token_urlsafe(API_KEY_LENGTH - len(API_KEY_PREFIX))
    api_key = API_KEY_PREFIX + raw_key[: API_KEY_LENGTH - len(API_KEY_PREFIX)]

    # Hash the key for storage (we store the hash, return the key once)
    key_hash = _hash_key(api_key)

    now = datetime.utcnow().isoformat() + "Z"
    key_record = {
        "key_hash": key_hash,
        "key_prefix": api_key[:12] + "...",
        "tier": tier,
        "owner": owner,
        "created_at": now,
        "last_used": None,
        "is_active": True,
        "total_requests": 0,
        "total_errors": 0,
        "endpoints_used": {},
    }

    with _file_lock:
        data = _load_api_keys()
        data["keys"][key_hash] = key_record
        data["metadata"]["last_key_created"] = now
        data["metadata"]["total_keys"] = len(data["keys"])
        _save_api_keys(data)

    tier_info = RATE_LIMIT_TIERS[tier]
    return {
        "success": True,
        "api_key": api_key,
        "key_prefix": key_record["key_prefix"],
        "tier": tier,
        "tier_label": tier_info["label"],
        "owner": owner,
        "created_at": now,
        "rate_limit": tier_info["requests_per_day"],
        "rate_limit_label": (
            "Unlimited"
            if tier_info["requests_per_day"] == -1
            else f"{tier_info['requests_per_day']}/day"
        ),
        "message": "Store this API key securely. It will not be shown again.",
    }


def validate_api_key(key: str) -> Dict[str, Any]:
    """Validate an API key and return its tier info.

    Args:
        key: The API key string to validate

    Returns:
        Dict with valid (bool), tier, owner, rate_limit, and usage info.
    """
    if not key or not key.strip():
        return {"valid": False, "error": "API key is required"}

    key = key.strip()
    if not key.startswith(API_KEY_PREFIX):
        return {"valid": False, "error": "Invalid API key format"}

    key_hash = _hash_key(key)

    with _file_lock:
        data = _load_api_keys()

    record = data["keys"].get(key_hash)
    if not record:
        return {"valid": False, "error": "API key not found"}

    if not record.get("is_active", True):
        return {"valid": False, "error": "API key has been revoked"}

    tier = record.get("tier", "free")
    tier_info = RATE_LIMIT_TIERS.get(tier, RATE_LIMIT_TIERS["free"])

    return {
        "valid": True,
        "tier": tier,
        "tier_label": tier_info["label"],
        "owner": record.get("owner", "Unknown"),
        "created_at": record.get("created_at") or "",
        "last_used": record.get("last_used"),
        "total_requests": record.get("total_requests") or 0,
        "rate_limit": tier_info["requests_per_day"],
        "rate_limit_label": (
            "Unlimited"
            if tier_info["requests_per_day"] == -1
            else f"{tier_info['requests_per_day']}/day"
        ),
        "key_prefix": record.get("key_prefix", key[:12] + "..."),
    }


def check_rate_limit(key: str) -> bool:
    """Check if the API key is within its rate limit.

    Args:
        key: The API key string

    Returns:
        True if within rate limit, False if exceeded.
    """
    if not key:
        return False

    key_hash = _hash_key(key)

    with _file_lock:
        data = _load_api_keys()

    record = data["keys"].get(key_hash)
    if not record or not record.get("is_active", True):
        return False

    tier = record.get("tier", "free")
    tier_info = RATE_LIMIT_TIERS.get(tier, RATE_LIMIT_TIERS["free"])
    max_requests = tier_info["requests_per_day"]

    # Enterprise tier has unlimited requests
    if max_requests == -1:
        return True

    # Check in-memory rate limit cache
    today = datetime.utcnow().strftime("%Y-%m-%d")

    with _rate_limit_lock:
        if key_hash not in _rate_limit_cache:
            _rate_limit_cache[key_hash] = {"date": today, "count": 0}

        cache = _rate_limit_cache[key_hash]

        # Reset counter if it's a new day
        if cache["date"] != today:
            cache["date"] = today
            cache["count"] = 0

        if cache["count"] >= max_requests:
            return False

        return True


def _increment_rate_limit(key_hash: str):
    """Increment the rate limit counter for a key."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    with _rate_limit_lock:
        if key_hash not in _rate_limit_cache:
            _rate_limit_cache[key_hash] = {"date": today, "count": 0}
        cache = _rate_limit_cache[key_hash]
        if cache["date"] != today:
            cache["date"] = today
            cache["count"] = 0
        cache["count"] += 1


def record_usage(
    key: str,
    endpoint: str,
    response_time_ms: float,
    status_code: int = 200,
    error: str = "",
) -> None:
    """Record API usage for a key.

    Args:
        key: The API key string
        endpoint: The API endpoint called
        response_time_ms: Response time in milliseconds
        status_code: HTTP status code
        error: Error message if any
    """
    if not key:
        return

    key_hash = _hash_key(key)
    now = datetime.utcnow().isoformat() + "Z"

    # Increment rate limit counter
    _increment_rate_limit(key_hash)

    # Buffer the usage record
    usage_entry = {
        "key_hash": key_hash,
        "endpoint": endpoint,
        "timestamp": now,
        "response_time_ms": round(response_time_ms, 1),
        "status_code": status_code,
        "error": error,
    }

    with _usage_buffer_lock:
        _usage_buffer.append(usage_entry)

    # Flush buffer periodically
    _maybe_flush_usage()


def _maybe_flush_usage():
    """Flush usage buffer to disk if enough time has passed."""
    global _last_flush_time
    now = time.time()
    if now - _last_flush_time < _USAGE_FLUSH_INTERVAL:
        return

    _last_flush_time = now

    with _usage_buffer_lock:
        if not _usage_buffer:
            return
        entries = list(_usage_buffer)
        _usage_buffer.clear()

    # Update key records and usage log
    with _file_lock:
        data = _load_api_keys()

        for entry in entries:
            kh = entry["key_hash"]
            if kh in data["keys"]:
                rec = data["keys"][kh]
                rec["total_requests"] = rec.get("total_requests") or 0 + 1
                rec["last_used"] = entry["timestamp"]
                if entry.get("error"):
                    rec["total_errors"] = rec.get("total_errors") or 0 + 1
                # Track endpoint usage
                ep = entry["endpoint"]
                if "endpoints_used" not in rec:
                    rec["endpoints_used"] = {}
                rec["endpoints_used"][ep] = rec["endpoints_used"].get(ep, 0) + 1

        data["usage_log"].extend(entries)
        _save_api_keys(data)


def get_usage_stats(key: str) -> Dict[str, Any]:
    """Get usage analytics for an API key.

    Args:
        key: The API key string

    Returns:
        Dict with usage statistics including daily counts, top endpoints,
        average response time, error rate, and rate limit status.
    """
    if not key:
        return {"error": "API key is required"}

    key_hash = _hash_key(key)

    # Flush any pending usage data first
    _force_flush_usage()

    with _file_lock:
        data = _load_api_keys()

    record = data["keys"].get(key_hash)
    if not record:
        return {"error": "API key not found"}

    tier = record.get("tier", "free")
    tier_info = RATE_LIMIT_TIERS.get(tier, RATE_LIMIT_TIERS["free"])

    # Compute daily usage from usage log
    usage_log = data.get("usage_log") or []
    key_entries = [e for e in usage_log if e.get("key_hash") == key_hash]

    # Last 30 days daily breakdown
    daily_usage: Dict[str, int] = defaultdict(int)
    daily_errors: Dict[str, int] = defaultdict(int)
    response_times: List[float] = []
    endpoint_counts: Dict[str, int] = defaultdict(int)
    hourly_distribution: Dict[int, int] = defaultdict(int)

    cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat() + "Z"

    for entry in key_entries:
        ts = entry.get("timestamp") or ""
        if ts < cutoff:
            continue

        day = ts[:10]
        daily_usage[day] += 1

        if entry.get("error"):
            daily_errors[day] += 1

        rt = entry.get("response_time_ms") or 0
        if rt > 0:
            response_times.append(rt)

        ep = entry.get("endpoint", "unknown")
        endpoint_counts[ep] += 1

        # Parse hour
        try:
            hour = int(ts[11:13])
            hourly_distribution[hour] += 1
        except (ValueError, IndexError):
            pass

    # Sort daily usage
    sorted_days = sorted(daily_usage.keys())

    # Current rate limit status
    today = datetime.utcnow().strftime("%Y-%m-%d")
    today_count = daily_usage.get(today, 0)
    max_requests = tier_info["requests_per_day"]

    # Check in-memory cache for most accurate today count
    with _rate_limit_lock:
        if (
            key_hash in _rate_limit_cache
            and _rate_limit_cache[key_hash]["date"] == today
        ):
            today_count = max(today_count, _rate_limit_cache[key_hash]["count"])

    avg_response_time = (
        round(sum(response_times) / len(response_times), 1) if response_times else 0
    )
    p95_response_time = round(
        (
            sorted(response_times)[int(len(response_times) * 0.95)]
            if response_times
            else 0
        ),
        1,
    )

    total_requests = record.get("total_requests") or 0
    total_errors = record.get("total_errors") or 0
    error_rate = (
        round((total_errors / total_requests * 100), 2) if total_requests > 0 else 0
    )

    # Top endpoints
    top_endpoints = sorted(endpoint_counts.items(), key=lambda x: x[1], reverse=True)[
        :10
    ]

    return {
        "key_prefix": record.get("key_prefix") or "",
        "tier": tier,
        "tier_label": tier_info["label"],
        "owner": record.get("owner", "Unknown"),
        "created_at": record.get("created_at") or "",
        "last_used": record.get("last_used"),
        "total_requests": total_requests,
        "total_errors": total_errors,
        "error_rate_pct": error_rate,
        "avg_response_time_ms": avg_response_time,
        "p95_response_time_ms": p95_response_time,
        "rate_limit": {
            "max_per_day": max_requests,
            "used_today": today_count,
            "remaining": max(0, max_requests - today_count) if max_requests > 0 else -1,
            "pct_used": (
                round((today_count / max_requests * 100), 1) if max_requests > 0 else 0
            ),
        },
        "daily_usage": [
            {"date": d, "requests": daily_usage[d], "errors": daily_errors.get(d, 0)}
            for d in sorted_days[-30:]
        ],
        "top_endpoints": [{"endpoint": ep, "count": cnt} for ep, cnt in top_endpoints],
        "hourly_distribution": [
            {"hour": h, "count": hourly_distribution.get(h, 0)} for h in range(24)
        ],
    }


def _force_flush_usage():
    """Force flush all buffered usage data to disk."""
    global _last_flush_time
    _last_flush_time = 0  # Force flush
    _maybe_flush_usage()


def list_api_keys(admin: bool = False) -> List[Dict[str, Any]]:
    """List all API keys (admin view) or summary info.

    Args:
        admin: If True, return full details; otherwise, return summary only.

    Returns:
        List of key info dicts.
    """
    with _file_lock:
        data = _load_api_keys()

    keys_list = []
    for key_hash, record in data.get("keys", {}).items():
        tier = record.get("tier", "free")
        tier_info = RATE_LIMIT_TIERS.get(tier, RATE_LIMIT_TIERS["free"])

        entry = {
            "key_prefix": record.get("key_prefix", "mpg_***..."),
            "tier": tier,
            "tier_label": tier_info["label"],
            "owner": record.get("owner", "Unknown"),
            "created_at": record.get("created_at") or "",
            "last_used": record.get("last_used"),
            "is_active": record.get("is_active", True),
            "total_requests": record.get("total_requests") or 0,
        }

        if admin:
            entry["key_hash"] = key_hash
            entry["total_errors"] = record.get("total_errors") or 0
            entry["endpoints_used"] = record.get("endpoints_used", {})

        keys_list.append(entry)

    # Sort by creation date descending
    keys_list.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return keys_list


def revoke_api_key(key: str) -> bool:
    """Revoke an API key, disabling all further requests.

    Args:
        key: The API key string to revoke

    Returns:
        True if key was found and revoked, False otherwise.
    """
    if not key:
        return False

    key_hash = _hash_key(key)

    with _file_lock:
        data = _load_api_keys()
        if key_hash not in data["keys"]:
            return False
        data["keys"][key_hash]["is_active"] = False
        data["keys"][key_hash]["revoked_at"] = datetime.utcnow().isoformat() + "Z"
        _save_api_keys(data)

    # Clear rate limit cache
    with _rate_limit_lock:
        _rate_limit_cache.pop(key_hash, None)

    return True


def revoke_api_key_by_prefix(key_prefix: str) -> bool:
    """Revoke an API key by its display prefix (for admin/UI use).

    Args:
        key_prefix: The key_prefix string (e.g., 'mpg_abc123...')

    Returns:
        True if a matching key was found and revoked.
    """
    if not key_prefix:
        return False

    with _file_lock:
        data = _load_api_keys()
        for key_hash, record in data["keys"].items():
            if record.get("key_prefix") == key_prefix and record.get("is_active", True):
                record["is_active"] = False
                record["revoked_at"] = datetime.utcnow().isoformat() + "Z"
                _save_api_keys(data)
                # Clear rate limit cache
                with _rate_limit_lock:
                    _rate_limit_cache.pop(key_hash, None)
                return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# 3. OPENAPI SPEC GENERATION
# ═══════════════════════════════════════════════════════════════════════════════


def get_openapi_spec() -> Dict[str, Any]:
    """Generate a complete OpenAPI 3.0 specification for all API endpoints.

    Returns:
        OpenAPI 3.0 spec dict describing all available endpoints.
    """
    spec = {
        "openapi": "3.0.3",
        "info": {
            "title": "Nova AI Media Plan Generator API",
            "description": (
                "REST API for generating AI-powered recruitment advertising media plans. "
                "Provides media plan generation, competitive intelligence, talent heatmaps, "
                "market pulse analysis, and more. Powered by Nova AI Suite."
            ),
            "version": "1.0.0",
            "contact": {
                "name": "Nova AI Suite Support",
                "url": "https://media-plan-generator.onrender.com",
            },
            "license": {
                "name": "Proprietary",
            },
        },
        "servers": [
            {
                "url": "https://media-plan-generator.onrender.com",
                "description": "Production server",
            },
            {
                "url": "http://localhost:8080",
                "description": "Local development",
            },
        ],
        "security": [
            {"ApiKeyHeader": []},
        ],
        "components": {
            "securitySchemes": {
                "ApiKeyHeader": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-API-Key",
                    "description": "API key for authentication. Get one from the API Portal.",
                },
            },
            "schemas": {
                "MediaPlanRequest": {
                    "type": "object",
                    "required": ["company_name", "industry", "roles", "locations"],
                    "properties": {
                        "company_name": {
                            "type": "string",
                            "description": "Company name",
                            "example": "Acme Corp",
                        },
                        "industry": {
                            "type": "string",
                            "description": "Industry key",
                            "example": "tech_engineering",
                            "enum": list(INDUSTRY_LABEL_MAP.keys()),
                        },
                        "roles": {
                            "type": "string",
                            "description": "Target roles (comma-separated)",
                            "example": "Software Engineer, Product Manager",
                        },
                        "locations": {
                            "type": "string",
                            "description": "Target locations (comma-separated)",
                            "example": "San Francisco, New York, Austin",
                        },
                        "budget": {
                            "type": "number",
                            "description": "Total budget in USD",
                            "example": 50000,
                            "default": 50000,
                        },
                        "campaign_duration_weeks": {
                            "type": "integer",
                            "description": "Campaign duration in weeks",
                            "example": 12,
                            "default": 12,
                        },
                        "num_openings": {
                            "type": "integer",
                            "description": "Number of open positions",
                            "example": 5,
                            "default": 5,
                        },
                    },
                },
                "MediaPlanResponse": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "enum": ["success", "partial", "error"],
                        },
                        "company_name": {"type": "string"},
                        "industry": {"type": "string"},
                        "industry_label": {"type": "string"},
                        "total_budget": {"type": "number"},
                        "channel_allocations": {
                            "type": "array",
                            "items": {"type": "object"},
                        },
                        "recommendations": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "generation_time_ms": {"type": "integer"},
                    },
                },
                "CompetitiveAnalysisRequest": {
                    "type": "object",
                    "required": ["company_name", "competitors"],
                    "properties": {
                        "company_name": {"type": "string", "example": "Netflix"},
                        "competitors": {
                            "type": "array",
                            "items": {"type": "string"},
                            "example": ["Disney", "HBO", "Amazon Prime"],
                        },
                        "industry": {
                            "type": "string",
                            "example": "media_entertainment",
                        },
                        "roles": {
                            "type": "array",
                            "items": {"type": "string"},
                            "example": ["Software Engineer"],
                        },
                    },
                },
                "TalentHeatmapRequest": {
                    "type": "object",
                    "required": ["role"],
                    "properties": {
                        "role": {"type": "string", "example": "Software Engineer"},
                        "industry": {"type": "string", "example": "tech_engineering"},
                        "locations": {
                            "type": "array",
                            "items": {"type": "string"},
                            "example": ["San Francisco", "Austin", "New York"],
                        },
                        "budget": {"type": "number", "example": 100000},
                        "num_hires": {"type": "integer", "example": 10},
                    },
                },
                "Error": {
                    "type": "object",
                    "properties": {
                        "error": {"type": "string"},
                        "status": {"type": "string", "enum": ["error"]},
                    },
                },
                "RateLimitError": {
                    "type": "object",
                    "properties": {
                        "error": {"type": "string", "example": "Rate limit exceeded"},
                        "limit": {"type": "integer"},
                        "remaining": {"type": "integer"},
                        "reset": {
                            "type": "string",
                            "description": "ISO 8601 timestamp when limit resets",
                        },
                    },
                },
            },
        },
        "paths": {
            "/api/generate": {
                "post": {
                    "summary": "Generate Media Plan",
                    "description": "Generate a comprehensive AI-powered recruitment advertising media plan with channel allocations, budget recommendations, and market insights.",
                    "operationId": "generateMediaPlan",
                    "tags": ["Media Plans"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/MediaPlanRequest"
                                },
                            },
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "Media plan generated successfully",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/MediaPlanResponse"
                                    }
                                }
                            },
                        },
                        "400": {
                            "description": "Invalid request",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Error"}
                                }
                            },
                        },
                        "429": {
                            "description": "Rate limit exceeded",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/RateLimitError"
                                    }
                                }
                            },
                        },
                    },
                },
            },
            "/api/competitive/analyze": {
                "post": {
                    "summary": "Run Competitive Analysis",
                    "description": "Analyze your company against competitors with hiring activity, ad benchmarks, market trends, and strategic recommendations.",
                    "operationId": "runCompetitiveAnalysis",
                    "tags": ["Competitive Intelligence"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/CompetitiveAnalysisRequest"
                                },
                            },
                        },
                    },
                    "responses": {
                        "200": {"description": "Analysis complete"},
                        "400": {"description": "Invalid request"},
                        "429": {"description": "Rate limit exceeded"},
                    },
                },
            },
            "/api/competitive/download/excel": {
                "post": {
                    "summary": "Download Competitive Intelligence Excel",
                    "description": "Generate and download competitive intelligence as an Excel workbook.",
                    "operationId": "downloadCompetitiveExcel",
                    "tags": ["Competitive Intelligence"],
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "responses": {
                        "200": {
                            "description": "Excel file",
                            "content": {
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {
                                    "schema": {"type": "string", "format": "binary"}
                                }
                            },
                        },
                    },
                },
            },
            "/api/competitive/download/ppt": {
                "post": {
                    "summary": "Download Competitive Intelligence PowerPoint",
                    "description": "Generate and download competitive intelligence as a PowerPoint presentation.",
                    "operationId": "downloadCompetitivePpt",
                    "tags": ["Competitive Intelligence"],
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "responses": {
                        "200": {
                            "description": "PowerPoint file",
                            "content": {
                                "application/vnd.openxmlformats-officedocument.presentationml.presentation": {
                                    "schema": {"type": "string", "format": "binary"}
                                }
                            },
                        },
                    },
                },
            },
            "/api/talent-heatmap/analyze": {
                "post": {
                    "summary": "Run Talent Supply Heatmap Analysis",
                    "description": "Analyze talent supply, salary data, hiring difficulty, and optimal locations for a given role and industry.",
                    "operationId": "runTalentHeatmap",
                    "tags": ["Talent Heatmap"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/TalentHeatmapRequest"
                                }
                            }
                        },
                    },
                    "responses": {
                        "200": {"description": "Heatmap analysis complete"},
                        "400": {"description": "Invalid request"},
                        "429": {"description": "Rate limit exceeded"},
                    },
                },
            },
            "/api/talent-heatmap/download/excel": {
                "post": {
                    "summary": "Download Talent Heatmap Excel",
                    "operationId": "downloadTalentHeatmapExcel",
                    "tags": ["Talent Heatmap"],
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "responses": {
                        "200": {"description": "Excel file"},
                    },
                },
            },
            "/api/talent-heatmap/download/ppt": {
                "post": {
                    "summary": "Download Talent Heatmap PowerPoint",
                    "operationId": "downloadTalentHeatmapPpt",
                    "tags": ["Talent Heatmap"],
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "responses": {
                        "200": {"description": "PowerPoint file"},
                    },
                },
            },
            "/api/quick-plan": {
                "post": {
                    "summary": "Generate Quick Plan",
                    "description": "Generate a streamlined 60-second media plan with essential channel recommendations.",
                    "operationId": "generateQuickPlan",
                    "tags": ["Media Plans"],
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "responses": {"200": {"description": "Quick plan generated"}},
                },
            },
            "/api/audit": {
                "post": {
                    "summary": "Run Recruitment Advertising Audit",
                    "description": "Audit your current recruitment advertising spend and get optimization recommendations.",
                    "operationId": "runAudit",
                    "tags": ["Audit"],
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "responses": {"200": {"description": "Audit complete"}},
                },
            },
            "/api/market-pulse": {
                "post": {
                    "summary": "Get Market Pulse Data",
                    "description": "Real-time market pulse data including salary trends, demand signals, and hiring activity.",
                    "operationId": "getMarketPulse",
                    "tags": ["Market Intelligence"],
                    "requestBody": {
                        "required": True,
                        "content": {"application/json": {"schema": {"type": "object"}}},
                    },
                    "responses": {"200": {"description": "Market pulse data"}},
                },
            },
            "/api/portal/keys": {
                "get": {
                    "summary": "List Your API Keys",
                    "description": "List all API keys associated with your account.",
                    "operationId": "listApiKeys",
                    "tags": ["API Portal"],
                    "responses": {"200": {"description": "List of API keys"}},
                },
                "post": {
                    "summary": "Generate New API Key",
                    "description": "Generate a new API key. The full key is only returned once at creation.",
                    "operationId": "generateApiKey",
                    "tags": ["API Portal"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["owner", "tier"],
                                    "properties": {
                                        "owner": {
                                            "type": "string",
                                            "example": "john@example.com",
                                        },
                                        "tier": {
                                            "type": "string",
                                            "enum": ["free", "pro", "enterprise"],
                                            "example": "free",
                                        },
                                    },
                                }
                            }
                        },
                    },
                    "responses": {"200": {"description": "API key generated"}},
                },
            },
            "/api/portal/keys/revoke": {
                "post": {
                    "summary": "Revoke API Key",
                    "description": "Revoke an API key, disabling all further requests with it.",
                    "operationId": "revokeApiKey",
                    "tags": ["API Portal"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "api_key": {"type": "string"},
                                        "key_prefix": {"type": "string"},
                                    },
                                }
                            }
                        },
                    },
                    "responses": {"200": {"description": "Key revoked"}},
                },
            },
            "/api/portal/usage": {
                "post": {
                    "summary": "Get API Key Usage Stats",
                    "description": "Get detailed usage analytics for an API key.",
                    "operationId": "getUsageStats",
                    "tags": ["API Portal"],
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "required": ["api_key"],
                                    "properties": {"api_key": {"type": "string"}},
                                }
                            }
                        },
                    },
                    "responses": {"200": {"description": "Usage statistics"}},
                },
            },
            "/api/portal/spec": {
                "get": {
                    "summary": "Get OpenAPI Specification",
                    "description": "Returns the OpenAPI 3.0 specification for this API.",
                    "operationId": "getOpenApiSpec",
                    "tags": ["API Portal"],
                    "security": [],
                    "responses": {"200": {"description": "OpenAPI spec"}},
                },
            },
        },
        "tags": [
            {
                "name": "Media Plans",
                "description": "Generate AI-powered recruitment media plans",
            },
            {
                "name": "Competitive Intelligence",
                "description": "Competitive analysis and benchmarking",
            },
            {
                "name": "Talent Heatmap",
                "description": "Talent supply and hiring difficulty mapping",
            },
            {
                "name": "Market Intelligence",
                "description": "Real-time market and salary data",
            },
            {"name": "Audit", "description": "Recruitment advertising audits"},
            {
                "name": "API Portal",
                "description": "API key management and usage tracking",
            },
        ],
    }

    return spec


# ═══════════════════════════════════════════════════════════════════════════════
# 4. API PORTAL REQUEST HANDLER
# ═══════════════════════════════════════════════════════════════════════════════


def handle_portal_api(
    path: str, method: str, body: Dict[str, Any], headers: Dict[str, str]
) -> Tuple[int, Dict[str, Any]]:
    """Handle all /api/portal/* requests.

    Routes:
      POST /api/portal/keys          -> generate new key
      GET  /api/portal/keys          -> list keys
      POST /api/portal/keys/revoke   -> revoke a key
      POST /api/portal/keys/validate -> validate a key
      POST /api/portal/usage         -> get usage stats
      GET  /api/portal/spec          -> OpenAPI spec
      GET  /api/portal/tiers         -> pricing tier info
      GET  /api/portal/endpoints     -> list available endpoints

    Args:
        path: The path after /api/portal/
        method: HTTP method (GET, POST)
        body: Parsed JSON body
        headers: Request headers dict

    Returns:
        Tuple of (status_code, response_dict)
    """
    start_time = time.time()

    try:
        # Normalize path
        path = path.strip("/")
        if path.startswith("portal/"):
            path = path[7:]  # Remove "portal/" prefix
        elif path.startswith("portal"):
            path = path[6:]  # Remove "portal" prefix
        path = path.strip("/")

        # ── Route: Generate API Key ──
        if path == "keys" and method == "POST":
            result = generate_api_key(
                tier=body.get("tier", "free"),
                owner=body.get("owner") or "",
            )
            status = 200 if result.get("success") else 400
            return status, result

        # ── Route: List API Keys ──
        elif path == "keys" and method == "GET":
            keys = list_api_keys(admin=body.get("admin", False))
            return 200, {"keys": keys, "total": len(keys)}

        # ── Route: Revoke API Key ──
        elif path == "keys/revoke" and method == "POST":
            api_key = body.get("api_key") or ""
            key_prefix = body.get("key_prefix") or ""

            if api_key:
                success = revoke_api_key(api_key)
            elif key_prefix:
                success = revoke_api_key_by_prefix(key_prefix)
            else:
                return 400, {
                    "error": "api_key or key_prefix is required",
                    "success": False,
                }

            if success:
                return 200, {"success": True, "message": "API key revoked successfully"}
            return 404, {
                "success": False,
                "error": "API key not found or already revoked",
            }

        # ── Route: Validate API Key ──
        elif path == "keys/validate" and method == "POST":
            api_key = body.get("api_key") or ""
            result = validate_api_key(api_key)
            status = 200 if result.get("valid") else 401
            return status, result

        # ── Route: Usage Stats ──
        elif path == "usage" and method == "POST":
            api_key = body.get("api_key") or ""
            if not api_key:
                return 400, {"error": "api_key is required"}
            result = get_usage_stats(api_key)
            if "error" in result:
                return 404, result
            return 200, result

        # ── Route: OpenAPI Spec ──
        elif path == "spec" and method in ("GET", "POST"):
            spec = get_openapi_spec()
            return 200, spec

        # ── Route: Tier Info ──
        elif path == "tiers" and method in ("GET", "POST"):
            tiers = []
            for key, info in RATE_LIMIT_TIERS.items():
                tiers.append(
                    {
                        "tier": key,
                        "label": info["label"],
                        "price": info["price"],
                        "requests_per_day": info["requests_per_day"],
                        "requests_label": (
                            "Unlimited"
                            if info["requests_per_day"] == -1
                            else f"{info['requests_per_day']}/day"
                        ),
                        "features": info["features"],
                    }
                )
            return 200, {"tiers": tiers}

        # ── Route: Available Endpoints ──
        elif path == "endpoints" and method in ("GET", "POST"):
            endpoints = _get_endpoint_summary()
            return 200, {"endpoints": endpoints}

        # ── Route: Dashboard Summary ──
        elif path == "dashboard" and method in ("GET", "POST"):
            summary = _get_dashboard_summary()
            return 200, summary

        else:
            return 404, {
                "error": f"Unknown portal endpoint: /api/portal/{path}",
                "method": method,
            }

    except Exception as exc:
        logger.error(
            "Portal API error at %s: %s\n%s", path, exc, traceback.format_exc()
        )
        return 500, {"error": str(exc), "status": "error"}


def _get_endpoint_summary() -> List[Dict[str, Any]]:
    """Get a summary of all available API endpoints for the explorer."""
    return [
        {
            "method": "POST",
            "path": "/api/generate",
            "name": "Generate Media Plan",
            "description": "Generate a comprehensive AI-powered recruitment advertising media plan with channel allocations, budget optimization, and market insights.",
            "category": "Media Plans",
            "sample_body": {
                "company_name": "Acme Corp",
                "industry": "tech_engineering",
                "roles": "Software Engineer",
                "locations": "San Francisco, CA",
                "budget": 50000,
                "campaign_duration_weeks": 12,
                "num_openings": 5,
            },
        },
        {
            "method": "POST",
            "path": "/api/competitive/analyze",
            "name": "Competitive Analysis",
            "description": "Compare your company against competitors with hiring activity, ad benchmarks, Google Trends, and strategic recommendations.",
            "category": "Competitive Intelligence",
            "sample_body": {
                "company_name": "Netflix",
                "competitors": ["Disney", "HBO", "Amazon Prime"],
                "industry": "media_entertainment",
            },
        },
        {
            "method": "POST",
            "path": "/api/talent-heatmap/analyze",
            "name": "Talent Supply Heatmap",
            "description": "Analyze talent density, salary benchmarks, hiring difficulty, and optimal locations for a given role.",
            "category": "Talent Heatmap",
            "sample_body": {
                "role": "Software Engineer",
                "industry": "tech_engineering",
                "locations": [
                    "San Francisco",
                    "Austin",
                    "New York",
                    "Denver",
                    "Seattle",
                ],
                "budget": 100000,
                "num_hires": 10,
            },
        },
        {
            "method": "POST",
            "path": "/api/quick-plan",
            "name": "Quick Plan (60-Second)",
            "description": "Generate a streamlined media plan in 60 seconds with essential channel recommendations.",
            "category": "Media Plans",
            "sample_body": {
                "role": "Registered Nurse",
                "location": "Dallas, TX",
                "budget": 25000,
                "industry": "healthcare_medical",
            },
        },
        {
            "method": "POST",
            "path": "/api/audit",
            "name": "Recruitment Ad Audit",
            "description": "Audit your current recruitment advertising spend and get optimization recommendations.",
            "category": "Audit",
            "sample_body": {
                "company_name": "Acme Healthcare",
                "industry": "healthcare_medical",
                "current_spend": {"indeed": 15000, "linkedin": 10000, "google": 5000},
                "monthly_budget": 30000,
                "roles": "Registered Nurse, Medical Assistant",
                "locations": "Houston, TX",
            },
        },
        {
            "method": "POST",
            "path": "/api/market-pulse",
            "name": "Market Pulse",
            "description": "Real-time market intelligence including salary trends, demand signals, and hiring activity for any role and location.",
            "category": "Market Intelligence",
            "sample_body": {
                "roles": ["Software Engineer", "Data Scientist"],
                "locations": ["San Francisco", "New York"],
                "industry": "tech_engineering",
            },
        },
        {
            "method": "POST",
            "path": "/api/competitive/download/excel",
            "name": "Download Competitive Intel (Excel)",
            "description": "Generate and download competitive intelligence report as an Excel workbook.",
            "category": "Competitive Intelligence",
            "sample_body": {"company_name": "Netflix", "brief": {}},
        },
        {
            "method": "POST",
            "path": "/api/competitive/download/ppt",
            "name": "Download Competitive Intel (PPT)",
            "description": "Generate and download competitive intelligence report as a PowerPoint presentation.",
            "category": "Competitive Intelligence",
            "sample_body": {"company_name": "Netflix", "brief": {}},
        },
    ]


def _get_dashboard_summary() -> Dict[str, Any]:
    """Get API portal dashboard summary statistics."""
    with _file_lock:
        data = _load_api_keys()

    keys = data.get("keys", {})
    usage_log = data.get("usage_log") or []

    total_keys = len(keys)
    active_keys = sum(1 for k in keys.values() if k.get("is_active", True))
    revoked_keys = total_keys - active_keys

    # Tier distribution
    tier_dist: Dict[str, int] = defaultdict(int)
    for record in keys.values():
        if record.get("is_active", True):
            tier_dist[record.get("tier", "free")] += 1

    # Total requests
    total_requests = sum(r.get("total_requests") or 0 for r in keys.values())
    total_errors = sum(r.get("total_errors") or 0 for r in keys.values())

    # Last 7 days usage
    cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat() + "Z"
    recent_entries = [e for e in usage_log if e.get("timestamp") or "" >= cutoff]
    daily_counts: Dict[str, int] = defaultdict(int)
    for entry in recent_entries:
        day = entry.get("timestamp") or ""[:10]
        daily_counts[day] += 1

    return {
        "total_keys": total_keys,
        "active_keys": active_keys,
        "revoked_keys": revoked_keys,
        "tier_distribution": dict(tier_dist),
        "total_requests": total_requests,
        "total_errors": total_errors,
        "error_rate_pct": (
            round((total_errors / total_requests * 100), 2) if total_requests > 0 else 0
        ),
        "recent_daily_usage": [
            {"date": d, "requests": daily_counts.get(d, 0)}
            for d in sorted(daily_counts.keys())
        ],
        "last_updated": datetime.utcnow().isoformat() + "Z",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 5. API MIDDLEWARE -- Key Validation & Rate Limiting
# ═══════════════════════════════════════════════════════════════════════════════


def validate_request(
    headers: Dict[str, str], endpoint: str
) -> Tuple[bool, int, Dict[str, Any]]:
    """Validate an incoming API request for authentication and rate limiting.

    Extracts API key from X-API-Key header, validates it, checks rate limits.

    Args:
        headers: Request headers dict
        endpoint: The API endpoint being called

    Returns:
        Tuple of (is_valid, status_code, response_or_key_info)
    """
    api_key = headers.get("X-API-Key", headers.get("x-api-key") or "")

    if not api_key:
        return (
            False,
            401,
            {
                "error": "API key required. Include X-API-Key header.",
                "status": "error",
                "docs": "https://media-plan-generator.onrender.com/api-portal",
            },
        )

    # Validate key
    validation = validate_api_key(api_key)
    if not validation.get("valid"):
        return (
            False,
            401,
            {
                "error": validation.get("error", "Invalid API key"),
                "status": "error",
            },
        )

    # Check rate limit
    if not check_rate_limit(api_key):
        tier = validation.get("tier", "free")
        tier_info = RATE_LIMIT_TIERS.get(tier, RATE_LIMIT_TIERS["free"])
        tomorrow = (datetime.utcnow() + timedelta(days=1)).replace(
            hour=0, minute=0, second=0
        ).isoformat() + "Z"
        return (
            False,
            429,
            {
                "error": "Rate limit exceeded",
                "status": "error",
                "limit": tier_info["requests_per_day"],
                "remaining": 0,
                "reset": tomorrow,
                "upgrade_url": "https://media-plan-generator.onrender.com/api-portal",
            },
        )

    return True, 200, {"api_key": api_key, "tier": validation["tier"]}


def get_rate_limit_info(key: str) -> Dict[str, Any]:
    """Get current rate limit status for an API key.

    Returns dict with limit, remaining, used, and reset time.
    """
    if not key:
        return {}

    key_hash = _hash_key(key)
    validation = validate_api_key(key)
    if not validation.get("valid"):
        return {}

    tier = validation.get("tier", "free")
    tier_info = RATE_LIMIT_TIERS.get(tier, RATE_LIMIT_TIERS["free"])
    max_requests = tier_info["requests_per_day"]

    today = datetime.utcnow().strftime("%Y-%m-%d")
    used = 0

    with _rate_limit_lock:
        if (
            key_hash in _rate_limit_cache
            and _rate_limit_cache[key_hash]["date"] == today
        ):
            used = _rate_limit_cache[key_hash]["count"]

    tomorrow = (datetime.utcnow() + timedelta(days=1)).replace(
        hour=0, minute=0, second=0
    ).isoformat() + "Z"

    return {
        "limit": max_requests,
        "remaining": max(0, max_requests - used) if max_requests > 0 else -1,
        "used": used,
        "reset": tomorrow,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 6. UTILITY HELPERS
# ═══════════════════════════════════════════════════════════════════════════════


def _hash_key(key: str) -> str:
    """Hash an API key for secure storage using SHA-256."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _safe_call(fn, *args, **kwargs):
    """Call a function, returning None on any exception."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        logger.warning("_safe_call(%s) failed: %s", fn.__name__, exc)
        return None


def get_industry_options() -> List[Dict[str, str]]:
    """Return industry options for frontend dropdowns."""
    return [{"value": key, "label": label} for key, label in INDUSTRY_LABEL_MAP.items()]


def get_tier_options() -> List[Dict[str, str]]:
    """Return tier options for frontend dropdowns."""
    return [
        {"value": key, "label": info["label"], "price": info["price"]}
        for key, info in RATE_LIMIT_TIERS.items()
    ]
