#!/usr/bin/env python3
"""API Key Authentication for Nova AI Suite.

Simple API key auth for /api/* endpoints. Keys stored in env var.
Admin endpoints require admin key. Public endpoints are unprotected.
"""

import hashlib
import hmac
import logging
import os
import secrets
import time
import threading
from typing import Optional

logger = logging.getLogger(__name__)

# Public endpoints that don't require auth
PUBLIC_ENDPOINTS = frozenset(
    {
        "/",
        "/hub",
        "/hub/",
        "/platform",
        "/platform/",
        "/media-plan",
        "/simulator",
        "/competitive",
        "/market-pulse",
        "/social-plan",
        "/tracker",
        "/compliance-guard",
        "/creative-ai",
        "/api-portal",
        "/quick-plan",
        "/quick-brief",
        "/api/health",
        "/api/health/ready",
        "/health",
        "/ready",
        "/api/csrf-token",
        "/api/dashboard/widgets",
        "/api/channels",
        "/api/health/data-matrix",
        "/api/health/enrichment",
        "/api/health/integrations",
        "/api/resilience/status",
        "/api/insights",
        "/api/insights/stats",
        "/robots.txt",
        "/sitemap.xml",
        "/static/nova-chat.js",
    }
)

# Endpoints that require admin key
ADMIN_ENDPOINTS_PREFIX = ("/api/admin/", "/dashboard", "/api/nova/metrics")

# API keys loaded from env
_API_KEYS: dict[str, dict] = {}  # key_hash -> {name, role, created_at}
_ADMIN_KEY_HASH: Optional[str] = None
_lock = threading.Lock()


def _hash_key(key: str) -> str:
    """Hash an API key for storage comparison."""
    return hashlib.sha256(key.encode()).hexdigest()


def init() -> None:
    """Initialize auth from environment variables."""
    global _ADMIN_KEY_HASH

    # Admin key
    admin_key = os.environ.get("NOVA_ADMIN_KEY") or ""
    if admin_key:
        _ADMIN_KEY_HASH = _hash_key(admin_key)
        logger.info("[Auth] Admin key configured")

    # API keys (comma-separated in NOVA_API_KEYS env var)
    api_keys_str = os.environ.get("NOVA_API_KEYS") or ""
    if api_keys_str:
        for i, key in enumerate(api_keys_str.split(",")):
            key = key.strip()
            if key:
                with _lock:
                    _API_KEYS[_hash_key(key)] = {
                        "name": f"key_{i}",
                        "role": "user",
                        "created_at": time.time(),
                    }
        logger.info("[Auth] Loaded %d API keys", len(_API_KEYS))

    # If no keys configured, auth is disabled (open access)
    if not admin_key and not api_keys_str:
        logger.warning("[Auth] No API keys configured -- all endpoints are open")


def is_auth_enabled() -> bool:
    """Check if authentication is configured."""
    return bool(_ADMIN_KEY_HASH or _API_KEYS)


def authenticate(
    path: str, auth_header: Optional[str] = None, query_key: Optional[str] = None
) -> dict:
    """Authenticate a request.

    Returns: {"authenticated": bool, "role": str, "reason": str}
    """
    # Public endpoints always allowed
    if path in PUBLIC_ENDPOINTS:
        return {"authenticated": True, "role": "public", "reason": "public_endpoint"}

    # Fragment endpoints (platform SPA)
    if path.startswith("/fragment/"):
        return {"authenticated": True, "role": "public", "reason": "fragment_endpoint"}

    # Static files
    if path.startswith("/static/"):
        return {"authenticated": True, "role": "public", "reason": "static_file"}

    # Template pages (non-API)
    if not path.startswith("/api/"):
        return {"authenticated": True, "role": "public", "reason": "page_route"}

    # All product API endpoints are public (UI calls them without auth)
    # Only /api/admin/* requires authentication
    _ADMIN_ONLY = ("/api/admin/",)
    if not any(path.startswith(p) for p in _ADMIN_ONLY):
        return {"authenticated": True, "role": "public", "reason": "product_api"}

    # If no auth configured, allow everything
    if not is_auth_enabled():
        return {"authenticated": True, "role": "admin", "reason": "auth_disabled"}

    # Extract key from header or query
    key = None
    if auth_header:
        if auth_header.startswith("Bearer "):
            key = auth_header[7:]
        else:
            key = auth_header
    elif query_key:
        key = query_key

    if not key:
        return {"authenticated": False, "role": "none", "reason": "no_api_key"}

    key_hash = _hash_key(key)

    # Check admin key
    is_admin_endpoint = any(path.startswith(p) for p in ADMIN_ENDPOINTS_PREFIX)

    if key_hash == _ADMIN_KEY_HASH:
        return {"authenticated": True, "role": "admin", "reason": "admin_key"}

    if is_admin_endpoint:
        return {"authenticated": False, "role": "none", "reason": "admin_required"}

    # Check user API keys
    with _lock:
        if key_hash in _API_KEYS:
            return {"authenticated": True, "role": "user", "reason": "api_key"}

    return {"authenticated": False, "role": "none", "reason": "invalid_key"}


def generate_api_key() -> str:
    """Generate a new API key."""
    return f"nova_{secrets.token_urlsafe(32)}"


def get_auth_status() -> dict:
    """Get auth system status for admin dashboard."""
    with _lock:
        return {
            "enabled": is_auth_enabled(),
            "admin_key_set": _ADMIN_KEY_HASH is not None,
            "api_key_count": len(_API_KEYS),
        }
