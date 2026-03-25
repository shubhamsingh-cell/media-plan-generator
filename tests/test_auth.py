#!/usr/bin/env python3
"""Comprehensive tests for auth.py -- API key authentication module.

Tests cover:
- Key hashing and comparison
- Public endpoint bypass
- Admin endpoint protection
- Product API public access
- Bearer token parsing
- Auth enable/disable logic
- Init from environment variables
"""

from __future__ import annotations

import hashlib
import os
import sys
import time
from pathlib import Path
from typing import Optional
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# Key Hashing
# ═══════════════════════════════════════════════════════════════════════════════


class TestKeyHashing:
    """Test API key hashing functions."""

    def test_hash_uses_sha256(self) -> None:
        """_hash_key should use SHA-256."""
        from auth import _hash_key

        expected = hashlib.sha256("test".encode()).hexdigest()
        assert _hash_key("test") == expected

    def test_hash_deterministic(self) -> None:
        """Same key should always produce same hash."""
        from auth import _hash_key

        assert _hash_key("abc123") == _hash_key("abc123")

    def test_hash_different_keys(self) -> None:
        """Different keys should produce different hashes."""
        from auth import _hash_key

        assert _hash_key("key1") != _hash_key("key2")

    def test_hash_length(self) -> None:
        """Hash should be 64 chars (SHA-256 hex digest)."""
        from auth import _hash_key

        assert len(_hash_key("any-key")) == 64


# ═══════════════════════════════════════════════════════════════════════════════
# Public Endpoints
# ═══════════════════════════════════════════════════════════════════════════════


class TestPublicEndpoints:
    """Test PUBLIC_ENDPOINTS frozenset."""

    def test_public_endpoints_is_frozenset(self) -> None:
        """PUBLIC_ENDPOINTS should be a frozenset."""
        from auth import PUBLIC_ENDPOINTS

        assert isinstance(PUBLIC_ENDPOINTS, frozenset)

    def test_health_endpoint_public(self) -> None:
        """Health check endpoints should be public."""
        from auth import PUBLIC_ENDPOINTS

        assert "/api/health" in PUBLIC_ENDPOINTS
        assert "/api/health/ready" in PUBLIC_ENDPOINTS

    def test_root_is_public(self) -> None:
        """Root path should be public."""
        from auth import PUBLIC_ENDPOINTS

        assert "/" in PUBLIC_ENDPOINTS

    def test_platform_is_public(self) -> None:
        """Platform path should be public."""
        from auth import PUBLIC_ENDPOINTS

        assert "/platform" in PUBLIC_ENDPOINTS

    def test_dashboard_widgets_public(self) -> None:
        """Dashboard widgets API should be public."""
        from auth import PUBLIC_ENDPOINTS

        assert "/api/dashboard/widgets" in PUBLIC_ENDPOINTS

    def test_csrf_token_public(self) -> None:
        """CSRF token endpoint should be public."""
        from auth import PUBLIC_ENDPOINTS

        assert "/api/csrf-token" in PUBLIC_ENDPOINTS

    def test_static_assets_public(self) -> None:
        """Static assets should be public."""
        from auth import PUBLIC_ENDPOINTS

        assert "/static/nova-chat.js" in PUBLIC_ENDPOINTS


# ═══════════════════════════════════════════════════════════════════════════════
# Authentication Logic
# ═══════════════════════════════════════════════════════════════════════════════


class TestAuthenticateFunction:
    """Test the authenticate() function logic."""

    def test_public_endpoint_always_passes(self) -> None:
        """Public endpoints should always authenticate."""
        from auth import authenticate

        result = authenticate("/", None)
        assert result["authenticated"] is True
        assert result["role"] == "public"

    def test_fragment_endpoint_passes(self) -> None:
        """Fragment endpoints should pass."""
        from auth import authenticate

        result = authenticate("/fragment/command-center", None)
        assert result["authenticated"] is True

    def test_static_file_passes(self) -> None:
        """Static files should pass."""
        from auth import authenticate

        result = authenticate("/static/app.js", None)
        assert result["authenticated"] is True

    def test_page_route_passes(self) -> None:
        """Non-API page routes should pass."""
        from auth import authenticate

        result = authenticate("/vendor-iq", None)
        assert result["authenticated"] is True

    def test_product_api_passes(self) -> None:
        """Product API endpoints (non-admin) should pass without key."""
        from auth import authenticate

        result = authenticate("/api/chat", None)
        assert result["authenticated"] is True

    def test_admin_endpoint_fails_without_key(self) -> None:
        """Admin endpoint should fail without a key when auth is enabled."""
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key("admin-key")):
            from auth import authenticate

            result = authenticate("/api/admin/keys", None)
            assert result["authenticated"] is False
            assert result["reason"] == "no_api_key"

    def test_admin_endpoint_passes_with_admin_key(self) -> None:
        """Admin endpoint should pass with correct admin key."""
        admin_key = "my-admin-key-test"
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key(admin_key)):
            from auth import authenticate

            result = authenticate("/api/admin/keys", f"Bearer {admin_key}")
            assert result["authenticated"] is True
            assert result["role"] == "admin"

    def test_bearer_prefix_stripped(self) -> None:
        """Bearer prefix should be stripped from auth header."""
        admin_key = "bearer-test-key"
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key(admin_key)):
            from auth import authenticate

            result = authenticate("/api/admin/keys", f"Bearer {admin_key}")
            assert result["authenticated"] is True

    def test_raw_key_accepted(self) -> None:
        """Key without Bearer prefix should also work."""
        admin_key = "raw-key-test"
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key(admin_key)):
            from auth import authenticate

            result = authenticate("/api/admin/keys", admin_key)
            assert result["authenticated"] is True

    def test_query_key_parameter(self) -> None:
        """API key via query parameter should work."""
        admin_key = "query-key-test"
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key(admin_key)):
            from auth import authenticate

            result = authenticate("/api/admin/keys", None, query_key=admin_key)
            assert result["authenticated"] is True

    def test_invalid_key_rejected(self) -> None:
        """Invalid key should be rejected for admin endpoints."""
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key("correct-key")):
            from auth import authenticate

            result = authenticate("/api/admin/keys", "Bearer wrong-key")
            assert result["authenticated"] is False

    def test_auth_disabled_allows_all(self) -> None:
        """When auth is disabled, all endpoints should be accessible."""
        with mock.patch("auth._ADMIN_KEY_HASH", None):
            with mock.patch("auth._API_KEYS", {}):
                from auth import authenticate

                result = authenticate("/api/admin/keys", None)
                assert result["authenticated"] is True
                assert result["reason"] == "auth_disabled"


# ═══════════════════════════════════════════════════════════════════════════════
# is_auth_enabled
# ═══════════════════════════════════════════════════════════════════════════════


class TestIsAuthEnabled:
    """Test is_auth_enabled() function."""

    def test_enabled_with_admin_key(self) -> None:
        """Auth should be enabled when admin key is set."""
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key("key")):
            from auth import is_auth_enabled

            assert is_auth_enabled() is True

    def test_enabled_with_api_keys(self) -> None:
        """Auth should be enabled when API keys are set."""
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", None):
            with mock.patch(
                "auth._API_KEYS", {_hash_key("k"): {"name": "k", "role": "user"}}
            ):
                from auth import is_auth_enabled

                assert is_auth_enabled() is True

    def test_disabled_when_no_keys(self) -> None:
        """Auth should be disabled when no keys are configured."""
        with mock.patch("auth._ADMIN_KEY_HASH", None):
            with mock.patch("auth._API_KEYS", {}):
                from auth import is_auth_enabled

                assert is_auth_enabled() is False


# ═══════════════════════════════════════════════════════════════════════════════
# Init from environment
# ═══════════════════════════════════════════════════════════════════════════════


class TestAuthInit:
    """Test auth.init() from environment variables."""

    @mock.patch.dict(os.environ, {"NOVA_ADMIN_KEY": "test-admin-key-init"}, clear=False)
    def test_init_loads_admin_key(self) -> None:
        """init() should load admin key from environment."""
        import auth

        # Save original state
        orig_hash = auth._ADMIN_KEY_HASH
        try:
            auth.init()
            assert auth._ADMIN_KEY_HASH is not None
            assert auth._ADMIN_KEY_HASH == auth._hash_key("test-admin-key-init")
        finally:
            auth._ADMIN_KEY_HASH = orig_hash

    @mock.patch.dict(os.environ, {"NOVA_API_KEYS": "key1,key2,key3"}, clear=False)
    def test_init_loads_api_keys(self) -> None:
        """init() should load comma-separated API keys from environment."""
        import auth

        orig_keys = auth._API_KEYS.copy()
        try:
            auth.init()
            # Should have loaded 3 keys
            assert len(auth._API_KEYS) >= 3
        finally:
            auth._API_KEYS = orig_keys
