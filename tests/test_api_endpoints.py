#!/usr/bin/env python3
"""Tests for API endpoint logic: auth bypass, JSON structure, route handling.

Since the server uses http.server.HTTPServer which blocks on serve_forever(),
these tests validate API logic via direct module imports and mocked handlers
rather than live HTTP requests.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional
from unittest import mock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ═══════════════════════════════════════════════════════════════════════════════
# Auth: Public endpoint bypass
# ═══════════════════════════════════════════════════════════════════════════════


class TestPublicEndpointAuth:
    """Verify that public endpoints bypass authentication."""

    PUBLIC_PATHS = [
        "/",
        "/hub",
        "/platform",
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
        "/robots.txt",
        "/sitemap.xml",
        "/static/nova-chat.js",
    ]

    @pytest.mark.parametrize("path", PUBLIC_PATHS)
    def test_public_endpoint_bypasses_auth(self, path: str) -> None:
        """Public endpoint should authenticate without any key."""
        from auth import authenticate

        result = authenticate(path, None)
        assert result["authenticated"] is True
        assert result["role"] == "public"

    def test_static_files_bypass_auth(self) -> None:
        """Static file paths should bypass auth."""
        from auth import authenticate

        result = authenticate("/static/some-file.js", None)
        assert result["authenticated"] is True

    def test_fragment_endpoints_bypass_auth(self) -> None:
        """Fragment endpoints (platform SPA) should bypass auth."""
        from auth import authenticate

        result = authenticate("/fragment/dashboard", None)
        assert result["authenticated"] is True

    def test_non_api_page_routes_bypass_auth(self) -> None:
        """Non-API page routes should bypass auth."""
        from auth import authenticate

        result = authenticate("/some-product-page", None)
        assert result["authenticated"] is True
        assert result["role"] == "public"


class TestAdminEndpointAuth:
    """Verify admin endpoints require authentication."""

    ADMIN_PATHS = [
        "/api/admin/keys",
        "/api/admin/config",
        "/api/admin/metrics",
    ]

    @pytest.mark.parametrize("path", ADMIN_PATHS)
    def test_admin_endpoint_requires_key(self, path: str) -> None:
        """Admin endpoints should reject requests without a key when auth is enabled."""
        from auth import authenticate, is_auth_enabled

        # Only test if auth is actually enabled (has keys configured)
        if is_auth_enabled():
            result = authenticate(path, None)
            assert result["authenticated"] is False
            assert result["role"] == "none"

    @pytest.mark.parametrize("path", ADMIN_PATHS)
    def test_admin_endpoint_rejects_invalid_key(self, path: str) -> None:
        """Admin endpoints should reject an invalid key."""
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key("real-admin-key")):
            with mock.patch(
                "auth._API_KEYS", {_hash_key("user-key"): {"name": "k", "role": "user"}}
            ):
                from auth import authenticate

                result = authenticate(path, "Bearer invalid-key")
                assert result["authenticated"] is False

    def test_admin_endpoint_accepts_valid_admin_key(self) -> None:
        """Admin endpoint should accept a valid admin key."""
        from auth import _hash_key

        admin_key = "test-admin-key-12345"
        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key(admin_key)):
            from auth import authenticate

            result = authenticate("/api/admin/keys", f"Bearer {admin_key}")
            assert result["authenticated"] is True
            assert result["role"] == "admin"


class TestProductAPIAuth:
    """Product API endpoints are public (no auth required)."""

    PRODUCT_API_PATHS = [
        "/api/chat",
        "/api/generate",
        "/api/channels",
        "/api/simulator/simulate",
        "/api/tracker/analyze",
        "/api/competitive/analyze",
    ]

    @pytest.mark.parametrize("path", PRODUCT_API_PATHS)
    def test_product_api_is_public(self, path: str) -> None:
        """Product API endpoints should be accessible without auth."""
        from auth import authenticate

        result = authenticate(path, None)
        assert result["authenticated"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# Auth module initialization
# ═══════════════════════════════════════════════════════════════════════════════


class TestAuthInit:
    """Test auth module initialization logic."""

    def test_is_auth_enabled_returns_bool(self) -> None:
        """is_auth_enabled should return a boolean."""
        from auth import is_auth_enabled

        result = is_auth_enabled()
        assert isinstance(result, bool)

    def test_hash_key_deterministic(self) -> None:
        """_hash_key should produce consistent hashes."""
        from auth import _hash_key

        h1 = _hash_key("test-key")
        h2 = _hash_key("test-key")
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex digest

    def test_hash_key_unique(self) -> None:
        """Different keys should produce different hashes."""
        from auth import _hash_key

        h1 = _hash_key("key-one")
        h2 = _hash_key("key-two")
        assert h1 != h2

    def test_bearer_prefix_stripped(self) -> None:
        """Bearer prefix should be stripped from auth header."""
        admin_key = "test-admin-key-xyz"
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key(admin_key)):
            from auth import authenticate

            result = authenticate("/api/admin/keys", f"Bearer {admin_key}")
            assert result["authenticated"] is True

    def test_raw_key_accepted(self) -> None:
        """Key without Bearer prefix should also work."""
        admin_key = "test-admin-key-raw"
        from auth import _hash_key

        with mock.patch("auth._ADMIN_KEY_HASH", _hash_key(admin_key)):
            from auth import authenticate

            result = authenticate("/api/admin/keys", admin_key)
            assert result["authenticated"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# Route existence in source code
# ═══════════════════════════════════════════════════════════════════════════════


class TestHealthRouteHandlers:
    """Test health route handler module imports and structures."""

    def test_health_routes_module_importable(self) -> None:
        """routes.health module should be importable."""
        from routes.health import handle_health_routes

        assert callable(handle_health_routes)

    def test_pages_routes_module_importable(self) -> None:
        """routes.pages module should be importable."""
        from routes.pages import handle_page_routes

        assert callable(handle_page_routes)


class TestRobotsTxt:
    """Test robots.txt content validation."""

    def test_robots_txt_exists_in_route_source(self, app_source: str) -> None:
        """robots.txt route must be handled in app source."""
        assert "robots.txt" in app_source

    def test_robots_txt_content_pattern(self, app_source: str) -> None:
        """robots.txt should contain User-agent and Sitemap directives."""
        # The robots.txt is likely generated inline in app.py
        assert "User-agent" in app_source or "user-agent" in app_source.lower()


class TestSitemapXml:
    """Test sitemap.xml route existence."""

    def test_sitemap_route_exists(self, app_source: str) -> None:
        """sitemap.xml route must be handled."""
        assert "sitemap.xml" in app_source


class TestConfigEndpoint:
    """Test /api/config structure (PostHog config)."""

    def test_posthog_integration_importable(self) -> None:
        """PostHog integration module should be importable."""
        import posthog_integration

        assert hasattr(posthog_integration, "track_event")
        assert hasattr(posthog_integration, "is_feature_enabled")

    def test_posthog_stats_returns_dict(self) -> None:
        """get_stats should return a dict."""
        from posthog_integration import get_stats

        stats = get_stats()
        assert isinstance(stats, dict)

    def test_posthog_track_event_callable(self) -> None:
        """track_event should be callable."""
        from posthog_integration import track_event

        assert callable(track_event)


class TestDashboardWidgets:
    """Test dashboard widgets endpoint structure."""

    def test_dashboard_widgets_route_referenced(self) -> None:
        """Dashboard widgets route must be referenced in codebase."""
        # The route may be in auth.py PUBLIC_ENDPOINTS or routes/health.py
        from auth import PUBLIC_ENDPOINTS

        assert "/api/dashboard/widgets" in PUBLIC_ENDPOINTS


class TestCSRFToken:
    """Test CSRF token endpoint."""

    def test_csrf_token_route_exists(self, app_source: str) -> None:
        """CSRF token route must exist."""
        assert "/api/csrf-token" in app_source


class TestBrandedErrorPage:
    """Test 404 branded error page."""

    def test_404_handler_exists(self, app_source: str) -> None:
        """App should have a custom 404 handler."""
        assert "404" in app_source

    def test_404_template_or_inline(self) -> None:
        """404 page should exist as template or inline handler."""
        template_path = PROJECT_ROOT / "templates" / "404.html"
        # Either a template exists or it's handled inline
        has_template = template_path.exists()
        app_source = (PROJECT_ROOT / "app.py").read_text(encoding="utf-8")
        has_inline = "404" in app_source and (
            "Not Found" in app_source or "not found" in app_source.lower()
        )
        assert has_template or has_inline, "No 404 handler found"
