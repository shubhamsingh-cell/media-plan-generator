"""End-to-end integration tests for the Nova AI Suite.

Uses only stdlib (http.client) so no external dependencies are needed.
Expects the server to be running on localhost:8000 (or the PORT env var).
Run with: python -m pytest tests/test_e2e.py -v
"""

import http.client
import json
import os
import unittest
from typing import Optional


SERVER_HOST: str = os.environ.get("TEST_HOST", "localhost")
SERVER_PORT: int = int(os.environ.get("TEST_PORT", os.environ.get("PORT", "8000")))


def _get(
    path: str,
    headers: Optional[dict[str, str]] = None,
) -> http.client.HTTPResponse:
    """Send a GET request to the test server.

    Args:
        path: URL path to request.
        headers: Optional extra headers.

    Returns:
        The HTTPResponse object.
    """
    conn = http.client.HTTPConnection(SERVER_HOST, SERVER_PORT, timeout=10)
    conn.request("GET", path, headers=headers or {})
    return conn.getresponse()


def _post(
    path: str,
    body: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
) -> http.client.HTTPResponse:
    """Send a POST request to the test server.

    Args:
        path: URL path to request.
        body: Request body bytes.
        headers: Optional extra headers.

    Returns:
        The HTTPResponse object.
    """
    conn = http.client.HTTPConnection(SERVER_HOST, SERVER_PORT, timeout=10)
    hdrs = headers or {}
    if body and "Content-Type" not in hdrs:
        hdrs["Content-Type"] = "application/json"
    conn.request("POST", path, body=body, headers=hdrs)
    return conn.getresponse()


class TestHomepage(unittest.TestCase):
    """Tests for the homepage / hub."""

    def test_homepage_returns_200(self) -> None:
        """Homepage should return 200 with HTML content-type."""
        resp = _get("/")
        self.assertEqual(resp.status, 200)
        content_type = resp.getheader("Content-Type") or ""
        self.assertIn("text/html", content_type)

    def test_hub_alias(self) -> None:
        """The /hub path should also return 200."""
        resp = _get("/hub")
        self.assertEqual(resp.status, 200)


class TestPlatform(unittest.TestCase):
    """Tests for the platform page."""

    def test_platform_returns_200(self) -> None:
        """/platform should return 200."""
        resp = _get("/platform")
        self.assertEqual(resp.status, 200)
        content_type = resp.getheader("Content-Type") or ""
        self.assertIn("text/html", content_type)


class TestHealthAPI(unittest.TestCase):
    """Tests for /api/health endpoint."""

    def test_health_returns_json(self) -> None:
        """/api/health should return valid JSON with required fields."""
        resp = _get("/api/health")
        self.assertEqual(resp.status, 200)
        content_type = resp.getheader("Content-Type") or ""
        self.assertIn("json", content_type.lower())
        body = resp.read().decode("utf-8")
        data = json.loads(body)
        self.assertIn("status", data)


class TestChatCSRF(unittest.TestCase):
    """Tests for /api/chat CSRF enforcement."""

    def test_chat_rejects_without_csrf(self) -> None:
        """/api/chat should reject requests without a CSRF token (403)."""
        body = json.dumps({"message": "hello"}).encode("utf-8")
        resp = _post("/api/chat", body=body)
        self.assertIn(resp.status, (403, 400))


class TestAdminDashboard(unittest.TestCase):
    """Tests for admin dashboard auth enforcement."""

    def test_admin_dashboard_requires_auth(self) -> None:
        """/admin-dashboard should return 401 without auth."""
        resp = _get("/admin-dashboard")
        self.assertIn(resp.status, (401, 403))


class TestProductPages(unittest.TestCase):
    """Test that all product pages return 200."""

    PRODUCT_PATHS: list[str] = [
        "/media-plan",
        "/tracker",
        "/simulator",
        "/vendor-iq",
        "/competitive",
        "/quick-plan",
        "/social-plan",
        "/audit",
        "/hire-signal",
        "/market-pulse",
        "/api-portal",
        "/payscale-sync",
        "/talent-heatmap",
        "/applyflow",
        "/skill-target",
        "/roi-calculator",
        "/ab-testing",
        "/creative-ai",
        "/post-campaign",
        "/market-intel",
        "/quick-brief",
        "/compliance-guard",
        "/nova",
    ]

    def test_all_product_pages_return_200(self) -> None:
        """Every product page should return HTTP 200."""
        for path in self.PRODUCT_PATHS:
            with self.subTest(path=path):
                resp = _get(path)
                self.assertEqual(
                    resp.status,
                    200,
                    f"{path} returned {resp.status} instead of 200",
                )


class TestNotFound(unittest.TestCase):
    """Tests for 404 handling."""

    def test_nonexistent_path_returns_404(self) -> None:
        """A clearly invalid path should return 404."""
        resp = _get("/this-path-does-not-exist-xyz-123")
        self.assertEqual(resp.status, 404)

    def test_random_api_returns_404(self) -> None:
        """A non-existent API endpoint should return 404."""
        resp = _get("/api/nonexistent-endpoint-xyz")
        self.assertIn(resp.status, (404, 405))


class TestSecurityHeaders(unittest.TestCase):
    """Tests for security headers on responses."""

    def test_security_headers_present(self) -> None:
        """Responses should include common security headers."""
        resp = _get("/")
        headers_lower = {k.lower(): v for k, v in resp.getheaders()}

        # X-Content-Type-Options should be nosniff
        x_cto = headers_lower.get("x-content-type-options", "")
        self.assertEqual(x_cto, "nosniff", "Missing X-Content-Type-Options: nosniff")

        # X-Frame-Options should be present
        x_fo = headers_lower.get("x-frame-options", "")
        self.assertTrue(
            len(x_fo) > 0,
            "Missing X-Frame-Options header",
        )


if __name__ == "__main__":
    unittest.main()
