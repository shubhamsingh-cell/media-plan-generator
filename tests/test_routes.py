"""Route coverage tests -- verify route patterns exist in app.py."""

import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class TestRouteExtraction:
    """Parse app.py and verify route coverage."""

    def test_can_extract_routes(self, app_source: str) -> None:
        """app.py should contain recognizable route patterns."""
        route_patterns = re.findall(r'path\s*==\s*"(/[^"]*)"', app_source)
        assert (
            len(route_patterns) > 10
        ), f"Expected 10+ routes, found {len(route_patterns)}"

    def test_root_route_exists(self, app_source: str) -> None:
        """The root route (/) must be handled."""
        assert (
            'path == "/"' in app_source or 'path == "/"' in app_source
        ), "Root route '/' not found in app.py"


class TestNewProductRoutes:
    """New product routes that were added must be present."""

    NEW_PRODUCT_ROUTES = [
        "/compliance-guard",
        "/payscale-sync",
        "/creative-ai",
        "/vendor-iq",
    ]

    @pytest.mark.parametrize("route", NEW_PRODUCT_ROUTES)
    def test_new_product_route_exists(self, route: str, app_source: str) -> None:
        """Each new product route must be handled in app.py."""
        assert route in app_source, f"Route {route} not found in app.py"


class TestCoreRoutes:
    """Core application routes must exist."""

    CORE_ROUTES = [
        "/dashboard",
        "/observability",
        "/docs",
        "/robots.txt",
        "/sitemap.xml",
    ]

    @pytest.mark.parametrize("route", CORE_ROUTES)
    def test_core_route_exists(self, route: str, app_source: str) -> None:
        """Each core route must be handled in app.py."""
        assert route in app_source, f"Core route {route} not found in app.py"


class TestAPIRoutes:
    """Key API endpoints must exist."""

    API_ROUTES = [
        "/api/generate",
        "/api/chat",
        "/api/channels",
        "/api/health",
        "/api/metrics",
        "/api/simulator/simulate",
        "/api/simulator/compare",
        "/api/tracker/analyze",
    ]

    @pytest.mark.parametrize("route", API_ROUTES)
    def test_api_route_exists(self, route: str, app_source: str) -> None:
        """Each API route must be referenced in app.py."""
        assert route in app_source, f"API route {route} not found in app.py"


class TestTemplateRouteMapping:
    """Each template should have a corresponding route in app.py."""

    # Map template filenames to their expected route paths
    # Some templates are served via different route patterns
    TEMPLATE_ROUTE_MAP = {
        "hub.html": ["/hub", "hub.html"],
        "index.html": ['path == "/"', "index.html"],
        "dashboard.html": ["/dashboard"],
        "observability.html": ["/observability"],
        "compliance-guard.html": ["/compliance-guard"],
        "payscale-sync.html": ["/payscale-sync"],
        "creative-ai.html": ["/creative-ai"],
        "vendor-iq.html": ["/vendor-iq"],
    }

    @pytest.mark.parametrize(
        "template,route_markers",
        TEMPLATE_ROUTE_MAP.items(),
        ids=TEMPLATE_ROUTE_MAP.keys(),
    )
    def test_template_has_route(
        self, template: str, route_markers: list[str], app_source: str
    ) -> None:
        """Each template file should be referenced or routed in app.py."""
        found = any(marker in app_source for marker in route_markers)
        assert found, (
            f"Template {template} has no matching route in app.py "
            f"(looked for: {route_markers})"
        )
