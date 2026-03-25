"""HTML page-serving GET route handlers.

Extracted from app.py to reduce its size.  Handles all static HTML page
routes that serve templates from the templates/ directory.  Returns
``True`` if the route was handled.
"""

import logging
import os
import sys
from typing import Any

logger = logging.getLogger(__name__)


def _get_dirs() -> tuple[str, str]:
    """Return (BASE_DIR, TEMPLATES_DIR) from app module."""
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    base_dir = getattr(_app, "BASE_DIR", os.path.dirname(os.path.abspath(__file__)))
    templates_dir = getattr(_app, "TEMPLATES_DIR", os.path.join(base_dir, "templates"))
    return base_dir, templates_dir


# ---------------------------------------------------------------------------
# Page route table: maps URL paths to template filenames
# ---------------------------------------------------------------------------

# Simple page routes: (url_variants) -> template_filename
_PAGE_ROUTES: list[tuple[tuple[str, ...], str]] = [
    (("/media-plan", "/media-plan/", "/generator", "/generator/"), "index.html"),
    (("/platform", "/platform/"), "platform.html"),
    (("/health-dashboard", "/health-dashboard/"), "health-dashboard.html"),
    (
        ("/tracker", "/tracker/", "/performance-tracker", "/performance-tracker/"),
        "tracker.html",
    ),
    (
        (
            "/simulator",
            "/simulator/",
            "/budget-simulator",
            "/budget-simulator/",
            "/budget-engine",
            "/budget-engine/",
        ),
        "simulator.html",
    ),
    (("/vendor-iq", "/vendor-iq/"), "vendor-iq.html"),
    (
        ("/competitive", "/competitive/", "/competitive-intel", "/competitive-intel/"),
        "competitive.html",
    ),
    (("/quick-plan", "/quick-plan/"), "quick-plan.html"),
    (("/social-plan", "/social-plan/"), "social-plan.html"),
    (("/audit", "/audit/"), "audit.html"),
    (
        ("/hire-signal", "/hire-signal/", "/hiresignal", "/hiresignal/"),
        "hire-signal.html",
    ),
    (("/market-pulse", "/market-pulse/"), "market-pulse.html"),
    (("/api-portal", "/api-portal/"), "api-portal.html"),
    (
        ("/payscale-sync", "/payscale-sync/", "/payscale", "/payscale/"),
        "payscale-sync.html",
    ),
    (("/talent-heatmap", "/talent-heatmap/"), "talent-heatmap.html"),
    (("/applyflow", "/applyflow/"), "applyflow-demo.html"),
    (
        ("/skill-target", "/skill-target/", "/skilltarget", "/skilltarget/"),
        "skill-target.html",
    ),
    (("/roi-calculator", "/roi-calculator/"), "roi-calculator.html"),
    (("/ab-testing", "/ab-testing/", "/abtesting", "/abtesting/"), "ab-testing.html"),
    (
        ("/creative-ai", "/creative-ai/", "/creativeai", "/creativeai/"),
        "creative-ai.html",
    ),
    (("/post-campaign", "/post-campaign/"), "post-campaign.html"),
    (
        (
            "/market-intel",
            "/market-intel/",
            "/market-intelligence",
            "/market-intelligence/",
            "/market-intel-reports",
            "/market-intel-reports/",
        ),
        "market-intel.html",
    ),
    (
        ("/quick-brief", "/quick-brief/", "/quickbrief", "/quickbrief/"),
        "quick-brief.html",
    ),
    (
        (
            "/compliance-guard",
            "/compliance-guard/",
            "/complianceguard",
            "/complianceguard/",
        ),
        "compliance-guard.html",
    ),
    (("/pricing", "/pricing/"), "pricing.html"),
    (("/privacy", "/privacy/"), "privacy.html"),
    (("/terms", "/terms/"), "terms.html"),
]

# Build a fast lookup dict: path -> template_filename
_PAGE_LOOKUP: dict[str, str] = {}
for _variants, _template in _PAGE_ROUTES:
    for _v in _variants:
        _PAGE_LOOKUP[_v] = _template

# Admin-protected page routes: path -> template_filename
_ADMIN_PAGE_ROUTES: dict[str, str] = {
    "/dashboard": "dashboard.html",
    "/observability": "observability.html",
    "/admin-dashboard": "admin-dashboard.html",
}

# Redirect routes: path -> target
_REDIRECT_ROUTES: dict[str, str] = {
    "/nova-jarvis": "/nova",
    "/nova-jarvis/": "/nova",
    "/auto-qc": "/hub",
    "/auto-qc/": "/hub",
    "/eval-framework": "/hub",
    "/eval-framework/": "/hub",
}


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_page_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch HTML page GET routes.  Returns True if handled."""
    base_dir, templates_dir = _get_dirs()

    # Hub / root
    if path == "/" or path == "" or path in ("/hub", "/hub/"):
        handler._serve_file(os.path.join(templates_dir, "hub.html"), "text/html")
        return True

    # Simple template pages (no auth required)
    template = _PAGE_LOOKUP.get(path)
    if template:
        _serve_template(handler, templates_dir, template)
        return True

    # Nova page (special handling)
    if path in ("/nova", "/nova/"):
        nova_html = os.path.join(base_dir, "templates", "nova.html")
        if os.path.exists(nova_html):
            with open(nova_html, "r") as f:
                html = f.read()
            handler.send_response(200)
            handler.send_header("Content-Type", "text/html; charset=utf-8")
            handler.end_headers()
            handler.wfile.write(html.encode())
        else:
            handler.send_error(404, "Nova page not found")
        return True

    # Admin-protected pages
    admin_template = _ADMIN_PAGE_ROUTES.get(path)
    if admin_template:
        if not handler._check_admin_auth():
            handler.send_error(
                401, "Unauthorized - set ADMIN_API_KEY env var and pass ?key=..."
            )
            return True
        _serve_template(handler, templates_dir, admin_template)
        return True

    # Admin Nova dashboard (requires admin + static dir)
    if path in ("/admin/nova", "/admin/nova/"):
        if not handler._check_admin_auth():
            handler.send_response(401)
            handler.send_header("Content-Type", "text/html; charset=utf-8")
            handler.end_headers()
            handler.wfile.write(
                b"<h1>401 Unauthorized</h1><p>Set ADMIN_API_KEY env var and pass ?key=YOUR_KEY</p>"
            )
            return True
        nova_admin = os.path.join(base_dir, "static", "nova-admin.html")
        if os.path.exists(nova_admin):
            with open(nova_admin, "r") as f:
                html = f.read()
            handler.send_response(200)
            handler.send_header("Content-Type", "text/html; charset=utf-8")
            handler.end_headers()
            handler.wfile.write(html.encode())
        else:
            handler.send_error(404, "Nova admin page not found")
        return True

    # Redirects
    target = _REDIRECT_ROUTES.get(path)
    if target:
        handler.send_response(301)
        handler.send_header("Location", target)
        handler.end_headers()
        return True

    return False


def _serve_template(handler: Any, templates_dir: str, template: str) -> None:
    """Serve a template file from the templates directory.

    Args:
        handler: The MediaPlanHandler instance.
        templates_dir: Path to templates directory.
        template: Template filename.
    """
    html_path = os.path.join(templates_dir, template)
    if os.path.exists(html_path):
        with open(html_path, "r") as f:
            html = f.read()
        handler.send_response(200)
        handler.send_header("Content-Type", "text/html; charset=utf-8")
        handler.end_headers()
        handler.wfile.write(html.encode())
    else:
        handler.send_error(404, f"{template} not found")
