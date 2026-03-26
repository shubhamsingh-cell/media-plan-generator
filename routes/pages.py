"""HTML page-serving GET route handlers.

Extracted from app.py to reduce its size.  Handles all static HTML page
routes that serve templates from the templates/ directory.  Returns
``True`` if the route was handled.
"""

import logging
import os
import re
import sys
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Platform sub-route SEO metadata
# ---------------------------------------------------------------------------
# Maps /platform/<section> paths to their SEO metadata and initial JS route.
# The initial_route value is injected into the platform template so the
# frontend router navigates directly to the correct section on page load.
_PLATFORM_SUB_ROUTES: dict[str, dict[str, str]] = {
    "plan": {
        "title": "Plan | Nova Platform - AI Campaign Planning Suite",
        "description": "AI-powered campaign planning tools: full media plans, quick plans, creative briefs, budget simulator, and A/B testing.",
        "initial_route": "plan/campaign",
    },
    "intelligence": {
        "title": "Intelligence | Nova Platform - Market & Competitive Intel",
        "description": "Real-time competitive monitoring, market pulse, vendor analysis, and talent intelligence for recruitment advertising.",
        "initial_route": "intelligence/competitive",
    },
    "compliance": {
        "title": "Compliance | Nova Platform - Regulatory Compliance Tools",
        "description": "Automated compliance checks, ad audits, and regulatory monitoring for recruitment advertising campaigns.",
        "initial_route": "compliance/comply",
    },
    "nova": {
        "title": "Nova AI | Nova Platform - AI Assistant",
        "description": "Nova AI assistant for recruitment intelligence, campaign analysis, and market insights.",
        "initial_route": "nova",
    },
}


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

    # Platform sub-routes: /platform/plan, /platform/intelligence, etc.
    # Serves the platform SPA shell with the correct initial route injected
    # so the frontend router navigates to the right section on page load.
    if path.startswith("/platform/"):
        section = path[len("/platform/") :].strip("/").split("/")[0]
        sub_route_meta = _PLATFORM_SUB_ROUTES.get(section)
        if sub_route_meta:
            # Build the full sub-path (may include deeper routes like /platform/plan/budget)
            sub_path = path[len("/platform/") :].strip("/")
            _serve_platform_with_route(handler, templates_dir, sub_path, sub_route_meta)
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

    Uses the template composer for templates that have partials (split templates).
    Falls back to direct file read for monolithic templates.

    Args:
        handler: The MediaPlanHandler instance.
        templates_dir: Path to templates directory.
        template: Template filename.
    """
    # Check if this template has been split into partials
    page_name = template.rsplit(".", 1)[0]  # "index.html" -> "index"
    try:
        from template_composer import get_composed_template

        composed = get_composed_template(page_name)
        if composed is not None:
            handler.send_response(200)
            handler.send_header("Content-Type", "text/html; charset=utf-8")
            handler.send_header("Content-Length", str(len(composed)))
            handler.end_headers()
            handler.wfile.write(composed)
            return
    except ImportError:
        pass  # Composer not available, fall back to direct read

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


_BASE_URL = "https://media-plan-generator.onrender.com"


def _serve_platform_with_route(
    handler: Any,
    templates_dir: str,
    sub_path: str,
    meta: dict[str, str],
) -> None:
    """Serve the platform template with SEO metadata and initial route injected.

    Replaces the generic <title>, <meta description>, and <link canonical>
    with section-specific values, and injects a __NOVA_INITIAL_ROUTE variable
    so the frontend router navigates to the correct section on page load.

    Args:
        handler: The MediaPlanHandler instance.
        templates_dir: Path to templates directory.
        sub_path: The sub-path after /platform/ (e.g. "plan", "plan/budget").
        meta: SEO metadata dict with 'title', 'description', 'initial_route'.
    """
    html_path = os.path.join(templates_dir, "platform.html")
    if not os.path.exists(html_path):
        handler.send_error(404, "platform.html not found")
        return

    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
    except OSError as e:
        logger.error("Failed to read platform.html: %s", e, exc_info=True)
        handler.send_error(500, "Failed to read platform template")
        return

    # Determine the actual route to pass to the frontend.
    # If the URL is /platform/plan/budget, use "plan/budget" directly.
    # If just /platform/plan, use the default initial_route from metadata.
    section_root = sub_path.split("/")[0]
    if "/" in sub_path:
        # Deeper path like plan/budget or intelligence/talent/hire-signal
        initial_route = sub_path
    else:
        # Top-level section: use the default first module route
        initial_route = meta["initial_route"]

    # Canonical URL always points to the clean section URL
    canonical_url = f"{_BASE_URL}/platform/{section_root}"

    # Replace <title>
    html = re.sub(
        r"<title>[^<]*</title>",
        f"<title>{meta['title']}</title>",
        html,
        count=1,
    )

    # Replace meta description
    html = re.sub(
        r'<meta\s+name="description"\s+content="[^"]*"\s*/?>',
        f'<meta name="description" content="{meta["description"]}" />',
        html,
        count=1,
    )

    # Replace canonical URL
    html = re.sub(
        r'<link\s+rel="canonical"\s+href="[^"]*"\s*/?>',
        f'<link rel="canonical" href="{canonical_url}" />',
        html,
        count=1,
    )

    # Inject initial route variable before the closing </head> tag.
    # The frontend router reads this to navigate on page load instead of
    # relying on hash fragments.
    route_script = (
        f'<script>window.__NOVA_INITIAL_ROUTE = "{initial_route}";</script>\n'
    )
    html = html.replace("</head>", f"{route_script}</head>", 1)

    body = html.encode("utf-8")
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)
