"""ATS Widget -- Embeddable Nova AI Plan Suggestions widget utilities.

Provides embed code generation and health/stats reporting for the
Nova ATS Widget that ATS platforms embed to show inline plan
recommendations.
"""

import html
import logging
import threading
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# ── Widget usage tracking (thread-safe) ──────────────────────────────────

_lock = threading.Lock()
_embed_requests: int = 0
_last_embed_ts: float = 0.0

_WIDGET_VERSION = "1.0.0"
_WIDGET_SCRIPT_PATH = "/static/nova-ats-widget.js"
_DEFAULT_API_ENDPOINT = "https://media-plan-generator.onrender.com"


def generate_embed_code(
    config: Optional[Dict[str, Any]] = None,
) -> str:
    """Generate the HTML embed snippet for the Nova ATS Widget.

    Returns a ready-to-paste ``<script>`` block that loads the widget JS
    and initialises it with the supplied configuration.

    Args:
        config: Optional dict with keys:
            - apiEndpoint (str): Base URL of the Nova API.
            - jobTitle (str): Target job title.
            - location (str): Job location.
            - budget (int|float): Monthly budget in USD.
            - theme (str): ``'light'`` or ``'dark'``.
            - position (str): ``'bottom-right'`` or ``'bottom-left'``.

    Returns:
        A string containing the full ``<script>`` embed snippet.
    """
    global _embed_requests, _last_embed_ts

    cfg = config or {}
    api_endpoint = cfg.get("apiEndpoint") or _DEFAULT_API_ENDPOINT
    job_title = cfg.get("jobTitle") or ""
    location = cfg.get("location") or ""
    budget = cfg.get("budget") or 5000
    theme = cfg.get("theme") or "light"
    position = cfg.get("position") or "bottom-right"

    # Sanitise values for safe embedding in HTML
    api_endpoint_safe = html.escape(str(api_endpoint), quote=True)
    job_title_safe = html.escape(str(job_title), quote=True)
    location_safe = html.escape(str(location), quote=True)
    theme_safe = "dark" if theme == "dark" else "light"
    position_safe = "bottom-left" if position == "bottom-left" else "bottom-right"

    try:
        budget_safe = int(float(budget))
    except (ValueError, TypeError):
        budget_safe = 5000

    script_url = f"{api_endpoint_safe}{_WIDGET_SCRIPT_PATH}"

    embed = (
        f"<!-- Nova ATS Widget v{_WIDGET_VERSION} -->\n"
        f'<script src="{script_url}"></script>\n'
        f"<script>\n"
        f"  NovaATS.init({{\n"
        f'    apiEndpoint: "{api_endpoint_safe}",\n'
        f'    jobTitle: "{job_title_safe}",\n'
        f'    location: "{location_safe}",\n'
        f"    budget: {budget_safe},\n"
        f'    theme: "{theme_safe}",\n'
        f'    position: "{position_safe}"\n'
        f"  }});\n"
        f"</script>"
    )

    # Track usage
    with _lock:
        _embed_requests += 1
        _last_embed_ts = time.time()

    return embed


def get_widget_stats() -> Dict[str, Any]:
    """Return widget health and usage statistics for ``/api/health``.

    Returns:
        Dict with widget version, embed request count, and
        last-served timestamp.
    """
    with _lock:
        embed_count = _embed_requests
        last_ts = _last_embed_ts

    return {
        "status": "ok",
        "version": _WIDGET_VERSION,
        "script_path": _WIDGET_SCRIPT_PATH,
        "embed_requests": embed_count,
        "last_embed_served": last_ts if last_ts > 0 else None,
    }
