"""Copilot suggestion POST route handler.

Extracted from app.py to reduce its size.  Handles:
- POST /api/copilot/suggest

The suggestion logic functions (_copilot_suggest, _copilot_suggest_roles, etc.)
remain in app.py as module-level functions since they depend on many app-level
globals (API clients, benchmark_registry, LLM router). This module only
contains the HTTP handler that delegates to those functions.
"""

import json
import logging
import re
import sys
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_copilot_post_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch copilot POST routes.  Returns True if handled."""
    if path == "/api/copilot/suggest":
        _handle_copilot_suggest(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# Individual route handlers
# ---------------------------------------------------------------------------


def _handle_copilot_suggest(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/copilot/suggest -- inline co-pilot suggestions."""
    _app = sys.modules.get("__main__") or sys.modules.get("app")
    _rl_copilot = getattr(_app, "_rl_copilot", None)

    # Rate limiting
    if _rl_copilot:
        client_ip = handler.client_address[0]
        if not _rl_copilot.is_allowed(client_ip, max_requests=30, window_seconds=60):
            handler.send_response(429)
            handler.send_header("Content-Type", "application/json")
            cors_origin = (
                handler._get_cors_origin()
                if hasattr(handler, "_get_cors_origin")
                else None
            )
            if cors_origin:
                handler.send_header("Access-Control-Allow-Origin", cors_origin)
            handler.end_headers()
            handler.wfile.write(
                json.dumps({"error": "Rate limit exceeded. Please slow down."}).encode()
            )
            return

    # Read body
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
    except (ValueError, TypeError):
        content_len = 0
    if content_len <= 0 or content_len > 65536:
        handler.send_response(400 if content_len <= 0 else 413)
        handler.send_header("Content-Type", "application/json")
        cors_origin = (
            handler._get_cors_origin() if hasattr(handler, "_get_cors_origin") else None
        )
        if cors_origin:
            handler.send_header("Access-Control-Allow-Origin", cors_origin)
        handler.end_headers()
        handler.wfile.write(json.dumps({"error": "Invalid request size"}).encode())
        return

    body = handler.rfile.read(content_len)
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        handler._send_json({"error": "Invalid JSON"}, status_code=400)
        return
    if not isinstance(data, dict):
        handler._send_json(
            {"error": "Request body must be a JSON object"}, status_code=400
        )
        return

    # Validate field
    field = (data.get("field") or "").strip()
    if field not in ("roles", "locations", "channels", "brief"):
        handler._send_json(
            {
                "error": "Invalid field. Must be one of: roles, locations, channels, brief"
            },
            status_code=400,
        )
        return

    partial_input = (data.get("partial_input") or "").strip()[:500]
    ctx = data.get("context") or {}
    if not isinstance(ctx, dict):
        ctx = {}
    # Sanitize context values
    for _ck in list(ctx.keys()):
        if isinstance(ctx[_ck], str):
            ctx[_ck] = re.sub(r"<[^>]+>", "", ctx[_ck]).strip()

    # Call the suggestion function from app module
    _copilot_suggest = getattr(_app, "_copilot_suggest", None)
    if not _copilot_suggest:
        handler._send_json(
            {"suggestions": [], "field": field, "error": "Copilot not available"},
        )
        return

    try:
        suggestions = _copilot_suggest(field, partial_input, ctx)
        handler._send_json({"suggestions": suggestions, "field": field})
    except Exception as e:
        logger.error("Copilot suggest endpoint error: %s", e, exc_info=True)
        handler._send_json(
            {
                "suggestions": [],
                "field": field,
                "error": "Suggestion service temporarily unavailable",
            }
        )
