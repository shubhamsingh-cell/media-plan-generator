"""Shared utilities for route handlers.

Provides common helpers that all route modules can import, avoiding
circular dependencies with app.py.
"""

import json
import logging
import sys
from typing import Any, Optional

logger = logging.getLogger(__name__)


def send_json_response(handler: Any, data: Any, status_code: int = 200) -> None:
    """Send a JSON response using the handler's _send_json or raw HTTP.

    Prefers handler._send_json() when available (handles CORS, compression).
    Falls back to raw HTTP response for compatibility.

    Args:
        handler: The MediaPlanHandler instance.
        data: Data to serialize as JSON.
        status_code: HTTP status code (default 200).
    """
    if hasattr(handler, "_send_json"):
        handler._send_json(data, status_code=status_code)
    else:
        body = json.dumps(data).encode("utf-8")
        handler.send_response(status_code)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)


def read_json_body(handler: Any, max_size: int = 1048576) -> Optional[dict]:
    """Read and parse a JSON body from the request.

    Args:
        handler: The MediaPlanHandler instance.
        max_size: Maximum body size in bytes (default 1 MB).

    Returns:
        Parsed dict, or None if parsing fails (error response already sent).
    """
    try:
        content_len = int(handler.headers.get("Content-Length") or 0)
    except (ValueError, TypeError):
        content_len = 0

    if content_len <= 0:
        send_json_response(handler, {"error": "Empty request body"}, status_code=400)
        return None

    if content_len > max_size:
        send_json_response(handler, {"error": "Request too large"}, status_code=413)
        return None

    body = handler.rfile.read(content_len)
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        send_json_response(handler, {"error": "Invalid JSON"}, status_code=400)
        return None

    if not isinstance(data, dict):
        send_json_response(
            handler, {"error": "Request body must be a JSON object"}, status_code=400
        )
        return None

    return data


def get_app_module() -> Any:
    """Get the main app module for accessing globals.

    Returns:
        The app module (either __main__ or 'app').
    """
    return sys.modules.get("__main__") or sys.modules.get("app")


def get_app_attr(name: str, default: Any = None) -> Any:
    """Get an attribute from the main app module.

    Args:
        name: Attribute name.
        default: Default value if not found.

    Returns:
        The attribute value, or default.
    """
    _app = get_app_module()
    return getattr(_app, name, default)
