"""SlotOps route handlers -- LinkedIn Slot Optimization (Product #6).

Handles all /api/slotops/* POST/GET endpoints and the /slotops page route.
Returns ``True`` if the route was handled, ``False`` otherwise.
"""

import logging
from typing import Any
from urllib.parse import parse_qs, urlparse

from routes.utils import read_json_body, send_json_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy-load slotops_engine (graceful degradation if module missing)
# ---------------------------------------------------------------------------
_engine = None
_engine_checked = False


def _lazy_engine() -> Any:
    """Lazy-load the slotops_engine module with caching."""
    global _engine, _engine_checked
    if _engine_checked:
        return _engine
    try:
        import slotops_engine as mod

        mod.load_baselines()
        _engine = mod
        logger.info("SlotOps engine loaded successfully")
    except ImportError:
        logger.warning("slotops_engine module not available; SlotOps disabled")
        _engine = None
    except Exception as exc:
        logger.error("Failed to initialize slotops_engine: %s", exc, exc_info=True)
        _engine = None
    _engine_checked = True
    return _engine


# ---------------------------------------------------------------------------
# GET route handler
# ---------------------------------------------------------------------------


def handle_slotops_get_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Handle SlotOps GET API routes.

    Args:
        handler: The MediaPlanHandler instance.
        path: The cleaned request path.
        parsed: Parsed URL object.

    Returns:
        True if route was handled, False otherwise.
    """
    if path == "/api/slotops/dashboard":
        engine = _lazy_engine()
        if not engine:
            send_json_response(
                handler,
                {"error": "SlotOps engine not available"},
                status_code=503,
            )
            return True
        try:
            qs = parse_qs(parsed.query)
            params = {k: v[0] for k, v in qs.items()}
            result = engine.handle_slotops_dashboard(params)
            send_json_response(handler, result)
        except Exception as exc:
            logger.error("SlotOps dashboard error: %s", exc, exc_info=True)
            send_json_response(
                handler,
                {"error": f"Dashboard error: {exc}"},
                status_code=500,
            )
        return True

    if path == "/api/slotops/template":
        import os

        template_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data",
            "slotops_upload_template.xlsx",
        )
        try:
            with open(template_path, "rb") as f:
                data = f.read()
            handler.send_response(200)
            handler.send_header(
                "Content-Type",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            handler.send_header(
                "Content-Disposition",
                'attachment; filename="SlotOps_Upload_Template.xlsx"',
            )
            handler.send_header("Content-Length", str(len(data)))
            handler.end_headers()
            handler.wfile.write(data)
        except FileNotFoundError:
            send_json_response(
                handler, {"error": "Template not found"}, status_code=404
            )
        return True

    if path == "/api/slotops/baselines":
        engine = _lazy_engine()
        if not engine:
            send_json_response(
                handler,
                {"error": "SlotOps engine not available"},
                status_code=503,
            )
            return True
        try:
            qs = parse_qs(parsed.query)
            params = {k: v[0] for k, v in qs.items()}
            result = engine.handle_slotops_baselines(params)
            send_json_response(handler, result)
        except Exception as exc:
            logger.error("SlotOps baselines error: %s", exc, exc_info=True)
            send_json_response(
                handler,
                {"error": f"Baselines error: {exc}"},
                status_code=500,
            )
        return True

    return False


# ---------------------------------------------------------------------------
# POST route handler
# ---------------------------------------------------------------------------


def handle_slotops_post_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Handle SlotOps POST API routes.

    Args:
        handler: The MediaPlanHandler instance.
        path: The cleaned request path.
        parsed: Parsed URL object.

    Returns:
        True if route was handled, False otherwise.
    """
    if not path.startswith("/api/slotops/"):
        return False

    engine = _lazy_engine()
    if not engine:
        send_json_response(
            handler,
            {"error": "SlotOps engine not available"},
            status_code=503,
        )
        return True

    body = read_json_body(handler)
    if body is None:
        return True  # Error already sent by read_json_body

    _dispatch: dict[str, Any] = {
        "/api/slotops/optimize": engine.handle_slotops_optimize,
        "/api/slotops/predict": engine.handle_slotops_predict,
        "/api/slotops/schedule": engine.handle_slotops_schedule,
        "/api/slotops/export": engine.handle_slotops_export,
        "/api/slotops/insights": engine.handle_slotops_insights,
        "/api/slotops/upload": engine.handle_slotops_upload,
        "/api/slotops/daily-actions": engine.handle_slotops_daily_actions,
        "/api/slotops/quick-wins": engine.handle_slotops_quick_wins,
        "/api/slotops/analyze": engine.handle_slotops_analyze,
    }

    handler_fn = _dispatch.get(path)
    if handler_fn is None:
        return False

    try:
        result = handler_fn(body)
        send_json_response(handler, result)
    except Exception as exc:
        logger.error("SlotOps %s error: %s", path, exc, exc_info=True)
        send_json_response(
            handler,
            {"error": f"SlotOps error: {exc}"},
            status_code=500,
        )
    return True
