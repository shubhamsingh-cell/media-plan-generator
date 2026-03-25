"""Canvas route handlers (GET + POST).

Handles:
- GET  /api/canvas/state/<plan_id> -- get current canvas state
- POST /api/canvas/edit            -- apply a canvas edit
"""

import json
import logging
from typing import Any

from routes.utils import send_json_response, read_json_body

logger = logging.getLogger(__name__)


def handle_canvas_get_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch canvas GET routes. Returns True if handled."""
    if path.startswith("/api/canvas/state/"):
        _handle_canvas_state(handler, path, parsed)
        return True
    return False


def handle_canvas_post_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch canvas POST routes. Returns True if handled."""
    if path == "/api/canvas/edit":
        _handle_canvas_edit(handler, path, parsed)
        return True
    return False


def _handle_canvas_state(handler: Any, path: str, parsed: Any) -> None:
    """GET /api/canvas/state/<plan_id> -- return current canvas state for a plan."""
    try:
        from canvas_engine import get_canvas_state, parse_plan_for_canvas

        plan_id = path.split("/api/canvas/state/", 1)[1].strip("/")
        if not plan_id:
            send_json_response(handler, {"error": "Missing plan_id"}, status_code=400)
            return

        state = get_canvas_state(plan_id)
        if state:
            send_json_response(handler, state)
        else:
            send_json_response(
                handler,
                {"error": f"Plan {plan_id} not found in canvas cache"},
                status_code=404,
            )
    except ImportError:
        send_json_response(
            handler,
            {"error": "Canvas engine not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Canvas state error: %s", e, exc_info=True)
        send_json_response(
            handler,
            {"error": f"Failed to get canvas state: {e}"},
            status_code=500,
        )


def _handle_canvas_edit(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/canvas/edit -- apply a drag/slider edit to the plan."""
    try:
        from canvas_engine import apply_canvas_edit, parse_plan_for_canvas

        body = read_json_body(handler)
        if body is None:
            return

        plan_id = body.get("plan_id") or ""
        edit = body.get("edit")

        if not plan_id:
            send_json_response(handler, {"error": "Missing plan_id"}, status_code=400)
            return
        if not edit or not isinstance(edit, dict):
            send_json_response(
                handler, {"error": "Missing or invalid edit object"}, status_code=400
            )
            return

        # If plan not in cache, try to parse from provided plan_data
        from canvas_engine import get_canvas_state

        if not get_canvas_state(plan_id):
            plan_data = body.get("plan_data")
            if plan_data and isinstance(plan_data, dict):
                plan_data["plan_id"] = plan_id
                parse_plan_for_canvas(plan_data)
            else:
                send_json_response(
                    handler,
                    {
                        "error": f"Plan {plan_id} not found. Provide plan_data to initialize."
                    },
                    status_code=404,
                )
                return

        result = apply_canvas_edit(plan_id, edit)
        status = 200 if "error" not in result else 400
        send_json_response(handler, result, status_code=status)

    except ImportError:
        send_json_response(
            handler,
            {"error": "Canvas engine not available"},
            status_code=503,
        )
    except Exception as e:
        logger.error("Canvas edit error: %s", e, exc_info=True)
        send_json_response(
            handler,
            {"error": f"Canvas edit failed: {e}"},
            status_code=500,
        )
