"""Excalidraw diagram generation POST route handler.

Extracted as a route module following the existing pattern.  Handles:
- POST /api/diagram

Generates a simple media plan flow diagram in Excalidraw-compatible JSON.
"""

import json
import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Route dispatch
# ---------------------------------------------------------------------------


def handle_diagram_post_routes(handler: Any, path: str, parsed: Any) -> bool:
    """Dispatch diagram POST routes.  Returns True if handled."""
    if path == "/api/diagram":
        _handle_diagram(handler, path, parsed)
        return True
    return False


# ---------------------------------------------------------------------------
# Individual route handlers
# ---------------------------------------------------------------------------


def _handle_diagram(handler: Any, path: str, parsed: Any) -> None:
    """POST /api/diagram -- generate an Excalidraw diagram for a media plan.

    Request body: {"plan": {"channels": [...], "budget": "$10,000"}}
    Response: {"elements": [...], "type": "excalidraw"}
    """
    try:
        body = handler._read_json_body()
    except (ValueError, json.JSONDecodeError) as exc:
        logger.warning("Diagram: invalid request body: %s", exc)
        handler._send_json({"error": "Invalid JSON body"}, 400)
        return

    plan: dict[str, Any] = body.get("plan") or {}
    channels: list[str] = plan.get("channels") or [
        "Indeed",
        "LinkedIn",
        "Google Jobs",
    ]
    budget: str = plan.get("budget") or "$10,000"

    try:
        elements = _build_flow_diagram(budget, channels[:5])
        handler._send_json({"elements": elements, "type": "excalidraw"})
    except (TypeError, KeyError) as exc:
        logger.error("Diagram generation failed: %s", exc, exc_info=True)
        handler._send_json({"error": "Diagram generation failed"}, 500)


# ---------------------------------------------------------------------------
# Diagram builder
# ---------------------------------------------------------------------------


def _build_flow_diagram(budget: str, channels: list[str]) -> list[dict[str, Any]]:
    """Build Excalidraw-compatible elements for a budget -> channels flow.

    Args:
        budget: Display string for the budget node.
        channels: List of channel names (max 5).

    Returns:
        List of Excalidraw element dicts.
    """
    elements: list[dict[str, Any]] = []
    budget_id = _uid()
    budget_x = 300
    budget_y = 100

    # Budget node (top)
    elements.append(
        {
            "id": budget_id,
            "type": "rectangle",
            "x": budget_x,
            "y": budget_y,
            "width": 200,
            "height": 60,
            "backgroundColor": "#5a54bd",
            "strokeColor": "#5a54bd",
            "fillStyle": "solid",
            "roundness": {"type": 3},
            "label": {"text": f"Budget: {budget}"},
        }
    )

    # Channel nodes (row below)
    channel_y = budget_y + 120
    x_start = 100
    for i, ch in enumerate(channels):
        ch_id = _uid()
        ch_x = x_start + i * 180

        elements.append(
            {
                "id": ch_id,
                "type": "rectangle",
                "x": ch_x,
                "y": channel_y,
                "width": 150,
                "height": 50,
                "backgroundColor": "#6bb3cd",
                "strokeColor": "#6bb3cd",
                "fillStyle": "solid",
                "roundness": {"type": 3},
                "label": {"text": ch},
            }
        )

        # Arrow from budget to channel
        elements.append(
            {
                "id": _uid(),
                "type": "arrow",
                "x": budget_x + 100,
                "y": budget_y + 60,
                "width": ch_x + 75 - (budget_x + 100),
                "height": channel_y - (budget_y + 60),
                "strokeColor": "#5a54bd",
                "startBinding": {"elementId": budget_id, "focus": 0, "gap": 1},
                "endBinding": {"elementId": ch_id, "focus": 0, "gap": 1},
            }
        )

    # Metrics row (below channels)
    metrics_y = channel_y + 100
    metrics_id = _uid()
    metrics_x = budget_x
    elements.append(
        {
            "id": metrics_id,
            "type": "rectangle",
            "x": metrics_x,
            "y": metrics_y,
            "width": 200,
            "height": 50,
            "backgroundColor": "#202058",
            "strokeColor": "#202058",
            "fillStyle": "solid",
            "roundness": {"type": 3},
            "label": {"text": "Performance Metrics"},
        }
    )

    return elements


def _uid() -> str:
    """Generate a short unique ID for Excalidraw elements."""
    return uuid.uuid4().hex[:16]
