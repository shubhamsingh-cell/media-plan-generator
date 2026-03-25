"""Canvas Engine -- backend logic for the Conversational Canvas.

Provides plan-to-canvas transformation, edit application, and health stats.
Thread-safe with per-plan locking for concurrent edits.
"""

import logging
import threading
import time
import uuid
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thread-safe state
# ---------------------------------------------------------------------------
_canvas_lock = threading.Lock()
_plan_locks: dict[str, threading.Lock] = {}
_canvas_states: dict[str, dict[str, Any]] = {}
_canvas_stats = {
    "total_edits": 0,
    "total_parses": 0,
    "active_plans": 0,
    "last_edit_at": None,
}

# Channel color palette (brand-aligned)
_CHANNEL_COLORS: list[str] = [
    "#6BB3CD",  # DOWNY_TEAL
    "#5A54BD",  # BLUE_VIOLET
    "#34D399",  # emerald
    "#F59E0B",  # amber
    "#F87171",  # red
    "#A78BFA",  # purple
    "#60A5FA",  # blue
    "#FBBF24",  # yellow
    "#FB923C",  # orange
    "#2DD4BF",  # teal
]


def _get_plan_lock(plan_id: str) -> threading.Lock:
    """Get or create a per-plan lock for thread-safe edits.

    Args:
        plan_id: Unique plan identifier.

    Returns:
        A threading.Lock for the given plan.
    """
    with _canvas_lock:
        if plan_id not in _plan_locks:
            _plan_locks[plan_id] = threading.Lock()
        return _plan_locks[plan_id]


def parse_plan_for_canvas(plan_data: dict[str, Any]) -> dict[str, Any]:
    """Transform raw plan data into a structure suitable for canvas rendering.

    Extracts channels, budget, and allocation percentages. Assigns
    colors and positions for draggable cards.

    Args:
        plan_data: Raw media plan dict (from /api/generate or saved plans).

    Returns:
        Canvas-ready dict with channels, budget info, and layout metadata.
    """
    with _canvas_lock:
        _canvas_stats["total_parses"] += 1

    plan_id = plan_data.get("plan_id") or plan_data.get("id") or str(uuid.uuid4())
    total_budget = 0
    channels: list[dict[str, Any]] = []

    # Extract channel allocations from various plan formats
    raw_channels = (
        plan_data.get("channels")
        or plan_data.get("channel_allocations")
        or plan_data.get("recommendations")
        or []
    )

    if isinstance(raw_channels, list):
        for i, ch in enumerate(raw_channels):
            name = (
                ch.get("channel")
                or ch.get("name")
                or ch.get("platform")
                or f"Channel {i + 1}"
            )
            spend = float(
                ch.get("spend") or ch.get("budget") or ch.get("allocation") or 0
            )
            total_budget += spend
            channels.append(
                {
                    "id": f"ch_{i}",
                    "name": name,
                    "spend": spend,
                    "color": _CHANNEL_COLORS[i % len(_CHANNEL_COLORS)],
                    "cpc": ch.get("cpc") or ch.get("cost_per_click"),
                    "cpa": ch.get("cpa") or ch.get("cost_per_apply"),
                }
            )
    elif isinstance(raw_channels, dict):
        for i, (name, details) in enumerate(raw_channels.items()):
            spend = (
                float(details.get("spend") or details.get("budget") or 0)
                if isinstance(details, dict)
                else float(details or 0)
            )
            total_budget += spend
            channels.append(
                {
                    "id": f"ch_{i}",
                    "name": name,
                    "spend": spend,
                    "color": _CHANNEL_COLORS[i % len(_CHANNEL_COLORS)],
                    "cpc": details.get("cpc") if isinstance(details, dict) else None,
                    "cpa": details.get("cpa") if isinstance(details, dict) else None,
                }
            )

    # Fallback total budget
    if total_budget == 0:
        total_budget = float(
            plan_data.get("total_budget") or plan_data.get("budget") or 0
        )

    # Calculate percentages
    for ch in channels:
        ch["percentage"] = round(
            (ch["spend"] / total_budget * 100) if total_budget > 0 else 0, 1
        )

    canvas_state: dict[str, Any] = {
        "plan_id": plan_id,
        "total_budget": total_budget,
        "allocated": sum(ch["spend"] for ch in channels),
        "remaining": max(0, total_budget - sum(ch["spend"] for ch in channels)),
        "channels": channels,
        "industry": plan_data.get("industry") or "",
        "role": plan_data.get("role") or plan_data.get("job_title") or "",
        "location": plan_data.get("location") or "",
        "suggestions": _generate_suggestions(channels, total_budget),
        "version": 1,
        "updated_at": time.time(),
    }

    # Store state
    lock = _get_plan_lock(plan_id)
    with lock:
        _canvas_states[plan_id] = canvas_state

    with _canvas_lock:
        _canvas_stats["active_plans"] = len(_canvas_states)

    return canvas_state


def apply_canvas_edit(plan_id: str, edit: dict[str, Any]) -> dict[str, Any]:
    """Apply a drag/slider edit from the canvas to the plan state.

    Supports edit types:
    - reallocate: change a channel's percentage/spend
    - add_channel: add a new channel
    - remove_channel: remove a channel
    - rename_channel: rename a channel
    - set_budget: change total budget

    Args:
        plan_id: The plan to edit.
        edit: Dict with 'type' and type-specific fields.

    Returns:
        Updated canvas state dict, or error dict.
    """
    lock = _get_plan_lock(plan_id)
    with lock:
        state = _canvas_states.get(plan_id)
        if not state:
            return {"error": f"Plan {plan_id} not found", "status": "error"}

        edit_type = edit.get("type") or ""
        channels = state["channels"]
        total_budget = state["total_budget"]

        try:
            if edit_type == "reallocate":
                channel_id = edit.get("channel_id") or ""
                new_pct = float(edit.get("percentage") or 0)
                new_pct = max(0, min(100, new_pct))

                target = next((c for c in channels if c["id"] == channel_id), None)
                if not target:
                    return {
                        "error": f"Channel {channel_id} not found",
                        "status": "error",
                    }

                old_pct = target["percentage"]
                delta = new_pct - old_pct

                # Redistribute delta proportionally among other channels
                others = [c for c in channels if c["id"] != channel_id]
                other_total = sum(c["percentage"] for c in others)

                if other_total > 0 and abs(delta) > 0.01:
                    for c in others:
                        ratio = c["percentage"] / other_total
                        c["percentage"] = round(
                            max(0, c["percentage"] - delta * ratio), 1
                        )

                target["percentage"] = round(new_pct, 1)

                # Recalculate spend from percentages
                for c in channels:
                    c["spend"] = round(total_budget * c["percentage"] / 100, 2)

                state["change_log"] = {
                    "type": "reallocate",
                    "channel": target["name"],
                    "from_pct": round(old_pct, 1),
                    "to_pct": round(new_pct, 1),
                }

            elif edit_type == "add_channel":
                name = edit.get("name") or "New Channel"
                pct = float(edit.get("percentage") or 10)
                idx = len(channels)
                new_ch: dict[str, Any] = {
                    "id": f"ch_{idx}",
                    "name": name,
                    "spend": round(total_budget * pct / 100, 2),
                    "percentage": round(pct, 1),
                    "color": _CHANNEL_COLORS[idx % len(_CHANNEL_COLORS)],
                    "cpc": None,
                    "cpa": None,
                }
                channels.append(new_ch)

                # Scale down others
                scale = (100 - pct) / 100 if pct < 100 else 0
                for c in channels[:-1]:
                    c["percentage"] = round(c["percentage"] * scale, 1)
                    c["spend"] = round(total_budget * c["percentage"] / 100, 2)

                state["change_log"] = {"type": "add_channel", "channel": name}

            elif edit_type == "remove_channel":
                channel_id = edit.get("channel_id") or ""
                removed = next((c for c in channels if c["id"] == channel_id), None)
                if not removed:
                    return {
                        "error": f"Channel {channel_id} not found",
                        "status": "error",
                    }

                freed_pct = removed["percentage"]
                channels.remove(removed)

                # Redistribute freed percentage
                remaining_total = sum(c["percentage"] for c in channels)
                if remaining_total > 0:
                    for c in channels:
                        ratio = c["percentage"] / remaining_total
                        c["percentage"] = round(c["percentage"] + freed_pct * ratio, 1)
                        c["spend"] = round(total_budget * c["percentage"] / 100, 2)

                state["change_log"] = {
                    "type": "remove_channel",
                    "channel": removed["name"],
                }

            elif edit_type == "rename_channel":
                channel_id = edit.get("channel_id") or ""
                new_name = edit.get("name") or ""
                target = next((c for c in channels if c["id"] == channel_id), None)
                if not target:
                    return {
                        "error": f"Channel {channel_id} not found",
                        "status": "error",
                    }
                old_name = target["name"]
                target["name"] = new_name
                state["change_log"] = {
                    "type": "rename_channel",
                    "from": old_name,
                    "to": new_name,
                }

            elif edit_type == "set_budget":
                new_budget = float(edit.get("budget") or 0)
                if new_budget <= 0:
                    return {"error": "Budget must be positive", "status": "error"}
                old_budget = total_budget
                state["total_budget"] = new_budget
                total_budget = new_budget
                for c in channels:
                    c["spend"] = round(new_budget * c["percentage"] / 100, 2)
                state["change_log"] = {
                    "type": "set_budget",
                    "from": old_budget,
                    "to": new_budget,
                }

            else:
                return {"error": f"Unknown edit type: {edit_type}", "status": "error"}

            # Update metadata
            state["allocated"] = sum(c["spend"] for c in channels)
            state["remaining"] = max(0, total_budget - state["allocated"])
            state["version"] = state.get("version", 0) + 1
            state["updated_at"] = time.time()
            state["suggestions"] = _generate_suggestions(channels, total_budget)

        except (ValueError, TypeError) as e:
            logger.error("Canvas edit error: %s", e, exc_info=True)
            return {"error": f"Invalid edit data: {e}", "status": "error"}

    with _canvas_lock:
        _canvas_stats["total_edits"] += 1
        _canvas_stats["last_edit_at"] = time.time()

    return state


def get_canvas_state(plan_id: str) -> Optional[dict[str, Any]]:
    """Retrieve the current canvas state for a plan.

    Args:
        plan_id: The plan identifier.

    Returns:
        Canvas state dict, or None if not found.
    """
    lock = _get_plan_lock(plan_id)
    with lock:
        return _canvas_states.get(plan_id)


def get_canvas_stats() -> dict[str, Any]:
    """Return canvas engine statistics for /api/health.

    Returns:
        Dict with edit counts, active plans, and uptime info.
    """
    with _canvas_lock:
        return {
            "status": "ok",
            "total_edits": _canvas_stats["total_edits"],
            "total_parses": _canvas_stats["total_parses"],
            "active_plans": _canvas_stats["active_plans"],
            "last_edit_at": _canvas_stats["last_edit_at"],
            "cached_plans": len(_canvas_states),
        }


def _generate_suggestions(
    channels: list[dict[str, Any]], total_budget: float
) -> list[dict[str, Any]]:
    """Generate inline AI suggestions based on current allocation.

    Analyzes channel distribution and produces actionable suggestions
    for budget optimization.

    Args:
        channels: List of channel dicts with percentage/spend.
        total_budget: Total plan budget.

    Returns:
        List of suggestion dicts with text, channel_id, and suggested_pct.
    """
    suggestions: list[dict[str, Any]] = []
    if not channels or total_budget <= 0:
        return suggestions

    # Find over-concentrated channels (> 40%)
    for ch in channels:
        if ch["percentage"] > 40:
            suggestions.append(
                {
                    "id": f"sug_{ch['id']}_reduce",
                    "text": f"Consider reducing {ch['name']} from {ch['percentage']}% -- high concentration risk",
                    "channel_id": ch["id"],
                    "type": "warning",
                    "suggested_pct": 35.0,
                }
            )

    # Find under-allocated channels (< 5% but present)
    for ch in channels:
        if 0 < ch["percentage"] < 5:
            suggestions.append(
                {
                    "id": f"sug_{ch['id']}_boost",
                    "text": f"Try increasing {ch['name']} by 5% for better channel diversity",
                    "channel_id": ch["id"],
                    "type": "tip",
                    "suggested_pct": ch["percentage"] + 5,
                }
            )

    # Suggest adding social if not present
    social_names = {"linkedin", "facebook", "instagram", "twitter", "social media"}
    has_social = any(ch["name"].lower() in social_names for ch in channels)
    if not has_social and len(channels) < 6:
        suggestions.append(
            {
                "id": "sug_add_social",
                "text": "Consider adding LinkedIn for professional recruitment reach",
                "channel_id": None,
                "type": "add",
                "suggested_pct": 15.0,
            }
        )

    # Suggest programmatic if budget is large enough
    prog_names = {"programmatic", "programmatic ads", "display"}
    has_prog = any(ch["name"].lower() in prog_names for ch in channels)
    if not has_prog and total_budget >= 50000:
        suggestions.append(
            {
                "id": "sug_add_programmatic",
                "text": "With this budget, programmatic ads could improve reach by 20-30%",
                "channel_id": None,
                "type": "add",
                "suggested_pct": 10.0,
            }
        )

    return suggestions[:5]  # Cap at 5 suggestions
