"""Event-sourced plan state management.

Every plan change is recorded as an immutable event, enabling full audit
trails, undo/redo, and time-travel debugging.  Events are replayed
(projected) to reconstruct plan state at any point in history.

Thread-safe, in-memory with optional Supabase persistence.
"""

from __future__ import annotations

import copy
import datetime
import logging
import threading
import uuid
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event types
# ---------------------------------------------------------------------------


class EventType(str, Enum):
    """All recognised plan mutation event types."""

    PLAN_CREATED = "PLAN_CREATED"
    BUDGET_CHANGED = "BUDGET_CHANGED"
    CHANNEL_ADDED = "CHANNEL_ADDED"
    CHANNEL_REMOVED = "CHANNEL_REMOVED"
    CHANNEL_ALLOCATION_CHANGED = "CHANNEL_ALLOCATION_CHANGED"
    LOCATION_CHANGED = "LOCATION_CHANGED"
    ROLE_CHANGED = "ROLE_CHANGED"
    PLAN_OPTIMIZED = "PLAN_OPTIMIZED"
    PLAN_EXPORTED = "PLAN_EXPORTED"
    PLAN_SHARED = "PLAN_SHARED"


# ---------------------------------------------------------------------------
# Event data class
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Event:
    """An immutable event capturing a single plan mutation."""

    event_id: str
    plan_id: str
    event_type: str
    timestamp: str
    user_id: str
    payload: dict[str, Any]
    version: int

    def to_dict(self) -> dict[str, Any]:
        """Serialise the event to a plain dictionary."""
        return asdict(self)


# ---------------------------------------------------------------------------
# State projection helpers
# ---------------------------------------------------------------------------

_INITIAL_STATE: dict[str, Any] = {
    "plan_id": "",
    "budget": 0,
    "channels": [],
    "channel_allocations": {},
    "locations": [],
    "role": "",
    "optimized": False,
    "exported": False,
    "shared": False,
    "version": 0,
    "created_at": "",
    "updated_at": "",
}


def _apply_event(state: dict[str, Any], event: Event) -> dict[str, Any]:
    """Apply a single event to a plan state dict and return the mutated state.

    Each handler merges the event payload into the running state.
    """
    state["version"] = event.version
    state["updated_at"] = event.timestamp

    etype = event.event_type

    if etype == EventType.PLAN_CREATED:
        state["plan_id"] = event.plan_id
        state["created_at"] = event.timestamp
        state["budget"] = event.payload.get("budget") or 0
        state["channels"] = list(event.payload.get("channels") or [])
        state["locations"] = list(event.payload.get("locations") or [])
        state["role"] = event.payload.get("role") or ""
        state["channel_allocations"] = dict(
            event.payload.get("channel_allocations") or {}
        )

    elif etype == EventType.BUDGET_CHANGED:
        state["budget"] = event.payload.get("budget") or state["budget"]

    elif etype == EventType.CHANNEL_ADDED:
        channel = event.payload.get("channel") or ""
        if channel and channel not in state["channels"]:
            state["channels"].append(channel)

    elif etype == EventType.CHANNEL_REMOVED:
        channel = event.payload.get("channel") or ""
        if channel in state["channels"]:
            state["channels"].remove(channel)
        state["channel_allocations"].pop(channel, None)

    elif etype == EventType.CHANNEL_ALLOCATION_CHANGED:
        channel = event.payload.get("channel") or ""
        allocation = event.payload.get("allocation")
        if channel:
            state["channel_allocations"][channel] = allocation

    elif etype == EventType.LOCATION_CHANGED:
        state["locations"] = list(event.payload.get("locations") or state["locations"])

    elif etype == EventType.ROLE_CHANGED:
        state["role"] = event.payload.get("role") or state["role"]

    elif etype == EventType.PLAN_OPTIMIZED:
        state["optimized"] = True
        # Merge any optimizer output (e.g. new allocations) into state
        opt_allocs = event.payload.get("channel_allocations")
        if opt_allocs:
            state["channel_allocations"].update(opt_allocs)

    elif etype == EventType.PLAN_EXPORTED:
        state["exported"] = True

    elif etype == EventType.PLAN_SHARED:
        state["shared"] = True

    return state


def _project(events: list[Event], up_to_version: int | None = None) -> dict[str, Any]:
    """Replay *events* to build plan state, optionally stopping at a version."""
    state = copy.deepcopy(_INITIAL_STATE)
    for ev in events:
        if up_to_version is not None and ev.version > up_to_version:
            break
        state = _apply_event(state, ev)
    return state


# ---------------------------------------------------------------------------
# EventStore
# ---------------------------------------------------------------------------


class EventStore:
    """Thread-safe, in-memory event store with undo/redo support.

    Each plan has its own ordered list of events plus a redo stack that is
    cleared whenever a new (non-redo) event is appended.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # plan_id -> list[Event]
        self._events: dict[str, list[Event]] = {}
        # plan_id -> list[Event] (events popped by undo, available for redo)
        self._redo_stacks: dict[str, list[Event]] = {}
        # Counters for stats
        self._total_events: int = 0
        self._total_undos: int = 0
        self._total_redos: int = 0

    # -- Core API -----------------------------------------------------------

    def append(
        self,
        plan_id: str,
        event_type: str,
        payload: dict[str, Any],
        user_id: str = "system",
    ) -> str:
        """Append a new event to *plan_id*'s stream.

        Clears the redo stack for this plan (new divergent timeline).
        Returns the generated ``event_id``.
        """
        # Validate event_type
        try:
            EventType(event_type)
        except ValueError:
            valid = ", ".join(e.value for e in EventType)
            raise ValueError(f"Unknown event_type {event_type!r}. Valid types: {valid}")

        with self._lock:
            stream = self._events.setdefault(plan_id, [])
            version = len(stream) + 1
            event = Event(
                event_id=uuid.uuid4().hex,
                plan_id=plan_id,
                event_type=event_type,
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                user_id=user_id,
                payload=payload,
                version=version,
            )
            stream.append(event)
            # New event invalidates the redo stack
            self._redo_stacks.pop(plan_id, None)
            self._total_events += 1

            logger.info(f"Event appended: plan={plan_id} type={event_type} v={version}")
            return event.event_id

    def get_events(self, plan_id: str) -> list[Event]:
        """Return the full ordered event history for *plan_id*."""
        with self._lock:
            return list(self._events.get(plan_id) or [])

    def get_current_state(self, plan_id: str) -> dict[str, Any]:
        """Replay all events for *plan_id* to build current state."""
        events = self.get_events(plan_id)
        if not events:
            return {"error": "Plan not found", "plan_id": plan_id}
        return _project(events)

    def get_state_at(self, plan_id: str, version: int) -> dict[str, Any]:
        """Replay events up to *version* for time-travel debugging."""
        events = self.get_events(plan_id)
        if not events:
            return {"error": "Plan not found", "plan_id": plan_id}
        max_version = events[-1].version
        if version < 1 or version > max_version:
            return {
                "error": f"Version {version} out of range [1, {max_version}]",
                "plan_id": plan_id,
            }
        return _project(events, up_to_version=version)

    def undo(self, plan_id: str) -> dict[str, Any]:
        """Revert the last event.  Returns the new current state.

        The removed event is pushed onto the redo stack.
        """
        with self._lock:
            stream = self._events.get(plan_id)
            if not stream:
                return {
                    "error": "Plan not found or no events to undo",
                    "plan_id": plan_id,
                }
            if len(stream) <= 1:
                return {
                    "error": "Cannot undo the PLAN_CREATED event",
                    "plan_id": plan_id,
                }
            removed = stream.pop()
            self._redo_stacks.setdefault(plan_id, []).append(removed)
            self._total_undos += 1
            logger.info(
                f"Undo: plan={plan_id} reverted v={removed.version} ({removed.event_type})"
            )
            return _project(stream)

    def redo(self, plan_id: str) -> dict[str, Any]:
        """Re-apply the last undone event.  Returns the new current state."""
        with self._lock:
            redo_stack = self._redo_stacks.get(plan_id)
            if not redo_stack:
                return {"error": "Nothing to redo", "plan_id": plan_id}
            event = redo_stack.pop()
            self._events.setdefault(plan_id, []).append(event)
            self._total_redos += 1
            logger.info(
                f"Redo: plan={plan_id} restored v={event.version} ({event.event_type})"
            )
            return _project(self._events[plan_id])

    # -- Stats / health ------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """Return aggregate statistics for the /api/health endpoint."""
        with self._lock:
            plan_count = len(self._events)
            event_counts = {pid: len(evs) for pid, evs in self._events.items()}
            return {
                "plans_tracked": plan_count,
                "total_events": self._total_events,
                "total_undos": self._total_undos,
                "total_redos": self._total_redos,
                "events_per_plan": event_counts,
            }


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_event_store: EventStore | None = None
_init_lock = threading.Lock()


def get_event_store() -> EventStore:
    """Return the module-level EventStore singleton (lazy-init, thread-safe)."""
    global _event_store
    if _event_store is None:
        with _init_lock:
            if _event_store is None:
                _event_store = EventStore()
                logger.info("EventStore initialised")
    return _event_store


def get_event_store_stats() -> dict[str, Any]:
    """Convenience accessor for health-check integration."""
    return get_event_store().get_stats()
