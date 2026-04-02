#!/usr/bin/env python3
"""Event-Sourced Plan State Machine.

Every plan change is an immutable event, enabling full history replay,
undo/redo, and branch/merge of plan versions.

Event types:
    PlanCreated, BudgetChanged, ChannelAdded, ChannelRemoved,
    AllocationChanged, LocationChanged, RoleChanged,
    PlanForked, PlanMerged, PlanFinalized

Each event carries:
    event_id (UUID), plan_id, event_type, timestamp, user_id,
    payload (dict), version (auto-incrementing per plan)

The EventStore class provides:
    append()        -- record an immutable event
    get_events()    -- full history for a plan
    get_snapshot()  -- reconstruct plan state by replaying events
    undo()          -- create a compensating event
    fork()          -- copy event stream to a new plan
    get_stats()     -- summary for /api/health
"""

import copy
import datetime
import json
import logging
import threading
import uuid
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# Event Types
# ═══════════════════════════════════════════════════════════════════════════════


class EventType(str, Enum):
    """All recognised plan event types."""

    PLAN_CREATED = "PlanCreated"
    BUDGET_CHANGED = "BudgetChanged"
    CHANNEL_ADDED = "ChannelAdded"
    CHANNEL_REMOVED = "ChannelRemoved"
    ALLOCATION_CHANGED = "AllocationChanged"
    LOCATION_CHANGED = "LocationChanged"
    ROLE_CHANGED = "RoleChanged"
    PLAN_FORKED = "PlanForked"
    PLAN_MERGED = "PlanMerged"
    PLAN_FINALIZED = "PlanFinalized"
    UNDO = "Undo"


# ═══════════════════════════════════════════════════════════════════════════════
# Event Data Class
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass(frozen=True)
class Event:
    """Immutable event record in the plan event stream."""

    event_id: str
    plan_id: str
    event_type: str
    timestamp: str
    user_id: str
    payload: dict
    version: int

    def to_dict(self) -> dict[str, Any]:
        """Serialise event to a plain dictionary."""
        return asdict(self)


# ═══════════════════════════════════════════════════════════════════════════════
# Snapshot Reducers
# ═══════════════════════════════════════════════════════════════════════════════


def _apply_event(state: dict[str, Any], event: Event) -> dict[str, Any]:
    """Apply a single event to *state* (mutated in-place) and return it.

    Each event type has its own reducer logic.  Unknown event types are
    stored in ``state["_unknown_events"]`` for forward-compatibility.
    """
    etype = event.event_type
    payload = event.payload

    if etype == EventType.PLAN_CREATED:
        state["plan_id"] = event.plan_id
        state["budget"] = payload.get("budget", 0)
        state["channels"] = list(payload.get("channels") or [])
        state["allocations"] = dict(payload.get("allocations") or {})
        state["location"] = payload.get("location") or ""
        state["role"] = payload.get("role") or ""
        state["finalized"] = False
        state["created_at"] = event.timestamp
        state["created_by"] = event.user_id
        state["forked_from"] = None
        state["merged_from"] = []

    elif etype == EventType.BUDGET_CHANGED:
        state["budget"] = payload.get("new_budget", state.get("budget", 0))

    elif etype == EventType.CHANNEL_ADDED:
        channel = payload.get("channel") or ""
        if channel and channel not in state.get("channels", []):
            state.setdefault("channels", []).append(channel)

    elif etype == EventType.CHANNEL_REMOVED:
        channel = payload.get("channel") or ""
        channels: list[str] = state.get("channels", [])
        if channel in channels:
            channels.remove(channel)
        # Also remove allocation for the channel
        state.get("allocations", {}).pop(channel, None)

    elif etype == EventType.ALLOCATION_CHANGED:
        channel = payload.get("channel") or ""
        allocation = payload.get("allocation", 0)
        if channel:
            state.setdefault("allocations", {})[channel] = allocation

    elif etype == EventType.LOCATION_CHANGED:
        state["location"] = payload.get("new_location") or ""

    elif etype == EventType.ROLE_CHANGED:
        state["role"] = payload.get("new_role") or ""

    elif etype == EventType.PLAN_FORKED:
        state["forked_from"] = payload.get("source_plan_id") or ""

    elif etype == EventType.PLAN_MERGED:
        merged_id = payload.get("merged_plan_id") or ""
        state.setdefault("merged_from", []).append(merged_id)
        # Merge channels and allocations from the merged plan snapshot
        for ch in payload.get("merged_channels") or []:
            if ch not in state.get("channels", []):
                state.setdefault("channels", []).append(ch)
        for ch, alloc in (payload.get("merged_allocations") or {}).items():
            state.setdefault("allocations", {})[ch] = alloc

    elif etype == EventType.PLAN_FINALIZED:
        state["finalized"] = True

    elif etype == EventType.UNDO:
        # Compensating event: restore the snapshot embedded in the payload
        restored: dict[str, Any] = payload.get("restored_state") or {}
        for key, value in restored.items():
            state[key] = value

    else:
        state.setdefault("_unknown_events", []).append(event.to_dict())

    # Track latest version
    state["_version"] = event.version
    state["_last_updated"] = event.timestamp
    state["_last_updated_by"] = event.user_id

    return state


# ═══════════════════════════════════════════════════════════════════════════════
# EventStore
# ═══════════════════════════════════════════════════════════════════════════════


class EventStore:
    """Thread-safe, in-memory event store with optional Supabase persistence.

    Parameters
    ----------
    persist_to_supabase : bool
        When ``True``, events are also written to the ``plan_events``
        Supabase table (best-effort; failures are logged, never raised).
    """

    def __init__(self, persist_to_supabase: bool = False) -> None:
        self._lock = threading.Lock()
        # plan_id -> list[Event]  (append-only)
        self._streams: dict[str, list[Event]] = {}
        # plan_id -> latest version counter
        self._versions: dict[str, int] = {}
        # Global counters
        self._total_events: int = 0
        self._created_at: str = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._persist = persist_to_supabase

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def append(
        self,
        plan_id: str,
        event_type: str,
        payload: dict[str, Any],
        *,
        user_id: str = "system",
    ) -> Event:
        """Record an immutable event for *plan_id*.

        Returns the created ``Event`` with an auto-assigned version.
        """
        with self._lock:
            version = self._versions.get(plan_id, 0) + 1
            self._versions[plan_id] = version

            event = Event(
                event_id=uuid.uuid4().hex,
                plan_id=plan_id,
                event_type=event_type,
                timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                user_id=user_id,
                payload=payload,
                version=version,
            )

            self._streams.setdefault(plan_id, []).append(event)
            self._total_events += 1

        # Best-effort Supabase persistence (outside lock)
        if self._persist:
            self._persist_event(event)

        logger.debug(f"Event appended: plan={plan_id} type={event_type} v={version}")
        return event

    def get_events(
        self,
        plan_id: str,
        *,
        since_version: int = 0,
    ) -> list[Event]:
        """Return the full (or partial) event history for *plan_id*.

        When the in-memory stream is empty and Supabase persistence is
        enabled, lazily loads events from the ``plan_events`` table so
        that undo/redo works correctly across gunicorn workers and deploys.

        Parameters
        ----------
        since_version : int
            If >0, only return events with ``version > since_version``.
        """
        with self._lock:
            stream = self._streams.get(plan_id)

        # Lazy-load from Supabase when in-memory cache is empty
        if not stream and self._persist:
            loaded = self._load_events_from_supabase(plan_id)
            if loaded:
                with self._lock:
                    # Double-check: another thread may have loaded already
                    if not self._streams.get(plan_id):
                        self._streams[plan_id] = loaded
                        # Rebuild version counter and total from loaded events
                        max_version = max(e.version for e in loaded)
                        self._versions[plan_id] = max_version
                        self._total_events += len(loaded)
                        logger.info(
                            f"Lazy-loaded {len(loaded)} events for plan={plan_id} "
                            f"from Supabase (max_version={max_version})"
                        )
                    stream = list(self._streams.get(plan_id) or [])
            else:
                stream = []
        else:
            with self._lock:
                stream = list(self._streams.get(plan_id) or [])

        if since_version > 0:
            stream = [e for e in stream if e.version > since_version]
        return stream

    def get_snapshot(
        self,
        plan_id: str,
        *,
        at_version: Optional[int] = None,
    ) -> dict[str, Any]:
        """Reconstruct plan state by replaying events.

        Parameters
        ----------
        at_version : int or None
            Replay events up to (and including) this version.  ``None``
            means replay all events to get the latest state.

        Returns
        -------
        dict
            The reconstructed plan state dictionary.
        """
        events = self.get_events(plan_id)
        if at_version is not None:
            events = [e for e in events if e.version <= at_version]

        state: dict[str, Any] = {}
        for event in events:
            _apply_event(state, event)
        return state

    def undo(self, plan_id: str, *, user_id: str = "system") -> Event:
        """Undo the last meaningful change by appending a compensating event.

        The compensating event restores the snapshot as of ``version - 1``
        (i.e. the state *before* the last event).

        Raises
        ------
        ValueError
            If there are no events or only a ``PlanCreated`` event.
        """
        # Ensure events are loaded (triggers Supabase lazy-load if needed)
        stream = self.get_events(plan_id)
        if len(stream) == 0:
            raise ValueError(f"No events exist for plan {plan_id}")

        last_event = stream[-1]
        if last_event.event_type == EventType.PLAN_CREATED:
            raise ValueError("Cannot undo plan creation")

        # Snapshot before the last event
        target_version = last_event.version - 1
        restored_state = self.get_snapshot(plan_id, at_version=target_version)

        # Strip internal tracking keys from the restored state
        clean_state = {k: v for k, v in restored_state.items() if not k.startswith("_")}

        compensating = self.append(
            plan_id,
            EventType.UNDO,
            {
                "undone_event_id": last_event.event_id,
                "undone_event_type": last_event.event_type,
                "undone_version": last_event.version,
                "restored_state": clean_state,
            },
            user_id=user_id,
        )

        logger.info(
            f"Undo: plan={plan_id} undone_type={last_event.event_type} "
            f"undone_v={last_event.version} compensating_v={compensating.version}"
        )
        return compensating

    def fork(
        self,
        source_plan_id: str,
        new_plan_id: str,
        *,
        user_id: str = "system",
    ) -> Event:
        """Fork a plan by copying its event stream to a new plan.

        Creates a ``PlanCreated`` event on the new plan with the source
        plan's current snapshot, then appends a ``PlanForked`` marker.

        Raises
        ------
        ValueError
            If the source plan has no events or the new plan already exists.
        """
        source_events = self.get_events(source_plan_id)
        if not source_events:
            raise ValueError(f"Source plan {source_plan_id} has no events")

        with self._lock:
            if new_plan_id in self._streams:
                raise ValueError(f"Plan {new_plan_id} already exists")

        # Get source snapshot
        snapshot = self.get_snapshot(source_plan_id)
        clean_snapshot = {
            k: copy.deepcopy(v) for k, v in snapshot.items() if not k.startswith("_")
        }

        # Create the new plan with the forked snapshot
        self.append(
            new_plan_id,
            EventType.PLAN_CREATED,
            clean_snapshot,
            user_id=user_id,
        )

        # Append fork marker on the new plan
        fork_event = self.append(
            new_plan_id,
            EventType.PLAN_FORKED,
            {"source_plan_id": source_plan_id},
            user_id=user_id,
        )

        logger.info(f"Fork: {source_plan_id} -> {new_plan_id} by {user_id}")
        return fork_event

    def merge(
        self,
        target_plan_id: str,
        source_plan_id: str,
        *,
        user_id: str = "system",
    ) -> Event:
        """Merge another plan's state into *target_plan_id*.

        Appends a ``PlanMerged`` event carrying the source plan's
        channels and allocations.

        Raises
        ------
        ValueError
            If either plan has no events.
        """
        source_snapshot = self.get_snapshot(source_plan_id)
        if not source_snapshot:
            raise ValueError(f"Source plan {source_plan_id} has no events")

        target_events = self.get_events(target_plan_id)
        if not target_events:
            raise ValueError(f"Target plan {target_plan_id} has no events")

        merge_event = self.append(
            target_plan_id,
            EventType.PLAN_MERGED,
            {
                "merged_plan_id": source_plan_id,
                "merged_channels": source_snapshot.get("channels", []),
                "merged_allocations": source_snapshot.get("allocations", {}),
            },
            user_id=user_id,
        )

        logger.info(f"Merge: {source_plan_id} -> {target_plan_id} by {user_id}")
        return merge_event

    # ------------------------------------------------------------------
    # Stats / health
    # ------------------------------------------------------------------

    def get_stats(self) -> dict[str, Any]:
        """Return summary statistics for /api/health."""
        with self._lock:
            plan_count = len(self._streams)
            total = self._total_events
            plans_summary: dict[str, int] = {
                pid: len(evts) for pid, evts in self._streams.items()
            }
        return {
            "status": "ok",
            "total_plans": plan_count,
            "total_events": total,
            "created_at": self._created_at,
            "plans": plans_summary,
        }

    # ------------------------------------------------------------------
    # Supabase persistence (best-effort, fire-and-forget)
    # ------------------------------------------------------------------
    # The ``plan_events`` Supabase table is the durable source of truth.
    # In-memory ``_streams`` acts as an L1 cache for fast snapshot replay
    # within a single gunicorn worker.  On cache miss (e.g. after deploy
    # or when a different worker handles the request), events are lazy-
    # loaded from Supabase.  Writes are fire-and-forget via daemon threads
    # so they never slow down the main request path.

    def _persist_event(self, event: Event) -> None:
        """Write event to Supabase ``plan_events`` table (fire-and-forget).

        Spawns a daemon thread so the main request path is never blocked.
        All errors are caught and logged -- Supabase downtime degrades
        gracefully to in-memory-only operation.
        """
        t = threading.Thread(
            target=self._persist_event_sync,
            args=(event,),
            daemon=True,
        )
        t.start()

    def _persist_event_sync(self, event: Event) -> None:
        """Synchronous Supabase write (runs in daemon thread)."""
        try:
            from supabase_client import get_client

            client = get_client()
            if client is None:
                return

            client.table("plan_events").insert(event.to_dict()).execute()
            logger.debug(f"Persisted event {event.event_id} to Supabase")
        except ImportError:
            logger.debug("supabase_client not available; skipping persistence")
        except Exception as exc:
            logger.error(
                f"Failed to persist event {event.event_id} to Supabase: {exc}",
                exc_info=True,
            )

    def _load_events_from_supabase(self, plan_id: str) -> list[Event]:
        """Load all events for *plan_id* from Supabase (blocking).

        Returns an ordered list of ``Event`` objects, or an empty list
        if Supabase is unavailable or the plan has no persisted events.
        """
        try:
            from supabase_client import get_client

            client = get_client()
            if client is None:
                return []

            response = (
                client.table("plan_events")
                .select("*")
                .eq("plan_id", plan_id)
                .order("version", desc=False)
                .execute()
            )

            rows = response.data or []
            if not rows:
                return []

            events: list[Event] = []
            for row in rows:
                # Deserialise payload from JSON string if needed
                payload = row.get("payload") or {}
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except (json.JSONDecodeError, TypeError):
                        payload = {}

                events.append(
                    Event(
                        event_id=row["event_id"],
                        plan_id=row["plan_id"],
                        event_type=row["event_type"],
                        timestamp=row["timestamp"],
                        user_id=row["user_id"],
                        payload=payload,
                        version=row["version"],
                    )
                )

            return events
        except ImportError:
            logger.debug("supabase_client not available; skipping load")
            return []
        except Exception as exc:
            logger.error(
                f"Failed to load events for plan={plan_id} from Supabase: {exc}",
                exc_info=True,
            )
            return []


# ═══════════════════════════════════════════════════════════════════════════════
# Singleton
# ═══════════════════════════════════════════════════════════════════════════════

_event_store: Optional[EventStore] = None
_store_lock = threading.Lock()


def get_event_store() -> EventStore:
    """Return the global EventStore singleton (lazy-initialised)."""
    global _event_store
    if _event_store is None:
        with _store_lock:
            if _event_store is None:
                # Enable Supabase persistence when env var is set
                import os

                persist = bool(
                    (os.environ.get("SUPABASE_URL") or "").strip()
                    and (
                        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
                        or os.environ.get("SUPABASE_KEY")
                        or ""
                    ).strip()
                )
                _event_store = EventStore(persist_to_supabase=persist)
                logger.info(f"EventStore initialised (supabase_persist={persist})")
    return _event_store


def get_event_store_stats() -> dict[str, Any]:
    """Convenience wrapper for /api/health integration."""
    try:
        return get_event_store().get_stats()
    except Exception as exc:
        logger.error(f"EventStore stats failed: {exc}", exc_info=True)
        return {"status": "error", "error": str(exc)}
