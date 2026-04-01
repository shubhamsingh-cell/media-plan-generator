#!/usr/bin/env python3
"""Persistent memory for Nova AI chatbot.

Stores conversation summaries and user preferences across sessions.
Injects relevant memory into system prompts for continuity.

Architecture:
- Short-term: Last 5 conversation summaries (injected every call)
- Long-term: User preferences, campaign history, learned facts
- User profile: Extracted roles, locations, industries from queries
- Storage: Supabase nova_memory table + local fallback
"""

import json
import logging
import os
import re
import time
import threading
from collections import Counter, deque
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_MAX_SHORT_TERM = 5  # Last N conversation summaries to inject
_MAX_LONG_TERM = 50  # Max long-term memory entries
_SUMMARY_MAX_CHARS = 500  # Max chars per conversation summary


class NovaMemory:
    """Persistent memory manager for Nova AI."""

    def __init__(self, user_id: str = "default"):
        self._user_id = user_id
        self._short_term: deque = deque(maxlen=_MAX_SHORT_TERM)
        self._long_term: list = []
        self._preferences: dict = {}
        self._lock = threading.Lock()
        self._loaded = False

    def load(self) -> None:
        """Load memory from Supabase or local fallback."""
        if self._loaded:
            return

        # Try Supabase first
        try:
            from supabase_client import get_client

            client = get_client()
            if client:
                # Load conversation summaries
                result = (
                    client.table("nova_memory")
                    .select("*")
                    .eq("user_id", self._user_id)
                    .order("created_at", desc=True)
                    .limit(_MAX_SHORT_TERM + _MAX_LONG_TERM)
                    .execute()
                )

                if result and result.data:
                    for row in result.data:
                        mem_type = row.get("memory_type") or "short_term"
                        entry = {
                            "id": row.get("id"),
                            "content": row.get("content") or "",
                            "memory_type": mem_type,
                            "created_at": row.get("created_at"),
                            "metadata": row.get("metadata") or {},
                        }
                        if mem_type == "short_term":
                            self._short_term.append(entry)
                        elif mem_type == "long_term":
                            self._long_term.append(entry)
                        elif mem_type == "preference":
                            key = (row.get("metadata") or {}).get("key") or ""
                            if key:
                                self._preferences[key] = entry["content"]

                    logger.info(
                        "[NovaMemory] Loaded %d short-term, %d long-term, %d preferences from Supabase",
                        len(self._short_term),
                        len(self._long_term),
                        len(self._preferences),
                    )
                    self._loaded = True
                    return
        except Exception as e:
            logger.debug("[NovaMemory] Supabase load failed, trying local: %s", e)

        # Local fallback
        try:
            mem_file = os.path.join("data", f"nova_memory_{self._user_id}.json")
            if os.path.exists(mem_file):
                with open(mem_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                for entry in data.get("short_term") or []:
                    self._short_term.append(entry)
                self._long_term = data.get("long_term") or []
                self._preferences = data.get("preferences") or {}
                logger.info("[NovaMemory] Loaded from local file")
        except Exception as e:
            logger.debug("[NovaMemory] Local load failed: %s", e)

        self._loaded = True

    def save_conversation_summary(
        self, conversation_id: str, messages: list, summary: str = ""
    ) -> None:
        """Save a conversation summary to memory.

        Args:
            conversation_id: Unique conversation ID.
            messages: List of message dicts [{role, text}].
            summary: Pre-computed summary, or auto-generate from messages.
        """
        if not summary:
            # Auto-generate summary from last few messages
            summary = self._auto_summarize(messages)

        if not summary:
            return

        entry = {
            "content": summary[:_SUMMARY_MAX_CHARS],
            "memory_type": "short_term",
            "created_at": time.time(),
            "metadata": {
                "conversation_id": conversation_id,
                "message_count": len(messages),
            },
        }

        with self._lock:
            self._short_term.append(entry)

        # Persist async
        threading.Thread(target=self._persist_entry, args=(entry,), daemon=True).start()

    def learn_fact(self, fact: str, category: str = "general") -> None:
        """Store a long-term fact learned from conversation.

        Examples: "User prefers LinkedIn for tech roles"
                  "Client Acme Corp budget is $50K/quarter"
        """
        entry = {
            "content": fact[:_SUMMARY_MAX_CHARS],
            "memory_type": "long_term",
            "created_at": time.time(),
            "metadata": {"category": category},
        }

        with self._lock:
            # Deduplicate
            existing = [e for e in self._long_term if e["content"] == fact]
            if not existing:
                self._long_term.append(entry)
                if len(self._long_term) > _MAX_LONG_TERM:
                    self._long_term = self._long_term[-_MAX_LONG_TERM:]

        threading.Thread(target=self._persist_entry, args=(entry,), daemon=True).start()

    def set_preference(self, key: str, value: str) -> None:
        """Store a user preference."""
        with self._lock:
            self._preferences[key] = value

        entry = {
            "content": value,
            "memory_type": "preference",
            "created_at": time.time(),
            "metadata": {"key": key},
        }
        threading.Thread(target=self._persist_entry, args=(entry,), daemon=True).start()

    def get_context_injection(self) -> str:
        """Get memory context to inject into LLM system prompt.

        Returns a formatted string suitable for appending to the system prompt.
        """
        parts = []

        with self._lock:
            # Preferences
            if self._preferences:
                prefs = "; ".join(f"{k}: {v}" for k, v in self._preferences.items())
                parts.append(f"User preferences: {prefs}")

            # Long-term facts
            if self._long_term:
                facts = [e["content"] for e in self._long_term[-10:]]
                parts.append("Known facts:\n- " + "\n- ".join(facts))

            # Recent conversation summaries
            if self._short_term:
                summaries = [e["content"] for e in self._short_term]
                parts.append(
                    "Recent conversation context:\n" + "\n---\n".join(summaries)
                )

        if not parts:
            return ""

        return (
            "\n\n[MEMORY - Previous sessions]\n" + "\n\n".join(parts) + "\n[/MEMORY]\n"
        )

    def _auto_summarize(self, messages: list) -> str:
        """Auto-generate a conversation summary from messages."""
        if not messages:
            return ""

        # Extract key info from last few messages
        recent = messages[-6:]  # Last 3 exchanges
        parts = []
        for msg in recent:
            role = msg.get("role") or ""
            text = (msg.get("text") or msg.get("content") or "")[:200]
            if role == "user":
                parts.append(f"User asked: {text}")
            elif role in ("nova", "assistant"):
                parts.append(f"Nova answered about: {text[:100]}")

        return " | ".join(parts)[:_SUMMARY_MAX_CHARS]

    def _persist_entry(self, entry: dict) -> None:
        """Persist a memory entry to Supabase or local file."""
        # Try Supabase
        try:
            from supabase_client import get_client

            client = get_client()
            if client:
                client.table("nova_memory").insert(
                    {
                        "user_id": self._user_id,
                        "content": entry["content"],
                        "memory_type": entry["memory_type"],
                        "metadata": json.dumps(entry.get("metadata") or {}),
                    }
                ).execute()
                return
        except Exception as e:
            logger.debug("[NovaMemory] Supabase persist failed: %s", e)

        # Local fallback
        try:
            mem_file = os.path.join("data", f"nova_memory_{self._user_id}.json")
            existing: dict = {"short_term": [], "long_term": [], "preferences": {}}
            if os.path.exists(mem_file):
                with open(mem_file, "r", encoding="utf-8") as f:
                    existing = json.load(f)

            if entry["memory_type"] == "short_term":
                existing["short_term"].append(entry)
                existing["short_term"] = existing["short_term"][-_MAX_SHORT_TERM:]
            elif entry["memory_type"] == "long_term":
                existing["long_term"].append(entry)
                existing["long_term"] = existing["long_term"][-_MAX_LONG_TERM:]
            elif entry["memory_type"] == "preference":
                key = (entry.get("metadata") or {}).get("key") or ""
                if key:
                    existing["preferences"][key] = entry["content"]

            with open(mem_file, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
        except Exception as e:
            logger.debug("[NovaMemory] Local persist failed: %s", e)

    def get_stats(self) -> dict:
        """Get memory statistics."""
        with self._lock:
            return {
                "short_term_count": len(self._short_term),
                "long_term_count": len(self._long_term),
                "preference_count": len(self._preferences),
                "loaded": self._loaded,
            }


# ---------------------------------------------------------------------------
# User Profile -- personalization engine
# ---------------------------------------------------------------------------

# Role extraction patterns: common recruitment job titles
_ROLE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"\b("
        r"software engineer|data scientist|data engineer|data analyst|"
        r"product manager|project manager|program manager|"
        r"registered nurse|nurse practitioner|"
        r"cdl driver|truck driver|warehouse worker|forklift operator|"
        r"accountant|financial analyst|"
        r"teacher|professor|instructor|"
        r"mechanic|electrician|plumber|welder|"
        r"security guard|janitor|cashier|"
        r"cook|chef|barista|"
        r"pharmacist|dentist|therapist|physician|doctor|"
        r"paralegal|lawyer|attorney|"
        r"marketing manager|sales manager|account executive|"
        r"hr manager|recruiter|talent acquisition|"
        r"designer|graphic designer|ux designer|ui designer|"
        r"devops engineer|sre|site reliability|"
        r"machine learning engineer|ai engineer|"
        r"business analyst|consultant|"
        r"nurse|driver|engineer|developer|analyst|manager"
        r")\b",
        re.IGNORECASE,
    ),
]

# Location extraction: US cities, states, and major global cities
_LOCATION_PATTERNS: list[re.Pattern[str]] = [
    # "in <City>" or "in <City>, <State>"
    re.compile(r"\bin\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:,\s*[A-Z]{2})?)\b"),
    # Standalone US states
    re.compile(
        r"\b(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|"
        r"Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|"
        r"Kansas|Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|"
        r"Minnesota|Mississippi|Missouri|Montana|Nebraska|Nevada|"
        r"New Hampshire|New Jersey|New Mexico|New York|North Carolina|"
        r"North Dakota|Ohio|Oklahoma|Oregon|Pennsylvania|Rhode Island|"
        r"South Carolina|South Dakota|Tennessee|Texas|Utah|Vermont|"
        r"Virginia|Washington|West Virginia|Wisconsin|Wyoming)\b"
    ),
    # Major US cities
    re.compile(
        r"\b(San Francisco|Los Angeles|New York|Chicago|Houston|Phoenix|"
        r"Philadelphia|San Antonio|San Diego|Dallas|Austin|Jacksonville|"
        r"San Jose|Fort Worth|Columbus|Charlotte|Indianapolis|Seattle|"
        r"Denver|Nashville|Boston|Portland|Las Vegas|Memphis|Atlanta|"
        r"Miami|Detroit|Minneapolis|Tampa|Orlando|St\.?\s*Louis|"
        r"Pittsburgh|Cincinnati|Cleveland|Kansas City|Raleigh|"
        r"Salt Lake City|Richmond|Hartford|Buffalo|Rochester|"
        r"Sacramento|Oakland|Honolulu|Anchorage)\b",
        re.IGNORECASE,
    ),
    # Major global cities
    re.compile(
        r"\b(London|Toronto|Vancouver|Sydney|Melbourne|Mumbai|Bangalore|"
        r"Delhi|Berlin|Paris|Tokyo|Singapore|Dubai|Hong Kong|"
        r"Amsterdam|Dublin|Zurich|Stockholm|Munich)\b",
        re.IGNORECASE,
    ),
]

# Industry extraction patterns
_INDUSTRY_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"\b("
        r"tech(?:nology)?|healthcare|finance|banking|"
        r"retail|e-?commerce|manufacturing|"
        r"education|government|defense|military|"
        r"hospitality|restaurant|food service|"
        r"logistics|transportation|supply chain|"
        r"pharma(?:ceutical)?|biotech|"
        r"construction|real estate|"
        r"energy|oil and gas|renewable|"
        r"media|advertising|marketing|"
        r"insurance|legal|consulting|"
        r"telecommunications|telecom|"
        r"automotive|aerospace|"
        r"nonprofit|non-profit|"
        r"agriculture|mining|"
        r"staffing|recruitment"
        r")\b",
        re.IGNORECASE,
    ),
]

_MAX_PROFILE_ITEMS = 20  # Cap frequent items to prevent unbounded growth


def extract_roles(query: str) -> list[str]:
    """Extract job role mentions from a user query.

    Args:
        query: The user's chat message.

    Returns:
        List of role strings found in the query (lowercased, deduplicated).
    """
    roles: list[str] = []
    for pattern in _ROLE_PATTERNS:
        matches = pattern.findall(query)
        for m in matches:
            role = m.strip().lower()
            if role and len(role) > 2 and role not in roles:
                roles.append(role)
    return roles


def extract_locations(query: str) -> list[str]:
    """Extract location mentions from a user query.

    Args:
        query: The user's chat message.

    Returns:
        List of location strings found (title-cased, deduplicated).
    """
    locations: list[str] = []
    for pattern in _LOCATION_PATTERNS:
        matches = pattern.findall(query)
        for m in matches:
            loc = m.strip().title()
            if loc and len(loc) > 1 and loc not in locations:
                locations.append(loc)
    return locations


def extract_industries(query: str) -> list[str]:
    """Extract industry mentions from a user query.

    Args:
        query: The user's chat message.

    Returns:
        List of industry strings found (lowercased, deduplicated).
    """
    industries: list[str] = []
    for pattern in _INDUSTRY_PATTERNS:
        matches = pattern.findall(query)
        for m in matches:
            ind = m.strip().lower()
            if ind and ind not in industries:
                industries.append(ind)
    return industries


class UserProfile:
    """Tracks user preferences extracted from their query history.

    Thread-safe accumulator of roles, locations, and industries
    the user has asked about, used to personalize follow-ups
    and system prompt context.
    """

    def __init__(self, session_id: str = "default") -> None:
        self.session_id = session_id
        self.frequent_roles: Counter = Counter()
        self.frequent_locations: Counter = Counter()
        self.frequent_industries: Counter = Counter()
        self.query_count: int = 0
        self.last_active: float = time.time()
        self._lock = threading.Lock()

    def update(self, query: str, tools_used: Optional[list[str]] = None) -> None:
        """Extract entities from a query and update the profile.

        Args:
            query: The user's chat message.
            tools_used: List of tool names invoked (reserved for future weighting).
        """
        roles = extract_roles(query)
        locations = extract_locations(query)
        industries = extract_industries(query)

        with self._lock:
            for r in roles:
                self.frequent_roles[r] += 1
            for loc in locations:
                self.frequent_locations[loc] += 1
            for ind in industries:
                self.frequent_industries[ind] += 1
            self.query_count += 1
            self.last_active = time.time()

            # Cap counters to prevent unbounded growth
            if len(self.frequent_roles) > _MAX_PROFILE_ITEMS:
                self.frequent_roles = Counter(
                    dict(self.frequent_roles.most_common(_MAX_PROFILE_ITEMS))
                )
            if len(self.frequent_locations) > _MAX_PROFILE_ITEMS:
                self.frequent_locations = Counter(
                    dict(self.frequent_locations.most_common(_MAX_PROFILE_ITEMS))
                )
            if len(self.frequent_industries) > _MAX_PROFILE_ITEMS:
                self.frequent_industries = Counter(
                    dict(self.frequent_industries.most_common(_MAX_PROFILE_ITEMS))
                )

    def get_top_roles(self, n: int = 3) -> list[str]:
        """Get the user's most frequently queried roles.

        Args:
            n: Number of top roles to return.

        Returns:
            List of role strings, most frequent first.
        """
        with self._lock:
            return [r for r, _ in self.frequent_roles.most_common(n)]

    def get_top_locations(self, n: int = 3) -> list[str]:
        """Get the user's most frequently queried locations.

        Args:
            n: Number of top locations to return.

        Returns:
            List of location strings, most frequent first.
        """
        with self._lock:
            return [loc for loc, _ in self.frequent_locations.most_common(n)]

    def get_primary_industry(self) -> Optional[str]:
        """Get the user's most frequently mentioned industry.

        Returns:
            The top industry string, or None if no industries tracked.
        """
        with self._lock:
            if not self.frequent_industries:
                return None
            return self.frequent_industries.most_common(1)[0][0]

    def get_context_injection(self) -> str:
        """Build a personalization context string for the system prompt.

        Returns an empty string if the profile has insufficient data
        (fewer than 3 queries), keeping the system prompt lean for new users.

        Returns:
            Formatted string for injection into the LLM system prompt.
        """
        with self._lock:
            if self.query_count < 3:
                return ""

            parts: list[str] = []

            top_roles = [r for r, _ in self.frequent_roles.most_common(3)]
            top_locs = [loc for loc, _ in self.frequent_locations.most_common(3)]
            top_ind = (
                self.frequent_industries.most_common(1)[0][0]
                if self.frequent_industries
                else None
            )

        if top_roles:
            parts.append(f"This user frequently asks about: {', '.join(top_roles)}.")
        if top_locs:
            parts.append(f"Their primary markets are: {', '.join(top_locs)}.")
        if top_ind:
            parts.append(f"Their primary industry focus is {top_ind}.")
        if parts:
            parts.append(
                "Tailor recommendations, examples, and benchmarks to their context when relevant."
            )

        if not parts:
            return ""

        return (
            "\n\n[USER PROFILE -- Personalization]\n"
            + " ".join(parts)
            + "\n[/USER PROFILE]\n"
        )

    def to_dict(self) -> dict:
        """Serialize the profile to a JSON-safe dict for frontend storage.

        Returns:
            Dict with serialized profile data suitable for localStorage.
        """
        with self._lock:
            return {
                "session_id": self.session_id,
                "frequent_roles": dict(self.frequent_roles.most_common(5)),
                "frequent_locations": dict(self.frequent_locations.most_common(5)),
                "preferred_industry": (
                    self.frequent_industries.most_common(1)[0][0]
                    if self.frequent_industries
                    else None
                ),
                "query_count": self.query_count,
                "last_active": self.last_active,
            }

    def from_dict(self, data: dict) -> None:
        """Restore profile state from a serialized dict (e.g., from frontend localStorage).

        Args:
            data: Dict previously returned by to_dict().
        """
        if not isinstance(data, dict):
            return
        with self._lock:
            self.frequent_roles = Counter(data.get("frequent_roles") or {})
            locs = data.get("frequent_locations") or {}
            self.frequent_locations = Counter(locs)
            ind = data.get("preferred_industry")
            if ind:
                self.frequent_industries = Counter({ind: 1})
            self.query_count = data.get("query_count") or 0
            self.last_active = data.get("last_active") or time.time()

    def generate_personalized_follow_ups(
        self, query_type: str, current_query: str
    ) -> list[str]:
        """Generate follow-up suggestions personalized to the user's profile.

        Falls back to empty list if the profile lacks data. The caller
        should merge these with the default follow-ups from _FOLLOW_UP_MAP.

        Args:
            query_type: The classified query type (salary, media_plan, etc.).
            current_query: The current user message.

        Returns:
            List of 0-3 personalized follow-up question strings.
        """
        suggestions: list[str] = []
        top_roles = self.get_top_roles(3)
        top_locs = self.get_top_locations(3)
        primary_ind = self.get_primary_industry()

        # Extract what was already asked about to avoid duplicating
        current_roles = extract_roles(current_query)
        current_locs = extract_locations(current_query)

        # Suggest a comparison to another location the user cares about
        other_locs = [
            loc
            for loc in top_locs
            if loc.lower() not in [cl.lower() for cl in current_locs]
        ]
        other_roles = [
            r
            for r in top_roles
            if r.lower() not in [cr.lower() for cr in current_roles]
        ]

        if query_type == "salary" and other_locs:
            suggestions.append(f"How does this compare to {other_locs[0]}?")
        elif query_type == "salary" and other_roles:
            suggestions.append(f"What's the salary outlook for {other_roles[0]}?")

        if query_type == "media_plan" and other_roles:
            suggestions.append(f"Create a similar plan for {other_roles[0]}?")

        if query_type in ("channels", "media_plan") and other_locs:
            suggestions.append(f"What channels work best in {other_locs[0]}?")

        if primary_ind and query_type == "general" and top_roles:
            suggestions.append(
                f"What are the {primary_ind} hiring trends for {top_roles[0]}?"
            )

        if other_roles and len(suggestions) < 2:
            suggestions.append(f"Show me benchmarks for {other_roles[0]}.")

        return suggestions[:3]


# Singleton registry for user profiles
_profile_instances: Dict[str, UserProfile] = {}
_profile_lock = threading.Lock()
_MAX_PROFILE_INSTANCES = 500  # Cap total profiles in memory


def get_user_profile(session_id: str = "default") -> UserProfile:
    """Get or create a UserProfile instance for a session.

    Args:
        session_id: The session or conversation identifier.

    Returns:
        The UserProfile for the given session.
    """
    with _profile_lock:
        if session_id not in _profile_instances:
            # Cap total instances
            if len(_profile_instances) >= _MAX_PROFILE_INSTANCES:
                # Evict oldest profile
                oldest_key = min(
                    _profile_instances,
                    key=lambda k: _profile_instances[k].last_active,
                )
                del _profile_instances[oldest_key]
            _profile_instances[session_id] = UserProfile(session_id)
        return _profile_instances[session_id]


def update_user_profile(
    session_id: str, query: str, tools_used: Optional[list[str]] = None
) -> UserProfile:
    """Extract entities from a query and update the user profile.

    Convenience function that gets or creates the profile and updates it.

    Args:
        session_id: The session or conversation identifier.
        query: The user's chat message.
        tools_used: List of tool names invoked.

    Returns:
        The updated UserProfile instance.
    """
    profile = get_user_profile(session_id)
    profile.update(query, tools_used)
    return profile


# Singleton
_memory_instances: dict = {}
_global_lock = threading.Lock()


def get_memory(user_id: str = "default") -> NovaMemory:
    """Get or create a NovaMemory instance for a user."""
    with _global_lock:
        if user_id not in _memory_instances:
            mem = NovaMemory(user_id)
            mem.load()
            _memory_instances[user_id] = mem
        return _memory_instances[user_id]
