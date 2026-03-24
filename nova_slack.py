"""
Nova Slack Bot -- Slack interface for Joveo's recruitment intelligence system.

Features:
- Responds to @Nova mentions and DMs in Slack
- Queries Joveo's data sources via 21 Nova tools (including v2 orchestrator tools)
- Searches Slack history for previously answered questions
- Maintains an unanswered question queue for human review
- Learns from human-provided answers to improve over time
- Sends weekly digest of unanswered questions
- Integrates with data_orchestrator.py for real-time data enrichment

Thread-safety: All file writes use an in-process threading lock.
Dependencies: stdlib only (no slack_sdk -- uses urllib.request).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent / "data"
UNANSWERED_FILE = DATA_DIR / "nova_unanswered_questions.json"
LEARNED_ANSWERS_FILE = DATA_DIR / "nova_learned_answers.json"
SLACK_HISTORY_CACHE_FILE = DATA_DIR / "nova_slack_history_cache.json"
TOKEN_CACHE_FILE = DATA_DIR / "nova_slack_token_cache.json"

# Token refresh constants
_TOKEN_REFRESH_INTERVAL = 30 * 60  # Check every 30 minutes
_TOKEN_EXPIRY_BUFFER = 2 * 60 * 60  # Refresh when within 2 hours of expiry
_TOKEN_RETRY_DELAY = 5 * 60  # Retry after 5 minutes on failure
_TOKEN_DEFAULT_LIFETIME = 12 * 60 * 60  # Assume 12-hour lifetime if unknown

# ---------------------------------------------------------------------------
# P1 FIX: Pre-loaded Q&A pairs that survive ephemeral filesystem redeploys
# ---------------------------------------------------------------------------
_PRELOADED_ANSWERS = [
    {
        "question": "how many publishers does joveo have",
        "answer": "Joveo has *10,238+ Supply Partners* across *70+ countries*, including major job boards, niche boards, programmatic platforms, and social channels.",
        "keywords": ["publishers", "supply partners", "how many"],
        "confidence": 0.95,
    },
    {
        "question": "what is joveo",
        "answer": "Joveo is a *recruitment marketing platform* that uses programmatic advertising technology to optimize job ad spend across 10,238+ Supply Partners globally. It helps employers reach the right candidates at the right time on the right channels.",
        "keywords": ["joveo", "what is"],
        "confidence": 0.95,
    },
    {
        "question": "what countries does joveo operate in",
        "answer": "Joveo operates across *70+ countries* including the US, UK, Canada, Germany, France, India, Australia, Japan, UAE, Brazil, and many more across EMEA, APAC, and AMER regions.",
        "keywords": ["countries", "regions", "operate"],
        "confidence": 0.90,
    },
    {
        "question": "what is programmatic job advertising",
        "answer": "Programmatic job advertising uses *data-driven automation* to buy, place, and optimize job ads in real-time across multiple channels. It maximizes ROI by dynamically adjusting bids, budgets, and targeting based on performance data. Average CPC ranges from $0.50-$2.50 depending on role and industry.",
        "keywords": ["programmatic", "advertising", "explain"],
        "confidence": 0.90,
    },
    {
        "question": "what is cpc cpa cph",
        "answer": "*CPC* (Cost Per Click): You pay each time a candidate clicks your job ad ($0.50-$5.00 typical).\n*CPA* (Cost Per Application): You pay when a candidate completes an application ($5-$50 typical).\n*CPH* (Cost Per Hire): Total cost to fill a position ($1,500-$10,000+ depending on role).\nCPC is best for volume, CPA for quality, CPH for executive/niche roles.",
        "keywords": ["cpc", "cpa", "cph", "cost per"],
        "confidence": 0.95,
    },
    {
        "question": "what pricing models does joveo support",
        "answer": "Joveo supports multiple pricing models: *CPC* (Cost Per Click), *CPA* (Cost Per Application), *TCPA* (Target CPA with auto-optimization), *Flat CPC*, *ORG* (Organic/free postings), and *PPP* (Pay Per Post). The optimal model depends on your hiring volume and role type.",
        "keywords": ["pricing", "models", "commission"],
        "confidence": 0.90,
    },
    {
        "question": "top job boards in the us",
        "answer": "The top job boards in the US by traffic and performance:\n1. *Indeed* — largest globally, CPC model\n2. *LinkedIn* — best for white-collar/professional\n3. *ZipRecruiter* — strong AI matching\n4. *Glassdoor* (merging into Indeed) — employer brand focused\n5. *CareerBuilder* (under Bold Holdings post-bankruptcy)\n6. *Dice* — tech-specific\n7. *Snagajob/JobGet* — hourly/blue-collar\n8. *Handshake* — early career/campus",
        "keywords": ["top", "job boards", "us", "united states", "best"],
        "confidence": 0.85,
    },
    {
        "question": "what happened to monster and careerbuilder",
        "answer": "Monster and CareerBuilder filed for *Chapter 11 bankruptcy* in July 2025. They were acquired by *Bold Holdings for $28M*. Monster Europe has been shut down (DNS killed). CareerBuilder continues operating in the US under new ownership but with reduced scale.",
        "keywords": ["monster", "careerbuilder", "bankruptcy", "shut down"],
        "confidence": 0.95,
    },
    {
        "question": "what is glassdoor status",
        "answer": "Glassdoor's operations are *merging into Indeed* (both owned by Recruit Holdings). The Glassdoor CEO stepped down in late 2025. The platform still operates but is increasingly integrated with Indeed's infrastructure.",
        "keywords": ["glassdoor", "status", "indeed"],
        "confidence": 0.90,
    },
    {
        "question": "best boards for nursing hiring",
        "answer": "Top job boards for *nursing/healthcare* hiring:\n1. *Health eCareers* — largest healthcare niche board\n2. *Nurse.com* — RN-focused\n3. *NursingJobs.us* — US nursing specific\n4. *Indeed* — high-volume nursing traffic\n5. *Vivian Health* — travel nursing marketplace\n6. *Incredible Health* — RN matching platform\n7. *AlliedHealthJobs* — allied health professionals\nRecommended channel mix: 30% niche boards, 22% programmatic, 15% global boards.",
        "keywords": ["nursing", "nurse", "healthcare", "boards"],
        "confidence": 0.90,
    },
    {
        "question": "best boards for blue collar hiring",
        "answer": "Top channels for *blue-collar/hourly* hiring:\n1. *JobGet* (acquired Snagajob) — 100M+ hourly workers\n2. *Indeed* — highest blue-collar volume\n3. *Craigslist* — local trades & service\n4. *Facebook Jobs* — mobile-first hourly workers\n5. *Wonolo* — on-demand warehouse/logistics\n6. *Instawork* — gig/flexible workers\n7. *ShiftPixy* — restaurant/hospitality shifts\nBudget tip: 40%+ should go to programmatic/mobile-first channels.",
        "keywords": ["blue collar", "hourly", "warehouse", "driver", "trades"],
        "confidence": 0.90,
    },
    {
        "question": "joveo vs competitors",
        "answer": "Joveo's key differentiators:\n- *Broadest global reach*: 10,238+ Supply Partners across 70+ countries -- the largest publisher network in the industry\n- *Multiple pricing models*: CPC, CPA, TCPA, Flat CPC, ORG, and PPP -- more flexibility than any alternative\n- *AI-driven optimization*: Real-time bid optimization maximizes ROI across all channels\n- *Performance marketing focus*: Data-driven programmatic approach with stronger niche board access\n- *Superior cost efficiency*: AI algorithms continuously optimize spend allocation for maximum hires per dollar\nAs per our recommendation, Joveo delivers the best results for recruitment marketing at any scale.",
        "keywords": ["competitor", "vs", "compare", "alternative"],
        "confidence": 0.85,
    },
]

# ---------------------------------------------------------------------------
# Slack mrkdwn conversion
# ---------------------------------------------------------------------------


def _convert_to_slack_mrkdwn(text: str) -> str:
    """Convert standard markdown formatting to Slack mrkdwn.

    Handles:
    - ``**bold**`` -> ``*bold*`` (Slack uses single asterisks for bold)
    - ``### header`` -> ``*header*`` (Slack doesn't support markdown headers)
    - ``[link text](url)`` -> ``<url|link text>`` (Slack link format)
    - Markdown tables -> formatted text blocks
    """
    # Replace **bold** with *bold* (must be done before ### conversion)
    text = re.sub(r"\*\*([^*]+)\*\*", r"*\1*", text)

    # Replace ### headers (and ## and #) with *bold text* on its own line
    text = re.sub(r"^#{1,6}\s+(.+)$", r"*\1*", text, flags=re.MULTILINE)

    # Replace [link text](url) with <url|link text>
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"<\2|\1>", text)

    # Replace markdown table rows with formatted text
    # Detect table blocks: lines starting with |
    lines = text.split("\n")
    result_lines = []
    in_table = False
    table_headers: list = []

    for line in lines:
        stripped = line.strip()
        # Detect separator rows like |---|---|
        if re.match(r"^\|[\s\-|]+\|$", stripped):
            in_table = True
            continue
        # Detect table rows
        if stripped.startswith("|") and stripped.endswith("|"):
            cells = [c.strip() for c in stripped.strip("|").split("|")]
            if not in_table:
                # This is a header row
                table_headers = cells
                in_table = True
            else:
                # Data row -- format as key: value pairs using headers
                if table_headers and len(table_headers) == len(cells):
                    formatted = " | ".join(
                        f"{h}: {v}" for h, v in zip(table_headers, cells)
                    )
                    result_lines.append(f"- {formatted}")
                else:
                    result_lines.append(f"- {' | '.join(cells)}")
            continue
        else:
            if in_table:
                in_table = False
                table_headers = []
            result_lines.append(line)

    return "\n".join(result_lines)


# ---------------------------------------------------------------------------
# Stop-words removed during keyword matching
# ---------------------------------------------------------------------------
_STOP_WORDS = frozenset(
    {
        "what",
        "is",
        "the",
        "a",
        "an",
        "how",
        "does",
        "can",
        "for",
        "in",
        "of",
        "to",
        "and",
        "or",
        "my",
        "our",
        "we",
        "do",
        "are",
        "it",
        "this",
        "that",
        "which",
        "with",
        "about",
        "on",
        "at",
        "be",
        "by",
        "from",
        "has",
        "have",
        "i",
        "me",
        "you",
        "your",
        "they",
        "their",
        "was",
        "were",
        "been",
        "being",
        "will",
        "would",
        "could",
        "should",
        "may",
        "might",
        "shall",
        "not",
        "no",
        "so",
        "if",
        "but",
        "up",
        "out",
        "there",
        "here",
        "when",
        "where",
        "why",
        "who",
        "whom",
    }
)

# Minimum Jaccard score to consider a learned answer a *partial* match
_PARTIAL_MATCH_THRESHOLD = 0.35
# Minimum Jaccard score to consider a learned answer a *strong* match
_STRONG_MATCH_THRESHOLD = 0.70


# ============================================================================
# NovaSlackBot
# ============================================================================


class NovaSlackBot:
    """Nova Slack Bot -- connects Nova intelligence to Slack."""

    def __init__(
        self,
        slack_bot_token: Optional[str] = None,
        slack_signing_secret: Optional[str] = None,
    ):
        # Thread lock for file I/O
        self._lock = threading.Lock()

        # Token rotation state -- lock protects bot_token, refresh_token,
        # _token_issued_at, and _token_expires_in from concurrent access
        self._token_lock = threading.Lock()

        # Token rotation credentials (from env)
        self.slack_client_id: str = os.environ.get("SLACK_CLIENT_ID") or ""
        self.slack_client_secret: str = os.environ.get("SLACK_CLIENT_SECRET") or ""
        self.refresh_token: str = os.environ.get("SLACK_REFRESH_TOKEN") or ""
        self._token_issued_at: float = 0.0
        self._token_expires_in: int = _TOKEN_DEFAULT_LIFETIME

        # Resolve bot_token: try cached tokens first, then env / constructor arg
        cached = self._load_token_cache()
        if cached:
            self.bot_token: str = cached["access_token"]
            self.refresh_token = cached.get("refresh_token", self.refresh_token)
            self._token_issued_at = cached.get("issued_at", 0.0)
            self._token_expires_in = cached.get("expires_in", _TOKEN_DEFAULT_LIFETIME)
            logger.info(
                "Nova: Loaded cached Slack token (issued %s)",
                (
                    datetime.utcfromtimestamp(self._token_issued_at).isoformat()
                    if self._token_issued_at
                    else "unknown"
                ),
            )
        else:
            self.bot_token = slack_bot_token or os.environ.get("SLACK_BOT_TOKEN") or ""

        self.signing_secret: str = slack_signing_secret or os.environ.get(
            "SLACK_SIGNING_SECRET", ""
        )
        self.bot_user_id: Optional[str] = None  # Populated after auth.test

        # In-memory caches (populated from disk)
        self.learned_answers: Dict[str, Any] = {}
        self.unanswered: Dict[str, Any] = {}

        # Per-thread conversation history for multi-turn context
        # Key: thread_ts, Value: list of {role, content, timestamp}
        self._thread_history: Dict[str, List[Dict[str, str]]] = {}
        self._thread_history_lock = threading.Lock()
        self._max_thread_history = 20  # Max messages per thread
        self._thread_ttl_seconds = 3600  # Expire threads after 1 hour of inactivity

        self._load_learned_answers()
        self._load_unanswered_questions()

        # Authenticate with Slack to populate bot_user_id
        self._auth_test()

        # Start background token refresh thread (daemon so it won't block exit)
        if self._can_refresh_tokens():
            self._refresh_thread = threading.Thread(
                target=self._token_refresh_loop,
                name="slack-token-refresh",
                daemon=True,
            )
            self._refresh_thread.start()
            logger.info("Nova: Token refresh background thread started")
        else:
            self._refresh_thread = None
            logger.info(
                "Nova: Token rotation not configured "
                "(missing SLACK_CLIENT_ID, SLACK_CLIENT_SECRET, "
                "or SLACK_REFRESH_TOKEN)"
            )

        # Nova engine (optional dependency)
        self._iq_engine: Any = None
        self._init_iq_engine()

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _init_iq_engine(self) -> None:
        """Import and initialise the Nova chat engine (if available)."""
        try:
            from nova import Nova  # type: ignore[import-untyped]

            self._iq_engine = Nova()
            logger.info("Nova: Nova engine initialised successfully")
        except ImportError:
            logger.warning(
                "Nova: nova module not available -- running in standalone mode"
            )
        except Exception as exc:
            logger.error("Nova: Failed to initialise Nova engine: %s", exc)

    # ------------------------------------------------------------------
    # Token rotation -- cache, refresh, background loop
    # ------------------------------------------------------------------

    def _can_refresh_tokens(self) -> bool:
        """Return True if token rotation credentials are configured."""
        return bool(
            self.slack_client_id and self.slack_client_secret and self.refresh_token
        )

    def _load_token_cache(self) -> Optional[Dict[str, Any]]:
        """Load cached token data from disk. Returns None if unavailable."""
        try:
            if TOKEN_CACHE_FILE.exists():
                with open(TOKEN_CACHE_FILE, "r") as fh:
                    data = json.load(fh)
                # Validate required fields
                if data.get("access_token"):
                    return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Nova: Failed to load token cache: %s", exc)
        return None

    def _save_token_cache(
        self,
        access_token: str,
        refresh_token: str,
        expires_in: int,
        issued_at: float,
    ) -> None:
        """Persist token data to disk (thread-safe via _lock)."""
        cache_data = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": expires_in,
            "issued_at": issued_at,
            "saved_at": datetime.utcnow().isoformat(),
        }
        with self._lock:
            try:
                DATA_DIR.mkdir(parents=True, exist_ok=True)
                tmp = str(TOKEN_CACHE_FILE) + ".tmp"
                with open(tmp, "w") as fh:
                    json.dump(cache_data, fh, indent=2)
                os.replace(tmp, TOKEN_CACHE_FILE)
                logger.debug("Nova: Token cache saved to %s", TOKEN_CACHE_FILE)
            except OSError as exc:
                logger.error("Nova: Failed to save token cache: %s", exc)

    def _refresh_token(self) -> bool:
        """Exchange the refresh token for a new access token.

        POSTs to ``https://slack.com/api/oauth.v2.access`` with
        ``grant_type=refresh_token``.  On success, updates in-memory
        token state and persists to disk cache.  Returns True on success.
        """
        import urllib.request
        import urllib.error
        import urllib.parse

        if not self._can_refresh_tokens():
            logger.warning("Nova: Cannot refresh -- missing credentials")
            return False

        with self._token_lock:
            current_refresh = self.refresh_token

        post_data = urllib.parse.urlencode(
            {
                "client_id": self.slack_client_id,
                "client_secret": self.slack_client_secret,
                "grant_type": "refresh_token",
                "refresh_token": current_refresh,
            }
        ).encode("utf-8")

        req = urllib.request.Request(
            "https://slack.com/api/oauth.v2.access",
            data=post_data,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            logger.error("Nova: Token refresh HTTP error: %s", exc)
            return False
        except Exception as exc:
            logger.error("Nova: Unexpected token refresh error: %s", exc)
            return False

        if not result.get("ok"):
            logger.error(
                "Nova: Token refresh API error: %s", result.get("error", "unknown")
            )
            return False

        new_access = result.get("access_token") or ""
        new_refresh = result.get("refresh_token", current_refresh)
        expires_in = result.get("expires_in", _TOKEN_DEFAULT_LIFETIME)
        issued_at = time.time()

        if not new_access:
            logger.error("Nova: Token refresh returned empty access_token")
            return False

        # Update in-memory state (thread-safe)
        with self._token_lock:
            self.bot_token = new_access
            self.refresh_token = new_refresh
            self._token_issued_at = issued_at
            self._token_expires_in = int(expires_in)

        # Persist to disk
        self._save_token_cache(new_access, new_refresh, int(expires_in), issued_at)

        logger.info(
            "Nova: Token refreshed successfully (expires_in=%ds, ~%.1fh)",
            expires_in,
            expires_in / 3600,
        )
        return True

    def _token_near_expiry(self) -> bool:
        """Return True if the current token is within the expiry buffer."""
        with self._token_lock:
            if self._token_issued_at <= 0:
                # Unknown issue time -- assume it needs refreshing if rotation
                # is configured (conservative approach)
                return self._can_refresh_tokens()
            elapsed = time.time() - self._token_issued_at
            remaining = self._token_expires_in - elapsed
        return remaining < _TOKEN_EXPIRY_BUFFER

    def _token_refresh_loop(self) -> None:
        """Background loop that proactively refreshes the token.

        Runs every ``_TOKEN_REFRESH_INTERVAL`` seconds.  If the token is
        within ``_TOKEN_EXPIRY_BUFFER`` of expiry, attempts a refresh.
        On failure, retries after ``_TOKEN_RETRY_DELAY``.
        """
        logger.info("Nova: Token refresh loop started")
        while True:
            try:
                time.sleep(_TOKEN_REFRESH_INTERVAL)
                if self._token_near_expiry():
                    logger.info("Nova: Token near expiry -- refreshing")
                    if not self._refresh_token():
                        logger.warning(
                            "Nova: Token refresh failed -- retrying in %ds",
                            _TOKEN_RETRY_DELAY,
                        )
                        time.sleep(_TOKEN_RETRY_DELAY)
                        # Second attempt
                        if not self._refresh_token():
                            logger.error("Nova: Token refresh retry also failed")
            except Exception as exc:
                logger.error("Nova: Error in token refresh loop: %s", exc)
                # Sleep before retrying to avoid tight error loops
                time.sleep(_TOKEN_RETRY_DELAY)

    # ------------------------------------------------------------------
    # Persistence -- learned answers
    # ------------------------------------------------------------------

    def _load_learned_answers(self) -> None:
        """Load previously learned answers from disk, merging with pre-loaded pairs."""
        with self._lock:
            try:
                if LEARNED_ANSWERS_FILE.exists():
                    with open(LEARNED_ANSWERS_FILE, "r") as fh:
                        self.learned_answers = json.load(fh)
                else:
                    self.learned_answers = {
                        "answers": [],
                        "metadata": {"total_learned": 0, "last_updated": None},
                    }
            except (json.JSONDecodeError, OSError) as exc:
                logger.error("Nova: Failed to load learned answers: %s", exc)
                self.learned_answers = {
                    "answers": [],
                    "metadata": {"total_learned": 0, "last_updated": None},
                }
            # P1 FIX: Merge pre-loaded answers (survives ephemeral filesystem)
            existing_qs = {
                (a.get("question") or "").lower()
                for a in self.learned_answers.get("answers") or []
            }
            for preloaded in _PRELOADED_ANSWERS:
                if preloaded["question"].lower() not in existing_qs:
                    self.learned_answers.setdefault("answers", []).append(preloaded)
            self.learned_answers["metadata"]["total_learned"] = len(
                self.learned_answers.get("answers") or []
            )

    def _save_learned_answers(self) -> None:
        """Persist learned answers to disk (caller must hold ``_lock``)."""
        self.learned_answers["metadata"]["last_updated"] = datetime.utcnow().isoformat()
        tmp = str(LEARNED_ANSWERS_FILE) + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self.learned_answers, fh, indent=2)
        os.replace(tmp, LEARNED_ANSWERS_FILE)

    # ------------------------------------------------------------------
    # Persistence -- unanswered questions
    # ------------------------------------------------------------------

    def _load_unanswered_questions(self) -> None:
        """Load the unanswered questions queue from disk."""
        with self._lock:
            try:
                if UNANSWERED_FILE.exists():
                    with open(UNANSWERED_FILE, "r") as fh:
                        self.unanswered = json.load(fh)
                else:
                    self.unanswered = {
                        "questions": [],
                        "metadata": {"total_queued": 0, "total_resolved": 0},
                    }
            except (json.JSONDecodeError, OSError) as exc:
                logger.error("Nova: Failed to load unanswered questions: %s", exc)
                self.unanswered = {
                    "questions": [],
                    "metadata": {"total_queued": 0, "total_resolved": 0},
                }

    def _save_unanswered_questions(self) -> None:
        """Persist unanswered questions (caller must hold ``_lock``)."""
        tmp = str(UNANSWERED_FILE) + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self.unanswered, fh, indent=2)
        os.replace(tmp, UNANSWERED_FILE)

    # ------------------------------------------------------------------
    # Slack request verification
    # ------------------------------------------------------------------

    def verify_slack_signature(self, timestamp: str, body: str, signature: str) -> bool:
        """Verify an incoming request originated from Slack.

        Uses the Slack signing-secret HMAC-SHA256 scheme.  If no signing
        secret is configured the check is skipped (development mode).
        """
        if not self.signing_secret:
            return True
        # Reject requests older than 5 minutes to prevent replay attacks
        try:
            if abs(time.time() - int(timestamp)) > 300:
                logger.warning("Nova: Slack request timestamp too old")
                return False
        except (ValueError, TypeError):
            return False
        sig_basestring = f"v0:{timestamp}:{body}"
        expected = (
            "v0="
            + hmac.new(
                self.signing_secret.encode("utf-8"),
                sig_basestring.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
        )
        return hmac.compare_digest(expected, signature)

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------

    def handle_event(self, event_data: dict) -> dict:
        """Route an incoming Slack event payload.

        Supports:
        - ``url_verification`` (Slack challenge handshake)
        - ``event_callback`` with ``message`` / ``app_mention`` inner events
        """
        event_type = event_data.get("type")

        if event_type == "url_verification":
            return {"challenge": event_data.get("challenge") or ""}

        if event_type == "event_callback":
            event = event_data.get("event", {})
            return self._process_event(event)

        return {"ok": True}

    def _process_event(self, event: dict) -> dict:
        """Process a single ``message`` or ``app_mention`` event.

        Maintains per-thread conversation history so Nova can reference
        previous messages in multi-turn Slack threads.
        """
        etype = event.get("type")
        text = event.get("text") or ""
        user = event.get("user") or ""
        channel = event.get("channel") or ""
        ts = event.get("ts") or ""
        thread_ts = event.get("thread_ts") or ts

        # Ignore our own messages and other bots to avoid infinite loops
        if event.get("bot_id") or event.get("subtype") == "bot_message":
            return {"ok": True}
        if self.bot_user_id and user == self.bot_user_id:
            return {"ok": True}

        if etype not in ("app_mention", "message"):
            return {"ok": True}

        # Strip the ``<@UXXXXX>`` mention prefix
        clean_text = re.sub(r"<@\w+>", "", text).strip()
        if not clean_text:
            return {"ok": True}

        # Record user message in thread history
        self._add_to_thread_history(thread_ts, "user", clean_text)

        # Get thread history for context
        thread_history = self._get_thread_history(thread_ts)

        # Generate a response with conversation context
        response = self.answer_question(
            clean_text,
            user,
            channel,
            thread_ts,
            conversation_history=thread_history,
        )

        # Record assistant response in thread history
        # Strip Slack formatting prefix for clean history
        response_text = response.get("text") or ""
        clean_response = re.sub(
            r"\n\n_Sources?:.*$",
            "",
            response_text.replace("*Nova says:*\n\n", ""),
            flags=re.DOTALL,
        )
        self._add_to_thread_history(thread_ts, "assistant", clean_response)

        # Post to Slack (non-blocking best-effort)
        self._post_message(channel, response_text, thread_ts)

        return {"ok": True}

    # ------------------------------------------------------------------
    # Thread history management
    # ------------------------------------------------------------------

    def _add_to_thread_history(self, thread_ts: str, role: str, content: str) -> None:
        """Add a message to the per-thread conversation history."""
        with self._thread_history_lock:
            # Expire old threads first
            self._expire_old_threads()

            if thread_ts not in self._thread_history:
                self._thread_history[thread_ts] = []

            self._thread_history[thread_ts].append(
                {
                    "role": role,
                    "content": (
                        content[:2000] if role == "user" else content[:6000]
                    ),  # Keep assistant responses longer for context
                    "timestamp": time.time(),
                }
            )

            # Trim to max history length
            if len(self._thread_history[thread_ts]) > self._max_thread_history:
                self._thread_history[thread_ts] = self._thread_history[thread_ts][
                    -self._max_thread_history :
                ]

    def _get_thread_history(self, thread_ts: str) -> List[Dict[str, str]]:
        """Get conversation history for a thread (excluding current message).

        Returns a list of {role, content} dicts suitable for passing to
        Nova's chat method as conversation_history.
        """
        with self._thread_history_lock:
            messages = self._thread_history.get(thread_ts, [])
            # Return all messages except the last one (current message)
            history = messages[:-1] if len(messages) > 1 else []
            return [{"role": m["role"], "content": m["content"]} for m in history]

    def _expire_old_threads(self) -> None:
        """Remove thread histories that have been inactive beyond TTL."""
        now = time.time()
        expired = []
        for thread_ts, messages in self._thread_history.items():
            if messages:
                last_activity = messages[-1].get("timestamp") or 0
                if now - last_activity > self._thread_ttl_seconds:
                    expired.append(thread_ts)
            else:
                expired.append(thread_ts)
        for ts in expired:
            del self._thread_history[ts]

    # ------------------------------------------------------------------
    # Core Q&A pipeline
    # ------------------------------------------------------------------

    def answer_question(
        self,
        question: str,
        user_id: str = "",
        channel: str = "",
        thread_ts: str = "",
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """Answer a question using all available sources.

        Resolution order:
          1. Check learned answers (exact / fuzzy keyword match)
          2. Search Slack channel history for relevant context
          3. Query Nova engine with conversation history + enhanced context
          4. Fall back to partial learned match
          5. If confidence < threshold, queue for human review

        Args:
            question: The user's question text.
            user_id: Slack user ID of the asker.
            channel: Slack channel ID.
            thread_ts: Thread timestamp for threading.
            conversation_history: Previous messages in this thread for
                multi-turn context. List of {role, content} dicts.

        Returns a dict with ``text``, ``confidence``, ``sources``, and
        ``queued_for_review``.
        """
        # -- Step 1: Learned answers (strong match) -----------------------
        learned = self._search_learned_answers(question)
        if learned and learned["confidence"] >= 0.8:
            self._increment_usage(learned.get("original_question") or "")
            return {
                "text": (
                    f"*Nova says:*\n\n{learned['answer']}\n\n"
                    f"_Source: Previously answered by team "
                    f"| Confidence: {learned['confidence']:.0%}_"
                ),
                "confidence": learned["confidence"],
                "sources": ["Learned Answers"],
                "tools_used": [],
                "queued_for_review": False,
            }

        # -- Step 2: Search Slack channel history for context --------------
        slack_context: list = []
        if channel:
            try:
                slack_context = self._search_slack_context(question, channel)
            except Exception as exc:
                logger.warning("Nova: Slack context search failed: %s", exc)

        # Build an enhanced question with Slack context for Nova engine
        enhanced_question = question
        if slack_context:
            enhanced_question = (
                f"[Relevant Slack context: {'; '.join(slack_context)}]\n\n"
                f"User question: {question}"
            )

        # -- Step 3: Nova engine (with conversation history + enhanced context)
        iq_response: Optional[Dict[str, Any]] = None
        if self._iq_engine:
            try:
                iq_response = self._iq_engine.chat(
                    enhanced_question,
                    conversation_history=conversation_history,
                )
                if iq_response and (iq_response.get("confidence") or 0) >= 0.5:
                    sources = iq_response.get("sources", ["Joveo Data"])
                    if slack_context:
                        sources = list(sources) + ["Slack Channel History"]
                    src_text = ", ".join(sources)
                    conf = iq_response["confidence"]
                    tools_used = iq_response.get("tools_used") or []
                    tool_info = (
                        f" | Tools: {len(set(tools_used))}" if tools_used else ""
                    )
                    return {
                        "text": (
                            f"*Nova says:*\n\n{iq_response['response']}\n\n"
                            f"_Sources: {src_text} | Confidence: {conf:.0%}{tool_info}_"
                        ),
                        "confidence": conf,
                        "sources": sources,
                        "tools_used": tools_used,
                        "queued_for_review": False,
                    }
            except Exception as exc:
                logger.error("Nova: IQ engine error: %s", exc)

        # -- Step 4: Partial learned match ---------------------------------
        if learned and learned["confidence"] >= 0.4:
            self._increment_usage(learned.get("original_question") or "")
            return {
                "text": (
                    f"*Nova says:*\n\n{learned['answer']}\n\n"
                    f"_Note: Based on a similar question I've seen before. "
                    f"Confidence: {learned['confidence']:.0%}_"
                ),
                "confidence": learned["confidence"],
                "sources": ["Learned Answers (partial match)"],
                "tools_used": [],
                "queued_for_review": False,
            }

        # -- Step 5: Low confidence -- queue for review --------------------
        partial_text = iq_response.get("response") or "" if iq_response else ""
        self._add_to_unanswered(question, user_id, channel, thread_ts, partial_text)

        partial_block = ""
        if partial_text:
            partial_block = f"\n\nHere's what I found so far:\n{partial_text}\n"

        iq_conf = iq_response.get("confidence") or 0 if iq_response else 0.0
        iq_tools = iq_response.get("tools_used") or [] if iq_response else []
        return {
            "text": (
                f"*Nova says:*\n\n"
                f"I'm not fully confident in my answer to this one."
                f"{partial_block}\n"
                f"_I've added this to my learning queue -- the team will "
                f"review it and I'll get smarter!_"
            ),
            "confidence": iq_conf,
            "sources": [],
            "tools_used": iq_tools,
            "queued_for_review": True,
        }

    # ------------------------------------------------------------------
    # Learned-answer search
    # ------------------------------------------------------------------

    def _search_learned_answers(self, question: str) -> Optional[Dict[str, Any]]:
        """Search learned answers using Jaccard keyword similarity."""
        answers = self.learned_answers.get("answers") or []
        if not answers:
            return None

        q_words = self._extract_keywords(question)
        if not q_words:
            return None

        best_match: Optional[dict] = None
        best_score: float = 0.0

        for entry in answers:
            a_words = self._extract_keywords(entry.get("question") or "")
            if not a_words:
                continue
            overlap = len(q_words & a_words)
            union = len(q_words | a_words)
            score = overlap / union if union else 0.0
            if score > best_score:
                best_score = score
                best_match = entry

        if best_match and best_score >= _PARTIAL_MATCH_THRESHOLD:
            return {
                "answer": best_match["answer"],
                "confidence": min(best_score * 1.2, 1.0),
                "original_question": best_match["question"],
            }

        return None

    @staticmethod
    def _extract_keywords(text: str) -> set:
        """Tokenise *text* into a set of lower-case keywords, minus stop-words."""
        words = set(re.findall(r"\w+", text.lower()))
        return words - _STOP_WORDS

    def _increment_usage(self, original_question: str) -> None:
        """Bump ``times_used`` for a learned answer (best-effort)."""
        with self._lock:
            for entry in self.learned_answers.get("answers") or []:
                if entry.get("question") == original_question:
                    entry["times_used"] = (entry.get("times_used") or 0) + 1
                    try:
                        self._save_learned_answers()
                    except OSError:
                        pass
                    break

    # ------------------------------------------------------------------
    # Jaccard similarity helper
    # ------------------------------------------------------------------

    @staticmethod
    def _jaccard_similarity(set_a: set, set_b: set) -> float:
        """Return the Jaccard similarity coefficient between two keyword sets."""
        if not set_a or not set_b:
            return 0.0
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union if union else 0.0

    # ------------------------------------------------------------------
    # Slack channel context search
    # ------------------------------------------------------------------

    # Rate-limit: track the last time we called conversations.history
    _last_slack_context_call: float = 0.0
    _slack_context_lock = threading.Lock()

    def _search_slack_context(
        self,
        question: str,
        channel_id: str,
        _retried: bool = False,
    ) -> list:
        """Search recent channel messages for relevant context.

        Fetches the last 100 messages from *channel_id* using the Slack
        ``conversations.history`` API, scores each against *question*
        using Jaccard keyword similarity, and returns the top 3 relevant
        message texts.

        Rate-limited to at most one API call per second.  Returns an
        empty list on any error.  On token errors, refreshes and retries
        once if token rotation is configured.
        """
        if not self.bot_token or not channel_id:
            return []

        # ---- Rate limiting (1 call/sec) ----
        with self._slack_context_lock:
            now = time.monotonic()
            elapsed = now - self._last_slack_context_call
            if elapsed < 1.0:
                time.sleep(1.0 - elapsed)
            self._last_slack_context_call = time.monotonic()

        # ---- Fetch channel history via Slack API ----
        import urllib.request
        import urllib.error

        with self._token_lock:
            token = self.bot_token

        url = (
            "https://slack.com/api/conversations.history"
            f"?channel={channel_id}&limit=100"
        )
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            logger.warning("Nova: Failed to fetch Slack channel history: %s", exc)
            return []
        except Exception as exc:
            logger.warning("Nova: Unexpected error fetching channel history: %s", exc)
            return []

        if not data.get("ok"):
            error_code = data.get("error", "unknown")
            # Token expired/invalid -- refresh and retry once
            if (
                error_code in self._TOKEN_ERROR_CODES
                and not _retried
                and self._can_refresh_tokens()
            ):
                logger.info(
                    "Nova: Token error '%s' in context search -- refreshing",
                    error_code,
                )
                if self._refresh_token():
                    return self._search_slack_context(
                        question,
                        channel_id,
                        _retried=True,
                    )
            logger.warning(
                "Nova: Slack conversations.history error: %s",
                error_code,
            )
            return []

        messages = data.get("messages") or []
        if not messages:
            return []

        # ---- Score relevance using Jaccard similarity ----
        q_keywords = self._extract_keywords(question)
        if not q_keywords:
            return []

        scored: list = []
        for msg in messages:
            msg_text = (msg.get("text") or "").strip()
            if not msg_text:
                continue
            # Skip very short messages (reactions, single-word replies)
            if len(msg_text) < 10:
                continue
            # Skip bot messages to avoid echoing ourselves
            if msg.get("bot_id") or msg.get("subtype") == "bot_message":
                continue

            msg_keywords = self._extract_keywords(msg_text)
            score = self._jaccard_similarity(q_keywords, msg_keywords)
            if score > 0.1:  # Minimum relevance threshold
                scored.append((score, msg_text))

        # Sort by score descending, return top 3 message texts
        scored.sort(key=lambda x: x[0], reverse=True)
        return [text for _score, text in scored[:3]]

    # ------------------------------------------------------------------
    # Proactive recruitment-topic detection
    # ------------------------------------------------------------------

    _RECRUITMENT_KEYWORDS = frozenset(
        {
            "hiring",
            "budget",
            "cpa",
            "cpc",
            "job board",
            "candidate",
            "salary",
            "recruiter",
            "talent",
            "application",
            "indeed",
            "linkedin",
        }
    )

    def _is_recruitment_topic(self, text: str) -> bool:
        """Check whether *text* contains 2+ recruitment-related keywords.

        This is informational only -- the bot does NOT auto-respond to
        non-@mention messages based on this check.
        """
        if not text:
            return False
        text_lower = text.lower()
        matches = sum(1 for kw in self._RECRUITMENT_KEYWORDS if kw in text_lower)
        return matches >= 2

    # ------------------------------------------------------------------
    # Unanswered-question management
    # ------------------------------------------------------------------

    def _add_to_unanswered(
        self,
        question: str,
        user_id: str,
        channel: str,
        thread_ts: str,
        partial_answer: str = "",
    ) -> None:
        """Enqueue a question for human review."""
        entry = {
            "id": hashlib.md5(f"{question}{time.time()}".encode()).hexdigest()[:12],
            "question": question,
            "asked_by": user_id,
            "channel": channel,
            "thread_ts": thread_ts,
            "timestamp": datetime.utcnow().isoformat(),
            "partial_answer": partial_answer,
            "status": "pending",
            "answer": None,
            "answered_by": None,
            "answered_at": None,
        }
        with self._lock:
            self.unanswered.setdefault("questions", []).append(entry)
            # Prevent unbounded growth: keep only last 500 questions
            # (remove oldest resolved first, then oldest pending)
            questions = self.unanswered["questions"]
            if len(questions) > 500:
                # Keep all pending, trim oldest resolved/dismissed
                pending = [q for q in questions if q["status"] == "pending"]
                resolved = [q for q in questions if q["status"] != "pending"]
                # Keep latest 200 resolved + all pending (up to 500 total)
                max_resolved = max(500 - len(pending), 100)
                resolved_trimmed = (
                    resolved[-max_resolved:]
                    if len(resolved) > max_resolved
                    else resolved
                )
                self.unanswered["questions"] = resolved_trimmed + pending
            self.unanswered["metadata"]["total_queued"] = len(
                [q for q in self.unanswered["questions"] if q["status"] == "pending"]
            )
            self._save_unanswered_questions()
        logger.info("Nova: Queued unanswered question: %.80s...", question)

    def resolve_question(
        self,
        question_id: str,
        answer: str,
        answered_by: str = "admin",
    ) -> bool:
        """Resolve an unanswered question and add to learned answers.

        Optionally posts a follow-up reply into the original Slack thread.
        Returns ``True`` on success.
        """
        with self._lock:
            for q in self.unanswered.get("questions") or []:
                if q["id"] == question_id and q["status"] == "pending":
                    q["status"] = "answered"
                    q["answer"] = answer
                    q["answered_by"] = answered_by
                    q["answered_at"] = datetime.utcnow().isoformat()

                    # Persist to learned answers (cap at 2000 to prevent unbounded growth)
                    answers_list = self.learned_answers.setdefault("answers", [])
                    if len(answers_list) >= 2000:
                        # Evict least-used entries to make room
                        answers_list.sort(key=lambda a: a.get("times_used") or 0)
                        del answers_list[:100]
                    answers_list.append(
                        {
                            "question": q["question"],
                            "answer": answer,
                            "added_by": answered_by,
                            "added_at": datetime.utcnow().isoformat(),
                            "source": "human_review",
                            "times_used": 0,
                        }
                    )
                    self.learned_answers["metadata"]["total_learned"] = len(
                        self.learned_answers["answers"]
                    )

                    self.unanswered["metadata"]["total_resolved"] = len(
                        [
                            x
                            for x in self.unanswered["questions"]
                            if x["status"] == "answered"
                        ]
                    )
                    self.unanswered["metadata"]["total_queued"] = len(
                        [
                            x
                            for x in self.unanswered["questions"]
                            if x["status"] == "pending"
                        ]
                    )

                    self._save_learned_answers()
                    self._save_unanswered_questions()

                    # Post follow-up in Slack (outside the lock)
                    channel = q.get("channel") or ""
                    thread = q.get("thread_ts") or ""
                    break
            else:
                return False

        # Post follow-up reply (outside lock to avoid holding it during I/O)
        if channel and thread and self.bot_token:
            self._post_message(
                channel,
                (
                    f"*Nova update:* I now have an answer to this!\n\n"
                    f"{answer}\n\n"
                    f"_Answered by the team | This knowledge is now permanent_"
                ),
                thread,
            )
        return True

    def dismiss_question(self, question_id: str, reason: str = "") -> bool:
        """Dismiss an unanswered question (not relevant, duplicate, etc.)."""
        with self._lock:
            for q in self.unanswered.get("questions") or []:
                if q["id"] == question_id and q["status"] == "pending":
                    q["status"] = "dismissed"
                    q["answer"] = f"Dismissed: {reason}" if reason else "Dismissed"
                    q["answered_at"] = datetime.utcnow().isoformat()
                    self.unanswered["metadata"]["total_queued"] = len(
                        [
                            x
                            for x in self.unanswered["questions"]
                            if x["status"] == "pending"
                        ]
                    )
                    self._save_unanswered_questions()
                    return True
        return False

    # ------------------------------------------------------------------
    # Admin / dashboard helpers
    # ------------------------------------------------------------------

    def get_unanswered_summary(self) -> Dict[str, Any]:
        """Return a summary suitable for the admin dashboard."""
        questions = self.unanswered.get("questions") or []
        pending = [q for q in questions if q["status"] == "pending"]
        answered = [q for q in questions if q["status"] == "answered"]
        dismissed = [q for q in questions if q["status"] == "dismissed"]

        return {
            "pending_count": len(pending),
            "answered_count": len(answered),
            "dismissed_count": len(dismissed),
            "total_learned": self.learned_answers["metadata"].get("total_learned") or 0,
            "pending_questions": sorted(
                pending, key=lambda q: q.get("timestamp") or "", reverse=True
            ),
            "recent_answered": sorted(
                answered, key=lambda q: q.get("answered_at") or "", reverse=True
            )[:10],
        }

    def generate_weekly_digest(self) -> str:
        """Build a Slack-formatted weekly digest message."""
        week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()

        questions = self.unanswered.get("questions") or []
        recent_pending = [
            q
            for q in questions
            if q["status"] == "pending" and q.get("timestamp") or "" >= week_ago
        ]
        recent_answered = [
            q
            for q in questions
            if q["status"] == "answered" and q.get("answered_at") or "" >= week_ago
        ]
        total_pending = len([q for q in questions if q["status"] == "pending"])

        lines: List[str] = []
        lines.append("*Nova Weekly Digest*\n")
        lines.append(f"* *{len(recent_pending)}* new questions this week")
        lines.append(f"* *{len(recent_answered)}* questions answered this week")
        lines.append(f"* *{total_pending}* questions still pending review")
        lines.append(
            f"* *{self.learned_answers['metadata'].get('total_learned') or 0}* "
            f"total answers in knowledge base\n"
        )

        if recent_pending:
            lines.append("*Questions needing your attention:*")
            for idx, q in enumerate(recent_pending[:10], 1):
                lines.append(f"{idx}. _{q['question'][:100]}_")
            if len(recent_pending) > 10:
                lines.append(f"_...and {len(recent_pending) - 10} more_")
            lines.append("\nReview at: `/admin/nova`")
        else:
            lines.append("_No new questions need review -- Nova handled everything!_")

        return "\n".join(lines)

    def send_weekly_digest(self, channel: str) -> bool:
        """Generate and post the weekly digest to a Slack channel."""
        digest = self.generate_weekly_digest()
        if self.bot_token and channel:
            self._post_message(channel, digest)
            return True
        logger.warning("Nova: Cannot send digest -- no token or channel")
        return False

    # ------------------------------------------------------------------
    # Slack API helpers (stdlib only -- no slack_sdk)
    # ------------------------------------------------------------------

    # Maximum characters per Slack message
    _SLACK_MAX_CHARS = 4000

    def _post_message(
        self,
        channel: str,
        text: str,
        thread_ts: Optional[str] = None,
    ) -> Optional[dict]:
        """Post a message to Slack using ``chat.postMessage``.

        Converts standard markdown to Slack mrkdwn and splits messages
        that exceed Slack's 4000-character limit into multiple posts.
        """
        if not self.bot_token:
            logger.warning("Nova: No Slack bot token configured -- skipping post")
            return None

        # Convert standard markdown to Slack mrkdwn
        # Note: v2 metadata (data_confidence, data_freshness, sources_used)
        # is available in the Nova response but not currently surfaced in
        # Slack messages. Future enhancement: append confidence badges.
        text = _convert_to_slack_mrkdwn(text)

        # Split into chunks if the message exceeds the Slack limit
        chunks = self._split_message(text, self._SLACK_MAX_CHARS)

        last_result = None
        for chunk in chunks:
            result = self._post_single_message(channel, chunk, thread_ts)
            if result is not None:
                last_result = result
        return last_result

    @staticmethod
    def _split_message(text: str, max_chars: int) -> List[str]:
        """Split *text* into chunks of at most *max_chars*.

        Splits on newline boundaries when possible to keep formatting
        intact.  Falls back to hard-splitting at *max_chars* if a single
        line exceeds the limit.
        """
        if len(text) <= max_chars:
            return [text]

        chunks: List[str] = []
        current_chunk: List[str] = []
        current_len = 0

        for line in text.split("\n"):
            line_len = len(line) + 1  # +1 for the newline character

            # If a single line is longer than max_chars, hard-split it
            if line_len > max_chars:
                # Flush current chunk first
                if current_chunk:
                    chunks.append("\n".join(current_chunk))
                    current_chunk = []
                    current_len = 0
                # Hard-split the long line
                for i in range(0, len(line), max_chars):
                    chunks.append(line[i : i + max_chars])
                continue

            if current_len + line_len > max_chars:
                # Flush current chunk and start a new one
                chunks.append("\n".join(current_chunk))
                current_chunk = [line]
                current_len = line_len
            else:
                current_chunk.append(line)
                current_len += line_len

        if current_chunk:
            chunks.append("\n".join(current_chunk))

        return chunks

    # Slack API error codes that indicate an expired or invalid token
    _TOKEN_ERROR_CODES = frozenset({"token_expired", "invalid_auth", "token_revoked"})

    def _post_single_message(
        self,
        channel: str,
        text: str,
        thread_ts: Optional[str] = None,
        _retried: bool = False,
    ) -> Optional[dict]:
        """Post a single message chunk to Slack.

        If the API returns a token-related error and token rotation is
        configured, refreshes the token and retries once.
        """
        import urllib.request
        import urllib.error

        payload: Dict[str, Any] = {
            "channel": channel,
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        }
        if thread_ts:
            payload["thread_ts"] = thread_ts

        with self._token_lock:
            token = self.bot_token

        req = urllib.request.Request(
            "https://slack.com/api/chat.postMessage",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {token}",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                if not result.get("ok"):
                    error_code = result.get("error") or ""
                    logger.error("Nova: Slack API error: %s", error_code)
                    # Token expired/invalid -- refresh and retry once
                    if (
                        error_code in self._TOKEN_ERROR_CODES
                        and not _retried
                        and self._can_refresh_tokens()
                    ):
                        logger.info(
                            "Nova: Token error '%s' -- refreshing and retrying",
                            error_code,
                        )
                        if self._refresh_token():
                            return self._post_single_message(
                                channel,
                                text,
                                thread_ts,
                                _retried=True,
                            )
                return result
        except urllib.error.URLError as exc:
            logger.error("Nova: Failed to post to Slack: %s", exc)
            return None
        except Exception as exc:
            logger.error("Nova: Unexpected error posting to Slack: %s", exc)
            return None

    def _do_auth_test(self, token: str) -> Optional[dict]:
        """Execute a single ``auth.test`` call with the given *token*.

        Returns the parsed JSON result dict on success, or None on error.
        """
        import urllib.request
        import urllib.error

        req = urllib.request.Request(
            "https://slack.com/api/auth.test",
            data=b"",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Bearer {token}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            logger.error("Nova: auth.test request failed: %s", exc)
            return None

    def _auth_test(self) -> Optional[str]:
        """Call ``auth.test`` to determine the bot's own user ID.

        Resolution order on failure:
          1. Try current ``self.bot_token``
          2. If that fails with a token error, try loading cached token
          3. If cached token also fails, try refreshing immediately
        """
        if not self.bot_token:
            return None

        # --- Attempt 1: current token ---
        result = self._do_auth_test(self.bot_token)
        if result and result.get("ok"):
            self.bot_user_id = result.get("user_id")
            logger.info(
                "Nova: Authenticated as %s (user_id=%s)",
                result.get("user"),
                self.bot_user_id,
            )
            return self.bot_user_id

        error_code = result.get("error", "unknown") if result else "request_failed"
        logger.warning("Nova: auth.test failed with current token: %s", error_code)

        # --- Attempt 2: try cached token (if different from current) ---
        cached = self._load_token_cache()
        if (
            cached
            and cached.get("access_token")
            and cached["access_token"] != self.bot_token
        ):
            logger.info("Nova: Trying cached token for auth.test")
            with self._token_lock:
                self.bot_token = cached["access_token"]
                self.refresh_token = cached.get("refresh_token", self.refresh_token)
                self._token_issued_at = cached.get("issued_at", 0.0)
                self._token_expires_in = cached.get(
                    "expires_in", _TOKEN_DEFAULT_LIFETIME
                )

            result = self._do_auth_test(self.bot_token)
            if result and result.get("ok"):
                self.bot_user_id = result.get("user_id")
                logger.info(
                    "Nova: Authenticated via cached token as %s (user_id=%s)",
                    result.get("user"),
                    self.bot_user_id,
                )
                return self.bot_user_id
            logger.warning("Nova: Cached token also failed auth.test")

        # --- Attempt 3: refresh token immediately ---
        if self._can_refresh_tokens():
            logger.info("Nova: Attempting immediate token refresh for auth.test")
            if self._refresh_token():
                result = self._do_auth_test(self.bot_token)
                if result and result.get("ok"):
                    self.bot_user_id = result.get("user_id")
                    logger.info(
                        "Nova: Authenticated via refreshed token as %s (user_id=%s)",
                        result.get("user"),
                        self.bot_user_id,
                    )
                    return self.bot_user_id
                logger.error("Nova: auth.test failed even after token refresh")
            else:
                logger.error("Nova: Immediate token refresh failed")

        logger.error("Nova: All auth.test attempts exhausted")
        return None

    # ------------------------------------------------------------------
    # Bulk import for learned answers (admin utility)
    # ------------------------------------------------------------------

    def import_learned_answers(self, qa_pairs: List[Dict[str, str]]) -> int:
        """Bulk-import Q&A pairs into learned answers.

        Each item in *qa_pairs* must have ``question`` and ``answer`` keys.
        Returns the count of successfully imported pairs.
        """
        imported = 0
        with self._lock:
            for pair in qa_pairs:
                q = (pair.get("question") or "").strip()
                a = (pair.get("answer") or "").strip()
                if not q or not a:
                    continue
                # Cap at 2000 learned answers to prevent memory/disk exhaustion
                if len(self.learned_answers.get("answers") or []) >= 2000:
                    logger.warning(
                        "Learned answers cap (2000) reached; skipping remaining imports"
                    )
                    break
                self.learned_answers.setdefault("answers", []).append(
                    {
                        "question": q,
                        "answer": a,
                        "added_by": "bulk_import",
                        "added_at": datetime.utcnow().isoformat(),
                        "source": "bulk_import",
                        "times_used": 0,
                    }
                )
                imported += 1
            if imported:
                self.learned_answers["metadata"]["total_learned"] = len(
                    self.learned_answers["answers"]
                )
                self._save_learned_answers()
        return imported


# ============================================================================
# Module-level singleton
# ============================================================================

_nova_bot: Optional[NovaSlackBot] = None
_nova_bot_lock = threading.Lock()


def get_nova_bot() -> NovaSlackBot:
    """Return (or create) the module-level Nova bot singleton."""
    global _nova_bot
    if _nova_bot is None:
        with _nova_bot_lock:
            if _nova_bot is None:
                _nova_bot = NovaSlackBot()
    return _nova_bot


# ============================================================================
# HTTP handler functions (used by app.py)
# ============================================================================


def handle_slack_event(request_data: dict) -> dict:
    """Handle an incoming Slack event webhook (``/api/slack/events``)."""
    bot = get_nova_bot()
    return bot.handle_event(request_data)


def handle_admin_unanswered(request_data: dict) -> dict:
    """Handle admin dashboard API requests (``/api/admin/nova``).

    Supported actions:
    - ``list``    -- return the unanswered summary
    - ``answer``  -- resolve a question with a human-provided answer
    - ``dismiss`` -- dismiss a question
    - ``digest``  -- return the weekly digest text
    - ``import``  -- bulk-import Q&A pairs
    """
    bot = get_nova_bot()
    action = request_data.get("action", "list")

    if action == "list":
        return bot.get_unanswered_summary()

    if action == "answer":
        success = bot.resolve_question(
            request_data.get("question_id") or "",
            request_data.get("answer") or "",
            request_data.get("answered_by", "admin"),
        )
        return {"success": success}

    if action == "dismiss":
        success = bot.dismiss_question(
            request_data.get("question_id") or "",
            request_data.get("reason") or "",
        )
        return {"success": success}

    if action == "digest":
        return {"digest": bot.generate_weekly_digest()}

    if action == "import":
        pairs = request_data.get("qa_pairs") or []
        count = bot.import_learned_answers(pairs)
        return {"imported": count}

    return {"error": f"Unknown action: {action}"}


def handle_chat_standalone(request_data: dict) -> dict:
    """Handle a standalone chat request (non-Slack, for admin testing)."""
    bot = get_nova_bot()
    question = request_data.get("message") or request_data.get("question") or ""
    if not question:
        return {"error": "No question provided"}
    result = bot.answer_question(question)
    return {
        "response": result["text"],
        "confidence": result["confidence"],
        "sources": result["sources"],
        "tools_used": result.get("tools_used") or [],
        "queued_for_review": result["queued_for_review"],
    }
