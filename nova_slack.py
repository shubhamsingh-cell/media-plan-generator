"""
Nova Slack Bot -- Slack interface for Joveo's recruitment intelligence system.

Features:
- Responds to @Nova mentions and DMs in Slack
- Queries Joveo's data sources (publishers, channels, benchmarks, APIs)
- Searches Slack history for previously answered questions
- Maintains an unanswered question queue for human review
- Learns from human-provided answers to improve over time
- Sends weekly digest of unanswered questions

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

# ---------------------------------------------------------------------------
# Stop-words removed during keyword matching
# ---------------------------------------------------------------------------
_STOP_WORDS = frozenset(
    {
        "what", "is", "the", "a", "an", "how", "does", "can", "for", "in",
        "of", "to", "and", "or", "my", "our", "we", "do", "are", "it",
        "this", "that", "which", "with", "about", "on", "at", "be", "by",
        "from", "has", "have", "i", "me", "you", "your", "they", "their",
        "was", "were", "been", "being", "will", "would", "could", "should",
        "may", "might", "shall", "not", "no", "so", "if", "but", "up",
        "out", "there", "here", "when", "where", "why", "who", "whom",
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
    """Nova Slack Bot -- connects Joveo IQ intelligence to Slack."""

    def __init__(
        self,
        slack_bot_token: Optional[str] = None,
        slack_signing_secret: Optional[str] = None,
    ):
        self.bot_token: str = slack_bot_token or os.environ.get("SLACK_BOT_TOKEN", "")
        self.signing_secret: str = slack_signing_secret or os.environ.get(
            "SLACK_SIGNING_SECRET", ""
        )
        self.bot_user_id: Optional[str] = None  # Populated after auth.test

        # Thread lock for file I/O
        self._lock = threading.Lock()

        # In-memory caches (populated from disk)
        self.learned_answers: Dict[str, Any] = {}
        self.unanswered: Dict[str, Any] = {}

        self._load_learned_answers()
        self._load_unanswered_questions()

        # Joveo IQ engine (optional dependency)
        self._iq_engine: Any = None
        self._init_iq_engine()

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _init_iq_engine(self) -> None:
        """Import and initialise the Joveo IQ chat engine (if available)."""
        try:
            from joveo_iq import JoveoIQ  # type: ignore[import-untyped]

            self._iq_engine = JoveoIQ()
            logger.info("Nova: Joveo IQ engine initialised successfully")
        except ImportError:
            logger.warning(
                "Nova: joveo_iq module not available -- running in standalone mode"
            )
        except Exception as exc:
            logger.error("Nova: Failed to initialise Joveo IQ engine: %s", exc)

    # ------------------------------------------------------------------
    # Persistence -- learned answers
    # ------------------------------------------------------------------

    def _load_learned_answers(self) -> None:
        """Load previously learned answers from disk."""
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

    def verify_slack_signature(
        self, timestamp: str, body: str, signature: str
    ) -> bool:
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
            return {"challenge": event_data.get("challenge", "")}

        if event_type == "event_callback":
            event = event_data.get("event", {})
            return self._process_event(event)

        return {"ok": True}

    def _process_event(self, event: dict) -> dict:
        """Process a single ``message`` or ``app_mention`` event."""
        etype = event.get("type")
        text = event.get("text", "")
        user = event.get("user", "")
        channel = event.get("channel", "")
        ts = event.get("ts", "")
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

        # Generate a response
        response = self.answer_question(clean_text, user, channel, thread_ts)

        # Post to Slack (non-blocking best-effort)
        self._post_message(channel, response["text"], thread_ts)

        return {"ok": True}

    # ------------------------------------------------------------------
    # Core Q&A pipeline
    # ------------------------------------------------------------------

    def answer_question(
        self,
        question: str,
        user_id: str = "",
        channel: str = "",
        thread_ts: str = "",
    ) -> Dict[str, Any]:
        """Answer a question using all available sources.

        Resolution order:
          1. Check learned answers (exact / fuzzy keyword match)
          2. Query Joveo IQ engine (data sources + tools)
          3. Fall back to partial learned match
          4. If confidence < threshold, queue for human review

        Returns a dict with ``text``, ``confidence``, ``sources``, and
        ``queued_for_review``.
        """
        # -- Step 1: Learned answers (strong match) -----------------------
        learned = self._search_learned_answers(question)
        if learned and learned["confidence"] >= 0.8:
            self._increment_usage(learned.get("original_question", ""))
            return {
                "text": (
                    f"*Nova says:*\n\n{learned['answer']}\n\n"
                    f"_Source: Previously answered by team "
                    f"| Confidence: {learned['confidence']:.0%}_"
                ),
                "confidence": learned["confidence"],
                "sources": ["Learned Answers"],
                "queued_for_review": False,
            }

        # -- Step 2: Joveo IQ engine --------------------------------------
        iq_response: Optional[Dict[str, Any]] = None
        if self._iq_engine:
            try:
                iq_response = self._iq_engine.chat(question)
                if iq_response and iq_response.get("confidence", 0) >= 0.5:
                    src_text = ", ".join(iq_response.get("sources", ["Joveo Data"]))
                    conf = iq_response["confidence"]
                    return {
                        "text": (
                            f"*Nova says:*\n\n{iq_response['response']}\n\n"
                            f"_Sources: {src_text} | Confidence: {conf:.0%}_"
                        ),
                        "confidence": conf,
                        "sources": iq_response.get("sources", []),
                        "queued_for_review": False,
                    }
            except Exception as exc:
                logger.error("Nova: IQ engine error: %s", exc)

        # -- Step 3: Partial learned match ---------------------------------
        if learned and learned["confidence"] >= 0.4:
            self._increment_usage(learned.get("original_question", ""))
            return {
                "text": (
                    f"*Nova says:*\n\n{learned['answer']}\n\n"
                    f"_Note: Based on a similar question I've seen before. "
                    f"Confidence: {learned['confidence']:.0%}_"
                ),
                "confidence": learned["confidence"],
                "sources": ["Learned Answers (partial match)"],
                "queued_for_review": False,
            }

        # -- Step 4: Low confidence -- queue for review --------------------
        partial_text = (
            iq_response.get("response", "") if iq_response else ""
        )
        self._add_to_unanswered(
            question, user_id, channel, thread_ts, partial_text
        )

        partial_block = ""
        if partial_text:
            partial_block = (
                f"\n\nHere's what I found so far:\n{partial_text}\n"
            )

        iq_conf = iq_response.get("confidence", 0) if iq_response else 0.0
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
            "queued_for_review": True,
        }

    # ------------------------------------------------------------------
    # Learned-answer search
    # ------------------------------------------------------------------

    def _search_learned_answers(self, question: str) -> Optional[Dict[str, Any]]:
        """Search learned answers using Jaccard keyword similarity."""
        answers = self.learned_answers.get("answers", [])
        if not answers:
            return None

        q_words = self._extract_keywords(question)
        if not q_words:
            return None

        best_match: Optional[dict] = None
        best_score: float = 0.0

        for entry in answers:
            a_words = self._extract_keywords(entry.get("question", ""))
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
            for entry in self.learned_answers.get("answers", []):
                if entry.get("question") == original_question:
                    entry["times_used"] = entry.get("times_used", 0) + 1
                    try:
                        self._save_learned_answers()
                    except OSError:
                        pass
                    break

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
            "id": hashlib.md5(
                f"{question}{time.time()}".encode()
            ).hexdigest()[:12],
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
            for q in self.unanswered.get("questions", []):
                if q["id"] == question_id and q["status"] == "pending":
                    q["status"] = "answered"
                    q["answer"] = answer
                    q["answered_by"] = answered_by
                    q["answered_at"] = datetime.utcnow().isoformat()

                    # Persist to learned answers
                    self.learned_answers.setdefault("answers", []).append(
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
                    channel = q.get("channel", "")
                    thread = q.get("thread_ts", "")
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
            for q in self.unanswered.get("questions", []):
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
        questions = self.unanswered.get("questions", [])
        pending = [q for q in questions if q["status"] == "pending"]
        answered = [q for q in questions if q["status"] == "answered"]
        dismissed = [q for q in questions if q["status"] == "dismissed"]

        return {
            "pending_count": len(pending),
            "answered_count": len(answered),
            "dismissed_count": len(dismissed),
            "total_learned": self.learned_answers["metadata"].get("total_learned", 0),
            "pending_questions": sorted(
                pending, key=lambda q: q.get("timestamp", ""), reverse=True
            ),
            "recent_answered": sorted(
                answered, key=lambda q: q.get("answered_at", ""), reverse=True
            )[:10],
        }

    def generate_weekly_digest(self) -> str:
        """Build a Slack-formatted weekly digest message."""
        week_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()

        questions = self.unanswered.get("questions", [])
        recent_pending = [
            q
            for q in questions
            if q["status"] == "pending" and q.get("timestamp", "") >= week_ago
        ]
        recent_answered = [
            q
            for q in questions
            if q["status"] == "answered"
            and q.get("answered_at", "") >= week_ago
        ]
        total_pending = len(
            [q for q in questions if q["status"] == "pending"]
        )

        lines: List[str] = []
        lines.append("*Nova Weekly Digest*\n")
        lines.append(f"* *{len(recent_pending)}* new questions this week")
        lines.append(f"* *{len(recent_answered)}* questions answered this week")
        lines.append(f"* *{total_pending}* questions still pending review")
        lines.append(
            f"* *{self.learned_answers['metadata'].get('total_learned', 0)}* "
            f"total answers in knowledge base\n"
        )

        if recent_pending:
            lines.append("*Questions needing your attention:*")
            for idx, q in enumerate(recent_pending[:10], 1):
                lines.append(f"{idx}. _{q['question'][:100]}_")
            if len(recent_pending) > 10:
                lines.append(
                    f"_...and {len(recent_pending) - 10} more_"
                )
            lines.append("\nReview at: `/admin/nova`")
        else:
            lines.append(
                "_No new questions need review -- Nova handled everything!_"
            )

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

    def _post_message(
        self,
        channel: str,
        text: str,
        thread_ts: Optional[str] = None,
    ) -> Optional[dict]:
        """Post a message to Slack using ``chat.postMessage``."""
        if not self.bot_token:
            logger.warning(
                "Nova: No Slack bot token configured -- skipping post"
            )
            return None

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

        req = urllib.request.Request(
            "https://slack.com/api/chat.postMessage",
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {self.bot_token}",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                if not result.get("ok"):
                    logger.error(
                        "Nova: Slack API error: %s", result.get("error")
                    )
                return result
        except urllib.error.URLError as exc:
            logger.error("Nova: Failed to post to Slack: %s", exc)
            return None
        except Exception as exc:
            logger.error("Nova: Unexpected error posting to Slack: %s", exc)
            return None

    def _auth_test(self) -> Optional[str]:
        """Call ``auth.test`` to determine the bot's own user ID."""
        if not self.bot_token:
            return None

        import urllib.request
        import urllib.error

        req = urllib.request.Request(
            "https://slack.com/api/auth.test",
            data=b"",
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Bearer {self.bot_token}",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                if result.get("ok"):
                    self.bot_user_id = result.get("user_id")
                    logger.info(
                        "Nova: Authenticated as %s (user_id=%s)",
                        result.get("user"),
                        self.bot_user_id,
                    )
                    return self.bot_user_id
                logger.error(
                    "Nova: auth.test failed: %s", result.get("error")
                )
        except Exception as exc:
            logger.error("Nova: auth.test request failed: %s", exc)
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
                q = pair.get("question", "").strip()
                a = pair.get("answer", "").strip()
                if not q or not a:
                    continue
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
            request_data.get("question_id", ""),
            request_data.get("answer", ""),
            request_data.get("answered_by", "admin"),
        )
        return {"success": success}

    if action == "dismiss":
        success = bot.dismiss_question(
            request_data.get("question_id", ""),
            request_data.get("reason", ""),
        )
        return {"success": success}

    if action == "digest":
        return {"digest": bot.generate_weekly_digest()}

    if action == "import":
        pairs = request_data.get("qa_pairs", [])
        count = bot.import_learned_answers(pairs)
        return {"imported": count}

    return {"error": f"Unknown action: {action}"}


def handle_chat_standalone(request_data: dict) -> dict:
    """Handle a standalone chat request (non-Slack, for admin testing)."""
    bot = get_nova_bot()
    question = request_data.get("message") or request_data.get("question", "")
    if not question:
        return {"error": "No question provided"}
    result = bot.answer_question(question)
    return {
        "response": result["text"],
        "confidence": result["confidence"],
        "sources": result["sources"],
        "queued_for_review": result["queued_for_review"],
    }
