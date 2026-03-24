"""
ApplyFlow -- Conversational Apply Widget Engine (v1.0)

AI-powered conversational application experience that replaces traditional
job application forms. Candidates interact with an AI chat agent that screens,
qualifies, collects information, and guides them through applying.

Works in two modes:
1. Rule-based (default): template-driven conversational responses
2. LLM-enhanced (optional): uses llm_router for natural language generation

Candidate-facing evolution of the Nova chatbot architecture.
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy-loaded dependencies (avoid circular imports, graceful degradation)
# ---------------------------------------------------------------------------
_collar_intel = None
_collar_intel_lock = threading.Lock()

_research_mod = None
_research_mod_lock = threading.Lock()

_llm_router = None
_llm_router_lock = threading.Lock()

_data_orchestrator = None
_data_orchestrator_lock = threading.Lock()


def _get_collar_intel():
    global _collar_intel
    if _collar_intel is None:
        with _collar_intel_lock:
            if _collar_intel is None:
                try:
                    import collar_intelligence

                    _collar_intel = collar_intelligence
                    logger.info("ApplyFlow: collar_intelligence loaded")
                except Exception as e:
                    logger.warning("ApplyFlow: collar_intelligence unavailable: %s", e)
                    _collar_intel = False
    return _collar_intel if _collar_intel is not False else None


def _get_research():
    global _research_mod
    if _research_mod is None:
        with _research_mod_lock:
            if _research_mod is None:
                try:
                    import research

                    _research_mod = research
                    logger.info("ApplyFlow: research module loaded")
                except Exception as e:
                    logger.warning("ApplyFlow: research module unavailable: %s", e)
                    _research_mod = False
    return _research_mod if _research_mod is not False else None


def _get_llm_router():
    global _llm_router
    if _llm_router is None:
        with _llm_router_lock:
            if _llm_router is None:
                try:
                    import llm_router

                    _llm_router = llm_router
                    logger.info("ApplyFlow: llm_router loaded")
                except Exception as e:
                    logger.warning("ApplyFlow: llm_router unavailable: %s", e)
                    _llm_router = False
    return _llm_router if _llm_router is not False else None


def _get_data_orchestrator():
    global _data_orchestrator
    if _data_orchestrator is None:
        with _data_orchestrator_lock:
            if _data_orchestrator is None:
                try:
                    import data_orchestrator

                    _data_orchestrator = data_orchestrator
                    logger.info("ApplyFlow: data_orchestrator loaded")
                except Exception as e:
                    logger.warning("ApplyFlow: data_orchestrator unavailable: %s", e)
                    _data_orchestrator = False
    return _data_orchestrator if _data_orchestrator is not False else None


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_ACTIVE_SESSIONS = 1000
SESSION_TTL_SECONDS = 3600  # 1 hour
MAX_MESSAGE_LENGTH = 2000

# Conversation stages (ordered)
STAGE_GREETING = "greeting"
STAGE_QUALIFICATION = "qualification"
STAGE_EXPERIENCE = "experience"
STAGE_SKILLS = "skills"
STAGE_CONTACT = "contact"
STAGE_CONFIRMATION = "confirmation"
STAGE_COMPLETE = "complete"

STAGES_ORDERED = [
    STAGE_GREETING,
    STAGE_QUALIFICATION,
    STAGE_EXPERIENCE,
    STAGE_SKILLS,
    STAGE_CONTACT,
    STAGE_CONFIRMATION,
    STAGE_COMPLETE,
]

STAGE_LABELS = {
    STAGE_GREETING: "Welcome",
    STAGE_QUALIFICATION: "Screening",
    STAGE_EXPERIENCE: "Experience",
    STAGE_SKILLS: "Skills",
    STAGE_CONTACT: "Contact Info",
    STAGE_CONFIRMATION: "Review",
    STAGE_COMPLETE: "Complete",
}

# ---------------------------------------------------------------------------
# Data Extraction Utilities
# ---------------------------------------------------------------------------

_EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE
)

_PHONE_PATTERN = re.compile(
    r"(?:(?:\+?1[\s.\-]?)?"
    r"(?:\(?\d{3}\)?[\s.\-]?)?\d{3}[\s.\-]?\d{4})"
    r"|(?:\+\d{1,3}[\s.\-]?\d{4,14})",
    re.IGNORECASE,
)

_YEARS_PATTERNS = [
    # "5 years", "5+ years", "5-7 years"
    (
        re.compile(r"(\d{1,2})\s*[\+]?\s*(?:to|-)\s*(\d{1,2})\s*(?:years?|yrs?)", re.I),
        lambda m: (int(m.group(1)) + int(m.group(2))) / 2.0,
    ),
    # "5 years", "5+ years"
    (
        re.compile(r"(\d{1,2})\s*\+?\s*(?:years?|yrs?)", re.I),
        lambda m: float(m.group(1)),
    ),
    # "over/about/around/nearly a decade"
    (
        re.compile(r"(?:over|about|around|nearly|almost)\s+a\s+decade", re.I),
        lambda m: 10.0,
    ),
    # "a decade"
    (re.compile(r"a\s+decade", re.I), lambda m: 10.0),
    # "over/about/around X"
    (
        re.compile(r"(?:over|about|around|nearly|almost|roughly)\s+(\d{1,2})", re.I),
        lambda m: float(m.group(1)),
    ),
    # "couple of years"
    (re.compile(r"couple\s+(?:of\s+)?(?:years?|yrs?)", re.I), lambda m: 2.0),
    # "few years"
    (re.compile(r"few\s+(?:years?|yrs?)", re.I), lambda m: 3.0),
    # "several years"
    (re.compile(r"several\s+(?:years?|yrs?)", re.I), lambda m: 5.0),
    # "no experience" / "none" / "just starting"
    (
        re.compile(
            r"\b(?:no\s+experience|none|just\s+starting|fresh\s+grad|new\s+grad|entry[\s\-]?level)\b",
            re.I,
        ),
        lambda m: 0.0,
    ),
]

_SALARY_PATTERNS = [
    # "$80,000 - $100,000" or "$80k-$100k"
    (
        re.compile(
            r"\$?\s*([\d,]+)\s*[kK]?\s*(?:to|-|and)\s*\$?\s*([\d,]+)\s*[kK]?", re.I
        ),
        lambda m: _parse_salary_range(
            m.group(1), m.group(2), "k" in m.group(0).lower()
        ),
    ),
    # "$80k" or "$80,000" or "80000"
    (
        re.compile(r"\$?\s*([\d,]+)\s*[kK]", re.I),
        lambda m: float(m.group(1).replace(",", "")) * 1000,
    ),
    # "$80,000" or "$80000"
    (re.compile(r"\$\s*([\d,]+)", re.I), lambda m: float(m.group(1).replace(",", ""))),
    # "80000" (plain number >= 10000)
    (re.compile(r"\b(\d{5,})\b"), lambda m: float(m.group(1))),
]


def _parse_salary_range(low_str: str, high_str: str, has_k: bool) -> float:
    """Parse a salary range and return the midpoint."""
    low = float(low_str.replace(",", ""))
    high = float(high_str.replace(",", ""))
    if has_k or low < 1000:
        low *= 1000
        high *= 1000
    return (low + high) / 2.0


def extract_email(text: str) -> Optional[str]:
    """Extract first email address from text."""
    if not text:
        return None
    match = _EMAIL_PATTERN.search(text)
    return match.group(0).lower() if match else None


def extract_phone(text: str) -> Optional[str]:
    """Extract first phone number from text."""
    if not text:
        return None
    match = _PHONE_PATTERN.search(text)
    if match:
        phone = re.sub(r"[^\d+]", "", match.group(0))
        return phone if len(phone) >= 7 else None
    return None


def extract_years_experience(text: str) -> Optional[float]:
    """Parse years of experience from natural language text."""
    if not text:
        return None
    for pattern, extractor in _YEARS_PATTERNS:
        match = pattern.search(text)
        if match:
            return extractor(match)
    return None


def extract_salary_expectation(text: str) -> Optional[float]:
    """Parse salary expectation from natural language text."""
    if not text:
        return None
    for pattern, extractor in _SALARY_PATTERNS:
        match = pattern.search(text)
        if match:
            val = extractor(match)
            if val and val > 0:
                return val
    return None


def extract_name(text: str) -> Optional[str]:
    """Best-effort name extraction from introductory responses."""
    if not text or len(text) > 200:
        return None
    text_clean = text.strip()
    # "My name is John Smith" / "I'm John Smith" / "I am John"
    name_patterns = [
        re.compile(
            r"(?:my\s+name\s+is|i'?m|i\s+am|call\s+me|this\s+is)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            re.I,
        ),
        re.compile(r"^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)$"),  # Just a name, nothing else
        re.compile(
            r"^(?:hi|hey|hello)[,!.]?\s*(?:i'?m|i\s+am|my\s+name\s+is)?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
            re.I,
        ),
    ]
    for pattern in name_patterns:
        match = pattern.search(text_clean)
        if match:
            name = match.group(1).strip()
            # Filter out common non-name words
            lower = name.lower()
            if lower not in {
                "interested",
                "applying",
                "looking",
                "ready",
                "yes",
                "no",
                "sure",
                "okay",
                "great",
                "good",
                "thanks",
                "the",
                "a",
                "an",
                "and",
                "or",
                "not",
                "expert",
                "proficient",
                "beginner",
                "basic",
                "advanced",
                "some",
                "none",
                "skip",
            }:
                return name.title()
    return None


def extract_start_date(text: str) -> Optional[str]:
    """Extract preferred start date from text."""
    if not text:
        return None
    # "immediately" / "right away" / "asap"
    if re.search(
        r"\b(?:immediately|right\s+away|asap|as\s+soon\s+as\s+possible|now)\b",
        text,
        re.I,
    ):
        return "Immediately"
    # "2 weeks" / "two weeks notice"
    if re.search(r"\b(?:2|two)\s*weeks?\b", text, re.I):
        return "2 weeks notice"
    # "1 month" / "one month" / "30 days"
    if re.search(r"\b(?:1|one)\s*months?\b|\b30\s*days?\b", text, re.I):
        return "1 month notice"
    # Specific date patterns: "January 15" / "Jan 15, 2026" / "01/15/2026"
    date_match = re.search(
        r"\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|"
        r"Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
        r"\s+\d{1,2}(?:\s*,?\s*\d{4})?\b",
        text,
        re.I,
    )
    if date_match:
        return date_match.group(0).strip()
    date_match2 = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", text)
    if date_match2:
        return date_match2.group(0)
    return None


# ---------------------------------------------------------------------------
# Job Configuration
# ---------------------------------------------------------------------------


def create_job_config(
    role: str,
    company: str,
    location: str,
    requirements: Optional[List[str]] = None,
    screening_questions: Optional[List[str]] = None,
    industry: str = "",
) -> Dict[str, Any]:
    """Create a job configuration dict that drives the ApplyFlow conversation.

    Auto-enriches with collar_intelligence (collar type, skills needed)
    and research.py (salary range, market data) when available.

    Args:
        role: Job title (e.g., "Software Engineer")
        company: Company name (e.g., "Acme Corp")
        location: Location (e.g., "San Francisco, CA")
        requirements: Optional list of requirements (bullet points)
        screening_questions: Optional list of custom screening questions (max 5)
        industry: Optional industry key for collar intelligence

    Returns:
        Dict with all job configuration needed for the conversation.
    """
    config: Dict[str, Any] = {
        "job_id": str(uuid.uuid4())[:12],
        "role": role.strip() if role else "Open Position",
        "company": company.strip() if company else "Company",
        "location": location.strip() if location else "Remote",
        "requirements": (requirements or [])[:10],
        "screening_questions": (screening_questions or [])[:5],
        "industry": industry,
        "collar_type": "white_collar",
        "collar_confidence": 0.5,
        "key_skills": [],
        "salary_range": "",
        "channel_strategy": "targeted",
        "created_at": time.time(),
    }

    # ── Enrich with collar intelligence ──
    ci = _get_collar_intel()
    if ci:
        try:
            collar_result = ci.classify_collar(config["role"], config["industry"])
            config["collar_type"] = collar_result.get("collar_type", "white_collar")
            config["collar_confidence"] = collar_result.get("confidence", 0.5)
            config["channel_strategy"] = collar_result.get(
                "channel_strategy", "targeted"
            )
            # Extract key skills from collar indicators if available
            indicators = collar_result.get("indicators") or []
            if indicators:
                config["key_skills"] = indicators[:6]
        except Exception as e:
            logger.warning("ApplyFlow: collar classification failed: %s", e)

    # ── Enrich with salary data ──
    research = _get_research()
    if research:
        try:
            loc_info = research.get_location_info(config["location"])
            coli = loc_info.get("coli", 100) if loc_info else 100
            salary_range = research.get_role_salary_range(config["role"], coli)
            if salary_range:
                config["salary_range"] = salary_range
        except Exception as e:
            logger.warning("ApplyFlow: salary enrichment failed: %s", e)

    # ── Enrich with data orchestrator salary ──
    if not config["salary_range"]:
        orch = _get_data_orchestrator()
        if orch:
            try:
                salary_data = orch.enrich_salary(
                    config["role"], config["location"], config["industry"]
                )
                sr = salary_data.get("salary_range") or ""
                if sr:
                    config["salary_range"] = sr
            except Exception as e:
                logger.warning(
                    "ApplyFlow: orchestrator salary enrichment failed: %s", e
                )

    # ── Generate screening questions based on collar type if none provided ──
    if not config["screening_questions"]:
        config["screening_questions"] = _generate_screening_questions(config)

    # ── Generate key skills if not from collar intelligence ──
    if not config["key_skills"]:
        config["key_skills"] = _infer_key_skills(config)

    return config


def _generate_screening_questions(config: Dict[str, Any]) -> List[str]:
    """Generate default screening questions based on collar type and role."""
    role = config.get("role", "this position")
    collar = config.get("collar_type", "white_collar")

    if collar == "blue_collar":
        return [
            f"Do you have any relevant certifications or licenses for this {role} position?",
            f"Are you comfortable with the physical requirements of a {role} role?",
            "Do you have a valid driver's license and reliable transportation?",
        ]
    elif collar == "pink_collar":
        return [
            f"How many years of experience do you have in a {role} or similar role?",
            f"Do you have any relevant certifications for the {role} position?",
            "Are you available to work flexible hours, including weekends if needed?",
        ]
    elif collar == "grey_collar":
        return [
            f"Do you have relevant technical certifications for the {role} role?",
            f"How many years of hands-on experience do you have as a {role}?",
            "Are you comfortable working in both field and office environments?",
        ]
    else:  # white_collar
        return [
            f"How many years of experience do you have in a {role} or similar role?",
            f"What is your highest level of education relevant to the {role} position?",
            "Are you authorized to work in the location listed for this position?",
        ]


def _infer_key_skills(config: Dict[str, Any]) -> List[str]:
    """Infer key skills from role title and requirements."""
    skills: List[str] = []
    role_lower = (config.get("role") or "").lower()

    # Extract skills from requirements
    for req in config.get("requirements") or []:
        cleaned = req.strip().strip("-").strip("*").strip()
        if cleaned and len(cleaned) < 80:
            skills.append(cleaned)

    # Common role-based skill inference
    role_skills_map = {
        "software": ["Programming", "Problem solving", "Version control"],
        "engineer": ["Technical analysis", "System design"],
        "nurse": ["Patient care", "Clinical assessment", "EMR systems"],
        "nursing": ["Patient care", "Clinical assessment", "EMR systems"],
        "driver": ["CDL license", "Route navigation", "Safety compliance"],
        "truck": ["CDL license", "DOT regulations", "Long-haul driving"],
        "sales": ["CRM proficiency", "Negotiation", "Pipeline management"],
        "marketing": ["Campaign management", "Analytics", "Content creation"],
        "data": ["SQL", "Data analysis", "Statistical modeling"],
        "manager": ["Team leadership", "Strategic planning", "Budget management"],
        "warehouse": ["Inventory management", "Forklift operation", "Safety protocols"],
        "retail": ["Customer service", "POS systems", "Inventory management"],
        "teacher": [
            "Curriculum development",
            "Classroom management",
            "Student assessment",
        ],
        "accountant": ["GAAP", "Financial reporting", "Tax preparation"],
        "designer": ["Design tools", "UI/UX principles", "Visual communication"],
        "analyst": ["Data analysis", "Reporting", "Critical thinking"],
        "admin": ["Organization", "Communication", "Office software"],
        "mechanic": ["Diagnostics", "Repair procedures", "Safety standards"],
        "electrician": ["Electrical codes", "Wiring", "Safety compliance"],
        "plumber": ["Plumbing codes", "Pipe fitting", "Problem diagnosis"],
        "chef": ["Food safety", "Menu planning", "Kitchen management"],
        "security": ["Surveillance", "Emergency response", "Access control"],
    }

    for keyword, role_skills in role_skills_map.items():
        if keyword in role_lower:
            for s in role_skills:
                if s not in skills:
                    skills.append(s)
            break

    return skills[:6]


# ---------------------------------------------------------------------------
# ApplyFlow Metrics (lightweight, thread-safe)
# ---------------------------------------------------------------------------


class _ApplyFlowMetrics:
    """Track ApplyFlow performance counters."""

    def __init__(self):
        self._lock = threading.Lock()
        self._start_time = time.time()
        self.total_sessions: int = 0
        self.completed_sessions: int = 0
        self.abandoned_sessions: int = 0
        self.total_messages: int = 0
        self.llm_calls: int = 0
        self.rule_based_calls: int = 0
        self._stage_counts: Dict[str, int] = {}

    def record_session_start(self) -> None:
        with self._lock:
            self.total_sessions += 1

    def record_session_complete(self) -> None:
        with self._lock:
            self.completed_sessions += 1

    def record_message(self) -> None:
        with self._lock:
            self.total_messages += 1

    def record_stage(self, stage: str) -> None:
        with self._lock:
            self._stage_counts[stage] = self._stage_counts.get(stage, 0) + 1

    def record_llm_call(self) -> None:
        with self._lock:
            self.llm_calls += 1

    def record_rule_based(self) -> None:
        with self._lock:
            self.rule_based_calls += 1

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            uptime = time.time() - self._start_time
            return {
                "uptime_seconds": round(uptime, 1),
                "total_sessions": self.total_sessions,
                "completed_sessions": self.completed_sessions,
                "abandoned_sessions": self.abandoned_sessions,
                "completion_rate": round(
                    self.completed_sessions / max(self.total_sessions, 1) * 100, 1
                ),
                "total_messages": self.total_messages,
                "llm_calls": self.llm_calls,
                "rule_based_calls": self.rule_based_calls,
                "stage_distribution": dict(self._stage_counts),
            }


_metrics = _ApplyFlowMetrics()


# ---------------------------------------------------------------------------
# ApplyFlow Session
# ---------------------------------------------------------------------------


class ApplyFlowSession:
    """Manages a single candidate's application conversation."""

    def __init__(self, session_id: str, job_config: Dict[str, Any]):
        self.session_id: str = session_id
        self.job_config: Dict[str, Any] = dict(job_config)
        self.state: str = STAGE_GREETING
        self.conversation_history: List[Dict[str, Any]] = []
        self.collected_data: Dict[str, Any] = {
            "name": None,
            "email": None,
            "phone": None,
            "years_experience": None,
            "salary_expectation": None,
            "start_date": None,
            "education": None,
            "current_role": None,
            "reason_for_looking": None,
            "skills_confirmed": [],
            "screening_answers": [],
            "work_authorization": None,
        }
        self.qualification_score: float = 50.0  # Start at neutral
        self.created_at: float = time.time()
        self.last_active: float = time.time()
        self._qualification_sub_question: int = 0
        self._skills_sub_question: int = 0
        self._contact_sub_field: int = 0
        self._experience_sub_question: int = 0

    @property
    def progress_pct(self) -> int:
        """Return progress percentage (0-100) based on current stage."""
        try:
            idx = STAGES_ORDERED.index(self.state)
        except ValueError:
            idx = 0
        return int((idx / (len(STAGES_ORDERED) - 1)) * 100)

    @property
    def is_expired(self) -> bool:
        return (time.time() - self.last_active) > SESSION_TTL_SECONDS

    @property
    def collected_fields(self) -> List[str]:
        """Return list of field names that have been collected."""
        return [
            k
            for k, v in self.collected_data.items()
            if v is not None and v != [] and v != ""
        ]

    def add_message(self, role: str, content: str) -> None:
        self.conversation_history.append(
            {
                "role": role,
                "content": content,
                "timestamp": time.time(),
            }
        )
        self.last_active = time.time()

    def to_summary(self) -> Dict[str, Any]:
        """Get structured application summary."""
        return {
            "session_id": self.session_id,
            "job_id": self.job_config.get("job_id") or "",
            "role": self.job_config.get("role") or "",
            "company": self.job_config.get("company") or "",
            "location": self.job_config.get("location") or "",
            "stage": self.state,
            "stage_label": STAGE_LABELS.get(self.state, self.state),
            "progress_pct": self.progress_pct,
            "qualification_score": round(self.qualification_score, 1),
            "collected_data": dict(self.collected_data),
            "collected_fields": self.collected_fields,
            "message_count": len(self.conversation_history),
            "created_at": self.created_at,
            "last_active": self.last_active,
            "is_complete": self.state == STAGE_COMPLETE,
        }


# ---------------------------------------------------------------------------
# Session Management (thread-safe, LRU eviction)
# ---------------------------------------------------------------------------

_sessions: OrderedDict[str, ApplyFlowSession] = OrderedDict()
_sessions_lock = threading.Lock()

# Store completed applications separately so they persist after session eviction
_completed_applications: Dict[str, List[Dict[str, Any]]] = {}  # job_id -> [summaries]
_completed_lock = threading.Lock()


def _evict_expired_sessions() -> None:
    """Remove expired sessions and enforce max capacity."""
    now = time.time()
    expired = [
        sid
        for sid, s in _sessions.items()
        if (now - s.last_active) > SESSION_TTL_SECONDS
    ]
    for sid in expired:
        session = _sessions.pop(sid, None)
        if session and session.state != STAGE_COMPLETE:
            _metrics.abandoned_sessions += 1

    # LRU eviction if over capacity
    while len(_sessions) > MAX_ACTIVE_SESSIONS:
        oldest_sid, oldest_session = _sessions.popitem(last=False)
        if oldest_session.state != STAGE_COMPLETE:
            _metrics.abandoned_sessions += 1


def _get_or_create_session(
    session_id: str, job_config: Dict[str, Any]
) -> ApplyFlowSession:
    """Get existing session or create a new one. Thread-safe."""
    with _sessions_lock:
        _evict_expired_sessions()
        if session_id in _sessions:
            session = _sessions[session_id]
            _sessions.move_to_end(session_id)
            return session
        # Create new session
        session = ApplyFlowSession(session_id, job_config)
        _sessions[session_id] = session
        _metrics.record_session_start()
        return session


def get_session_summary(session_id: str) -> Optional[Dict[str, Any]]:
    """Get structured application data for a session."""
    with _sessions_lock:
        session = _sessions.get(session_id)
        if session:
            return session.to_summary()
    return None


def get_all_applications(job_id: str) -> List[Dict[str, Any]]:
    """List all completed applications for a job."""
    with _completed_lock:
        return list(_completed_applications.get(job_id, []))


def get_metrics() -> Dict[str, Any]:
    """Return ApplyFlow metrics snapshot."""
    return _metrics.snapshot()


# ---------------------------------------------------------------------------
# Response Templates (rule-based, warm and conversational)
# ---------------------------------------------------------------------------


class _Templates:
    """Rule-based conversational templates. Warm, professional, candidate-friendly."""

    @staticmethod
    def greeting(config: Dict[str, Any]) -> Tuple[str, List[str]]:
        """Generate greeting message and quick-reply chips."""
        role = config.get("role", "this position")
        company = config.get("company", "our company")
        location = config.get("location") or ""
        salary_range = config.get("salary_range") or ""

        loc_part = f" in {location}" if location else ""
        salary_part = ""
        if salary_range:
            salary_part = f"\n\nThe salary range for this role is {salary_range}."

        msg = (
            f"Hi there! Thanks for your interest in the **{role}** position "
            f"at **{company}**{loc_part}.\n\n"
            f"I'm here to help you apply quickly and easily. Instead of filling "
            f"out a long form, we'll have a quick conversation -- it should take "
            f"about 3-5 minutes.{salary_part}\n\n"
            f"Ready to get started?"
        )
        chips = ["Yes, let's go!", "Tell me more about the role", "Not right now"]
        return msg, chips

    @staticmethod
    def qualification_question(
        config: Dict[str, Any], q_index: int
    ) -> Tuple[str, List[str]]:
        """Generate a screening question."""
        questions = config.get("screening_questions") or []
        if q_index >= len(questions):
            return "", []

        question = questions[q_index]
        prefix = ""
        if q_index == 0:
            prefix = "Great! Let me ask you a few quick screening questions.\n\n"

        chips = []
        q_lower = question.lower()
        if "years" in q_lower or "experience" in q_lower:
            chips = [
                "Less than 1 year",
                "1-3 years",
                "3-5 years",
                "5-10 years",
                "10+ years",
            ]
        elif "certification" in q_lower or "license" in q_lower:
            chips = ["Yes, I do", "Working on it", "No, but willing to get one"]
        elif "education" in q_lower:
            chips = ["High school", "Associate's", "Bachelor's", "Master's", "PhD"]
        elif "authorized" in q_lower or "authorization" in q_lower:
            chips = ["Yes", "Yes, with sponsorship", "No"]
        elif "comfortable" in q_lower or "available" in q_lower or "driver" in q_lower:
            chips = ["Yes", "No", "It depends"]
        else:
            chips = ["Yes", "No"]

        return f"{prefix}{question}", chips

    @staticmethod
    def qualification_followup(answer: str, q_index: int, total: int) -> str:
        """Acknowledge screening answer."""
        acknowledgments = [
            "Got it, thanks for sharing that.",
            "Thank you for that information.",
            "Noted, appreciate you letting me know.",
            "Thanks, that's helpful.",
        ]
        return acknowledgments[q_index % len(acknowledgments)]

    @staticmethod
    def experience_question(
        sub_index: int, config: Dict[str, Any]
    ) -> Tuple[str, List[str]]:
        """Generate experience-related questions."""
        role = config.get("role", "this field")
        if sub_index == 0:
            return (
                f"Now, tell me a bit about your background. "
                f"What is your current or most recent role?",
                [
                    "I'm currently employed",
                    "Between jobs",
                    "This would be my first role",
                ],
            )
        elif sub_index == 1:
            return (
                "And what's motivating your job search right now?",
                [
                    "Career growth",
                    "Better compensation",
                    "Relocation",
                    "Looking for new challenges",
                    "Company changes",
                ],
            )
        return "", []

    @staticmethod
    def skills_question(skill: str, idx: int, total: int) -> Tuple[str, List[str]]:
        """Ask about a specific skill."""
        prefix = ""
        if idx == 0:
            prefix = "Let's quickly check on some key skills for this role.\n\n"

        msg = f"{prefix}How would you rate your experience with **{skill}**?"
        chips = ["Expert", "Proficient", "Some experience", "Beginner", "No experience"]
        return msg, chips

    @staticmethod
    def contact_question(
        sub_index: int, collected: Dict[str, Any]
    ) -> Tuple[str, List[str]]:
        """Ask for contact information step by step."""
        if sub_index == 0:
            prefix = "We're almost done! Just need a few contact details.\n\n"
            return f"{prefix}What is your full name?", []
        elif sub_index == 1:
            name = collected.get("name") or ""
            greeting = f"Nice to meet you, {name}! " if name else ""
            return f"{greeting}What is the best email address to reach you?", []
        elif sub_index == 2:
            return "And a phone number where we can reach you?", [
                "I'd prefer not to share"
            ]
        elif sub_index == 3:
            return "When would you be available to start?", [
                "Immediately",
                "2 weeks notice",
                "1 month",
                "Flexible",
            ]
        elif sub_index == 4:
            return (
                "Last one -- what are your salary expectations for this role?",
                ["Open to discuss", "Based on market rate"],
            )
        return "", []

    @staticmethod
    def confirmation_summary(session: "ApplyFlowSession") -> Tuple[str, List[str]]:
        """Generate confirmation summary of collected data."""
        d = session.collected_data
        config = session.job_config

        lines = [
            "Here's a summary of your application. Please review:\n",
            f"**Position:** {config.get('role', 'N/A')} at {config.get('company', 'N/A')}",
            f"**Location:** {config.get('location', 'N/A')}",
        ]

        if d.get("name"):
            lines.append(f"**Name:** {d['name']}")
        if d.get("email"):
            lines.append(f"**Email:** {d['email']}")
        if d.get("phone"):
            lines.append(f"**Phone:** {d['phone']}")
        if d.get("years_experience") is not None:
            yrs = d["years_experience"]
            lines.append(f"**Experience:** {yrs:.0f} year{'s' if yrs != 1 else ''}")
        if d.get("current_role"):
            lines.append(f"**Current Role:** {d['current_role']}")
        if d.get("salary_expectation"):
            lines.append(f"**Salary Expectation:** ${d['salary_expectation']:,.0f}")
        if d.get("start_date"):
            lines.append(f"**Available:** {d['start_date']}")
        if d.get("skills_confirmed"):
            skills_str = ", ".join(d["skills_confirmed"])
            lines.append(f"**Key Skills:** {skills_str}")

        lines.append("\nDoes everything look correct?")

        return "\n".join(lines), [
            "Yes, submit my application",
            "I need to make a change",
        ]

    @staticmethod
    def complete_message(config: Dict[str, Any]) -> str:
        """Generate completion message."""
        company = config.get("company", "the hiring team")
        role = config.get("role", "the position")
        return (
            f"Your application for **{role}** at **{company}** has been submitted "
            f"successfully!\n\n"
            f"Here's what happens next:\n"
            f"- The hiring team will review your application\n"
            f"- You should hear back within 5-7 business days\n"
            f"- Keep an eye on your email for updates\n\n"
            f"Thank you for your time and interest. Good luck!"
        )


# ---------------------------------------------------------------------------
# LLM-Enhanced Response Generation (optional)
# ---------------------------------------------------------------------------


def _generate_llm_response(
    session: ApplyFlowSession,
    stage: str,
    user_message: str,
    context: str,
) -> Optional[str]:
    """Use llm_router for natural language response generation.

    Returns None if LLM is unavailable, allowing fallback to templates.
    """
    router = _get_llm_router()
    if not router:
        return None

    try:
        role = session.job_config.get("role", "the position")
        company = session.job_config.get("company", "the company")
        location = session.job_config.get("location") or ""
        collar = session.job_config.get("collar_type", "white_collar")

        system_prompt = (
            f"You are a friendly, professional AI recruiting assistant for {company}. "
            f"You are helping a candidate apply for the {role} position"
            f"{' in ' + location if location else ''}. "
            f"This is a {collar.replace('_', ' ')} role. "
            f"Be warm, conversational, and encouraging. Keep responses concise "
            f"(2-3 sentences max). Never be robotic or overly formal. "
            f"Current stage: {stage}. "
            f"Context: {context}"
        )

        # Build message history (last 4 turns max)
        messages = []
        recent_history = session.conversation_history[-8:]
        for msg in recent_history:
            messages.append(
                {
                    "role": (
                        msg["role"] if msg["role"] in ("user", "assistant") else "user"
                    ),
                    "content": msg["content"],
                }
            )

        if not messages or messages[-1]["content"] != user_message:
            messages.append({"role": "user", "content": user_message})

        result = router.call_llm(
            messages=messages,
            system_prompt=system_prompt,
            max_tokens=300,
            task_type="conversational",
            query_text=user_message,
        )

        if result and result.get("content"):
            _metrics.record_llm_call()
            return result["content"]

    except Exception as e:
        logger.warning("ApplyFlow: LLM response generation failed: %s", e)

    return None


# ---------------------------------------------------------------------------
# Conversation Stage Handlers
# ---------------------------------------------------------------------------


def _handle_greeting(session: ApplyFlowSession, message: str) -> Dict[str, Any]:
    """Handle the greeting stage."""
    msg_lower = message.lower().strip()

    # Check for disinterest
    if any(w in msg_lower for w in ["not right now", "no", "not interested", "later"]):
        response = (
            "No problem at all! The position will be open for a while, so feel free "
            "to come back anytime. Have a great day!"
        )
        return _build_response(session, response, [], advance=False)

    # Check for "tell me more"
    if any(
        w in msg_lower
        for w in ["tell me more", "more about", "details", "about the role"]
    ):
        config = session.job_config
        role = config.get("role", "this position")
        company = config.get("company", "the company")
        location = config.get("location") or ""
        salary = config.get("salary_range") or ""
        reqs = config.get("requirements") or []

        parts = [
            f"Here's what we know about the **{role}** position at **{company}**:\n"
        ]
        if location:
            parts.append(f"**Location:** {location}")
        if salary:
            parts.append(f"**Salary Range:** {salary}")
        if reqs:
            parts.append("\n**Requirements:**")
            for r in reqs[:5]:
                parts.append(f"- {r}")
        parts.append("\nWould you like to start the application?")

        response = "\n".join(parts)
        chips = ["Yes, let's go!", "Not right now"]
        return _build_response(session, response, chips, advance=False)

    # Affirmative / any other response -> advance to qualification
    session.state = STAGE_QUALIFICATION
    session._qualification_sub_question = 0
    _metrics.record_stage(STAGE_QUALIFICATION)

    response, chips = _Templates.qualification_question(session.job_config, 0)
    if not response:
        # No screening questions, skip to experience
        session.state = STAGE_EXPERIENCE
        session._experience_sub_question = 0
        _metrics.record_stage(STAGE_EXPERIENCE)
        response, chips = _Templates.experience_question(0, session.job_config)

    return _build_response(session, response, chips)


def _handle_qualification(session: ApplyFlowSession, message: str) -> Dict[str, Any]:
    """Handle screening qualification questions."""
    questions = session.job_config.get("screening_questions") or []
    q_idx = session._qualification_sub_question

    # Record the answer
    session.collected_data["screening_answers"].append(
        {
            "question": (
                questions[q_idx] if q_idx < len(questions) else f"Question {q_idx + 1}"
            ),
            "answer": message.strip(),
        }
    )

    # Extract structured data from qualification answers
    years = extract_years_experience(message)
    if years is not None and session.collected_data["years_experience"] is None:
        session.collected_data["years_experience"] = years
        # Adjust qualification score based on years
        if years >= 5:
            session.qualification_score += 15
        elif years >= 3:
            session.qualification_score += 10
        elif years >= 1:
            session.qualification_score += 5

    # Check for authorization
    msg_lower = message.lower()
    if any(w in msg_lower for w in ["authorized", "yes", "citizen", "permanent"]):
        if (
            "authorization"
            in (questions[q_idx] if q_idx < len(questions) else "").lower()
        ):
            session.collected_data["work_authorization"] = "Yes"
            session.qualification_score += 5

    # Check certifications
    if any(w in msg_lower for w in ["yes", "certified", "licensed", "have"]):
        if q_idx < len(questions) and any(
            w in questions[q_idx].lower() for w in ["certif", "licens"]
        ):
            session.qualification_score += 10

    # Extract education
    education_levels = {
        "phd": "PhD",
        "doctorate": "PhD",
        "master": "Master's",
        "mba": "MBA",
        "bachelor": "Bachelor's",
        "bs": "Bachelor's",
        "ba": "Bachelor's",
        "associate": "Associate's",
        "high school": "High School",
        "ged": "GED",
    }
    for key, val in education_levels.items():
        if key in msg_lower:
            session.collected_data["education"] = val
            session.qualification_score += 5
            break

    # Acknowledge and move to next question or next stage
    ack = _Templates.qualification_followup(message, q_idx, len(questions))
    session._qualification_sub_question += 1

    if session._qualification_sub_question < len(questions):
        next_q, chips = _Templates.qualification_question(
            session.job_config, session._qualification_sub_question
        )
        response = f"{ack} {next_q}"
        return _build_response(session, response, chips)
    else:
        # Advance to experience stage
        session.state = STAGE_EXPERIENCE
        session._experience_sub_question = 0
        _metrics.record_stage(STAGE_EXPERIENCE)
        next_q, chips = _Templates.experience_question(0, session.job_config)
        response = f"{ack}\n\n{next_q}"
        return _build_response(session, response, chips)


def _handle_experience(session: ApplyFlowSession, message: str) -> Dict[str, Any]:
    """Handle experience-related questions."""
    sub_idx = session._experience_sub_question

    if sub_idx == 0:
        # Current/recent role
        session.collected_data["current_role"] = message.strip()[:200]
        # Try to extract years if mentioned
        years = extract_years_experience(message)
        if years is not None and session.collected_data["years_experience"] is None:
            session.collected_data["years_experience"] = years

        session._experience_sub_question = 1
        response, chips = _Templates.experience_question(1, session.job_config)
        return _build_response(session, response, chips)

    elif sub_idx == 1:
        # Reason for looking
        session.collected_data["reason_for_looking"] = message.strip()[:200]
        # Positive signals boost qualification score
        positive_signals = [
            "growth",
            "challenge",
            "advance",
            "learn",
            "opportunity",
            "passionate",
            "excited",
            "motivated",
        ]
        if any(w in message.lower() for w in positive_signals):
            session.qualification_score += 5

        # Advance to skills
        session.state = STAGE_SKILLS
        session._skills_sub_question = 0
        _metrics.record_stage(STAGE_SKILLS)

        key_skills = session.job_config.get("key_skills") or []
        if key_skills:
            response, chips = _Templates.skills_question(
                key_skills[0], 0, len(key_skills)
            )
        else:
            # No skills to check, skip to contact
            session.state = STAGE_CONTACT
            session._contact_sub_field = 0
            _metrics.record_stage(STAGE_CONTACT)
            response, chips = _Templates.contact_question(0, session.collected_data)

        return _build_response(session, response, chips)

    return _build_response(session, "Thanks for sharing that.", [])


def _handle_skills(session: ApplyFlowSession, message: str) -> Dict[str, Any]:
    """Handle skills verification."""
    key_skills = session.job_config.get("key_skills") or []
    s_idx = session._skills_sub_question

    if s_idx < len(key_skills):
        skill = key_skills[s_idx]
        msg_lower = message.lower()

        # Score the skill level
        if any(w in msg_lower for w in ["expert", "advanced", "strong", "extensive"]):
            session.collected_data["skills_confirmed"].append(f"{skill} (Expert)")
            session.qualification_score += 8
        elif any(
            w in msg_lower for w in ["proficient", "good", "solid", "experienced"]
        ):
            session.collected_data["skills_confirmed"].append(f"{skill} (Proficient)")
            session.qualification_score += 5
        elif any(w in msg_lower for w in ["some", "basic", "familiar", "learning"]):
            session.collected_data["skills_confirmed"].append(f"{skill} (Basic)")
            session.qualification_score += 2
        elif any(w in msg_lower for w in ["beginner", "little", "new to"]):
            session.collected_data["skills_confirmed"].append(f"{skill} (Beginner)")
            session.qualification_score += 1
        else:
            # No experience or unrecognized
            session.collected_data["skills_confirmed"].append(
                f"{skill} (Self-assessed)"
            )
            session.qualification_score += 1

    session._skills_sub_question += 1

    if session._skills_sub_question < len(key_skills):
        next_skill = key_skills[session._skills_sub_question]
        response, chips = _Templates.skills_question(
            next_skill, session._skills_sub_question, len(key_skills)
        )
        return _build_response(session, response, chips)
    else:
        # Advance to contact
        session.state = STAGE_CONTACT
        session._contact_sub_field = 0
        _metrics.record_stage(STAGE_CONTACT)
        response, chips = _Templates.contact_question(0, session.collected_data)
        return _build_response(session, response, chips)


def _handle_contact(session: ApplyFlowSession, message: str) -> Dict[str, Any]:
    """Handle contact information collection step by step."""
    sub = session._contact_sub_field
    msg_stripped = message.strip()

    if sub == 0:
        # Name
        name = extract_name(msg_stripped)
        if not name:
            # If we can't parse it with patterns, just use what they typed
            # (as long as it's reasonable length and not a common non-name word)
            skip_words = {
                "expert",
                "proficient",
                "beginner",
                "basic",
                "advanced",
                "yes",
                "no",
                "sure",
                "okay",
                "skip",
                "none",
                "some",
                "good",
                "great",
                "thanks",
            }
            if (
                1 < len(msg_stripped) < 60
                and not msg_stripped.isdigit()
                and msg_stripped.lower() not in skip_words
            ):
                name = msg_stripped.title()
        if name:
            session.collected_data["name"] = name
            session.qualification_score += 3

    elif sub == 1:
        # Email
        email = extract_email(msg_stripped)
        if email:
            session.collected_data["email"] = email
            session.qualification_score += 5
        else:
            # Ask again
            return _build_response(
                session,
                "I didn't catch a valid email address. Could you please share your email? "
                "For example: name@example.com",
                [],
                advance=False,
            )

    elif sub == 2:
        # Phone
        if any(
            w in msg_stripped.lower()
            for w in ["prefer not", "rather not", "skip", "no"]
        ):
            session.collected_data["phone"] = "Not provided"
        else:
            phone = extract_phone(msg_stripped)
            if phone:
                session.collected_data["phone"] = phone
                session.qualification_score += 2
            else:
                session.collected_data["phone"] = "Not provided"

    elif sub == 3:
        # Start date
        start_date = extract_start_date(msg_stripped)
        if start_date:
            session.collected_data["start_date"] = start_date
        else:
            session.collected_data["start_date"] = msg_stripped[:100]
        session.qualification_score += 2

    elif sub == 4:
        # Salary expectation
        salary = extract_salary_expectation(msg_stripped)
        if salary:
            session.collected_data["salary_expectation"] = salary
        else:
            # Store as text if unparseable
            if any(
                w in msg_stripped.lower()
                for w in [
                    "open",
                    "discuss",
                    "negotiable",
                    "market",
                    "flexible",
                    "competitive",
                ]
            ):
                session.collected_data["salary_expectation"] = None  # Open to discuss
            else:
                session.collected_data["salary_expectation"] = None

        # Advance to confirmation
        session.state = STAGE_CONFIRMATION
        _metrics.record_stage(STAGE_CONFIRMATION)
        response, chips = _Templates.confirmation_summary(session)
        return _build_response(session, response, chips)

    # Move to next contact sub-field
    session._contact_sub_field += 1
    if session._contact_sub_field <= 4:
        response, chips = _Templates.contact_question(
            session._contact_sub_field, session.collected_data
        )
        return _build_response(session, response, chips)

    # Fallback: advance to confirmation
    session.state = STAGE_CONFIRMATION
    _metrics.record_stage(STAGE_CONFIRMATION)
    response, chips = _Templates.confirmation_summary(session)
    return _build_response(session, response, chips)


def _handle_confirmation(session: ApplyFlowSession, message: str) -> Dict[str, Any]:
    """Handle the confirmation stage."""
    msg_lower = message.lower().strip()

    if any(
        w in msg_lower
        for w in ["change", "edit", "update", "fix", "wrong", "incorrect"]
    ):
        # Let them fix -- go back to contact for now
        session.state = STAGE_CONTACT
        session._contact_sub_field = 0
        response = (
            "No problem! Let's go through the details again. "
            "You can update anything that needs changing.\n\n"
            "What is your full name?"
        )
        return _build_response(session, response, [])

    # Mark as complete
    session.state = STAGE_COMPLETE
    _metrics.record_stage(STAGE_COMPLETE)
    _metrics.record_session_complete()

    # Clamp qualification score to 0-100
    session.qualification_score = max(0.0, min(100.0, session.qualification_score))

    # Store completed application
    summary = session.to_summary()
    job_id = session.job_config.get("job_id", "unknown")
    MAX_COMPLETED_APPS = 5000
    with _completed_lock:
        if job_id not in _completed_applications:
            _completed_applications[job_id] = []
        _completed_applications[job_id].append(summary)
        # Evict oldest entries to prevent unbounded memory growth
        while (
            sum(len(v) for v in _completed_applications.values()) > MAX_COMPLETED_APPS
        ):
            oldest_key = next(iter(_completed_applications))
            _completed_applications.pop(oldest_key)

    response = _Templates.complete_message(session.job_config)
    return _build_response(session, response, [])


def _build_response(
    session: ApplyFlowSession,
    response: str,
    chips: List[str],
    advance: bool = True,
) -> Dict[str, Any]:
    """Build the standard response dict."""
    return {
        "response": response,
        "stage": session.state,
        "stage_label": STAGE_LABELS.get(session.state, session.state),
        "progress_pct": session.progress_pct,
        "collected_fields": session.collected_fields,
        "qualification_score": round(session.qualification_score, 1),
        "chips": chips,
        "is_complete": session.state == STAGE_COMPLETE,
    }


# ---------------------------------------------------------------------------
# Main Entry Point
# ---------------------------------------------------------------------------

_STAGE_HANDLERS = {
    STAGE_GREETING: _handle_greeting,
    STAGE_QUALIFICATION: _handle_qualification,
    STAGE_EXPERIENCE: _handle_experience,
    STAGE_SKILLS: _handle_skills,
    STAGE_CONTACT: _handle_contact,
    STAGE_CONFIRMATION: _handle_confirmation,
}


def handle_candidate_message(
    session_id: str,
    message: str,
    job_config: Dict[str, Any],
) -> Dict[str, Any]:
    """Main entry point for processing a candidate message.

    Routes to appropriate conversation stage, extracts structured data,
    generates contextual follow-up questions, and tracks qualification signals.

    Args:
        session_id: Unique session identifier (from client)
        message: Candidate's message text
        job_config: Job configuration dict (from create_job_config)

    Returns:
        {
            "response": str,           # AI response text (markdown supported)
            "stage": str,              # Current stage key
            "stage_label": str,        # Human-readable stage label
            "progress_pct": int,       # 0-100 progress percentage
            "collected_fields": list,  # List of collected field names
            "qualification_score": float,  # 0-100 qualification score
            "chips": list,             # Quick-reply suggestion chips
            "is_complete": bool,       # Whether application is complete
        }
    """
    # Input validation
    if not message or not message.strip():
        return {
            "response": "I didn't catch that. Could you please try again?",
            "stage": "unknown",
            "stage_label": "Unknown",
            "progress_pct": 0,
            "collected_fields": [],
            "qualification_score": 0,
            "chips": [],
            "is_complete": False,
            "error": "Empty message",
        }

    message = message.strip()[:MAX_MESSAGE_LENGTH]

    # Ensure job_config has required fields
    if not job_config.get("role"):
        job_config["role"] = "Open Position"
    if not job_config.get("company"):
        job_config["company"] = "Company"

    # Get or create session
    if not session_id:
        session_id = str(uuid.uuid4())

    session = _get_or_create_session(session_id, job_config)
    _metrics.record_message()

    # Record user message
    session.add_message("user", message)

    # Route to stage handler
    handler = _STAGE_HANDLERS.get(session.state)

    if session.state == STAGE_COMPLETE:
        # Already completed -- just acknowledge
        result = _build_response(
            session,
            "Your application has already been submitted. If you need to update anything, "
            "please contact the hiring team directly. Thank you!",
            [],
        )
    elif handler:
        try:
            result = handler(session, message)
        except Exception as e:
            logger.error(
                "ApplyFlow stage handler error (stage=%s): %s",
                session.state,
                e,
                exc_info=True,
            )
            result = _build_response(
                session,
                "I'm sorry, something went wrong. Could you please repeat that?",
                [],
            )
    else:
        logger.error(
            "ApplyFlow: unknown stage '%s' for session %s", session.state, session_id
        )
        result = _build_response(
            session,
            "I'm sorry, something went wrong. Let me restart our conversation.",
            ["Let's start over"],
        )
        session.state = STAGE_GREETING

    # Record AI response in conversation history
    session.add_message("assistant", result["response"])

    # Add session_id to response
    result["session_id"] = session_id

    _metrics.record_rule_based()

    return result


def handle_init_request(job_config: Dict[str, Any]) -> Dict[str, Any]:
    """Initialize a new ApplyFlow session and return the greeting.

    Called when the widget opens. Creates a session and returns the
    initial greeting message without requiring a user message.

    Args:
        job_config: Job configuration dict (from create_job_config or raw)

    Returns:
        Same format as handle_candidate_message response, plus session_id.
    """
    session_id = str(uuid.uuid4())

    # Ensure job_config is enriched
    if "job_id" not in job_config:
        job_config = create_job_config(
            role=job_config.get("role", "Open Position"),
            company=job_config.get("company", "Company"),
            location=job_config.get("location") or "",
            requirements=job_config.get("requirements"),
            screening_questions=job_config.get("screening_questions"),
            industry=job_config.get("industry") or "",
        )

    session = _get_or_create_session(session_id, job_config)
    _metrics.record_message()

    # Generate greeting
    response, chips = _Templates.greeting(session.job_config)
    session.add_message("assistant", response)

    return {
        "session_id": session_id,
        "response": response,
        "stage": STAGE_GREETING,
        "stage_label": STAGE_LABELS[STAGE_GREETING],
        "progress_pct": 0,
        "collected_fields": [],
        "qualification_score": session.qualification_score,
        "chips": chips,
        "is_complete": False,
        "job_config": {
            "job_id": job_config.get("job_id") or "",
            "role": job_config.get("role") or "",
            "company": job_config.get("company") or "",
            "location": job_config.get("location") or "",
            "salary_range": job_config.get("salary_range") or "",
            "collar_type": job_config.get("collar_type", "white_collar"),
        },
    }


# ---------------------------------------------------------------------------
# HTTP Handler (integrated into the main app server)
# ---------------------------------------------------------------------------


def handle_applyflow_request(request_data: dict) -> dict:
    """Handle an incoming ApplyFlow API request.

    Expected request format::

        {
            "action": "init" | "chat" | "summary" | "applications",
            "session_id": "optional-session-id",
            "message": "candidate message text",
            "job_config": {
                "role": "Software Engineer",
                "company": "Acme Corp",
                "location": "San Francisco, CA",
                "requirements": ["3+ years Python", "..."],
                "screening_questions": ["..."],
                "industry": "tech_engineering"
            }
        }

    Returns::

        {
            "response": "AI response text",
            "stage": "greeting",
            "stage_label": "Welcome",
            "progress_pct": 0,
            "collected_fields": [],
            "qualification_score": 50.0,
            "chips": ["Yes", "No"],
            "is_complete": false,
            "session_id": "uuid-string"
        }
    """
    action = request_data.get("action", "chat")
    session_id = request_data.get("session_id") or ""
    message = request_data.get("message") or ""
    job_config = request_data.get("job_config", {})

    try:
        if action == "init":
            return handle_init_request(job_config)

        elif action == "chat":
            return handle_candidate_message(session_id, message, job_config)

        elif action == "summary":
            if not session_id:
                return {"error": "session_id required for summary action"}
            summary = get_session_summary(session_id)
            if summary:
                return {"summary": summary}
            return {"error": "Session not found"}

        elif action == "applications":
            job_id = job_config.get("job_id", request_data.get("job_id") or "")
            if not job_id:
                return {"error": "job_id required for applications action"}
            apps = get_all_applications(job_id)
            return {"applications": apps, "count": len(apps)}

        elif action == "metrics":
            return {"metrics": get_metrics()}

        else:
            return {"error": f"Unknown action: {action}"}

    except Exception as e:
        logger.error("ApplyFlow request error: %s", e, exc_info=True)
        return {
            "error": "Internal error processing request",
            "response": "I'm sorry, something went wrong. Please try again.",
            "stage": "error",
            "stage_label": "Error",
            "progress_pct": 0,
            "collected_fields": [],
            "qualification_score": 0,
            "chips": [],
            "is_complete": False,
        }
