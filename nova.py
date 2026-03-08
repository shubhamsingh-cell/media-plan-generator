"""
Nova -- AI-powered recruitment marketing intelligence chatbot.

Provides conversational access to:
- Joveo's proprietary supply data (publishers, channels, global supply)
- 25 live API enrichment sources (salary, demand, location, ad platforms)
- Recruitment industry knowledge base (42 sources)
- Data synthesis engine (fused intelligence with confidence scores)
- Budget allocation engine ($ projections)

Works in two modes:
1. Rule-based (default): keyword-matching routes questions to data sources
2. Claude API (optional): uses Anthropic Claude for natural-language reasoning

Enable Claude mode by setting ANTHROPIC_API_KEY environment variable.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Unified data orchestrator (lazy import to avoid circular deps)
# ---------------------------------------------------------------------------
_orchestrator = None
_orchestrator_lock = threading.Lock()

def _get_orchestrator():
    global _orchestrator
    if _orchestrator is None:
        with _orchestrator_lock:
            if _orchestrator is None:
                try:
                    import data_orchestrator
                    _orchestrator = data_orchestrator
                    logger.info("Nova: data_orchestrator loaded")
                except Exception as e:
                    logger.warning("Nova: data_orchestrator import failed: %s", e)
                    _orchestrator = False  # sentinel: tried and failed
    return _orchestrator if _orchestrator is not False else None

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).resolve().parent / "data"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
JOVEO_PRIMARY_COLOR = "#0066CC"
MAX_HISTORY_TURNS = 6
MAX_MESSAGE_LENGTH = 4000
CLAUDE_MODEL_PRIMARY = "claude-haiku-4-5-20241022"    # $1/$5 per M tokens
CLAUDE_MODEL_FALLBACK = "claude-sonnet-4-20250514"    # $3/$15 per M tokens
CLAUDE_MODEL = CLAUDE_MODEL_PRIMARY

# Response cache settings
RESPONSE_CACHE_TTL = 7 * 86400  # 7 days
RESPONSE_CACHE_FILE = DATA_DIR / "nova_response_cache.json"
MAX_RESPONSE_CACHE_SIZE = 200
_response_cache: Dict[str, Any] = {}
_response_cache_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Nova Metrics Tracker (lightweight, thread-safe)
# ---------------------------------------------------------------------------
class _NovaMetrics:
    """Track Nova chatbot performance counters for the /api/nova/metrics endpoint."""

    def __init__(self):
        self._lock = threading.Lock()
        self._start_time = time.time()
        # Response mode counters
        self.learned_answer_hits: int = 0
        self.cache_hits: int = 0
        self.claude_api_calls: int = 0
        self.rule_based_calls: int = 0
        # Token tracking (from Claude API responses)
        self.total_input_tokens: int = 0
        self.total_output_tokens: int = 0
        self.total_cache_creation_tokens: int = 0
        self.total_cache_read_tokens: int = 0
        # Latency tracking (last 200 response times in ms)
        self._latencies: List[float] = []
        # Error counter
        self.api_errors: int = 0

    def record_learned_answer(self) -> None:
        with self._lock:
            self.learned_answer_hits += 1

    def record_cache_hit(self) -> None:
        with self._lock:
            self.cache_hits += 1

    def record_claude_call(self, input_tokens: int = 0, output_tokens: int = 0,
                           cache_creation: int = 0, cache_read: int = 0) -> None:
        with self._lock:
            self.claude_api_calls += 1
            self.total_input_tokens += input_tokens
            self.total_output_tokens += output_tokens
            self.total_cache_creation_tokens += cache_creation
            self.total_cache_read_tokens += cache_read

    def record_rule_based(self) -> None:
        with self._lock:
            self.rule_based_calls += 1

    def record_latency(self, ms: float) -> None:
        with self._lock:
            self._latencies.append(ms)
            if len(self._latencies) > 200:
                self._latencies = self._latencies[-200:]

    def record_api_error(self) -> None:
        with self._lock:
            self.api_errors += 1

    def snapshot(self) -> Dict[str, Any]:
        """Return a metrics snapshot for the /api/nova/metrics endpoint."""
        with self._lock:
            total = (self.learned_answer_hits + self.cache_hits +
                     self.claude_api_calls + self.rule_based_calls)
            lats = sorted(self._latencies) if self._latencies else []
            avg_lat = round(sum(lats) / len(lats), 1) if lats else 0
            p95_lat = round(lats[int(len(lats) * 0.95)] if lats else 0, 1)

            # Estimated cost (Haiku 4.5: $1/M input, $5/M output)
            input_cost = self.total_input_tokens / 1_000_000 * 1.0
            output_cost = self.total_output_tokens / 1_000_000 * 5.0
            # Cache read tokens are 90% cheaper
            cache_read_cost = self.total_cache_read_tokens / 1_000_000 * 0.1
            cache_creation_cost = self.total_cache_creation_tokens / 1_000_000 * 1.25
            total_cost = input_cost + output_cost + cache_read_cost + cache_creation_cost

            return {
                "total_requests": total,
                "response_modes": {
                    "learned_answers": self.learned_answer_hits,
                    "cache_hits": self.cache_hits,
                    "claude_api": self.claude_api_calls,
                    "rule_based": self.rule_based_calls,
                },
                "cache_hit_rate_pct": round(
                    (self.learned_answer_hits + self.cache_hits) / max(1, total) * 100, 1
                ),
                "tokens": {
                    "total_input": self.total_input_tokens,
                    "total_output": self.total_output_tokens,
                    "total_cache_read": self.total_cache_read_tokens,
                    "total_cache_creation": self.total_cache_creation_tokens,
                    "avg_input_per_call": round(
                        self.total_input_tokens / max(1, self.claude_api_calls)
                    ),
                    "avg_output_per_call": round(
                        self.total_output_tokens / max(1, self.claude_api_calls)
                    ),
                },
                "estimated_cost_usd": round(total_cost, 4),
                "latency_ms": {
                    "avg": avg_lat,
                    "p95": p95_lat,
                    "samples": len(lats),
                },
                "api_errors": self.api_errors,
                "model": CLAUDE_MODEL,
                "uptime_seconds": round(time.time() - self._start_time, 1),
            }

_nova_metrics = _NovaMetrics()

# Country name aliases for fuzzy matching
_COUNTRY_ALIASES: Dict[str, str] = {
    "us": "United States", "usa": "United States", "united states": "United States",
    "america": "United States", "uk": "United Kingdom", "britain": "United Kingdom",
    "united kingdom": "United Kingdom", "england": "United Kingdom",
    "germany": "Germany", "deutschland": "Germany",
    "france": "France", "india": "India", "australia": "Australia",
    "canada": "Canada", "japan": "Japan", "italy": "Italy",
    "netherlands": "Netherlands", "holland": "Netherlands",
    "spain": "Spain", "brazil": "Brazil", "mexico": "Mexico",
    "south africa": "South Africa", "ireland": "Ireland",
    "singapore": "Singapore", "uae": "United Arab Emirates",
    "saudi arabia": "Saudi Arabia", "poland": "Poland",
    "sweden": "Sweden", "norway": "Norway", "denmark": "Denmark",
    "switzerland": "Switzerland", "belgium": "Belgium", "austria": "Austria",
    "south korea": "South Korea", "korea": "South Korea",
    "new zealand": "New Zealand", "china": "China",
    "philippines": "Philippines", "indonesia": "Indonesia",
    "malaysia": "Malaysia", "thailand": "Thailand", "vietnam": "Vietnam",
    "argentina": "Argentina", "colombia": "Colombia", "chile": "Chile",
    "portugal": "Portugal", "czech republic": "Czech Republic",
    "romania": "Romania", "hungary": "Hungary", "turkey": "Turkey",
    "nigeria": "Nigeria", "kenya": "Kenya", "egypt": "Egypt",
    "israel": "Israel", "taiwan": "Taiwan",
}

# US state aliases -- map to United States so budget/publisher lookups work
_US_STATE_ALIASES: Dict[str, str] = {
    "alabama": "Alabama", "alaska": "Alaska", "arizona": "Arizona", "arkansas": "Arkansas",
    "california": "California", "colorado": "Colorado", "connecticut": "Connecticut",
    "delaware": "Delaware", "florida": "Florida", "georgia": "Georgia", "hawaii": "Hawaii",
    "idaho": "Idaho", "illinois": "Illinois", "indiana": "Indiana", "iowa": "Iowa",
    "kansas": "Kansas", "kentucky": "Kentucky", "louisiana": "Louisiana", "maine": "Maine",
    "maryland": "Maryland", "massachusetts": "Massachusetts", "michigan": "Michigan",
    "minnesota": "Minnesota", "mississippi": "Mississippi", "missouri": "Missouri",
    "montana": "Montana", "nebraska": "Nebraska", "nevada": "Nevada",
    "new hampshire": "New Hampshire", "new jersey": "New Jersey", "new mexico": "New Mexico",
    "new york": "New York", "north carolina": "North Carolina", "north dakota": "North Dakota",
    "ohio": "Ohio", "oklahoma": "Oklahoma", "oregon": "Oregon", "pennsylvania": "Pennsylvania",
    "rhode island": "Rhode Island", "south carolina": "South Carolina", "south dakota": "South Dakota",
    "tennessee": "Tennessee", "texas": "Texas", "utah": "Utah", "vermont": "Vermont",
    "virginia": "Virginia", "washington": "Washington", "west virginia": "West Virginia",
    "wisconsin": "Wisconsin", "wyoming": "Wyoming",
    # Common abbreviations
    "ca": "California", "tx": "Texas", "ny": "New York", "fl": "Florida",
    "il": "Illinois", "pa": "Pennsylvania", "oh": "Ohio", "nc": "North Carolina",
    "mi": "Michigan", "nj": "New Jersey", "va": "Virginia", "wa": "Washington",
    "ma": "Massachusetts", "az": "Arizona", "co": "Colorado", "mn": "Minnesota",
    "wi": "Wisconsin", "mo": "Missouri", "md": "Maryland", "in": "Indiana",
    "tn": "Tennessee", "ct": "Connecticut", "or": "Oregon", "la": "Louisiana",
    "sc": "South Carolina", "ky": "Kentucky", "ok": "Oklahoma", "ga": "Georgia",
}

# Role keywords for intent detection
_ROLE_KEYWORDS: Dict[str, List[str]] = {
    "nursing": ["nurse", "nursing", "rn", "lpn", "cna", "registered nurse"],
    "engineering": ["engineer", "engineering", "developer", "programmer", "coder", "devops", "sre"],
    "technology": ["tech", "software", "data scientist", "data engineer", "ml engineer", "ai engineer"],
    "healthcare": ["doctor", "physician", "therapist", "pharmacist", "medical", "clinical",
                    "dental", "veterinary", "paramedic", "emt"],
    "retail": ["retail", "cashier", "store associate", "merchandiser", "store manager"],
    "hospitality": ["chef", "cook", "waiter", "waitress", "bartender", "hotel", "restaurant"],
    "transportation": ["driver", "trucker", "cdl", "logistics", "warehouse", "forklift",
                       "blue collar", "blue-collar"],
    "finance": ["accountant", "analyst", "banker", "financial", "auditor", "actuary"],
    "executive": ["executive", "director", "vp", "vice president", "c-suite", "cfo", "cto", "ceo"],
    "hourly": ["hourly", "part-time", "part time", "entry-level", "entry level", "seasonal", "gig",
               "blue collar", "blue-collar"],
    "education": ["teacher", "professor", "instructor", "educator", "principal", "tutor"],
    "construction": ["construction", "carpenter", "plumber", "electrician", "mason", "welder"],
    "sales": ["sales", "account executive", "business development", "bdr", "sdr"],
    "marketing": ["marketing", "seo", "content", "social media manager", "brand"],
    "remote": ["remote", "work from home", "wfh", "distributed", "virtual"],
}

# Metric keywords for intent detection
_METRIC_KEYWORDS: Dict[str, List[str]] = {
    "cpc": ["cpc", "cost per click", "cost-per-click"],
    "cpa": ["cpa", "cost per application", "cost-per-application", "cost per apply"],
    "cph": ["cost per hire", "cost-per-hire", "cph", "hiring cost"],
    "salary": ["salary", "compensation", "pay", "wage", "earnings", "income"],
    "budget": ["budget", "spend", "allocation", "investment", "roi"],
    "time_to_fill": ["time to fill", "time-to-fill", "days to fill", "time to hire",
                      "time-to-hire", "ttf"],
    "apply_rate": ["apply rate", "application rate", "conversion rate", "cvr",
                    "conversion funnel", "recruitment funnel"],
    "benchmark": ["benchmark", "average", "industry average", "standard", "comparison",
                   "programmatic", "programmatic job advertising", "kpi", "measure success",
                   "metrics that matter"],
}

# ---------------------------------------------------------------------------
# Stop words for cache key normalization and keyword extraction
# ---------------------------------------------------------------------------
_CACHE_STOP_WORDS = frozenset(
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

# Preloaded learned answers (same as nova_slack.py)
_PRELOADED_ANSWERS = [
    {"question": "how many publishers does joveo have", "answer": "Joveo has **10,238+ Supply Partners** across **70+ countries**, including major job boards, niche boards, programmatic platforms, and social channels.", "keywords": ["publishers", "supply partners", "how many"], "confidence": 0.95},
    {"question": "what is joveo", "answer": "Joveo is a **recruitment marketing platform** that uses programmatic advertising technology to optimize job ad spend across 10,238+ Supply Partners globally. It helps employers reach the right candidates at the right time on the right channels.", "keywords": ["joveo", "what is"], "confidence": 0.95},
    {"question": "what countries does joveo operate in", "answer": "Joveo operates across **70+ countries** including the US, UK, Canada, Germany, France, India, Australia, Japan, UAE, Brazil, and many more across EMEA, APAC, and AMER regions.", "keywords": ["countries", "regions", "operate"], "confidence": 0.90},
    {"question": "what is programmatic job advertising", "answer": "Programmatic job advertising uses **data-driven automation** to buy, place, and optimize job ads in real-time across multiple channels. It maximizes ROI by dynamically adjusting bids, budgets, and targeting based on performance data. Average CPC ranges from $0.50-$2.50 depending on role and industry.", "keywords": ["programmatic", "advertising", "explain"], "confidence": 0.90},
    {"question": "what is cpc cpa cph", "answer": "**CPC** (Cost Per Click): You pay each time a candidate clicks your job ad ($0.50-$5.00 typical).\n**CPA** (Cost Per Application): You pay when a candidate completes an application ($5-$50 typical).\n**CPH** (Cost Per Hire): Total cost to fill a position ($1,500-$10,000+ depending on role).\nCPC is best for volume, CPA for quality, CPH for executive/niche roles.", "keywords": ["cpc", "cpa", "cph", "cost per"], "confidence": 0.95},
    {"question": "what pricing models does joveo support", "answer": "Joveo supports multiple pricing models: **CPC** (Cost Per Click), **CPA** (Cost Per Application), **TCPA** (Target CPA with auto-optimization), **Flat CPC**, **ORG** (Organic/free postings), and **PPP** (Pay Per Post). The optimal model depends on your hiring volume and role type.", "keywords": ["pricing", "models", "commission"], "confidence": 0.90},
    {"question": "top job boards in the us", "answer": "The top job boards in the US by traffic and performance:\n1. **Indeed** -- largest globally, CPC model\n2. **LinkedIn** -- best for white-collar/professional\n3. **ZipRecruiter** -- strong AI matching\n4. **Glassdoor** (merging into Indeed) -- employer brand focused\n5. **CareerBuilder** (under Bold Holdings post-bankruptcy)\n6. **Dice** -- tech-specific\n7. **Snagajob/JobGet** -- hourly/blue-collar\n8. **Handshake** -- early career/campus", "keywords": ["top", "job boards", "us", "united states", "best"], "confidence": 0.85},
    {"question": "what happened to monster and careerbuilder", "answer": "Monster and CareerBuilder filed for **Chapter 11 bankruptcy** in July 2025. They were acquired by **Bold Holdings for $28M**. Monster Europe has been shut down (DNS killed). CareerBuilder continues operating in the US under new ownership but with reduced scale.", "keywords": ["monster", "careerbuilder", "bankruptcy", "shut down"], "confidence": 0.95},
    {"question": "what is glassdoor status", "answer": "Glassdoor's operations are **merging into Indeed** (both owned by Recruit Holdings). The Glassdoor CEO stepped down in late 2025. The platform still operates but is increasingly integrated with Indeed's infrastructure.", "keywords": ["glassdoor", "status", "indeed"], "confidence": 0.90},
    {"question": "best boards for nursing hiring", "answer": "Top job boards for **nursing/healthcare** hiring:\n1. **Health eCareers** -- largest healthcare niche board\n2. **Nurse.com** -- RN-focused\n3. **NursingJobs.us** -- US nursing specific\n4. **Indeed** -- high-volume nursing traffic\n5. **Vivian Health** -- travel nursing marketplace\n6. **Incredible Health** -- RN matching platform\n7. **AlliedHealthJobs** -- allied health professionals\nRecommended channel mix: 30% niche boards, 22% programmatic, 15% global boards.", "keywords": ["nursing", "nurse", "healthcare", "boards"], "confidence": 0.90},
    {"question": "best boards for blue collar hiring", "answer": "Top channels for **blue-collar/hourly** hiring:\n1. **JobGet** (acquired Snagajob) -- 100M+ hourly workers\n2. **Indeed** -- highest blue-collar volume\n3. **Craigslist** -- local trades & service\n4. **Facebook Jobs** -- mobile-first hourly workers\n5. **Wonolo** -- on-demand warehouse/logistics\n6. **Instawork** -- gig/flexible workers\n7. **ShiftPixy** -- restaurant/hospitality shifts\nBudget tip: 40%+ should go to programmatic/mobile-first channels.", "keywords": ["blue collar", "hourly", "warehouse", "driver", "trades"], "confidence": 0.90},
    {"question": "joveo vs competitors", "answer": "Joveo's key differentiators vs competitors:\n- **PandoLogic** (now Veritone Hire): Joveo has broader global publisher network (70+ vs ~20 countries)\n- **Appcast** (owned by StepStone): Joveo offers more pricing models (CPC+CPA+TCPA)\n- **Recruitics**: Joveo has stronger programmatic optimization and niche board access\n- **Radancy**: Joveo focuses on performance marketing, Radancy on employer branding\nJoveo uniquely offers access to 10,238+ Supply Partners with real-time bid optimization.", "keywords": ["competitor", "vs", "pandologic", "appcast", "recruitics"], "confidence": 0.85},
]

_PARTIAL_MATCH_THRESHOLD = 0.35


def _normalize_cache_key(question: str) -> str:
    """Normalize a question into a canonical cache key.

    Lowercase, strip punctuation, expand contractions, remove stop words,
    sort remaining words alphabetically.
    """
    text = question.lower().strip()
    # Expand common contractions
    text = text.replace("what's", "what is").replace("how's", "how is")
    text = text.replace("it's", "it is").replace("who's", "who is")
    text = text.replace("where's", "where is").replace("there's", "there is")
    text = text.replace("that's", "that is").replace("doesn't", "does not")
    text = text.replace("don't", "do not").replace("can't", "cannot")
    text = text.replace("won't", "will not").replace("isn't", "is not")
    # Strip punctuation
    text = re.sub(r"[^\w\s]", "", text)
    # Tokenize and remove stop words
    words = text.split()
    filtered = [w for w in words if w not in _CACHE_STOP_WORDS]
    # Sort alphabetically for order-invariant key
    filtered.sort()
    return " ".join(filtered)


def _extract_keywords(text: str) -> set:
    """Tokenise *text* into a set of lower-case keywords, minus stop-words."""
    words = set(re.findall(r"\w+", text.lower()))
    return words - _CACHE_STOP_WORDS


def _check_learned_answers(question: str) -> Optional[Dict[str, Any]]:
    """Check preloaded + on-disk learned answers using Jaccard similarity."""
    # Merge preloaded with disk-based learned answers
    all_answers = list(_PRELOADED_ANSWERS)
    try:
        learned_file = DATA_DIR / "nova_learned_answers.json"
        if learned_file.exists():
            with open(learned_file, "r", encoding="utf-8") as f:
                disk_data = json.load(f)
                disk_answers = disk_data.get("answers", [])
                all_answers.extend(disk_answers)
    except Exception as exc:
        logger.warning("Could not load learned answers from disk: %s", exc)

    q_words = _extract_keywords(question)
    if not q_words:
        return None

    best_match: Optional[dict] = None
    best_score: float = 0.0

    for entry in all_answers:
        a_words = _extract_keywords(entry.get("question", ""))
        if not a_words:
            continue
        overlap = len(q_words & a_words)
        union = len(q_words | a_words)
        score = overlap / union if union else 0.0
        if score > best_score:
            best_score = score
            best_match = entry

    if best_match and best_score >= _PARTIAL_MATCH_THRESHOLD:
        logger.info("Learned answer match (score=%.2f): %s", best_score, best_match.get("question", ""))
        return {
            "response": best_match["answer"],
            "confidence": min(best_score * 1.2, 1.0),
            "sources": ["Joveo Knowledge Base (learned answers)"],
            "tools_used": [],
            "cached": True,
        }

    return None


def _get_response_cache(key: str) -> Optional[Dict[str, Any]]:
    """Check response cache: memory first, then disk. Returns cached result or None."""
    now = time.time()

    # 1) Memory check
    with _response_cache_lock:
        if key in _response_cache:
            entry = _response_cache[key]
            if entry.get("expires", 0) > now:
                logger.info("Nova cache HIT (memory)")
                return entry.get("data")
            else:
                del _response_cache[key]

    # 2) Disk check
    try:
        if RESPONSE_CACHE_FILE.exists():
            with open(RESPONSE_CACHE_FILE, "r", encoding="utf-8") as f:
                disk_cache = json.load(f)
            if key in disk_cache:
                entry = disk_cache[key]
                if entry.get("expires", 0) > now:
                    logger.info("Nova cache HIT (disk)")
                    data = entry.get("data")
                    # Promote to memory
                    with _response_cache_lock:
                        _response_cache[key] = entry
                    return data
    except Exception as exc:
        logger.warning("Disk cache read error: %s", exc)

    return None


def _set_response_cache(key: str, data: Dict[str, Any], ttl: int = RESPONSE_CACHE_TTL) -> None:
    """Write to memory cache (with LRU eviction) + disk (atomic write).

    Both memory and disk writes are protected by _response_cache_lock to
    prevent concurrent read-modify-write races on the disk cache file.
    """
    now = time.time()
    entry = {"data": data, "expires": now + ttl, "created": now}

    with _response_cache_lock:
        # 1) Memory write with LRU eviction
        _response_cache[key] = entry
        if len(_response_cache) > MAX_RESPONSE_CACHE_SIZE:
            # Evict oldest entry
            oldest_key = min(_response_cache, key=lambda k: _response_cache[k].get("created", 0))
            del _response_cache[oldest_key]

        # 2) Disk write (atomic via tmp + rename, evict expired on write)
        try:
            disk_cache: Dict[str, Any] = {}
            if RESPONSE_CACHE_FILE.exists():
                try:
                    with open(RESPONSE_CACHE_FILE, "r", encoding="utf-8") as f:
                        disk_cache = json.load(f)
                except (json.JSONDecodeError, IOError):
                    disk_cache = {}

            # Evict expired entries
            disk_cache = {k: v for k, v in disk_cache.items() if v.get("expires", 0) > now}
            disk_cache[key] = entry

            # Atomic write via temp file + rename
            fd, tmp_path = tempfile.mkstemp(dir=str(DATA_DIR), suffix=".tmp")
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as tmp_f:
                    json.dump(disk_cache, tmp_f, default=str)
                os.replace(tmp_path, str(RESPONSE_CACHE_FILE))
            except Exception:
                # Clean up temp file on failure
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except Exception as exc:
            logger.warning("Disk cache write error: %s", exc)


def _classify_query_complexity(user_message: str) -> int:
    """Classify query complexity to determine adaptive max_tokens.

    Returns:
        1024 for simple queries, 2048 for medium, 4096 for complex.
    """
    msg_lower = user_message.lower().strip()

    # Complex keywords -> 4096
    complex_patterns = [
        "budget", "plan", "strategy", "compare", "versus", " vs ",
        "allocat", "media plan", "hiring plan", "project",
        "how should i", "recommend", "analyze", "analysis",
    ]
    if any(p in msg_lower for p in complex_patterns):
        return 4096

    # Simple patterns -> 512 (short factual answers)
    simple_patterns = [
        r"^(hi|hello|hey|good morning|good afternoon)\b",
        r"^what is\s",
        r"^who is\s",
        r"^what does\s",
        r"^which is\s",
        r"^what('s| is) the (biggest|largest|best|top|most|cheapest)\s",
        r"^name\s",
        r"^list\s",
        r"^define\s",
        r"^explain\s",
        r"^how many\s",
        r"^(thanks|thank you|ok|okay|got it)",
    ]
    if any(re.search(p, msg_lower) for p in simple_patterns):
        return 512

    # Default medium
    return 2048


# Industry keywords
_INDUSTRY_KEYWORDS: Dict[str, List[str]] = {
    "healthcare": ["healthcare", "health care", "hospital", "medical", "pharma", "biotech"],
    "technology": ["technology", "tech", "software", "saas", "it", "information technology"],
    "finance": ["finance", "banking", "insurance", "financial", "fintech"],
    "retail": ["retail", "e-commerce", "ecommerce", "store", "shopping"],
    "hospitality": ["hospitality", "hotel", "restaurant", "tourism", "travel"],
    "manufacturing": ["manufacturing", "industrial", "production", "factory", "automotive"],
    "transportation": ["transportation", "logistics", "trucking", "shipping", "supply chain"],
    "construction": ["construction", "real estate", "building", "contractor"],
    "education": ["education", "school", "university", "academic", "k-12"],
    "energy": ["energy", "oil", "gas", "renewable", "solar", "utility"],
    "government": ["government", "federal", "military", "defense", "public sector"],
}


# ═══════════════════════════════════════════════════════════════════════════════
# JOVEO IQ ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class Nova:
    """Nova chatbot engine.

    Loads Joveo's proprietary data sources and provides tool-based access
    for answering recruitment marketing questions.
    """

    def __init__(self):
        self._data_cache: Dict[str, Any] = {}
        self._load_data_sources()

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def _load_data_sources(self):
        """Load all static data sources into memory."""
        data_files = {
            "global_supply": "global_supply.json",
            "channels_db": "channels_db.json",
            "joveo_publishers": "joveo_publishers.json",
            "knowledge_base": "recruitment_industry_knowledge.json",
            "linkedin_guidewire": "linkedin_guidewire_data.json",
        }
        for key, filename in data_files.items():
            filepath = DATA_DIR / filename
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    self._data_cache[key] = json.load(f)
                logger.info("Loaded %s from %s", key, filepath)
            except FileNotFoundError:
                logger.warning("Data file not found: %s", filepath)
                self._data_cache[key] = {}
            except json.JSONDecodeError as exc:
                logger.error("JSON parse error in %s: %s", filepath, exc)
                self._data_cache[key] = {}
            except Exception as exc:
                logger.error("Failed to load %s: %s", key, exc)
                self._data_cache[key] = {}

        # Load research intelligence files
        _research_files = {
            "platform_intelligence": "platform_intelligence_deep.json",
            "recruitment_benchmarks": "recruitment_benchmarks_deep.json",
            "recruitment_strategy": "recruitment_strategy_intelligence.json",
            "regional_hiring": "regional_hiring_intelligence.json",
            "supply_ecosystem": "supply_ecosystem_intelligence.json",
            "workforce_trends": "workforce_trends_intelligence.json",
            "white_papers": "industry_white_papers.json",
        }
        for _cache_key, _rf_name in _research_files.items():
            _rf_path = os.path.join(str(DATA_DIR), _rf_name)
            try:
                with open(_rf_path, "r", encoding="utf-8") as _rf:
                    self._data_cache[_cache_key] = json.load(_rf)
                    logger.info("Nova loaded %s", _cache_key)
            except Exception as _rf_err:
                self._data_cache[_cache_key] = {}
                logger.warning("Nova could not load %s: %s", _rf_name, _rf_err)

    # ------------------------------------------------------------------
    # System prompt (for Claude API mode)
    # ------------------------------------------------------------------

    def get_system_prompt(self) -> str:
        """Build the compressed system prompt for Claude."""
        publishers = self._data_cache.get("joveo_publishers", {})
        total_pubs = publishers.get("total_active_publishers", 0)
        pub_countries = list(publishers.get("by_country", {}).keys())

        supply = self._data_cache.get("global_supply", {})
        supply_countries = list(supply.get("country_job_boards", {}).keys())

        return f"""You are Nova, Joveo's recruitment marketing AI assistant. Joveo optimizes job ad spend across {total_pubs:,}+ publishers in {len(pub_countries)} countries via programmatic advertising.

## CRITICAL: ASK BEFORE ANSWERING

Before providing data, check if the user's question has enough specifics. If ANY of these are missing, ASK FIRST:
- **Salary questions** without a location/country -> Ask: "Which country or region? Salary ranges vary significantly by location."
- **Budget questions** without a budget amount -> Ask: "What's your total budget? I need a number to create an allocation plan."
- **Channel/board questions** without a country -> Ask: "Which country or region are you hiring in?"
- **Benchmark questions** without an industry -> Ask: "Which industry? Benchmarks differ dramatically by sector."

Do NOT default to US/USD data when the user hasn't specified a location. Always ask first.

When a country IS specified: use LOCAL CURRENCY (INR for India, GBP for UK, EUR for Germany, etc.), reference local boards, cite local norms.

## REASONING

- Identify intent, check for missing parameters (location, role, industry, budget), ask before calling tools if unclear.
- Call multiple tools when questions span domains. Cross-reference and flag discrepancies.
- Synthesize data into actionable insights; do not dump raw data.

## DATA SOURCES (via tools)

1. `query_publishers` -- {total_pubs:,}+ publishers, {len(pub_countries)} countries, search by name/category/country
2. `query_global_supply` -- {len(supply_countries)} countries: boards, DEI boards, monthly spend
3. `query_channels` -- channel recs by industry (traditional + non-traditional)
4. `query_knowledge_base` -- 42 sources: CPC/CPA/CPH benchmarks, trends, platform insights
5. `query_budget_projection` -- spend allocation with projected clicks/apps/hires
6. `query_salary_data` -- compensation ranges by role and location
7. `query_market_demand` -- applicant ratios, source-of-hire, hiring strength
8. `query_platform_deep` -- 91 platforms: CPC, CPA, apply rates, features, pros/cons (best for comparisons)
9. `query_recruitment_benchmarks` -- 22 industries: CPA/CPC/CPH with YoY trends (use over query_knowledge_base for industry data)
10. `query_employer_branding` -- ROI data, best practices, channel effectiveness
11. `query_regional_market` -- US regions + global markets: boards, salaries, regulations
12. `query_supply_ecosystem` -- programmatic mechanics, bidding, publisher waterfall
13. `query_workforce_trends` -- Gen-Z, remote work, DEI, salary expectations
14. `query_white_papers` -- 47 industry reports (use when citing research)
15. `query_linkedin_guidewire` -- LinkedIn hiring case study, peer benchmarks
16. `query_location_profile` -- location cost, workforce, supply data
17. `query_ad_platform` -- platform recs by role type with CPC benchmarks
18. `suggest_smart_defaults` -- auto-detect budget/channel defaults from partial info
19. `query_employer_brand` -- employer brand intel: Glassdoor ratings, hiring channels, strategies for 30+ major companies
20. `query_ad_benchmarks` -- CPC/CPM/CTR by platform (Google, Meta, LinkedIn, Indeed, Programmatic) per industry
21. `query_hiring_insights` -- computed insights: hiring difficulty index (0-1), salary competitiveness, days until peak hiring

## TOOL STRATEGY

Always call tools before answering data questions. Use `query_platform_deep` for platform comparisons, `query_recruitment_benchmarks` for industry-specific data, `query_white_papers` for evidence.
Use `query_hiring_insights` for strategic timing and difficulty questions.
Use `query_employer_brand` when discussing specific company hiring practices.
Use `query_ad_benchmarks` for platform cost comparisons.

## RESPONSE LENGTH — MATCH THE QUESTION

- **Simple factual questions** ("which is the biggest job board?", "what is CPC?"): Give a 1-3 sentence answer. Name the answer, add ONE key stat if relevant. Do NOT elaborate unless asked.
- **Moderate questions** ("compare Indeed vs LinkedIn"): 1-2 short paragraphs with key data points.
- **Complex/strategic questions** ("build me a media plan for healthcare hiring in Texas"): Full detailed response with sections, data, recommendations.
- NEVER over-explain simple questions. If the user wants more detail, they will ask follow-up questions.

## RESPONSE RULES

- Cite sources for every data point. Note convergence or flag discrepancies.
- Tool results include `data_confidence` (0.0-1.0) and `data_freshness`. Use these to calibrate your certainty:
  - confidence >= 0.8: state directly as reliable data
  - confidence 0.5-0.8: qualify as "based on available data"
  - confidence < 0.5: label as estimate or approximation
- High confidence (3+ sources): state directly. Medium (1-2): qualify. Low: label as estimate. No data: say so.
- NEVER invent statistics. NEVER present estimates as facts.
- Lead with the answer, use markdown, end complex answers with a recommendation.
- Proactively surface related insights. Flag if budget is too low/high.
"""

    # ------------------------------------------------------------------
    # Tool definitions (for Claude API mode)
    # ------------------------------------------------------------------

    def get_tool_definitions(self) -> list:
        """Define tools that Claude can call to access Joveo's data."""
        return [
            {
                "name": "query_global_supply",
                "description": "Country-specific job boards, DEI boards, women-focused boards, monthly spend. Use for boards in a specific country or DEI boards. Not for benchmarks (query_knowledge_base) or platform details (query_platform_deep).",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "country": {"type": "string", "description": "Country name. Omit for all countries."},
                        "board_type": {"type": "string", "enum": ["general", "dei", "women", "all"], "description": "Board type filter. Default: 'all'."},
                        "category": {"type": "string", "description": "Board category filter (e.g., 'Tech', 'Healthcare')."}
                    },
                    "required": []
                }
            },
            {
                "name": "query_channels",
                "description": "Channel recommendations by type: regional, global, niche industry, non-traditional. Use for channel strategy or niche boards. Not for publisher counts (query_publishers) or platform CPC (query_platform_deep).",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "industry": {"type": "string", "description": "Industry filter (e.g., 'healthcare_medical', 'tech_engineering')."},
                        "channel_type": {"type": "string", "enum": ["regional_local", "global_reach", "niche_by_industry", "non_traditional", "all"], "description": "Channel category. Default: 'all'."}
                    },
                    "required": []
                }
            },
            {
                "name": "query_publishers",
                "description": "Search Joveo's 10,238+ publisher network by country, category, or name. Use for publisher counts, name search, or filtered lists. Not for performance benchmarks (query_platform_deep).",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "country": {"type": "string", "description": "Country filter"},
                        "category": {"type": "string", "description": "Category (e.g., 'DEI', 'Health', 'Tech', 'Programmatic')"},
                        "search_term": {"type": "string", "description": "Name search (case-insensitive substring)"}
                    },
                    "required": []
                }
            },
            {
                "name": "query_knowledge_base",
                "description": "Core recruitment KB: CPC/CPA/CPH benchmarks, market trends, platform insights from 42 sources. Use for general benchmarks and trends. Not for industry-specific data (query_recruitment_benchmarks) or platform comparisons (query_platform_deep).",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "topic": {"type": "string", "enum": ["benchmarks", "trends", "platforms", "regional", "industry_specific", "all"], "description": "Topic area."},
                        "metric": {"type": "string", "description": "Metric: 'cpc', 'cpa', 'cost_per_hire', 'apply_rate', 'time_to_fill', 'source_of_hire', 'conversion_rate'."},
                        "industry": {"type": "string", "description": "Industry filter."},
                        "platform": {"type": "string", "description": "Platform name filter."}
                    },
                    "required": []
                }
            },
            {
                "name": "query_salary_data",
                "description": "Salary ranges by role and location with tier classification and CPH benchmarks. Use for compensation, pay, and wage questions.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "role": {"type": "string", "description": "Job title (e.g., 'Registered Nurse', 'Software Engineer')"},
                        "location": {"type": "string", "description": "Location (city, state, or country)"}
                    },
                    "required": ["role"]
                }
            },
            {
                "name": "query_market_demand",
                "description": "Job market demand: applicant ratios, source-of-hire, hiring strength, labor trends. Use for talent supply/demand and competition questions.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "role": {"type": "string", "description": "Job title"},
                        "location": {"type": "string", "description": "Location"},
                        "industry": {"type": "string", "description": "Industry"}
                    },
                    "required": []
                }
            },
            {
                "name": "query_budget_projection",
                "description": "Budget allocation across 6 channels with projected clicks, applications, hires. Use when user provides a dollar budget or asks about ROI/spend allocation.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "budget": {"type": "number", "description": "Total budget in USD"},
                        "roles": {"type": "array", "items": {"type": "string"}, "description": "Role titles"},
                        "locations": {"type": "array", "items": {"type": "string"}, "description": "Hiring locations"},
                        "industry": {"type": "string", "description": "Industry"}
                    },
                    "required": ["budget"]
                }
            },
            {
                "name": "query_location_profile",
                "description": "Location intelligence: monthly spend, key metros, publisher availability. Use for location-specific hiring market context.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "city": {"type": "string", "description": "City"},
                        "state": {"type": "string", "description": "State/province"},
                        "country": {"type": "string", "description": "Country"}
                    },
                    "required": []
                }
            },
            {
                "name": "query_ad_platform",
                "description": "Platform recommendations by role type with CPC benchmarks. Use for 'which platform for [role type]' questions. Not for detailed comparisons (query_platform_deep).",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "role_type": {"type": "string", "enum": ["executive", "professional", "hourly", "clinical", "trades"], "description": "Role type"},
                        "platforms": {"type": "array", "items": {"type": "string"}, "description": "Specific platforms"}
                    },
                    "required": []
                }
            },
            {
                "name": "query_linkedin_guidewire",
                "description": "LinkedIn Hiring Value Review for Guidewire Software: hiring performance, influenced hires, skill density, recruiter efficiency, peer benchmarks. Use for Guidewire, LinkedIn ROI, or tech company benchmarks.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "section": {"type": "string", "enum": ["executive_summary", "hiring_performance", "hire_efficiency", "all"], "description": "Section to query"},
                        "metric": {"type": "string", "description": "Specific metric (e.g., 'influenced_hires', 'skill_density')"}
                    },
                    "required": []
                }
            },
            {
                "name": "query_platform_deep",
                "description": "Detailed 91-platform database: CPC, CPA, apply rates, visitors, mobile %, demographics, DEI/AI features, pros/cons. BEST tool for platform comparisons -- pass platform and compare_with.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "platform": {"type": "string", "description": "Platform name (e.g., 'indeed', 'linkedin')"},
                        "compare_with": {"type": "string", "description": "Second platform to compare"}
                    },
                    "required": ["platform"]
                }
            },
            {
                "name": "query_recruitment_benchmarks",
                "description": "Industry-specific benchmarks (22 industries): CPA, CPC, CPH, apply rates, time-to-fill, funnel data with YoY trends. More detailed than query_knowledge_base for industry questions.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "industry": {"type": "string", "description": "Industry (e.g., 'healthcare', 'technology', 'finance')"},
                        "metric": {"type": "string", "description": "Metric: 'cpa', 'cpc', 'cph', 'apply_rate', 'time_to_fill', or 'all'"}
                    },
                    "required": ["industry"]
                }
            },
            {
                "name": "query_employer_branding",
                "description": "Employer branding intel (34 sources): ROI data, best practices, channel effectiveness. Use for EVP, Glassdoor impact, or brand strategy questions.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "aspect": {"type": "string", "description": "'roi', 'best_practices', 'channel_effectiveness', or 'all'"}
                    },
                    "required": []
                }
            },
            {
                "name": "query_regional_market",
                "description": "US regional + global market hiring intel (16 sources): top boards, industries, salaries, regulations. Regions: us_northeast, us_southeast, us_midwest, us_west, us_south.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "region": {"type": "string", "description": "Region key (e.g., 'us_northeast', 'us_south')"},
                        "market": {"type": "string", "description": "Market key (e.g., 'boston_ma', 'new_york_ny')"}
                    },
                    "required": ["region"]
                }
            },
            {
                "name": "query_supply_ecosystem",
                "description": "Programmatic advertising mechanics (24 sources): bidding models, publisher waterfall, quality signals, budget pacing. Use for 'how does programmatic work' questions.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "topic": {"type": "string", "description": "'how_it_works', 'bidding_models', 'publisher_waterfall', 'quality_signals', 'budget_pacing', or 'all'"}
                    },
                    "required": []
                }
            },
            {
                "name": "query_workforce_trends",
                "description": "Workforce trends (44 sources): Gen-Z behavior, platform preferences, remote work, DEI, salary expectations. Use for generational and demographic questions.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "topic": {"type": "string", "description": "'gen_z', 'remote_work', 'dei', 'salary_expectations', 'platform_preferences', or 'all'"}
                    },
                    "required": []
                }
            },
            {
                "name": "query_white_papers",
                "description": "47 industry reports from Appcast, Radancy, Recruitics, PandoLogic, Joveo. Use when citing research or backing claims with evidence.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "search_term": {"type": "string", "description": "Search term (e.g., 'CPA trends', 'healthcare hiring')"},
                        "report_key": {"type": "string", "description": "Specific report key if known"}
                    },
                    "required": []
                }
            },
            {
                "name": "suggest_smart_defaults",
                "description": "Auto-detect budget range, channel split, CPA/CPH from partial info (roles, locations). Use when user asks 'how much should I budget' or provides roles without budget.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "roles": {"type": "array", "items": {"type": "string"}, "description": "Role titles"},
                        "hire_count": {"type": "integer", "description": "Number of hires. Default: 10"},
                        "locations": {"type": "array", "items": {"type": "string"}, "description": "Hiring locations"},
                        "industry": {"type": "string", "description": "Industry"},
                        "urgency": {"type": "string", "enum": ["standard", "urgent", "critical"], "description": "Urgency level"}
                    },
                    "required": ["roles"]
                }
            },
            {
                "name": "query_employer_brand",
                "description": "Get employer brand intelligence for a specific company: Glassdoor rating, hiring channels, recruitment strategies, talent focus, company size. Covers 30+ major employers (HCA, Kaiser, Google, Amazon, Microsoft, etc.). Use when user asks about a company's hiring approach or employer brand.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "company": {"type": "string", "description": "Company name (e.g., 'Kaiser Permanente', 'Google', 'Amazon')"}
                    },
                    "required": ["company"]
                }
            },
            {
                "name": "query_ad_benchmarks",
                "description": "Get CPC/CPM/CTR benchmarks by ad platform (Google Ads, Meta/Facebook, LinkedIn, Indeed, Programmatic) for a specific industry. Use when user asks about advertising costs, platform pricing, or campaign benchmarks.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "industry": {"type": "string", "description": "Industry (e.g., 'healthcare', 'tech', 'finance', 'retail')"}
                    },
                    "required": ["industry"]
                }
            },
            {
                "name": "query_hiring_insights",
                "description": "Get computed hiring insights: hiring difficulty index (0-1), salary competitiveness score, days until next peak hiring window, current job posting volume. Best called AFTER using salary/market/location tools to get richest data. Use when user asks 'how hard is it to hire...', 'when should I start hiring...', or needs strategic hiring timing advice.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "role": {"type": "string", "description": "Job role"},
                        "location": {"type": "string", "description": "Hiring location"},
                        "industry": {"type": "string", "description": "Industry"}
                    },
                    "required": []
                }
            },
        ]

    # ------------------------------------------------------------------
    # Tool execution
    # ------------------------------------------------------------------

    def execute_tool(self, tool_name: str, tool_input: dict) -> str:
        """Execute a tool call and return the result as a JSON string."""
        handlers = {
            "query_global_supply": self._query_global_supply,
            "query_channels": self._query_channels,
            "query_publishers": self._query_publishers,
            "query_knowledge_base": self._query_knowledge_base,
            "query_salary_data": self._query_salary_data,
            "query_market_demand": self._query_market_demand,
            "query_budget_projection": self._query_budget_projection,
            "query_location_profile": self._query_location_profile,
            "query_ad_platform": self._query_ad_platform,
            "query_linkedin_guidewire": self._query_linkedin_guidewire,
            "query_platform_deep": self._query_platform_deep,
            "query_recruitment_benchmarks": self._query_recruitment_benchmarks,
            "query_employer_branding": self._query_employer_branding,
            "query_regional_market": self._query_regional_market,
            "query_supply_ecosystem": self._query_supply_ecosystem,
            "query_workforce_trends": self._query_workforce_trends,
            "query_white_papers": self._query_white_papers,
            "suggest_smart_defaults": self._suggest_smart_defaults,
            "query_employer_brand": self._query_employer_brand,
            "query_ad_benchmarks": self._query_ad_benchmarks,
            "query_hiring_insights": self._query_hiring_insights,
        }
        handler = handlers.get(tool_name)
        if not handler:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})
        try:
            result = handler(tool_input)
            return json.dumps(result, default=str)
        except Exception as e:
            logger.error("Tool %s failed: %s", tool_name, e, exc_info=True)
            return json.dumps({"error": f"Tool '{tool_name}' encountered an internal error"})

    # ------------------------------------------------------------------
    # Tool handlers
    # ------------------------------------------------------------------

    def _query_global_supply(self, params: dict) -> dict:
        """Query global supply data: country boards, DEI boards, spend data."""
        supply = self._data_cache.get("global_supply", {})
        country = params.get("country", "").strip()
        board_type = params.get("board_type", "all")
        category_filter = params.get("category", "").lower().strip()

        result: Dict[str, Any] = {"source": "Joveo Global Supply Intelligence"}

        # Resolve country alias
        country_resolved = _resolve_country(country)

        if board_type in ("general", "all"):
            country_boards = supply.get("country_job_boards", {})
            if country_resolved and country_resolved in country_boards:
                entry = country_boards[country_resolved]
                boards = entry.get("boards", [])
                if category_filter:
                    boards = [b for b in boards if category_filter in b.get("category", "").lower()]
                result["country_boards"] = {
                    "country": country_resolved,
                    "boards": boards,
                    "monthly_spend": entry.get("monthly_spend", "N/A"),
                    "key_metros": entry.get("key_metros", []),
                }
            elif not country:
                # Return summary of all countries
                result["available_countries"] = list(country_boards.keys())
                result["total_countries"] = len(country_boards)
            else:
                result["country_boards"] = {"message": f"No data for country: {country}"}

        if board_type in ("dei", "all"):
            dei_boards = supply.get("dei_boards_by_country", {})
            if country_resolved and country_resolved in dei_boards:
                result["dei_boards"] = {
                    "country": country_resolved,
                    "boards": dei_boards[country_resolved],
                }
            elif not country:
                # Return global DEI boards
                result["dei_boards"] = {
                    "global": dei_boards.get("Global", []),
                    "available_countries": list(dei_boards.keys()),
                }
            else:
                # Check global list
                result["dei_boards"] = {
                    "global": dei_boards.get("Global", []),
                    "note": f"No country-specific DEI boards for {country}; showing global options",
                }

        if board_type in ("women", "all"):
            women_boards = supply.get("women_boards_by_country", {})
            if country_resolved and country_resolved in women_boards:
                result["women_boards"] = {
                    "country": country_resolved,
                    "boards": women_boards[country_resolved],
                }
            elif not country:
                result["women_boards"] = {
                    "global": women_boards.get("Global", []),
                    "available_countries": list(women_boards.keys()),
                }

        return result

    def _query_channels(self, params: dict) -> dict:
        """Query channel database: traditional and non-traditional channels."""
        channels = self._data_cache.get("channels_db", {})
        industry = params.get("industry", "").strip().lower()
        channel_type = params.get("channel_type", "all")

        result: Dict[str, Any] = {"source": "Joveo Channel Database"}

        traditional = channels.get("traditional_channels", {})
        non_traditional = channels.get("non_traditional_channels", {})

        if channel_type in ("regional_local", "all"):
            result["regional_local"] = traditional.get("regional_local", [])

        if channel_type in ("global_reach", "all"):
            result["global_reach"] = traditional.get("global_reach", [])

        if channel_type in ("niche_by_industry", "all"):
            niche = traditional.get("niche_by_industry", {})
            if industry:
                # Find matching industry key
                matched_key = _match_industry_key(industry, list(niche.keys()))
                if matched_key:
                    result["niche_industry_channels"] = {
                        "industry": matched_key,
                        "channels": niche[matched_key],
                    }
                else:
                    result["niche_industry_channels"] = {
                        "message": f"No niche channels for industry: {industry}",
                        "available_industries": list(niche.keys()),
                    }
            else:
                result["niche_industries_available"] = list(niche.keys())

        if channel_type in ("non_traditional", "all"):
            result["non_traditional"] = non_traditional

        return result

    def _query_publishers(self, params: dict) -> dict:
        """Query Joveo publisher network by country, category, or search term."""
        publishers = self._data_cache.get("joveo_publishers", {})
        country = params.get("country", "").strip()
        category = params.get("category", "").strip()
        search_term = params.get("search_term", "").strip().lower()

        result: Dict[str, Any] = {
            "source": "Joveo Publisher Network",
            "total_active_publishers": publishers.get("total_active_publishers", 0),
        }

        country_resolved = _resolve_country(country)
        by_category = publishers.get("by_category", {})
        by_country = publishers.get("by_country", {})

        if search_term:
            # Search across all publishers
            matches = []
            for cat, pubs in by_category.items():
                for pub in pubs:
                    if search_term in pub.lower():
                        matches.append({"name": pub, "category": cat})
            result["search_results"] = matches
            result["search_term"] = search_term
            result["match_count"] = len(matches)

        elif category:
            # Filter by category
            cat_key = _match_category_key(category, list(by_category.keys()))
            if cat_key:
                result["category"] = cat_key
                result["publishers"] = by_category[cat_key]
                result["count"] = len(by_category[cat_key])
            else:
                result["message"] = f"No category match for: {category}"
                result["available_categories"] = list(by_category.keys())

        elif country_resolved:
            # Filter by country
            if country_resolved in by_country:
                pubs = by_country[country_resolved]
                result["country"] = country_resolved
                result["publishers"] = pubs
                result["count"] = len(pubs)
            else:
                result["message"] = f"No publishers specifically listed for: {country_resolved}"
                result["available_countries"] = list(by_country.keys())[:20]

        else:
            # Return overview
            result["categories"] = {k: len(v) for k, v in by_category.items()}
            result["countries_covered"] = len(by_country)

        return result

    def _query_knowledge_base(self, params: dict) -> dict:
        """Query recruitment industry knowledge base."""
        kb = self._data_cache.get("knowledge_base", {})
        topic = params.get("topic", "all")
        metric = params.get("metric", "").strip().lower()
        industry = params.get("industry", "").strip().lower()
        platform = params.get("platform", "").strip().lower()

        result: Dict[str, Any] = {"source": "Recruitment Industry Knowledge Base"}

        benchmarks = kb.get("benchmarks", {})
        trends = kb.get("market_trends", {})
        industry_benchmarks = kb.get("industry_specific_benchmarks", {})

        if topic in ("benchmarks", "all"):
            if metric:
                metric_map = {
                    "cpc": "cost_per_click",
                    "cpa": "cost_per_application",
                    "cph": "cost_per_hire",
                    "cost_per_hire": "cost_per_hire",
                    "apply_rate": "apply_rates",
                    "time_to_fill": "time_to_fill",
                    "source_of_hire": "source_of_hire",
                    "conversion_rate": "conversion_rates",
                }
                bm_key = metric_map.get(metric, metric)
                if bm_key in benchmarks:
                    result["benchmarks"] = {bm_key: benchmarks[bm_key]}
                else:
                    # Try partial match
                    matched = {k: v for k, v in benchmarks.items() if metric in k.lower()}
                    if matched:
                        result["benchmarks"] = matched
                    else:
                        result["benchmarks"] = {"message": f"No benchmark data for metric: {metric}",
                                                "available_metrics": list(benchmarks.keys())}
            elif platform:
                # Extract platform-specific CPC data
                cpc_data = benchmarks.get("cost_per_click", {}).get("by_platform", {})
                if platform in cpc_data:
                    result["platform_benchmarks"] = {platform: cpc_data[platform]}
                else:
                    matched = {k: v for k, v in cpc_data.items() if platform in k.lower()}
                    result["platform_benchmarks"] = matched if matched else {
                        "message": f"No platform data for: {platform}",
                        "available_platforms": list(cpc_data.keys()),
                    }
            else:
                result["benchmark_categories"] = list(benchmarks.keys())

        if topic in ("trends", "all"):
            result["trend_topics"] = list(trends.keys())
            # Return summary of top trends
            trend_summaries = {}
            for tk, tv in trends.items():
                if isinstance(tv, dict):
                    trend_summaries[tk] = {
                        "title": tv.get("title", tk),
                        "description": tv.get("description", ""),
                    }
            result["trend_summaries"] = trend_summaries

        if topic in ("industry_specific", "all") or industry:
            if industry:
                ind_key = _match_industry_key(industry, list(industry_benchmarks.keys()))
                if ind_key:
                    result["industry_benchmarks"] = {ind_key: industry_benchmarks[ind_key]}
                else:
                    result["industry_benchmarks"] = {
                        "message": f"No industry-specific data for: {industry}",
                        "available_industries": list(industry_benchmarks.keys()),
                    }
            else:
                result["industries_available"] = list(industry_benchmarks.keys())

        if topic == "platforms" or platform:
            platform_data = kb.get("platform_insights", {})
            if platform:
                matched = {k: v for k, v in platform_data.items() if platform in k.lower()}
                result["platform_insights"] = matched if matched else {
                    "available_platforms": list(platform_data.keys()),
                }
            else:
                result["platform_insights_available"] = list(platform_data.keys())

        if topic == "regional":
            result["regional_insights"] = kb.get("regional_insights", {})

        return result

    def _query_salary_data(self, params: dict) -> dict:
        """Get salary intelligence for roles and locations.

        Uses DataOrchestrator to cascade:
            research.py (COLI-adjusted) -> BLS API (cached 24h) -> KB fallback.
        """
        role = params.get("role", "").strip()
        location = params.get("location", "").strip()

        orch = _get_orchestrator()
        if orch:
            try:
                enriched = orch.enrich_salary(role, location)
                result: Dict[str, Any] = {
                    "source": f"Joveo Salary Intelligence ({enriched.get('source', 'multi-source')})",
                    "role": role,
                    "location": location or "National",
                    "salary_range_estimate": enriched.get("salary_range", "N/A"),
                    "role_tier": enriched.get("role_tier", "Professional"),
                }
                if enriched.get("coli"):
                    result["cost_of_living_index"] = enriched["coli"]
                if enriched.get("metro_name"):
                    result["metro_name"] = enriched["metro_name"]
                if enriched.get("country"):
                    result["country"] = enriched["country"]
                if enriched.get("currency") and enriched["currency"] != "USD":
                    result["currency"] = enriched["currency"]
                if enriched.get("bls_percentiles"):
                    result["bls_salary_percentiles"] = enriched["bls_percentiles"]
                # v2 metadata: confidence and freshness for Claude reasoning
                if enriched.get("confidence") is not None:
                    result["data_confidence"] = enriched["confidence"]
                if enriched.get("data_freshness"):
                    result["data_freshness"] = enriched["data_freshness"]
                if enriched.get("sources_used"):
                    result["sources_used"] = enriched["sources_used"]
                return result
            except Exception as e:
                logger.warning("Orchestrator enrich_salary failed, using KB fallback: %s", e)

        # --- KB-only fallback (original logic) ---
        result = {
            "source": "Joveo Salary Intelligence (KB)",
            "role": role,
            "location": location or "National",
        }
        role_lower = role.lower()
        tier = "Professional"
        if any(kw in role_lower for kw in ["nurse", "rn", "lpn", "therapist", "physician", "clinical"]):
            tier = "Clinical"
        elif any(kw in role_lower for kw in ["executive", "director", "vp", "chief", "president"]):
            tier = "Executive"
        elif any(kw in role_lower for kw in ["driver", "warehouse", "construction", "electrician", "welder"]):
            tier = "Trades"
        elif any(kw in role_lower for kw in ["cashier", "retail", "hourly", "part-time", "entry"]):
            tier = "Hourly"
        elif not any(kw in role_lower for kw in ["engineer", "developer", "data scientist", "software"]):
            tier = "General"

        _US_RANGES = {
            "Professional": ("$75,000", "$200,000"), "Clinical": ("$45,000", "$120,000"),
            "Executive": ("$150,000", "$500,000+"), "Trades": ("$35,000", "$80,000"),
            "Hourly": ("$25,000", "$45,000"), "General": ("$50,000", "$120,000"),
        }
        low, high = _US_RANGES.get(tier, _US_RANGES["General"])
        result["salary_range_estimate"] = f"{low} - {high}"
        result["role_tier"] = tier
        return result

    def _query_market_demand(self, params: dict) -> dict:
        """Get job market demand signals for roles and locations.

        Uses DataOrchestrator to cascade:
            research.py (labor market intel) -> Adzuna/Jooble API -> KB fallback.
        """
        role = params.get("role", "").strip()
        location = params.get("location", "").strip()
        industry = params.get("industry", "").strip()

        kb = self._data_cache.get("knowledge_base", {})
        benchmarks = kb.get("benchmarks", {})
        trends = kb.get("market_trends", {})
        industry_benchmarks = kb.get("industry_specific_benchmarks", {})

        result: Dict[str, Any] = {
            "source": "Joveo Market Demand Intelligence",
            "role": role or "General",
            "location": location or "National",
        }

        # KB data (always include)
        apo = benchmarks.get("applicants_per_opening", {})
        result["applicants_per_opening"] = apo

        soh = benchmarks.get("source_of_hire", {})
        result["source_of_hire"] = {
            "job_boards_usage": soh.get("job_boards", {}).get("employer_usage", "68.6%"),
            "referrals_usage": soh.get("employee_referrals", {}).get("employer_usage", "82%"),
            "career_sites_usage": soh.get("career_sites", {}).get("employer_usage", "49.5%"),
            "linkedin_usage": soh.get("linkedin_professional_networks", {}).get("employer_usage", "46.1%"),
        }

        if industry:
            ind_key = _match_industry_key(industry, list(industry_benchmarks.keys()))
            if ind_key:
                ind_data = industry_benchmarks[ind_key]
                result["industry_demand"] = {
                    "industry": ind_key,
                    "hiring_strength": ind_data.get("hiring_strength", "N/A"),
                    "recruitment_difficulty": ind_data.get("recruitment_difficulty", "N/A"),
                }

        labor = trends.get("labor_market_shifts", {})
        if labor:
            result["labor_market"] = {
                "title": labor.get("title", ""),
                "description": labor.get("description", ""),
            }

        # Orchestrator enrichment (research.py + live API data)
        orch = _get_orchestrator()
        if orch:
            try:
                enriched = orch.enrich_market_demand(role, location, industry)
                if enriched.get("labour_market"):
                    result["research_labour_market"] = enriched["labour_market"]
                    result["source"] = f"Joveo Market Demand Intelligence (KB + {enriched.get('source', 'Research')})"
                if enriched.get("api_job_market"):
                    result["live_job_market"] = enriched["api_job_market"]
                if enriched.get("competitors"):
                    result["top_competitors"] = enriched["competitors"]
                if enriched.get("seasonal"):
                    result["seasonal_patterns"] = enriched["seasonal"]
                if enriched.get("current_posting_count"):
                    result["current_posting_count"] = enriched["current_posting_count"]
                # v2 metadata
                if enriched.get("confidence") is not None:
                    result["data_confidence"] = enriched["confidence"]
                if enriched.get("data_freshness"):
                    result["data_freshness"] = enriched["data_freshness"]
                if enriched.get("sources_used"):
                    result["sources_used"] = enriched["sources_used"]
            except Exception as e:
                logger.debug("Orchestrator enrich_market_demand failed: %s", e)

        return result

    def _query_budget_projection(self, params: dict) -> dict:
        """Project budget allocation for given parameters.

        Uses DataOrchestrator to pass cached enrichment data to the budget
        engine for more accurate projections (instead of synthesized_data=None).
        """
        budget = params.get("budget", 0)
        roles_list = params.get("roles", [])
        locations_list = params.get("locations", [])
        industry = params.get("industry", "general")

        if budget <= 0:
            return {"error": "Budget must be greater than zero", "source": "Joveo Budget Engine"}

        result: Dict[str, Any] = {
            "source": "Joveo Budget Allocation Engine",
            "total_budget": budget,
            "industry": industry,
        }

        # Build role dicts
        roles = []
        for r in (roles_list or ["General Hire"]):
            role_lower = r.lower() if isinstance(r, str) else ""
            tier = "Professional / White-Collar"
            if any(kw in role_lower for kw in ["nurse", "clinical", "therapist"]):
                tier = "Clinical / Licensed"
            elif any(kw in role_lower for kw in ["executive", "director", "vp"]):
                tier = "Executive / Leadership"
            elif any(kw in role_lower for kw in ["driver", "warehouse", "construction"]):
                tier = "Skilled Trades / Technical"
            elif any(kw in role_lower for kw in ["cashier", "hourly", "retail"]):
                tier = "Hourly / Entry-Level"
            roles.append({"title": r, "count": 1, "tier": tier})

        # Build location dicts
        locations = []
        for loc in (locations_list or ["United States"]):
            if isinstance(loc, str):
                locations.append({"city": loc, "state": "", "country": "United States"})

        kb = self._data_cache.get("knowledge_base", {})

        # Try orchestrator first (passes cached enrichment data to budget engine)
        orch = _get_orchestrator()
        if orch:
            try:
                allocation = orch.enrich_budget(
                    budget=budget, roles=roles, locations=locations,
                    industry=industry, knowledge_base=kb,
                )
                if isinstance(allocation, dict) and "error" not in allocation:
                    result["channel_allocations"] = allocation.get("channel_allocations", {})
                    result["total_projected"] = allocation.get("total_projected", {})
                    result["sufficiency"] = allocation.get("sufficiency", {})
                    result["recommendations"] = allocation.get("recommendations", [])
                    return result
            except Exception as e:
                logger.debug("Orchestrator enrich_budget failed: %s", e)

        # Fallback: direct budget engine call without synthesized data
        try:
            from budget_engine import calculate_budget_allocation
            channel_pcts = {
                "Programmatic & DSP": 30, "Global Job Boards": 25,
                "Niche & Industry Boards": 15, "Social Media Channels": 15,
                "Regional & Local Boards": 10, "Employer Branding": 5,
            }
            allocation = calculate_budget_allocation(
                total_budget=budget, roles=roles, locations=locations,
                industry=industry, channel_percentages=channel_pcts,
                synthesized_data=None, knowledge_base=kb,
            )
            result["channel_allocations"] = allocation.get("channel_allocations", {})
            result["total_projected"] = allocation.get("total_projected", {})
            result["sufficiency"] = allocation.get("sufficiency", {})
            result["recommendations"] = allocation.get("recommendations", [])
        except Exception as e:
            logger.error("Budget engine call failed: %s", e, exc_info=True)
            result["estimated_allocation"] = {
                "programmatic_dsp": {"pct": 30, "amount": round(budget * 0.30, 2)},
                "global_job_boards": {"pct": 25, "amount": round(budget * 0.25, 2)},
                "niche_industry_boards": {"pct": 15, "amount": round(budget * 0.15, 2)},
                "social_media": {"pct": 15, "amount": round(budget * 0.15, 2)},
                "regional_local": {"pct": 10, "amount": round(budget * 0.10, 2)},
                "employer_branding": {"pct": 5, "amount": round(budget * 0.05, 2)},
            }
            result["note"] = "Estimated allocation (budget engine unavailable)"

        return result

    def _query_location_profile(self, params: dict) -> dict:
        """Get location cost, workforce, and supply data.

        Uses DataOrchestrator to cascade:
            research.py (40+ countries, 100+ metros) -> Census/World Bank API -> KB.
        """
        city = params.get("city", "").strip()
        state = params.get("state", "").strip()
        country = params.get("country", "").strip()
        location_str = city or state or country or "United States"

        country_resolved = _resolve_country(country) or _resolve_country(city) or "United States"

        result: Dict[str, Any] = {
            "source": "Joveo Location Intelligence",
            "location": {
                "city": city,
                "state": state,
                "country": country_resolved,
            }
        }

        # Orchestrator enrichment (research.py + Census/World Bank)
        orch = _get_orchestrator()
        if orch:
            try:
                enriched = orch.enrich_location(location_str)
                if enriched.get("coli"):
                    result["cost_of_living_index"] = enriched["coli"]
                if enriched.get("population") and enriched["population"] != "Data not available":
                    result["population"] = enriched["population"]
                if enriched.get("median_salary"):
                    result["median_salary"] = enriched["median_salary"]
                if enriched.get("unemployment"):
                    result["unemployment_rate"] = enriched["unemployment"]
                if enriched.get("major_employers"):
                    result["major_industries"] = enriched["major_employers"]
                if enriched.get("top_boards"):
                    result["top_job_boards"] = enriched["top_boards"]
                if enriched.get("currency") and enriched["currency"] != "USD":
                    result["currency"] = enriched["currency"]
                if enriched.get("region"):
                    result["region"] = enriched["region"]
                if enriched.get("recommended_boards"):
                    result["recommended_boards"] = enriched["recommended_boards"]
                if enriched.get("source"):
                    result["source"] = f"Joveo Location Intelligence ({enriched['source']})"
                # v2 metadata
                if enriched.get("confidence") is not None:
                    result["data_confidence"] = enriched["confidence"]
                if enriched.get("data_freshness"):
                    result["data_freshness"] = enriched["data_freshness"]
                if enriched.get("sources_used"):
                    result["sources_used"] = enriched["sources_used"]
            except Exception as e:
                logger.debug("Orchestrator enrich_location failed: %s", e)

        # Supply data from KB (always include if available)
        supply = self._data_cache.get("global_supply", {})
        country_boards = supply.get("country_job_boards", {})
        if country_resolved in country_boards:
            entry = country_boards[country_resolved]
            result["supply_data"] = {
                "monthly_spend": entry.get("monthly_spend", "N/A"),
                "key_metros": entry.get("key_metros", []),
                "total_boards": len(entry.get("boards", [])),
            }

        # Publisher count from KB
        publishers = self._data_cache.get("joveo_publishers", {})
        by_country = publishers.get("by_country", {})
        if country_resolved in by_country:
            result["publisher_count"] = len(by_country[country_resolved])

        return result

    def _query_ad_platform(self, params: dict) -> dict:
        """Get ad platform recommendations and benchmarks."""
        role_type = params.get("role_type", "professional")
        platforms = params.get("platforms", [])

        kb = self._data_cache.get("knowledge_base", {})
        benchmarks = kb.get("benchmarks", {})
        cpc_data = benchmarks.get("cost_per_click", {}).get("by_platform", {})

        result: Dict[str, Any] = {
            "source": "Joveo Ad Platform Intelligence",
            "role_type": role_type,
        }

        # Platform recommendations by role type
        platform_recs = {
            "executive": {
                "primary": ["LinkedIn", "Indeed"],
                "secondary": ["Glassdoor", "ZipRecruiter"],
                "rationale": "Executive roles require targeted professional networks with advanced targeting",
            },
            "professional": {
                "primary": ["LinkedIn", "Indeed", "Google Ads"],
                "secondary": ["ZipRecruiter", "Glassdoor", "Dice"],
                "rationale": "Professional roles benefit from a mix of job boards and search advertising",
            },
            "hourly": {
                "primary": ["Indeed", "Snagajob", "Facebook/Meta"],
                "secondary": ["Craigslist", "Google Ads", "Jobcase"],
                "rationale": "Hourly roles perform best on high-volume, mobile-first platforms",
            },
            "clinical": {
                "primary": ["Indeed", "Health eCareers", "Doximity"],
                "secondary": ["LinkedIn", "Nurse.com", "Vivian Health"],
                "rationale": "Clinical roles require niche healthcare boards for qualified candidates",
            },
            "trades": {
                "primary": ["Indeed", "Facebook/Meta", "CDLlife"],
                "secondary": ["Craigslist", "Jobcase", "Google Ads"],
                "rationale": "Trades roles benefit from local/regional targeting and mobile-first platforms",
            },
        }

        result["recommendations"] = platform_recs.get(role_type, platform_recs["professional"])

        # CPC benchmarks for requested platforms or all
        if platforms:
            for p in platforms:
                p_lower = p.lower().replace(" ", "_")
                for key, data in cpc_data.items():
                    if p_lower in key.lower() or key.lower() in p_lower:
                        result.setdefault("platform_benchmarks", {})[key] = data
        else:
            result["platform_benchmarks"] = cpc_data

        # Enrich with platform audience data from research.py
        orch = _get_orchestrator()
        if orch:
            try:
                # Use industry from params if available, else infer from role_type
                _ind = params.get("industry", "")
                audiences = orch.enrich_platform_audiences(_ind) if _ind else {}
                if audiences:
                    result["platform_audience_data"] = audiences
            except Exception as e:
                logger.debug("Orchestrator enrich_platform_audiences failed: %s", e)

        return result

    def _query_employer_brand(self, params: dict) -> dict:
        """Get employer brand intelligence for a specific company.

        Uses DataOrchestrator to access KNOWN_EMPLOYER_PROFILES (30+ companies)
        with Glassdoor ratings, hiring channels, recruitment strategies.
        """
        company = params.get("company", "").strip()
        if not company:
            return {"error": "Please provide a company name.", "source": "employer_brand"}

        orch = _get_orchestrator()
        if orch:
            try:
                enriched = orch.enrich_employer_brand(company)
                result: Dict[str, Any] = {
                    "source": f"Joveo Employer Brand Intelligence ({enriched.get('source', 'multi-source')})",
                    "company": company,
                }
                for key in ("employer_brand_strength", "glassdoor_rating",
                            "primary_hiring_channels", "known_recruitment_strategies",
                            "talent_focus", "company_size", "industry"):
                    if enriched.get(key):
                        result[key] = enriched[key]
                if enriched.get("confidence") is not None:
                    result["data_confidence"] = enriched["confidence"]
                if enriched.get("data_freshness"):
                    result["data_freshness"] = enriched["data_freshness"]
                return result
            except Exception as e:
                logger.debug("Orchestrator enrich_employer_brand failed: %s", e)

        return {
            "source": "Joveo Employer Brand Intelligence (limited)",
            "company": company,
            "note": "Employer brand data not available. Check Glassdoor and LinkedIn company page.",
        }

    def _query_ad_benchmarks(self, params: dict) -> dict:
        """Get CPC/CPM/CTR benchmarks by ad platform for an industry.

        Uses DataOrchestrator to expose ad platform benchmark data previously
        only available in the bulk pipeline.
        """
        industry = params.get("industry", "").strip()

        orch = _get_orchestrator()
        if orch:
            try:
                enriched = orch.get_ad_platform_benchmarks(industry)
                result: Dict[str, Any] = {
                    "source": f"Joveo Ad Platform Benchmarks ({enriched.get('source', 'curated')})",
                    "industry": industry or "General",
                }
                if enriched.get("platforms"):
                    result["platform_benchmarks"] = enriched["platforms"]
                if enriched.get("platform_audiences"):
                    result["platform_audiences"] = enriched["platform_audiences"]
                if enriched.get("confidence") is not None:
                    result["data_confidence"] = enriched["confidence"]
                if enriched.get("data_freshness"):
                    result["data_freshness"] = enriched["data_freshness"]
                return result
            except Exception as e:
                logger.debug("Orchestrator get_ad_platform_benchmarks failed: %s", e)

        return {
            "source": "Joveo Ad Platform Benchmarks (unavailable)",
            "industry": industry or "General",
            "note": "Ad benchmark data not available through orchestrator.",
        }

    def _query_hiring_insights(self, params: dict) -> dict:
        """Get computed hiring insights: difficulty index, salary competitiveness,
        days until next peak hiring window.

        Uses DataOrchestrator compute_insights() which synthesizes data from
        salary, market demand, and location enrichments.
        """
        role = params.get("role", "").strip()
        location = params.get("location", "").strip()
        industry = params.get("industry", "").strip()

        orch = _get_orchestrator()
        if orch:
            try:
                insights = orch.compute_insights(role, location, industry)
                result: Dict[str, Any] = {
                    "source": "Joveo Computed Hiring Insights",
                }
                for key in ("hiring_difficulty_index", "market_median_salary",
                            "salary_competitiveness_at_market",
                            "days_until_next_peak_hiring", "peak_hiring_months",
                            "current_posting_count"):
                    if insights.get(key) is not None:
                        result[key] = insights[key]
                if insights.get("confidence") is not None:
                    result["data_confidence"] = insights["confidence"]
                # Add interpretation guidance for Claude
                hdi = insights.get("hiring_difficulty_index")
                if hdi is not None:
                    if hdi >= 0.7:
                        result["difficulty_interpretation"] = "Very difficult to hire -- consider premium channels and higher budgets"
                    elif hdi >= 0.5:
                        result["difficulty_interpretation"] = "Moderately difficult -- standard approach with competitive offers"
                    else:
                        result["difficulty_interpretation"] = "Relatively easy to hire -- standard job board approach should work"
                return result
            except Exception as e:
                logger.debug("Orchestrator compute_insights failed: %s", e)

        return {
            "source": "Joveo Computed Hiring Insights (limited)",
            "note": "Call salary, market demand, and location tools first for best results.",
        }

    def _query_linkedin_guidewire(self, params: dict) -> dict:
        """Query LinkedIn Hiring Value Review data for Guidewire Software."""
        gw_data = self._data_cache.get("linkedin_guidewire", {})
        if not gw_data:
            return {"error": "LinkedIn Guidewire data not available.", "source": "linkedin_guidewire"}

        section = params.get("section", "all")
        metric = params.get("metric", "")
        result = ""

        if section == "executive_summary" or section == "all":
            exec_sum = gw_data.get("executive_summary", {})
            result = f"*Guidewire LinkedIn Hiring Review*\n"
            result += f"Headline: {exec_sum.get('headline', 'N/A')}\n"
            result += f"Context: {exec_sum.get('context', 'N/A')}\n\n"
            for theme in exec_sum.get("key_themes", []):
                result += f"*{theme.get('theme', '')}*\n"
                for pt in theme.get("points", []):
                    result += f"- {pt}\n"
                result += "\n"
            if section == "executive_summary":
                return {"text": result, "source": "LinkedIn Hiring Value Review for Guidewire Software"}

        if section == "hiring_performance" or section == "all":
            # Return hiring performance data
            hp = gw_data.get("hiring_performance", gw_data.get("hiring_performance_l12m", {}))
            if isinstance(hp, dict):
                result_hp = "*Hiring Performance (L12M)*\n"
                for key, val in hp.items():
                    if isinstance(val, dict):
                        result_hp += f"\n*{key.replace('_', ' ').title()}*:\n"
                        for k2, v2 in val.items():
                            result_hp += f"  - {k2}: {v2}\n"
                    else:
                        result_hp += f"- {key}: {val}\n"
                if section == "hiring_performance":
                    return {"text": result_hp, "source": "LinkedIn Hiring Value Review for Guidewire Software"}
                result += result_hp

        if section == "hire_efficiency" or section == "all":
            he = gw_data.get("hire_efficiency", {})
            if isinstance(he, dict):
                result_he = "*Hire Efficiency*\n"
                for key, val in he.items():
                    if isinstance(val, dict):
                        result_he += f"\n*{key.replace('_', ' ').title()}*:\n"
                        for k2, v2 in val.items():
                            result_he += f"  - {k2}: {v2}\n"
                    else:
                        result_he += f"- {key}: {val}\n"
                result += result_he

        if result:
            return {"text": result, "source": "LinkedIn Hiring Value Review for Guidewire Software"}
        return {"data": gw_data, "source": "LinkedIn Hiring Value Review for Guidewire Software"}

    def _query_platform_deep(self, args: dict) -> dict:
        """Handler for query_platform_deep tool."""
        platform = (args.get("platform", "") or "").lower().strip()
        compare_with = (args.get("compare_with", "") or "").lower().strip()
        pi = self._data_cache.get("platform_intelligence", {})
        platforms = pi.get("platforms", {})

        result = {}
        if platform:
            p_data = platforms.get(platform, {})
            if p_data:
                result["platform"] = platform
                result["data"] = {
                    "name": p_data.get("name", platform),
                    "type": p_data.get("type"),
                    "monthly_visitors": p_data.get("monthly_visitors"),
                    "avg_cpc": p_data.get("avg_cpc"),
                    "avg_cpa": p_data.get("avg_cpa"),
                    "apply_rate": p_data.get("apply_rate"),
                    "mobile_traffic_pct": p_data.get("mobile_traffic_pct"),
                    "best_for": p_data.get("best_for", []),
                    "programmatic_compatible": p_data.get("programmatic_compatible"),
                    "dei_features": p_data.get("dei_features", []),
                    "ai_features": p_data.get("ai_features", []),
                    "pros": p_data.get("pros", []),
                    "cons": p_data.get("cons", []),
                }
            else:
                result["error"] = f"Platform '{platform}' not found. Available: {', '.join(list(platforms.keys())[:20])}"

        if compare_with:
            c_data = platforms.get(compare_with, {})
            if c_data:
                result["comparison"] = {
                    "name": c_data.get("name", compare_with),
                    "avg_cpc": c_data.get("avg_cpc"),
                    "avg_cpa": c_data.get("avg_cpa"),
                    "apply_rate": c_data.get("apply_rate"),
                    "best_for": c_data.get("best_for", []),
                }

        result["source"] = "platform_intelligence_deep (91 platforms)"
        return result

    def _query_recruitment_benchmarks(self, args: dict) -> dict:
        """Handler for query_recruitment_benchmarks tool."""
        industry = (args.get("industry", "") or "").lower().strip().replace(" ", "_")
        metric = (args.get("metric", "all") or "all").lower().strip()
        rb = self._data_cache.get("recruitment_benchmarks", {})
        benchmarks = rb.get("industry_benchmarks", {})

        ind_data = benchmarks.get(industry, {})
        if not ind_data:
            # Try partial match
            for k in benchmarks:
                if industry in k.lower():
                    ind_data = benchmarks[k]
                    industry = k
                    break

        if not ind_data:
            return {"error": f"Industry '{industry}' not found", "available": list(benchmarks.keys())[:15], "source": "recruitment_benchmarks_deep"}

        if metric != "all" and metric in ind_data:
            return {"industry": industry, "metric": metric, "data": ind_data[metric], "source": "recruitment_benchmarks_deep (22 industries)"}

        return {"industry": industry, "data": ind_data, "source": "recruitment_benchmarks_deep (22 industries)"}

    def _query_employer_branding(self, args: dict) -> dict:
        """Handler for query_employer_branding tool."""
        aspect = (args.get("aspect", "all") or "all").lower().strip()
        rs = self._data_cache.get("recruitment_strategy", {})
        eb = rs.get("employer_branding", {})

        if not eb:
            return {"error": "Employer branding data not available", "source": "recruitment_strategy_intelligence"}

        if aspect == "all":
            return {"data": eb, "source": "recruitment_strategy_intelligence (34 sources)"}
        elif aspect in eb:
            return {"aspect": aspect, "data": eb[aspect], "source": "recruitment_strategy_intelligence"}
        else:
            return {"error": f"Aspect '{aspect}' not found", "available": list(eb.keys()), "source": "recruitment_strategy_intelligence"}

    def _query_regional_market(self, args: dict) -> dict:
        """Handler for query_regional_market tool."""
        region = (args.get("region", "") or "").lower().strip()
        market = (args.get("market", "") or "").lower().strip()
        rh = self._data_cache.get("regional_hiring", {})
        regions = rh.get("regions", {})

        if not region:
            return {"available_regions": list(regions.keys()), "source": "regional_hiring_intelligence"}

        region_data = regions.get(region, {})
        if not region_data:
            return {"error": f"Region '{region}' not found", "available": list(regions.keys()), "source": "regional_hiring_intelligence"}

        if market:
            market_data = region_data.get(market, {})
            if market_data:
                return {"region": region, "market": market, "data": market_data, "source": "regional_hiring_intelligence (16 sources)"}
            else:
                return {"region": region, "error": f"Market '{market}' not found", "available_markets": list(region_data.keys())[:15], "source": "regional_hiring_intelligence"}

        # Return region overview with market list
        market_list = []
        for mk, mv in region_data.items():
            if isinstance(mv, dict) and mv.get("name"):
                market_list.append({"key": mk, "name": mv.get("name"), "population": mv.get("metro_population")})
        return {"region": region, "markets": market_list, "source": "regional_hiring_intelligence"}

    def _query_supply_ecosystem(self, args: dict) -> dict:
        """Handler for query_supply_ecosystem tool."""
        topic = (args.get("topic", "all") or "all").lower().strip()
        se = self._data_cache.get("supply_ecosystem", {})
        pe = se.get("programmatic_ecosystem", {})

        if not pe:
            return {"error": "Supply ecosystem data not available", "source": "supply_ecosystem_intelligence"}

        if topic == "all":
            # Return overview, not everything (too large)
            return {
                "overview": pe.get("how_it_works", {}).get("overview", ""),
                "available_topics": list(pe.keys()),
                "bidding_model_types": list(pe.get("bidding_models", {}).keys()),
                "source": "supply_ecosystem_intelligence (24 sources)",
            }

        data = pe.get(topic, pe.get("key_concepts", {}).get(topic, {}))
        if data:
            return {"topic": topic, "data": data, "source": "supply_ecosystem_intelligence"}
        return {"error": f"Topic '{topic}' not found", "available": list(pe.keys()), "source": "supply_ecosystem_intelligence"}

    def _query_workforce_trends(self, args: dict) -> dict:
        """Handler for query_workforce_trends tool."""
        topic = (args.get("topic", "all") or "all").lower().strip()
        wt = self._data_cache.get("workforce_trends", {})

        if not wt:
            return {"error": "Workforce trends data not available", "source": "workforce_trends_intelligence"}

        gen_z = wt.get("generational_trends", {}).get("gen_z", {})

        topic_map = {
            "gen_z": gen_z,
            "platform_preferences": gen_z.get("job_search_behavior", {}).get("platform_usage", {}),
            "remote_work": gen_z.get("workplace_expectations", {}).get("flexibility", {}),
            "dei": gen_z.get("workplace_expectations", {}).get("dei_expectations", {}),
            "salary_expectations": gen_z.get("salary_expectations", {}),
            "all": {
                "gen_z_summary": {
                    "workforce_share": gen_z.get("workforce_share"),
                    "top_platforms": list(gen_z.get("job_search_behavior", {}).get("platform_usage", {}).keys())[:5],
                    "key_expectations": list(gen_z.get("workplace_expectations", {}).keys()),
                },
                "supply_partner_trends": wt.get("supply_partner_trends", {}),
                "job_type_trends": wt.get("job_type_trends", {}),
            },
        }

        data = topic_map.get(topic, {})
        if data:
            return {"topic": topic, "data": data, "source": "workforce_trends_intelligence (44 sources)"}
        return {"error": f"Topic '{topic}' not found", "available": list(topic_map.keys()), "source": "workforce_trends_intelligence"}

    def _query_white_papers(self, args: dict) -> dict:
        """Handler for query_white_papers tool."""
        search_term = (args.get("search_term", "") or "").lower().strip()
        report_key = (args.get("report_key", "") or "").strip()
        wp = self._data_cache.get("white_papers", {})
        reports = wp.get("reports", {})

        if not reports:
            return {"error": "White papers data not available", "source": "industry_white_papers"}

        if report_key:
            r = reports.get(report_key, {})
            if r:
                return {"report_key": report_key, "data": r, "source": "industry_white_papers"}
            return {"error": f"Report '{report_key}' not found", "available": list(reports.keys())[:15], "source": "industry_white_papers"}

        if search_term:
            matches = []
            for rk, rv in reports.items():
                if not isinstance(rv, dict):
                    continue
                title = (rv.get("title", "") or "").lower()
                publisher = (rv.get("publisher", "") or "").lower()
                findings_text = " ".join(str(f) for f in rv.get("key_findings", []) if f).lower()
                if search_term in title or search_term in publisher or search_term in findings_text or search_term in rk.lower():
                    matches.append({
                        "key": rk,
                        "title": rv.get("title"),
                        "publisher": rv.get("publisher"),
                        "year": rv.get("year"),
                        "finding_count": len(rv.get("key_findings", [])),
                        "top_findings": rv.get("key_findings", [])[:3],
                    })
            return {"search_term": search_term, "results": matches[:10], "total_reports": len(reports), "source": "industry_white_papers (47 reports)"}

        # No search term, return overview
        overview = []
        for rk, rv in list(reports.items())[:15]:
            if isinstance(rv, dict):
                overview.append({"key": rk, "title": rv.get("title"), "publisher": rv.get("publisher"), "year": rv.get("year")})
        return {"total_reports": len(reports), "sample": overview, "source": "industry_white_papers"}

    def _suggest_smart_defaults(self, args: dict) -> dict:
        """Auto-detect optimal hiring parameters and suggest budget/channel defaults.

        Uses role-tier classification, industry benchmarks, and location
        cost adjustments to produce smart budget recommendations at three
        tiers: minimum, recommended, and premium.
        """
        roles = args.get("roles", ["General Hire"])
        hire_count = args.get("hire_count", 10)
        locations = args.get("locations", ["United States"])
        industry = args.get("industry", "general")
        urgency = args.get("urgency", "standard")

        kb = self._data_cache.get("knowledge_base", {})
        benchmarks = kb.get("benchmarks", {})
        cph_data = benchmarks.get("cost_per_hire", {})

        # Determine average CPH by role tier
        role_cph_estimates = []
        role_tiers = []
        for role in roles:
            role_lower = role.lower() if isinstance(role, str) else ""
            if any(kw in role_lower for kw in ["executive", "director", "vp", "chief", "president"]):
                tier = "Executive"
                cph = 14000
            elif any(kw in role_lower for kw in ["nurse", "clinical", "therapist", "physician"]):
                tier = "Clinical"
                cph = 8500
            elif any(kw in role_lower for kw in ["engineer", "developer", "data scientist", "architect"]):
                tier = "Technology"
                cph = 10000
            elif any(kw in role_lower for kw in ["driver", "warehouse", "construction", "electrician", "welder"]):
                tier = "Trades"
                cph = 4500
            elif any(kw in role_lower for kw in ["cashier", "retail", "hourly", "part-time", "seasonal"]):
                tier = "Hourly"
                cph = 2500
            else:
                tier = "Professional"
                cph = 6000

            role_cph_estimates.append(cph)
            role_tiers.append({"role": role, "tier": tier, "estimated_cph": cph})

        avg_cph = sum(role_cph_estimates) / len(role_cph_estimates) if role_cph_estimates else 5000

        # Urgency multiplier
        urgency_multiplier = {"standard": 1.0, "urgent": 1.20, "critical": 1.40}.get(urgency, 1.0)
        adjusted_cph = avg_cph * urgency_multiplier

        # Budget tiers
        min_budget = round(adjusted_cph * hire_count * 0.60)  # Lean/aggressive
        rec_budget = round(adjusted_cph * hire_count)          # Recommended
        premium_budget = round(adjusted_cph * hire_count * 1.50)  # Premium/comfortable

        # Channel split recommendations by role tier mix
        has_exec = any(t["tier"] == "Executive" for t in role_tiers)
        has_hourly = any(t["tier"] in ("Hourly", "Trades") for t in role_tiers)
        has_clinical = any(t["tier"] == "Clinical" for t in role_tiers)

        if has_exec:
            channel_split = {
                "LinkedIn Ads": 35, "Programmatic & DSP": 20,
                "Global Job Boards": 20, "Niche Executive Boards": 15,
                "Employer Branding": 10,
            }
        elif has_hourly:
            channel_split = {
                "Programmatic & DSP": 35, "Global Job Boards": 25,
                "Social Media (Meta/TikTok)": 20, "Regional & Local Boards": 15,
                "Employer Branding": 5,
            }
        elif has_clinical:
            channel_split = {
                "Niche Healthcare Boards": 30, "Programmatic & DSP": 25,
                "Global Job Boards": 20, "Social Media Channels": 15,
                "Regional & Local Boards": 10,
            }
        else:
            channel_split = {
                "Programmatic & DSP": 30, "Global Job Boards": 25,
                "Niche & Industry Boards": 15, "Social Media Channels": 15,
                "Regional & Local Boards": 10, "Employer Branding": 5,
            }

        return {
            "source": "Joveo Smart Defaults Engine",
            "input": {
                "roles": roles,
                "hire_count": hire_count,
                "locations": locations,
                "industry": industry,
                "urgency": urgency,
            },
            "role_analysis": role_tiers,
            "budget_recommendations": {
                "minimum_budget": {
                    "amount": min_budget,
                    "per_hire": round(min_budget / max(hire_count, 1)),
                    "note": "Lean budget -- requires aggressive optimization and may extend time-to-fill",
                },
                "recommended_budget": {
                    "amount": rec_budget,
                    "per_hire": round(rec_budget / max(hire_count, 1)),
                    "note": "Balanced budget for quality hires within standard timelines",
                },
                "premium_budget": {
                    "amount": premium_budget,
                    "per_hire": round(premium_budget / max(hire_count, 1)),
                    "note": "Comfortable budget allowing for employer branding and faster fills",
                },
            },
            "recommended_channel_split": channel_split,
            "urgency_adjustment": f"{urgency} ({urgency_multiplier:.0%} of base)" if urgency != "standard" else "standard (no adjustment)",
            "benchmarks_used": {
                "shrm_avg_cph": cph_data.get("shrm_2026", {}).get("average_cost_per_hire", "$4,800"),
                "note": "Budget estimates based on role tier, industry benchmarks, and urgency",
            },
        }


    # ------------------------------------------------------------------
    # Chat orchestration
    # ------------------------------------------------------------------

    def chat(self, user_message: str, conversation_history: Optional[list] = None,
             enrichment_context: Optional[dict] = None) -> dict:
        """Process a chat message and return a response.

        Args:
            user_message: The user's question.
            conversation_history: List of previous messages [{role, content}].
            enrichment_context: Optional pre-computed enrichment data.

        Returns:
            Dict with response, sources, confidence, tools_used.
        """
        if not user_message or not user_message.strip():
            return {
                "response": "Please ask a question about recruitment marketing, and I will help you with data-driven insights.",
                "sources": [],
                "confidence": 1.0,
                "tools_used": [],
            }

        # Truncate message
        user_message = user_message.strip()[:MAX_MESSAGE_LENGTH]

        # --- Learned answers (fastest exit path, 0 API tokens) ---
        _t0 = time.time()
        learned = _check_learned_answers(user_message)
        if learned:
            logger.info("NOVA MODE: Learned answer match -- returning cached answer")
            _nova_metrics.record_learned_answer()
            _nova_metrics.record_latency((time.time() - _t0) * 1000)
            return learned

        # --- Response cache (standalone questions only) ---
        history = conversation_history or []
        cache_key = _normalize_cache_key(user_message)
        if len(history) <= 2 and cache_key:
            cached = _get_response_cache(cache_key)
            if cached:
                logger.info("NOVA MODE: Cache hit -- returning cached response")
                _nova_metrics.record_cache_hit()
                _nova_metrics.record_latency((time.time() - _t0) * 1000)
                return cached

        # Check for Claude API mode
        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if api_key:
            try:
                logger.info("NOVA MODE: Using Claude API (Anthropic) for chat")
                result = self._chat_with_claude(user_message, conversation_history, enrichment_context, api_key)
                logger.info("NOVA MODE: Claude API response received successfully")
                _nova_metrics.record_latency((time.time() - _t0) * 1000)
                # Cache successful responses with sufficient confidence
                if result.get("confidence", 0) >= 0.6 and cache_key and len(history) <= 2:
                    _set_response_cache(cache_key, result)
                return result
            except Exception as e:
                logger.error("Claude API call failed, falling back to rule-based: %s", e)
                _nova_metrics.record_api_error()
        else:
            logger.info("NOVA MODE: No ANTHROPIC_API_KEY set, using rule-based mode")

        # Rule-based fallback
        logger.info("NOVA MODE: Using rule-based fallback")
        _nova_metrics.record_rule_based()
        result = self._chat_rule_based(user_message, enrichment_context, conversation_history)
        _nova_metrics.record_latency((time.time() - _t0) * 1000)
        return result

    def _chat_with_claude(self, user_message: str, conversation_history: Optional[list],
                          enrichment_context: Optional[dict], api_key: str) -> dict:
        """Use Claude API for natural-language chat with tool use.

        Features:
        - Structured conversation history with session context
        - Multi-turn tool use (up to 8 iterations for complex queries)
        - Automatic source tracking across tool calls
        - Confidence scoring based on data quality
        - Graceful degradation on API errors
        """
        import urllib.request
        import urllib.error

        messages = []

        # Build conversation history with context preservation
        if conversation_history:
            # Keep more recent history for context continuity
            recent_history = conversation_history[-MAX_HISTORY_TURNS:]
            for msg in recent_history:
                role = msg.get("role", "user")
                content = msg.get("content", "")
                if role in ("user", "assistant") and content:
                    messages.append({"role": role, "content": content})

        messages.append({"role": "user", "content": user_message})

        # Build system prompt with session context
        system_prompt = self.get_system_prompt()
        if enrichment_context:
            context_summary = _summarize_enrichment(enrichment_context)
            system_prompt += f"\n\n## ACTIVE SESSION CONTEXT\nThe user is working on a media plan with the following parameters:\n{context_summary}\nUse this context to provide more relevant answers. If the user asks about budget, roles, or locations, use these values as defaults unless they specify otherwise."

        # Add conversation memory summary if multi-turn
        if conversation_history and len(conversation_history) > 2:
            memory_summary = _build_conversation_memory(conversation_history)
            if memory_summary:
                system_prompt += f"\n\n## CONVERSATION MEMORY\nKey context from this conversation so far:\n{memory_summary}"

        tools_used = []
        sources = set()
        tool_call_details = []  # Track detailed tool interactions for debugging
        max_iterations = 8  # Allow more iterations for complex multi-tool queries

        adaptive_max_tokens = _classify_query_complexity(user_message)
        tool_defs = self.get_tool_definitions()

        # --- Prompt caching: structured system + cache_control on last tool ---
        system_content = [
            {
                "type": "text",
                "text": system_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ]
        if tool_defs:
            tool_defs[-1]["cache_control"] = {"type": "ephemeral"}

        for iteration in range(max_iterations):
            payload = {
                "model": CLAUDE_MODEL,
                "max_tokens": adaptive_max_tokens,
                "system": system_content,
                "messages": messages,
                "tools": tool_defs,
            }

            try:
                req = urllib.request.Request(
                    "https://api.anthropic.com/v1/messages",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "anthropic-beta": "prompt-caching-2024-07-31",
                    },
                )

                with urllib.request.urlopen(req, timeout=45) as resp:
                    resp_data = json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as http_err:
                logger.error("Claude API HTTP error (iter %d): %s", iteration, http_err)
                if iteration == 0:
                    raise  # Let caller handle first-iteration failures
                break  # Use partial results from previous iterations
            except Exception as exc:
                logger.error("Claude API error (iter %d): %s", iteration, exc)
                if iteration == 0:
                    raise
                break

            # Track token usage from API response
            _usage = resp_data.get("usage", {})
            _in_tok = _usage.get("input_tokens", 0)
            _out_tok = _usage.get("output_tokens", 0)
            _cache_create = _usage.get("cache_creation_input_tokens", 0)
            _cache_read = _usage.get("cache_read_input_tokens", 0)
            _nova_metrics.record_claude_call(_in_tok, _out_tok, _cache_create, _cache_read)
            logger.info("Nova tokens: in=%d out=%d cache_read=%d cache_create=%d",
                        _in_tok, _out_tok, _cache_read, _cache_create)

            stop_reason = resp_data.get("stop_reason", "end_turn")
            content_blocks = resp_data.get("content", [])

            if stop_reason == "tool_use":
                # Process tool calls
                tool_results = []
                for block in content_blocks:
                    if block.get("type") == "tool_use":
                        tool_name = block["name"]
                        tool_input = block.get("input", {})
                        tool_id = block.get("id", "")

                        tools_used.append(tool_name)
                        logger.info("Nova Claude: tool call [%d] %s(%s)",
                                    iteration, tool_name, json.dumps(tool_input)[:200])

                        tool_result = self.execute_tool(tool_name, tool_input)

                        # Track source from result
                        try:
                            result_parsed = json.loads(tool_result)
                            if "source" in result_parsed:
                                sources.add(result_parsed["source"])
                            # Track tool details for confidence scoring
                            has_data = not result_parsed.get("error")
                            tool_call_details.append({
                                "tool": tool_name,
                                "has_data": has_data,
                                "source": result_parsed.get("source", ""),
                            })
                        except (json.JSONDecodeError, TypeError):
                            tool_call_details.append({
                                "tool": tool_name,
                                "has_data": bool(tool_result),
                                "source": "",
                            })

                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": tool_result,
                        })

                # Add assistant message with tool_use blocks and tool results
                messages.append({"role": "assistant", "content": content_blocks})
                messages.append({"role": "user", "content": tool_results})
            else:
                # Extract text response
                response_text = ""
                for block in content_blocks:
                    if block.get("type") == "text":
                        response_text += block.get("text", "")

                confidence = _estimate_confidence_v2(tools_used, sources, tool_call_details)
                return {
                    "response": response_text,
                    "sources": list(sources),
                    "confidence": confidence,
                    "tools_used": tools_used,
                    "tool_iterations": iteration + 1,
                }

        # If we exhausted iterations, extract any partial text
        partial_text = ""
        for block in content_blocks:
            if block.get("type") == "text":
                partial_text += block.get("text", "")

        if partial_text:
            return {
                "response": partial_text + "\n\n_Note: I used all available tool iterations. Some data may be incomplete._",
                "sources": list(sources),
                "confidence": max(0.3, _estimate_confidence_v2(tools_used, sources, tool_call_details) - 0.1),
                "tools_used": tools_used,
                "tool_iterations": max_iterations,
            }

        return {
            "response": "I gathered data but could not finalize a response. Please try rephrasing your question.",
            "sources": list(sources),
            "confidence": 0.3,
            "tools_used": tools_used,
            "tool_iterations": max_iterations,
        }

    def _chat_rule_based(self, user_message: str, enrichment_context: Optional[dict] = None,
                         conversation_history: Optional[list] = None) -> dict:
        """Rule-based chat engine using keyword matching and data lookups."""
        msg_lower = user_message.lower()
        tools_used = []
        sources = set()
        sections = []

        # Detect intents
        detected_roles = _detect_keywords(msg_lower, _ROLE_KEYWORDS)
        detected_metrics = _detect_keywords(msg_lower, _METRIC_KEYWORDS)
        detected_industries = _detect_keywords(msg_lower, _INDUSTRY_KEYWORDS)
        detected_country = _detect_country(msg_lower)

        # ── Conversation context: detect follow-up intent from history ──
        _last_intent = None
        _last_role_title = None
        if conversation_history:
            for prev_msg in reversed(conversation_history):
                if prev_msg.get("role") == "user":
                    prev_text = prev_msg.get("content", "").lower()
                    if any(kw in prev_text for kw in ["salary", "compensation", "pay range", "wage"]):
                        _last_intent = "salary"
                        # Try to extract the role from the previous salary question
                        prev_roles = _detect_keywords(prev_text, _ROLE_KEYWORDS)
                        if prev_roles:
                            _prev_role = _pick_best_role(prev_roles, prev_text)
                            _role_titles = {
                                "nursing": "Registered Nurse", "engineering": "Software Engineer",
                                "technology": "Software Developer", "healthcare": "Healthcare Professional",
                                "retail": "Retail Associate", "hospitality": "Hospitality Worker",
                                "transportation": "CDL Driver", "finance": "Financial Analyst",
                                "executive": "Senior Executive", "hourly": "Hourly Worker",
                                "education": "Teacher", "construction": "Construction Worker",
                                "sales": "Sales Representative", "marketing": "Marketing Manager",
                                "remote": "Remote Worker",
                            }
                            _last_role_title = _role_titles.get(_prev_role, _prev_role.title())
                    elif any(kw in prev_text for kw in ["budget", "allocat", "spend"]):
                        _last_intent = "budget"
                    elif any(kw in prev_text for kw in ["publisher", "job board", "board"]):
                        _last_intent = "publisher"
                    elif any(kw in prev_text for kw in ["benchmark", "cpc", "cpa"]):
                        _last_intent = "benchmark"
                    break

        # Detect question type
        is_publisher_question = any(kw in msg_lower for kw in ["publisher", "job board", "board", "where to post", "which board"])
        is_channel_question = any(kw in msg_lower for kw in ["channel", "source", "platform",
                                                                "where to advertise",
                                                                "non-traditional", "nontraditional"])
        is_budget_question = any(kw in msg_lower for kw in ["budget", "allocat", "spend", "invest",
                                                                "roi", "$", "media plan", "hiring plan",
                                                                "cost projection", "cost estimate"])
        is_benchmark_question = any(kw in msg_lower for kw in ["benchmark", "average", "industry average",
                                                                    "typical", "programmatic"])
        is_salary_question = "salary" in detected_metrics or any(kw in msg_lower for kw in ["salary", "compensation", "pay range", "wage"])
        is_dei_question = any(kw in msg_lower for kw in ["dei", "diversity", "inclusion", "women", "minority", "veteran", "disability"])
        is_trend_question = any(kw in msg_lower for kw in ["trend", "future", "outlook", "forecast", "what's new", "emerging"])
        is_cpc_cpa_question = "cpc" in detected_metrics or "cpa" in detected_metrics or "cph" in detected_metrics

        # Greeting detection — use word boundary matching for short keywords
        import re as _re
        _greeting_patterns = [
            r'\bhello\b', r'\bhi\b', r'\bhey\b', r'\bgood morning\b', r'\bgood afternoon\b',
            r'^help$', r'^help\s*me$', r'^help\s*$', r'what can you do', r'who are you',
        ]
        is_greeting = any(_re.search(pat, msg_lower) for pat in _greeting_patterns)
        # Prevent false positives: if "help" appears but message is longer and contains
        # suspicious/action words, it's NOT a greeting
        if is_greeting and len(msg_lower.split()) > 4:
            _non_greeting_signals = ["hack", "break", "steal", "attack", "exploit",
                                     "inject", "password", "admin", "ignore", "previous instructions"]
            if any(sig in msg_lower for sig in _non_greeting_signals):
                is_greeting = False

        # Also check for Guidewire/DEI/trend/CPC questions before returning greeting
        _is_guidewire = any(kw in msg_lower for kw in ["guidewire", "linkedin hiring", "influenced hire", "skill density", "inmail"])
        if is_greeting and not (is_publisher_question or is_channel_question or is_budget_question
                                or is_benchmark_question or is_salary_question or is_dei_question
                                or is_trend_question or is_cpc_cpa_question or _is_guidewire):
            return {
                "response": (
                    "Hello! I'm *Nova*, your recruitment marketing intelligence assistant. "
                    "I have access to data from *10,238+ Supply Partners*, job boards across *70+ countries*, "
                    "and comprehensive industry benchmarks and salary data.\n\n"
                    "Here are some things I can help with:\n\n"
                    "- *Publisher & Board Recommendations*: \"What publishers work best for nursing roles?\"\n"
                    "- *Industry Benchmarks*: \"What's the average CPA for tech roles?\"\n"
                    "- *Budget Planning*: \"How should I allocate a $50K budget for 10 engineering hires?\"\n"
                    "- *Market Intelligence*: \"What's the talent supply for tech roles in Germany?\"\n"
                    "- *DEI Strategy*: \"What DEI-focused job boards are available in the US?\"\n\n"
                    "What would you like to know?"
                ),
                "sources": [],
                "confidence": 1.0,
                "tools_used": [],
            }

        # ── Guidewire / LinkedIn hiring data ──
        if any(kw in msg_lower for kw in ["guidewire", "linkedin hiring", "influenced hire", "skill density", "inmail"]):
            gw_data = self._data_cache.get("linkedin_guidewire", {})
            if gw_data:
                exec_sum = gw_data.get("executive_summary", {})
                response_parts = [f"*Guidewire Software — LinkedIn Hiring Intelligence*\n"]
                response_parts.append(f"{exec_sum.get('headline', '')}\n")
                for theme in exec_sum.get("key_themes", [])[:3]:
                    response_parts.append(f"\n*{theme.get('theme', '')}*")
                    for pt in theme.get("points", [])[:3]:
                        response_parts.append(f"- {pt}")

                # Add peer comparison if available
                peers = gw_data.get("document_metadata", {}).get("peer_companies", [])
                if peers:
                    response_parts.append(f"\n*Peer Companies*: {', '.join(peers)}")

                return {
                    "response": "\n".join(response_parts),
                    "sources": ["LinkedIn Hiring Value Review for Guidewire Software (Jan 2025 - Dec 2025)"],
                    "confidence": 0.95,
                }

        # ── Publisher count question (e.g., "How many publishers does Joveo have?") ──
        is_count_question = any(kw in msg_lower for kw in ["how many publisher", "total publisher",
                                                             "publisher count", "number of publisher"])
        if is_count_question:
            pub_data = self._query_publishers({})
            tools_used.append("query_publishers")
            sources.add("Joveo Publisher Network")
            total = pub_data.get("total_active_publishers", 0)
            cats = pub_data.get("categories", {})
            countries_covered = pub_data.get("countries_covered", 0)
            count_parts = [
                f"*Joveo Publisher Network*\n",
                f"Joveo has *{total:,} active publishers* across *{countries_covered} countries*.\n",
            ]
            if detected_country:
                # Also show country-specific count
                country_pub = self._query_publishers({"country": detected_country})
                c_count = country_pub.get("count", 0)
                c_pubs = country_pub.get("publishers", [])
                count_parts.append(f"*In {detected_country}*: {c_count} publishers")
                if c_pubs:
                    for p in c_pubs[:10]:
                        count_parts.append(f"- {p}")
                    if len(c_pubs) > 10:
                        count_parts.append(f"_...and {len(c_pubs) - 10} more_")
            else:
                # Show category breakdown
                if cats:
                    count_parts.append("*By Category:*")
                    for cat, count in sorted(cats.items(), key=lambda x: x[1], reverse=True)[:12]:
                        count_parts.append(f"- *{cat}*: {count} publishers")
            sections.append("\n".join(count_parts))

        # ── Publisher / Job Board questions ──
        elif is_publisher_question or (detected_country and not is_benchmark_question and not is_budget_question
                                        and not is_salary_question and not is_trend_question
                                        and not is_cpc_cpa_question
                                        and _last_intent not in ("salary", "budget", "benchmark")):
            country = detected_country or "United States"
            if is_dei_question:
                data = self._query_global_supply({"country": country, "board_type": "dei"})
            else:
                category = ""
                for role_cat in detected_roles:
                    if role_cat in ("nursing", "healthcare"):
                        category = "Healthcare"
                    elif role_cat in ("engineering", "technology"):
                        category = "Tech"
                    break
                data = self._query_global_supply({"country": country, "board_type": "general", "category": category})

            tools_used.append("query_global_supply")
            sources.add("Joveo Global Supply Intelligence")
            sections.append(_format_supply_response(data, country, is_dei_question))

            # Also query publishers
            pub_params = {"country": country}
            if detected_roles:
                role_cat = list(detected_roles)[0]
                cat_map = {
                    "nursing": "Health", "healthcare": "Health", "engineering": "Tech",
                    "technology": "Tech", "finance": "Job Board",
                }
                if role_cat in cat_map:
                    pub_params["category"] = cat_map[role_cat]
            pub_data = self._query_publishers(pub_params)
            tools_used.append("query_publishers")
            sources.add("Joveo Publisher Network")
            sections.append(_format_publisher_response(pub_data))

        # ── Channel questions ──
        if is_channel_question and not is_publisher_question:
            industry = list(detected_industries)[0] if detected_industries else ""
            if not industry and not detected_country and not detected_roles:
                # Ask for clarification -- channel recommendations depend on context
                sections.append(
                    "I can recommend the best recruitment channels, but I need a bit more context.\n\n"
                    "Could you specify any of the following?\n"
                    "- *Industry*: healthcare, technology, retail, etc.\n"
                    "- *Country*: US, India, UK, Germany, etc.\n"
                    "- *Role type*: nursing, engineering, hourly, executive, etc.\n\n"
                    "For example: _\"What channels work best for tech hiring in India?\"_"
                )
            else:
                ch_data = self._query_channels({"industry": industry, "channel_type": "all"})
                tools_used.append("query_channels")
                sources.add("Joveo Channel Database")
                sections.append(_format_channel_response(ch_data, industry))

        # ── CPC / CPA / Benchmark questions ──
        if is_cpc_cpa_question or is_benchmark_question:
            metric = ""
            if "cpc" in detected_metrics:
                metric = "cpc"
            elif "cpa" in detected_metrics:
                metric = "cpa"
            elif "cph" in detected_metrics:
                metric = "cost_per_hire"
            elif "apply_rate" in detected_metrics:
                metric = "apply_rate"
            elif "time_to_fill" in detected_metrics:
                metric = "time_to_fill"
            elif "benchmark" in detected_metrics:
                metric = ""

            industry = list(detected_industries)[0] if detected_industries else ""
            if not industry and not metric:
                # Ask for specifics -- benchmarks are meaningless without context
                sections.append(
                    "I have benchmark data across *22 industries*, but results vary dramatically by sector.\n\n"
                    "Could you specify:\n"
                    "- *Which metric?* CPC, CPA, cost-per-hire, apply rate, or time-to-fill\n"
                    "- *Which industry?* Healthcare, technology, retail, finance, etc.\n\n"
                    "For example: _\"What's the average CPA for healthcare roles?\"_ or "
                    "_\"What CPC should I expect for tech hiring?\"_"
                )
            else:
                kb_data = self._query_knowledge_base({"topic": "benchmarks", "metric": metric, "industry": industry})
                tools_used.append("query_knowledge_base")
                sources.add("Recruitment Industry Knowledge Base")
                sections.append(_format_benchmark_response(kb_data, metric, industry))

        # ── Follow-up: country-only message after a salary question ──
        if (detected_country and not is_publisher_question and not is_channel_question
                and not is_benchmark_question and not is_budget_question
                and not is_salary_question and not is_cpc_cpa_question
                and not is_dei_question and not is_trend_question
                and _last_intent == "salary"):
            # User said something like "in india" after a salary question
            role_title = _last_role_title or "General Professional"
            sal_data = self._query_salary_data({"role": role_title, "location": detected_country})
            tools_used.append("query_salary_data")
            sources.add("Joveo Salary Intelligence")
            sections.append(_format_salary_response(sal_data))

        # ── Salary questions ──
        if is_salary_question:
            role = _pick_best_role(detected_roles, msg_lower) if detected_roles else "general"
            role_titles = {
                "nursing": "Registered Nurse", "engineering": "Software Engineer",
                "technology": "Software Developer", "healthcare": "Healthcare Professional",
                "retail": "Retail Associate", "hospitality": "Hospitality Worker",
                "transportation": "CDL Driver", "finance": "Financial Analyst",
                "executive": "Senior Executive", "hourly": "Hourly Worker",
                "education": "Teacher", "construction": "Construction Worker",
                "sales": "Sales Representative", "marketing": "Marketing Manager",
                "remote": "Remote Worker",
            }
            role_title = role_titles.get(role, role.title())
            # Use state name if detected, otherwise country
            detected_state = _detect_us_state(user_message)
            location = detected_state or detected_country or ""
            if not location:
                # Ask for clarification -- salary varies hugely by country
                sections.append(
                    f"I can provide salary data for *{role_title}* roles, but compensation varies "
                    "significantly by location.\n\n"
                    "Which country or region are you interested in? For example:\n"
                    "- United States (or a specific state like California, Texas)\n"
                    "- India\n"
                    "- United Kingdom\n"
                    "- Germany\n\n"
                    "Please specify a location so I can give you accurate data."
                )
            else:
                sal_data = self._query_salary_data({"role": role_title, "location": location})
                tools_used.append("query_salary_data")
                sources.add("Joveo Salary Intelligence")
                sections.append(_format_salary_response(sal_data))

        # ── Budget questions ──
        if is_budget_question:
            # Extract budget amount from message
            budget_amount = _extract_budget(msg_lower)

            # Check for missing critical parameters
            _budget_missing = []
            if budget_amount <= 0:
                _budget_missing.append("*Budget amount*: How much is the total budget? (e.g., $50K, $100K)")
            if not detected_roles:
                _budget_missing.append("*Role(s)*: What positions are you hiring for? (e.g., software engineers, nurses)")
            if not detected_country:
                _budget_missing.append("*Location*: Which country or region? (e.g., US, India, UK)")

            if _budget_missing:
                sections.append(
                    "I can create a detailed budget allocation plan, but I need a few more details:\n\n"
                    + "\n".join(f"- {m}" for m in _budget_missing) + "\n\n"
                    "For example: _\"How should I allocate a $50K budget to hire 10 software engineers in the US?\"_"
                )
            else:
                roles_for_budget = []
                for r in detected_roles:
                    role_titles = {
                        "nursing": "Registered Nurse", "engineering": "Software Engineer",
                        "technology": "Software Developer", "healthcare": "Healthcare Professional",
                        "retail": "Retail Associate", "transportation": "CDL Driver",
                        "finance": "Financial Analyst", "executive": "Senior Executive",
                        "hourly": "Hourly Worker", "education": "Teacher",
                        "construction": "Construction Worker", "sales": "Sales Representative",
                        "remote": "Remote Worker", "marketing": "Marketing Manager",
                    }
                    roles_for_budget.append(role_titles.get(r, r.title()))

                locations_for_budget = [detected_country] if detected_country else ["United States"]
                industry = list(detected_industries)[0] if detected_industries else "general"

                budget_data = self._query_budget_projection({
                    "budget": budget_amount,
                    "roles": roles_for_budget or ["General Hire"],
                    "locations": locations_for_budget,
                    "industry": industry,
                })
                tools_used.append("query_budget_projection")
                sources.add("Joveo Budget Allocation Engine")
                sections.append(_format_budget_response(budget_data, budget_amount))

                # Also add role-specific niche channel recommendations for budget questions
                if detected_roles:
                    role_cat = list(detected_roles)[0]
                    cat_map = {
                        "nursing": "Health", "healthcare": "Health", "engineering": "Tech",
                        "technology": "Tech", "retail": "Retail", "finance": "Job Board",
                        "transportation": "Transportation", "construction": "Construction",
                        "education": "Education", "hourly": "Hourly",
                    }
                    country_for_ch = detected_country or "United States"
                    pub_params = {"country": country_for_ch}
                    if role_cat in cat_map:
                        pub_params["category"] = cat_map[role_cat]
                    pub_data = self._query_publishers(pub_params)
                    tools_used.append("query_publishers")
                    sources.add("Joveo Publisher Network")
                    sections.append(f"\n*Recommended Channels for {roles_for_budget[0] if roles_for_budget else role_cat.title()}*\n" +
                                    _format_publisher_response(pub_data))

        # ── Comparison questions (vs / compare) ──
        is_comparison = any(kw in msg_lower for kw in [" vs ", " versus ", "compare ", "comparison"])
        if is_comparison:
            # Split the comparison into two sides and provide data for each
            comparison_parts = _re.split(r'\bvs\.?\b|\bversus\b|\bcompare\b', msg_lower, maxsplit=1)
            kb_data = self._query_knowledge_base({"topic": "benchmarks"})
            tools_used.append("query_knowledge_base")
            sources.add("Recruitment Industry Knowledge Base")

            comp_sections = ["*Comparison Analysis*\n"]

            # Detect if this is a platform comparison (e.g. Indeed vs LinkedIn)
            _platform_names = {
                "indeed": "Indeed", "linkedin": "LinkedIn", "ziprecruiter": "ZipRecruiter",
                "glassdoor": "Glassdoor", "google ads": "Google Ads", "google": "Google Ads",
                "meta": "Meta/Facebook", "facebook": "Meta/Facebook", "careerbuilder": "CareerBuilder",
                "dice": "Dice", "snagajob": "Snagajob", "jobget": "JobGet",
                "craigslist": "Craigslist", "monster": "Monster", "handshake": "Handshake",
                "appcast": "Appcast", "pandologic": "PandoLogic", "recruitics": "Recruitics",
            }

            # Determine if either side of the comparison is a known platform
            platform_matches = []
            for part in comparison_parts[:2]:
                part_clean = part.strip().rstrip("?.,!").lower()
                matched_platform = None
                for alias, canonical in _platform_names.items():
                    if alias in part_clean:
                        matched_platform = canonical
                        break
                platform_matches.append(matched_platform)

            is_platform_comparison = all(pm is not None for pm in platform_matches[:2]) and len(platform_matches) >= 2

            if is_platform_comparison:
                # Platform-specific comparison using knowledge base data
                cpc_data = self._query_knowledge_base({"topic": "benchmarks", "metric": "cpc"})
                cpc_by_platform = cpc_data.get("benchmarks", {}).get("cost_per_click", {}).get("by_platform", {})

                for idx, pm in enumerate(platform_matches[:2]):
                    if pm is None:
                        continue
                    comp_sections.append(f"*{pm}:*")
                    # Look up CPC data for this platform
                    plat_key_lower = pm.lower().replace(" ", "_").replace("/", "_")
                    found_data = None
                    for k, v in cpc_by_platform.items():
                        if plat_key_lower in k.lower() or k.lower() in plat_key_lower:
                            found_data = v
                            break
                    if found_data and isinstance(found_data, dict):
                        for fk, fv in list(found_data.items())[:5]:
                            comp_sections.append(f"  - {fk.replace('_', ' ').title()}: {fv}")
                    else:
                        # Provide hardcoded platform summaries
                        _platform_summaries = {
                            "Indeed": "- CPC Range: $0.25-$1.50\n- Model: CPC (pay per click)\n- Best For: High-volume hiring across all roles\n- Reach: Largest job site globally",
                            "LinkedIn": "- CPC Range: $2.00-$5.00+\n- Model: CPC / Sponsored Jobs\n- Best For: White-collar, professional, executive roles\n- Reach: 900M+ professionals",
                            "ZipRecruiter": "- CPC Range: $0.50-$2.00\n- Model: Pay-per-click with AI matching\n- Best For: SMB hiring, broad role types\n- Reach: Strong US coverage",
                            "Glassdoor": "- CPC Range: $0.50-$2.00\n- Model: CPC (merging with Indeed)\n- Best For: Employer brand-driven hiring\n- Reach: Merging into Indeed",
                            "Google Ads": "- CPC Range: $1.00-$4.00 (job-related keywords)\n- Model: PPC auction\n- Best For: Programmatic reach, candidate capture\n- Reach: Broadest search traffic",
                            "Meta/Facebook": "- CPC Range: $0.50-$2.50\n- Model: Social PPC\n- Best For: Hourly, local, blue-collar roles\n- Reach: 3B+ users, mobile-first",
                        }
                        summary = _platform_summaries.get(pm, f"- Contact Joveo for detailed {pm} benchmarks")
                        for line in summary.split("\n"):
                            comp_sections.append(f"  {line}")
                    comp_sections.append("")

                if len(platform_matches) >= 2 and platform_matches[0] and platform_matches[1]:
                    comp_sections.append(f"*Key Differences ({platform_matches[0]} vs {platform_matches[1]}):*")
                    comp_sections.append("- Compare CPC ranges and pricing models to choose based on your budget")
                    comp_sections.append("- Consider your target role type — niche platforms outperform generalists for specialized roles")
                    comp_sections.append("- Programmatic platforms (via Joveo) can optimize spend across both automatically")
            else:
                # Category-based comparison (blue-collar vs white-collar, etc.)
                for i, part in enumerate(comparison_parts[:2]):
                    part_clean = part.strip().rstrip("?.,!")
                    if not part_clean:
                        continue
                    label = part_clean.title()
                    comp_sections.append(f"*{label}:*")

                    # Check if it's a role type
                    is_blue_collar = any(kw in part for kw in ["blue collar", "hourly", "warehouse", "driver", "construction", "retail"])
                    is_white_collar = any(kw in part for kw in ["white collar", "professional", "office", "corporate", "engineer", "analyst"])

                    if is_blue_collar:
                        comp_sections.append("- *Typical CPA*: $15-$40")
                        comp_sections.append("- *Apply Rate*: 8-15%")
                        comp_sections.append("- *Top Channels*: Snagajob, Indeed, Craigslist, Wonolo, Instawork, ShiftPixy")
                        comp_sections.append("- *Best Platforms*: Google Ads, Meta (mobile-first targeting)")
                        comp_sections.append("- *Key Trait*: High volume, mobile-first, quick apply needed")
                    elif is_white_collar:
                        comp_sections.append("- *Typical CPA*: $50-$150")
                        comp_sections.append("- *Apply Rate*: 3-6%")
                        comp_sections.append("- *Top Channels*: LinkedIn, Indeed, Glassdoor, ZipRecruiter, niche boards")
                        comp_sections.append("- *Best Platforms*: LinkedIn Ads, Google Ads, programmatic DSP")
                        comp_sections.append("- *Key Trait*: Quality over quantity, employer brand matters")
                    else:
                        # Generic: pull benchmarks from KB
                        comp_sections.append(f"- Search recruitment benchmarks for '{label}' in the knowledge base")

                    comp_sections.append("")

                if len(comparison_parts) >= 2:
                    comp_sections.append("*Key Differences:*")
                    comp_sections.append("- Blue-collar: higher apply rates, lower CPA, mobile-centric, speed matters")
                    comp_sections.append("- White-collar: lower apply rates, higher CPA, brand-driven, quality-focused")
                    comp_sections.append("- Budget split: blue-collar favors job boards (60%+), white-collar favors LinkedIn + programmatic (50%+)")

            sections.append("\n".join(comp_sections))

        # ── DEI questions (standalone) ──
        if is_dei_question and not is_publisher_question:
            country = detected_country or ""
            dei_data = self._query_global_supply({"country": country, "board_type": "dei"})
            tools_used.append("query_global_supply")
            sources.add("Joveo Global Supply Intelligence")
            sections.append(_format_dei_response(dei_data, country))

        # ── Trend questions ──
        if is_trend_question:
            trend_data = self._query_knowledge_base({"topic": "trends"})
            tools_used.append("query_knowledge_base")
            sources.add("Recruitment Industry Knowledge Base")
            sections.append(_format_trend_response(trend_data))

        # ── Remote work questions ── (before market demand so "remote" doesn't fall through)
        if "remote" in detected_roles and not sections:
            remote_boards = [
                "*FlexJobs* - Curated remote & flexible job listings",
                "*We Work Remotely* - Largest remote work community",
                "*Remote.co* - Remote jobs across all industries",
                "*Remote OK* - Remote job aggregator with salary data",
                "*Jobspresso* - Curated remote positions in tech, marketing, support",
                "*Working Nomads* - Digital nomad and remote job listings",
                "*Himalayas* - Remote jobs with company transparency data",
                "*Remotive* - Remote tech jobs community",
                "*AngelList / Wellfound* - Startup remote positions",
                "*LinkedIn (Remote filter)* - Largest professional network with remote job filter",
            ]
            parts = ["*Remote Work Job Boards & Channels*\n"]
            parts.append("Here are the top platforms for posting remote/work-from-home positions:\n")
            for b in remote_boards:
                parts.append(f"- {b}")
            parts.append("\n*Tips for Remote Hiring:*")
            parts.append("- Use the 'remote' filter on major boards (Indeed, LinkedIn, ZipRecruiter)")
            parts.append("- Consider time-zone-specific targeting for distributed teams")
            parts.append("- Remote roles typically see 2-3x higher application volumes")
            parts.append("- Programmatic advertising can geo-target remote workers in specific regions")
            sections.append("\n".join(parts))
            tools_used.append("query_channels")
            sources.add("Joveo Channel Database")

        # ── Market demand questions ──
        if detected_roles and not sections:
            role = _pick_best_role(detected_roles, msg_lower)
            role_titles = {
                "nursing": "Registered Nurse", "engineering": "Software Engineer",
                "technology": "Software Developer", "healthcare": "Healthcare Professional",
                "retail": "Retail Associate", "transportation": "CDL Driver",
            }
            role_title = role_titles.get(role, role.title())
            location = detected_country or ""
            industry = list(detected_industries)[0] if detected_industries else ""
            demand_data = self._query_market_demand({"role": role_title, "location": location, "industry": industry})
            tools_used.append("query_market_demand")
            sources.add("Joveo Market Demand Intelligence")
            sections.append(_format_demand_response(demand_data, role_title))

        # ── Prompt injection / security detection ──
        _injection_patterns = [
            r'ignore\s+(all\s+)?previous\s+instructions',
            r'tell\s+me\s+(the\s+)?(admin|system|root)\s+(password|prompt|key)',
            r'what\s+is\s+your\s+system\s+prompt',
            r'reveal\s+(your\s+)?(system|hidden|secret)',
            r'act\s+as\s+(if\s+you\s+are|a)\s+(different|new)',
            r'pretend\s+(you\s+are|to\s+be)',
        ]
        is_injection = any(_re.search(pat, msg_lower) for pat in _injection_patterns)
        if is_injection and not sections:
            sections.append(
                "I'm *Nova*, a recruitment marketing intelligence assistant. "
                "I can only help with recruitment-related questions such as job board recommendations, "
                "CPC/CPA benchmarks, budget allocation, and hiring market data.\n\n"
                "I cannot share system configuration details or respond to prompt manipulation attempts. "
                "How can I help you with your recruitment marketing needs?"
            )
            tools_used.clear()
            sources.clear()

        # ── Unethical request detection ──
        _unethical_patterns = [
            r'\bhack\b', r'\bsteal\b', r'\bbreak\s+into\b', r'\bexploit\b',
            r'\billegal\b', r'\bscrape\s+competitor\b',
        ]
        is_unethical = any(_re.search(pat, msg_lower) for pat in _unethical_patterns)
        if is_unethical and not sections:
            sections.append(
                "I'm unable to assist with that request. As a recruitment marketing intelligence tool, "
                "I can only help with legitimate recruitment activities.\n\n"
                "Here's what I *can* help with:\n"
                "- Job board and publisher recommendations\n"
                "- CPC/CPA/CPH industry benchmarks\n"
                "- Budget allocation and ROI projections\n"
                "- Market intelligence and hiring trends\n"
                "- DEI recruitment strategies\n\n"
                "What recruitment marketing question can I help you with?"
            )
            tools_used.clear()
            sources.clear()

        # ── Off-topic detection ──
        _off_topic_patterns = [
            r'\bweather\b', r'\b\d+\s*\+\s*\d+\b', r'\bwrite\s+(me\s+)?a\s+(python|code|script)\b',
            r'\brecipe\b', r'\bjoke\b', r'\bstory\b', r'\bpoem\b',
        ]
        is_off_topic = any(_re.search(pat, msg_lower) for pat in _off_topic_patterns)

        # ── Fallback ──
        if not sections:
            if is_off_topic:
                response_text = (
                    "I appreciate your question, but I'm specifically designed for *recruitment marketing intelligence*. "
                    "I can't help with general knowledge questions.\n\n"
                    "Here's what I can help with:\n\n"
                    "- *Job boards and publishers* for specific countries or industries\n"
                    "- *CPC, CPA, and cost-per-hire benchmarks* by industry and platform\n"
                    "- *Budget allocation* recommendations with projected outcomes\n"
                    "- *Salary intelligence* for specific roles and locations\n"
                    "- *DEI recruitment channels* and diversity-focused boards\n"
                    "- *Market trends* in recruitment advertising\n\n"
                    "Try asking something like: _\"What's the average CPC for tech roles?\"_ "
                    "or _\"How should I allocate a $100K hiring budget?\"_"
                )
            else:
                # Try a general knowledge base search
                kb_data = self._query_knowledge_base({"topic": "all"})
                tools_used.append("query_knowledge_base")
                sources.add("Recruitment Industry Knowledge Base")

                response_text = (
                    "I can help you with recruitment marketing intelligence. "
                    "Based on Joveo's data across *10,238+ Supply Partners* in *70+ countries*, "
                    "I can answer questions about:\n\n"
                    "- *Job boards and publishers* for specific countries or industries\n"
                    "- *CPC, CPA, and cost-per-hire benchmarks* by industry and platform\n"
                    "- *Budget allocation* recommendations with projected outcomes\n"
                    "- *Salary intelligence* for specific roles and locations\n"
                    "- *DEI recruitment channels* and diversity-focused boards\n"
                    "- *Market trends* in recruitment advertising\n\n"
                    "Could you rephrase your question with more specifics? "
                    "For example, mention a role, location, industry, or metric."
                )
            sections.append(response_text)

        response = "\n\n".join(sections)
        confidence = _estimate_confidence(tools_used, sources)

        # Lower confidence for fallback/off-topic/injection responses
        if is_off_topic or is_injection or is_unethical:
            confidence = 1.0  # we're confident in our refusal/redirect
        elif not tools_used or (len(tools_used) == 1 and tools_used[0] == "query_knowledge_base" and
                                 "Could you rephrase" in response):
            confidence = round(min(confidence, 0.4), 2)  # generic fallback = lower confidence

        return {
            "response": response,
            "sources": list(sources),
            "confidence": confidence,
            "tools_used": tools_used,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def _resolve_country(name: str) -> Optional[str]:
    """Resolve a country name or alias to its canonical form."""
    if not name:
        return None
    name_lower = name.lower().strip()
    if name_lower in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[name_lower]
    # Try title case match
    title = name.strip().title()
    # Check if it's already a valid country name in our data
    return title if title != "" else None


def _match_industry_key(query: str, available_keys: List[str]) -> Optional[str]:
    """Find the best matching industry key from available options."""
    query_lower = query.lower().strip()
    # Exact match
    if query_lower in available_keys:
        return query_lower
    # Partial match
    for key in available_keys:
        if query_lower in key or key in query_lower:
            return key
    # Keyword match
    for key in available_keys:
        key_parts = key.replace("_", " ").split()
        if any(part in query_lower for part in key_parts):
            return key
    return None


def _match_category_key(query: str, available_keys: List[str]) -> Optional[str]:
    """Find the best matching category key."""
    query_lower = query.lower().strip()
    for key in available_keys:
        if query_lower == key.lower():
            return key
    for key in available_keys:
        if query_lower in key.lower() or key.lower() in query_lower:
            return key
    return None


def _pick_best_role(detected_roles: set, text: str) -> str:
    """Pick the most relevant role from a set of detected roles.

    Uses a priority order (more specific roles first) and checks which role
    keyword appears earliest in the text to break ties.
    """
    if not detected_roles:
        return "general"
    if len(detected_roles) == 1:
        return list(detected_roles)[0]

    # Priority order: more specific roles ranked higher
    priority = [
        "nursing", "healthcare", "executive", "engineering", "technology",
        "construction", "transportation", "education", "finance", "sales",
        "marketing", "retail", "hospitality", "hourly", "remote",
    ]
    # Find which role keyword appears first in the text
    earliest_pos = {}
    for role in detected_roles:
        keywords = _ROLE_KEYWORDS.get(role, [])
        for kw in keywords:
            pos = text.find(kw)
            if pos >= 0:
                if role not in earliest_pos or pos < earliest_pos[role]:
                    earliest_pos[role] = pos

    # Sort by earliest appearance, then by priority
    def sort_key(role):
        pos = earliest_pos.get(role, 9999)
        pri = priority.index(role) if role in priority else 99
        return (pos, pri)

    sorted_roles = sorted(detected_roles, key=sort_key)
    return sorted_roles[0]


def _detect_keywords(text: str, keyword_map: Dict[str, List[str]]) -> set:
    """Detect which keyword categories are present in text."""
    found = set()
    for category, keywords in keyword_map.items():
        for kw in keywords:
            if kw in text:
                found.add(category)
                break
    return found


def _detect_country(text: str) -> Optional[str]:
    """Detect a country name in the text."""
    text_lower = text.lower()
    # Check country aliases (longest first to avoid partial matches)
    sorted_aliases = sorted(_COUNTRY_ALIASES.keys(), key=len, reverse=True)
    for alias in sorted_aliases:
        # Use word boundary check to avoid false matches
        pattern = r'\b' + re.escape(alias) + r'\b'
        if re.search(pattern, text_lower):
            # For short aliases (2 chars like "us", "uk"), require uppercase in
            # original text to avoid false positives on common English words
            # e.g. "help us find" should NOT match "United States"
            if len(alias) <= 2:
                upper_pat = r'\b' + re.escape(alias.upper()) + r'\b'
                if not re.search(upper_pat, text):
                    continue
            return _COUNTRY_ALIASES[alias]
    # Check US state aliases -- return "United States" if a US state is mentioned
    sorted_states = sorted(_US_STATE_ALIASES.keys(), key=len, reverse=True)
    for state_alias in sorted_states:
        if len(state_alias) <= 2:
            # For 2-letter abbrevs, require word boundary and uppercase in original text
            pattern = r'\b' + re.escape(state_alias) + r'\b'
            if re.search(pattern, text_lower):
                # Only match if it's uppercase in original (avoid matching "in", "or", etc.)
                upper_pat = r'\b' + re.escape(state_alias.upper()) + r'\b'
                if re.search(upper_pat, text):
                    return "United States"
        else:
            pattern = r'\b' + re.escape(state_alias) + r'\b'
            if re.search(pattern, text_lower):
                return "United States"
    return None


def _detect_us_state(text: str) -> Optional[str]:
    """Detect a US state name in the text and return the canonical state name."""
    text_lower = text.lower()
    sorted_states = sorted(_US_STATE_ALIASES.keys(), key=len, reverse=True)
    for state_alias in sorted_states:
        if len(state_alias) <= 2:
            pattern = r'\b' + re.escape(state_alias) + r'\b'
            if re.search(pattern, text_lower):
                upper_pat = r'\b' + re.escape(state_alias.upper()) + r'\b'
                if re.search(upper_pat, text):
                    return _US_STATE_ALIASES[state_alias]
        else:
            pattern = r'\b' + re.escape(state_alias) + r'\b'
            if re.search(pattern, text_lower):
                return _US_STATE_ALIASES[state_alias]
    return None


def _extract_budget(text: str) -> float:
    """Extract a dollar budget amount from text."""
    # Match patterns like $50K, $50,000, 50K, 50000, $1M, $1.5M
    patterns = [
        r'\$\s*([\d,.]+)\s*[mM](?:illion)?',     # $1M, $1.5 million
        r'\$\s*([\d,.]+)\s*[kK]',                  # $50K, $50k
        r'([\d,.]+)\s*[mM](?:illion)?\s*(?:dollar|usd|budget)',  # 1M dollars
        r'([\d,.]+)\s*[kK]\s*(?:dollar|usd|budget)',              # 50K dollars
        r'\$\s*([\d,.]+)',                          # $50,000
        r'([\d,.]+)\s*(?:dollar|usd)',             # 50000 dollars
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            num_str = match.group(1).replace(",", "")
            try:
                val = float(num_str)
                if "m" in text[match.start():match.end()].lower():
                    val *= 1_000_000
                elif "k" in text[match.start():match.end()].lower():
                    val *= 1_000
                return val
            except ValueError:
                continue
    return 50000.0  # Default budget


def _estimate_confidence(tools_used: list, sources: set) -> float:
    """Estimate response confidence based on tools and sources used (legacy)."""
    if not tools_used:
        return 0.5
    base = 0.6
    base += min(len(tools_used) * 0.05, 0.2)
    base += min(len(sources) * 0.05, 0.15)
    return round(min(base, 0.95), 2)


def _estimate_confidence_v2(tools_used: list, sources: set, tool_details: list) -> float:
    """Enhanced confidence scoring based on tool call quality.

    Scoring factors:
    - Number of unique tools called (breadth)
    - Number of tools that returned actual data vs errors (reliability)
    - Number of distinct sources cited (corroboration)
    - Whether high-weight sources (government/official) are present
    """
    if not tools_used:
        return 0.5

    unique_tools = set(tools_used)
    successful_calls = sum(1 for d in tool_details if d.get("has_data"))
    total_calls = max(len(tool_details), 1)
    success_rate = successful_calls / total_calls

    # Base score from tool breadth
    breadth_score = min(len(unique_tools) * 0.08, 0.30)

    # Success rate contribution
    success_score = success_rate * 0.25

    # Source diversity contribution
    source_score = min(len(sources) * 0.06, 0.20)

    # High-quality source bonus
    high_quality_sources = {"Joveo Publisher Network", "Recruitment Industry Knowledge Base",
                            "Joveo Budget Allocation Engine", "Joveo Global Supply Intelligence"}
    has_quality = any(s in high_quality_sources for s in sources)
    quality_bonus = 0.10 if has_quality else 0.0

    confidence = 0.40 + breadth_score + success_score + source_score + quality_bonus
    return round(min(confidence, 0.95), 2)


def _build_conversation_memory(history: list) -> str:
    """Extract key entities and context from conversation history.

    Scans previous messages to build a running memory of:
    - Roles mentioned
    - Locations discussed
    - Industries referenced
    - Budget figures
    - Key decisions or preferences expressed

    This helps Claude maintain context across multi-turn conversations.
    """
    roles_mentioned = set()
    locations_mentioned = set()
    industries_mentioned = set()
    budgets_mentioned = []
    key_topics = []

    for msg in history:
        content = msg.get("content", "")
        if not isinstance(content, str):
            continue
        content_lower = content.lower()

        # Detect roles
        for category, keywords in _ROLE_KEYWORDS.items():
            for kw in keywords:
                if kw in content_lower:
                    roles_mentioned.add(category)
                    break

        # Detect locations
        detected_country = _detect_country(content_lower)
        if detected_country:
            locations_mentioned.add(detected_country)
        detected_state = _detect_us_state(content)
        if detected_state:
            locations_mentioned.add(detected_state)

        # Detect industries
        for category, keywords in _INDUSTRY_KEYWORDS.items():
            for kw in keywords:
                if kw in content_lower:
                    industries_mentioned.add(category)
                    break

        # Detect budgets
        budget = _extract_budget(content_lower)
        if budget != 50000.0:  # 50000 is the default, skip it
            budgets_mentioned.append(budget)

    parts = []
    if roles_mentioned:
        parts.append(f"- Roles discussed: {', '.join(sorted(roles_mentioned))}")
    if locations_mentioned:
        parts.append(f"- Locations mentioned: {', '.join(sorted(locations_mentioned))}")
    if industries_mentioned:
        parts.append(f"- Industries: {', '.join(sorted(industries_mentioned))}")
    if budgets_mentioned:
        parts.append(f"- Budget figures: {', '.join(f'${b:,.0f}' for b in budgets_mentioned)}")

    return "\n".join(parts)


def _summarize_enrichment(context: dict) -> str:
    """Create a brief text summary of enrichment context."""
    parts = []
    if context.get("roles"):
        roles = context["roles"]
        if isinstance(roles, list):
            role_names = [r.get("title", str(r)) if isinstance(r, dict) else str(r) for r in roles[:5]]
            parts.append(f"Roles: {', '.join(role_names)}")
    if context.get("locations"):
        locs = context["locations"]
        if isinstance(locs, list):
            loc_names = []
            for loc in locs[:5]:
                if isinstance(loc, dict):
                    loc_names.append(f"{loc.get('city', '')}, {loc.get('state', '')}, {loc.get('country', '')}".strip(", "))
                else:
                    loc_names.append(str(loc))
            parts.append(f"Locations: {', '.join(loc_names)}")
    if context.get("industry"):
        parts.append(f"Industry: {context['industry']}")
    if context.get("budget"):
        parts.append(f"Budget: ${context['budget']:,.0f}")
    if context.get("company_name"):
        parts.append(f"Company: {context['company_name']}")
    if context.get("target_roles"):
        target = context["target_roles"]
        if isinstance(target, list):
            names = [r.get("title", str(r)) if isinstance(r, dict) else str(r) for r in target[:5]]
            parts.append(f"Target Roles: {', '.join(names)}")
    return "\n".join(parts) if parts else "No additional context available."


# ═══════════════════════════════════════════════════════════════════════════════
# RESPONSE FORMATTERS
# ═══════════════════════════════════════════════════════════════════════════════

def _format_supply_response(data: dict, country: str, is_dei: bool = False) -> str:
    """Format global supply data into a readable response."""
    parts = []

    if is_dei:
        dei = data.get("dei_boards", {})
        boards = dei.get("boards", dei.get("global", []))
        if boards:
            parts.append(f"*DEI Job Boards{' for ' + country if country else ''}*\n")
            for b in boards[:10]:
                if isinstance(b, dict):
                    parts.append(f"- *{b.get('name', 'N/A')}* - Focus: {b.get('focus', 'General')} ({b.get('regions', 'Global')})")
                else:
                    parts.append(f"- {b}")
            if len(boards) > 10:
                parts.append(f"\n_...and {len(boards) - 10} more DEI boards available_")
        return "\n".join(parts)

    cb = data.get("country_boards", {})
    if cb and "boards" in cb:
        parts.append(f"*Job Boards in {cb.get('country', country)}*\n")
        parts.append(f"*Monthly Spend*: {cb.get('monthly_spend', 'N/A')}")
        parts.append(f"*Key Metros*: {', '.join(cb.get('key_metros', []))}\n")

        # Group by tier
        boards = cb["boards"]
        tiers = {}
        for b in boards:
            tier = b.get("tier", "Other")
            tiers.setdefault(tier, []).append(b)

        for tier in ["Tier 1", "Tier 2", "Niche", "Govt"]:
            if tier in tiers:
                parts.append(f"*{tier}:*")
                for b in tiers[tier]:
                    parts.append(f"- {b['name']} ({b.get('billing', 'N/A')}) - {b.get('category', 'General')}")
                parts.append("")

    elif "available_countries" in data:
        parts.append("*Available Countries in Joveo's Global Supply Data*\n")
        countries = data["available_countries"]
        parts.append(f"We have job board data for *{len(countries)} countries*: {', '.join(countries[:15])}{'...' if len(countries) > 15 else ''}")

    return "\n".join(parts) if parts else "No supply data available for this query."


def _format_publisher_response(data: dict) -> str:
    """Format publisher network data into a readable response."""
    parts = []
    total = data.get("total_active_publishers", 0)

    if "search_results" in data:
        matches = data["search_results"]
        parts.append(f"*Publisher Search Results ({data.get('match_count', 0)} matches)*\n")
        for m in matches[:15]:
            parts.append(f"- *{m['name']}* (Category: {m['category']})")
    elif "publishers" in data:
        pubs = data["publishers"]
        label = data.get("country", data.get("category", ""))
        parts.append(f"*Joveo Publishers{' in ' + label if label else ''} ({data.get('count', len(pubs))} publishers)*\n")
        for p in pubs[:15]:
            parts.append(f"- {p}")
        if len(pubs) > 15:
            parts.append(f"\n_...and {len(pubs) - 15} more publishers_")
    elif "categories" in data:
        parts.append(f"*Joveo Publisher Network Overview*\n")
        parts.append(f"*Total Active Publishers*: {total:,}\n")
        cats = data["categories"]
        for cat, count in sorted(cats.items(), key=lambda x: x[1], reverse=True)[:12]:
            parts.append(f"- *{cat}*: {count} publishers")

    return "\n".join(parts) if parts else ""


def _format_channel_response(data: dict, industry: str) -> str:
    """Format channel data into a readable response."""
    parts = []
    parts.append("*Recruitment Channels*\n")

    if "niche_industry_channels" in data:
        nic = data["niche_industry_channels"]
        parts.append(f"*Niche Channels for {nic.get('industry', industry)}:*")
        for ch in nic.get("channels", [])[:12]:
            parts.append(f"- {ch}")
        parts.append("")

    if "regional_local" in data:
        parts.append(f"*Regional/Local Boards* ({len(data['regional_local'])} channels):")
        for ch in data["regional_local"][:8]:
            parts.append(f"- {ch}")
        parts.append("")

    if "global_reach" in data:
        parts.append(f"*Global Reach* ({len(data['global_reach'])} channels):")
        for ch in data["global_reach"][:8]:
            parts.append(f"- {ch}")

    return "\n".join(parts) if parts else "No channel data available."


def _format_benchmark_response(data: dict, metric: str, industry: str) -> str:
    """Format benchmark data into a readable response."""
    parts = []
    bm = data.get("benchmarks", {})

    # When no specific metric is requested, show a summary of available benchmark categories
    if "benchmark_categories" in data and not bm:
        categories = data["benchmark_categories"]
        parts.append("*Recruitment Advertising Benchmarks Overview*\n")
        parts.append("Joveo's knowledge base covers the following benchmark categories:\n")
        cat_descriptions = {
            "cost_per_click": "CPC benchmarks by platform (Indeed, LinkedIn, Google, Meta, etc.)",
            "cost_per_application": "CPA benchmarks by industry and platform",
            "apply_rates": "Application conversion rates (clicks to applications)",
            "cost_per_hire": "Total cost-per-hire benchmarks (SHRM data)",
            "time_to_fill": "Average days to fill positions",
            "source_of_hire": "Percentage of hires from each channel",
            "applicants_per_opening": "Average applicants per job opening",
            "conversion_rates": "Funnel conversion rates (impression to hire)",
        }
        for cat in categories:
            desc = cat_descriptions.get(cat, "")
            nice_name = cat.replace("_", " ").title()
            parts.append(f"- *{nice_name}*: {desc}" if desc else f"- *{nice_name}*")
        parts.append("\nAsk about a specific metric for detailed data (e.g., _\"What is the average CPC?\"_)")
        return "\n".join(parts)

    if not bm or "message" in bm:
        # Try industry benchmarks
        ind_bm = data.get("industry_benchmarks", {})
        if ind_bm and "message" not in ind_bm:
            parts.append(f"*Industry Benchmarks*\n")
            for ind_key, ind_data in ind_bm.items():
                parts.append(f"*{ind_key.replace('_', ' ').title()}:*")
                if isinstance(ind_data, dict):
                    for k, v in list(ind_data.items())[:8]:
                        parts.append(f"- {k.replace('_', ' ').title()}: {v}")
                parts.append("")
            return "\n".join(parts)
        parts.append("No specific benchmark data found. ")
        parts.append("Available metrics: CPC, CPA, Cost per Hire, Apply Rate, Time to Fill.")
        return "\n".join(parts)

    for bm_key, bm_data in bm.items():
        nice_key = bm_key.replace("_", " ").title()
        parts.append(f"*{nice_key} Benchmarks*\n")

        if isinstance(bm_data, dict):
            desc = bm_data.get("description", "")
            if desc:
                parts.append(f"_{desc}_\n")

            # Format platform-specific data
            if "by_platform" in bm_data:
                parts.append("*By Platform:*")
                for plat, plat_data in bm_data["by_platform"].items():
                    if isinstance(plat_data, dict):
                        key_val = ""
                        for k in ["average_cpc_range", "job_ad_cpc_range", "average_cpc",
                                   "model", "starting_price", "median_cpc_peak_nov_2025"]:
                            if k in plat_data:
                                key_val = f"{plat_data[k]}"
                                break
                        parts.append(f"- *{plat.replace('_', ' ').title()}*: {key_val}")

            # Format report data
            for rkey in ["appcast_2025_report", "appcast_2026_report", "shrm_2025", "shrm_2026",
                         "google_ads_benchmark", "joveo_historical"]:
                if rkey in bm_data:
                    rdata = bm_data[rkey]
                    parts.append(f"\n*{rkey.replace('_', ' ').title()}:*")
                    if isinstance(rdata, dict):
                        for k, v in list(rdata.items())[:6]:
                            if k not in ("year", "dataset"):
                                parts.append(f"- {k.replace('_', ' ').title()}: {v}")

        parts.append("")

    # Add industry-specific data if available
    if industry:
        ind_bm = data.get("industry_benchmarks", {})
        for ind_key, ind_data in ind_bm.items():
            parts.append(f"\n*Industry-Specific: {ind_key.replace('_', ' ').title()}*\n")
            if isinstance(ind_data, dict):
                for k, v in list(ind_data.items())[:8]:
                    parts.append(f"- {k.replace('_', ' ').title()}: {v}")

    return "\n".join(parts)


def _format_salary_response(data: dict) -> str:
    """Format salary data into a readable response."""
    parts = []
    parts.append(f"*Salary Intelligence: {data.get('role', 'N/A')}*\n")
    parts.append(f"*Location*: {data.get('location', 'National')}")
    parts.append(f"*Role Tier*: {data.get('role_tier', 'N/A')}")
    parts.append(f"*Estimated Range*: {data.get('salary_range_estimate', 'N/A')}")
    if data.get("notes"):
        parts.append(f"_{data['notes']}_")

    return "\n".join(parts)


def _format_budget_response(data: dict, budget: float) -> str:
    """Format budget projection data into a readable response."""
    parts = []
    parts.append(f"*Budget Allocation: ${budget:,.0f}*\n")

    if "channel_allocations" in data:
        allocs = data["channel_allocations"]
        parts.append("*Channel Spend Breakdown:*\n")
        for ch_name, ch_data in allocs.items():
            spend = ch_data.get("dollar_amount", ch_data.get("dollars", ch_data.get("spend", 0)))
            clicks = ch_data.get("projected_clicks", 0)
            apps = ch_data.get("projected_applications", 0)
            parts.append(f"- *{ch_name}*: ${spend:,.0f} | Clicks: {clicks:,.0f} | Applications: {apps:,.0f}")

        total = data.get("total_projected", {})
        if total:
            parts.append(f"\n*Projected Totals:*")
            parts.append(f"- Total Clicks: {total.get('clicks', 0):,.0f}")
            parts.append(f"- Total Applications: {total.get('applications', 0):,.0f}")
            parts.append(f"- Projected Hires: {total.get('hires', 0):,.0f}")
            cph_val = total.get("cost_per_hire", 0)
            if cph_val:
                parts.append(f"- Estimated Cost per Hire: ${cph_val:,.0f}")

    elif "estimated_allocation" in data:
        allocs = data["estimated_allocation"]
        parts.append("*Estimated Channel Allocation:*\n")
        for ch_name, ch_data in allocs.items():
            nice_name = ch_name.replace("_", " ").title()
            parts.append(f"- *{nice_name}*: ${ch_data['amount']:,.0f} ({ch_data['pct']}%)")

    recs = data.get("recommendations", [])
    if recs:
        parts.append("\n*Optimization Recommendations:*")
        for rec in recs[:4]:
            if isinstance(rec, str):
                parts.append(f"- {rec}")
            elif isinstance(rec, dict):
                parts.append(f"- {rec.get('recommendation', rec.get('message', str(rec)))}")

    return "\n".join(parts)


def _format_dei_response(data: dict, country: str) -> str:
    """Format DEI board data."""
    return _format_supply_response(data, country, is_dei=True)


def _format_trend_response(data: dict) -> str:
    """Format trend data into a readable response."""
    parts = []
    parts.append("*Recruitment Market Trends (2025-2026)*\n")

    summaries = data.get("trend_summaries", {})
    for tk, tv in list(summaries.items())[:6]:
        parts.append(f"*{tv.get('title', tk.replace('_', ' ').title())}*")
        desc = tv.get("description", "")
        if desc:
            parts.append(f"{desc}\n")

    return "\n".join(parts) if parts else "No trend data available."


def _format_demand_response(data: dict, role: str) -> str:
    """Format market demand data."""
    parts = []
    parts.append(f"*Market Demand: {role}*\n")

    apo = data.get("applicants_per_opening", {})
    if apo:
        icims = apo.get("icims_2025", {})
        if icims:
            parts.append(f"*Applicants per Opening*: {icims.get('ratio', 'N/A')} (iCIMS 2025)")

    soh = data.get("source_of_hire", {})
    if soh:
        parts.append("\n*Source of Hire Breakdown:*")
        parts.append(f"- Job Boards: {soh.get('job_boards_usage', 'N/A')}")
        parts.append(f"- Referrals: {soh.get('referrals_usage', 'N/A')}")
        parts.append(f"- Career Sites: {soh.get('career_sites_usage', 'N/A')}")
        parts.append(f"- LinkedIn: {soh.get('linkedin_usage', 'N/A')}")

    ind = data.get("industry_demand", {})
    if ind:
        parts.append(f"\n*Industry Demand ({ind.get('industry', 'N/A')}):*")
        parts.append(f"- Hiring Strength: {ind.get('hiring_strength', 'N/A')}")
        parts.append(f"- Recruitment Difficulty: {ind.get('recruitment_difficulty', 'N/A')}")

    return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# HTTP HANDLER
# ═══════════════════════════════════════════════════════════════════════════════

# Module-level singleton (double-checked locking for thread safety)
_nova_instance: Optional[Nova] = None
_nova_init_lock = threading.Lock()


def _get_iq() -> Nova:
    """Get or create the Nova singleton (thread-safe)."""
    global _nova_instance
    if _nova_instance is None:
        with _nova_init_lock:
            if _nova_instance is None:
                _nova_instance = Nova()
    return _nova_instance


def handle_chat_request(request_data: dict) -> dict:
    """Handle an incoming chat API request.

    Expected request format::

        {
            "message": "What's the average CPA for nursing roles in Texas?",
            "conversation_id": "optional-session-id",
            "history": [{"role": "user", "content": "..."}, ...],
            "context": {
                "roles": [...],
                "locations": [...],
                "industry": "...",
                "enriched": {...},
                "synthesized": {...}
            }
        }

    Returns::

        {
            "response": "Based on Joveo's data...",
            "sources": ["Joveo Publisher Network", "Recruitment Industry KB"],
            "confidence": 0.85,
            "tools_used": ["query_publishers", "query_knowledge_base"]
        }
    """
    if not isinstance(request_data, dict):
        return {
            "response": "Invalid request format.",
            "sources": [],
            "confidence": 0.0,
            "tools_used": [],
            "error": "Request must be a JSON object",
        }

    message = (request_data.get("message") or "").strip()
    if not message:
        return {
            "response": "Please provide a message.",
            "sources": [],
            "confidence": 0.0,
            "tools_used": [],
            "error": "No message provided",
        }

    history = request_data.get("history", [])
    context = request_data.get("context")

    iq = _get_iq()

    try:
        result = iq.chat(
            user_message=message,
            conversation_history=history if isinstance(history, list) else [],
            enrichment_context=context if isinstance(context, dict) else None,
        )
        return result
    except Exception as e:
        logger.error("Chat request failed: %s", e, exc_info=True)
        return {
            "response": "I encountered an error processing your question. Please try again.",
            "sources": [],
            "confidence": 0.0,
            "tools_used": [],
            "error": "Internal error processing request",
        }


def get_nova_metrics() -> Dict[str, Any]:
    """Return Nova chatbot metrics snapshot for the health/metrics endpoint."""
    return _nova_metrics.snapshot()
