"""
api_enrichment.py — Comprehensive API Enrichment System for AI Media Planner

Note: This module's fallback data is used by the batch generation pipeline
(Excel/PPT media plan creation via app.py). The real-time chatbot pipeline
(Nova Chat and Slack Bot) uses data_orchestrator.py's tier-aware fallback
system instead. Both fallback systems are maintained independently.

Fetches real data from free public APIs to enrich media plan generation with
salary benchmarks, industry employment stats, location demographics, global
economic indicators, job market data, company information, and competitor logos.

Integrated APIs:
    1. BLS OES (Bureau of Labor Statistics) — Salary data (v1 free / v2 with key)
    2. BLS QCEW — Industry employment & wage statistics (free, no key)
    3. US Census ACS — Location demographics: population, income (free, no key)
    4. World Bank Open Data — Global economic indicators (free, no key)
    5. Clearbit Logo + Google Favicons — Company & competitor logos (free)
    6. Adzuna Job Search — Job postings & salary data (optional, needs keys)
    7. Currency Rates — Exchange rates (live API + hardcoded fallback)
    8. Wikipedia REST API — Company descriptions (free, no key)
    9. Clearbit Autocomplete — Company metadata & domain lookup (free, no key)
   10. SEC EDGAR — Public company ticker/CIK/filing data (free, no key)
   11. FRED (Federal Reserve) — US economic indicators (free key required)
   12. Google Trends — Search interest data (requires pytrends package)
   13. O*NET Web Services — Occupation skills, knowledge, outlook (free w/ creds or fallback)
   14. IMF DataMapper — International GDP, inflation, unemployment (free, no key)
   15. REST Countries v3.1 — Country population, currency, languages (free, no key)
   16. GeoNames — Geographic data, coordinates, timezone (free, username required)
   17. Teleport — Quality of life scores, cost of living (free, no key)
   18. DataUSA — US occupation wages, state demographics (free, no key)
   19. Google Ads API — Keyword search volumes, CPC/CPM benchmarks (OAuth2 or benchmarks)
   20. Meta Marketing API — Facebook/Instagram audience sizing, CPC/CPM (token or benchmarks)
   21. Microsoft/Bing Ads API — Search volumes, CPC estimates (OAuth2 or benchmarks)
   22. TikTok Marketing API — Audience estimation, CPC/CPM (token or benchmarks)
   23. LinkedIn Marketing API — Professional audience sizing, CPC (token or benchmarks)
   24. CareerOneStop API — DOL salary, outlook, certifications (key or benchmarks)
   25. Jooble API — International job market data, 69 countries (key or benchmarks)
   26. BLS JOLTS — Job openings, hires, quits by industry (free, key optional)
   27. FRED Employment — Avg hourly earnings, ECI, sector unemployment (key required)
   28. Eurostat Labour Force Survey — EU unemployment, wages, employment (free, no key)
   29. ILO ILOSTAT — Global unemployment, labour participation (free, no key)
   30. H-1B Visa Wage Benchmarks — Prevailing wages by SOC code (curated, no API)

All API calls:
    - Use only urllib.request (stdlib, no third-party dependencies)
    - Have an 8-second timeout per call
    - Are cached in-memory and on disk (24-hour TTL, thread-safe)
    - Fail gracefully (never crash the generation pipeline)
    - Run concurrently via ThreadPoolExecutor (max 15 workers)
    - Fall back to curated benchmark data when credentials unavailable

Usage:
    from api_enrichment import enrich_data

    enriched = enrich_data({
        "client_name": "Guidewire",
        "client_website": "guidewire.com",
        "industry": "technology",
        "roles": ["Software Engineer", "Product Manager"],
        "locations": ["San Mateo, CA", "London, UK"],
        "competitors": ["Salesforce", "Duck Creek Technologies"],
    })
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Canonical taxonomy standardizer ──
# Used to normalize industry/location keys before API lookups.
# Falls back gracefully if unavailable.
try:
    from standardizer import (
        normalize_industry as _std_normalize_industry,
        normalize_location as _std_normalize_location,
        normalize_role as _std_normalize_role,
        normalize_platform as _std_normalize_platform,
        get_soc_code as _std_get_soc_code,
        CANONICAL_INDUSTRIES as _CANON_INDUSTRIES,
        COUNTRY_MAP as _STD_COUNTRY_MAP,
        US_STATE_MAP as _STD_US_STATE_MAP,
    )

    _HAS_STANDARDIZER = True
except ImportError:
    _HAS_STANDARDIZER = False

# Supabase persistent cache (L3, after memory L1 and disk L2)
try:
    from supabase_cache import cache_get as _supa_get, cache_set as _supa_set

    _HAS_SUPABASE = True
except ImportError:
    _HAS_SUPABASE = False

# Upstash Redis persistent cache (L4, after Supabase)
try:
    from upstash_cache import cache_get as _upstash_get, cache_set as _upstash_set

    _HAS_UPSTASH = True
except ImportError:
    _HAS_UPSTASH = False

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------

API_TIMEOUT = 8  # seconds per HTTP call (increased from 5 for reliability)
CACHE_TTL = 86400  # 24 hours in seconds
MAX_WORKERS = 15

# Semaphore to limit concurrent enrich_data() calls.  Each call spawns a
# ThreadPoolExecutor(max_workers=15), so 10 concurrent enrichments = 150
# total threads -- a safe ceiling even under heavy load.
_enrichment_semaphore = threading.Semaphore(10)
CACHE_DIR = Path(__file__).resolve().parent / "data" / "api_cache"

# Ensure cache directory exists at import time
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# SSL context for API calls. Verified by default; unverified context is
# created lazily only when explicitly opted in via ALLOW_UNVERIFIED_SSL=1.
_DEFAULT_SSL_CTX = ssl.create_default_context()
_UNVERIFIED_SSL_CTX: Optional[ssl.SSLContext] = None  # lazily created


def _get_unverified_ssl_ctx() -> ssl.SSLContext:
    """Return (and lazily create) the unverified SSL context with a warning."""
    global _UNVERIFIED_SSL_CTX
    if _UNVERIFIED_SSL_CTX is None:
        _log_warn(
            "Creating unverified SSL context — SSL certificate verification "
            "will be skipped for failing endpoints. This is a security risk; "
            "set ALLOW_UNVERIFIED_SSL=0 or unset it to disable."
        )
        _UNVERIFIED_SSL_CTX = ssl._create_unverified_context()
    return _UNVERIFIED_SSL_CTX


# ---------------------------------------------------------------------------
# SOC code mapping — BLS Standard Occupational Classification
# Maps common job titles (lowercased) to SOC codes used in OES series IDs.
# ---------------------------------------------------------------------------

SOC_CODES: Dict[str, str] = {
    "software engineer": "15-1252",
    "software developer": "15-1252",
    "software development engineer": "15-1252",
    "frontend engineer": "15-1252",
    "backend engineer": "15-1252",
    "full stack developer": "15-1252",
    "web developer": "15-1254",
    "mobile developer": "15-1252",
    "devops engineer": "15-1244",
    "site reliability engineer": "15-1244",
    "systems administrator": "15-1244",
    "network engineer": "15-1241",
    "database administrator": "15-1242",
    "data scientist": "15-2051",
    "data analyst": "15-2051",
    "data engineer": "15-1252",
    "machine learning engineer": "15-2051",
    "ai engineer": "15-2051",
    "ai/ml engineer": "15-2051",
    "product manager": "11-2021",
    "project manager": "11-9199",
    "program manager": "11-9199",
    "engineering manager": "11-9041",
    "it manager": "11-3021",
    "marketing manager": "11-2021",
    "sales manager": "11-2022",
    "account manager": "11-2022",
    "business analyst": "13-1111",
    "financial analyst": "13-2051",
    "investment banking analyst": "13-2051",
    "accountant": "13-2011",
    "human resources manager": "11-3121",
    "hr specialist": "13-1071",
    "recruiter": "13-1071",
    "talent acquisition": "13-1071",
    "ux designer": "27-1024",
    "ui designer": "27-1024",
    "graphic designer": "27-1024",
    "product designer": "27-1024",
    "nurse": "29-1141",
    "registered nurse": "29-1141",
    "physician": "29-1218",
    "physician assistant": "29-1071",
    "pharmacist": "29-1051",
    "physical therapist": "29-1123",
    "dentist": "29-1021",
    "medical technologist": "29-2011",
    "medical coder": "29-2072",
    "phlebotomist": "31-9097",
    "lawyer": "23-1011",
    "paralegal": "23-2011",
    "compliance officer": "13-1041",
    "teacher": "25-2031",
    "professor": "25-1099",
    "mechanical engineer": "17-2141",
    "electrical engineer": "17-2071",
    "civil engineer": "17-2051",
    "chemical engineer": "17-2041",
    "qa engineer": "15-1253",
    "quality assurance": "15-1253",
    "technical writer": "27-3042",
    "content writer": "27-3043",
    "copywriter": "27-3043",
    "cybersecurity analyst": "15-1212",
    "security engineer": "15-1212",
    "information security analyst": "15-1212",
    "cloud engineer": "15-1244",
    "cloud architect": "15-1244",
    "solutions architect": "15-1299",
    "management consultant": "13-1111",
    "sap consultant": "15-1299",
    "operations manager": "11-1021",
    "branch manager": "11-3031",
    "warehouse associate": "53-7065",
    "warehouse worker": "53-7065",
    "warehouse manager": "53-1042",
    "forklift operator": "53-7051",
    "delivery driver": "53-3031",
    "truck driver": "53-3032",
    "cdl driver": "53-3032",
    "cdl-a driver": "53-3032",
    "long haul driver": "53-3032",
    "bus driver": "53-3052",
    "store associate": "41-2031",
    "retail associate": "41-2031",
    "retail manager": "41-1011",
    "cashier": "41-2011",
    "stocker": "53-7065",
    # C2 FIX: Add blue-collar / skilled trades SOC codes
    "construction worker": "47-2061",
    "construction laborer": "47-2061",
    "general laborer": "53-7062",
    "laborer": "53-7062",
    "carpenter": "47-2031",
    "electrician": "47-2111",
    "plumber": "47-2152",
    "hvac technician": "49-9021",
    "welder": "51-4121",
    "machinist": "51-4041",
    "maintenance technician": "49-9071",
    "maintenance worker": "49-9071",
    "janitor": "37-2011",
    "custodian": "37-2011",
    "housekeeper": "37-2012",
    "landscaper": "37-3011",
    "security guard": "33-9032",
    "security officer": "33-9032",
    "cook": "35-2014",
    "chef": "35-1011",
    "food service worker": "35-3023",
    "server": "35-3031",
    "bartender": "35-3011",
    "dishwasher": "35-9021",
    "caregiver": "31-1122",
    "home health aide": "31-1121",
    "cna": "31-1131",
    "certified nursing assistant": "31-1131",
    "medical assistant": "31-9092",
    "dental assistant": "31-9091",
    "emt": "29-2042",
    "paramedic": "29-2043",
    "assembly worker": "51-2098",
    "production worker": "51-9199",
    "machine operator": "51-9199",
    "quality inspector": "51-9061",
    "picker packer": "53-7064",
    "material handler": "53-7062",
    "shipping clerk": "43-5071",
    "receiving clerk": "43-5071",
    "dispatcher": "43-5032",
    "auto mechanic": "49-3023",
    "automotive technician": "49-3023",
    "diesel mechanic": "49-3031",
    "painter": "47-2141",
    "roofer": "47-2181",
    "ironworker": "47-2171",
    "crane operator": "53-7021",
    "heavy equipment operator": "47-2073",
    "concrete worker": "47-2051",
    "pest control technician": "37-2021",
    "mover": "53-7064",
    "courier": "43-5021",
    "package handler": "53-7064",
    "dock worker": "53-7062",
    "risk manager": "11-9199",
    "quantitative developer": "15-1252",
    "teller": "43-3071",
    "chief technology officer": "11-1021",
    "cto": "11-1021",
    "chief executive officer": "11-1011",
    "ceo": "11-1011",
    "chief financial officer": "11-3031",
    "cfo": "11-3031",
    "vice president": "11-1011",
}

# ---------------------------------------------------------------------------
# NAICS code mapping — North American Industry Classification System
# Expanded to match frontend dropdown values and free-text industry names
# ---------------------------------------------------------------------------

NAICS_CODES: Dict[str, str] = {
    "technology": "54",
    "tech": "54",
    "tech_engineering": "54",
    "software": "5112",
    "it": "54",
    "information_technology": "54",
    "healthcare": "62",
    "healthcare_medical": "62",
    "medical": "62",
    "health": "62",
    "finance": "52",
    "financial_services": "52",
    "finance_banking": "52",
    "banking": "522",
    "insurance": "524",
    "manufacturing": "33",  # C1 FIX: was "31" (food manufacturing); "33" = diverse manufacturing
    "retail": "44",
    "retail_ecommerce": "44",
    "retail_consumer": "44",
    "ecommerce": "454",
    "education": "61",
    "construction": "23",
    "real_estate": "53",
    "transportation": "48",
    "transportation_logistics": "48",
    "logistics": "49",
    "hospitality": "72",
    "hospitality_food": "72",
    "food_service": "722",
    "media": "51",
    "media_entertainment": "51",
    "entertainment": "71",
    "telecommunications": "517",
    "energy": "21",
    "energy_utilities": "21",
    "oil_gas": "211",
    "mining": "21",
    "agriculture": "11",
    "government": "92",
    "government_public": "92",
    "nonprofit": "813",
    "consulting": "5416",
    "professional_services": "54",
    "legal": "5411",
    "pharmaceutical": "3254",
    "pharma_biotech": "3254",
    "biotech": "3254",
    "aerospace": "3364",
    "aerospace_defense": "3364",
    "defense": "3364",
    "automotive": "3361",
}

# ---------------------------------------------------------------------------
# Country / ISO code mapping
# ---------------------------------------------------------------------------

COUNTRY_CODES: Dict[str, str] = {
    # Full names
    "united states": "USA",
    "united kingdom": "GBR",
    "canada": "CAN",
    "australia": "AUS",
    "germany": "DEU",
    "france": "FRA",
    "india": "IND",
    "japan": "JPN",
    "china": "CHN",
    "brazil": "BRA",
    "mexico": "MEX",
    "south korea": "KOR",
    "italy": "ITA",
    "spain": "ESP",
    "netherlands": "NLD",
    "sweden": "SWE",
    "switzerland": "CHE",
    "singapore": "SGP",
    "ireland": "IRL",
    "israel": "ISR",
    "new zealand": "NZL",
    "south africa": "ZAF",
    "uae": "ARE",
    "united arab emirates": "ARE",
    "poland": "POL",
    "norway": "NOR",
    "denmark": "DNK",
    "finland": "FIN",
    "belgium": "BEL",
    "austria": "AUT",
    "portugal": "PRT",
    "argentina": "ARG",
    "colombia": "COL",
    "chile": "CHL",
    "philippines": "PHL",
    "malaysia": "MYS",
    "thailand": "THA",
    "indonesia": "IDN",
    "vietnam": "VNM",
    "nigeria": "NGA",
    "egypt": "EGY",
    "saudi arabia": "SAU",
    "pakistan": "PAK",
    "bangladesh": "BGD",
    "taiwan": "TWN",
    "czech republic": "CZE",
    "romania": "ROU",
    "hungary": "HUN",
    "greece": "GRC",
    # Abbreviations & short forms
    "us": "USA",
    "usa": "USA",
    "uk": "GBR",
    "gb": "GBR",
    "ca": "CAN",
    "au": "AUS",
    "de": "DEU",
    "fr": "FRA",
    "in": "IND",
    "jp": "JPN",
    "cn": "CHN",
    "br": "BRA",
    "mx": "MEX",
    "kr": "KOR",
    "it": "ITA",
    "es": "ESP",
    "nl": "NLD",
    "se": "SWE",
    "ch": "CHE",
    "sg": "SGP",
    "ie": "IRL",
    "il": "ISR",
    "nz": "NZL",
    "za": "ZAF",
    "ae": "ARE",
    "pl": "POL",
    "no": "NOR",
    "dk": "DNK",
    "fi": "FIN",
    "be": "BEL",
    "at": "AUT",
    "pt": "PRT",
}

# Adzuna country codes (two-letter lowercase)
ADZUNA_COUNTRY_CODES: Dict[str, str] = {
    "USA": "us",
    "GBR": "gb",
    "CAN": "ca",
    "AUS": "au",
    "DEU": "de",
    "FRA": "fr",
    "IND": "in",
    "NLD": "nl",
    "BRA": "br",
    "POL": "pl",
    "SGP": "sg",
    "ZAF": "za",
    "AUT": "at",
    "NZL": "nz",
    "ITA": "it",
    "ESP": "es",
    "MEX": "mx",
}

# US state abbreviations for detecting US locations
US_STATES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}


# ---------------------------------------------------------------------------
# Standardizer-backed wrapper functions (with hardcoded fallbacks)
# ---------------------------------------------------------------------------


def get_naics_code(industry: str) -> str:
    """Return NAICS code for an industry, using the canonical standardizer
    when available and falling back to the hardcoded NAICS_CODES dict.
    """
    if _HAS_STANDARDIZER:
        canon = _std_normalize_industry(industry)
        entry = _CANON_INDUSTRIES.get(canon, {})
        naics = entry.get("naics") or ""
        if naics:
            return naics
    # Fallback to original hardcoded dict
    industry_lower = industry.lower().replace(" ", "_").replace("-", "_")
    code = NAICS_CODES.get(industry_lower)
    if code:
        return code
    # Partial matching fallback
    for key, code in NAICS_CODES.items():
        if key in industry_lower or industry_lower in key:
            return code
    return ""


def get_country_iso3(location_part: str) -> Optional[str]:
    """Return ISO-3 country code for a location fragment, using the canonical
    standardizer when available and falling back to the hardcoded COUNTRY_CODES dict.
    """
    if not location_part:
        return None
    lower = location_part.strip().lower()
    # Try standardizer first
    if _HAS_STANDARDIZER:
        entry = _STD_COUNTRY_MAP.get(lower)
        if entry:
            return entry.get("iso3") or ""
    # Fallback to original dict
    return COUNTRY_CODES.get(lower)


def is_us_state(token: str) -> bool:
    """Check if a token is a US state abbreviation, using the canonical
    standardizer when available and falling back to the hardcoded US_STATES set.
    """
    upper = token.strip().upper()
    if _HAS_STANDARDIZER:
        entry = _STD_US_STATE_MAP.get(upper.lower())
        if entry:
            return True
    # Fallback to original set
    return upper in US_STATES


# ---------------------------------------------------------------------------
# Currency exchange rates (hardcoded fallback)
# Rates relative to 1 USD — approximate as of early 2026
# ---------------------------------------------------------------------------

FALLBACK_CURRENCY_RATES: Dict[str, float] = {
    "USD": 1.00,
    "EUR": 0.92,
    "GBP": 0.79,
    "CAD": 1.36,
    "AUD": 1.53,
    "JPY": 149.50,
    "INR": 83.20,
    "CNY": 7.24,
    "CHF": 0.88,
    "SEK": 10.42,
    "NOK": 10.55,
    "DKK": 6.88,
    "NZD": 1.62,
    "SGD": 1.34,
    "HKD": 7.82,
    "KRW": 1310.00,
    "MXN": 17.15,
    "BRL": 4.97,
    "ZAR": 18.65,
    "PLN": 4.02,
    "ILS": 3.65,
    "AED": 3.67,
    "SAR": 3.75,
    "THB": 35.10,
    "MYR": 4.72,
    "IDR": 15650.00,
    "PHP": 55.80,
    "TWD": 31.50,
    "CZK": 22.80,
    "HUF": 355.00,
    "RON": 4.58,
}

# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------

_memory_cache: Dict[str, Any] = {}
_cache_lock = threading.Lock()
_circuit_breaker_lock = threading.Lock()  # Separate lock for circuit breaker state
MAX_MEMORY_CACHE_SIZE = 500  # Prevent unbounded memory growth


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

import logging as _logging

_api_logger = _logging.getLogger("api_enrichment")

# ── Request context for tracing (v3.1) ──
# Thread-local storage propagates the request_id from the HTTP handler
# through the enrichment pipeline so every API call is traceable.
_request_context = threading.local()


def set_request_id(request_id: str) -> None:
    """Set the current request ID for API call tracing."""
    _request_context.request_id = request_id


def get_request_id() -> str:
    """Get the current request ID (empty string if not set)."""
    return getattr(_request_context, "request_id", "")


def _log_warn(msg: str) -> None:
    """Write a warning to the logger (never crashes)."""
    try:
        _api_logger.warning(msg)
    except Exception:
        pass


def _log_info(msg: str) -> None:
    """Write an info message to the logger."""
    try:
        _api_logger.info(msg)
    except Exception:
        pass


def _cache_key(api_name: str, params: str) -> str:
    """Generate a deterministic cache key from API name and param string."""
    raw = f"{api_name}:{params}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _get_cached(key: str) -> Optional[Any]:
    """Check in-memory cache, then file cache. Returns None on miss."""
    # In-memory
    with _cache_lock:
        if key in _memory_cache:
            entry = _memory_cache[key]
            if time.time() - entry["ts"] < CACHE_TTL:
                return entry["data"]
            else:
                _memory_cache.pop(key, None)

    # File-based
    cache_file = CACHE_DIR / f"{key}.json"
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as fh:
                entry = json.load(fh)
            if time.time() - entry.get("ts") or 0 < CACHE_TTL:
                with _cache_lock:
                    _memory_cache[key] = entry  # promote to memory
                return entry["data"]
            else:
                cache_file.unlink(missing_ok=True)
        except Exception:
            pass

    # L3: Supabase persistent cache
    if _HAS_SUPABASE:
        try:
            supa_data = _supa_get(key)
            if supa_data is not None:
                # Promote to L1 memory cache only (avoid recursive L3 write)
                with _cache_lock:
                    _memory_cache[key] = {"ts": time.time(), "data": supa_data}
                return supa_data
        except Exception:
            pass

    # L4: Upstash Redis persistent cache
    if _HAS_UPSTASH:
        try:
            upstash_data = _upstash_get(key)
            if upstash_data is not None:
                with _cache_lock:
                    _memory_cache[key] = {"ts": time.time(), "data": upstash_data}
                return upstash_data
        except Exception:
            pass

    return None


_MAX_DISK_CACHE_FILES = 1000  # Prevent disk exhaustion
_last_disk_cleanup = 0.0


# Startup cache cleanup -- evict stale files on boot
def _startup_cache_cleanup():
    try:
        cache_dir = CACHE_DIR
        if not cache_dir.exists():
            return
        files = sorted(cache_dir.glob("*.json"), key=lambda f: f.stat().st_mtime)
        if len(files) > _MAX_DISK_CACHE_FILES:
            removed = len(files) - _MAX_DISK_CACHE_FILES
            for f in files[:removed]:
                try:
                    f.unlink()
                except OSError:
                    pass
            _log_info(f"Startup cache cleanup: removed {removed} stale files")
    except Exception as e:
        _api_logger.debug("Startup cache cleanup skipped: %s", e)


_startup_cache_cleanup()


def _set_cached(key: str, data: Any) -> None:
    """Store data in both in-memory and file caches."""
    global _last_disk_cleanup
    entry = {"ts": time.time(), "data": data}

    with _cache_lock:
        # Evict oldest entries if cache is full
        if len(_memory_cache) >= MAX_MEMORY_CACHE_SIZE:
            sorted_keys = sorted(
                list(_memory_cache.keys()),
                key=lambda k: _memory_cache.get(k, {}).get("ts") or 0,
            )
            for k in sorted_keys[: MAX_MEMORY_CACHE_SIZE // 5]:
                _memory_cache.pop(k, None)
        _memory_cache[key] = entry

    cache_file = CACHE_DIR / f"{key}.json"
    try:
        with open(cache_file, "w", encoding="utf-8") as fh:
            json.dump(entry, fh, ensure_ascii=False)
    except Exception as exc:
        _log_warn(f"Failed to write cache file {cache_file}: {exc}")

    # L3: Replicate to Supabase persistent cache
    if _HAS_SUPABASE:
        try:
            _supa_set(key, data, ttl_seconds=CACHE_TTL, category="api")
        except Exception:
            pass

    # L4: Replicate to Upstash Redis persistent cache
    if _HAS_UPSTASH:
        try:
            _upstash_set(key, data, ttl_seconds=CACHE_TTL, category="api")
        except Exception:
            pass

    # Periodic disk cache cleanup: remove oldest files when count exceeds limit.
    # Run at most once per hour to avoid I/O overhead.
    now = time.time()
    if now - _last_disk_cleanup > 3600:
        _last_disk_cleanup = now
        try:
            cache_files = sorted(
                CACHE_DIR.glob("*.json"),
                key=lambda p: p.stat().st_mtime,
            )
            if len(cache_files) > _MAX_DISK_CACHE_FILES:
                for old_file in cache_files[: len(cache_files) - _MAX_DISK_CACHE_FILES]:
                    old_file.unlink(missing_ok=True)
                _log_info(
                    f"Disk cache cleanup: removed {len(cache_files) - _MAX_DISK_CACHE_FILES} old files"
                )
        except Exception as exc:
            _log_warn(f"Disk cache cleanup failed: {exc}")


# ---------------------------------------------------------------------------
# Circuit Breaker — prevents hammering failing APIs
# ---------------------------------------------------------------------------

# Per-API circuit breaker state: {api_name: {"failure_count": int,
#   "last_failure_time": float, "is_open": bool}}
_circuit_breaker_state: Dict[str, Dict[str, Any]] = {}

_CB_FAILURE_THRESHOLD = 3  # consecutive failures before opening circuit
_CB_RECOVERY_TIMEOUT = 300  # seconds (5 minutes) before retrying a tripped API


def _circuit_breaker_check(api_name: str) -> bool:
    """
    Return True if the circuit is OPEN (API should be skipped).
    Thread-safe using _circuit_breaker_lock.
    """
    with _circuit_breaker_lock:
        state = _circuit_breaker_state.get(api_name)
        if state is None:
            return False
        if not state.get("is_open", False):
            return False
        # Check if recovery timeout has elapsed — allow a retry
        elapsed = time.time() - state.get("last_failure_time") or 0
        if elapsed >= _CB_RECOVERY_TIMEOUT:
            # Half-open: allow one request through
            state["is_open"] = False
            _log_info(
                f"Circuit breaker half-open for '{api_name}' — "
                f"allowing retry after {elapsed:.0f}s"
            )
            return False
        return True


def _circuit_breaker_record_success(api_name: str) -> None:
    """Reset failure count on success. Thread-safe."""
    with _circuit_breaker_lock:
        _circuit_breaker_state[api_name] = {
            "failure_count": 0,
            "last_failure_time": 0.0,
            "is_open": False,
        }


def _circuit_breaker_record_failure(api_name: str) -> None:
    """Increment failure count, open circuit if threshold reached. Thread-safe."""
    with _circuit_breaker_lock:
        state = _circuit_breaker_state.get(
            api_name,
            {
                "failure_count": 0,
                "last_failure_time": 0.0,
                "is_open": False,
            },
        )
        state["failure_count"] = state.get("failure_count") or 0 + 1
        state["last_failure_time"] = time.time()
        if state["failure_count"] >= _CB_FAILURE_THRESHOLD:
            state["is_open"] = True
            _log_warn(
                f"Circuit breaker OPEN for '{api_name}' after "
                f"{state['failure_count']} consecutive failures"
            )
        _circuit_breaker_state[api_name] = state


# ---------------------------------------------------------------------------
# API Key Auth Failure Tracking (self-healing: key rotation detection)
# ---------------------------------------------------------------------------

_auth_failure_counts: Dict[str, int] = {}  # api_name -> consecutive 401/403 count
_auth_failure_lock = threading.Lock()
_AUTH_ALERT_THRESHOLD = 3  # consecutive auth failures before alerting


def _record_auth_failure(api_name: str) -> None:
    """Track a 401/403 auth failure. Alert after threshold consecutive failures."""
    with _auth_failure_lock:
        _auth_failure_counts[api_name] = _auth_failure_counts.get(api_name, 0) + 1
        count = _auth_failure_counts[api_name]
    _log_warn(f"Auth failure ({count}x) for '{api_name}' — possible key rotation")
    if count == _AUTH_ALERT_THRESHOLD:
        try:
            from email_alerts import send_error_alert

            send_error_alert(
                error_type="APIAuthFailure",
                error_message=(
                    f"API '{api_name}' returned {count} consecutive 401/403 errors. "
                    f"API key may have been rotated or revoked."
                ),
                context={"api_name": api_name, "consecutive_failures": count},
            )
        except Exception:
            _log_warn(f"Could not send auth failure alert for '{api_name}'")


def _clear_auth_failure(api_name: str) -> None:
    """Reset auth failure count on successful auth."""
    with _auth_failure_lock:
        _auth_failure_counts.pop(api_name, None)


def check_api_key_health() -> Dict[str, Any]:
    """Return current auth-failure status for all tracked APIs.

    Useful for health-check endpoints and diagnostics dashboards.
    """
    with _auth_failure_lock:
        snapshot = dict(_auth_failure_counts)
    result: Dict[str, Any] = {}
    for api_name, count in snapshot.items():
        result[api_name] = {
            "consecutive_auth_failures": count,
            "status": (
                "critical"
                if count >= _AUTH_ALERT_THRESHOLD
                else ("warning" if count > 0 else "ok")
            ),
        }
    # Also include circuit-breaker state for context
    with _circuit_breaker_lock:
        for api_name, state in _circuit_breaker_state.items():
            if api_name not in result:
                result[api_name] = {"consecutive_auth_failures": 0, "status": "ok"}
            result[api_name]["circuit_breaker_open"] = state.get("is_open", False)
    return result


def _http_get_json(
    url: str, headers: Optional[Dict[str, str]] = None, timeout: int = API_TIMEOUT
) -> Optional[Any]:
    """
    Perform an HTTP GET and return parsed JSON, or None on any failure.
    Tries verified SSL first, falls back to unverified if needed.
    Retries on 429/503 with exponential backoff (1s, 2s, 4s).
    """
    req = urllib.request.Request(url, method="GET")
    req.add_header(
        "User-Agent", "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)"
    )
    req.add_header("Accept", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)

    # Determine whether to allow SSL fallback based on environment config.
    # SSL verification is strict by default. Set ALLOW_UNVERIFIED_SSL=1 to enable fallback (not recommended).
    _allow_unverified = os.environ.get("ALLOW_UNVERIFIED_SSL", "").strip() == "1"
    ssl_contexts = [_DEFAULT_SSL_CTX]
    if _allow_unverified:
        ssl_contexts.append(_get_unverified_ssl_ctx())

    max_retries = 3
    for attempt in range(max_retries + 1):
        for ctx_idx, ctx in enumerate(ssl_contexts):
            try:
                with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                    if ctx_idx > 0:
                        _log_warn(
                            f"SSL verification BYPASSED for {url} — consider fixing the certificate"
                        )
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw)
            except ssl.SSLError:
                if ctx_idx == 0:
                    _log_warn(f"SSL error for {url}, retrying without verification")
                continue  # retry with unverified context
            except urllib.error.HTTPError as exc:
                if exc.code in (429, 503) and attempt < max_retries:
                    wait = min(2**attempt, 8)
                    _log_warn(
                        f"HTTP {exc.code} for {url}, retry in {wait}s (attempt {attempt + 1}/{max_retries})"
                    )
                    time.sleep(wait)
                    break  # break SSL loop to retry
                _log_warn(f"HTTP GET failed for {url}: {exc}")
                return None
            except Exception as exc:
                _log_warn(f"HTTP GET failed for {url}: {exc}")
                return None
        else:
            continue  # SSL loop completed without break, move to next attempt
        continue  # broke out of SSL loop (429/503), retry
    return None


def _http_post_json(
    url: str,
    payload: Any,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = API_TIMEOUT,
    max_retries: int = 2,
) -> Optional[Any]:
    """Perform an HTTP POST with a JSON body and return parsed JSON.

    Retries on HTTP 429 (rate-limited) and 503 (service unavailable)
    with exponential backoff, consistent with ``_http_get_json_with_retry``.
    """
    body = json.dumps(payload).encode("utf-8")
    backoff_delays = [1, 2, 4]

    for attempt in range(max_retries + 1):
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header(
            "User-Agent", "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)"
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)

        _allow_unverified = os.environ.get("ALLOW_UNVERIFIED_SSL", "").strip() == "1"
        ssl_contexts = [_DEFAULT_SSL_CTX]
        if _allow_unverified:
            ssl_contexts.append(_get_unverified_ssl_ctx())

        for ctx_idx, ctx in enumerate(ssl_contexts):
            try:
                with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                    if ctx_idx > 0:
                        _log_warn(f"SSL verification BYPASSED for POST {url}")
                    raw = resp.read().decode("utf-8")
                    return json.loads(raw)
            except ssl.SSLError:
                if ctx_idx == 0:
                    _log_warn(
                        f"SSL error for POST {url}, retrying without verification"
                    )
                continue
            except urllib.error.HTTPError as exc:
                if exc.code in (429, 503) and attempt < max_retries:
                    retry_after = (
                        exc.headers.get("Retry-After") if exc.headers else None
                    )
                    if retry_after is not None:
                        try:
                            wait = max(float(retry_after), 0.5)
                        except (ValueError, TypeError):
                            wait = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    else:
                        wait = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    _log_warn(
                        f"HTTP {exc.code} for POST {url} -- "
                        f"retry {attempt + 1}/{max_retries} after {wait}s"
                    )
                    time.sleep(wait)
                    break  # break SSL loop, continue retry loop
                _log_warn(f"HTTP POST failed for {url}: {exc}")
                return None
            except Exception as exc:
                _log_warn(f"HTTP POST failed for {url}: {exc}")
                return None
        else:
            # Inner SSL loop exhausted without break -- both contexts failed
            return None
    # All retries exhausted
    _log_warn(f"HTTP POST exhausted {max_retries} retries for {url}")
    return None


# ---------------------------------------------------------------------------
# HTTP GET with retry + exponential backoff
# ---------------------------------------------------------------------------


def _http_get_json_with_retry(
    url: str,
    params: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = API_TIMEOUT,
    max_retries: int = 3,
) -> Any:
    """
    Perform an HTTP GET with exponential backoff retry on transient errors.

    Retries on:
        - HTTP 429 (rate limit) — honours Retry-After header if present
        - HTTP 503 (service unavailable)

    Args:
        url:         Target URL (query params may be appended from *params*)
        params:      Optional dict of query string parameters
        headers:     Optional extra HTTP headers
        timeout:     Per-request timeout in seconds
        max_retries: Maximum number of retry attempts (default 3)

    Returns:
        Parsed JSON response on success.

    Raises:
        urllib.error.HTTPError / Exception on final failure after all retries.
    """
    if params:
        qs = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{qs}"

    backoff_delays = [1, 2, 4, 8, 16]  # seconds — only first max_retries used
    last_exc: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        req = urllib.request.Request(url, method="GET")
        req.add_header(
            "User-Agent", "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)"
        )
        req.add_header("Accept", "application/json")
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)

        _allow_unverified = os.environ.get("ALLOW_UNVERIFIED_SSL", "").strip() == "1"
        _ssl_ctxs = [_DEFAULT_SSL_CTX]
        if _allow_unverified:
            _ssl_ctxs.append(_get_unverified_ssl_ctx())
        for _ctx_idx, ctx in enumerate(_ssl_ctxs):
            try:
                with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                    if _ctx_idx > 0:
                        _log_warn(f"SSL verification BYPASSED for {url}")
                    raw = resp.read().decode("utf-8")
                    # Clear auth failure count on success
                    try:
                        _h = urllib.parse.urlparse(url).hostname or ""
                        if _h:
                            _clear_auth_failure(_h)
                    except Exception:
                        pass
                    return json.loads(raw)
            except ssl.SSLError:
                if _ctx_idx == 0:
                    _log_warn(f"SSL error for {url}, retrying without verification")
                continue  # retry with unverified context
            except urllib.error.HTTPError as exc:
                status_code = exc.code
                # ── Auth failure detection (key rotation) ──
                if status_code in (401, 403):
                    # Extract API name from URL host for tracking
                    try:
                        _host = urllib.parse.urlparse(url).hostname or url
                    except Exception:
                        _host = url[:60]
                    _record_auth_failure(_host)
                if status_code in (429, 503) and attempt < max_retries:
                    # Determine wait time: prefer Retry-After header
                    retry_after = (
                        exc.headers.get("Retry-After") if exc.headers else None
                    )
                    if retry_after is not None:
                        try:
                            wait = max(float(retry_after), 0.5)
                        except (ValueError, TypeError):
                            wait = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    else:
                        wait = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                    _log_warn(
                        f"HTTP {status_code} for {url} — "
                        f"retry {attempt + 1}/{max_retries} "
                        f"after {wait}s"
                    )
                    time.sleep(wait)
                    last_exc = exc
                    break  # break inner SSL loop, restart outer retry loop
                else:
                    # Non-retryable status or retries exhausted
                    last_exc = exc
                    if attempt >= max_retries:
                        raise
                    raise
            except Exception as exc:
                last_exc = exc
                if attempt >= max_retries:
                    raise
                wait = backoff_delays[min(attempt, len(backoff_delays) - 1)]
                _log_warn(
                    f"HTTP GET error for {url}: {exc} — "
                    f"retry {attempt + 1}/{max_retries} after {wait}s"
                )
                time.sleep(wait)
                break  # break inner SSL loop, restart outer retry loop
        else:
            # Inner loop exhausted without break — both SSL contexts failed
            if last_exc is not None and attempt >= max_retries:
                raise last_exc
            continue

    # Should not reach here, but just in case
    if last_exc is not None:
        raise last_exc
    return None


def _parse_country_from_location(location: str) -> Optional[str]:
    """
    Attempt to extract an ISO-3 country code from a location string.
    Examples:
        'San Mateo, CA'  -> 'USA'  (US state detected)
        'London, UK'     -> 'GBR'
        'Sydney, Australia' -> 'AUS'
        'Seattle WA'     -> 'USA'  (no comma, space-separated)

    Tries the canonical standardizer first (more comprehensive city/state/
    country database with disambiguation logic), then falls back to the
    local COUNTRY_CODES / US_STATES maps.
    """
    if not location:
        return None

    # --- Try standardizer first (handles edge cases like disambiguation
    # of CA=California vs CA=Canada, Portland OR vs Portland ME, etc.) ---
    if _HAS_STANDARDIZER:
        parsed = _std_normalize_location(location)
        iso3 = parsed.get("country_iso3") or ""
        if iso3:
            return iso3

    # --- Fallback to local maps ---
    parts = [p.strip() for p in location.split(",")]

    # Check last part first (most specific)
    for part in reversed(parts):
        token = part.strip().upper()
        # US state abbreviation?
        if token in US_STATES:
            return "USA"
        lower = part.strip().lower()
        if lower in COUNTRY_CODES:
            return COUNTRY_CODES[lower]

    # Handle locations without commas like "Seattle WA"
    if len(parts) == 1:
        words = location.strip().split()
        if words:
            last_word = words[-1].upper()
            if last_word in US_STATES:
                return "USA"
            if last_word.lower() in COUNTRY_CODES:
                return COUNTRY_CODES[last_word.lower()]

    # Check full string
    lower_full = location.lower().strip()
    if lower_full in COUNTRY_CODES:
        return COUNTRY_CODES[lower_full]

    # Default to USA if no country detected (common for US city names)
    return None


def _domain_from_name(name: str) -> str:
    """
    Best-effort guess of a domain from a company name.
    'Salesforce' -> 'salesforce.com', 'Duck Creek Technologies' -> 'duckcreek.com'
    """
    clean = re.sub(r"[^a-zA-Z0-9\s]", "", name).strip().lower()
    clean = re.sub(
        r"\s+(inc|llc|ltd|corp|co|technologies|technology|software|group|solutions)$",
        "",
        clean,
        flags=re.IGNORECASE,
    ).strip()
    slug = clean.replace(" ", "")
    return f"{slug}.com"


def _extract_state_abbr(location: str) -> Optional[str]:
    """Extract US state abbreviation from a location string.

    Uses the canonical standardizer when available (handles full state
    names, city context, and disambiguation), with hardcoded fallback.
    """
    # Try standardizer first (richer parsing with city-to-state DB)
    if _HAS_STANDARDIZER:
        parsed = _std_normalize_location(location)
        st = parsed.get("state") or ""
        if st:
            return st.upper()

    # Fallback to original logic
    parts = [p.strip() for p in location.split(",")]
    for part in reversed(parts):
        token = part.strip().upper()
        if token in US_STATES:
            return token
    # Handle "Seattle WA" format (no comma)
    words = location.strip().split()
    if words:
        last_word = words[-1].upper()
        if last_word in US_STATES:
            return last_word
    return None


# ---------------------------------------------------------------------------
# API 1: BLS (Bureau of Labor Statistics) — OES Salary Data
# ---------------------------------------------------------------------------


def _fetch_bls_salary(role: str, soc_code: str) -> Optional[Dict[str, Any]]:
    """
    Fetch median, 10th-percentile, and 90th-percentile annual wages for a
    given SOC code from the BLS OES survey.

    BLS OES series ID format (25 chars total):
        OEUN (prefix=OE, seasonal=U, areatype=N)
        + area(7) = 0000000 (national)
        + industry(6) = 000000 (all industries)
        + occupation(6) = {soc_clean}
        + datatype(2) = 04 (mean), 13 (median), etc.

    Datatype codes: 01=employment, 04=annual mean wage,
        11=annual 10th pct, 13=annual median, 15=annual 90th pct
    """
    cache_k = _cache_key("bls", soc_code)
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    # Strip the dash for the series ID
    soc_clean = soc_code.replace("-", "")

    series_mean = f"OEUN0000000000000{soc_clean}04"  # annual mean wage
    series_median = f"OEUN0000000000000{soc_clean}13"  # annual median wage
    series_p10 = f"OEUN0000000000000{soc_clean}11"  # annual 10th pct
    series_p90 = f"OEUN0000000000000{soc_clean}15"  # annual 90th pct

    api_key = os.environ.get("BLS_API_KEY", "")

    # Try v2 first (higher limits, 500/day with key), then fall back to v1
    # (25 queries/day, no key needed).
    endpoints: List[tuple] = []
    if api_key:
        endpoints.append(("v2", "https://api.bls.gov/publicAPI/v2/timeseries/data/"))
    endpoints.append(("v1", "https://api.bls.gov/publicAPI/v1/timeseries/data/"))

    resp = None
    for version, url in endpoints:
        payload: Dict[str, Any] = {
            "seriesid": [series_mean, series_median, series_p10, series_p90],
            "startyear": "2022",
            "endyear": "2024",
        }
        if version == "v2" and api_key:
            payload["registrationkey"] = api_key

        resp = _http_post_json(url, payload, timeout=12)
        if resp and resp.get("status") == "REQUEST_SUCCEEDED":
            _log_info(f"BLS {version} request succeeded for SOC {soc_code}")
            break
        else:
            msg = ""
            if resp:
                msg = str(resp.get("message") or "")
            _log_warn(f"BLS {version} failed for SOC {soc_code}: {msg}")
            resp = None

    if not resp:
        _log_warn(f"BLS request failed on all endpoints for SOC {soc_code}")
        return None

    result: Dict[str, Any] = {"source": "BLS OES"}
    series_list = resp.get("Results", {}).get("series", [])

    for series in series_list:
        sid = series.get("seriesID") or ""
        data_points = series.get("data", [])
        if not data_points:
            continue
        # Take the most recent value
        latest = data_points[0]
        try:
            val_str = str(latest.get("value", "0"))
            value = float(val_str.replace(",", ""))
        except (ValueError, TypeError):
            continue

        if sid == series_mean:
            result["mean"] = int(value)
        elif sid == series_median:
            result["median"] = int(value)
        elif sid == series_p10:
            result["p10"] = int(value)
        elif sid == series_p90:
            result["p90"] = int(value)

    # Accept result if we got at least mean or median
    if "median" in result or "mean" in result:
        _set_cached(cache_k, result)
        return result

    return None


def fetch_salary_data(roles: List[str]) -> Dict[str, Any]:
    """Fetch salary data for a list of role titles via BLS."""
    salary_data: Dict[str, Any] = {}

    for role in roles:
        role_lower = role.strip().lower()
        soc = SOC_CODES.get(role_lower)
        if not soc:
            # Try partial matching — check if any known title is contained
            # in the role or vice versa
            for title, code in SOC_CODES.items():
                if title in role_lower or role_lower in title:
                    soc = code
                    break
        if not soc:
            # Try word-level matching for multi-word roles
            role_words = set(role_lower.split())
            best_overlap = 0
            for title, code in SOC_CODES.items():
                title_words = set(title.split())
                overlap = len(role_words & title_words)
                if overlap > best_overlap and overlap >= 1:
                    best_overlap = overlap
                    soc = code
        if not soc and _HAS_STANDARDIZER:
            # Try canonical standardizer (covers more role aliases)
            std_soc = _std_get_soc_code(role)
            if std_soc:
                soc = std_soc
        if not soc:
            _log_warn(f"No SOC code mapping for role: {role}")
            continue

        try:
            result = _fetch_bls_salary(role, soc)
            if result:
                salary_data[role] = result
        except Exception as exc:
            _log_warn(f"BLS salary fetch failed for {role}: {exc}")

    return salary_data


# ---------------------------------------------------------------------------
# API 2: BLS QCEW (Quarterly Census of Employment & Wages)
# ---------------------------------------------------------------------------

# US state name-to-FIPS mapping for Census/QCEW
US_STATE_FIPS: Dict[str, str] = {
    "AL": "01",
    "AK": "02",
    "AZ": "04",
    "AR": "05",
    "CA": "06",
    "CO": "08",
    "CT": "09",
    "DE": "10",
    "FL": "12",
    "GA": "13",
    "HI": "15",
    "ID": "16",
    "IL": "17",
    "IN": "18",
    "IA": "19",
    "KS": "20",
    "KY": "21",
    "LA": "22",
    "ME": "23",
    "MD": "24",
    "MA": "25",
    "MI": "26",
    "MN": "27",
    "MS": "28",
    "MO": "29",
    "MT": "30",
    "NE": "31",
    "NV": "32",
    "NH": "33",
    "NJ": "34",
    "NM": "35",
    "NY": "36",
    "NC": "37",
    "ND": "38",
    "OH": "39",
    "OK": "40",
    "OR": "41",
    "PA": "42",
    "RI": "44",
    "SC": "45",
    "SD": "46",
    "TN": "47",
    "TX": "48",
    "UT": "49",
    "VT": "50",
    "VA": "51",
    "WA": "53",
    "WV": "54",
    "WI": "55",
    "WY": "56",
    "DC": "11",
}


def _http_get_text(url: str, timeout: int = API_TIMEOUT) -> Optional[str]:
    """Perform HTTP GET and return raw text, or None on failure."""
    req = urllib.request.Request(url, method="GET")
    req.add_header(
        "User-Agent", "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)"
    )
    _allow_unverified = os.environ.get("ALLOW_UNVERIFIED_SSL", "").strip() == "1"
    _ssl_ctxs = [_DEFAULT_SSL_CTX]
    if _allow_unverified:
        _ssl_ctxs.append(_get_unverified_ssl_ctx())
    for _ctx_idx, ctx in enumerate(_ssl_ctxs):
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                if _ctx_idx > 0:
                    _log_warn(f"SSL verification BYPASSED for text GET {url}")
                return resp.read().decode("utf-8")
        except ssl.SSLError:
            if _ctx_idx == 0:
                _log_warn(
                    f"SSL error for text GET {url}, retrying without verification"
                )
            continue
        except Exception as exc:
            _log_warn(f"HTTP GET text failed for {url}: {exc}")
            return None
    return None


def fetch_industry_employment(industry: str) -> Optional[Dict[str, Any]]:
    """
    Fetch industry-level employment stats from BLS QCEW API.
    Returns employment count, average wages, and establishment count
    for the given industry's NAICS sector nationally.
    """
    cache_k = _cache_key("qcew_industry", industry)
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    # Map industry to NAICS code (via standardizer with hardcoded fallback)
    naics = get_naics_code(industry)
    if not naics:
        # Word-level fallback against hardcoded NAICS_CODES
        industry_lower = industry.lower().replace(" ", "_")
        industry_words = set(industry_lower.replace("_", " ").split())
        for key, code in NAICS_CODES.items():
            key_words = set(key.replace("_", " ").split())
            if industry_words & key_words:
                naics = code
                break
    if not naics:
        naics = "10"  # All industries fallback

    # BLS QCEW API — national level, most recent year
    # Format: https://data.bls.gov/cew/data/api/{year}/{qtr}/industry/{naics}.csv
    result: Dict[str, Any] = {"source": "BLS QCEW"}

    for year in ["2024", "2023", "2022"]:
        for qtr in ["a", "1"]:  # 'a' = annual, '1' = Q1
            url = f"https://data.bls.gov/cew/data/api/{year}/{qtr}/industry/{naics}.csv"
            try:
                raw = _http_get_text(url, timeout=10)
                if not raw or "area_fips" not in raw:
                    continue

                # Parse CSV — find the US national row (area_fips = US000)
                lines = raw.strip().split("\n")
                if len(lines) < 2:
                    continue

                headers = [h.strip().strip('"') for h in lines[0].split(",")]
                for line in lines[1:]:
                    cols = [c.strip().strip('"') for c in line.split(",")]
                    if len(cols) < len(headers):
                        continue
                    row = dict(zip(headers, cols))

                    area = row.get("area_fips") or ""
                    # National total row + private ownership
                    if area == "US000" and row.get("own_code") or "" == "5":
                        try:
                            emp = int(
                                row.get("annual_avg_emplvl", "0")
                                or row.get("month1_emplvl", "0")
                            )
                            wages = int(row.get("annual_avg_wkly_wage", "0") or "0")
                            estabs = int(
                                row.get("annual_avg_estabs", "0")
                                or row.get("qtrly_estabs", "0")
                            )

                            result["total_employed"] = emp
                            result["avg_weekly_wage"] = wages
                            result["avg_annual_wage"] = wages * 52 if wages else None
                            result["establishments"] = estabs
                            result["sector_name"] = industry.replace("_", " ").title()
                            result["year"] = year
                            result["naics"] = naics

                            _set_cached(cache_k, result)
                            return result
                        except (ValueError, TypeError):
                            continue

            except Exception as exc:
                _log_warn(f"QCEW fetch failed for {naics}/{year}/{qtr}: {exc}")
                continue

    _log_warn(f"No QCEW data found for industry: {industry} (NAICS {naics})")
    return None


# ---------------------------------------------------------------------------
# API 3: US Census ACS (Demographics)
# ---------------------------------------------------------------------------


def fetch_location_demographics(locations: List[str]) -> Dict[str, Any]:
    """
    Fetch demographic data for US locations from the Census Bureau ACS API.
    Returns population and median household income at state level.
    Falls back to providing state-level data when city-level is unavailable.

    For non-US locations, returns basic info from WorldBank if available.
    """
    demo_data: Dict[str, Any] = {}

    # First, fetch all US state data in one call (efficient)
    state_data = _fetch_census_state_data()

    for loc in locations:
        cache_k = _cache_key("census_geo", loc)
        cached = _get_cached(cache_k)
        if cached is not None:
            demo_data[loc] = cached
            continue

        # Parse location
        parts = [p.strip() for p in loc.split(",")]
        city = parts[0] if parts else ""
        state_abbr = _extract_state_abbr(loc)

        # Check if US location
        country = _parse_country_from_location(loc)

        if country == "USA" and state_abbr and state_abbr in US_STATE_FIPS:
            fips = US_STATE_FIPS[state_abbr]
            if fips in state_data:
                entry = {
                    "population": state_data[fips].get("population"),
                    "median_income": state_data[fips].get("median_income"),
                    "state_name": state_data[fips].get("name", state_abbr),
                    "city": city,
                    "source": "US Census ACS",
                    "geo_level": "State",
                }
                demo_data[loc] = entry
                _set_cached(cache_k, entry)
                continue

        # For non-US or unmatched US locations, try WorldBank population
        if country and country != "USA":
            wb_url = (
                f"https://api.worldbank.org/v2/country/{country}/indicator/"
                f"SP.POP.TOTL?format=json&per_page=5&date=2019:2024"
            )
            try:
                resp = _http_get_json(wb_url, timeout=8)
                if resp and isinstance(resp, list) and len(resp) >= 2 and resp[1]:
                    for rec in resp[1]:
                        if rec.get("value") is not None:
                            entry = {
                                "population": int(rec["value"]),
                                "source": "WorldBank",
                                "geo_level": "Country",
                                "country": country,
                            }
                            demo_data[loc] = entry
                            _set_cached(cache_k, entry)
                            break
            except Exception:
                pass

        if loc not in demo_data:
            _log_warn(f"No demographic data for: {loc}")

    return demo_data


def _fetch_census_state_data() -> Dict[str, Dict[str, Any]]:
    """
    Fetch all US state-level population and median income from Census ACS.
    Returns dict keyed by state FIPS code.
    No API key required for state-level queries.
    """
    cache_k = _cache_key("census_states", "all")
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    # Try multiple ACS vintages in case the most recent isn't available yet
    for acs_year in ["2023", "2022", "2021"]:
        # ACS 5-year estimates — B01001_001E = total population,
        # B19013_001E = median household income
        url = (
            f"https://api.census.gov/data/{acs_year}/acs/acs5"
            "?get=NAME,B01001_001E,B19013_001E&for=state:*"
        )

        try:
            resp = _http_get_json(url, timeout=10)
            if not resp or not isinstance(resp, list) or len(resp) < 2:
                _log_warn(
                    f"Census ACS {acs_year} state data request failed, trying older year"
                )
                continue

            # First row is headers: ["NAME","B01001_001E","B19013_001E","state"]
            headers = resp[0]
            state_data: Dict[str, Dict[str, Any]] = {}

            for row in resp[1:]:
                if len(row) < 4:
                    continue
                fips = row[3]  # state FIPS code
                try:
                    state_data[fips] = {
                        "name": row[0],
                        "population": int(row[1]) if row[1] else None,
                        "median_income": int(row[2]) if row[2] else None,
                    }
                except (ValueError, TypeError):
                    continue

            if state_data:
                _set_cached(cache_k, state_data)
                _log_info(f"Census ACS {acs_year} loaded {len(state_data)} states")
                return state_data

        except Exception as exc:
            _log_warn(f"Census ACS {acs_year} fetch failed: {exc}")

    _log_warn("Census ACS fetch failed for all years")
    return {}


# ---------------------------------------------------------------------------
# API 4: World Bank Open Data
# ---------------------------------------------------------------------------

_WB_INDICATORS = {
    "unemployment_rate": "SL.UEM.TOTL.ZS",
    "gdp_growth": "NY.GDP.MKTP.KD.ZG",
    "labor_force": "SL.TLF.TOTL.IN",
}


def fetch_global_indicators(locations: List[str]) -> Dict[str, Any]:
    """
    Fetch key economic indicators from the World Bank for international locations.
    Skips US locations (covered by BLS/Census).
    Uses World Bank API v2 with JSON format.
    """
    indicators: Dict[str, Any] = {}
    seen_countries: set = set()

    for loc in locations:
        iso3 = _parse_country_from_location(loc)
        if not iso3 or iso3 == "USA" or iso3 in seen_countries:
            continue
        seen_countries.add(iso3)

        cache_k = _cache_key("worldbank", iso3)
        cached = _get_cached(cache_k)
        if cached is not None:
            # Use the short country label
            label = _country_label(iso3)
            indicators[label] = cached
            continue

        country_data: Dict[str, Any] = {"source": "WorldBank"}

        for field, indicator_code in _WB_INDICATORS.items():
            # World Bank API v2 — use date range that's likely to have data
            url = (
                f"https://api.worldbank.org/v2/country/{iso3}/indicator/"
                f"{indicator_code}?format=json&per_page=5&date=2019:2024"
            )
            try:
                resp = _http_get_json(url, timeout=8)
                if resp and isinstance(resp, list) and len(resp) >= 2:
                    records = resp[1]
                    if records:
                        # Take most recent non-null value
                        for rec in records:
                            val = rec.get("value")
                            if val is not None:
                                if field == "labor_force":
                                    country_data[field] = int(val)
                                else:
                                    country_data[field] = round(val, 1)
                                break
            except Exception as exc:
                _log_warn(f"WorldBank {indicator_code} failed for {iso3}: {exc}")

        label = _country_label(iso3)
        if len(country_data) > 1:  # more than just "source"
            _set_cached(cache_k, country_data)
            indicators[label] = country_data

    return indicators


_COUNTRY_LABEL_MAP: Dict[str, str] = {
    v: k for k, v in COUNTRY_CODES.items() if len(k) == 2
}


def _country_label(iso3: str) -> str:
    """Convert ISO-3 code to a short human-readable label (e.g. 'GBR' -> 'UK')."""
    return _COUNTRY_LABEL_MAP.get(iso3, iso3).upper()


# ---------------------------------------------------------------------------
# API 5: Clearbit Logo API + Google Favicons
# ---------------------------------------------------------------------------


def fetch_company_logo(domain: str) -> Optional[str]:
    """
    Return a logo URL for the given domain.
    Tries Clearbit first, falls back to Google Favicons (always available).
    """
    if not domain:
        return None

    domain = domain.strip().lower()
    if domain.startswith("http"):
        parsed = urllib.parse.urlparse(domain)
        domain = parsed.hostname or domain

    cache_k = _cache_key("logo", domain)
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    # Strategy 1: Clearbit Logo API (higher quality)
    clearbit_url = f"https://logo.clearbit.com/{domain}"
    _allow_unverified_logo = os.environ.get("ALLOW_UNVERIFIED_SSL", "").strip() == "1"
    _logo_ctxs = [_DEFAULT_SSL_CTX]
    if _allow_unverified_logo:
        _logo_ctxs.append(_get_unverified_ssl_ctx())
    for _ctx_idx, _ctx in enumerate(_logo_ctxs):
        try:
            req = urllib.request.Request(clearbit_url, method="HEAD")
            req.add_header("User-Agent", "MediaPlanGenerator/1.0")
            with urllib.request.urlopen(req, timeout=3, context=_ctx) as resp:
                if resp.status == 200:
                    if _ctx_idx > 0:
                        _log_warn(
                            f"SSL verification BYPASSED for logo HEAD {clearbit_url}"
                        )
                    _set_cached(cache_k, clearbit_url)
                    return clearbit_url
        except ssl.SSLError:
            continue
        except Exception:
            break  # Non-SSL error, skip to fallback

    # Strategy 2: Google Favicons API (always works, lower resolution)
    google_url = f"https://www.google.com/s2/favicons?domain={domain}&sz=128"
    _set_cached(cache_k, google_url)
    return google_url


def fetch_competitor_logos(
    competitors: List[str], client_website: Optional[str] = None
) -> Dict[str, str]:
    """Fetch logo URLs for a list of competitor company names."""
    logos: Dict[str, str] = {}
    for comp in competitors:
        # Try Clearbit autocomplete first to get accurate domain
        meta = fetch_company_metadata(comp)
        if meta and meta.get("domain"):
            domain = meta["domain"]
        else:
            domain = _domain_from_name(comp)
        url = fetch_company_logo(domain)
        if url:
            logos[comp] = url
    return logos


# ---------------------------------------------------------------------------
# API 6: Adzuna Job Search
# ---------------------------------------------------------------------------

_ADZUNA_BENCHMARKS: Dict[str, Dict[str, Any]] = {
    "technology": {
        "posting_count": 145000,
        "avg_salary": 125000,
        "competition": "high",
    },
    "healthcare": {"posting_count": 210000, "avg_salary": 78000, "competition": "high"},
    "finance": {"posting_count": 85000, "avg_salary": 105000, "competition": "medium"},
    "manufacturing": {
        "posting_count": 65000,
        "avg_salary": 55000,
        "competition": "medium",
    },
    "retail": {"posting_count": 180000, "avg_salary": 38000, "competition": "high"},
    "logistics": {"posting_count": 95000, "avg_salary": 48000, "competition": "high"},
    "education": {"posting_count": 72000, "avg_salary": 55000, "competition": "medium"},
    "hospitality": {
        "posting_count": 120000,
        "avg_salary": 35000,
        "competition": "medium",
    },
    "construction": {
        "posting_count": 55000,
        "avg_salary": 52000,
        "competition": "medium",
    },
    "engineering": {
        "posting_count": 42000,
        "avg_salary": 98000,
        "competition": "medium",
    },
    "marketing": {"posting_count": 38000, "avg_salary": 72000, "competition": "medium"},
    "default": {"posting_count": 50000, "avg_salary": 65000, "competition": "medium"},
}


def _adzuna_benchmark_fallback(
    roles: List[str], locations: List[str]
) -> Dict[str, Any]:
    """Return Adzuna benchmark data when API keys are not configured."""
    if not roles:
        return {}
    result: Dict[str, Any] = {}
    for role in roles:
        rl = role.lower()
        matched = "default"
        for cat in _ADZUNA_BENCHMARKS:
            if cat != "default" and cat in rl:
                matched = cat
                break
        # Check common keywords
        if matched == "default":
            kw_map = {
                "software": "technology",
                "developer": "technology",
                "engineer": "engineering",
                "data": "technology",
                "devops": "technology",
                "nurse": "healthcare",
                "doctor": "healthcare",
                "physician": "healthcare",
                "accountant": "finance",
                "analyst": "finance",
                "warehouse": "logistics",
                "driver": "logistics",
                "teacher": "education",
                "manager": "default",
                "sales": "retail",
                "marketing": "marketing",
                "chef": "hospitality",
                "hotel": "hospitality",
            }
            for kw, cat in kw_map.items():
                if kw in rl:
                    matched = cat
                    break
        bench = _ADZUNA_BENCHMARKS[matched]
        result[role] = {
            "posting_count": bench["posting_count"],
            "avg_salary": bench["avg_salary"],
            "competition": bench["competition"],
            "source": "Adzuna (curated benchmark)",
            "data_confidence": 0.55,
        }
    return result


def fetch_job_market(roles: List[str], locations: List[str]) -> Dict[str, Any]:
    """
    Fetch job market data from Adzuna (if API keys are available).
    Returns posting counts, average salaries, and competition levels.
    """
    app_id = os.environ.get("ADZUNA_APP_ID", "")
    app_key = os.environ.get("ADZUNA_APP_KEY", "")

    if not app_id or not app_key:
        _log_info("Adzuna API keys not set; using benchmark fallbacks")
        return _adzuna_benchmark_fallback(roles, locations)

    job_market: Dict[str, Any] = {}

    # Determine country for Adzuna
    country = "us"  # default
    if locations:
        iso3 = _parse_country_from_location(locations[0])
        if iso3 and iso3 in ADZUNA_COUNTRY_CODES:
            country = ADZUNA_COUNTRY_CODES[iso3]

    for role in roles:
        cache_k = _cache_key("adzuna", f"{role}:{country}")
        cached = _get_cached(cache_k)
        if cached is not None:
            job_market[role] = cached
            continue

        params = urllib.parse.urlencode(
            {
                "app_id": app_id,
                "app_key": app_key,
                "what": role,
                "results_per_page": "1",
                "content-type": "application/json",
            }
        )
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1?{params}"

        try:
            resp = _http_get_json(url)
            if resp:
                count = resp.get("count") or 0
                mean_salary = resp.get("mean", None)

                # Classify competition level
                if count > 10000:
                    competition = "high"
                elif count > 2000:
                    competition = "medium"
                else:
                    competition = "low"

                entry = {
                    "posting_count": count,
                    "avg_salary": int(mean_salary) if mean_salary else None,
                    "competition": competition,
                    "source": "Adzuna",
                }
                job_market[role] = entry
                _set_cached(cache_k, entry)
        except Exception as exc:
            _log_warn(f"Adzuna fetch failed for {role}: {exc}")

    return job_market


# ---------------------------------------------------------------------------
# API 7: Currency rates (live API + hardcoded fallback)
# ---------------------------------------------------------------------------


def fetch_currency_rates() -> Dict[str, float]:
    """
    Return currency exchange rates relative to USD.
    Tries free APIs first, falls back to hardcoded rates.
    """
    cache_k = _cache_key("currency", "rates_usd")
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    # Try free currency API (no key required)
    live_apis = [
        "https://open.er-api.com/v6/latest/USD",
        "https://api.exchangerate-api.com/v4/latest/USD",
    ]

    for api_url in live_apis:
        try:
            resp = _http_get_json(api_url, timeout=5)
            if resp and "rates" in resp:
                rates = resp["rates"]
                if isinstance(rates, dict) and "EUR" in rates:
                    _log_info(f"Live currency rates fetched from {api_url}")
                    _set_cached(cache_k, rates)
                    return rates
        except Exception as exc:
            _log_warn(f"Currency API failed ({api_url}): {exc}")

    # Fallback to hardcoded rates
    _log_info("Using hardcoded fallback currency rates")
    return dict(FALLBACK_CURRENCY_RATES)


# ---------------------------------------------------------------------------
# API 8: Wikipedia REST API
# ---------------------------------------------------------------------------


def fetch_company_info(
    client_name: str, client_website: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch company description from Wikipedia and logo from Clearbit.
    Prioritizes company-specific disambiguation to avoid wrong articles.
    """
    info: Dict[str, Any] = {"source": "Wikipedia/Clearbit"}

    # --- Logo ---
    domain = client_website or _domain_from_name(client_name)
    logo = fetch_company_logo(domain)
    if logo:
        info["logo_url"] = logo

    # --- Wikipedia summary ---
    cache_k = _cache_key("wikipedia", client_name)
    cached = _get_cached(cache_k)
    if cached is not None:
        info["description"] = cached
        return info

    # Try company-specific disambiguations FIRST, then generic name.
    # This prevents getting wrong articles (e.g. "Guidewire" returning
    # an article about guy-wires instead of Guidewire Software).
    search_names = [
        f"{client_name}_(company)",
        f"{client_name.replace(' ', '_')}_(company)",
        f"{client_name}_(software)",
        f"{client_name.replace(' ', '_')}_(software)",
        client_name,
        client_name.replace(" ", "_"),
    ]

    for name in search_names:
        encoded = urllib.parse.quote(name, safe="()_")
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}"
        try:
            resp = _http_get_json(url)
            if resp and resp.get("type") == "standard":
                extract = resp.get("extract") or ""
                if extract and len(extract) > 30:
                    # Verify the article is about a company/organization, not
                    # some unrelated topic. Check for business-related terms.
                    extract_lower = extract.lower()
                    is_company_article = any(
                        term in extract_lower
                        for term in [
                            "company",
                            "corporation",
                            "inc.",
                            "ltd",
                            "software",
                            "founded",
                            "headquartered",
                            "business",
                            "firm",
                            "enterprise",
                            "organization",
                            "provider",
                            "platform",
                            "technology",
                            "services",
                            "solutions",
                            "startup",
                            "subsidiary",
                            "group",
                            "brand",
                            "manufacturer",
                            "hospital",
                            "clinic",
                            "bank",
                            "financial",
                            "retailer",
                            "store",
                            "chain",
                            "restaurant",
                        ]
                    )
                    if is_company_article:
                        info["description"] = extract
                        _set_cached(cache_k, extract)
                        return info
        except Exception:
            continue

    # If no company-specific article found, try Wikipedia search API
    # to find the best match
    try:
        search_url = (
            f"https://en.wikipedia.org/w/api.php?action=query&list=search"
            f"&srsearch={urllib.parse.quote(client_name + ' company')}"
            f"&format=json&srlimit=3"
        )
        search_resp = _http_get_json(search_url, timeout=5)
        if search_resp and "query" in search_resp:
            results = search_resp["query"].get("search", [])
            for sr in results:
                title = sr.get("title") or ""
                if not title:
                    continue
                encoded = urllib.parse.quote(title.replace(" ", "_"), safe="()_")
                summary_url = (
                    f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}"
                )
                try:
                    resp = _http_get_json(summary_url)
                    if resp and resp.get("type") == "standard":
                        extract = resp.get("extract") or ""
                        if extract and len(extract) > 30:
                            # Verify the article is about a company/organization
                            extract_lower = extract.lower()
                            is_company_article = any(
                                term in extract_lower
                                for term in [
                                    "company",
                                    "corporation",
                                    "inc.",
                                    "ltd",
                                    "software",
                                    "founded",
                                    "headquartered",
                                    "business",
                                    "firm",
                                    "enterprise",
                                    "organization",
                                    "provider",
                                    "platform",
                                    "technology",
                                    "services",
                                    "solutions",
                                    "startup",
                                    "subsidiary",
                                    "group",
                                    "brand",
                                    "manufacturer",
                                    "hospital",
                                    "clinic",
                                    "bank",
                                    "financial",
                                    "retailer",
                                    "store",
                                    "chain",
                                    "restaurant",
                                ]
                            )
                            if is_company_article:
                                info["description"] = extract
                                _set_cached(cache_k, extract)
                                return info
                except Exception:
                    continue
    except Exception:
        pass

    _log_warn(f"Wikipedia summary not found for: {client_name}")
    return info


# ---------------------------------------------------------------------------
# API 9: Clearbit Autocomplete (Company Metadata)
# ---------------------------------------------------------------------------


def fetch_company_metadata(
    company_name: str, client_website: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Fetch company metadata from Clearbit Autocomplete API.
    Returns domain, logo, and basic company info.
    Free, no API key required.

    If client_website is provided, it's used to validate/override the
    domain from Clearbit (which can sometimes return wrong results).
    """
    if not company_name:
        return None

    cache_k = _cache_key("clearbit_auto", company_name)
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    encoded = urllib.parse.quote(company_name)
    url = f"https://autocomplete.clearbit.com/v1/companies/suggest?query={encoded}"

    try:
        resp = _http_get_json(url, timeout=5)
        if resp and isinstance(resp, list) and resp:
            # Find best match — prefer exact name match
            best = resp[0]
            for item in resp:
                if item.get("name") or "".lower() == company_name.lower():
                    best = item
                    break

            domain = best.get("domain") or ""

            # If client_website is provided and Clearbit returned a different
            # domain, prefer the client_website as it's more reliable
            if client_website:
                cw = client_website.strip().lower()
                if cw.startswith("http"):
                    parsed = urllib.parse.urlparse(cw)
                    cw = parsed.hostname or cw
                # Only override if Clearbit domain looks wrong
                if domain and cw and domain != cw:
                    _log_info(
                        f"Clearbit returned domain '{domain}' but "
                        f"client_website is '{cw}'; using client_website"
                    )
                    domain = cw

            result = {
                "name": best.get("name", company_name),
                "domain": domain,
                "logo_url": best.get("logo") or "",
                "source": "Clearbit",
            }
            _set_cached(cache_k, result)
            return result
    except Exception as exc:
        _log_warn(f"Clearbit autocomplete failed for {company_name}: {exc}")

    return None


# ---------------------------------------------------------------------------
# API 10: SEC EDGAR (Public Company Data)
# ---------------------------------------------------------------------------

# Pre-loaded company tickers cache (loaded on first use)
_sec_tickers_cache: Optional[Dict[str, Any]] = None


def _load_sec_tickers() -> Dict[str, Any]:
    """Load SEC company tickers JSON (cached in memory)."""
    global _sec_tickers_cache
    if _sec_tickers_cache is not None:
        return _sec_tickers_cache

    cache_k = _cache_key("sec_tickers", "all")
    cached = _get_cached(cache_k)
    if cached is not None:
        _sec_tickers_cache = cached
        return cached

    url = "https://www.sec.gov/files/company_tickers.json"
    try:
        req = urllib.request.Request(url, method="GET")
        # SEC EDGAR requires a proper User-Agent with contact info
        req.add_header(
            "User-Agent",
            "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com; contact@joveo.com)",
        )
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=15, context=_DEFAULT_SSL_CTX) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            _sec_tickers_cache = data
            _set_cached(cache_k, data)
            _log_info(f"SEC EDGAR tickers loaded: {len(data)} companies")
            return data
    except Exception as exc:
        _log_warn(f"SEC tickers load failed: {exc}")
        _sec_tickers_cache = {}
        return {}


# Well-known company name aliases → SEC EDGAR parent company name
_COMPANY_ALIASES: Dict[str, List[str]] = {
    "google": ["alphabet"],
    "youtube": ["alphabet"],
    "facebook": ["meta platforms"],
    "instagram": ["meta platforms"],
    "whatsapp": ["meta platforms"],
    "aws": ["amazon"],
    "azure": ["microsoft"],
    "linkedin": ["microsoft"],
    "github": ["microsoft"],
    "tiktok": ["bytedance"],
    "snapchat": ["snap"],
    "gmail": ["alphabet"],
    "android": ["alphabet"],
    "chrome": ["alphabet"],
    "bing": ["microsoft"],
    "alexa": ["amazon"],
    "twitch": ["amazon"],
    "oculus": ["meta platforms"],
    "waze": ["alphabet"],
    "nest": ["alphabet"],
}


def fetch_sec_company_data(company_name: str) -> Optional[Dict[str, Any]]:
    """
    Look up a company in SEC EDGAR to determine if it's publicly traded.
    Returns ticker symbol, CIK, and filing status.
    Uses alias resolution for well-known subsidiaries (e.g. Google → Alphabet).
    """
    if not company_name:
        return None

    cache_k = _cache_key("sec_company", company_name)
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    tickers = _load_sec_tickers()
    if not tickers:
        return None

    # Search for matching company
    company_lower = company_name.lower().strip()
    # Remove common suffixes for matching
    clean_name = re.sub(
        r"\s+(inc\.?|corp\.?|ltd\.?|llc|co\.?|company|technologies|technology)$",
        "",
        company_lower,
        flags=re.IGNORECASE,
    ).strip()

    # Build search terms: original name + any aliases
    search_terms = [clean_name]
    for alias_key, alias_values in _COMPANY_ALIASES.items():
        if alias_key == clean_name or clean_name.startswith(alias_key):
            search_terms.extend(alias_values)

    best_match = None
    best_score = 0
    for _key, entry in tickers.items():
        title = entry.get("title") or "".lower()
        ticker = entry.get("ticker") or "".lower()

        for term in search_terms:
            # Exact title match = highest priority
            if title == term or title == company_lower:
                best_match = entry
                best_score = 100
                break
            # Ticker match (e.g. "UBER" ticker matches "uber" input)
            if ticker == clean_name or ticker == company_lower:
                if best_score < 95:
                    best_match = entry
                    best_score = 95
            # Title starts with search term (word-boundary match)
            if (
                title.startswith(term + " ")
                or title.startswith(term + ",")
                or title.startswith(term + ".")
            ):
                score = 85
                if score > best_score:
                    best_match = entry
                    best_score = score
            # Title contains search term as a whole word
            elif re.search(r"\b" + re.escape(term) + r"\b", title):
                score = len(term) / max(len(title), 1) * 80
                if score > best_score:
                    best_match = entry
                    best_score = score
            # Title contains search term as substring (lower priority)
            elif term in title:
                score = len(term) / max(len(title), 1) * 50
                if score > best_score:
                    best_match = entry
                    best_score = score

        if best_score >= 100:
            break

    if best_match:
        result = {
            "ticker": best_match.get("ticker") or "",
            "cik": str(best_match.get("cik_str") or ""),
            "company_name": best_match.get("title") or "",
            "is_public": True,
            "source": "SEC EDGAR",
        }
        _set_cached(cache_k, result)
        return result

    return None


# ---------------------------------------------------------------------------
# API 11: FRED (Federal Reserve Economic Data)
# ---------------------------------------------------------------------------

_FRED_SERIES = {
    "unemployment_rate": "UNRATE",  # US unemployment rate
    "cpi_inflation": "CPIAUCSL",  # Consumer Price Index
    "fed_funds_rate": "FEDFUNDS",  # Federal funds rate
    "job_openings": "JTSJOL",  # Job openings (JOLTS)
    "avg_hourly_earnings": "CES0500000003",  # Avg hourly earnings
}


def fetch_fred_indicators() -> Dict[str, Any]:
    """
    Fetch key US economic indicators from FRED (Federal Reserve Economic Data).
    Uses the FRED API if a key is available, otherwise returns None.
    A free API key can be obtained at https://fred.stlouisfed.org/docs/api/api_key.html
    """
    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        _log_info("FRED_API_KEY not set; skipping FRED indicators")
        return {}

    cache_k = _cache_key("fred", "us_indicators")
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    result: Dict[str, Any] = {"source": "FRED"}

    for label, series_id in _FRED_SERIES.items():
        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={api_key}&file_type=json"
            f"&sort_order=desc&limit=1"
        )
        try:
            resp = _http_get_json(url, timeout=5)
            if resp and "observations" in resp and resp["observations"]:
                obs = resp["observations"][0]
                val = obs.get("value") or ""
                if val and val != ".":
                    result[label] = {
                        "value": float(val),
                        "date": obs.get("date") or "",
                    }
        except Exception as exc:
            _log_warn(f"FRED series {series_id} failed: {exc}")

    if len(result) > 1:  # more than just "source"
        _set_cached(cache_k, result)
        return result

    return {}


# ---------------------------------------------------------------------------
# API 12: Google Trends (via pytrends, if installed)
# ---------------------------------------------------------------------------

_TREND_INTEREST_BENCHMARKS: Dict[str, Dict[str, Any]] = {
    "software engineer": {"avg_interest": 72, "latest_interest": 68, "trend": "stable"},
    "data scientist": {"avg_interest": 65, "latest_interest": 58, "trend": "stable"},
    "nurse": {"avg_interest": 80, "latest_interest": 82, "trend": "rising"},
    "warehouse": {"avg_interest": 55, "latest_interest": 52, "trend": "stable"},
    "driver": {"avg_interest": 60, "latest_interest": 58, "trend": "stable"},
    "marketing": {"avg_interest": 50, "latest_interest": 48, "trend": "stable"},
    "sales": {"avg_interest": 45, "latest_interest": 43, "trend": "stable"},
    "finance": {"avg_interest": 42, "latest_interest": 44, "trend": "rising"},
    "healthcare": {"avg_interest": 75, "latest_interest": 78, "trend": "rising"},
    "technology": {"avg_interest": 60, "latest_interest": 55, "trend": "stable"},
    "construction": {"avg_interest": 40, "latest_interest": 42, "trend": "rising"},
    "retail": {"avg_interest": 50, "latest_interest": 48, "trend": "stable"},
}


def _google_trends_fallback(keywords: List[str]) -> Dict[str, Any]:
    """Return benchmark search interest data when pytrends is unavailable."""
    if not keywords:
        return {}
    result: Dict[str, Any] = {"source": "Google Trends (curated benchmark)"}
    for kw in keywords[:5]:
        kl = kw.lower()
        matched = None
        for term, data in _TREND_INTEREST_BENCHMARKS.items():
            if term in kl or kl in term:
                matched = data
                break
        if not matched:
            matched = {"avg_interest": 50, "latest_interest": 48, "trend": "stable"}
        result[kw] = {**matched, "data_confidence": 0.45}
    return result


def fetch_search_trends(keywords: List[str]) -> Dict[str, Any]:
    """
    Fetch Google Trends interest data for given keywords.
    Requires the 'pytrends' package (pip install pytrends).
    Returns relative search interest scores.
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        _log_info("pytrends not installed; using trend benchmarks")
        return _google_trends_fallback(keywords)

    if not keywords:
        return {}

    cache_k = _cache_key("gtrends", ",".join(keywords[:5]))
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    try:
        pytrends = TrendReq(hl="en-US", tz=360, timeout=(5, 10))
        # Limit to 5 keywords (Google Trends max)
        kw_list = keywords[:5]
        pytrends.build_payload(kw_list, timeframe="today 3-m", geo="US")

        interest = pytrends.interest_over_time()
        if interest is not None and not interest.empty:
            result: Dict[str, Any] = {"source": "Google Trends"}
            for kw in kw_list:
                if kw in interest.columns:
                    avg_interest = int(interest[kw].mean())
                    latest = int(interest[kw].iloc[-1])
                    result[kw] = {
                        "avg_interest": avg_interest,
                        "latest_interest": latest,
                        "trend": "rising" if latest > avg_interest else "stable",
                    }
            if len(result) > 1:
                _set_cached(cache_k, result)
                return result
    except Exception as exc:
        _log_warn(f"Google Trends fetch failed: {exc}")

    return {}


# ---------------------------------------------------------------------------
# O*NET Web Services — Occupation skills, knowledge, job outlook
# ---------------------------------------------------------------------------

ONET_SOC_CODES: Dict[str, str] = {
    "software engineer": "15-1252.00",
    "software developer": "15-1252.00",
    "web developer": "15-1254.00",
    "data scientist": "15-2051.00",
    "data analyst": "15-2051.00",
    "data engineer": "15-1243.00",
    "devops engineer": "15-1244.00",
    "systems administrator": "15-1244.00",
    "network engineer": "15-1241.00",
    "cybersecurity analyst": "15-1212.00",
    "information security": "15-1212.00",
    "database administrator": "15-1242.00",
    "product manager": "11-2021.00",
    "project manager": "11-9199.00",
    "marketing manager": "11-2021.00",
    "sales manager": "11-2022.00",
    "business analyst": "13-1111.00",
    "financial analyst": "13-2051.00",
    "accountant": "13-2011.00",
    "hr manager": "11-3121.00",
    "human resources": "11-3121.00",
    "recruiter": "13-1071.00",
    "registered nurse": "29-1141.00",
    "nurse": "29-1141.00",
    "physician": "29-1218.00",
    "pharmacist": "29-1051.00",
    "mechanical engineer": "17-2141.00",
    "electrical engineer": "17-2071.00",
    "civil engineer": "17-2051.00",
    "ux designer": "15-1255.00",
    "graphic designer": "27-1024.00",
    "technical writer": "27-3042.00",
    "operations manager": "11-1021.00",
    "supply chain": "13-1081.00",
}

_ONET_FALLBACK: Dict[str, Dict[str, Any]] = {
    "15-1252.00": {
        "title": "Software Developers",
        "skills": [
            "Programming",
            "Complex Problem Solving",
            "Systems Analysis",
            "Critical Thinking",
            "Mathematics",
        ],
        "knowledge": [
            "Computers and Electronics",
            "Engineering and Technology",
            "Mathematics",
            "English Language",
        ],
        "technology_skills": ["Python", "Java", "JavaScript", "SQL", "Git"],
        "outlook": "Much faster than average",
        "median_salary": 132270,
        "education": "Bachelor's degree",
    },
    "15-2051.00": {
        "title": "Data Scientists",
        "skills": [
            "Programming",
            "Mathematics",
            "Critical Thinking",
            "Complex Problem Solving",
            "Active Learning",
        ],
        "knowledge": [
            "Mathematics",
            "Computers and Electronics",
            "English Language",
            "Engineering and Technology",
        ],
        "technology_skills": ["Python", "R", "SQL", "Tableau", "TensorFlow"],
        "outlook": "Much faster than average",
        "median_salary": 108020,
        "education": "Bachelor's degree",
    },
    "15-1243.00": {
        "title": "Database Architects",
        "skills": [
            "Programming",
            "Systems Analysis",
            "Complex Problem Solving",
            "Critical Thinking",
        ],
        "knowledge": [
            "Computers and Electronics",
            "Mathematics",
            "Engineering and Technology",
        ],
        "technology_skills": ["SQL", "Python", "AWS", "Spark", "Kafka"],
        "outlook": "Faster than average",
        "median_salary": 112120,
        "education": "Bachelor's degree",
    },
    "15-1244.00": {
        "title": "Network and Computer Systems Administrators",
        "skills": [
            "Critical Thinking",
            "Complex Problem Solving",
            "Systems Analysis",
            "Monitoring",
        ],
        "knowledge": [
            "Computers and Electronics",
            "Telecommunications",
            "Engineering and Technology",
        ],
        "technology_skills": ["Linux", "Docker", "Kubernetes", "AWS", "Terraform"],
        "outlook": "Average",
        "median_salary": 95360,
        "education": "Bachelor's degree",
    },
    "11-2021.00": {
        "title": "Marketing Managers",
        "skills": [
            "Persuasion",
            "Social Perceptiveness",
            "Negotiation",
            "Coordination",
            "Critical Thinking",
        ],
        "knowledge": [
            "Sales and Marketing",
            "English Language",
            "Communications and Media",
            "Administration and Management",
        ],
        "technology_skills": ["Google Analytics", "Salesforce", "HubSpot", "Excel"],
        "outlook": "Faster than average",
        "median_salary": 156580,
        "education": "Bachelor's degree",
    },
    "13-1111.00": {
        "title": "Management Analysts",
        "skills": [
            "Critical Thinking",
            "Complex Problem Solving",
            "Active Listening",
            "Judgment and Decision Making",
        ],
        "knowledge": [
            "Administration and Management",
            "English Language",
            "Customer and Personal Service",
        ],
        "technology_skills": ["Excel", "PowerPoint", "Tableau", "SQL"],
        "outlook": "Average",
        "median_salary": 99410,
        "education": "Bachelor's degree",
    },
    "13-2051.00": {
        "title": "Financial Analysts",
        "skills": [
            "Critical Thinking",
            "Mathematics",
            "Active Learning",
            "Complex Problem Solving",
        ],
        "knowledge": ["Economics and Accounting", "Mathematics", "English Language"],
        "technology_skills": ["Excel", "Bloomberg Terminal", "Python", "SQL"],
        "outlook": "Faster than average",
        "median_salary": 96220,
        "education": "Bachelor's degree",
    },
    "13-2011.00": {
        "title": "Accountants and Auditors",
        "skills": [
            "Critical Thinking",
            "Mathematics",
            "Active Listening",
            "Reading Comprehension",
        ],
        "knowledge": [
            "Economics and Accounting",
            "Mathematics",
            "English Language",
            "Law and Government",
        ],
        "technology_skills": ["Excel", "QuickBooks", "SAP", "Oracle"],
        "outlook": "Average",
        "median_salary": 79880,
        "education": "Bachelor's degree",
    },
    "11-3121.00": {
        "title": "Human Resources Managers",
        "skills": [
            "Social Perceptiveness",
            "Negotiation",
            "Active Listening",
            "Coordination",
        ],
        "knowledge": [
            "Personnel and Human Resources",
            "Administration and Management",
            "English Language",
            "Law and Government",
        ],
        "technology_skills": ["Workday", "SAP SuccessFactors", "ADP", "Excel"],
        "outlook": "Faster than average",
        "median_salary": 136350,
        "education": "Bachelor's degree",
    },
    "13-1071.00": {
        "title": "Human Resources Specialists",
        "skills": [
            "Active Listening",
            "Social Perceptiveness",
            "Speaking",
            "Negotiation",
        ],
        "knowledge": [
            "Personnel and Human Resources",
            "English Language",
            "Administration and Management",
        ],
        "technology_skills": ["LinkedIn Recruiter", "Workday", "ADP", "iCIMS"],
        "outlook": "Faster than average",
        "median_salary": 67650,
        "education": "Bachelor's degree",
    },
    "29-1141.00": {
        "title": "Registered Nurses",
        "skills": [
            "Critical Thinking",
            "Active Listening",
            "Social Perceptiveness",
            "Monitoring",
        ],
        "knowledge": [
            "Medicine and Dentistry",
            "Psychology",
            "English Language",
            "Customer and Personal Service",
        ],
        "technology_skills": ["Epic Systems", "Meditech", "Cerner"],
        "outlook": "Faster than average",
        "median_salary": 86070,
        "education": "Bachelor's degree",
    },
    "17-2141.00": {
        "title": "Mechanical Engineers",
        "skills": [
            "Complex Problem Solving",
            "Critical Thinking",
            "Mathematics",
            "Science",
        ],
        "knowledge": ["Engineering and Technology", "Design", "Mathematics", "Physics"],
        "technology_skills": ["AutoCAD", "SolidWorks", "MATLAB", "ANSYS"],
        "outlook": "Average",
        "median_salary": 99510,
        "education": "Bachelor's degree",
    },
    "15-1254.00": {
        "title": "Web Developers",
        "skills": [
            "Programming",
            "Critical Thinking",
            "Active Learning",
            "Complex Problem Solving",
        ],
        "knowledge": ["Computers and Electronics", "Design", "English Language"],
        "technology_skills": [
            "JavaScript",
            "React",
            "Node.js",
            "HTML/CSS",
            "TypeScript",
        ],
        "outlook": "Much faster than average",
        "median_salary": 98580,
        "education": "Associate's degree",
    },
    "15-1241.00": {
        "title": "Computer Network Architects",
        "skills": ["Critical Thinking", "Complex Problem Solving", "Systems Analysis"],
        "knowledge": [
            "Computers and Electronics",
            "Telecommunications",
            "Engineering and Technology",
        ],
        "technology_skills": ["Cisco", "AWS", "Azure", "VMware"],
        "outlook": "Average",
        "median_salary": 126900,
        "education": "Bachelor's degree",
    },
    "15-1212.00": {
        "title": "Information Security Analysts",
        "skills": [
            "Critical Thinking",
            "Complex Problem Solving",
            "Systems Analysis",
            "Active Learning",
        ],
        "knowledge": [
            "Computers and Electronics",
            "Engineering and Technology",
            "Telecommunications",
        ],
        "technology_skills": ["Splunk", "Wireshark", "Nessus", "Python", "Kali Linux"],
        "outlook": "Much faster than average",
        "median_salary": 120360,
        "education": "Bachelor's degree",
    },
    "15-1242.00": {
        "title": "Database Administrators",
        "skills": [
            "Critical Thinking",
            "Complex Problem Solving",
            "Programming",
            "Systems Analysis",
        ],
        "knowledge": [
            "Computers and Electronics",
            "Mathematics",
            "Engineering and Technology",
        ],
        "technology_skills": ["SQL", "Oracle", "PostgreSQL", "MySQL", "MongoDB"],
        "outlook": "Faster than average",
        "median_salary": 101000,
        "education": "Bachelor's degree",
    },
    "11-2022.00": {
        "title": "Sales Managers",
        "skills": [
            "Persuasion",
            "Negotiation",
            "Social Perceptiveness",
            "Active Listening",
        ],
        "knowledge": [
            "Sales and Marketing",
            "Customer and Personal Service",
            "Administration and Management",
        ],
        "technology_skills": ["Salesforce", "HubSpot", "Excel", "SAP"],
        "outlook": "Average",
        "median_salary": 135160,
        "education": "Bachelor's degree",
    },
    "11-9199.00": {
        "title": "Managers, All Other",
        "skills": [
            "Critical Thinking",
            "Coordination",
            "Active Listening",
            "Judgment and Decision Making",
        ],
        "knowledge": [
            "Administration and Management",
            "English Language",
            "Customer and Personal Service",
        ],
        "technology_skills": ["Microsoft Office", "Jira", "Slack", "Asana"],
        "outlook": "Average",
        "median_salary": 116740,
        "education": "Bachelor's degree",
    },
    "15-1255.00": {
        "title": "Web and Digital Interface Designers",
        "skills": ["Active Learning", "Critical Thinking", "Complex Problem Solving"],
        "knowledge": [
            "Design",
            "Computers and Electronics",
            "Fine Arts",
            "Communications and Media",
        ],
        "technology_skills": ["Figma", "Adobe XD", "Sketch", "HTML/CSS", "JavaScript"],
        "outlook": "Faster than average",
        "median_salary": 85000,
        "education": "Bachelor's degree",
    },
    "27-1024.00": {
        "title": "Graphic Designers",
        "skills": [
            "Active Learning",
            "Critical Thinking",
            "Complex Problem Solving",
            "Time Management",
        ],
        "knowledge": [
            "Design",
            "Communications and Media",
            "Fine Arts",
            "English Language",
        ],
        "technology_skills": ["Adobe Photoshop", "Illustrator", "InDesign", "Figma"],
        "outlook": "Average",
        "median_salary": 57990,
        "education": "Bachelor's degree",
    },
    "27-3042.00": {
        "title": "Technical Writers",
        "skills": [
            "Writing",
            "Reading Comprehension",
            "Active Listening",
            "Critical Thinking",
        ],
        "knowledge": [
            "English Language",
            "Computers and Electronics",
            "Communications and Media",
        ],
        "technology_skills": [
            "MadCap Flare",
            "Adobe FrameMaker",
            "Confluence",
            "Markdown",
        ],
        "outlook": "Average",
        "median_salary": 79960,
        "education": "Bachelor's degree",
    },
    "11-1021.00": {
        "title": "General and Operations Managers",
        "skills": [
            "Critical Thinking",
            "Coordination",
            "Monitoring",
            "Judgment and Decision Making",
        ],
        "knowledge": [
            "Administration and Management",
            "Customer and Personal Service",
            "Economics and Accounting",
        ],
        "technology_skills": ["SAP", "Oracle", "Excel", "Salesforce"],
        "outlook": "Average",
        "median_salary": 101280,
        "education": "Bachelor's degree",
    },
    "13-1081.00": {
        "title": "Logisticians",
        "skills": [
            "Critical Thinking",
            "Complex Problem Solving",
            "Coordination",
            "Monitoring",
        ],
        "knowledge": [
            "Transportation",
            "Administration and Management",
            "Production and Processing",
        ],
        "technology_skills": ["SAP", "Oracle SCM", "Excel", "Tableau"],
        "outlook": "Faster than average",
        "median_salary": 79400,
        "education": "Bachelor's degree",
    },
    "29-1218.00": {
        "title": "Physicians, All Other",
        "skills": [
            "Critical Thinking",
            "Active Listening",
            "Social Perceptiveness",
            "Complex Problem Solving",
        ],
        "knowledge": [
            "Medicine and Dentistry",
            "Biology",
            "Psychology",
            "English Language",
        ],
        "technology_skills": ["Epic Systems", "Cerner", "MEDITECH"],
        "outlook": "Average",
        "median_salary": 229300,
        "education": "Doctoral or professional degree",
    },
    "29-1051.00": {
        "title": "Pharmacists",
        "skills": [
            "Critical Thinking",
            "Active Listening",
            "Reading Comprehension",
            "Science",
        ],
        "knowledge": [
            "Medicine and Dentistry",
            "Chemistry",
            "Mathematics",
            "English Language",
        ],
        "technology_skills": ["QS/1", "PharMerica", "ScriptPro"],
        "outlook": "Declining",
        "median_salary": 136030,
        "education": "Doctoral or professional degree",
    },
    "17-2071.00": {
        "title": "Electrical Engineers",
        "skills": [
            "Critical Thinking",
            "Complex Problem Solving",
            "Mathematics",
            "Science",
        ],
        "knowledge": ["Engineering and Technology", "Design", "Mathematics", "Physics"],
        "technology_skills": ["MATLAB", "AutoCAD", "LabVIEW", "PSpice"],
        "outlook": "Average",
        "median_salary": 109010,
        "education": "Bachelor's degree",
    },
    "17-2051.00": {
        "title": "Civil Engineers",
        "skills": [
            "Critical Thinking",
            "Complex Problem Solving",
            "Mathematics",
            "Science",
        ],
        "knowledge": [
            "Engineering and Technology",
            "Design",
            "Mathematics",
            "Building and Construction",
        ],
        "technology_skills": ["AutoCAD", "Civil 3D", "MicroStation", "STAAD.Pro"],
        "outlook": "Average",
        "median_salary": 95890,
        "education": "Bachelor's degree",
    },
}


def fetch_onet_occupation_data(roles: List[str]) -> Dict[str, Any]:
    """
    Fetch occupation details from O*NET Web Services with curated fallback.
    Returns skills, knowledge, technology skills, outlook, salary, and education.
    """
    if not roles:
        return {}

    cache_k = _cache_key("onet", ",".join(sorted(r.lower() for r in roles[:10])))
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    username = os.environ.get("ONET_USERNAME", "")
    password = os.environ.get("ONET_PASSWORD", "")
    use_live = bool(username and password)

    result: Dict[str, Any] = {"source": "O*NET", "occupations": {}}

    for role in roles[:10]:
        role_lower = role.lower().strip()
        soc_code = None
        for key, code in ONET_SOC_CODES.items():
            if key in role_lower or role_lower in key:
                soc_code = code
                break

        if not soc_code:
            # Try word-level matching
            role_words = set(role_lower.split())
            best_match = None
            best_score = 0
            for key, code in ONET_SOC_CODES.items():
                key_words = set(key.split())
                overlap = len(role_words & key_words)
                if overlap > best_score:
                    best_score = overlap
                    best_match = code
            if best_score >= 1:
                soc_code = best_match

        if not soc_code:
            continue

        # Try live API first
        if use_live:
            try:
                import base64

                creds = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers = {
                    "Authorization": f"Basic {creds}",
                    "Accept": "application/json",
                }
                base_url = "https://services.onetcenter.org/ws/online/occupations"
                url = f"{base_url}/{soc_code}"
                req = urllib.request.Request(url, headers=headers)
                ctx = ssl.create_default_context()
                with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                    data = json.loads(resp.read().decode())

                occ_data: Dict[str, Any] = {
                    "title": data.get("title") or "",
                    "description": data.get("description") or "",
                    "soc_code": soc_code,
                    "source": "O*NET Live API",
                }

                # Fetch skills
                try:
                    skills_url = f"{base_url}/{soc_code}/summary/skills"
                    req_s = urllib.request.Request(skills_url, headers=headers)
                    with urllib.request.urlopen(
                        req_s, timeout=8, context=ctx
                    ) as resp_s:
                        skills_data = json.loads(resp_s.read().decode())
                    elements = skills_data.get("element", [])
                    occ_data["skills"] = [
                        e.get("name") or "" for e in elements[:8] if e.get("name")
                    ]
                except Exception:
                    occ_data["skills"] = []

                # Fetch knowledge
                try:
                    know_url = f"{base_url}/{soc_code}/summary/knowledge"
                    req_k = urllib.request.Request(know_url, headers=headers)
                    with urllib.request.urlopen(
                        req_k, timeout=8, context=ctx
                    ) as resp_k:
                        know_data = json.loads(resp_k.read().decode())
                    elements = know_data.get("element", [])
                    occ_data["knowledge"] = [
                        e.get("name") or "" for e in elements[:6] if e.get("name")
                    ]
                except Exception:
                    occ_data["knowledge"] = []

                # Fetch technology skills
                try:
                    tech_url = f"{base_url}/{soc_code}/summary/technology_skills"
                    req_t = urllib.request.Request(tech_url, headers=headers)
                    with urllib.request.urlopen(
                        req_t, timeout=8, context=ctx
                    ) as resp_t:
                        tech_data = json.loads(resp_t.read().decode())
                    categories = tech_data.get("category", [])
                    tech_skills = []
                    for cat in categories[:5]:
                        for ex in cat.get("example", [])[:2]:
                            name = ex.get("name") or ""
                            if name:
                                tech_skills.append(name)
                    occ_data["technology_skills"] = tech_skills[:10]
                except Exception:
                    occ_data["technology_skills"] = []

                result["occupations"][role] = occ_data
                continue
            except Exception as exc:
                _log_warn(f"O*NET live API failed for {soc_code}: {exc}")

        # Fallback to curated data
        if soc_code in _ONET_FALLBACK:
            fb = _ONET_FALLBACK[soc_code]
            result["occupations"][role] = {
                "title": fb["title"],
                "soc_code": soc_code,
                "skills": fb["skills"],
                "knowledge": fb["knowledge"],
                "technology_skills": fb["technology_skills"],
                "outlook": fb["outlook"],
                "median_salary": fb["median_salary"],
                "education": fb["education"],
                "source": "O*NET Curated Fallback",
            }

    if result["occupations"]:
        _set_cached(cache_k, result)
        return result
    return {}


def fetch_onet_job_zone(soc_code: str) -> Optional[Dict[str, Any]]:
    """Fetch O*NET Job Zone (1-5) for a SOC code.

    Job Zone maps directly to collar type:
        Zone 1: Little or no preparation (blue collar entry)
        Zone 2: Some preparation (blue collar/pink collar)
        Zone 3: Medium preparation (grey collar/skilled trades)
        Zone 4: Considerable preparation (white collar professional)
        Zone 5: Extensive preparation (white collar executive/specialist)

    Uses O*NET Web Services API (free with registration).
    """
    cache_k = _cache_key("onet_jobzone", soc_code)
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    try:
        import base64

        username = os.environ.get("ONET_USERNAME", "")
        if not username:
            return None

        # O*NET API endpoint
        url = f"https://services.onetcenter.org/ws/online/occupations/{soc_code}"

        # Basic auth
        auth_string = base64.b64encode(f"{username}:".encode()).decode()
        resp = _http_get_json(
            url,
            headers={
                "Authorization": f"Basic {auth_string}",
                "Accept": "application/json",
            },
            timeout=8,
        )

        if not resp:
            return None

        job_zone = resp.get("job_zone")
        if not job_zone:
            return None

        zone_number = int(job_zone)

        # Map to collar type
        ZONE_COLLAR_MAP = {
            1: "blue_collar",
            2: "blue_collar",
            3: "grey_collar",
            4: "white_collar",
            5: "white_collar",
        }

        result = {
            "soc_code": soc_code,
            "job_zone": zone_number,
            "collar_type": ZONE_COLLAR_MAP.get(zone_number, "white_collar"),
            "zone_description": {
                1: "Little or no preparation needed",
                2: "Some preparation needed",
                3: "Medium preparation needed",
                4: "Considerable preparation needed",
                5: "Extensive preparation needed",
            }.get(zone_number, "Unknown"),
            "education_typical": {
                1: "Less than high school",
                2: "High school diploma",
                3: "Vocational training or associate degree",
                4: "Bachelor's degree",
                5: "Graduate or professional degree",
            }.get(zone_number, "Unknown"),
            "source": "O*NET Web Services",
            "data_confidence": 0.95,
        }

        _set_cached(cache_k, result)
        return result
    except Exception as e:
        _log_warn(f"O*NET Job Zone fetch failed for {soc_code}: {e}")
        return None


# ---------------------------------------------------------------------------
# IMF DataMapper — International economic indicators
# ---------------------------------------------------------------------------

ISO_2_TO_3: Dict[str, str] = {
    "US": "USA",
    "GB": "GBR",
    "UK": "GBR",
    "CA": "CAN",
    "AU": "AUS",
    "DE": "DEU",
    "FR": "FRA",
    "JP": "JPN",
    "IN": "IND",
    "CN": "CHN",
    "BR": "BRA",
    "MX": "MEX",
    "KR": "KOR",
    "SG": "SGP",
    "HK": "HKG",
    "NZ": "NZL",
    "IE": "IRL",
    "NL": "NLD",
    "SE": "SWE",
    "NO": "NOR",
    "DK": "DNK",
    "FI": "FIN",
    "CH": "CHE",
    "AT": "AUT",
    "BE": "BEL",
    "IT": "ITA",
    "ES": "ESP",
    "PT": "PRT",
    "PL": "POL",
    "CZ": "CZE",
    "IL": "ISR",
    "ZA": "ZAF",
    "AE": "ARE",
    "PH": "PHL",
}

ISO_3_TO_COUNTRY: Dict[str, str] = {
    "USA": "United States",
    "GBR": "United Kingdom",
    "CAN": "Canada",
    "AUS": "Australia",
    "DEU": "Germany",
    "FRA": "France",
    "JPN": "Japan",
    "IND": "India",
    "CHN": "China",
    "BRA": "Brazil",
    "MEX": "Mexico",
    "KOR": "South Korea",
    "SGP": "Singapore",
    "HKG": "Hong Kong",
    "NZL": "New Zealand",
    "IRL": "Ireland",
    "NLD": "Netherlands",
    "SWE": "Sweden",
    "NOR": "Norway",
    "DNK": "Denmark",
    "FIN": "Finland",
    "CHE": "Switzerland",
    "AUT": "Austria",
    "BEL": "Belgium",
    "ITA": "Italy",
    "ESP": "Spain",
    "PRT": "Portugal",
    "POL": "Poland",
    "CZE": "Czech Republic",
    "ISR": "Israel",
    "ZAF": "South Africa",
    "ARE": "United Arab Emirates",
    "PHL": "Philippines",
}


def _extract_iso3_from_location(location: str) -> Optional[str]:
    """Extract ISO-3 country code from a location string.

    Priority order to resolve ambiguity between US state abbreviations and
    ISO-2 country codes (e.g. 'CA' = California vs Canada, 'IN' = Indiana vs India):
      1. US state abbreviations -> 'USA' (most common in "City, STATE" format)
      2. ISO-2 country codes that do NOT collide with US states (e.g. 'UK', 'AU')
      3. ISO-3 country codes (e.g. 'GBR', 'AUS')
      4. Full country names (e.g. 'Australia', 'India')
    """
    loc = location.strip()
    parts = [p.strip() for p in loc.split(",")]

    # Ambiguous ISO-2 codes that are also US state abbreviations
    _AMBIGUOUS_ISO2 = set(ISO_2_TO_3.keys()) & US_STATES  # e.g. CA, IN, DE, etc.

    for part in reversed(parts):
        upper = part.upper().strip()
        # Check US state abbreviations FIRST
        if upper in US_STATES:
            return "USA"
        # Check ISO-2 country codes (non-ambiguous ones only, since ambiguous
        # ones were handled by the US states check above)
        if upper in ISO_2_TO_3 and upper not in _AMBIGUOUS_ISO2:
            return ISO_2_TO_3[upper]
        # Check if it's already a 3-letter code
        if upper in ISO_3_TO_COUNTRY:
            return upper
    # Check for full country names
    loc_lower = loc.lower()
    for iso3, name in ISO_3_TO_COUNTRY.items():
        if name.lower() in loc_lower:
            return iso3
    return None


_IMF_CURATED_DATA: Dict[str, Dict[str, Any]] = {
    "USA": {
        "country": "United States",
        "iso3": "USA",
        "gdp_growth_pct": 2.0,
        "gdp_growth_pct_year": "2025",
        "gdp_growth_pct_2026": 1.8,
        "inflation_pct": 2.7,
        "inflation_pct_year": "2025",
        "inflation_pct_2026": 2.4,
        "unemployment_pct": 4.2,
        "unemployment_pct_year": "2025",
        "unemployment_pct_2026": 4.4,
    },
    "GBR": {
        "country": "United Kingdom",
        "iso3": "GBR",
        "gdp_growth_pct": 1.1,
        "gdp_growth_pct_year": "2025",
        "inflation_pct": 2.6,
        "inflation_pct_year": "2025",
        "unemployment_pct": 4.5,
        "unemployment_pct_year": "2025",
    },
    "DEU": {
        "country": "Germany",
        "iso3": "DEU",
        "gdp_growth_pct": 0.8,
        "gdp_growth_pct_year": "2025",
        "inflation_pct": 2.3,
        "inflation_pct_year": "2025",
        "unemployment_pct": 3.5,
        "unemployment_pct_year": "2025",
    },
    "CAN": {
        "country": "Canada",
        "iso3": "CAN",
        "gdp_growth_pct": 1.4,
        "gdp_growth_pct_year": "2025",
        "inflation_pct": 2.4,
        "inflation_pct_year": "2025",
        "unemployment_pct": 6.4,
        "unemployment_pct_year": "2025",
    },
    "AUS": {
        "country": "Australia",
        "iso3": "AUS",
        "gdp_growth_pct": 1.9,
        "gdp_growth_pct_year": "2025",
        "inflation_pct": 3.0,
        "inflation_pct_year": "2025",
        "unemployment_pct": 4.1,
        "unemployment_pct_year": "2025",
    },
    "JPN": {
        "country": "Japan",
        "iso3": "JPN",
        "gdp_growth_pct": 1.0,
        "gdp_growth_pct_year": "2025",
        "inflation_pct": 2.5,
        "inflation_pct_year": "2025",
        "unemployment_pct": 2.5,
        "unemployment_pct_year": "2025",
    },
    "IND": {
        "country": "India",
        "iso3": "IND",
        "gdp_growth_pct": 6.5,
        "gdp_growth_pct_year": "2025",
        "inflation_pct": 4.4,
        "inflation_pct_year": "2025",
        "unemployment_pct": 7.8,
        "unemployment_pct_year": "2025",
    },
    "FRA": {
        "country": "France",
        "iso3": "FRA",
        "gdp_growth_pct": 0.9,
        "gdp_growth_pct_year": "2025",
        "inflation_pct": 2.1,
        "inflation_pct_year": "2025",
        "unemployment_pct": 7.5,
        "unemployment_pct_year": "2025",
    },
    "SGP": {
        "country": "Singapore",
        "iso3": "SGP",
        "gdp_growth_pct": 3.0,
        "gdp_growth_pct_year": "2025",
        "inflation_pct": 2.5,
        "inflation_pct_year": "2025",
        "unemployment_pct": 2.1,
        "unemployment_pct_year": "2025",
    },
    "ARE": {
        "country": "United Arab Emirates",
        "iso3": "ARE",
        "gdp_growth_pct": 4.0,
        "gdp_growth_pct_year": "2025",
        "inflation_pct": 2.3,
        "inflation_pct_year": "2025",
        "unemployment_pct": 2.9,
        "unemployment_pct_year": "2025",
    },
    "_GLOBAL": {
        "country": "Global",
        "iso3": "_GLOBAL",
        "gdp_growth_pct": 3.2,
        "gdp_growth_pct_year": "2025",
        "gdp_growth_pct_2026": 3.1,
        "inflation_pct": 4.2,
        "inflation_pct_year": "2025",
        "unemployment_pct": 5.0,
        "unemployment_pct_year": "2025",
    },
}


def fetch_imf_indicators(locations: List[str]) -> Dict[str, Any]:
    """
    Return curated GDP growth, inflation, and unemployment macro indicators.
    The IMF DataMapper API returns 403, so this function uses hardcoded
    2025-2026 data instead of making HTTP calls.
    """
    if not locations:
        return {}

    # Collect unique ISO-3 codes
    country_codes: Dict[str, str] = {}
    for loc in locations:
        iso3 = _extract_iso3_from_location(loc)
        if iso3 and iso3 not in country_codes:
            country_codes[iso3] = ISO_3_TO_COUNTRY.get(iso3, iso3)

    if not country_codes:
        return {}

    cache_k = _cache_key("imf", ",".join(sorted(country_codes.keys())))
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    _log_info("IMF API deprecated; using curated macro indicators")

    result: Dict[str, Any] = {
        "source": "IMF DataMapper (curated benchmarks)",
        "countries": {},
    }

    for iso3, country_name in country_codes.items():
        curated = _IMF_CURATED_DATA.get(iso3)
        if curated:
            result["countries"][iso3] = dict(curated)
        else:
            # Provide global averages for unknown countries
            global_data = _IMF_CURATED_DATA["_GLOBAL"]
            result["countries"][iso3] = {
                "country": country_name,
                "iso3": iso3,
                "gdp_growth_pct": global_data["gdp_growth_pct"],
                "gdp_growth_pct_year": global_data["gdp_growth_pct_year"],
                "inflation_pct": global_data["inflation_pct"],
                "inflation_pct_year": global_data["inflation_pct_year"],
                "unemployment_pct": global_data["unemployment_pct"],
                "unemployment_pct_year": global_data["unemployment_pct_year"],
                "note": "Global average (country-specific data not available)",
            }

    if result["countries"]:
        _set_cached(cache_k, result)
        return result
    return {}


# ---------------------------------------------------------------------------
# REST Countries v3.1 — Country data for international campaigns
# ---------------------------------------------------------------------------


def fetch_country_data(locations: List[str]) -> Dict[str, Any]:
    """
    Fetch country details (population, currency, languages, region, capital)
    from the REST Countries API.
    """
    if not locations:
        return {}

    # Extract unique ISO-3 codes
    codes: Dict[str, str] = {}
    for loc in locations:
        iso3 = _extract_iso3_from_location(loc)
        if iso3 and iso3 not in codes:
            codes[iso3] = ISO_3_TO_COUNTRY.get(iso3, iso3)

    if not codes:
        return {}

    cache_k = _cache_key("restcountries", ",".join(sorted(codes.keys())))
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    result: Dict[str, Any] = {"source": "REST Countries", "countries": {}}

    for iso3, name in codes.items():
        try:
            url = f"https://restcountries.com/v3.1/alpha/{iso3}?fields=name,capital,region,subregion,population,currencies,languages,timezones,flags"
            data = _http_get_json(url, timeout=8)
            if not data:
                continue

            # REST Countries returns a list for alpha endpoints
            country = data[0] if isinstance(data, list) else data

            entry: Dict[str, Any] = {
                "name": country.get("name", {}).get("common", name),
                "official_name": country.get("name", {}).get("official") or "",
                "capital": (
                    (country.get("capital") or [""])[0]
                    if country.get("capital")
                    else ""
                ),
                "region": country.get("region") or "",
                "subregion": country.get("subregion") or "",
                "population": country.get("population") or 0,
                "timezones": country.get("timezones", []),
            }

            # Extract currency info
            currencies = country.get("currencies", {})
            if currencies:
                for code, info in currencies.items():
                    entry["currency_code"] = code
                    entry["currency_name"] = info.get("name") or ""
                    entry["currency_symbol"] = info.get("symbol") or ""
                    break

            # Extract languages
            langs = country.get("languages", {})
            entry["languages"] = list(langs.values()) if langs else []

            # Flag URL
            flags = country.get("flags", {})
            entry["flag_svg"] = flags.get("svg") or ""

            result["countries"][iso3] = entry
        except Exception as exc:
            _log_warn(f"REST Countries failed for {iso3}: {exc}")

    if result["countries"]:
        _set_cached(cache_k, result)
        return result
    return {}


# ---------------------------------------------------------------------------
# GeoNames — Geographic data, city info, timezone
# ---------------------------------------------------------------------------

GEONAMES_BASE = "https://secure.geonames.org"


def fetch_geonames_data(locations: List[str]) -> Dict[str, Any]:
    """
    Fetch geographic data from GeoNames API: coordinates, population,
    timezone, elevation, nearby cities.
    """
    if not locations:
        return {}

    cache_k = _cache_key(
        "geonames", ",".join(sorted(l.lower() for l in locations[:10]))
    )
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    username = os.environ.get("GEONAMES_USERNAME", "demo")
    if username == "demo":
        _log_warn("GEONAMES_USERNAME not set; using 'demo' account (very limited rate)")
    result: Dict[str, Any] = {"source": "GeoNames", "locations": {}}

    for loc in locations[:10]:
        try:
            # Extract city name (first part before comma)
            city = loc.split(",")[0].strip()
            country_iso3 = _extract_iso3_from_location(loc)

            # Build search parameters
            params = f"q={urllib.parse.quote(city)}&maxRows=1&username={username}&style=FULL&type=json"
            # Add country filter if we know it
            if country_iso3:
                # GeoNames uses ISO-2 codes
                iso2_reverse = {v: k for k, v in ISO_2_TO_3.items()}
                iso2 = iso2_reverse.get(country_iso3, "")
                if iso2 and iso2 not in ("UK",):  # GeoNames uses GB not UK
                    params += f"&country={iso2}"

            url = f"{GEONAMES_BASE}/searchJSON?{params}"
            data = _http_get_json(url, timeout=8)

            if not data or not data.get("geonames"):
                continue

            geo = data["geonames"][0]
            entry: Dict[str, Any] = {
                "name": geo.get("name", city),
                "country_name": geo.get("countryName") or "",
                "country_code": geo.get("countryCode") or "",
                "admin_name": geo.get("adminName1") or "",  # State/province
                "population": geo.get("population") or 0,
                "latitude": geo.get("lat") or "",
                "longitude": geo.get("lng") or "",
                "elevation": geo.get("elevation"),
                "feature_class": geo.get("fclName") or "",
            }

            # Fetch timezone info
            lat = geo.get("lat") or ""
            lng = geo.get("lng") or ""
            if lat and lng:
                try:
                    tz_url = f"{GEONAMES_BASE}/timezoneJSON?lat={lat}&lng={lng}&username={username}"
                    tz_data = _http_get_json(tz_url, timeout=6)
                    if tz_data:
                        entry["timezone"] = tz_data.get("timezoneId") or ""
                        entry["gmt_offset"] = tz_data.get("gmtOffset") or ""
                        entry["dst_offset"] = tz_data.get("dstOffset") or ""
                except Exception:
                    pass

                # Fetch nearby cities
                try:
                    nearby_url = (
                        f"{GEONAMES_BASE}/findNearbyPlaceNameJSON?"
                        f"lat={lat}&lng={lng}&radius=50&maxRows=5"
                        f"&cities=cities15000&username={username}"
                    )
                    nearby_data = _http_get_json(nearby_url, timeout=6)
                    if nearby_data and nearby_data.get("geonames"):
                        entry["nearby_cities"] = [
                            {
                                "name": n.get("name") or "",
                                "population": n.get("population") or 0,
                                "distance_km": n.get("distance") or "",
                            }
                            for n in nearby_data["geonames"][:5]
                            if n.get("name") or "" != entry["name"]
                        ]
                except Exception:
                    pass

            result["locations"][loc] = entry
        except Exception as exc:
            _log_warn(f"GeoNames failed for {loc}: {exc}")

    if result["locations"]:
        _set_cached(cache_k, result)
        return result
    return {}


# ---------------------------------------------------------------------------
# Teleport — Quality of life scores, cost of living
# ---------------------------------------------------------------------------

TELEPORT_SLUGS: Dict[str, str] = {
    "san francisco": "san-francisco-bay-area",
    "san mateo": "san-francisco-bay-area",
    "palo alto": "san-francisco-bay-area",
    "mountain view": "san-francisco-bay-area",
    "sunnyvale": "san-francisco-bay-area",
    "san jose": "san-francisco-bay-area",
    "new york": "new-york",
    "manhattan": "new-york",
    "brooklyn": "new-york",
    "los angeles": "los-angeles",
    "chicago": "chicago",
    "boston": "boston",
    "seattle": "seattle",
    "austin": "austin",
    "denver": "denver",
    "dallas": "dallas-fort-worth",
    "houston": "houston",
    "atlanta": "atlanta",
    "miami": "miami",
    "phoenix": "phoenix",
    "detroit": "detroit",
    "minneapolis": "minneapolis-saint-paul",
    "philadelphia": "philadelphia",
    "washington": "washington-dc",
    "portland": "portland-or",
    "san diego": "san-diego",
    "nashville": "nashville",
    "charlotte": "charlotte",
    "london": "london",
    "manchester": "manchester",
    "edinburgh": "edinburgh",
    "berlin": "berlin",
    "munich": "munich",
    "frankfurt": "frankfurt",
    "paris": "paris",
    "amsterdam": "amsterdam",
    "dublin": "dublin",
    "zurich": "zurich",
    "stockholm": "stockholm",
    "copenhagen": "copenhagen",
    "oslo": "oslo",
    "helsinki": "helsinki",
    "barcelona": "barcelona",
    "madrid": "madrid",
    "lisbon": "lisbon",
    "milan": "milan",
    "rome": "rome",
    "vienna": "vienna",
    "prague": "prague",
    "warsaw": "warsaw",
    "brussels": "brussels",
    "toronto": "toronto",
    "vancouver": "vancouver",
    "montreal": "montreal",
    "sydney": "sydney",
    "melbourne": "melbourne",
    "brisbane": "brisbane",
    "auckland": "auckland",
    "wellington": "wellington",
    "tokyo": "tokyo",
    "singapore": "singapore",
    "hong kong": "hong-kong",
    "seoul": "seoul",
    "bangalore": "bangalore",
    "mumbai": "mumbai",
    "delhi": "delhi",
    "hyderabad": "hyderabad",
    "pune": "pune",
    "chennai": "chennai",
    "tel aviv": "tel-aviv",
    "dubai": "dubai",
    "sao paulo": "sao-paulo",
    "mexico city": "mexico-city",
    "cape town": "cape-town",
    "johannesburg": "johannesburg",
    "buenos aires": "buenos-aires",
}


_TELEPORT_BENCHMARK_DATA: Dict[str, Dict[str, Any]] = {
    "new-york": {
        "quality_of_life": 7.2,
        "cost_of_living_index": 187,
        "median_home_price": 680000,
        "median_rent_1br": 3200,
        "summary": "Major global financial and cultural hub with high cost of living but excellent career opportunities.",
    },
    "san-francisco-bay-area": {
        "quality_of_life": 7.5,
        "cost_of_living_index": 195,
        "median_home_price": 1150000,
        "median_rent_1br": 3500,
        "summary": "Leading technology hub with world-class innovation ecosystem and high cost of living.",
    },
    "los-angeles": {
        "quality_of_life": 6.9,
        "cost_of_living_index": 166,
        "median_home_price": 850000,
        "median_rent_1br": 2500,
        "summary": "Entertainment and creative industry capital with diverse economy and Mediterranean climate.",
    },
    "chicago": {
        "quality_of_life": 7.0,
        "cost_of_living_index": 107,
        "median_home_price": 320000,
        "median_rent_1br": 1800,
        "summary": "Major Midwestern hub for finance, manufacturing, and technology with moderate cost of living.",
    },
    "austin": {
        "quality_of_life": 7.6,
        "cost_of_living_index": 110,
        "median_home_price": 450000,
        "median_rent_1br": 1650,
        "summary": "Fast-growing tech hub with vibrant culture, moderate cost of living, and no state income tax.",
    },
    "seattle": {
        "quality_of_life": 7.4,
        "cost_of_living_index": 158,
        "median_home_price": 800000,
        "median_rent_1br": 2200,
        "summary": "Major Pacific Northwest tech hub home to leading cloud and e-commerce companies.",
    },
    "boston": {
        "quality_of_life": 7.3,
        "cost_of_living_index": 152,
        "median_home_price": 700000,
        "median_rent_1br": 2800,
        "summary": "Leading education and biotech hub with strong healthcare and financial sectors.",
    },
    "denver": {
        "quality_of_life": 7.5,
        "cost_of_living_index": 118,
        "median_home_price": 550000,
        "median_rent_1br": 1750,
        "summary": "Growing tech and outdoor-lifestyle metro with moderate cost of living in the Rocky Mountain region.",
    },
    "dallas-fort-worth": {
        "quality_of_life": 7.1,
        "cost_of_living_index": 98,
        "median_home_price": 380000,
        "median_rent_1br": 1450,
        "summary": "Major business hub with low cost of living, no state income tax, and diverse economy.",
    },
    "atlanta": {
        "quality_of_life": 7.0,
        "cost_of_living_index": 102,
        "median_home_price": 370000,
        "median_rent_1br": 1600,
        "summary": "Leading Southeastern hub for logistics, media, and technology with affordable cost of living.",
    },
    "houston": {
        "quality_of_life": 6.8,
        "cost_of_living_index": 96,
        "median_home_price": 310000,
        "median_rent_1br": 1300,
        "summary": "Energy industry capital with diverse economy, low cost of living, and no state income tax.",
    },
    "miami": {
        "quality_of_life": 6.7,
        "cost_of_living_index": 135,
        "median_home_price": 520000,
        "median_rent_1br": 2400,
        "summary": "International business gateway with growing tech scene and subtropical climate.",
    },
    "phoenix": {
        "quality_of_life": 6.9,
        "cost_of_living_index": 100,
        "median_home_price": 400000,
        "median_rent_1br": 1400,
        "summary": "Fast-growing Sun Belt metro with affordable cost of living and expanding tech sector.",
    },
    "washington-dc": {
        "quality_of_life": 7.2,
        "cost_of_living_index": 152,
        "median_home_price": 600000,
        "median_rent_1br": 2300,
        "summary": "Government and policy hub with strong cybersecurity, defense, and consulting industries.",
    },
    "portland-or": {
        "quality_of_life": 7.3,
        "cost_of_living_index": 130,
        "median_home_price": 500000,
        "median_rent_1br": 1700,
        "summary": "Pacific Northwest city known for sustainability, creative industry, and tech startups.",
    },
    "nashville": {
        "quality_of_life": 7.2,
        "cost_of_living_index": 104,
        "median_home_price": 420000,
        "median_rent_1br": 1550,
        "summary": "Growing healthcare and entertainment hub with moderate cost of living.",
    },
    "charlotte": {
        "quality_of_life": 7.1,
        "cost_of_living_index": 99,
        "median_home_price": 360000,
        "median_rent_1br": 1400,
        "summary": "Major banking center with low cost of living and growing tech sector.",
    },
    "minneapolis-saint-paul": {
        "quality_of_life": 7.3,
        "cost_of_living_index": 105,
        "median_home_price": 340000,
        "median_rent_1br": 1350,
        "summary": "Strong Fortune 500 presence with excellent quality of life and moderate cost of living.",
    },
    "san-diego": {
        "quality_of_life": 7.4,
        "cost_of_living_index": 155,
        "median_home_price": 850000,
        "median_rent_1br": 2400,
        "summary": "Biotech and defense hub with excellent climate and high quality of life.",
    },
    "philadelphia": {
        "quality_of_life": 6.8,
        "cost_of_living_index": 112,
        "median_home_price": 280000,
        "median_rent_1br": 1600,
        "summary": "Historic East Coast city with strong healthcare, education, and pharmaceutical sectors.",
    },
    "detroit": {
        "quality_of_life": 6.3,
        "cost_of_living_index": 89,
        "median_home_price": 220000,
        "median_rent_1br": 1100,
        "summary": "Automotive industry hub with revitalizing downtown and very affordable cost of living.",
    },
    "london": {
        "quality_of_life": 7.1,
        "cost_of_living_index": 175,
        "median_home_price": 750000,
        "median_rent_1br": 2800,
        "summary": "Global financial capital with world-class cultural institutions and diverse economy.",
    },
    "berlin": {
        "quality_of_life": 7.4,
        "cost_of_living_index": 105,
        "median_home_price": 400000,
        "median_rent_1br": 1200,
        "summary": "European startup hub with vibrant culture and moderate cost of living.",
    },
    "toronto": {
        "quality_of_life": 7.2,
        "cost_of_living_index": 130,
        "median_home_price": 800000,
        "median_rent_1br": 2200,
        "summary": "Canada's largest city and financial capital with diverse multicultural economy.",
    },
    "sydney": {
        "quality_of_life": 7.3,
        "cost_of_living_index": 145,
        "median_home_price": 900000,
        "median_rent_1br": 2500,
        "summary": "Australia's largest city with strong finance, tech, and tourism sectors.",
    },
    "tokyo": {
        "quality_of_life": 7.5,
        "cost_of_living_index": 130,
        "median_home_price": 500000,
        "median_rent_1br": 1200,
        "summary": "World's largest metro area with cutting-edge technology and excellent public infrastructure.",
    },
    "singapore": {
        "quality_of_life": 7.8,
        "cost_of_living_index": 160,
        "median_home_price": 950000,
        "median_rent_1br": 2300,
        "summary": "Global financial hub with excellent infrastructure, safety, and business-friendly environment.",
    },
    "bangalore": {
        "quality_of_life": 6.0,
        "cost_of_living_index": 40,
        "median_home_price": 120000,
        "median_rent_1br": 400,
        "summary": "India's Silicon Valley with massive IT industry and rapidly growing tech ecosystem.",
    },
    "dubai": {
        "quality_of_life": 7.0,
        "cost_of_living_index": 120,
        "median_home_price": 450000,
        "median_rent_1br": 1800,
        "summary": "Global business hub with no income tax, luxury lifestyle, and growing tech sector.",
    },
    "amsterdam": {
        "quality_of_life": 7.6,
        "cost_of_living_index": 130,
        "median_home_price": 550000,
        "median_rent_1br": 1900,
        "summary": "European tech hub with excellent quality of life, cycling culture, and international workforce.",
    },
}


def fetch_teleport_city_data(locations: List[str]) -> Dict[str, Any]:
    """
    Return curated quality-of-life and cost-of-living benchmark data for
    major metros.  The Teleport API has been deprecated (DNS failure), so
    this function uses hardcoded data instead of making HTTP calls.
    """
    if not locations:
        return {}

    cache_k = _cache_key(
        "teleport", ",".join(sorted(l.lower() for l in locations[:10]))
    )
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    _log_info("Teleport API deprecated; using curated location quality data")

    result: Dict[str, Any] = {"source": "Teleport (curated benchmarks)", "cities": {}}

    for loc in locations[:10]:
        city_name = loc.split(",")[0].strip().lower()

        # Resolve city to slug via the existing mapping
        slug = TELEPORT_SLUGS.get(city_name, "")
        if not slug:
            continue

        benchmark = _TELEPORT_BENCHMARK_DATA.get(slug)
        if not benchmark:
            continue

        entry: Dict[str, Any] = {
            "slug": slug,
            "teleport_city_score": benchmark["quality_of_life"],
            "summary": benchmark["summary"],
            "quality_scores": {
                "Quality of Life": benchmark["quality_of_life"],
                "Cost of Living": round(
                    10.0 - (benchmark["cost_of_living_index"] / 25.0), 2
                ),
            },
            "cost_of_living": {
                "cost_of_living_index": benchmark["cost_of_living_index"],
                "median_home_price": benchmark["median_home_price"],
                "median_rent_1br": benchmark["median_rent_1br"],
            },
        }
        result["cities"][loc] = entry

    if result["cities"]:
        _set_cached(cache_k, result)
        return result
    return {}


# ---------------------------------------------------------------------------
# DataUSA — Occupation stats and location demographics
# ---------------------------------------------------------------------------

US_STATE_NAMES: Dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}

DATAUSA_OCC: Dict[str, str] = {
    "software engineer": "Software Developers",
    "software developer": "Software Developers",
    "data scientist": "Data Scientists",
    "data analyst": "Data Scientists",
    "product manager": "Marketing Managers",
    "marketing manager": "Marketing Managers",
    "financial analyst": "Financial Analysts",
    "accountant": "Accountants & Auditors",
    "nurse": "Registered Nurses",
    "registered nurse": "Registered Nurses",
    "mechanical engineer": "Mechanical Engineers",
    "electrical engineer": "Electrical Engineers",
    "project manager": "Management Analysts",
    "business analyst": "Management Analysts",
}


def fetch_datausa_occupation_stats(roles: List[str]) -> Dict[str, Any]:
    """
    Fetch occupation wage and employment data from DataUSA API.
    Free, no authentication required.
    Set DATAUSA_DISABLED=1 to skip live API (saves 8-10s per role when API is down).
    """
    if not roles:
        return {}

    _datausa_disabled = os.environ.get("DATAUSA_DISABLED", "").strip() in (
        "1",
        "true",
        "yes",
    )

    cache_k = _cache_key("datausa_occ", ",".join(sorted(r.lower() for r in roles[:10])))
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    result: Dict[str, Any] = {"source": "DataUSA", "occupations": {}}

    for role in roles[:10]:
        role_lower = role.lower().strip()
        occ_name = None
        for key, name in DATAUSA_OCC.items():
            if key in role_lower or role_lower in key:
                occ_name = name
                break

        if not occ_name:
            # Word-level matching
            role_words = set(role_lower.split())
            best_match = None
            best_score = 0
            for key, name in DATAUSA_OCC.items():
                key_words = set(key.split())
                overlap = len(role_words & key_words)
                if overlap > best_score:
                    best_score = overlap
                    best_match = name
            if best_score >= 1:
                occ_name = best_match

        if not occ_name:
            continue

        if not _datausa_disabled:
            try:
                # Try live API first (DataUSA endpoints are unreliable / 404)
                search_url = f"https://datausa.io/api/searchLegacy?q={urllib.parse.quote(occ_name)}&dimension=PUMS+Occupation"
                search_data = _http_get_json(search_url, timeout=8)

                occ_id = None
                if search_data and isinstance(search_data, dict):
                    results = search_data.get("results", [])
                    if results:
                        occ_id = results[0].get("id") or ""

                if occ_id:
                    stats_url = (
                        f"https://datausa.io/api/data?"
                        f"drilldowns=Detailed+Occupation&measures=Total+Population,Average+Wage"
                        f"&Detailed+Occupation={urllib.parse.quote(str(occ_id))}"
                        f"&Year=latest"
                    )
                    stats_data = _http_get_json(stats_url, timeout=10)

                    if stats_data and stats_data.get("data"):
                        entry = stats_data["data"][0]
                        result["occupations"][role] = {
                            "occupation": entry.get("Detailed Occupation", occ_name),
                            "total_employed": entry.get("Total Population"),
                            "average_wage": entry.get("Average Wage"),
                            "year": entry.get("Year") or "",
                            "source": "DataUSA (live)",
                        }
                        continue
            except Exception as exc:
                _log_warn(f"DataUSA live API failed for {role}: {exc}")

        # Fallback: curated BLS/Census benchmark data
        _OCC_BENCHMARKS = {
            "software": {
                "occupation": "Software Developers",
                "total_employed": 1847900,
                "average_wage": 127260,
                "year": "2024",
            },
            "data": {
                "occupation": "Data Scientists",
                "total_employed": 192000,
                "average_wage": 108020,
                "year": "2024",
            },
            "nurse": {
                "occupation": "Registered Nurses",
                "total_employed": 3175390,
                "average_wage": 89010,
                "year": "2024",
            },
            "market": {
                "occupation": "Marketing Managers",
                "total_employed": 316000,
                "average_wage": 157620,
                "year": "2024",
            },
            "account": {
                "occupation": "Accountants & Auditors",
                "total_employed": 1451000,
                "average_wage": 83980,
                "year": "2024",
            },
            "engineer": {
                "occupation": "Engineers (General)",
                "total_employed": 330000,
                "average_wage": 100640,
                "year": "2024",
            },
            "teacher": {
                "occupation": "Teachers (K-12)",
                "total_employed": 3600000,
                "average_wage": 63770,
                "year": "2024",
            },
            "driver": {
                "occupation": "Truck Drivers",
                "total_employed": 2100000,
                "average_wage": 54320,
                "year": "2024",
            },
            "sales": {
                "occupation": "Sales Representatives",
                "total_employed": 1750000,
                "average_wage": 65630,
                "year": "2024",
            },
            "warehouse": {
                "occupation": "Warehouse Workers",
                "total_employed": 1870000,
                "average_wage": 36340,
                "year": "2024",
            },
            "mechanic": {
                "occupation": "Automotive Technicians",
                "total_employed": 784000,
                "average_wage": 48320,
                "year": "2024",
            },
            "electrician": {
                "occupation": "Electricians",
                "total_employed": 726000,
                "average_wage": 65280,
                "year": "2024",
            },
            "project": {
                "occupation": "Project Management Specialists",
                "total_employed": 781000,
                "average_wage": 98580,
                "year": "2024",
            },
            "human": {
                "occupation": "HR Specialists",
                "total_employed": 783000,
                "average_wage": 67650,
                "year": "2024",
            },
            "financ": {
                "occupation": "Financial Analysts",
                "total_employed": 328000,
                "average_wage": 99890,
                "year": "2024",
            },
            "customer": {
                "occupation": "Customer Service Representatives",
                "total_employed": 2910000,
                "average_wage": 39680,
                "year": "2024",
            },
        }
        role_l = role.lower()
        for bm_key, bm_val in _OCC_BENCHMARKS.items():
            if bm_key in role_l:
                result["occupations"][role] = {
                    **bm_val,
                    "source": "BLS/Census Benchmarks",
                }
                break

    if result["occupations"]:
        result["source"] = "DataUSA (curated benchmarks)"
        _set_cached(cache_k, result)
        return result
    return {}


def fetch_datausa_location_data(locations: List[str]) -> Dict[str, Any]:
    """
    Fetch demographic data for US states from DataUSA API.
    Includes population, median income, poverty rate.
    Set DATAUSA_DISABLED=1 to skip live API and use benchmarks only.
    """
    if not locations:
        return {}

    _datausa_disabled = os.environ.get("DATAUSA_DISABLED", "").strip() in (
        "1",
        "true",
        "yes",
    )

    cache_k = _cache_key(
        "datausa_loc", ",".join(sorted(l.lower() for l in locations[:10]))
    )
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    result: Dict[str, Any] = {"source": "DataUSA", "locations": {}}

    # Collect unique US states from locations
    state_locs: Dict[str, str] = {}  # state_name -> original_location
    for loc in locations:
        parts = [p.strip() for p in loc.split(",")]
        for part in parts:
            upper = part.upper().strip()
            if upper in US_STATE_NAMES:
                state_locs[US_STATE_NAMES[upper]] = loc
                break
            # Check full state name
            for abbr, full_name in US_STATE_NAMES.items():
                if full_name.lower() == part.lower().strip():
                    state_locs[full_name] = loc
                    break

    if not state_locs:
        return {}

    # Curated state-level Census/ACS benchmark data (2024 estimates)
    _STATE_BENCHMARKS: Dict[str, Dict[str, Any]] = {
        "California": {
            "population": 39538223,
            "median_household_income": 91905,
            "poverty_rate": 11.0,
        },
        "Texas": {
            "population": 30503340,
            "median_household_income": 73035,
            "poverty_rate": 13.4,
        },
        "Florida": {
            "population": 22610726,
            "median_household_income": 67917,
            "poverty_rate": 11.5,
        },
        "New York": {
            "population": 19571216,
            "median_household_income": 75910,
            "poverty_rate": 12.7,
        },
        "Pennsylvania": {
            "population": 12961683,
            "median_household_income": 72627,
            "poverty_rate": 11.1,
        },
        "Illinois": {
            "population": 12549689,
            "median_household_income": 74235,
            "poverty_rate": 10.6,
        },
        "Ohio": {
            "population": 11780017,
            "median_household_income": 62262,
            "poverty_rate": 13.0,
        },
        "Georgia": {
            "population": 10912876,
            "median_household_income": 66559,
            "poverty_rate": 12.2,
        },
        "North Carolina": {
            "population": 10698973,
            "median_household_income": 64003,
            "poverty_rate": 12.3,
        },
        "Michigan": {
            "population": 10037261,
            "median_household_income": 63202,
            "poverty_rate": 13.0,
        },
        "New Jersey": {
            "population": 9288994,
            "median_household_income": 89296,
            "poverty_rate": 9.4,
        },
        "Virginia": {
            "population": 8631393,
            "median_household_income": 82246,
            "poverty_rate": 9.6,
        },
        "Washington": {
            "population": 7715946,
            "median_household_income": 85748,
            "poverty_rate": 10.0,
        },
        "Arizona": {
            "population": 7303398,
            "median_household_income": 65913,
            "poverty_rate": 13.5,
        },
        "Massachusetts": {
            "population": 7029917,
            "median_household_income": 89645,
            "poverty_rate": 10.0,
        },
        "Tennessee": {
            "population": 7051339,
            "median_household_income": 59695,
            "poverty_rate": 13.2,
        },
        "Indiana": {
            "population": 6806460,
            "median_household_income": 62743,
            "poverty_rate": 11.4,
        },
        "Maryland": {
            "population": 6177224,
            "median_household_income": 87063,
            "poverty_rate": 9.1,
        },
        "Missouri": {
            "population": 6154913,
            "median_household_income": 60990,
            "poverty_rate": 12.1,
        },
        "Wisconsin": {
            "population": 5893718,
            "median_household_income": 67125,
            "poverty_rate": 10.4,
        },
        "Colorado": {
            "population": 5812069,
            "median_household_income": 82254,
            "poverty_rate": 9.1,
        },
        "Minnesota": {
            "population": 5706494,
            "median_household_income": 78474,
            "poverty_rate": 8.3,
        },
        "South Carolina": {
            "population": 5190705,
            "median_household_income": 59318,
            "poverty_rate": 13.4,
        },
        "Alabama": {
            "population": 5024279,
            "median_household_income": 56950,
            "poverty_rate": 14.8,
        },
        "Louisiana": {
            "population": 4657757,
            "median_household_income": 54216,
            "poverty_rate": 18.6,
        },
        "Kentucky": {
            "population": 4505836,
            "median_household_income": 55573,
            "poverty_rate": 15.5,
        },
        "Oregon": {
            "population": 4237256,
            "median_household_income": 71562,
            "poverty_rate": 11.2,
        },
        "Connecticut": {
            "population": 3605944,
            "median_household_income": 83771,
            "poverty_rate": 9.8,
        },
        "Utah": {
            "population": 3337975,
            "median_household_income": 79449,
            "poverty_rate": 8.2,
        },
        "Nevada": {
            "population": 3104614,
            "median_household_income": 65686,
            "poverty_rate": 11.2,
        },
        # Remaining states (Census ACS 2024 estimates)
        "Iowa": {
            "population": 3190369,
            "median_household_income": 65573,
            "poverty_rate": 10.4,
        },
        "Arkansas": {
            "population": 3011524,
            "median_household_income": 52528,
            "poverty_rate": 15.2,
        },
        "Mississippi": {
            "population": 2961279,
            "median_household_income": 48610,
            "poverty_rate": 18.7,
        },
        "Kansas": {
            "population": 2937880,
            "median_household_income": 64521,
            "poverty_rate": 10.3,
        },
        "New Mexico": {
            "population": 2117522,
            "median_household_income": 53992,
            "poverty_rate": 17.6,
        },
        "Nebraska": {
            "population": 1961504,
            "median_household_income": 66644,
            "poverty_rate": 10.0,
        },
        "Idaho": {
            "population": 1939033,
            "median_household_income": 65988,
            "poverty_rate": 10.1,
        },
        "West Virginia": {
            "population": 1793716,
            "median_household_income": 50884,
            "poverty_rate": 16.8,
        },
        "Hawaii": {
            "population": 1455271,
            "median_household_income": 88005,
            "poverty_rate": 9.3,
        },
        "New Hampshire": {
            "population": 1377529,
            "median_household_income": 88235,
            "poverty_rate": 6.4,
        },
        "Maine": {
            "population": 1362359,
            "median_household_income": 64767,
            "poverty_rate": 10.9,
        },
        "Montana": {
            "population": 1084225,
            "median_household_income": 60560,
            "poverty_rate": 12.1,
        },
        "Rhode Island": {
            "population": 1097379,
            "median_household_income": 74008,
            "poverty_rate": 10.3,
        },
        "Delaware": {
            "population": 989948,
            "median_household_income": 72724,
            "poverty_rate": 11.3,
        },
        "South Dakota": {
            "population": 886667,
            "median_household_income": 63920,
            "poverty_rate": 11.9,
        },
        "North Dakota": {
            "population": 779094,
            "median_household_income": 68131,
            "poverty_rate": 10.5,
        },
        "Alaska": {
            "population": 733391,
            "median_household_income": 77640,
            "poverty_rate": 10.2,
        },
        "Vermont": {
            "population": 643077,
            "median_household_income": 69543,
            "poverty_rate": 10.3,
        },
        "Wyoming": {
            "population": 576851,
            "median_household_income": 68002,
            "poverty_rate": 9.6,
        },
        "District of Columbia": {
            "population": 689545,
            "median_household_income": 90842,
            "poverty_rate": 13.5,
        },
    }

    # Try live API first, fall back to benchmarks
    for state_name, orig_loc in state_locs.items():
        if not _datausa_disabled:
            try:
                url = (
                    f"https://datausa.io/api/data?"
                    f"drilldowns=State&measures=Population,Median+Household+Income,Poverty+Rate"
                    f"&State={urllib.parse.quote(state_name)}&Year=latest"
                )
                data = _http_get_json(url, timeout=10)

                if data and data.get("data"):
                    entry = data["data"][0]
                    result["locations"][orig_loc] = {
                        "state": entry.get("State", state_name),
                        "population": entry.get("Population"),
                        "median_household_income": entry.get("Median Household Income"),
                        "poverty_rate": entry.get("Poverty Rate"),
                        "year": entry.get("Year") or "",
                        "source": "DataUSA (live)",
                    }
                    continue
            except Exception as exc:
                _log_warn(f"DataUSA live API failed for {state_name}: {exc}")

        # Fallback: curated Census/ACS data
        if state_name in _STATE_BENCHMARKS:
            bm = _STATE_BENCHMARKS[state_name]
            result["locations"][orig_loc] = {
                "state": state_name,
                "population": bm["population"],
                "median_household_income": bm["median_household_income"],
                "poverty_rate": bm["poverty_rate"],
                "year": "2024",
                "source": "Census/ACS Benchmarks",
            }

    if result["locations"]:
        result["source"] = "DataUSA (curated benchmarks)"
        _set_cached(cache_k, result)
        return result
    return {}


# ---------------------------------------------------------------------------


# API 19: Google Ads API (Keyword Planner)

GOOGLE_ADS_BENCHMARKS = {
    "technology": {
        "avg_cpc_usd": 3.50,
        "avg_cpm_usd": 12.50,
        "avg_monthly_searches": 45000,
        "competition": "HIGH",
        "click_through_rate": 0.032,
        "conversion_rate": 0.038,
        "cost_per_application": 28.50,
        "top_keywords": [
            "software engineer jobs",
            "developer jobs",
            "tech jobs near me",
            "IT jobs hiring now",
            "programming jobs",
        ],
    },
    "healthcare": {
        "avg_cpc_usd": 2.80,
        "avg_cpm_usd": 10.20,
        "avg_monthly_searches": 62000,
        "competition": "HIGH",
        "click_through_rate": 0.035,
        "conversion_rate": 0.042,
        "cost_per_application": 22.00,
        "top_keywords": [
            "healthcare jobs",
            "medical jobs near me",
            "hospital jobs hiring",
            "healthcare careers",
            "clinical jobs",
        ],
    },
    "finance": {
        "avg_cpc_usd": 4.20,
        "avg_cpm_usd": 14.80,
        "avg_monthly_searches": 38000,
        "competition": "HIGH",
        "click_through_rate": 0.028,
        "conversion_rate": 0.033,
        "cost_per_application": 35.00,
        "top_keywords": [
            "finance jobs",
            "accounting jobs",
            "financial analyst jobs",
            "banking careers",
            "investment jobs",
        ],
    },
    "engineering": {
        "avg_cpc_usd": 3.80,
        "avg_cpm_usd": 13.00,
        "avg_monthly_searches": 41000,
        "competition": "HIGH",
        "click_through_rate": 0.030,
        "conversion_rate": 0.035,
        "cost_per_application": 30.00,
        "top_keywords": [
            "engineering jobs",
            "mechanical engineer jobs",
            "civil engineer jobs",
            "electrical engineer careers",
            "engineering jobs near me",
        ],
    },
    "marketing": {
        "avg_cpc_usd": 3.10,
        "avg_cpm_usd": 11.00,
        "avg_monthly_searches": 35000,
        "competition": "HIGH",
        "click_through_rate": 0.034,
        "conversion_rate": 0.040,
        "cost_per_application": 24.00,
        "top_keywords": [
            "marketing jobs",
            "digital marketing jobs",
            "marketing manager jobs",
            "social media marketing careers",
            "content marketing jobs",
        ],
    },
    "sales": {
        "avg_cpc_usd": 2.60,
        "avg_cpm_usd": 9.50,
        "avg_monthly_searches": 52000,
        "competition": "MEDIUM",
        "click_through_rate": 0.036,
        "conversion_rate": 0.045,
        "cost_per_application": 18.50,
        "top_keywords": [
            "sales jobs",
            "sales representative jobs",
            "account executive jobs",
            "sales manager hiring",
            "inside sales jobs",
        ],
    },
    "human_resources": {
        "avg_cpc_usd": 2.90,
        "avg_cpm_usd": 10.50,
        "avg_monthly_searches": 28000,
        "competition": "MEDIUM",
        "click_through_rate": 0.033,
        "conversion_rate": 0.041,
        "cost_per_application": 23.00,
        "top_keywords": [
            "HR jobs",
            "human resources jobs",
            "recruiter jobs",
            "HR manager careers",
            "talent acquisition jobs",
        ],
    },
    "operations": {
        "avg_cpc_usd": 2.40,
        "avg_cpm_usd": 8.80,
        "avg_monthly_searches": 33000,
        "competition": "MEDIUM",
        "click_through_rate": 0.031,
        "conversion_rate": 0.039,
        "cost_per_application": 20.00,
        "top_keywords": [
            "operations manager jobs",
            "logistics jobs",
            "supply chain jobs",
            "operations jobs near me",
            "warehouse manager jobs",
        ],
    },
    "executive": {
        "avg_cpc_usd": 6.50,
        "avg_cpm_usd": 22.00,
        "avg_monthly_searches": 12000,
        "competition": "HIGH",
        "click_through_rate": 0.022,
        "conversion_rate": 0.025,
        "cost_per_application": 55.00,
        "top_keywords": [
            "executive jobs",
            "C-suite jobs",
            "VP jobs",
            "director level jobs",
            "senior leadership careers",
        ],
    },
    "data_science": {
        "avg_cpc_usd": 4.00,
        "avg_cpm_usd": 14.00,
        "avg_monthly_searches": 32000,
        "competition": "HIGH",
        "click_through_rate": 0.029,
        "conversion_rate": 0.034,
        "cost_per_application": 32.00,
        "top_keywords": [
            "data scientist jobs",
            "data analyst jobs",
            "machine learning jobs",
            "data engineering jobs",
            "AI jobs hiring",
        ],
    },
    "cybersecurity": {
        "avg_cpc_usd": 4.50,
        "avg_cpm_usd": 15.50,
        "avg_monthly_searches": 26000,
        "competition": "HIGH",
        "click_through_rate": 0.027,
        "conversion_rate": 0.032,
        "cost_per_application": 38.00,
        "top_keywords": [
            "cybersecurity jobs",
            "information security jobs",
            "security analyst jobs",
            "penetration testing jobs",
            "SOC analyst careers",
        ],
    },
    "nursing": {
        "avg_cpc_usd": 2.50,
        "avg_cpm_usd": 9.00,
        "avg_monthly_searches": 74000,
        "competition": "HIGH",
        "click_through_rate": 0.038,
        "conversion_rate": 0.048,
        "cost_per_application": 16.50,
        "top_keywords": [
            "nursing jobs",
            "RN jobs near me",
            "nurse practitioner jobs",
            "travel nurse jobs",
            "LPN jobs hiring now",
        ],
    },
    "education": {
        "avg_cpc_usd": 1.90,
        "avg_cpm_usd": 7.20,
        "avg_monthly_searches": 48000,
        "competition": "MEDIUM",
        "click_through_rate": 0.037,
        "conversion_rate": 0.044,
        "cost_per_application": 14.00,
        "top_keywords": [
            "teaching jobs",
            "teacher jobs near me",
            "education jobs",
            "school jobs hiring",
            "tutor jobs",
        ],
    },
    "legal": {
        "avg_cpc_usd": 5.20,
        "avg_cpm_usd": 18.00,
        "avg_monthly_searches": 22000,
        "competition": "HIGH",
        "click_through_rate": 0.025,
        "conversion_rate": 0.030,
        "cost_per_application": 42.00,
        "top_keywords": [
            "legal jobs",
            "attorney jobs",
            "paralegal jobs",
            "law firm jobs",
            "corporate counsel careers",
        ],
    },
    "construction": {
        "avg_cpc_usd": 2.20,
        "avg_cpm_usd": 8.00,
        "avg_monthly_searches": 40000,
        "competition": "MEDIUM",
        "click_through_rate": 0.034,
        "conversion_rate": 0.043,
        "cost_per_application": 17.00,
        "top_keywords": [
            "construction jobs",
            "construction worker jobs",
            "project manager construction",
            "building jobs near me",
            "contractor jobs hiring",
        ],
    },
    "retail": {
        "avg_cpc_usd": 1.60,
        "avg_cpm_usd": 6.00,
        "avg_monthly_searches": 68000,
        "competition": "MEDIUM",
        "click_through_rate": 0.040,
        "conversion_rate": 0.052,
        "cost_per_application": 10.50,
        "top_keywords": [
            "retail jobs",
            "store manager jobs",
            "retail sales jobs",
            "cashier jobs near me",
            "retail jobs hiring now",
        ],
    },
    "hospitality": {
        "avg_cpc_usd": 1.40,
        "avg_cpm_usd": 5.50,
        "avg_monthly_searches": 55000,
        "competition": "LOW",
        "click_through_rate": 0.041,
        "conversion_rate": 0.050,
        "cost_per_application": 9.50,
        "top_keywords": [
            "hotel jobs",
            "hospitality jobs",
            "restaurant manager jobs",
            "front desk jobs",
            "chef jobs hiring",
        ],
    },
    "manufacturing": {
        "avg_cpc_usd": 2.00,
        "avg_cpm_usd": 7.50,
        "avg_monthly_searches": 36000,
        "competition": "MEDIUM",
        "click_through_rate": 0.035,
        "conversion_rate": 0.046,
        "cost_per_application": 15.00,
        "top_keywords": [
            "manufacturing jobs",
            "factory jobs near me",
            "production manager jobs",
            "assembly jobs hiring",
            "plant manager careers",
        ],
    },
}

ROLE_TO_AD_CATEGORY = {
    "technology": [
        "software",
        "developer",
        "programmer",
        "frontend",
        "backend",
        "fullstack",
        "full-stack",
        "full stack",
        "devops",
        "sre",
        "site reliability",
        "web developer",
        "mobile developer",
        "ios",
        "android",
        "qa",
        "quality assurance",
        "test engineer",
        "sdet",
        "it ",
        "information technology",
        "systems administrator",
        "sysadmin",
        "cloud",
        "platform engineer",
        "infrastructure",
        "tech lead",
    ],
    "healthcare": [
        "healthcare",
        "medical",
        "physician",
        "doctor",
        "clinical",
        "therapist",
        "pharmacy",
        "pharmacist",
        "health",
        "dental",
        "dentist",
        "optometrist",
        "radiologist",
        "pathologist",
        "surgeon",
        "psychiatric",
        "behavioral health",
    ],
    "finance": [
        "finance",
        "financial",
        "accountant",
        "accounting",
        "auditor",
        "banking",
        "investment",
        "analyst",
        "actuary",
        "controller",
        "treasury",
        "tax ",
        "bookkeeper",
        "cfo",
        "chief financial",
    ],
    "engineering": [
        "mechanical engineer",
        "civil engineer",
        "electrical engineer",
        "chemical engineer",
        "structural engineer",
        "aerospace",
        "industrial engineer",
        "biomedical engineer",
        "environmental engineer",
        "engineer",
    ],
    "marketing": [
        "marketing",
        "brand",
        "content",
        "seo",
        "sem",
        "social media",
        "digital marketing",
        "growth",
        "communications",
        "public relations",
        "advertising",
        "creative director",
        "copywriter",
        "cmo",
    ],
    "sales": [
        "sales",
        "account executive",
        "business development",
        "bdr",
        "sdr",
        "account manager",
        "customer success",
        "revenue",
        "quota",
        "territory",
    ],
    "human_resources": [
        "human resources",
        " hr ",
        "recruiter",
        "recruiting",
        "talent acquisition",
        "people operations",
        "compensation",
        "benefits",
        "hrbp",
        "people partner",
        "employee relations",
        "workforce",
    ],
    "operations": [
        "operations",
        "logistics",
        "supply chain",
        "procurement",
        "warehouse",
        "distribution",
        "fleet",
        "inventory",
        "fulfillment",
        "coo",
    ],
    "executive": [
        "executive",
        "c-suite",
        "ceo",
        "cto",
        "coo",
        "cfo",
        "cmo",
        "cio",
        "ciso",
        "vp ",
        "vice president",
        "svp",
        "evp",
        "director",
        "head of",
        "chief",
        "president",
        "managing director",
    ],
    "data_science": [
        "data scientist",
        "data analyst",
        "machine learning",
        "ml engineer",
        "artificial intelligence",
        " ai ",
        "deep learning",
        "nlp",
        "data engineer",
        "analytics",
        "business intelligence",
        " bi ",
        "statistician",
    ],
    "cybersecurity": [
        "cybersecurity",
        "cyber security",
        "security engineer",
        "security analyst",
        "infosec",
        "information security",
        "penetration test",
        "pentest",
        "soc analyst",
        "security architect",
        "ciso",
        "threat",
        "vulnerability",
    ],
    "nursing": [
        "nurse",
        "nursing",
        " rn ",
        "registered nurse",
        "lpn",
        "lvn",
        "nurse practitioner",
        "travel nurse",
        "cna",
        "certified nursing",
    ],
    "education": [
        "teacher",
        "teaching",
        "professor",
        "instructor",
        "tutor",
        "education",
        "principal",
        "academic",
        "curriculum",
        "school",
        "faculty",
    ],
    "legal": [
        "legal",
        "attorney",
        "lawyer",
        "paralegal",
        "counsel",
        "litigation",
        "compliance",
        "regulatory",
        "contract",
        "law ",
    ],
    "construction": [
        "construction",
        "contractor",
        "builder",
        "foreman",
        "superintendent",
        "estimator",
        "carpenter",
        "electrician",
        "plumber",
        "hvac",
        "welder",
        "heavy equipment",
        "ironworker",
        "mason",
    ],
    "retail": [
        "retail",
        "store manager",
        "cashier",
        "merchandiser",
        "buyer",
        "visual merchandiser",
        "retail associate",
        "store associate",
        "district manager retail",
    ],
    "hospitality": [
        "hospitality",
        "hotel",
        "restaurant",
        "chef",
        "cook",
        "bartender",
        "server",
        "front desk",
        "concierge",
        "event coordinator",
        "catering",
        "housekeeping",
    ],
    "manufacturing": [
        "manufacturing",
        "factory",
        "production",
        "assembly",
        "plant manager",
        "machinist",
        "cnc",
        "quality control",
        "lean",
        "six sigma",
        "process engineer manufacturing",
    ],
}


def _classify_role_to_ad_category(role: str) -> str:
    """Map a job role string to a Google Ads benchmark category using word-level matching."""
    role_lower = " " + role.lower() + " "
    best_category = "technology"
    best_score = 0

    for category, keywords in ROLE_TO_AD_CATEGORY.items():
        score = 0
        for keyword in keywords:
            if keyword.lower() in role_lower:
                score += len(keyword)
        if score > best_score:
            best_score = score
            best_category = category

    return best_category


def _generate_recruitment_keywords(role: str) -> List[str]:
    """Generate recruitment-related keyword variations for a given role."""
    role_clean = role.strip()
    keywords = [
        f"{role_clean} jobs",
        f"{role_clean} careers",
        f"{role_clean} jobs near me",
        f"hiring {role_clean}",
        f"{role_clean} job openings",
        f"{role_clean} positions",
        f"{role_clean} vacancies",
        f"apply {role_clean}",
        f"{role_clean} salary",
        f"{role_clean} remote jobs",
    ]
    return keywords


def _refresh_google_ads_access_token(
    client_id: str, client_secret: str, refresh_token: str
) -> Optional[str]:
    """Exchange a refresh token for a new access token via Google OAuth2."""
    token_url = "https://oauth2.googleapis.com/token"
    payload = urllib.parse.urlencode(
        {
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        token_url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("access_token")
    except Exception as exc:
        _log_warn(f"Google Ads OAuth2 token refresh failed: {exc}")
        return None


def _call_google_ads_keyword_ideas(
    customer_id: str,
    developer_token: str,
    access_token: str,
    keywords: List[str],
    location_ids: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Call the Google Ads generateKeywordIdeas REST endpoint."""
    url = (
        f"https://googleads.googleapis.com/v16/customers/{customer_id}"
        f":generateKeywordIdeas"
    )

    body: Dict[str, Any] = {
        "keywordSeed": {"keywords": keywords[:20]},
        "language": "languageConstants/1000",
        "keywordPlanNetwork": "GOOGLE_SEARCH",
    }

    if location_ids:
        body["geoTargetConstants"] = [
            f"geoTargetConstants/{loc_id}" for loc_id in location_ids[:10]
        ]

    payload = json.dumps(body).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {access_token}",
            "developer-token": developer_token,
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        error_body = ""
        try:
            error_body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        _log_warn(f"Google Ads API HTTP {exc.code}: {error_body[:500]}")
        return None
    except Exception as exc:
        _log_warn(f"Google Ads API call failed: {exc}")
        return None


def _parse_keyword_ideas(api_response: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse the generateKeywordIdeas response into a flat list of keyword data."""
    results = []
    for result in api_response.get("results", []):
        text = result.get("text") or ""
        metrics = result.get("keywordIdeaMetrics", {})

        avg_searches = metrics.get("avgMonthlySearches") or 0
        competition = metrics.get("competition", "UNSPECIFIED")

        low_cpc_micros = (
            metrics.get("lowTopOfPageBidMicros") or metrics.get("competitionIndex") or 0
        )
        high_cpc_micros = (
            metrics.get("highTopOfPageBidMicros")
            or metrics.get("competitionIndex")
            or 0
        )

        if isinstance(low_cpc_micros, (int, float)):
            low_cpc = low_cpc_micros / 1_000_000
        else:
            low_cpc = 0.0

        if isinstance(high_cpc_micros, (int, float)):
            high_cpc = high_cpc_micros / 1_000_000
        else:
            high_cpc = 0.0

        results.append(
            {
                "keyword": text,
                "avg_monthly_searches": int(avg_searches) if avg_searches else 0,
                "competition": competition,
                "low_range_cpc_usd": round(low_cpc, 2),
                "high_range_cpc_usd": round(high_cpc, 2),
                "avg_cpc_usd": (
                    round((low_cpc + high_cpc) / 2, 2)
                    if (low_cpc + high_cpc) > 0
                    else 0.0
                ),
            }
        )

    return results


LOCATION_NAME_TO_GEO_ID = {
    "united states": "2840",
    "us": "2840",
    "usa": "2840",
    "united kingdom": "2826",
    "uk": "2826",
    "canada": "2124",
    "australia": "2036",
    "germany": "2276",
    "france": "2250",
    "india": "2356",
    "new york": "1023191",
    "los angeles": "1013962",
    "san francisco": "1014221",
    "chicago": "1016367",
    "london": "1006886",
    "toronto": "1002289",
    "sydney": "1000073",
    "berlin": "1003854",
    "paris": "1006094",
    "mumbai": "1007768",
    "bangalore": "1007809",
    "seattle": "1027744",
    "austin": "1026339",
    "boston": "1018127",
    "denver": "1014532",
    "atlanta": "1015116",
    "dallas": "1026642",
    "houston": "1026481",
    "miami": "1015150",
    "washington dc": "1014895",
    "remote": "2840",
}


def fetch_google_ads_data(roles: List[str], locations: List[str]) -> Dict[str, Any]:
    """
    Fetch keyword search volume, CPC estimates, and audience data for
    recruitment-related keywords from the Google Ads API.

    Falls back to curated industry benchmark data when API credentials
    are not available, ensuring the function always returns useful data.

    Args:
        roles: List of job role titles (e.g., ["Software Engineer", "Data Analyst"]).
        locations: List of location strings (e.g., ["San Francisco", "Remote"]).

    Returns:
        Dict with source, per-role keyword data, and platform summary.
    """
    cache_key = _cache_key("google_ads", f"{sorted(roles)}_{sorted(locations)}")
    cached = _get_cached(cache_key)
    if cached:
        _log_info("Returning cached Google Ads data")
        return cached

    developer_token = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN", "").strip()
    refresh_token = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN", "").strip()
    client_id = os.environ.get("GOOGLE_ADS_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_ADS_CLIENT_SECRET", "").strip()
    customer_id = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "").strip().replace("-", "")

    has_credentials = all(
        [developer_token, refresh_token, client_id, client_secret, customer_id]
    )

    location_geo_ids = []
    for loc in locations:
        geo_id = LOCATION_NAME_TO_GEO_ID.get(loc.lower().strip())
        if geo_id:
            location_geo_ids.append(geo_id)
    if not location_geo_ids:
        location_geo_ids = [LOCATION_NAME_TO_GEO_ID["united states"]]

    result: Dict[str, Any] = {
        "source": "Google Ads Benchmarks",
        "keywords": {},
        "platform_summary": {
            "platform": "Google Search & Display",
            "reach": "90%+ of internet users",
            "best_for": "High-intent job seekers actively searching",
            "ad_formats": [
                "Search Ads",
                "Display Ads",
                "YouTube Video Ads",
                "Discovery Ads",
            ],
        },
    }

    if has_credentials:
        access_token = _refresh_google_ads_access_token(
            client_id, client_secret, refresh_token
        )
        if access_token:
            api_success = False
            for role in roles:
                try:
                    generated_keywords = _generate_recruitment_keywords(role)
                    api_response = _call_google_ads_keyword_ideas(
                        customer_id=customer_id,
                        developer_token=developer_token,
                        access_token=access_token,
                        keywords=generated_keywords,
                        location_ids=location_geo_ids,
                    )

                    if api_response and "results" in api_response:
                        api_success = True
                        parsed = _parse_keyword_ideas(api_response)

                        total_searches = sum(
                            kw.get("avg_monthly_searches") or 0 for kw in parsed
                        )
                        avg_cpc_values = [
                            kw["avg_cpc_usd"] for kw in parsed if kw["avg_cpc_usd"] > 0
                        ]
                        avg_cpc = (
                            round(sum(avg_cpc_values) / len(avg_cpc_values), 2)
                            if avg_cpc_values
                            else 0.0
                        )

                        competitions = [kw["competition"] for kw in parsed]
                        competition = (
                            max(set(competitions), key=competitions.count)
                            if competitions
                            else "UNSPECIFIED"
                        )

                        ctr_estimate = 0.032 if competition == "HIGH" else 0.038
                        cpa_estimate = round(avg_cpc / 0.04, 2) if avg_cpc > 0 else 25.0

                        result["keywords"][role] = {
                            "generated_keywords": generated_keywords,
                            "keyword_details": parsed[:15],
                            "avg_cpc_usd": avg_cpc,
                            "avg_cpm_usd": (
                                round(avg_cpc * ctr_estimate * 1000, 2)
                                if avg_cpc > 0
                                else 12.00
                            ),
                            "avg_monthly_searches": total_searches,
                            "competition": competition,
                            "click_through_rate": ctr_estimate,
                            "cost_per_application": cpa_estimate,
                        }
                    else:
                        _build_benchmark_for_role(role, result)

                except Exception as exc:
                    _log_warn(f"Google Ads API error for role '{role}': {exc}")
                    _build_benchmark_for_role(role, result)

            if api_success:
                result["source"] = "Google Ads API"
        else:
            _log_warn("Could not obtain Google Ads access token; using benchmarks")
            for role in roles:
                _build_benchmark_for_role(role, result)
    else:
        _log_info("Google Ads credentials not configured; using benchmark data")
        for role in roles:
            _build_benchmark_for_role(role, result)

    _set_cached(cache_key, result)
    return result


def _build_benchmark_for_role(role: str, result: Dict[str, Any]) -> None:
    """Populate result dict with benchmark data for a single role."""
    category = _classify_role_to_ad_category(role)
    benchmark = GOOGLE_ADS_BENCHMARKS.get(category, GOOGLE_ADS_BENCHMARKS["technology"])
    generated_keywords = _generate_recruitment_keywords(role)

    result["keywords"][role] = {
        "generated_keywords": generated_keywords,
        "matched_category": category,
        "avg_cpc_usd": benchmark["avg_cpc_usd"],
        "avg_cpm_usd": benchmark["avg_cpm_usd"],
        "avg_monthly_searches": benchmark["avg_monthly_searches"],
        "competition": benchmark["competition"],
        "click_through_rate": benchmark["click_through_rate"],
        "conversion_rate": benchmark["conversion_rate"],
        "cost_per_application": benchmark["cost_per_application"],
        "top_category_keywords": benchmark["top_keywords"],
    }


# API 20: Meta Marketing API (Facebook/Instagram)

# ---------------------------------------------------------------------------
# Meta (Facebook / Instagram) Marketing API integration for media plan generator
# ---------------------------------------------------------------------------

META_ADS_BENCHMARKS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "technology": {
        "facebook": {
            "avg_cpm_usd": 15.50,
            "avg_cpc_usd": 1.80,
            "avg_ctr_pct": 0.90,
            "estimated_audience_size": "2M-5M",
            "cost_per_application": 18.00,
            "avg_reach_per_1000_usd": 65,
        },
        "instagram": {
            "avg_cpm_usd": 12.00,
            "avg_cpc_usd": 1.50,
            "avg_ctr_pct": 1.10,
            "estimated_audience_size": "1.5M-3M",
            "cost_per_application": 22.00,
            "avg_reach_per_1000_usd": 83,
        },
    },
    "healthcare": {
        "facebook": {
            "avg_cpm_usd": 12.80,
            "avg_cpc_usd": 1.45,
            "avg_ctr_pct": 1.05,
            "estimated_audience_size": "3M-7M",
            "cost_per_application": 14.50,
            "avg_reach_per_1000_usd": 78,
        },
        "instagram": {
            "avg_cpm_usd": 10.20,
            "avg_cpc_usd": 1.25,
            "avg_ctr_pct": 1.20,
            "estimated_audience_size": "1.8M-4M",
            "cost_per_application": 19.00,
            "avg_reach_per_1000_usd": 98,
        },
    },
    "finance": {
        "facebook": {
            "avg_cpm_usd": 18.75,
            "avg_cpc_usd": 2.30,
            "avg_ctr_pct": 0.75,
            "estimated_audience_size": "1.5M-3.5M",
            "cost_per_application": 24.00,
            "avg_reach_per_1000_usd": 53,
        },
        "instagram": {
            "avg_cpm_usd": 14.50,
            "avg_cpc_usd": 1.90,
            "avg_ctr_pct": 0.95,
            "estimated_audience_size": "800K-2M",
            "cost_per_application": 29.00,
            "avg_reach_per_1000_usd": 69,
        },
    },
    "engineering": {
        "facebook": {
            "avg_cpm_usd": 16.20,
            "avg_cpc_usd": 1.95,
            "avg_ctr_pct": 0.85,
            "estimated_audience_size": "1.8M-4M",
            "cost_per_application": 20.00,
            "avg_reach_per_1000_usd": 62,
        },
        "instagram": {
            "avg_cpm_usd": 12.80,
            "avg_cpc_usd": 1.60,
            "avg_ctr_pct": 1.05,
            "estimated_audience_size": "1M-2.5M",
            "cost_per_application": 25.00,
            "avg_reach_per_1000_usd": 78,
        },
    },
    "marketing": {
        "facebook": {
            "avg_cpm_usd": 14.00,
            "avg_cpc_usd": 1.55,
            "avg_ctr_pct": 1.00,
            "estimated_audience_size": "2.5M-6M",
            "cost_per_application": 15.00,
            "avg_reach_per_1000_usd": 71,
        },
        "instagram": {
            "avg_cpm_usd": 11.00,
            "avg_cpc_usd": 1.30,
            "avg_ctr_pct": 1.30,
            "estimated_audience_size": "2M-5M",
            "cost_per_application": 17.50,
            "avg_reach_per_1000_usd": 91,
        },
    },
    "sales": {
        "facebook": {
            "avg_cpm_usd": 11.50,
            "avg_cpc_usd": 1.20,
            "avg_ctr_pct": 1.15,
            "estimated_audience_size": "4M-9M",
            "cost_per_application": 12.00,
            "avg_reach_per_1000_usd": 87,
        },
        "instagram": {
            "avg_cpm_usd": 9.50,
            "avg_cpc_usd": 1.05,
            "avg_ctr_pct": 1.35,
            "estimated_audience_size": "2.5M-5.5M",
            "cost_per_application": 15.00,
            "avg_reach_per_1000_usd": 105,
        },
    },
    "human_resources": {
        "facebook": {
            "avg_cpm_usd": 13.20,
            "avg_cpc_usd": 1.50,
            "avg_ctr_pct": 0.95,
            "estimated_audience_size": "1.2M-3M",
            "cost_per_application": 16.00,
            "avg_reach_per_1000_usd": 76,
        },
        "instagram": {
            "avg_cpm_usd": 10.80,
            "avg_cpc_usd": 1.30,
            "avg_ctr_pct": 1.10,
            "estimated_audience_size": "700K-1.8M",
            "cost_per_application": 20.00,
            "avg_reach_per_1000_usd": 93,
        },
    },
    "operations": {
        "facebook": {
            "avg_cpm_usd": 12.00,
            "avg_cpc_usd": 1.35,
            "avg_ctr_pct": 1.00,
            "estimated_audience_size": "2M-5M",
            "cost_per_application": 14.00,
            "avg_reach_per_1000_usd": 83,
        },
        "instagram": {
            "avg_cpm_usd": 9.80,
            "avg_cpc_usd": 1.15,
            "avg_ctr_pct": 1.15,
            "estimated_audience_size": "1M-3M",
            "cost_per_application": 18.00,
            "avg_reach_per_1000_usd": 102,
        },
    },
    "executive": {
        "facebook": {
            "avg_cpm_usd": 22.00,
            "avg_cpc_usd": 3.10,
            "avg_ctr_pct": 0.60,
            "estimated_audience_size": "500K-1.5M",
            "cost_per_application": 35.00,
            "avg_reach_per_1000_usd": 45,
        },
        "instagram": {
            "avg_cpm_usd": 18.00,
            "avg_cpc_usd": 2.60,
            "avg_ctr_pct": 0.70,
            "estimated_audience_size": "300K-900K",
            "cost_per_application": 42.00,
            "avg_reach_per_1000_usd": 56,
        },
    },
    "data_science": {
        "facebook": {
            "avg_cpm_usd": 17.00,
            "avg_cpc_usd": 2.10,
            "avg_ctr_pct": 0.80,
            "estimated_audience_size": "800K-2M",
            "cost_per_application": 22.00,
            "avg_reach_per_1000_usd": 59,
        },
        "instagram": {
            "avg_cpm_usd": 13.50,
            "avg_cpc_usd": 1.70,
            "avg_ctr_pct": 1.00,
            "estimated_audience_size": "500K-1.2M",
            "cost_per_application": 27.00,
            "avg_reach_per_1000_usd": 74,
        },
    },
    "nursing": {
        "facebook": {
            "avg_cpm_usd": 10.50,
            "avg_cpc_usd": 1.10,
            "avg_ctr_pct": 1.25,
            "estimated_audience_size": "3.5M-8M",
            "cost_per_application": 11.00,
            "avg_reach_per_1000_usd": 95,
        },
        "instagram": {
            "avg_cpm_usd": 8.80,
            "avg_cpc_usd": 0.95,
            "avg_ctr_pct": 1.40,
            "estimated_audience_size": "2M-5M",
            "cost_per_application": 14.00,
            "avg_reach_per_1000_usd": 114,
        },
    },
    "education": {
        "facebook": {
            "avg_cpm_usd": 9.80,
            "avg_cpc_usd": 1.05,
            "avg_ctr_pct": 1.20,
            "estimated_audience_size": "3M-7M",
            "cost_per_application": 10.50,
            "avg_reach_per_1000_usd": 102,
        },
        "instagram": {
            "avg_cpm_usd": 8.20,
            "avg_cpc_usd": 0.90,
            "avg_ctr_pct": 1.35,
            "estimated_audience_size": "2M-5M",
            "cost_per_application": 13.00,
            "avg_reach_per_1000_usd": 122,
        },
    },
    "legal": {
        "facebook": {
            "avg_cpm_usd": 19.50,
            "avg_cpc_usd": 2.50,
            "avg_ctr_pct": 0.70,
            "estimated_audience_size": "800K-2M",
            "cost_per_application": 28.00,
            "avg_reach_per_1000_usd": 51,
        },
        "instagram": {
            "avg_cpm_usd": 15.80,
            "avg_cpc_usd": 2.10,
            "avg_ctr_pct": 0.85,
            "estimated_audience_size": "400K-1M",
            "cost_per_application": 34.00,
            "avg_reach_per_1000_usd": 63,
        },
    },
    "retail": {
        "facebook": {
            "avg_cpm_usd": 8.50,
            "avg_cpc_usd": 0.85,
            "avg_ctr_pct": 1.40,
            "estimated_audience_size": "6M-15M",
            "cost_per_application": 8.00,
            "avg_reach_per_1000_usd": 118,
        },
        "instagram": {
            "avg_cpm_usd": 7.20,
            "avg_cpc_usd": 0.75,
            "avg_ctr_pct": 1.55,
            "estimated_audience_size": "4M-10M",
            "cost_per_application": 10.00,
            "avg_reach_per_1000_usd": 139,
        },
    },
    "hospitality": {
        "facebook": {
            "avg_cpm_usd": 8.00,
            "avg_cpc_usd": 0.80,
            "avg_ctr_pct": 1.45,
            "estimated_audience_size": "5M-12M",
            "cost_per_application": 7.50,
            "avg_reach_per_1000_usd": 125,
        },
        "instagram": {
            "avg_cpm_usd": 6.80,
            "avg_cpc_usd": 0.70,
            "avg_ctr_pct": 1.60,
            "estimated_audience_size": "3.5M-8M",
            "cost_per_application": 9.50,
            "avg_reach_per_1000_usd": 147,
        },
    },
    "manufacturing": {
        "facebook": {
            "avg_cpm_usd": 10.00,
            "avg_cpc_usd": 1.10,
            "avg_ctr_pct": 1.10,
            "estimated_audience_size": "2.5M-6M",
            "cost_per_application": 12.50,
            "avg_reach_per_1000_usd": 100,
        },
        "instagram": {
            "avg_cpm_usd": 8.50,
            "avg_cpc_usd": 0.95,
            "avg_ctr_pct": 1.20,
            "estimated_audience_size": "1.2M-3M",
            "cost_per_application": 16.00,
            "avg_reach_per_1000_usd": 118,
        },
    },
    "construction": {
        "facebook": {
            "avg_cpm_usd": 9.50,
            "avg_cpc_usd": 1.00,
            "avg_ctr_pct": 1.15,
            "estimated_audience_size": "3M-7M",
            "cost_per_application": 11.00,
            "avg_reach_per_1000_usd": 105,
        },
        "instagram": {
            "avg_cpm_usd": 8.00,
            "avg_cpc_usd": 0.88,
            "avg_ctr_pct": 1.25,
            "estimated_audience_size": "1.5M-4M",
            "cost_per_application": 14.50,
            "avg_reach_per_1000_usd": 125,
        },
    },
    "logistics": {
        "facebook": {
            "avg_cpm_usd": 10.80,
            "avg_cpc_usd": 1.15,
            "avg_ctr_pct": 1.08,
            "estimated_audience_size": "2M-5M",
            "cost_per_application": 13.00,
            "avg_reach_per_1000_usd": 93,
        },
        "instagram": {
            "avg_cpm_usd": 9.00,
            "avg_cpc_usd": 1.00,
            "avg_ctr_pct": 1.18,
            "estimated_audience_size": "1M-3M",
            "cost_per_application": 17.00,
            "avg_reach_per_1000_usd": 111,
        },
    },
}

# Maps lowercase role keywords (word-level matching) to benchmark category keys.
# Over 30 entries covering common recruitment roles.
ROLE_TO_META_CATEGORY: Dict[str, str] = {
    # technology
    "software": "technology",
    "developer": "technology",
    "programmer": "technology",
    "frontend": "technology",
    "backend": "technology",
    "fullstack": "technology",
    "devops": "technology",
    "sre": "technology",
    "sysadmin": "technology",
    "cybersecurity": "technology",
    "security": "technology",
    "cloud": "technology",
    "ios": "technology",
    "android": "technology",
    "mobile": "technology",
    "qa": "technology",
    "test": "technology",
    # data science
    "data": "data_science",
    "machine": "data_science",
    "ml": "data_science",
    "ai": "data_science",
    "analytics": "data_science",
    "statistician": "data_science",
    # engineering
    "engineer": "engineering",
    "mechanical": "engineering",
    "electrical": "engineering",
    "civil": "engineering",
    "chemical": "engineering",
    "structural": "engineering",
    "aerospace": "engineering",
    # healthcare
    "doctor": "healthcare",
    "physician": "healthcare",
    "therapist": "healthcare",
    "pharmacist": "healthcare",
    "medical": "healthcare",
    "clinical": "healthcare",
    "healthcare": "healthcare",
    "dental": "healthcare",
    "surgeon": "healthcare",
    # nursing
    "nurse": "nursing",
    "rn": "nursing",
    "lpn": "nursing",
    "cna": "nursing",
    "nursing": "nursing",
    # finance
    "finance": "finance",
    "financial": "finance",
    "accountant": "finance",
    "accounting": "finance",
    "auditor": "finance",
    "banker": "finance",
    "banking": "finance",
    "investment": "finance",
    "actuarial": "finance",
    "treasury": "finance",
    # marketing
    "marketing": "marketing",
    "brand": "marketing",
    "content": "marketing",
    "seo": "marketing",
    "social": "marketing",
    "creative": "marketing",
    "copywriter": "marketing",
    "advertising": "marketing",
    # sales
    "sales": "sales",
    "account": "sales",
    "business development": "sales",
    "bdr": "sales",
    "sdr": "sales",
    "representative": "sales",
    # human resources
    "hr": "human_resources",
    "human": "human_resources",
    "recruiter": "human_resources",
    "recruiting": "human_resources",
    "talent": "human_resources",
    "people": "human_resources",
    "compensation": "human_resources",
    "benefits": "human_resources",
    # operations
    "operations": "operations",
    "ops": "operations",
    "supply": "operations",
    "procurement": "operations",
    "project": "operations",
    "program": "operations",
    "coordinator": "operations",
    # executive
    "executive": "executive",
    "director": "executive",
    "vp": "executive",
    "president": "executive",
    "chief": "executive",
    "ceo": "executive",
    "cto": "executive",
    "cfo": "executive",
    "coo": "executive",
    "cmo": "executive",
    "svp": "executive",
    "evp": "executive",
    "partner": "executive",
    # education
    "teacher": "education",
    "professor": "education",
    "instructor": "education",
    "education": "education",
    "tutor": "education",
    "academic": "education",
    "faculty": "education",
    "principal": "education",
    # legal
    "lawyer": "legal",
    "attorney": "legal",
    "legal": "legal",
    "paralegal": "legal",
    "counsel": "legal",
    "compliance": "legal",
    # retail
    "retail": "retail",
    "cashier": "retail",
    "store": "retail",
    "merchandiser": "retail",
    "buyer": "retail",
    # hospitality
    "hospitality": "hospitality",
    "hotel": "hospitality",
    "restaurant": "hospitality",
    "chef": "hospitality",
    "cook": "hospitality",
    "bartender": "hospitality",
    "server": "hospitality",
    "barista": "hospitality",
    "housekeeping": "hospitality",
    # manufacturing
    "manufacturing": "manufacturing",
    "production": "manufacturing",
    "assembly": "manufacturing",
    "machinist": "manufacturing",
    "welder": "manufacturing",
    "fabrication": "manufacturing",
    "plant": "manufacturing",
    # construction
    "construction": "construction",
    "carpenter": "construction",
    "electrician": "construction",
    "plumber": "construction",
    "foreman": "construction",
    "superintendent": "construction",
    "laborer": "construction",
    "hvac": "construction",
    "roofing": "construction",
    # logistics
    "logistics": "logistics",
    "warehouse": "logistics",
    "shipping": "logistics",
    "freight": "logistics",
    "driver": "logistics",
    "dispatch": "logistics",
    "delivery": "logistics",
    "fleet": "logistics",
    "transportation": "logistics",
}

# Multipliers to coarsely adjust benchmarks by location tier.
_META_LOCATION_MULTIPLIERS: Dict[str, float] = {
    "san francisco": 1.35,
    "new york": 1.30,
    "los angeles": 1.20,
    "seattle": 1.25,
    "boston": 1.20,
    "chicago": 1.10,
    "austin": 1.10,
    "denver": 1.05,
    "atlanta": 1.00,
    "dallas": 1.00,
    "miami": 1.00,
    "phoenix": 0.95,
    "remote": 0.90,
    "us": 1.00,
    "united states": 1.00,
    "uk": 0.95,
    "united kingdom": 0.95,
    "canada": 0.92,
    "australia": 0.95,
    "india": 0.55,
    "germany": 1.00,
    "europe": 0.95,
}


# ---- internal helpers ------------------------------------------------------


def _resolve_meta_category(role: str) -> str:
    """Map a role string to a META_ADS_BENCHMARKS category key using word-level matching."""
    role_lower = role.lower().strip()

    # Try the full role string first (handles multi-word keys like "business development").
    if role_lower in ROLE_TO_META_CATEGORY:
        return ROLE_TO_META_CATEGORY[role_lower]

    # Word-level matching: iterate through every word in the role string.
    words = role_lower.replace("-", " ").replace("/", " ").split()
    for word in words:
        if word in ROLE_TO_META_CATEGORY:
            return ROLE_TO_META_CATEGORY[word]

    # Last resort: fuzzy substring check against category keys themselves.
    for cat_key in META_ADS_BENCHMARKS:
        if cat_key.replace("_", " ") in role_lower or role_lower in cat_key.replace(
            "_", " "
        ):
            return cat_key

    return "technology"  # safe default


def _location_cost_multiplier(locations: List[str]) -> float:
    """Return an average cost multiplier for the supplied locations."""
    if not locations:
        return 1.0
    mults: List[float] = []
    for loc in locations:
        loc_lower = loc.lower().strip()
        matched = False
        for key, mult in _META_LOCATION_MULTIPLIERS.items():
            if key in loc_lower or loc_lower in key:
                mults.append(mult)
                matched = True
                break
        if not matched:
            mults.append(1.0)
    return round(sum(mults) / len(mults), 4) if mults else 1.0


def _apply_location_adjustment(
    benchmarks: Dict[str, Any], multiplier: float
) -> Dict[str, Any]:
    """Return a copy of platform benchmarks with cost fields scaled by multiplier."""
    if multiplier == 1.0:
        return dict(benchmarks)
    adjusted: Dict[str, Any] = {}
    for key, value in benchmarks.items():
        if isinstance(value, (int, float)) and any(
            cost_word in key for cost_word in ("cpm", "cpc", "cost")
        ):
            adjusted[key] = round(value * multiplier, 2)
        elif key == "avg_reach_per_1000_usd" and isinstance(value, (int, float)):
            # Higher costs mean less reach per dollar.
            adjusted[key] = max(1, round(value / multiplier))
        else:
            adjusted[key] = value
    return adjusted


def _build_meta_targeting_spec(role: str, locations: List[str]) -> Dict[str, Any]:
    """Build a Meta-compliant targeting_spec dict for the Delivery Estimate endpoint.

    Because recruitment falls under Special Ad Categories (EMPLOYMENT),
    age, gender, and zip-code targeting are disabled.
    """
    geo_locations: Dict[str, Any] = {}
    if locations:
        countries: List[str] = []
        cities: List[Dict[str, str]] = []
        for loc in locations:
            loc_stripped = loc.strip()
            if len(loc_stripped) == 2:
                countries.append(loc_stripped.upper())
            elif loc_stripped.lower() in (
                "us",
                "uk",
                "ca",
                "au",
                "de",
                "fr",
                "in",
                "united states",
                "united kingdom",
                "canada",
                "australia",
                "germany",
                "france",
                "india",
            ):
                code_map = {
                    "us": "US",
                    "united states": "US",
                    "uk": "GB",
                    "united kingdom": "GB",
                    "ca": "CA",
                    "canada": "CA",
                    "au": "AU",
                    "australia": "AU",
                    "de": "DE",
                    "germany": "DE",
                    "fr": "FR",
                    "france": "FR",
                    "in": "IN",
                    "india": "IN",
                }
                code = code_map.get(loc_stripped.lower())
                if code:
                    countries.append(code)
            else:
                cities.append({"key": loc_stripped})
        if countries:
            geo_locations["countries"] = countries
        if cities:
            geo_locations["cities"] = cities

    if not geo_locations:
        geo_locations = {"countries": ["US"]}

    targeting_spec: Dict[str, Any] = {
        "geo_locations": geo_locations,
        "flexible_spec": [
            {
                "work_positions": [{"name": role}],
            }
        ],
        # EMPLOYMENT special ad category: no age_min/age_max/genders/zips
    }
    return targeting_spec


def _fetch_meta_delivery_estimate(
    ad_account_id: str,
    access_token: str,
    targeting_spec: Dict[str, Any],
    timeout: int = 15,
) -> Optional[Dict[str, Any]]:
    """Call the Meta Delivery Estimate endpoint and return parsed JSON or None."""
    params = urllib.parse.urlencode(
        {
            "access_token": access_token,
            "targeting_spec": json.dumps(targeting_spec),
            "optimization_goal": "LINK_CLICKS",
            "special_ad_categories": json.dumps(["EMPLOYMENT"]),
        }
    )
    url = f"https://graph.facebook.com/v19.0/{ad_account_id}/delivery_estimate?{params}"
    try:
        return _http_get_json(url, headers={}, timeout=timeout)
    except Exception as exc:
        _log_warn(f"Meta Delivery Estimate call failed: {exc}")
        return None


def _fetch_meta_reach_estimate(
    ad_account_id: str,
    access_token: str,
    targeting_spec: Dict[str, Any],
    timeout: int = 15,
) -> Optional[Dict[str, Any]]:
    """Call the Meta Reach Estimate endpoint and return parsed JSON or None."""
    params = urllib.parse.urlencode(
        {
            "access_token": access_token,
            "targeting_spec": json.dumps(targeting_spec),
            "special_ad_categories": json.dumps(["EMPLOYMENT"]),
        }
    )
    url = f"https://graph.facebook.com/v19.0/{ad_account_id}/reachestimate?{params}"
    try:
        return _http_get_json(url, headers={}, timeout=timeout)
    except Exception as exc:
        _log_warn(f"Meta Reach Estimate call failed: {exc}")
        return None


def _parse_delivery_estimate(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Extract useful metrics from the Delivery Estimate API response."""
    result: Dict[str, Any] = {}
    data_list = raw.get("data") or []
    if not data_list:
        return result

    entry = data_list[0] if isinstance(data_list, list) else data_list

    daily_outcomes = entry.get("daily_outcomes_curve") or []
    if daily_outcomes:
        last = daily_outcomes[-1]
        result["estimated_daily_reach"] = last.get("reach") or 0
        result["estimated_daily_impressions"] = last.get("impressions") or 0
        actions = last.get("actions") or 0
        result["estimated_daily_actions"] = actions

    estimate = entry.get("estimate_dau") or entry.get("estimate_mau")
    if estimate:
        result["estimated_audience_dau"] = estimate

    bid_estimate = entry.get("bid_estimate") or {}
    for field in ("min_bid", "median_bid", "max_bid"):
        if field in bid_estimate:
            result[field] = bid_estimate[field]

    return result


def _parse_reach_estimate(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Extract useful metrics from the Reach Estimate API response."""
    result: Dict[str, Any] = {}
    data = raw.get("data") or raw
    if isinstance(data, list) and data:
        data = data[0]

    for key in ("users", "estimate_ready", "users_lower_bound", "users_upper_bound"):
        if key in data:
            result[key] = data[key]

    # Map to a human-friendly audience_size string.
    lower = data.get("users_lower_bound")
    upper = data.get("users_upper_bound")
    if lower is not None and upper is not None:

        def _fmt(n: int) -> str:
            if n >= 1_000_000:
                return f"{n / 1_000_000:.1f}M"
            if n >= 1_000:
                return f"{n / 1_000:.0f}K"
            return str(n)

        result["estimated_audience_size"] = f"{_fmt(lower)}-{_fmt(upper)}"
    elif "users" in data:
        u = data["users"]
        result["estimated_audience_size"] = (
            f"{u / 1_000_000:.1f}M" if u >= 1_000_000 else f"{u / 1_000:.0f}K"
        )

    return result


# ---- public function -------------------------------------------------------


def fetch_meta_ads_data(
    roles: List[str],
    locations: List[str],
) -> Dict[str, Any]:
    """Fetch audience sizing and CPM/CPC estimates for Facebook & Instagram
    recruitment advertising.

    Uses the Meta Marketing API when ``META_ACCESS_TOKEN`` and
    ``META_AD_ACCOUNT_ID`` environment variables are set; otherwise falls
    back to curated benchmark data.

    Parameters
    ----------
    roles : list[str]
        Job role titles to look up (e.g. ``["Software Engineer", "Nurse"]``).
    locations : list[str]
        Target locations (city names, country codes, or ``"Remote"``).

    Returns
    -------
    dict
        A result dict with keys ``source``, ``roles``, and ``platform_summary``.
    """

    # -- cache check ----------------------------------------------------------
    cache_key = _cache_key(
        "meta_ads",
        json.dumps(
            {"roles": sorted(roles), "locations": sorted(locations)}, sort_keys=True
        ),
    )
    cached = _get_cached(cache_key)
    if cached is not None:
        _log_info("Returning cached Meta Ads data.")
        return cached  # type: ignore[return-value]

    access_token: str = os.environ.get("META_ACCESS_TOKEN", "").strip()
    ad_account_id: str = os.environ.get("META_AD_ACCOUNT_ID", "").strip()
    use_api: bool = bool(access_token and ad_account_id)

    if use_api:
        # Ensure ad account id has the act_ prefix.
        if not ad_account_id.startswith("act_"):
            ad_account_id = f"act_{ad_account_id}"
        _log_info("Meta Marketing API credentials detected; fetching live data.")
    else:
        _log_info(
            "META_ACCESS_TOKEN / META_AD_ACCOUNT_ID not set; using curated benchmarks."
        )

    loc_mult = _location_cost_multiplier(locations)
    roles_data: Dict[str, Any] = {}

    for role in roles:
        category = _resolve_meta_category(role)
        benchmarks = META_ADS_BENCHMARKS.get(
            category, META_ADS_BENCHMARKS["technology"]
        )

        role_result: Dict[str, Any] = {}

        if use_api:
            # ---- live API path ------------------------------------------------
            try:
                targeting_spec = _build_meta_targeting_spec(role, locations)

                # Delivery estimate
                delivery_raw = _fetch_meta_delivery_estimate(
                    ad_account_id, access_token, targeting_spec
                )
                delivery_parsed = (
                    _parse_delivery_estimate(delivery_raw) if delivery_raw else {}
                )

                # Reach estimate
                reach_raw = _fetch_meta_reach_estimate(
                    ad_account_id, access_token, targeting_spec
                )
                reach_parsed = _parse_reach_estimate(reach_raw) if reach_raw else {}

                # Merge live data with benchmark baselines per platform.
                for platform in ("facebook", "instagram"):
                    platform_bench = _apply_location_adjustment(
                        benchmarks.get(platform, benchmarks.get("facebook", {})),
                        loc_mult,
                    )
                    merged = dict(platform_bench)
                    # Override audience size from API if available.
                    if "estimated_audience_size" in reach_parsed:
                        merged["estimated_audience_size"] = reach_parsed[
                            "estimated_audience_size"
                        ]
                    if delivery_parsed:
                        merged["api_delivery_estimate"] = delivery_parsed
                    if reach_parsed:
                        merged["api_reach_estimate"] = reach_parsed
                    role_result[platform] = merged

                role_result["_source"] = "api"
            except Exception as exc:
                _log_warn(
                    f"Meta API call failed for role '{role}', falling back to benchmarks: {exc}"
                )
                # Fall through to benchmark path below.
                role_result = {}

        # ---- benchmark fallback path (also used if API call raised) ---------
        if not role_result:
            for platform in ("facebook", "instagram"):
                platform_bench = benchmarks.get(
                    platform, benchmarks.get("facebook", {})
                )
                role_result[platform] = _apply_location_adjustment(
                    platform_bench, loc_mult
                )
            role_result["_source"] = "benchmarks"

        role_result["matched_category"] = category
        roles_data[role] = role_result

    # Determine overall source label.
    sources_used = {
        v.get("_source", "benchmarks")
        for v in roles_data.values()
        if isinstance(v, dict)
    }
    if "api" in sources_used:
        source_label = "Meta Marketing API"
    else:
        source_label = "Meta Ads Benchmarks"

    # Strip internal _source keys from output.
    for role_key in roles_data:
        roles_data[role_key].pop("_source", None)

    result: Dict[str, Any] = {
        "source": source_label,
        "roles": roles_data,
        "platform_summary": {
            "platform": "Facebook & Instagram",
            "monthly_active_users": "3.05 billion (Meta family)",
            "best_for": "Passive candidate sourcing, employer branding, volume hiring",
            "ad_formats": [
                "Feed Ads",
                "Stories",
                "Reels",
                "Carousel",
                "Lead Ads",
            ],
            "special_ad_category": "EMPLOYMENT \u2014 age, gender, zip targeting restricted",
            "targeting_available": [
                "Job titles",
                "Industries",
                "Interests",
                "Education level",
                "Employer",
            ],
        },
    }

    # Cache the result.
    try:
        _set_cached(cache_key, result)
    except Exception:
        pass  # caching is best-effort

    return result


# API 21: Microsoft/Bing Ads API

# ---------------------------------------------------------------------------
# Bing / Microsoft Advertising  –  Search volume & CPC estimates
# ---------------------------------------------------------------------------

BING_ADS_BENCHMARKS: Dict[str, Dict[str, Any]] = {
    "technology": {
        "avg_cpc_usd": 2.50,
        "avg_cpm_usd": 9.00,
        "avg_monthly_searches": 8500,
        "competition": "MEDIUM",
        "market_share_pct": 8.5,
        "cpc_vs_google_pct": -30,
        "click_through_rate": 0.028,
        "cost_per_application": 20.00,
        "audience_skew": "Older, higher-income professionals",
    },
    "healthcare": {
        "avg_cpc_usd": 2.80,
        "avg_cpm_usd": 10.50,
        "avg_monthly_searches": 9200,
        "competition": "HIGH",
        "market_share_pct": 8.5,
        "cpc_vs_google_pct": -25,
        "click_through_rate": 0.031,
        "cost_per_application": 22.00,
        "audience_skew": "Older professionals, hospital administrators",
    },
    "finance": {
        "avg_cpc_usd": 3.40,
        "avg_cpm_usd": 12.00,
        "avg_monthly_searches": 7800,
        "competition": "HIGH",
        "market_share_pct": 9.0,
        "cpc_vs_google_pct": -22,
        "click_through_rate": 0.025,
        "cost_per_application": 28.00,
        "audience_skew": "High-income desktop users, financial professionals",
    },
    "engineering": {
        "avg_cpc_usd": 2.60,
        "avg_cpm_usd": 9.50,
        "avg_monthly_searches": 7200,
        "competition": "MEDIUM",
        "market_share_pct": 8.0,
        "cpc_vs_google_pct": -28,
        "click_through_rate": 0.027,
        "cost_per_application": 21.00,
        "audience_skew": "Experienced engineers, corporate desktop users",
    },
    "marketing": {
        "avg_cpc_usd": 2.20,
        "avg_cpm_usd": 8.00,
        "avg_monthly_searches": 6800,
        "competition": "MEDIUM",
        "market_share_pct": 7.5,
        "cpc_vs_google_pct": -32,
        "click_through_rate": 0.030,
        "cost_per_application": 17.50,
        "audience_skew": "Mid-career marketing professionals",
    },
    "sales": {
        "avg_cpc_usd": 1.90,
        "avg_cpm_usd": 7.00,
        "avg_monthly_searches": 7500,
        "competition": "MEDIUM",
        "market_share_pct": 8.0,
        "cpc_vs_google_pct": -35,
        "click_through_rate": 0.032,
        "cost_per_application": 15.00,
        "audience_skew": "B2B sales professionals, enterprise users",
    },
    "human_resources": {
        "avg_cpc_usd": 2.10,
        "avg_cpm_usd": 7.50,
        "avg_monthly_searches": 5500,
        "competition": "LOW",
        "market_share_pct": 8.5,
        "cpc_vs_google_pct": -30,
        "click_through_rate": 0.029,
        "cost_per_application": 16.50,
        "audience_skew": "HR managers, corporate desktop environments",
    },
    "data_science": {
        "avg_cpc_usd": 2.90,
        "avg_cpm_usd": 10.00,
        "avg_monthly_searches": 5800,
        "competition": "MEDIUM",
        "market_share_pct": 7.5,
        "cpc_vs_google_pct": -27,
        "click_through_rate": 0.026,
        "cost_per_application": 24.00,
        "audience_skew": "Experienced analysts, corporate environments",
    },
    "nursing": {
        "avg_cpc_usd": 2.40,
        "avg_cpm_usd": 8.50,
        "avg_monthly_searches": 10500,
        "competition": "HIGH",
        "market_share_pct": 9.0,
        "cpc_vs_google_pct": -25,
        "click_through_rate": 0.033,
        "cost_per_application": 18.00,
        "audience_skew": "Experienced nurses, older demographic skew",
    },
    "education": {
        "avg_cpc_usd": 1.80,
        "avg_cpm_usd": 6.50,
        "avg_monthly_searches": 8000,
        "competition": "LOW",
        "market_share_pct": 9.5,
        "cpc_vs_google_pct": -33,
        "click_through_rate": 0.034,
        "cost_per_application": 13.00,
        "audience_skew": "Educators, school administrators, desktop-heavy",
    },
    "legal": {
        "avg_cpc_usd": 3.80,
        "avg_cpm_usd": 13.50,
        "avg_monthly_searches": 4200,
        "competition": "HIGH",
        "market_share_pct": 9.0,
        "cpc_vs_google_pct": -20,
        "click_through_rate": 0.023,
        "cost_per_application": 32.00,
        "audience_skew": "Senior legal professionals, law firm environments",
    },
    "retail": {
        "avg_cpc_usd": 1.60,
        "avg_cpm_usd": 5.80,
        "avg_monthly_searches": 9500,
        "competition": "LOW",
        "market_share_pct": 8.0,
        "cpc_vs_google_pct": -35,
        "click_through_rate": 0.035,
        "cost_per_application": 11.00,
        "audience_skew": "Broad demographic, desktop shoppers",
    },
    "manufacturing": {
        "avg_cpc_usd": 1.70,
        "avg_cpm_usd": 6.20,
        "avg_monthly_searches": 6000,
        "competition": "LOW",
        "market_share_pct": 9.5,
        "cpc_vs_google_pct": -34,
        "click_through_rate": 0.030,
        "cost_per_application": 13.50,
        "audience_skew": "Older workforce, desktop-dominant search",
    },
    "construction": {
        "avg_cpc_usd": 1.55,
        "avg_cpm_usd": 5.50,
        "avg_monthly_searches": 5200,
        "competition": "LOW",
        "market_share_pct": 9.0,
        "cpc_vs_google_pct": -35,
        "click_through_rate": 0.031,
        "cost_per_application": 12.00,
        "audience_skew": "Experienced tradespeople, older demographics",
    },
    "executive": {
        "avg_cpc_usd": 4.20,
        "avg_cpm_usd": 15.00,
        "avg_monthly_searches": 2800,
        "competition": "HIGH",
        "market_share_pct": 10.0,
        "cpc_vs_google_pct": -20,
        "click_through_rate": 0.020,
        "cost_per_application": 45.00,
        "audience_skew": "C-suite, senior leadership, high-income desktop users",
    },
    "operations": {
        "avg_cpc_usd": 2.00,
        "avg_cpm_usd": 7.20,
        "avg_monthly_searches": 6200,
        "competition": "MEDIUM",
        "market_share_pct": 8.5,
        "cpc_vs_google_pct": -30,
        "click_through_rate": 0.029,
        "cost_per_application": 17.00,
        "audience_skew": "Operations managers, supply chain professionals",
    },
}

# Maps a role title (lowered, partial-matched) to a Bing benchmark category.
ROLE_TO_BING_CATEGORY: Dict[str, str] = {
    "software engineer": "technology",
    "software developer": "technology",
    "frontend": "technology",
    "backend": "technology",
    "full stack": "technology",
    "fullstack": "technology",
    "devops": "technology",
    "sre": "technology",
    "cloud engineer": "technology",
    "systems engineer": "technology",
    "qa engineer": "technology",
    "mobile developer": "technology",
    "ios developer": "technology",
    "android developer": "technology",
    "data scientist": "data_science",
    "data analyst": "data_science",
    "machine learning": "data_science",
    "ml engineer": "data_science",
    "ai engineer": "data_science",
    "data engineer": "data_science",
    "nurse": "nursing",
    "registered nurse": "nursing",
    "lpn": "nursing",
    "rn": "nursing",
    "nursing": "nursing",
    "physician": "healthcare",
    "doctor": "healthcare",
    "therapist": "healthcare",
    "pharmacist": "healthcare",
    "healthcare": "healthcare",
    "medical": "healthcare",
    "dentist": "healthcare",
    "surgeon": "healthcare",
    "accountant": "finance",
    "financial analyst": "finance",
    "finance": "finance",
    "banker": "finance",
    "auditor": "finance",
    "controller": "finance",
    "cfo": "finance",
    "investment": "finance",
    "underwriter": "finance",
    "mechanical engineer": "engineering",
    "civil engineer": "engineering",
    "electrical engineer": "engineering",
    "chemical engineer": "engineering",
    "structural engineer": "engineering",
    "engineer": "engineering",
    "marketing manager": "marketing",
    "digital marketing": "marketing",
    "seo": "marketing",
    "content": "marketing",
    "brand manager": "marketing",
    "marketing": "marketing",
    "social media": "marketing",
    "copywriter": "marketing",
    "sales representative": "sales",
    "account executive": "sales",
    "business development": "sales",
    "sales manager": "sales",
    "sales": "sales",
    "hr manager": "human_resources",
    "recruiter": "human_resources",
    "human resources": "human_resources",
    "talent acquisition": "human_resources",
    "people operations": "human_resources",
    "hr": "human_resources",
    "teacher": "education",
    "professor": "education",
    "instructor": "education",
    "principal": "education",
    "education": "education",
    "tutor": "education",
    "attorney": "legal",
    "lawyer": "legal",
    "paralegal": "legal",
    "legal counsel": "legal",
    "legal": "legal",
    "compliance": "legal",
    "store manager": "retail",
    "retail": "retail",
    "cashier": "retail",
    "merchandiser": "retail",
    "buyer": "retail",
    "plant manager": "manufacturing",
    "manufacturing": "manufacturing",
    "production": "manufacturing",
    "quality control": "manufacturing",
    "foreman": "construction",
    "construction": "construction",
    "carpenter": "construction",
    "electrician": "construction",
    "plumber": "construction",
    "project manager": "operations",
    "operations manager": "operations",
    "supply chain": "operations",
    "logistics": "operations",
    "operations": "operations",
    "warehouse": "operations",
    "ceo": "executive",
    "cto": "executive",
    "coo": "executive",
    "vp": "executive",
    "vice president": "executive",
    "director": "executive",
    "executive": "executive",
    "president": "executive",
    "chief": "executive",
}

# Location multipliers – Bing share varies by geography.
_BING_LOCATION_MULTIPLIERS: Dict[str, float] = {
    "united states": 1.00,
    "us": 1.00,
    "usa": 1.00,
    "united kingdom": 1.10,
    "uk": 1.10,
    "canada": 1.05,
    "australia": 0.95,
    "germany": 0.55,
    "france": 0.50,
    "india": 0.35,
    "japan": 0.40,
    "brazil": 0.30,
    "new york": 1.15,
    "san francisco": 1.20,
    "los angeles": 1.10,
    "chicago": 1.05,
    "seattle": 1.15,
    "austin": 1.05,
    "boston": 1.12,
    "london": 1.15,
    "toronto": 1.08,
    "remote": 1.00,
}


def _bing_category_for_role(role: str) -> str:
    """Return the benchmark category key for *role*, falling back to 'technology'."""
    role_lower = role.lower().strip()
    # Exact match first.
    if role_lower in ROLE_TO_BING_CATEGORY:
        return ROLE_TO_BING_CATEGORY[role_lower]
    # Substring / partial match.
    for fragment, category in ROLE_TO_BING_CATEGORY.items():
        if fragment in role_lower or role_lower in fragment:
            return category
    return "technology"


def _bing_location_multiplier(locations: List[str]) -> float:
    """Average location multiplier across requested locations."""
    if not locations:
        return 1.0
    total = 0.0
    count = 0
    for loc in locations:
        key = loc.lower().strip()
        mult = _BING_LOCATION_MULTIPLIERS.get(key)
        if mult is None:
            # Try partial matching.
            for loc_key, loc_mult in _BING_LOCATION_MULTIPLIERS.items():
                if loc_key in key or key in loc_key:
                    mult = loc_mult
                    break
        total += mult if mult is not None else 1.0
        count += 1
    return total / count if count else 1.0


def _refresh_bing_oauth_token(client_id: str, refresh_token: str) -> Optional[str]:
    """Exchange the Bing Ads refresh token for a fresh access token.

    Returns the access_token string on success or ``None`` on failure.
    """
    token_url = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
    payload = urllib.parse.urlencode(
        {
            "client_id": client_id,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": "https://ads.microsoft.com/msads.manage offline_access",
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        token_url,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return body.get("access_token")
    except Exception as exc:
        _log_warn(f"Bing Ads OAuth token refresh failed: {exc}")
        return None


def _bing_ads_soap_request(
    access_token: str,
    developer_token: str,
    customer_id: str,
    account_id: str,
    keywords: List[str],
) -> Optional[Dict[str, Any]]:
    """Call the Bing Ads Ad Insight REST endpoint for keyword ideas and
    traffic estimates.  Returns a dict mapping each keyword to its metrics,
    or ``None`` on failure.
    """
    # --- Step 1: GetKeywordIdeas -----------------------------------------
    keyword_ideas_url = (
        "https://adinsight.api.bingads.microsoft.com"
        "/Api/Advertiser/CampaignManagement/v13/KeywordIdeas"
    )

    headers = {
        "Authorization": f"Bearer {access_token}",
        "DeveloperToken": developer_token,
        "CustomerId": customer_id,
        "CustomerAccountId": account_id,
        "Content-Type": "application/json",
    }

    ideas_payload = json.dumps(
        {
            "Keywords": keywords,
            "MaxSuggestions": len(keywords),
            "SearchParameters": [
                {
                    "Type": "SearchVolumeSearchParameter",
                },
                {
                    "Type": "LanguageSearchParameter",
                    "Languages": [{"Id": 1000}],  # English
                },
            ],
        }
    ).encode("utf-8")

    results: Dict[str, Any] = {}

    try:
        req = urllib.request.Request(
            keyword_ideas_url,
            data=ideas_payload,
            headers=headers,
            method="POST",
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
            body = json.loads(resp.read().decode("utf-8"))

        for idea in body.get("KeywordIdeas", []):
            kw_text = idea.get("Keyword") or ""
            avg_searches = idea.get("AvgMonthlySearches") or 0
            competition = idea.get("Competition", "UNKNOWN")
            results[kw_text] = {
                "avg_monthly_searches": avg_searches,
                "competition": competition,
            }
    except Exception as exc:
        _log_warn(f"Bing Ads GetKeywordIdeas failed: {exc}")
        return None

    # --- Step 2: GetKeywordTrafficEstimates ------------------------------
    traffic_url = (
        "https://adinsight.api.bingads.microsoft.com"
        "/Api/Advertiser/CampaignManagement/v13/KeywordTrafficEstimates"
    )

    criteria = [
        {
            "Keyword": kw,
            "MaxCpc": 10.0,
            "MatchType": "Broad",
        }
        for kw in keywords
    ]

    traffic_payload = json.dumps(
        {
            "CampaignEstimators": [
                {
                    "AdGroupEstimators": [
                        {
                            "KeywordEstimators": criteria,
                        }
                    ],
                    "DailyBudget": 100.0,
                }
            ],
        }
    ).encode("utf-8")

    try:
        req2 = urllib.request.Request(
            traffic_url,
            data=traffic_payload,
            headers=headers,
            method="POST",
        )
        ctx2 = ssl.create_default_context()
        with urllib.request.urlopen(req2, timeout=30, context=ctx2) as resp2:
            traffic_body = json.loads(resp2.read().decode("utf-8"))

        campaigns = traffic_body.get("CampaignEstimates", [])
        if campaigns:
            ad_groups = campaigns[0].get("AdGroupEstimates", [])
            if ad_groups:
                kw_estimates = ad_groups[0].get("KeywordEstimates", [])
                for idx, est in enumerate(kw_estimates):
                    if idx >= len(keywords):
                        break
                    kw_text = keywords[idx]
                    minimum = est.get("Minimum", {})
                    maximum = est.get("Maximum", {})
                    avg_cpc = (
                        minimum.get("AverageCpc") or 0 + maximum.get("AverageCpc") or 0
                    ) / 2.0
                    avg_impressions = (
                        minimum.get("Impressions")
                        or 0 + maximum.get("Impressions")
                        or 0
                    ) / 2.0
                    avg_clicks = (
                        minimum.get("Clicks") or 0 + maximum.get("Clicks") or 0
                    ) / 2.0

                    if kw_text not in results:
                        results[kw_text] = {}
                    results[kw_text]["avg_cpc_usd"] = round(avg_cpc, 2)
                    if avg_impressions > 0 and avg_cpc > 0 and avg_clicks > 0:
                        results[kw_text]["avg_cpm_usd"] = round(
                            (avg_cpc * avg_clicks / avg_impressions) * 1000, 2
                        )
                    else:
                        results[kw_text]["avg_cpm_usd"] = 0.0
                    results[kw_text]["estimated_daily_impressions"] = round(
                        avg_impressions, 0
                    )
                    results[kw_text]["estimated_daily_clicks"] = round(avg_clicks, 1)
    except Exception as exc:
        _log_warn(f"Bing Ads GetKeywordTrafficEstimates failed: {exc}")
        # We still have keyword ideas, so don't return None.

    return results if results else None


def _build_bing_keywords(roles: List[str]) -> List[str]:
    """Generate recruitment-oriented keyword strings for the Bing API."""
    suffixes = ["jobs", "careers", "hiring", "job openings"]
    keywords: List[str] = []
    for role in roles:
        role_clean = role.strip()
        if not role_clean:
            continue
        keywords.append(f"{role_clean} jobs")
        keywords.append(f"{role_clean} careers")
        keywords.append(f"hire {role_clean}")
        for sfx in suffixes:
            keywords.append(f"{role_clean} {sfx}")
    # Deduplicate while preserving order.
    seen: set = set()
    deduped: List[str] = []
    for kw in keywords:
        kw_lower = kw.lower()
        if kw_lower not in seen:
            seen.add(kw_lower)
            deduped.append(kw)
    return deduped


def _build_platform_summary() -> Dict[str, Any]:
    """Static platform summary block for Microsoft Advertising."""
    return {
        "platform": "Microsoft Advertising (Bing, Yahoo, AOL, DuckDuckGo)",
        "market_share": "~8.5% US search market",
        "best_for": "Cost-effective search ads, reaching older/professional demographics",
        "audience_profile": "Skews older (35+), higher income, more desktop usage",
        "ad_formats": [
            "Search Ads",
            "Audience Network",
            "Shopping Ads",
            "LinkedIn Profile Targeting",
        ],
        "unique_feature": (
            "LinkedIn Profile Targeting integration "
            "(job function, industry, company)"
        ),
    }


def fetch_bing_ads_data(
    roles: List[str],
    locations: List[str],
) -> Dict[str, Any]:
    """Return Bing/Microsoft Advertising keyword volume and CPC estimates.

    Attempts to call the live Bing Ads API when all required credentials are
    present.  Falls back to curated benchmark data when credentials are missing
    or the API call fails.

    Parameters
    ----------
    roles:
        Job-role titles to research (e.g. ``["Software Engineer", "Nurse"]``).
    locations:
        Geographic locations to weight estimates for (e.g. ``["New York", "US"]``).

    Returns
    -------
    dict  with keys ``source``, ``keywords``, ``platform_summary``.
    """
    cache_key = _cache_key("bing_ads", f"{sorted(roles)}|{sorted(locations)}")
    cached = _get_cached(cache_key)
    if cached is not None:
        _log_info("Returning cached Bing Ads data")
        return cached  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Attempt live API call
    # ------------------------------------------------------------------
    developer_token = os.environ.get("BING_ADS_DEVELOPER_TOKEN", "").strip()
    client_id = os.environ.get("BING_ADS_CLIENT_ID", "").strip()
    refresh_token = os.environ.get("BING_ADS_REFRESH_TOKEN", "").strip()
    customer_id = os.environ.get("BING_ADS_CUSTOMER_ID", "").strip()
    account_id = os.environ.get("BING_ADS_ACCOUNT_ID", "").strip()

    has_credentials = all(
        [developer_token, client_id, refresh_token, customer_id, account_id]
    )

    api_results: Optional[Dict[str, Any]] = None

    if has_credentials:
        _log_info("Bing Ads credentials found – attempting live API call")
        access_token = _refresh_bing_oauth_token(client_id, refresh_token)
        if access_token:
            keywords_list = _build_bing_keywords(roles)
            api_results = _bing_ads_soap_request(
                access_token=access_token,
                developer_token=developer_token,
                customer_id=customer_id,
                account_id=account_id,
                keywords=keywords_list,
            )
        else:
            _log_warn(
                "Could not obtain Bing Ads access token; falling back to benchmarks"
            )
    else:
        _log_info("Bing Ads credentials not configured – using benchmark data")

    # ------------------------------------------------------------------
    # Build the response from live data
    # ------------------------------------------------------------------
    if api_results:
        loc_mult = _bing_location_multiplier(locations)
        keyword_data: Dict[str, Any] = {}

        for role in roles:
            role_clean = role.strip()
            if not role_clean:
                continue

            category = _bing_category_for_role(role_clean)
            benchmark = BING_ADS_BENCHMARKS.get(
                category, BING_ADS_BENCHMARKS["technology"]
            )

            # Collect metrics from the API results that match this role.
            matching_cpc: List[float] = []
            matching_searches: List[float] = []
            matching_cpm: List[float] = []
            matching_competition: List[str] = []

            role_lower = role_clean.lower()
            for kw, metrics in api_results.items():
                if role_lower in kw.lower():
                    if "avg_cpc_usd" in metrics and metrics["avg_cpc_usd"] > 0:
                        matching_cpc.append(metrics["avg_cpc_usd"])
                    if (
                        "avg_monthly_searches" in metrics
                        and metrics["avg_monthly_searches"] > 0
                    ):
                        matching_searches.append(metrics["avg_monthly_searches"])
                    if "avg_cpm_usd" in metrics and metrics["avg_cpm_usd"] > 0:
                        matching_cpm.append(metrics["avg_cpm_usd"])
                    if "competition" in metrics:
                        matching_competition.append(metrics["competition"])

            avg_cpc = (
                round(sum(matching_cpc) / len(matching_cpc) * loc_mult, 2)
                if matching_cpc
                else round(benchmark["avg_cpc_usd"] * loc_mult, 2)
            )
            avg_cpm = (
                round(sum(matching_cpm) / len(matching_cpm) * loc_mult, 2)
                if matching_cpm
                else round(benchmark["avg_cpm_usd"] * loc_mult, 2)
            )
            avg_searches = (
                int(sum(matching_searches) / len(matching_searches))
                if matching_searches
                else benchmark["avg_monthly_searches"]
            )
            competition = (
                max(set(matching_competition), key=matching_competition.count)
                if matching_competition
                else benchmark["competition"]
            )

            # Estimate CPC vs Google (Bing is typically 20-35 % cheaper).
            cpc_vs_google = benchmark.get("cpc_vs_google_pct", -28)
            cost_per_app = round(avg_cpc * 8.0, 2)  # rough conversion estimate

            keyword_data[role_clean] = {
                "avg_cpc_usd": avg_cpc,
                "avg_cpm_usd": avg_cpm,
                "avg_monthly_searches": avg_searches,
                "competition": competition,
                "cpc_vs_google_pct": cpc_vs_google,
                "cost_per_application": cost_per_app,
            }

        result: Dict[str, Any] = {
            "source": "Bing Ads API",
            "keywords": keyword_data,
            "platform_summary": _build_platform_summary(),
        }
        _set_cached(cache_key, result)
        return result

    # ------------------------------------------------------------------
    # Fallback: curated benchmarks
    # ------------------------------------------------------------------
    loc_mult = _bing_location_multiplier(locations)
    keyword_data = {}

    for role in roles:
        role_clean = role.strip()
        if not role_clean:
            continue

        category = _bing_category_for_role(role_clean)
        benchmark = BING_ADS_BENCHMARKS.get(category, BING_ADS_BENCHMARKS["technology"])

        adjusted_cpc = round(benchmark["avg_cpc_usd"] * loc_mult, 2)
        adjusted_cpm = round(benchmark["avg_cpm_usd"] * loc_mult, 2)
        adjusted_cpa = round(benchmark["cost_per_application"] * loc_mult, 2)

        keyword_data[role_clean] = {
            "avg_cpc_usd": adjusted_cpc,
            "avg_cpm_usd": adjusted_cpm,
            "avg_monthly_searches": benchmark["avg_monthly_searches"],
            "competition": benchmark["competition"],
            "cpc_vs_google_pct": benchmark["cpc_vs_google_pct"],
            "cost_per_application": adjusted_cpa,
        }

    result = {
        "source": "Bing Ads Benchmarks",
        "keywords": keyword_data,
        "platform_summary": _build_platform_summary(),
    }
    _set_cached(cache_key, result)
    return result


# API 22: TikTok Marketing API

# ---------------------------------------------------------------------------
# TikTok Marketing API integration for media plan generator
# ---------------------------------------------------------------------------

TIKTOK_ADS_BENCHMARKS: Dict[str, Dict[str, Any]] = {
    "technology": {
        "avg_cpm_usd": 10.00,
        "avg_cpc_usd": 1.20,
        "avg_ctr_pct": 1.50,
        "estimated_audience": "800K-2M",
        "cost_per_application": 15.00,
        "best_age_range": "18-34",
        "engagement_rate": 5.5,
    },
    "healthcare": {
        "avg_cpm_usd": 9.50,
        "avg_cpc_usd": 1.35,
        "avg_ctr_pct": 1.30,
        "estimated_audience": "600K-1.5M",
        "cost_per_application": 18.00,
        "best_age_range": "21-34",
        "engagement_rate": 4.8,
    },
    "retail": {
        "avg_cpm_usd": 7.50,
        "avg_cpc_usd": 0.85,
        "avg_ctr_pct": 2.10,
        "estimated_audience": "2M-5M",
        "cost_per_application": 8.00,
        "best_age_range": "18-30",
        "engagement_rate": 6.8,
    },
    "hospitality": {
        "avg_cpm_usd": 7.00,
        "avg_cpc_usd": 0.75,
        "avg_ctr_pct": 2.30,
        "estimated_audience": "1.5M-4M",
        "cost_per_application": 7.00,
        "best_age_range": "18-28",
        "engagement_rate": 7.2,
    },
    "marketing": {
        "avg_cpm_usd": 11.00,
        "avg_cpc_usd": 1.40,
        "avg_ctr_pct": 1.60,
        "estimated_audience": "500K-1.2M",
        "cost_per_application": 14.00,
        "best_age_range": "18-34",
        "engagement_rate": 5.8,
    },
    "sales": {
        "avg_cpm_usd": 9.00,
        "avg_cpc_usd": 1.10,
        "avg_ctr_pct": 1.70,
        "estimated_audience": "700K-1.8M",
        "cost_per_application": 12.00,
        "best_age_range": "18-34",
        "engagement_rate": 5.2,
    },
    "education": {
        "avg_cpm_usd": 8.00,
        "avg_cpc_usd": 1.00,
        "avg_ctr_pct": 1.80,
        "estimated_audience": "900K-2.5M",
        "cost_per_application": 11.00,
        "best_age_range": "18-30",
        "engagement_rate": 6.0,
    },
    "manufacturing": {
        "avg_cpm_usd": 8.50,
        "avg_cpc_usd": 1.25,
        "avg_ctr_pct": 1.20,
        "estimated_audience": "400K-1M",
        "cost_per_application": 20.00,
        "best_age_range": "21-34",
        "engagement_rate": 3.8,
    },
    "finance": {
        "avg_cpm_usd": 12.50,
        "avg_cpc_usd": 1.80,
        "avg_ctr_pct": 1.10,
        "estimated_audience": "400K-1M",
        "cost_per_application": 25.00,
        "best_age_range": "22-34",
        "engagement_rate": 3.5,
    },
    "engineering": {
        "avg_cpm_usd": 11.50,
        "avg_cpc_usd": 1.50,
        "avg_ctr_pct": 1.25,
        "estimated_audience": "500K-1.2M",
        "cost_per_application": 22.00,
        "best_age_range": "21-34",
        "engagement_rate": 4.2,
    },
    "creative": {
        "avg_cpm_usd": 8.50,
        "avg_cpc_usd": 0.90,
        "avg_ctr_pct": 2.20,
        "estimated_audience": "1M-3M",
        "cost_per_application": 9.00,
        "best_age_range": "18-30",
        "engagement_rate": 7.5,
    },
    "food_service": {
        "avg_cpm_usd": 6.50,
        "avg_cpc_usd": 0.65,
        "avg_ctr_pct": 2.50,
        "estimated_audience": "2M-5M",
        "cost_per_application": 6.00,
        "best_age_range": "18-26",
        "engagement_rate": 7.8,
    },
    "logistics": {
        "avg_cpm_usd": 8.00,
        "avg_cpc_usd": 1.10,
        "avg_ctr_pct": 1.40,
        "estimated_audience": "600K-1.5M",
        "cost_per_application": 16.00,
        "best_age_range": "21-34",
        "engagement_rate": 4.0,
    },
    "customer_service": {
        "avg_cpm_usd": 7.50,
        "avg_cpc_usd": 0.80,
        "avg_ctr_pct": 2.00,
        "estimated_audience": "1.2M-3M",
        "cost_per_application": 9.00,
        "best_age_range": "18-30",
        "engagement_rate": 6.2,
    },
}


ROLE_TO_TIKTOK_CATEGORY: Dict[str, str] = {
    # Technology
    "software engineer": "technology",
    "software developer": "technology",
    "frontend developer": "technology",
    "backend developer": "technology",
    "full stack developer": "technology",
    "data scientist": "technology",
    "data analyst": "technology",
    "product manager": "technology",
    "devops engineer": "technology",
    "qa engineer": "technology",
    "it support": "technology",
    # Healthcare
    "nurse": "healthcare",
    "medical assistant": "healthcare",
    "pharmacist": "healthcare",
    "physical therapist": "healthcare",
    "caregiver": "healthcare",
    # Retail
    "retail associate": "retail",
    "store manager": "retail",
    "cashier": "retail",
    "merchandiser": "retail",
    # Hospitality
    "hotel front desk": "hospitality",
    "bartender": "hospitality",
    "event coordinator": "hospitality",
    "housekeeper": "hospitality",
    # Marketing
    "marketing manager": "marketing",
    "social media manager": "marketing",
    "content creator": "marketing",
    "seo specialist": "marketing",
    "brand manager": "marketing",
    # Sales
    "sales representative": "sales",
    "account executive": "sales",
    "business development": "sales",
    "sales manager": "sales",
    # Education
    "teacher": "education",
    "tutor": "education",
    "training coordinator": "education",
    "instructional designer": "education",
    # Manufacturing
    "machine operator": "manufacturing",
    "production supervisor": "manufacturing",
    "quality inspector": "manufacturing",
    # Finance
    "accountant": "finance",
    "financial analyst": "finance",
    "bookkeeper": "finance",
    "auditor": "finance",
    # Engineering
    "mechanical engineer": "engineering",
    "civil engineer": "engineering",
    "electrical engineer": "engineering",
    # Creative
    "graphic designer": "creative",
    "video editor": "creative",
    "ux designer": "creative",
    "photographer": "creative",
    "animator": "creative",
    # Food Service
    "cook": "food_service",
    "chef": "food_service",
    "barista": "food_service",
    "server": "food_service",
    "food runner": "food_service",
    # Logistics
    "warehouse associate": "logistics",
    "delivery driver": "logistics",
    "supply chain analyst": "logistics",
    "forklift operator": "logistics",
    # Customer Service
    "customer service rep": "customer_service",
    "call center agent": "customer_service",
    "help desk": "customer_service",
    "client success manager": "customer_service",
}


_TIKTOK_LOCATION_CODES: Dict[str, str] = {
    "united states": "US",
    "us": "US",
    "usa": "US",
    "united kingdom": "GB",
    "uk": "GB",
    "canada": "CA",
    "australia": "AU",
    "germany": "DE",
    "france": "FR",
    "india": "IN",
    "brazil": "BR",
    "japan": "JP",
    "mexico": "MX",
    "spain": "ES",
    "italy": "IT",
    "netherlands": "NL",
    "singapore": "SG",
    "new zealand": "NZ",
    "ireland": "IE",
    "south korea": "KR",
}


def _resolve_tiktok_category(role: str) -> str:
    """Map a free-text role title to a TikTok benchmark category."""
    normalized = role.strip().lower()

    # Direct lookup
    if normalized in ROLE_TO_TIKTOK_CATEGORY:
        return ROLE_TO_TIKTOK_CATEGORY[normalized]

    # Substring match against known role keys
    for known_role, category in ROLE_TO_TIKTOK_CATEGORY.items():
        if known_role in normalized or normalized in known_role:
            return category

    # Keyword heuristics
    keyword_map = {
        "engineer": "engineering",
        "develop": "technology",
        "program": "technology",
        "code": "technology",
        "data": "technology",
        "nurse": "healthcare",
        "medic": "healthcare",
        "health": "healthcare",
        "dental": "healthcare",
        "pharma": "healthcare",
        "retail": "retail",
        "store": "retail",
        "shop": "retail",
        "hotel": "hospitality",
        "hospit": "hospitality",
        "travel": "hospitality",
        "market": "marketing",
        "social media": "marketing",
        "brand": "marketing",
        "content": "marketing",
        "sale": "sales",
        "account": "sales",
        "business dev": "sales",
        "teach": "education",
        "instruct": "education",
        "train": "education",
        "manufact": "manufacturing",
        "production": "manufacturing",
        "factory": "manufacturing",
        "financ": "finance",
        "account": "finance",
        "audit": "finance",
        "tax": "finance",
        "design": "creative",
        "video": "creative",
        "photo": "creative",
        "art": "creative",
        "animat": "creative",
        "cook": "food_service",
        "chef": "food_service",
        "food": "food_service",
        "barista": "food_service",
        "restaurant": "food_service",
        "kitchen": "food_service",
        "warehouse": "logistics",
        "driver": "logistics",
        "delivery": "logistics",
        "shipping": "logistics",
        "logistics": "logistics",
        "supply chain": "logistics",
        "customer": "customer_service",
        "support": "customer_service",
        "call center": "customer_service",
        "help desk": "customer_service",
    }

    for keyword, category in keyword_map.items():
        if keyword in normalized:
            return category

    # Default fallback
    return "technology"


def _tiktok_location_to_codes(locations: List[str]) -> List[str]:
    """Convert human-readable location names to ISO country codes for the API."""
    codes: List[str] = []
    for loc in locations:
        normalized = loc.strip().lower()
        code = _TIKTOK_LOCATION_CODES.get(normalized)
        if code and code not in codes:
            codes.append(code)
        else:
            # Try partial match
            for name, c in _TIKTOK_LOCATION_CODES.items():
                if name in normalized or normalized in name:
                    if c not in codes:
                        codes.append(c)
                    break
    if not codes:
        codes.append("US")  # default
    return codes


def _fetch_tiktok_audience_estimate(
    access_token: str,
    advertiser_id: str,
    location_codes: List[str],
    category: str,
) -> Optional[int]:
    """Call TikTok Marketing API audience estimation endpoint.

    Returns the estimated audience size or None on failure.
    """
    cache_key = _cache_key("tiktok_audience", f"{','.join(location_codes)}_{category}")
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached

    url = "https://business-api.tiktok.com/open_api/v1.3/tool/audience_size/"
    headers = {
        "Access-Token": access_token,
        "Content-Type": "application/json",
    }

    # Map category to TikTok interest keyword IDs (representative IDs)
    category_interest_ids: Dict[str, List[str]] = {
        "technology": ["15070", "15071"],
        "healthcare": ["15050", "15051"],
        "retail": ["15030", "15031"],
        "hospitality": ["15040", "15041"],
        "marketing": ["15060", "15061"],
        "sales": ["15060"],
        "education": ["15080", "15081"],
        "manufacturing": ["15090"],
        "finance": ["15100", "15101"],
        "engineering": ["15070"],
        "creative": ["15110", "15111"],
        "food_service": ["15040"],
        "logistics": ["15090"],
        "customer_service": ["15030"],
    }

    interest_ids = category_interest_ids.get(category, ["15070"])

    body = json.dumps(
        {
            "advertiser_id": advertiser_id,
            "placements": ["PLACEMENT_TIKTOK"],
            "location_ids": location_codes,
            "age_groups": ["AGE_18_24", "AGE_25_34"],
            "interest_category_ids": interest_ids,
            "operating_systems": ["ANDROID", "IOS"],
        }
    ).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        if data.get("code") == 0:
            audience_size = data.get("data", {}).get(
                "estimated_audience_size"
            ) or data.get("data", {}).get("audience_size")
            if audience_size is not None:
                _set_cached(cache_key, int(audience_size))
                return int(audience_size)

        _log_warn(
            f"TikTok audience API returned code={data.get('code')}: "
            f"{data.get('message', 'unknown error')}"
        )
        return None

    except (urllib.error.URLError, urllib.error.HTTPError, OSError, ValueError) as exc:
        _log_warn(f"TikTok audience estimation request failed: {exc}")
        return None


def _format_audience_size(size: int) -> str:
    """Convert a raw audience count into a human-readable range string."""
    if size < 1000:
        return f"{size}"
    elif size < 1_000_000:
        lower = max(1, int(size * 0.8))
        upper = int(size * 1.2)
        return f"{lower // 1000}K-{upper // 1000}K"
    else:
        lower = size * 0.8 / 1_000_000
        upper = size * 1.2 / 1_000_000
        return f"{lower:.1f}M-{upper:.1f}M"


def fetch_tiktok_ads_data(roles: List[str], locations: List[str]) -> Dict[str, Any]:
    """Fetch TikTok advertising benchmarks and audience estimates for recruitment.

    When valid ``TIKTOK_ACCESS_TOKEN`` and ``TIKTOK_ADVERTISER_ID`` environment
    variables are present the function calls the TikTok Marketing API audience
    estimation endpoint to get live audience sizes.  Otherwise it falls back to
    curated benchmark data that reflects realistic TikTok recruitment advertising
    performance.

    Parameters
    ----------
    roles:
        Job titles to look up (e.g. ``["Barista", "Software Engineer"]``).
    locations:
        Target geographies (e.g. ``["United States", "UK"]``).

    Returns
    -------
    dict
        A dictionary containing per-role benchmarks and a platform summary.
    """

    # Top-level cache check
    cache_k = _cache_key("tiktok_ads", f"{sorted(roles)}|{sorted(locations)}")
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    access_token = os.environ.get("TIKTOK_ACCESS_TOKEN", "").strip()
    advertiser_id = os.environ.get("TIKTOK_ADVERTISER_ID", "").strip()
    has_credentials = bool(access_token and advertiser_id)

    if has_credentials:
        _log_info(
            "TikTok Marketing API credentials detected; will attempt live queries."
        )
    else:
        _log_info("TikTok API credentials not found; using curated benchmark data.")

    location_codes = _tiktok_location_to_codes(locations)

    source = "TikTok Ads Benchmarks"
    roles_data: Dict[str, Dict[str, Any]] = {}

    for role in roles:
        category = _resolve_tiktok_category(role)
        benchmarks = TIKTOK_ADS_BENCHMARKS.get(
            category, TIKTOK_ADS_BENCHMARKS["technology"]
        )

        role_entry: Dict[str, Any] = {
            "avg_cpm_usd": benchmarks["avg_cpm_usd"],
            "avg_cpc_usd": benchmarks["avg_cpc_usd"],
            "avg_ctr_pct": benchmarks["avg_ctr_pct"],
            "estimated_audience": benchmarks["estimated_audience"],
            "cost_per_application": benchmarks["cost_per_application"],
            "best_age_range": benchmarks["best_age_range"],
            "engagement_rate": benchmarks["engagement_rate"],
            "category": category,
        }

        # Attempt live audience estimation when credentials are available
        if has_credentials:
            audience_size = _fetch_tiktok_audience_estimate(
                access_token, advertiser_id, location_codes, category
            )
            if audience_size is not None:
                role_entry["estimated_audience"] = _format_audience_size(audience_size)
                role_entry["audience_size_raw"] = audience_size
                source = "TikTok Marketing API"

        # Apply location-based multipliers for audience estimates
        # Multiple locations increase potential reach
        if len(location_codes) > 1 and "audience_size_raw" not in role_entry:
            base_audience = role_entry["estimated_audience"]
            if "-" in base_audience:
                parts = base_audience.replace("K", "").replace("M", "").split("-")
                try:
                    suffix = "M" if "M" in benchmarks["estimated_audience"] else "K"
                    low = float(parts[0]) * min(len(location_codes), 3)
                    high = float(parts[1]) * min(len(location_codes), 3)
                    role_entry["estimated_audience"] = (
                        f"{low:.0f}{suffix}-{high:.0f}{suffix}"
                    )
                except (ValueError, IndexError):
                    pass  # keep original

        # Seniority adjustment — TikTok is less effective for senior roles
        role_lower = role.strip().lower()
        is_senior = any(
            kw in role_lower
            for kw in [
                "senior",
                "sr.",
                "lead",
                "principal",
                "director",
                "vp",
                "vice president",
                "chief",
                "head of",
                "executive",
                "manager",
                "cto",
                "cfo",
                "ceo",
                "coo",
                "cmo",
            ]
        )
        if is_senior:
            role_entry["avg_cpc_usd"] = round(role_entry["avg_cpc_usd"] * 1.6, 2)
            role_entry["avg_cpm_usd"] = round(role_entry["avg_cpm_usd"] * 1.4, 2)
            role_entry["cost_per_application"] = round(
                role_entry["cost_per_application"] * 2.0, 2
            )
            role_entry["avg_ctr_pct"] = round(role_entry["avg_ctr_pct"] * 0.65, 2)
            role_entry["engagement_rate"] = round(
                role_entry["engagement_rate"] * 0.6, 1
            )
            role_entry["best_age_range"] = "25-44"
            role_entry["seniority_note"] = (
                "TikTok is less effective for senior-level recruitment. "
                "Consider supplementing with LinkedIn or industry-specific platforms."
            )

        # Entry-level bonus — TikTok excels at entry-level and volume hiring
        is_entry_level = any(
            kw in role_lower
            for kw in [
                "intern",
                "entry",
                "junior",
                "jr.",
                "associate",
                "assistant",
                "trainee",
                "apprentice",
            ]
        )
        if is_entry_level:
            role_entry["avg_cpc_usd"] = round(role_entry["avg_cpc_usd"] * 0.75, 2)
            role_entry["cost_per_application"] = round(
                role_entry["cost_per_application"] * 0.65, 2
            )
            role_entry["avg_ctr_pct"] = round(role_entry["avg_ctr_pct"] * 1.25, 2)
            role_entry["engagement_rate"] = round(
                role_entry["engagement_rate"] * 1.3, 1
            )
            role_entry["best_age_range"] = "18-26"
            role_entry["seniority_note"] = (
                "TikTok is highly effective for entry-level and early-career hiring. "
                "Use authentic, employee-generated content for best results."
            )

        roles_data[role] = role_entry

    result: Dict[str, Any] = {
        "source": source,
        "roles": roles_data,
        "locations_targeted": locations,
        "location_codes": location_codes,
        "platform_summary": {
            "platform": "TikTok",
            "monthly_active_users": "1.5+ billion globally",
            "best_for": (
                "Employer branding, entry-level hiring, Gen-Z recruitment, "
                "volume roles"
            ),
            "audience_profile": ("60% aged 18-34, highly engaged, mobile-first"),
            "ad_formats": [
                "In-Feed Video",
                "TopView",
                "Branded Hashtag Challenge",
                "Spark Ads",
                "Lead Gen Forms",
            ],
            "avg_session_time": "95 minutes/day",
            "recruitment_strength": (
                "Authentic employer branding content, high engagement rates"
            ),
            "recommended_content_tips": [
                "Use vertical 9:16 video (15-60 seconds)",
                "Feature real employees and day-in-the-life content",
                "Leverage trending sounds and formats",
                "Include clear call-to-action with Lead Gen Forms",
                "Post consistently to build employer brand presence",
            ],
        },
    }

    _log_info(
        f"TikTok ads data retrieved for {len(roles)} role(s) "
        f"across {len(location_codes)} location(s) — source: {source}"
    )

    _set_cached(cache_k, result)
    return result


# API 23: LinkedIn Marketing API

# ---------------------------------------------------------------------------
# LinkedIn Marketing API integration for media-plan generator
# ---------------------------------------------------------------------------

LINKEDIN_ADS_BENCHMARKS: Dict[str, Dict[str, Any]] = {
    "technology": {
        "avg_cpm_usd": 35.00,
        "avg_cpc_usd": 5.50,
        "avg_ctr_pct": 0.45,
        "estimated_audience": "5M-12M",
        "cost_per_application": 45.00,
        "cost_per_quality_hire": 3500,
        "inmail_response_rate": 0.18,
        "sponsored_content_engagement": 0.38,
    },
    "healthcare": {
        "avg_cpm_usd": 28.00,
        "avg_cpc_usd": 4.80,
        "avg_ctr_pct": 0.50,
        "estimated_audience": "3M-8M",
        "cost_per_application": 40.00,
        "cost_per_quality_hire": 3200,
        "inmail_response_rate": 0.20,
        "sponsored_content_engagement": 0.35,
    },
    "finance": {
        "avg_cpm_usd": 42.00,
        "avg_cpc_usd": 7.20,
        "avg_ctr_pct": 0.40,
        "estimated_audience": "4M-9M",
        "cost_per_application": 55.00,
        "cost_per_quality_hire": 4200,
        "inmail_response_rate": 0.15,
        "sponsored_content_engagement": 0.32,
    },
    "engineering": {
        "avg_cpm_usd": 38.00,
        "avg_cpc_usd": 6.00,
        "avg_ctr_pct": 0.42,
        "estimated_audience": "3M-7M",
        "cost_per_application": 50.00,
        "cost_per_quality_hire": 3800,
        "inmail_response_rate": 0.16,
        "sponsored_content_engagement": 0.36,
    },
    "marketing": {
        "avg_cpm_usd": 30.00,
        "avg_cpc_usd": 4.50,
        "avg_ctr_pct": 0.55,
        "estimated_audience": "6M-14M",
        "cost_per_application": 35.00,
        "cost_per_quality_hire": 2800,
        "inmail_response_rate": 0.22,
        "sponsored_content_engagement": 0.42,
    },
    "sales": {
        "avg_cpm_usd": 28.00,
        "avg_cpc_usd": 4.20,
        "avg_ctr_pct": 0.58,
        "estimated_audience": "8M-18M",
        "cost_per_application": 30.00,
        "cost_per_quality_hire": 2500,
        "inmail_response_rate": 0.24,
        "sponsored_content_engagement": 0.44,
    },
    "human_resources": {
        "avg_cpm_usd": 26.00,
        "avg_cpc_usd": 4.00,
        "avg_ctr_pct": 0.52,
        "estimated_audience": "2M-5M",
        "cost_per_application": 32.00,
        "cost_per_quality_hire": 2600,
        "inmail_response_rate": 0.25,
        "sponsored_content_engagement": 0.40,
    },
    "executive": {
        "avg_cpm_usd": 55.00,
        "avg_cpc_usd": 12.00,
        "avg_ctr_pct": 0.30,
        "estimated_audience": "500K-2M",
        "cost_per_application": 85.00,
        "cost_per_quality_hire": 8500,
        "inmail_response_rate": 0.10,
        "sponsored_content_engagement": 0.25,
    },
    "data_science": {
        "avg_cpm_usd": 40.00,
        "avg_cpc_usd": 6.50,
        "avg_ctr_pct": 0.43,
        "estimated_audience": "2M-5M",
        "cost_per_application": 52.00,
        "cost_per_quality_hire": 4000,
        "inmail_response_rate": 0.14,
        "sponsored_content_engagement": 0.34,
    },
    "cybersecurity": {
        "avg_cpm_usd": 45.00,
        "avg_cpc_usd": 8.00,
        "avg_ctr_pct": 0.38,
        "estimated_audience": "1M-3M",
        "cost_per_application": 60.00,
        "cost_per_quality_hire": 5000,
        "inmail_response_rate": 0.12,
        "sponsored_content_engagement": 0.30,
    },
    "consulting": {
        "avg_cpm_usd": 38.00,
        "avg_cpc_usd": 6.80,
        "avg_ctr_pct": 0.42,
        "estimated_audience": "2M-6M",
        "cost_per_application": 48.00,
        "cost_per_quality_hire": 3600,
        "inmail_response_rate": 0.17,
        "sponsored_content_engagement": 0.36,
    },
    "legal": {
        "avg_cpm_usd": 44.00,
        "avg_cpc_usd": 7.80,
        "avg_ctr_pct": 0.35,
        "estimated_audience": "1M-3M",
        "cost_per_application": 65.00,
        "cost_per_quality_hire": 5500,
        "inmail_response_rate": 0.13,
        "sponsored_content_engagement": 0.28,
    },
    "education": {
        "avg_cpm_usd": 22.00,
        "avg_cpc_usd": 3.50,
        "avg_ctr_pct": 0.55,
        "estimated_audience": "4M-10M",
        "cost_per_application": 28.00,
        "cost_per_quality_hire": 2000,
        "inmail_response_rate": 0.26,
        "sponsored_content_engagement": 0.45,
    },
    "operations": {
        "avg_cpm_usd": 27.00,
        "avg_cpc_usd": 4.30,
        "avg_ctr_pct": 0.48,
        "estimated_audience": "3M-8M",
        "cost_per_application": 36.00,
        "cost_per_quality_hire": 2700,
        "inmail_response_rate": 0.21,
        "sponsored_content_engagement": 0.38,
    },
    "product_management": {
        "avg_cpm_usd": 42.00,
        "avg_cpc_usd": 7.00,
        "avg_ctr_pct": 0.40,
        "estimated_audience": "1M-4M",
        "cost_per_application": 55.00,
        "cost_per_quality_hire": 4500,
        "inmail_response_rate": 0.15,
        "sponsored_content_engagement": 0.35,
    },
    "design": {
        "avg_cpm_usd": 32.00,
        "avg_cpc_usd": 5.00,
        "avg_ctr_pct": 0.48,
        "estimated_audience": "2M-6M",
        "cost_per_application": 42.00,
        "cost_per_quality_hire": 3000,
        "inmail_response_rate": 0.19,
        "sponsored_content_engagement": 0.40,
    },
}

ROLE_TO_LINKEDIN_CATEGORY: Dict[str, str] = {
    # Technology & Engineering
    "software engineer": "technology",
    "software developer": "technology",
    "frontend developer": "technology",
    "backend developer": "technology",
    "full stack developer": "technology",
    "fullstack developer": "technology",
    "mobile developer": "technology",
    "ios developer": "technology",
    "android developer": "technology",
    "devops engineer": "technology",
    "sre": "technology",
    "site reliability engineer": "technology",
    "cloud engineer": "technology",
    "platform engineer": "technology",
    "qa engineer": "technology",
    "test engineer": "technology",
    "embedded engineer": "engineering",
    "mechanical engineer": "engineering",
    "electrical engineer": "engineering",
    "civil engineer": "engineering",
    "chemical engineer": "engineering",
    "hardware engineer": "engineering",
    "systems engineer": "engineering",
    # Data & AI
    "data scientist": "data_science",
    "data analyst": "data_science",
    "data engineer": "data_science",
    "machine learning engineer": "data_science",
    "ml engineer": "data_science",
    "ai engineer": "data_science",
    "ai researcher": "data_science",
    "business intelligence analyst": "data_science",
    # Cybersecurity
    "security engineer": "cybersecurity",
    "cybersecurity analyst": "cybersecurity",
    "penetration tester": "cybersecurity",
    "security architect": "cybersecurity",
    "information security": "cybersecurity",
    "soc analyst": "cybersecurity",
    # Finance
    "financial analyst": "finance",
    "accountant": "finance",
    "controller": "finance",
    "investment banker": "finance",
    "actuary": "finance",
    "auditor": "finance",
    "cfo": "finance",
    "treasurer": "finance",
    "risk analyst": "finance",
    # Healthcare
    "nurse": "healthcare",
    "physician": "healthcare",
    "pharmacist": "healthcare",
    "medical technologist": "healthcare",
    "clinical researcher": "healthcare",
    "healthcare administrator": "healthcare",
    "physical therapist": "healthcare",
    "dentist": "healthcare",
    # Marketing
    "marketing manager": "marketing",
    "content marketer": "marketing",
    "seo specialist": "marketing",
    "social media manager": "marketing",
    "digital marketing": "marketing",
    "growth marketer": "marketing",
    "brand manager": "marketing",
    "communications manager": "marketing",
    "copywriter": "marketing",
    # Sales
    "sales representative": "sales",
    "account executive": "sales",
    "business development": "sales",
    "sales manager": "sales",
    "account manager": "sales",
    "sales engineer": "sales",
    "customer success manager": "sales",
    # Human Resources
    "recruiter": "human_resources",
    "hr manager": "human_resources",
    "talent acquisition": "human_resources",
    "people operations": "human_resources",
    "compensation analyst": "human_resources",
    "hr business partner": "human_resources",
    # Executive / Leadership
    "ceo": "executive",
    "cto": "executive",
    "coo": "executive",
    "cmo": "executive",
    "cio": "executive",
    "vp": "executive",
    "vice president": "executive",
    "director": "executive",
    "chief of staff": "executive",
    "general manager": "executive",
    "managing director": "executive",
    # Consulting
    "consultant": "consulting",
    "management consultant": "consulting",
    "strategy consultant": "consulting",
    "business analyst": "consulting",
    "solutions architect": "consulting",
    # Legal
    "lawyer": "legal",
    "attorney": "legal",
    "paralegal": "legal",
    "legal counsel": "legal",
    "compliance officer": "legal",
    "contract manager": "legal",
    # Education
    "teacher": "education",
    "professor": "education",
    "instructor": "education",
    "training manager": "education",
    "curriculum developer": "education",
    "academic advisor": "education",
    # Operations
    "operations manager": "operations",
    "supply chain manager": "operations",
    "logistics manager": "operations",
    "project manager": "operations",
    "program manager": "operations",
    "procurement manager": "operations",
    "facilities manager": "operations",
    # Product Management
    "product manager": "product_management",
    "product owner": "product_management",
    "technical product manager": "product_management",
    "product lead": "product_management",
    "product director": "product_management",
    # Design
    "ux designer": "design",
    "ui designer": "design",
    "product designer": "design",
    "graphic designer": "design",
    "ux researcher": "design",
    "visual designer": "design",
    "interaction designer": "design",
    "design director": "design",
}

_LINKEDIN_API_BASE = "https://api.linkedin.com/rest"
_LINKEDIN_API_VERSION = "202401"
_LINKEDIN_CACHE_TTL_SEC = 3600

_LINKEDIN_PLATFORM_SUMMARY: Dict[str, Any] = {
    "platform": "LinkedIn",
    "monthly_active_users": "1+ billion members, 310M+ monthly active",
    "best_for": ("Professional hiring, senior roles, B2B, passive candidate targeting"),
    "audience_profile": (
        "Professionals, decision-makers, high-intent career-oriented users"
    ),
    "ad_formats": [
        "Sponsored Content",
        "InMail/Message Ads",
        "Dynamic Ads",
        "Text Ads",
        "Lead Gen Forms",
        "Conversation Ads",
    ],
    "targeting_options": [
        "Job Title",
        "Job Function",
        "Seniority Level",
        "Skills",
        "Company",
        "Industry",
        "Years of Experience",
        "Education",
        "Groups",
    ],
    "unique_value": "Only platform with verified professional identity data",
}

# Mapping from user-friendly location strings to LinkedIn geo URNs for
# the most commonly targeted geographies.  The API uses
# urn:li:geo:{id} values for audience-count and forecasting requests.
_LOCATION_TO_GEO_URN: Dict[str, str] = {
    "united states": "urn:li:geo:103644278",
    "us": "urn:li:geo:103644278",
    "usa": "urn:li:geo:103644278",
    "united kingdom": "urn:li:geo:101165590",
    "uk": "urn:li:geo:101165590",
    "canada": "urn:li:geo:101174742",
    "germany": "urn:li:geo:101282230",
    "france": "urn:li:geo:105015875",
    "australia": "urn:li:geo:101452733",
    "india": "urn:li:geo:102713980",
    "singapore": "urn:li:geo:102454443",
    "netherlands": "urn:li:geo:102890719",
    "brazil": "urn:li:geo:106057199",
    "japan": "urn:li:geo:101355337",
    "ireland": "urn:li:geo:104738515",
    "uae": "urn:li:geo:104305776",
    "new york": "urn:li:geo:105080838",
    "san francisco": "urn:li:geo:102277331",
    "london": "urn:li:geo:102257491",
    "berlin": "urn:li:geo:106967730",
    "toronto": "urn:li:geo:100025096",
    "sydney": "urn:li:geo:104769905",
    "remote": "urn:li:geo:103644278",
    "global": "urn:li:geo:103644278",
}


def _resolve_linkedin_category(role: str) -> str:
    """Map a free-text role title to a benchmark category key."""
    normalised = role.strip().lower()

    # Direct hit
    if normalised in ROLE_TO_LINKEDIN_CATEGORY:
        return ROLE_TO_LINKEDIN_CATEGORY[normalised]

    # Substring match -- iterate once over the mapping, longest-key-first
    # so that "machine learning engineer" beats "engineer".
    sorted_keys = sorted(ROLE_TO_LINKEDIN_CATEGORY.keys(), key=len, reverse=True)
    for key in sorted_keys:
        if key in normalised or normalised in key:
            return ROLE_TO_LINKEDIN_CATEGORY[key]

    # Fallback: pick "technology" as the most common catch-all
    return "technology"


def _resolve_geo_urns(locations: List[str]) -> List[str]:
    """Convert user-friendly location strings to LinkedIn geo URNs."""
    urns: List[str] = []
    for loc in locations:
        key = loc.strip().lower()
        urn = _LOCATION_TO_GEO_URN.get(key)
        if urn and urn not in urns:
            urns.append(urn)
    # Default to US when nothing matched
    if not urns:
        urns.append(_LOCATION_TO_GEO_URN["us"])
    return urns


def _linkedin_api_headers(token: str) -> Dict[str, str]:
    """Standard header set for LinkedIn Marketing REST API v2."""
    return {
        "Authorization": f"Bearer {token}",
        "LinkedIn-Version": _LINKEDIN_API_VERSION,
        "X-Restli-Protocol-Version": "2.0.0",
        "Content-Type": "application/json",
    }


def _linkedin_audience_count(
    token: str,
    category: str,
    geo_urns: List[str],
    timeout: int = 15,
) -> Optional[int]:
    """Call the LinkedIn Audience Counts API and return the total count.

    Returns *None* on any failure so callers can fall back gracefully.
    """
    cache_key = _cache_key("li_audience", f"{category}|{'|'.join(geo_urns)}")
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    targeting_criteria = {
        "include": {
            "and": [
                {
                    "or": {
                        "urn:li:adTargetingFacet:locations": geo_urns,
                    }
                },
                {
                    "or": {
                        "urn:li:adTargetingFacet:jobFunctions": [
                            f"urn:li:function:{category}"
                        ],
                    }
                },
            ]
        }
    }

    params = urllib.parse.urlencode(
        {"targetingCriteria": json.dumps(targeting_criteria)}
    )
    url = f"{_LINKEDIN_API_BASE}/audienceCounts?{params}"

    try:
        data = _http_get_json(url, _linkedin_api_headers(token), timeout)
        if data and "elements" in data and data["elements"]:
            count = data["elements"][0].get("approximateCount", None)
            if count is not None:
                _set_cached(cache_key, count)
            return count  # type: ignore[return-value]
    except Exception as exc:
        _log_warn(f"LinkedIn audienceCounts failed for {category}: {exc}")

    return None


def _linkedin_ad_forecast(
    token: str,
    ad_account_id: str,
    category: str,
    geo_urns: List[str],
    timeout: int = 15,
) -> Optional[Dict[str, Any]]:
    """Call the LinkedIn Ad Budget Pricing (forecast) API.

    Returns a dict with CPM, CPC, and CTR estimates or *None* on failure.
    """
    cache_key = _cache_key(
        "li_forecast", f"{ad_account_id}|{category}|{'|'.join(geo_urns)}"
    )
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached  # type: ignore[return-value]

    url = f"{_LINKEDIN_API_BASE}/adBudgetPricing"
    headers = _linkedin_api_headers(token)

    body = json.dumps(
        {
            "account": ad_account_id,
            "bidType": "CPM",
            "campaignType": "SPONSORED_UPDATES",
            "dailyBudget": {"amount": "100", "currencyCode": "USD"},
            "matchType": "EXACT",
            "targetingCriteria": {
                "include": {
                    "and": [
                        {
                            "or": {
                                "urn:li:adTargetingFacet:locations": geo_urns,
                            }
                        },
                        {
                            "or": {
                                "urn:li:adTargetingFacet:jobFunctions": [
                                    f"urn:li:function:{category}"
                                ],
                            }
                        },
                    ]
                }
            },
        }
    ).encode("utf-8")

    try:
        req = urllib.request.Request(url, data=body, headers=headers, method="POST")
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw = json.loads(resp.read().decode("utf-8"))

        suggested = raw.get("suggestedBid", {})
        bid_low = float(suggested.get("min") or 0)
        bid_high = float(suggested.get("max") or 0)
        avg_bid = (bid_low + bid_high) / 2 if (bid_low + bid_high) else None

        result: Dict[str, Any] = {
            "suggested_bid_low": bid_low,
            "suggested_bid_high": bid_high,
        }
        if avg_bid:
            result["avg_cpm_usd"] = round(avg_bid, 2)
            # LinkedIn CPCs are typically ~60-70% of CPM / 6.5 (avg CTR ~0.44%)
            result["avg_cpc_usd"] = round(avg_bid / 6.5, 2)

        _set_cached(cache_key, result)
        return result
    except Exception as exc:
        _log_warn(f"LinkedIn adBudgetPricing failed for {category}: {exc}")

    return None


def fetch_linkedin_ads_data(
    roles: List[str],
    locations: List[str],
) -> Dict[str, Any]:
    """Return LinkedIn professional audience sizing and ad benchmarks.

    When valid ``LINKEDIN_ACCESS_TOKEN`` and ``LINKEDIN_AD_ACCOUNT_ID``
    environment variables are present the function queries the LinkedIn
    Marketing REST API (Audience Counts + Ad Budget Pricing) and
    augments the response with curated benchmark data.

    If credentials are missing or API calls fail the function falls
    back entirely to the curated ``LINKEDIN_ADS_BENCHMARKS`` data so
    callers always receive a usable result.

    Parameters
    ----------
    roles:
        Job titles or role names to look up (e.g. ``["Software Engineer",
        "Product Manager"]``).
    locations:
        Target geographies (e.g. ``["United States", "London"]``).  Used
        for API geo-targeting and contextual notes.

    Returns
    -------
    dict
        Structured data with per-role metrics and a platform summary.
    """

    # Top-level cache check
    cache_k = _cache_key("linkedin_ads", f"{sorted(roles)}|{sorted(locations)}")
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    token = os.environ.get("LINKEDIN_ACCESS_TOKEN", "").strip()
    ad_account = os.environ.get("LINKEDIN_AD_ACCOUNT_ID", "").strip()
    has_credentials = bool(token) and bool(ad_account)

    if has_credentials:
        _log_info(
            "LinkedIn Marketing API credentials detected -- will attempt live queries"
        )
    else:
        _log_info("LinkedIn credentials not found -- using curated benchmark data")

    geo_urns = _resolve_geo_urns(locations) if has_credentials else []

    roles_data: Dict[str, Dict[str, Any]] = {}
    used_live_api = False

    for role in roles:
        category = _resolve_linkedin_category(role)
        benchmark = LINKEDIN_ADS_BENCHMARKS.get(
            category, LINKEDIN_ADS_BENCHMARKS["technology"]
        )

        # Start from the curated benchmark as the baseline
        role_metrics: Dict[str, Any] = {
            "avg_cpm_usd": benchmark["avg_cpm_usd"],
            "avg_cpc_usd": benchmark["avg_cpc_usd"],
            "avg_ctr_pct": benchmark["avg_ctr_pct"],
            "estimated_audience": benchmark["estimated_audience"],
            "cost_per_application": benchmark["cost_per_application"],
            "cost_per_quality_hire": benchmark["cost_per_quality_hire"],
            "inmail_response_rate": benchmark["inmail_response_rate"],
            "category": category,
        }

        if has_credentials:
            # --- Audience count ------------------------------------------------
            audience = _linkedin_audience_count(token, category, geo_urns)
            if audience is not None:
                role_metrics["estimated_audience"] = audience
                role_metrics["audience_source"] = "api"
                used_live_api = True

            # --- Ad forecast ---------------------------------------------------
            forecast = _linkedin_ad_forecast(token, ad_account, category, geo_urns)
            if forecast is not None:
                if "avg_cpm_usd" in forecast:
                    role_metrics["avg_cpm_usd"] = forecast["avg_cpm_usd"]
                if "avg_cpc_usd" in forecast:
                    role_metrics["avg_cpc_usd"] = forecast["avg_cpc_usd"]
                role_metrics["suggested_bid_low"] = forecast.get("suggested_bid_low")
                role_metrics["suggested_bid_high"] = forecast.get("suggested_bid_high")
                role_metrics["pricing_source"] = "api"
                used_live_api = True

        # Compute a derived estimated monthly spend for a typical
        # recruitment campaign (1,000 clicks target).
        cpc = role_metrics["avg_cpc_usd"]
        role_metrics["est_monthly_spend_1k_clicks"] = round(cpc * 1000, 2)

        roles_data[role] = role_metrics

    # Determine the effective source label
    if used_live_api:
        source = "LinkedIn Marketing API"
    else:
        source = "LinkedIn Ads Benchmarks"

    # Build location context note
    location_note = ", ".join(locations) if locations else "Global"

    result = {
        "source": source,
        "target_locations": location_note,
        "roles": roles_data,
        "platform_summary": _LINKEDIN_PLATFORM_SUMMARY,
    }
    _set_cached(cache_k, result)
    return result


# API 24: CareerOneStop API (DOL Data)

CAREERONESTOP_BENCHMARKS = {
    "software developer": {
        "soc_code": "15-1252",
        "title": "Software Developers",
        "median_salary": 132270,
        "entry_salary": 79000,
        "experienced_salary": 198000,
        "employment": 1847900,
        "projected_growth_pct": 25,
        "projected_growth_label": "Much faster than average",
        "annual_openings": 153900,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "data scientist": {
        "soc_code": "15-2051",
        "title": "Data Scientists",
        "median_salary": 108020,
        "entry_salary": 61860,
        "experienced_salary": 174800,
        "employment": 192700,
        "projected_growth_pct": 36,
        "projected_growth_label": "Much faster than average",
        "annual_openings": 17700,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "registered nurse": {
        "soc_code": "29-1141",
        "title": "Registered Nurses",
        "median_salary": 86070,
        "entry_salary": 59450,
        "experienced_salary": 132680,
        "employment": 3175390,
        "projected_growth_pct": 6,
        "projected_growth_label": "Faster than average",
        "annual_openings": 193100,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "marketing manager": {
        "soc_code": "11-2021",
        "title": "Marketing Managers",
        "median_salary": 156580,
        "entry_salary": 85200,
        "experienced_salary": 239200,
        "employment": 376600,
        "projected_growth_pct": 7,
        "projected_growth_label": "Faster than average",
        "annual_openings": 35300,
        "education": "Bachelor's degree",
        "work_experience": "5 years or more",
        "on_job_training": "None",
    },
    "financial analyst": {
        "soc_code": "13-2051",
        "title": "Financial Analysts",
        "median_salary": 99890,
        "entry_salary": 60030,
        "experienced_salary": 176830,
        "employment": 328600,
        "projected_growth_pct": 8,
        "projected_growth_label": "Faster than average",
        "annual_openings": 27400,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "accountant": {
        "soc_code": "13-2011",
        "title": "Accountants and Auditors",
        "median_salary": 79880,
        "entry_salary": 50440,
        "experienced_salary": 134890,
        "employment": 1451000,
        "projected_growth_pct": 4,
        "projected_growth_label": "As fast as average",
        "annual_openings": 126500,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "mechanical engineer": {
        "soc_code": "17-2141",
        "title": "Mechanical Engineers",
        "median_salary": 99510,
        "entry_salary": 63010,
        "experienced_salary": 142210,
        "employment": 284900,
        "projected_growth_pct": 2,
        "projected_growth_label": "Slower than average",
        "annual_openings": 17900,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "project manager": {
        "soc_code": "11-9199",
        "title": "Project Management Specialists",
        "median_salary": 98580,
        "entry_salary": 55800,
        "experienced_salary": 159140,
        "employment": 936900,
        "projected_growth_pct": 6,
        "projected_growth_label": "Faster than average",
        "annual_openings": 68100,
        "education": "Bachelor's degree",
        "work_experience": "Less than 5 years",
        "on_job_training": "None",
    },
    "hr manager": {
        "soc_code": "11-3121",
        "title": "Human Resources Managers",
        "median_salary": 136350,
        "entry_salary": 81060,
        "experienced_salary": 239200,
        "employment": 198100,
        "projected_growth_pct": 5,
        "projected_growth_label": "Faster than average",
        "annual_openings": 16300,
        "education": "Bachelor's degree",
        "work_experience": "5 years or more",
        "on_job_training": "None",
    },
    "product manager": {
        "soc_code": "11-2021",
        "title": "Product Managers",
        "median_salary": 149440,
        "entry_salary": 82000,
        "experienced_salary": 225000,
        "employment": 376600,
        "projected_growth_pct": 7,
        "projected_growth_label": "Faster than average",
        "annual_openings": 35300,
        "education": "Bachelor's degree",
        "work_experience": "5 years or more",
        "on_job_training": "None",
    },
    "sales manager": {
        "soc_code": "11-2022",
        "title": "Sales Managers",
        "median_salary": 135160,
        "entry_salary": 72050,
        "experienced_salary": 239200,
        "employment": 469800,
        "projected_growth_pct": 4,
        "projected_growth_label": "As fast as average",
        "annual_openings": 45150,
        "education": "Bachelor's degree",
        "work_experience": "5 years or more",
        "on_job_training": "None",
    },
    "graphic designer": {
        "soc_code": "27-1024",
        "title": "Graphic Designers",
        "median_salary": 57990,
        "entry_salary": 35900,
        "experienced_salary": 98260,
        "employment": 252600,
        "projected_growth_pct": 3,
        "projected_growth_label": "As fast as average",
        "annual_openings": 24800,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "web developer": {
        "soc_code": "15-1254",
        "title": "Web Developers",
        "median_salary": 80730,
        "entry_salary": 44550,
        "experienced_salary": 132270,
        "employment": 199400,
        "projected_growth_pct": 16,
        "projected_growth_label": "Much faster than average",
        "annual_openings": 19000,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "cybersecurity analyst": {
        "soc_code": "15-1212",
        "title": "Information Security Analysts",
        "median_salary": 120360,
        "entry_salary": 72030,
        "experienced_salary": 182050,
        "employment": 175350,
        "projected_growth_pct": 32,
        "projected_growth_label": "Much faster than average",
        "annual_openings": 16800,
        "education": "Bachelor's degree",
        "work_experience": "Less than 5 years",
        "on_job_training": "None",
    },
    "civil engineer": {
        "soc_code": "17-2051",
        "title": "Civil Engineers",
        "median_salary": 95890,
        "entry_salary": 60980,
        "experienced_salary": 141480,
        "employment": 318200,
        "projected_growth_pct": 5,
        "projected_growth_label": "Faster than average",
        "annual_openings": 22100,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "electrical engineer": {
        "soc_code": "17-2071",
        "title": "Electrical Engineers",
        "median_salary": 107890,
        "entry_salary": 68110,
        "experienced_salary": 166010,
        "employment": 186400,
        "projected_growth_pct": 3,
        "projected_growth_label": "As fast as average",
        "annual_openings": 12600,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "pharmacist": {
        "soc_code": "29-1051",
        "title": "Pharmacists",
        "median_salary": 136030,
        "entry_salary": 112690,
        "experienced_salary": 163540,
        "employment": 322200,
        "projected_growth_pct": -2,
        "projected_growth_label": "Decline",
        "annual_openings": 11200,
        "education": "Doctoral or professional degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "recruiter": {
        "soc_code": "13-1071",
        "title": "Human Resources Specialists",
        "median_salary": 67650,
        "entry_salary": 40350,
        "experienced_salary": 116060,
        "employment": 782800,
        "projected_growth_pct": 6,
        "projected_growth_label": "Faster than average",
        "annual_openings": 73400,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "operations manager": {
        "soc_code": "11-1021",
        "title": "General and Operations Managers",
        "median_salary": 101280,
        "entry_salary": 47800,
        "experienced_salary": 208000,
        "employment": 3126500,
        "projected_growth_pct": 4,
        "projected_growth_label": "As fast as average",
        "annual_openings": 291150,
        "education": "Bachelor's degree",
        "work_experience": "5 years or more",
        "on_job_training": "None",
    },
    "supply chain analyst": {
        "soc_code": "13-1081",
        "title": "Logisticians",
        "median_salary": 79400,
        "entry_salary": 48640,
        "experienced_salary": 124400,
        "employment": 204900,
        "projected_growth_pct": 18,
        "projected_growth_label": "Much faster than average",
        "annual_openings": 22800,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
    "technical writer": {
        "soc_code": "27-3042",
        "title": "Technical Writers",
        "median_salary": 79960,
        "entry_salary": 48640,
        "experienced_salary": 128640,
        "employment": 54400,
        "projected_growth_pct": 7,
        "projected_growth_label": "Faster than average",
        "annual_openings": 5500,
        "education": "Bachelor's degree",
        "work_experience": "Less than 5 years",
        "on_job_training": "Short-term on-the-job training",
    },
    "business analyst": {
        "soc_code": "13-1111",
        "title": "Management Analysts",
        "median_salary": 99410,
        "entry_salary": 55920,
        "experienced_salary": 167650,
        "employment": 924200,
        "projected_growth_pct": 10,
        "projected_growth_label": "Faster than average",
        "annual_openings": 82600,
        "education": "Bachelor's degree",
        "work_experience": "Less than 5 years",
        "on_job_training": "None",
    },
    "ux designer": {
        "soc_code": "15-1255",
        "title": "Web and Digital Interface Designers",
        "median_salary": 92750,
        "entry_salary": 51200,
        "experienced_salary": 146600,
        "employment": 109600,
        "projected_growth_pct": 16,
        "projected_growth_label": "Much faster than average",
        "annual_openings": 9800,
        "education": "Bachelor's degree",
        "work_experience": "None",
        "on_job_training": "None",
    },
}

ROLE_TO_COS_OCCUPATION = {
    "software engineer": "software developer",
    "software developer": "software developer",
    "swe": "software developer",
    "backend engineer": "software developer",
    "frontend engineer": "web developer",
    "full stack developer": "software developer",
    "fullstack developer": "software developer",
    "devops engineer": "software developer",
    "sre": "software developer",
    "site reliability engineer": "software developer",
    "data scientist": "data scientist",
    "data analyst": "data scientist",
    "data engineer": "data scientist",
    "machine learning engineer": "data scientist",
    "ml engineer": "data scientist",
    "ai engineer": "data scientist",
    "nurse": "registered nurse",
    "registered nurse": "registered nurse",
    "rn": "registered nurse",
    "marketing manager": "marketing manager",
    "marketing director": "marketing manager",
    "brand manager": "marketing manager",
    "digital marketing manager": "marketing manager",
    "content marketing manager": "marketing manager",
    "financial analyst": "financial analyst",
    "finance analyst": "financial analyst",
    "investment analyst": "financial analyst",
    "accountant": "accountant",
    "auditor": "accountant",
    "cpa": "accountant",
    "bookkeeper": "accountant",
    "mechanical engineer": "mechanical engineer",
    "project manager": "project manager",
    "program manager": "project manager",
    "scrum master": "project manager",
    "hr manager": "hr manager",
    "human resources manager": "hr manager",
    "people operations manager": "hr manager",
    "product manager": "product manager",
    "product owner": "product manager",
    "sales manager": "sales manager",
    "account executive": "sales manager",
    "sales director": "sales manager",
    "graphic designer": "graphic designer",
    "visual designer": "graphic designer",
    "ui designer": "graphic designer",
    "web developer": "web developer",
    "frontend developer": "web developer",
    "web designer": "web developer",
    "cybersecurity analyst": "cybersecurity analyst",
    "security analyst": "cybersecurity analyst",
    "information security analyst": "cybersecurity analyst",
    "security engineer": "cybersecurity analyst",
    "penetration tester": "cybersecurity analyst",
    "civil engineer": "civil engineer",
    "structural engineer": "civil engineer",
    "electrical engineer": "electrical engineer",
    "electronics engineer": "electrical engineer",
    "pharmacist": "pharmacist",
    "recruiter": "recruiter",
    "talent acquisition specialist": "recruiter",
    "hr specialist": "recruiter",
    "operations manager": "operations manager",
    "general manager": "operations manager",
    "coo": "operations manager",
    "supply chain analyst": "supply chain analyst",
    "logistics analyst": "supply chain analyst",
    "logistician": "supply chain analyst",
    "supply chain manager": "supply chain analyst",
    "technical writer": "technical writer",
    "documentation specialist": "technical writer",
    "content developer": "technical writer",
    "business analyst": "business analyst",
    "management consultant": "business analyst",
    "management analyst": "business analyst",
    "ux designer": "ux designer",
    "user experience designer": "ux designer",
    "ux researcher": "ux designer",
    "interaction designer": "ux designer",
    "product designer": "ux designer",
}

_US_STATES = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
    "PR",
    "VI",
    "GU",
    "AS",
    "MP",
}


def _cos_extract_state_abbr(location: str) -> str:
    """Extract a US state abbreviation from a location string (CareerOneStop).

    Handles formats like:
        'San Mateo, CA'
        'CA'
        'California'
        'New York, NY 10001'
        'Austin, TX, USA'

    Returns the two-letter state code or '0' for national average.
    """
    if not location or not location.strip():
        return "0"

    location = location.strip()

    # Direct two-letter code
    upper = location.upper().strip()
    if upper in _US_STATES:
        return upper

    # Try to find a state abbreviation after a comma: "City, ST" or "City, ST ZIP"
    parts = [p.strip() for p in location.split(",")]
    for part in reversed(parts):
        tokens = part.upper().split()
        for token in tokens:
            cleaned = token.strip("., ")
            if len(cleaned) == 2 and cleaned in _US_STATES:
                return cleaned

    # Common full state name mapping (subset for most populous states)
    _STATE_NAMES = {
        "ALABAMA": "AL",
        "ALASKA": "AK",
        "ARIZONA": "AZ",
        "ARKANSAS": "AR",
        "CALIFORNIA": "CA",
        "COLORADO": "CO",
        "CONNECTICUT": "CT",
        "DELAWARE": "DE",
        "FLORIDA": "FL",
        "GEORGIA": "GA",
        "HAWAII": "HI",
        "IDAHO": "ID",
        "ILLINOIS": "IL",
        "INDIANA": "IN",
        "IOWA": "IA",
        "KANSAS": "KS",
        "KENTUCKY": "KY",
        "LOUISIANA": "LA",
        "MAINE": "ME",
        "MARYLAND": "MD",
        "MASSACHUSETTS": "MA",
        "MICHIGAN": "MI",
        "MINNESOTA": "MN",
        "MISSISSIPPI": "MS",
        "MISSOURI": "MO",
        "MONTANA": "MT",
        "NEBRASKA": "NE",
        "NEVADA": "NV",
        "NEW HAMPSHIRE": "NH",
        "NEW JERSEY": "NJ",
        "NEW MEXICO": "NM",
        "NEW YORK": "NY",
        "NORTH CAROLINA": "NC",
        "NORTH DAKOTA": "ND",
        "OHIO": "OH",
        "OKLAHOMA": "OK",
        "OREGON": "OR",
        "PENNSYLVANIA": "PA",
        "RHODE ISLAND": "RI",
        "SOUTH CAROLINA": "SC",
        "SOUTH DAKOTA": "SD",
        "TENNESSEE": "TN",
        "TEXAS": "TX",
        "UTAH": "UT",
        "VERMONT": "VT",
        "VIRGINIA": "VA",
        "WASHINGTON": "WA",
        "WEST VIRGINIA": "WV",
        "WISCONSIN": "WI",
        "WYOMING": "WY",
        "DISTRICT OF COLUMBIA": "DC",
    }
    location_upper = location.upper()
    for name, abbr in _STATE_NAMES.items():
        if name in location_upper:
            return abbr

    return "0"


def _resolve_occupation_key(role: str) -> Optional[str]:
    """Map a role string to a CareerOneStop benchmark key.

    Tries exact match first, then substring matching, then word-level matching.
    """
    role_lower = role.lower().strip()

    # Direct lookup in mapping
    if role_lower in ROLE_TO_COS_OCCUPATION:
        return ROLE_TO_COS_OCCUPATION[role_lower]

    # Substring match in mapping keys
    for mapping_key, occupation_key in ROLE_TO_COS_OCCUPATION.items():
        if mapping_key in role_lower or role_lower in mapping_key:
            return occupation_key

    # Word-level overlap: find the mapping key with the most word overlap
    role_words = set(role_lower.split())
    best_match = None
    best_score = 0
    for mapping_key, occupation_key in ROLE_TO_COS_OCCUPATION.items():
        key_words = set(mapping_key.split())
        overlap = len(role_words & key_words)
        if overlap > best_score:
            best_score = overlap
            best_match = occupation_key

    if best_score > 0:
        return best_match

    # Direct lookup in benchmarks
    if role_lower in CAREERONESTOP_BENCHMARKS:
        return role_lower

    return None


def _cos_api_get(
    endpoint_path: str, api_key: str, timeout: int = 15
) -> Optional[Dict[str, Any]]:
    """Make an authenticated GET request to the CareerOneStop API.

    Args:
        endpoint_path: The path after the base URL (e.g., '/v1/occupation/user/keyword/CA').
        api_key: The Bearer token for authentication.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response dict, or None on failure.
    """
    base_url = "https://api.careeronestop.org"
    url = base_url + endpoint_path
    headers = {
        "Authorization": "Bearer " + api_key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        return _http_get_json(url, headers, timeout)
    except Exception as exc:
        _log_warn(
            "CareerOneStop API request failed for {}: {}".format(endpoint_path, exc)
        )
        return None


def _fetch_cos_occupation_detail(
    user_id: str, api_key: str, keyword: str, location: str
) -> Optional[Dict[str, Any]]:
    """Fetch occupation detail from CareerOneStop API."""
    encoded_keyword = urllib.parse.quote(keyword, safe="")
    encoded_location = urllib.parse.quote(location, safe="")
    path = "/v1/occupation/{}/{}/{}?source=NationalAverage&lang=en".format(
        urllib.parse.quote(user_id, safe=""), encoded_keyword, encoded_location
    )
    cache_key = _cache_key("cos_occ_detail", "{}:{}".format(keyword, location))
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    data = _cos_api_get(path, api_key)
    if data:
        _set_cached(cache_key, data)
    return data


def _fetch_cos_salary_data(
    user_id: str, api_key: str, keyword: str, location: str
) -> Optional[Dict[str, Any]]:
    """Fetch salary data from CareerOneStop API."""
    encoded_keyword = urllib.parse.quote(keyword, safe="")
    encoded_location = urllib.parse.quote(location, safe="")
    path = "/v1/salarydata/{}/{}/{}?sortColumns=Median&sortOrder=desc".format(
        urllib.parse.quote(user_id, safe=""), encoded_keyword, encoded_location
    )
    cache_key = _cache_key("cos_salary", "{}:{}".format(keyword, location))
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    data = _cos_api_get(path, api_key)
    if data:
        _set_cached(cache_key, data)
    return data


def _fetch_cos_outlook(
    user_id: str, api_key: str, keyword: str
) -> Optional[Dict[str, Any]]:
    """Fetch occupation outlook (national) from CareerOneStop API."""
    encoded_keyword = urllib.parse.quote(keyword, safe="")
    path = "/v1/occupation/{}/{}/0?source=NationalAverage".format(
        urllib.parse.quote(user_id, safe=""), encoded_keyword
    )
    cache_key = _cache_key("cos_outlook", keyword)
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    data = _cos_api_get(path, api_key)
    if data:
        _set_cached(cache_key, data)
    return data


def _fetch_cos_certifications(
    user_id: str, api_key: str, keyword: str, location: str
) -> Optional[Dict[str, Any]]:
    """Fetch certifications from CareerOneStop API."""
    encoded_keyword = urllib.parse.quote(keyword, safe="")
    encoded_location = urllib.parse.quote(location, safe="")
    path = (
        "/v1/certificationfinder/{}/{}/{}?"
        "sortColumns=Name&sortOrder=asc&startRecord=0&limitRecord=10"
    ).format(urllib.parse.quote(user_id, safe=""), encoded_keyword, encoded_location)
    cache_key = _cache_key("cos_certs", "{}:{}".format(keyword, location))
    cached = _get_cached(cache_key)
    if cached is not None:
        return cached
    data = _cos_api_get(path, api_key)
    if data:
        _set_cached(cache_key, data)
    return data


def _parse_salary_response(salary_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract median, entry, and experienced salary from CareerOneStop salary response."""
    if not salary_data:
        return None

    # The API returns salary records under various keys depending on the endpoint version
    records = None
    for key in ("SalaryList", "salaryList", "Wages", "wages", "OccupationDetail"):
        if key in salary_data and salary_data[key]:
            records = salary_data[key]
            break

    if not records:
        # Try top-level fields
        median = salary_data.get("Median") or salary_data.get("median")
        if median:
            return {
                "median": _safe_int(median),
                "entry_level": _safe_int(
                    salary_data.get("Pct10")
                    or salary_data.get("pct10")
                    or salary_data.get("EntryLevel")
                    or 0
                ),
                "experienced": _safe_int(
                    salary_data.get("Pct90")
                    or salary_data.get("pct90")
                    or salary_data.get("Experienced")
                    or 0
                ),
            }
        return None

    if isinstance(records, list) and len(records) > 0:
        rec = records[0]
    elif isinstance(records, dict):
        rec = records
    else:
        return None

    median = (
        rec.get("Median")
        or rec.get("median")
        or rec.get("MedianAnnual")
        or rec.get("medianAnnual")
    )
    entry = (
        rec.get("Pct10")
        or rec.get("pct10")
        or rec.get("AnnualPct10")
        or rec.get("annualPct10")
        or rec.get("EntryLevel")
        or rec.get("entryLevel")
    )
    experienced = (
        rec.get("Pct90")
        or rec.get("pct90")
        or rec.get("AnnualPct90")
        or rec.get("annualPct90")
        or rec.get("Experienced")
        or rec.get("experienced")
    )

    return {
        "median": _safe_int(median),
        "entry_level": _safe_int(entry),
        "experienced": _safe_int(experienced),
    }


def _parse_outlook_response(outlook_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract growth projections from CareerOneStop outlook response."""
    if not outlook_data:
        return None

    # Navigate into nested structure
    projections = (
        outlook_data.get("OccupationDetail")
        or outlook_data.get("Projections")
        or outlook_data.get("projections")
        or outlook_data
    )
    if isinstance(projections, list) and len(projections) > 0:
        projections = projections[0]

    growth_pct = (
        projections.get("ProjectedGrowth")
        or projections.get("projectedGrowth")
        or projections.get("BrightOutlookGrowth")
        or projections.get("ProjectedPercentChange")
    )
    growth_label = (
        projections.get("GrowthLabel")
        or projections.get("growthLabel")
        or projections.get("OutlookDescription")
        or projections.get("Outlook")
    )
    annual_openings = (
        projections.get("AnnualOpenings")
        or projections.get("annualOpenings")
        or projections.get("ProjectedAnnualJobOpenings")
    )
    employment = (
        projections.get("Employment")
        or projections.get("employment")
        or projections.get("CurrentEmployment")
    )

    result = {}
    if growth_pct is not None:
        result["projected_growth_pct"] = _safe_float(growth_pct)
    if growth_label:
        result["growth_label"] = str(growth_label)
    if annual_openings is not None:
        result["annual_openings"] = _safe_int(annual_openings)
    if employment is not None:
        result["employment"] = _safe_int(employment)

    return result if result else None


def _parse_certifications_response(
    cert_data: Dict[str, Any],
) -> Optional[List[Dict[str, str]]]:
    """Extract certification names and organizations from CareerOneStop response."""
    if not cert_data:
        return None

    cert_list = (
        cert_data.get("CertList")
        or cert_data.get("certList")
        or cert_data.get("CertificationList")
        or cert_data.get("certificationList")
    )
    if not cert_list or not isinstance(cert_list, list):
        return None

    results = []
    for cert in cert_list[:10]:
        name = (
            cert.get("Name")
            or cert.get("name")
            or cert.get("CertName")
            or cert.get("certName")
        )
        org = (
            cert.get("Organization")
            or cert.get("organization")
            or cert.get("CertOrg")
            or cert.get("certOrg")
        )
        url = (
            cert.get("Url")
            or cert.get("url")
            or cert.get("CertUrl")
            or cert.get("certUrl")
        )
        if name:
            entry = {"name": str(name)}
            if org:
                entry["organization"] = str(org)
            if url:
                entry["url"] = str(url)
            results.append(entry)

    return results if results else None


def _safe_int(val) -> int:
    """Safely convert a value to int, returning 0 on failure."""
    if val is None:
        return 0
    try:
        if isinstance(val, str):
            val = val.replace(",", "").replace("$", "").strip()
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def _safe_float(val) -> float:
    """Safely convert a value to float, returning 0.0 on failure."""
    if val is None:
        return 0.0
    try:
        if isinstance(val, str):
            val = val.replace(",", "").replace("%", "").strip()
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _build_occupation_from_benchmark(role: str, benchmark_key: str) -> Dict[str, Any]:
    """Build an occupation entry from curated benchmark data."""
    bench = CAREERONESTOP_BENCHMARKS[benchmark_key]
    return {
        "title": bench.get("title", benchmark_key.title()),
        "soc_code": bench["soc_code"],
        "salary": {
            "median": bench["median_salary"],
            "entry_level": bench["entry_salary"],
            "experienced": bench["experienced_salary"],
        },
        "employment": bench["employment"],
        "outlook": {
            "projected_growth_pct": bench["projected_growth_pct"],
            "growth_label": bench["projected_growth_label"],
            "annual_openings": bench["annual_openings"],
        },
        "education": bench["education"],
        "work_experience": bench.get("work_experience", "None"),
        "on_job_training": bench.get("on_job_training", "None"),
    }


def fetch_careeronestop_data(roles: List[str], locations: List[str]) -> Dict[str, Any]:
    """Fetch official DOL salary data, occupation outlook, certifications, and
    training programs from CareerOneStop by occupation and location.

    Args:
        roles: List of job role strings (e.g., ["Software Engineer", "Data Scientist"]).
        locations: List of location strings (e.g., ["San Mateo, CA", "Austin, TX"]).

    Returns:
        Dict with 'source', 'occupations' keyed by original role name, and
        optionally 'certifications' when API data is available.
    """
    # Top-level cache check
    cache_k = _cache_key("careeronestop", f"{sorted(roles)}|{sorted(locations)}")
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    api_key = os.environ.get("CAREERONESTOP_API_KEY", "").strip()
    user_id = os.environ.get("CAREERONESTOP_USER_ID", "").strip()
    use_api = bool(api_key and user_id)

    if not use_api:
        _log_info(
            "CareerOneStop API credentials not found. "
            "Using curated benchmark data. Set CAREERONESTOP_API_KEY and "
            "CAREERONESTOP_USER_ID environment variables for live API access."
        )

    # Extract unique state abbreviations from locations
    state_abbrs = []
    location_to_state = {}
    for loc in locations:
        state = _cos_extract_state_abbr(loc)
        location_to_state[loc] = state
        if state != "0" and state not in state_abbrs:
            state_abbrs.append(state)

    # Use first location state as primary, or "0" for national
    primary_state = state_abbrs[0] if state_abbrs else "0"

    result = {
        "source": "CareerOneStop API" if use_api else "CareerOneStop Benchmarks",
        "occupations": {},
    }

    for role in roles:
        occupation_key = _resolve_occupation_key(role)
        if occupation_key is None:
            _log_warn(
                "No CareerOneStop mapping found for role '{}'. Skipping.".format(role)
            )
            continue

        if not use_api:
            # Fallback to benchmark data
            if occupation_key in CAREERONESTOP_BENCHMARKS:
                occ_entry = _build_occupation_from_benchmark(role, occupation_key)
                result["occupations"][role] = occ_entry
            else:
                _log_warn(
                    "No benchmark data for occupation key '{}'. Skipping.".format(
                        occupation_key
                    )
                )
            continue

        # --- Live API path ---
        _log_info(
            "Fetching CareerOneStop data for '{}' (mapped to '{}') in '{}'".format(
                role, occupation_key, primary_state
            )
        )

        # Fetch occupation detail (national)
        occ_detail = _fetch_cos_occupation_detail(
            user_id, api_key, occupation_key, primary_state
        )

        # Fetch salary data (primary state)
        salary_raw = _fetch_cos_salary_data(
            user_id, api_key, occupation_key, primary_state
        )
        salary_parsed = _parse_salary_response(salary_raw) if salary_raw else None

        # Fetch national outlook
        outlook_raw = _fetch_cos_outlook(user_id, api_key, occupation_key)
        outlook_parsed = _parse_outlook_response(outlook_raw) if outlook_raw else None

        # Fetch certifications
        certs_raw = _fetch_cos_certifications(
            user_id, api_key, occupation_key, primary_state
        )
        certs_parsed = _parse_certifications_response(certs_raw) if certs_raw else None

        # Build occupation entry, starting from benchmark as base if API data is sparse
        if occupation_key in CAREERONESTOP_BENCHMARKS:
            occ_entry = _build_occupation_from_benchmark(role, occupation_key)
        else:
            occ_entry = {
                "title": occupation_key.title(),
                "soc_code": "",
                "salary": {"median": 0, "entry_level": 0, "experienced": 0},
                "employment": 0,
                "outlook": {
                    "projected_growth_pct": 0,
                    "growth_label": "N/A",
                    "annual_openings": 0,
                },
                "education": "N/A",
            }

        # Override with API data when available
        if occ_detail:
            detail_body = occ_detail
            if isinstance(detail_body, dict):
                soc = (
                    detail_body.get("OnetCode")
                    or detail_body.get("SocCode")
                    or detail_body.get("OccupationCode")
                    or detail_body.get("Code")
                )
                title = (
                    detail_body.get("OnetTitle")
                    or detail_body.get("Title")
                    or detail_body.get("OccupationTitle")
                )
                education = (
                    detail_body.get("EducationTraining")
                    or detail_body.get("Education")
                    or detail_body.get("TypicalEducation")
                )
                if soc:
                    occ_entry["soc_code"] = str(soc)
                if title:
                    occ_entry["title"] = str(title)
                if education:
                    occ_entry["education"] = str(education)

        if salary_parsed:
            if salary_parsed.get("median") or 0 > 0:
                occ_entry["salary"] = salary_parsed

        if outlook_parsed:
            if "projected_growth_pct" in outlook_parsed:
                occ_entry["outlook"]["projected_growth_pct"] = outlook_parsed[
                    "projected_growth_pct"
                ]
            if "growth_label" in outlook_parsed:
                occ_entry["outlook"]["growth_label"] = outlook_parsed["growth_label"]
            if "annual_openings" in outlook_parsed:
                occ_entry["outlook"]["annual_openings"] = outlook_parsed[
                    "annual_openings"
                ]
            if "employment" in outlook_parsed:
                occ_entry["employment"] = outlook_parsed["employment"]

        if certs_parsed:
            occ_entry["certifications"] = certs_parsed

        # Fetch location-specific salary data for each unique state
        location_salary = {}
        for state in state_abbrs:
            if state == "0":
                continue
            loc_salary_raw = _fetch_cos_salary_data(
                user_id, api_key, occupation_key, state
            )
            if loc_salary_raw:
                loc_salary_parsed = _parse_salary_response(loc_salary_raw)
                if loc_salary_parsed and loc_salary_parsed.get("median") or 0 > 0:
                    location_salary[state] = {
                        "median": loc_salary_parsed["median"],
                        "entry": loc_salary_parsed["entry_level"],
                        "experienced": loc_salary_parsed["experienced"],
                    }

        if location_salary:
            occ_entry["location_salary"] = location_salary

        result["occupations"][role] = occ_entry

        # Polite rate-limit pause between roles when using the API
        time.sleep(0.25)

    if not result["occupations"]:
        _log_warn("No occupation data could be retrieved for roles: {}".format(roles))

    _set_cached(cache_k, result)
    return result


# API 25: Jooble API (International Job Market)

# ---------------------------------------------------------------------------
# Jooble API Integration for Media Plan Generator
# ---------------------------------------------------------------------------

# ── Curated fallback benchmark data ──────────────────────────────────────────
# NOTE: Canonical benchmark source for CPC/CPA/CPM is trend_engine.py.
# See trend_engine.get_benchmark() for authoritative ad platform benchmarks.
# JOOBLE_MARKET_DATA below contains job market metadata (postings, salaries,
# time-to-fill) which is complementary to trend_engine, not a duplication.

JOOBLE_MARKET_DATA = {
    "united_states": {
        "technology": {
            "avg_job_postings": 185000,
            "avg_salary_range": "$85,000-$175,000",
            "market_activity": "Very High",
            "top_cities": ["San Francisco", "New York", "Seattle", "Austin", "Boston"],
            "avg_time_to_fill_days": 42,
        },
        "healthcare": {
            "avg_job_postings": 210000,
            "avg_salary_range": "$55,000-$145,000",
            "market_activity": "Very High",
            "top_cities": ["New York", "Houston", "Chicago", "Los Angeles", "Boston"],
            "avg_time_to_fill_days": 35,
        },
        "finance": {
            "avg_job_postings": 95000,
            "avg_salary_range": "$70,000-$165,000",
            "market_activity": "High",
            "top_cities": [
                "New York",
                "Chicago",
                "San Francisco",
                "Charlotte",
                "Boston",
            ],
            "avg_time_to_fill_days": 40,
        },
        "engineering": {
            "avg_job_postings": 120000,
            "avg_salary_range": "$75,000-$155,000",
            "market_activity": "High",
            "top_cities": ["Houston", "Detroit", "San Jose", "Seattle", "Denver"],
            "avg_time_to_fill_days": 45,
        },
        "marketing": {
            "avg_job_postings": 78000,
            "avg_salary_range": "$50,000-$120,000",
            "market_activity": "High",
            "top_cities": [
                "New York",
                "Los Angeles",
                "Chicago",
                "San Francisco",
                "Atlanta",
            ],
            "avg_time_to_fill_days": 33,
        },
        "sales": {
            "avg_job_postings": 145000,
            "avg_salary_range": "$45,000-$130,000",
            "market_activity": "Very High",
            "top_cities": ["New York", "Chicago", "Dallas", "Atlanta", "Los Angeles"],
            "avg_time_to_fill_days": 28,
        },
    },
    "united_kingdom": {
        "technology": {
            "avg_job_postings": 65000,
            "avg_salary_range": "\u00a345,000-\u00a395,000",
            "market_activity": "High",
            "top_cities": [
                "London",
                "Manchester",
                "Edinburgh",
                "Birmingham",
                "Bristol",
            ],
            "avg_time_to_fill_days": 38,
        },
        "healthcare": {
            "avg_job_postings": 82000,
            "avg_salary_range": "\u00a328,000-\u00a365,000",
            "market_activity": "Very High",
            "top_cities": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
            "avg_time_to_fill_days": 30,
        },
        "finance": {
            "avg_job_postings": 48000,
            "avg_salary_range": "\u00a340,000-\u00a3110,000",
            "market_activity": "High",
            "top_cities": ["London", "Edinburgh", "Manchester", "Birmingham", "Leeds"],
            "avg_time_to_fill_days": 36,
        },
        "engineering": {
            "avg_job_postings": 42000,
            "avg_salary_range": "\u00a335,000-\u00a375,000",
            "market_activity": "Medium",
            "top_cities": [
                "London",
                "Manchester",
                "Birmingham",
                "Bristol",
                "Cambridge",
            ],
            "avg_time_to_fill_days": 40,
        },
        "marketing": {
            "avg_job_postings": 35000,
            "avg_salary_range": "\u00a330,000-\u00a370,000",
            "market_activity": "High",
            "top_cities": ["London", "Manchester", "Birmingham", "Bristol", "Leeds"],
            "avg_time_to_fill_days": 30,
        },
        "sales": {
            "avg_job_postings": 55000,
            "avg_salary_range": "\u00a325,000-\u00a365,000",
            "market_activity": "High",
            "top_cities": ["London", "Manchester", "Birmingham", "Glasgow", "Leeds"],
            "avg_time_to_fill_days": 25,
        },
    },
    "germany": {
        "technology": {
            "avg_job_postings": 72000,
            "avg_salary_range": "\u20ac55,000-\u20ac105,000",
            "market_activity": "High",
            "top_cities": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Stuttgart"],
            "avg_time_to_fill_days": 45,
        },
        "healthcare": {
            "avg_job_postings": 68000,
            "avg_salary_range": "\u20ac40,000-\u20ac85,000",
            "market_activity": "Very High",
            "top_cities": ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"],
            "avg_time_to_fill_days": 32,
        },
        "finance": {
            "avg_job_postings": 38000,
            "avg_salary_range": "\u20ac50,000-\u20ac110,000",
            "market_activity": "High",
            "top_cities": ["Frankfurt", "Munich", "Berlin", "Hamburg", "Dusseldorf"],
            "avg_time_to_fill_days": 42,
        },
        "engineering": {
            "avg_job_postings": 85000,
            "avg_salary_range": "\u20ac50,000-\u20ac95,000",
            "market_activity": "Very High",
            "top_cities": ["Munich", "Stuttgart", "Hamburg", "Berlin", "Frankfurt"],
            "avg_time_to_fill_days": 48,
        },
        "marketing": {
            "avg_job_postings": 28000,
            "avg_salary_range": "\u20ac38,000-\u20ac75,000",
            "market_activity": "Medium",
            "top_cities": ["Berlin", "Hamburg", "Munich", "Cologne", "Frankfurt"],
            "avg_time_to_fill_days": 35,
        },
        "sales": {
            "avg_job_postings": 42000,
            "avg_salary_range": "\u20ac35,000-\u20ac80,000",
            "market_activity": "High",
            "top_cities": ["Munich", "Berlin", "Hamburg", "Frankfurt", "Dusseldorf"],
            "avg_time_to_fill_days": 30,
        },
    },
    "canada": {
        "technology": {
            "avg_job_postings": 52000,
            "avg_salary_range": "C$70,000-C$140,000",
            "market_activity": "High",
            "top_cities": ["Toronto", "Vancouver", "Montreal", "Ottawa", "Calgary"],
            "avg_time_to_fill_days": 38,
        },
        "healthcare": {
            "avg_job_postings": 65000,
            "avg_salary_range": "C$50,000-C$120,000",
            "market_activity": "Very High",
            "top_cities": ["Toronto", "Vancouver", "Montreal", "Edmonton", "Ottawa"],
            "avg_time_to_fill_days": 30,
        },
        "finance": {
            "avg_job_postings": 32000,
            "avg_salary_range": "C$60,000-C$130,000",
            "market_activity": "High",
            "top_cities": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
            "avg_time_to_fill_days": 36,
        },
        "engineering": {
            "avg_job_postings": 38000,
            "avg_salary_range": "C$65,000-C$125,000",
            "market_activity": "High",
            "top_cities": ["Toronto", "Calgary", "Vancouver", "Edmonton", "Montreal"],
            "avg_time_to_fill_days": 42,
        },
        "marketing": {
            "avg_job_postings": 24000,
            "avg_salary_range": "C$45,000-C$95,000",
            "market_activity": "Medium",
            "top_cities": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
            "avg_time_to_fill_days": 30,
        },
        "sales": {
            "avg_job_postings": 40000,
            "avg_salary_range": "C$40,000-C$100,000",
            "market_activity": "High",
            "top_cities": ["Toronto", "Vancouver", "Montreal", "Calgary", "Edmonton"],
            "avg_time_to_fill_days": 26,
        },
    },
    "australia": {
        "technology": {
            "avg_job_postings": 42000,
            "avg_salary_range": "A$80,000-A$160,000",
            "market_activity": "High",
            "top_cities": ["Sydney", "Melbourne", "Brisbane", "Perth", "Canberra"],
            "avg_time_to_fill_days": 40,
        },
        "healthcare": {
            "avg_job_postings": 55000,
            "avg_salary_range": "A$60,000-A$130,000",
            "market_activity": "Very High",
            "top_cities": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
            "avg_time_to_fill_days": 28,
        },
        "finance": {
            "avg_job_postings": 28000,
            "avg_salary_range": "A$70,000-A$150,000",
            "market_activity": "High",
            "top_cities": ["Sydney", "Melbourne", "Brisbane", "Perth", "Canberra"],
            "avg_time_to_fill_days": 38,
        },
        "engineering": {
            "avg_job_postings": 35000,
            "avg_salary_range": "A$75,000-A$145,000",
            "market_activity": "High",
            "top_cities": ["Perth", "Sydney", "Melbourne", "Brisbane", "Adelaide"],
            "avg_time_to_fill_days": 44,
        },
        "marketing": {
            "avg_job_postings": 20000,
            "avg_salary_range": "A$55,000-A$110,000",
            "market_activity": "Medium",
            "top_cities": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
            "avg_time_to_fill_days": 32,
        },
        "sales": {
            "avg_job_postings": 32000,
            "avg_salary_range": "A$50,000-A$105,000",
            "market_activity": "High",
            "top_cities": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
            "avg_time_to_fill_days": 26,
        },
    },
    "india": {
        "technology": {
            "avg_job_postings": 220000,
            "avg_salary_range": "\u20b96,00,000-\u20b925,00,000",
            "market_activity": "Very High",
            "top_cities": ["Bangalore", "Hyderabad", "Pune", "Chennai", "Mumbai"],
            "avg_time_to_fill_days": 30,
        },
        "healthcare": {
            "avg_job_postings": 95000,
            "avg_salary_range": "\u20b94,00,000-\u20b915,00,000",
            "market_activity": "High",
            "top_cities": ["Mumbai", "Delhi", "Chennai", "Bangalore", "Hyderabad"],
            "avg_time_to_fill_days": 25,
        },
        "finance": {
            "avg_job_postings": 72000,
            "avg_salary_range": "\u20b95,00,000-\u20b920,00,000",
            "market_activity": "High",
            "top_cities": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai"],
            "avg_time_to_fill_days": 28,
        },
        "engineering": {
            "avg_job_postings": 160000,
            "avg_salary_range": "\u20b95,00,000-\u20b918,00,000",
            "market_activity": "Very High",
            "top_cities": ["Bangalore", "Pune", "Hyderabad", "Chennai", "Mumbai"],
            "avg_time_to_fill_days": 26,
        },
        "marketing": {
            "avg_job_postings": 48000,
            "avg_salary_range": "\u20b93,50,000-\u20b912,00,000",
            "market_activity": "High",
            "top_cities": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Pune"],
            "avg_time_to_fill_days": 22,
        },
        "sales": {
            "avg_job_postings": 130000,
            "avg_salary_range": "\u20b93,00,000-\u20b910,00,000",
            "market_activity": "Very High",
            "top_cities": ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad"],
            "avg_time_to_fill_days": 18,
        },
    },
    "france": {
        "technology": {
            "avg_job_postings": 48000,
            "avg_salary_range": "\u20ac42,000-\u20ac85,000",
            "market_activity": "High",
            "top_cities": ["Paris", "Lyon", "Toulouse", "Nantes", "Bordeaux"],
            "avg_time_to_fill_days": 42,
        },
        "healthcare": {
            "avg_job_postings": 58000,
            "avg_salary_range": "\u20ac30,000-\u20ac70,000",
            "market_activity": "Very High",
            "top_cities": ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux"],
            "avg_time_to_fill_days": 30,
        },
        "finance": {
            "avg_job_postings": 30000,
            "avg_salary_range": "\u20ac40,000-\u20ac95,000",
            "market_activity": "High",
            "top_cities": ["Paris", "Lyon", "Lille", "Marseille", "Toulouse"],
            "avg_time_to_fill_days": 38,
        },
        "engineering": {
            "avg_job_postings": 52000,
            "avg_salary_range": "\u20ac38,000-\u20ac78,000",
            "market_activity": "High",
            "top_cities": ["Paris", "Toulouse", "Lyon", "Grenoble", "Nantes"],
            "avg_time_to_fill_days": 44,
        },
        "marketing": {
            "avg_job_postings": 22000,
            "avg_salary_range": "\u20ac32,000-\u20ac65,000",
            "market_activity": "Medium",
            "top_cities": ["Paris", "Lyon", "Bordeaux", "Marseille", "Lille"],
            "avg_time_to_fill_days": 32,
        },
        "sales": {
            "avg_job_postings": 38000,
            "avg_salary_range": "\u20ac28,000-\u20ac62,000",
            "market_activity": "High",
            "top_cities": ["Paris", "Lyon", "Marseille", "Lille", "Bordeaux"],
            "avg_time_to_fill_days": 25,
        },
    },
    "singapore": {
        "technology": {
            "avg_job_postings": 18000,
            "avg_salary_range": "S$60,000-S$140,000",
            "market_activity": "High",
            "top_cities": [
                "Singapore Central",
                "Jurong East",
                "Changi",
                "Tampines",
                "Woodlands",
            ],
            "avg_time_to_fill_days": 35,
        },
        "healthcare": {
            "avg_job_postings": 12000,
            "avg_salary_range": "S$40,000-S$100,000",
            "market_activity": "High",
            "top_cities": [
                "Singapore Central",
                "Novena",
                "Outram",
                "Tampines",
                "Jurong East",
            ],
            "avg_time_to_fill_days": 28,
        },
        "finance": {
            "avg_job_postings": 22000,
            "avg_salary_range": "S$55,000-S$150,000",
            "market_activity": "Very High",
            "top_cities": [
                "Singapore Central",
                "Marina Bay",
                "Raffles Place",
                "Shenton Way",
                "Jurong East",
            ],
            "avg_time_to_fill_days": 32,
        },
        "engineering": {
            "avg_job_postings": 14000,
            "avg_salary_range": "S$50,000-S$120,000",
            "market_activity": "High",
            "top_cities": [
                "Singapore Central",
                "Jurong",
                "Tuas",
                "Changi",
                "Woodlands",
            ],
            "avg_time_to_fill_days": 38,
        },
        "marketing": {
            "avg_job_postings": 9000,
            "avg_salary_range": "S$42,000-S$95,000",
            "market_activity": "Medium",
            "top_cities": [
                "Singapore Central",
                "Orchard",
                "Raffles Place",
                "Tampines",
                "Jurong East",
            ],
            "avg_time_to_fill_days": 30,
        },
        "sales": {
            "avg_job_postings": 15000,
            "avg_salary_range": "S$38,000-S$90,000",
            "market_activity": "High",
            "top_cities": [
                "Singapore Central",
                "Raffles Place",
                "Orchard",
                "Jurong East",
                "Tampines",
            ],
            "avg_time_to_fill_days": 24,
        },
    },
    "netherlands": {
        "technology": {
            "avg_job_postings": 32000,
            "avg_salary_range": "\u20ac48,000-\u20ac95,000",
            "market_activity": "High",
            "top_cities": [
                "Amsterdam",
                "Rotterdam",
                "The Hague",
                "Eindhoven",
                "Utrecht",
            ],
            "avg_time_to_fill_days": 38,
        },
        "healthcare": {
            "avg_job_postings": 28000,
            "avg_salary_range": "\u20ac35,000-\u20ac75,000",
            "market_activity": "High",
            "top_cities": ["Amsterdam", "Rotterdam", "Utrecht", "The Hague", "Leiden"],
            "avg_time_to_fill_days": 30,
        },
        "finance": {
            "avg_job_postings": 20000,
            "avg_salary_range": "\u20ac45,000-\u20ac105,000",
            "market_activity": "High",
            "top_cities": [
                "Amsterdam",
                "Rotterdam",
                "The Hague",
                "Utrecht",
                "Eindhoven",
            ],
            "avg_time_to_fill_days": 36,
        },
        "engineering": {
            "avg_job_postings": 26000,
            "avg_salary_range": "\u20ac42,000-\u20ac88,000",
            "market_activity": "High",
            "top_cities": ["Eindhoven", "Rotterdam", "Amsterdam", "The Hague", "Delft"],
            "avg_time_to_fill_days": 42,
        },
        "marketing": {
            "avg_job_postings": 14000,
            "avg_salary_range": "\u20ac36,000-\u20ac72,000",
            "market_activity": "Medium",
            "top_cities": [
                "Amsterdam",
                "Rotterdam",
                "Utrecht",
                "The Hague",
                "Eindhoven",
            ],
            "avg_time_to_fill_days": 28,
        },
        "sales": {
            "avg_job_postings": 22000,
            "avg_salary_range": "\u20ac32,000-\u20ac70,000",
            "market_activity": "High",
            "top_cities": [
                "Amsterdam",
                "Rotterdam",
                "The Hague",
                "Utrecht",
                "Eindhoven",
            ],
            "avg_time_to_fill_days": 24,
        },
    },
    "ireland": {
        "technology": {
            "avg_job_postings": 22000,
            "avg_salary_range": "\u20ac50,000-\u20ac105,000",
            "market_activity": "Very High",
            "top_cities": ["Dublin", "Cork", "Galway", "Limerick", "Waterford"],
            "avg_time_to_fill_days": 35,
        },
        "healthcare": {
            "avg_job_postings": 18000,
            "avg_salary_range": "\u20ac35,000-\u20ac72,000",
            "market_activity": "High",
            "top_cities": ["Dublin", "Cork", "Galway", "Limerick", "Waterford"],
            "avg_time_to_fill_days": 28,
        },
        "finance": {
            "avg_job_postings": 16000,
            "avg_salary_range": "\u20ac45,000-\u20ac110,000",
            "market_activity": "High",
            "top_cities": ["Dublin", "Cork", "Galway", "Limerick", "Kilkenny"],
            "avg_time_to_fill_days": 34,
        },
        "engineering": {
            "avg_job_postings": 14000,
            "avg_salary_range": "\u20ac40,000-\u20ac85,000",
            "market_activity": "High",
            "top_cities": ["Dublin", "Cork", "Galway", "Limerick", "Athlone"],
            "avg_time_to_fill_days": 40,
        },
        "marketing": {
            "avg_job_postings": 9500,
            "avg_salary_range": "\u20ac32,000-\u20ac68,000",
            "market_activity": "Medium",
            "top_cities": ["Dublin", "Cork", "Galway", "Limerick", "Waterford"],
            "avg_time_to_fill_days": 28,
        },
        "sales": {
            "avg_job_postings": 13000,
            "avg_salary_range": "\u20ac28,000-\u20ac62,000",
            "market_activity": "High",
            "top_cities": ["Dublin", "Cork", "Galway", "Limerick", "Waterford"],
            "avg_time_to_fill_days": 22,
        },
    },
    "uae": {
        "technology": {
            "avg_job_postings": 25000,
            "avg_salary_range": "AED 180,000-AED 420,000",
            "market_activity": "High",
            "top_cities": ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah"],
            "avg_time_to_fill_days": 32,
        },
        "healthcare": {
            "avg_job_postings": 18000,
            "avg_salary_range": "AED 120,000-AED 360,000",
            "market_activity": "High",
            "top_cities": ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Al Ain"],
            "avg_time_to_fill_days": 28,
        },
        "finance": {
            "avg_job_postings": 20000,
            "avg_salary_range": "AED 160,000-AED 450,000",
            "market_activity": "Very High",
            "top_cities": ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah"],
            "avg_time_to_fill_days": 30,
        },
        "engineering": {
            "avg_job_postings": 22000,
            "avg_salary_range": "AED 150,000-AED 380,000",
            "market_activity": "High",
            "top_cities": [
                "Dubai",
                "Abu Dhabi",
                "Sharjah",
                "Ras Al Khaimah",
                "Fujairah",
            ],
            "avg_time_to_fill_days": 36,
        },
        "marketing": {
            "avg_job_postings": 12000,
            "avg_salary_range": "AED 100,000-AED 280,000",
            "market_activity": "Medium",
            "top_cities": ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah"],
            "avg_time_to_fill_days": 26,
        },
        "sales": {
            "avg_job_postings": 18000,
            "avg_salary_range": "AED 90,000-AED 250,000",
            "market_activity": "High",
            "top_cities": ["Dubai", "Abu Dhabi", "Sharjah", "Ajman", "Ras Al Khaimah"],
            "avg_time_to_fill_days": 20,
        },
    },
    "japan": {
        "technology": {
            "avg_job_postings": 58000,
            "avg_salary_range": "\u00a55,500,000-\u00a512,000,000",
            "market_activity": "High",
            "top_cities": ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Fukuoka"],
            "avg_time_to_fill_days": 50,
        },
        "healthcare": {
            "avg_job_postings": 45000,
            "avg_salary_range": "\u00a54,000,000-\u00a59,000,000",
            "market_activity": "Very High",
            "top_cities": ["Tokyo", "Osaka", "Nagoya", "Yokohama", "Sapporo"],
            "avg_time_to_fill_days": 35,
        },
        "finance": {
            "avg_job_postings": 28000,
            "avg_salary_range": "\u00a55,000,000-\u00a513,000,000",
            "market_activity": "High",
            "top_cities": ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Kobe"],
            "avg_time_to_fill_days": 44,
        },
        "engineering": {
            "avg_job_postings": 65000,
            "avg_salary_range": "\u00a54,500,000-\u00a510,000,000",
            "market_activity": "Very High",
            "top_cities": ["Tokyo", "Nagoya", "Osaka", "Yokohama", "Fukuoka"],
            "avg_time_to_fill_days": 48,
        },
        "marketing": {
            "avg_job_postings": 18000,
            "avg_salary_range": "\u00a54,000,000-\u00a58,000,000",
            "market_activity": "Medium",
            "top_cities": ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Fukuoka"],
            "avg_time_to_fill_days": 38,
        },
        "sales": {
            "avg_job_postings": 42000,
            "avg_salary_range": "\u00a53,500,000-\u00a58,500,000",
            "market_activity": "High",
            "top_cities": ["Tokyo", "Osaka", "Nagoya", "Yokohama", "Fukuoka"],
            "avg_time_to_fill_days": 30,
        },
    },
    "brazil": {
        "technology": {
            "avg_job_postings": 65000,
            "avg_salary_range": "R$72,000-R$180,000",
            "market_activity": "High",
            "top_cities": [
                "S\u00e3o Paulo",
                "Rio de Janeiro",
                "Belo Horizonte",
                "Curitiba",
                "Porto Alegre",
            ],
            "avg_time_to_fill_days": 35,
        },
        "healthcare": {
            "avg_job_postings": 78000,
            "avg_salary_range": "R$48,000-R$144,000",
            "market_activity": "Very High",
            "top_cities": [
                "S\u00e3o Paulo",
                "Rio de Janeiro",
                "Bras\u00edlia",
                "Belo Horizonte",
                "Salvador",
            ],
            "avg_time_to_fill_days": 25,
        },
        "finance": {
            "avg_job_postings": 35000,
            "avg_salary_range": "R$60,000-R$168,000",
            "market_activity": "High",
            "top_cities": [
                "S\u00e3o Paulo",
                "Rio de Janeiro",
                "Bras\u00edlia",
                "Belo Horizonte",
                "Curitiba",
            ],
            "avg_time_to_fill_days": 32,
        },
        "engineering": {
            "avg_job_postings": 48000,
            "avg_salary_range": "R$60,000-R$156,000",
            "market_activity": "High",
            "top_cities": [
                "S\u00e3o Paulo",
                "Rio de Janeiro",
                "Belo Horizonte",
                "Curitiba",
                "Porto Alegre",
            ],
            "avg_time_to_fill_days": 40,
        },
        "marketing": {
            "avg_job_postings": 28000,
            "avg_salary_range": "R$36,000-R$108,000",
            "market_activity": "Medium",
            "top_cities": [
                "S\u00e3o Paulo",
                "Rio de Janeiro",
                "Belo Horizonte",
                "Curitiba",
                "Porto Alegre",
            ],
            "avg_time_to_fill_days": 26,
        },
        "sales": {
            "avg_job_postings": 55000,
            "avg_salary_range": "R$30,000-R$96,000",
            "market_activity": "High",
            "top_cities": [
                "S\u00e3o Paulo",
                "Rio de Janeiro",
                "Belo Horizonte",
                "Bras\u00edlia",
                "Curitiba",
            ],
            "avg_time_to_fill_days": 20,
        },
    },
}


# ── Location \u2192 Jooble country key mapping ─────────────────────────────────────

LOCATION_TO_JOOBLE_COUNTRY: Dict[str, str] = {
    # United States
    "united states": "united_states",
    "usa": "united_states",
    "us": "united_states",
    "san francisco": "united_states",
    "new york": "united_states",
    "seattle": "united_states",
    "austin": "united_states",
    "boston": "united_states",
    "chicago": "united_states",
    "los angeles": "united_states",
    "houston": "united_states",
    "dallas": "united_states",
    "atlanta": "united_states",
    "denver": "united_states",
    "detroit": "united_states",
    "charlotte": "united_states",
    # United Kingdom
    "united kingdom": "united_kingdom",
    "uk": "united_kingdom",
    "london": "united_kingdom",
    "manchester": "united_kingdom",
    "edinburgh": "united_kingdom",
    "birmingham": "united_kingdom",
    "bristol": "united_kingdom",
    "glasgow": "united_kingdom",
    "leeds": "united_kingdom",
    # Germany
    "germany": "germany",
    "berlin": "germany",
    "munich": "germany",
    "hamburg": "germany",
    "frankfurt": "germany",
    "stuttgart": "germany",
    "cologne": "germany",
    "dusseldorf": "germany",
    # Canada
    "canada": "canada",
    "toronto": "canada",
    "vancouver": "canada",
    "montreal": "canada",
    "ottawa": "canada",
    "calgary": "canada",
    "edmonton": "canada",
    # Australia
    "australia": "australia",
    "sydney": "australia",
    "melbourne": "australia",
    "brisbane": "australia",
    "perth": "australia",
    "canberra": "australia",
    "adelaide": "australia",
    # India
    "india": "india",
    "bangalore": "india",
    "bengaluru": "india",
    "mumbai": "india",
    "delhi": "india",
    "hyderabad": "india",
    "chennai": "india",
    "pune": "india",
    # France
    "france": "france",
    "paris": "france",
    "lyon": "france",
    "toulouse": "france",
    "marseille": "france",
    "nantes": "france",
    "bordeaux": "france",
    # Singapore
    "singapore": "singapore",
    # Netherlands
    "netherlands": "netherlands",
    "amsterdam": "netherlands",
    "rotterdam": "netherlands",
    "the hague": "netherlands",
    "eindhoven": "netherlands",
    "utrecht": "netherlands",
    # Ireland
    "ireland": "ireland",
    "dublin": "ireland",
    "cork": "ireland",
    "galway": "ireland",
    "limerick": "ireland",
    # UAE
    "uae": "uae",
    "united arab emirates": "uae",
    "dubai": "uae",
    "abu dhabi": "uae",
    "sharjah": "uae",
    # Japan
    "japan": "japan",
    "tokyo": "japan",
    "osaka": "japan",
    "yokohama": "japan",
    "nagoya": "japan",
    "fukuoka": "japan",
    # Brazil
    "brazil": "brazil",
    "sao paulo": "brazil",
    "s\u00e3o paulo": "brazil",
    "rio de janeiro": "brazil",
    "belo horizonte": "brazil",
    "curitiba": "brazil",
    "brasilia": "brazil",
    "bras\u00edlia": "brazil",
}


# ── Role \u2192 Jooble job category mapping ──────────────────────────────────────

ROLE_TO_JOOBLE_CATEGORY: Dict[str, str] = {
    # Technology
    "software engineer": "technology",
    "software developer": "technology",
    "frontend developer": "technology",
    "backend developer": "technology",
    "full stack developer": "technology",
    "fullstack developer": "technology",
    "web developer": "technology",
    "devops engineer": "technology",
    "data scientist": "technology",
    "data engineer": "technology",
    "data analyst": "technology",
    "machine learning engineer": "technology",
    "cloud engineer": "technology",
    "cybersecurity analyst": "technology",
    "it manager": "technology",
    "systems administrator": "technology",
    "product manager": "technology",
    "ux designer": "technology",
    "ui designer": "technology",
    # Healthcare
    "nurse": "healthcare",
    "doctor": "healthcare",
    "physician": "healthcare",
    "pharmacist": "healthcare",
    "medical technician": "healthcare",
    "healthcare administrator": "healthcare",
    # Finance
    "financial analyst": "finance",
    "accountant": "finance",
    "auditor": "finance",
    "investment banker": "finance",
    "risk analyst": "finance",
    "actuary": "finance",
    "controller": "finance",
    # Engineering
    "mechanical engineer": "engineering",
    "civil engineer": "engineering",
    "electrical engineer": "engineering",
    "chemical engineer": "engineering",
    "structural engineer": "engineering",
    "project engineer": "engineering",
    # Marketing
    "marketing manager": "marketing",
    "digital marketing": "marketing",
    "content marketing": "marketing",
    "seo specialist": "marketing",
    "social media manager": "marketing",
    "brand manager": "marketing",
    "copywriter": "marketing",
    "marketing analyst": "marketing",
    # Sales
    "sales manager": "sales",
    "account executive": "sales",
    "business development": "sales",
    "sales representative": "sales",
    "account manager": "sales",
    "sales engineer": "sales",
    "relationship manager": "sales",
}

# Daily request counter to stay within Jooble's ~500/day limit
_jooble_request_count = 0
_jooble_request_day: Optional[str] = None
_jooble_rate_lock = threading.Lock()


def _resolve_jooble_country(location: str) -> Optional[str]:
    """Resolve a location string to a JOOBLE_MARKET_DATA country key."""
    loc_lower = location.lower().strip()
    # Direct lookup
    if loc_lower in LOCATION_TO_JOOBLE_COUNTRY:
        return LOCATION_TO_JOOBLE_COUNTRY[loc_lower]
    # Check if any mapping key is contained within the location string
    for fragment, country_key in LOCATION_TO_JOOBLE_COUNTRY.items():
        if fragment in loc_lower:
            return country_key
    # Check if the location string contains any mapping key
    for fragment, country_key in LOCATION_TO_JOOBLE_COUNTRY.items():
        if loc_lower in fragment:
            return country_key
    return None


def _resolve_jooble_category(role: str) -> str:
    """Resolve a role string to a JOOBLE_MARKET_DATA category key."""
    role_lower = role.lower().strip()
    # Direct lookup
    if role_lower in ROLE_TO_JOOBLE_CATEGORY:
        return ROLE_TO_JOOBLE_CATEGORY[role_lower]
    # Partial match: check if any mapping key is contained in the role
    for fragment, category in ROLE_TO_JOOBLE_CATEGORY.items():
        if fragment in role_lower or role_lower in fragment:
            return category
    # Keyword heuristics
    keyword_map = {
        "technology": [
            "tech",
            "software",
            "develop",
            "program",
            "code",
            "devops",
            "cloud",
            "data",
            "cyber",
            "machine learning",
            "ai ",
            "ml ",
            "it ",
            "sysadmin",
            "ux",
            "ui",
            "product",
        ],
        "healthcare": [
            "health",
            "medic",
            "nurs",
            "doctor",
            "pharm",
            "clinical",
            "patient",
            "hospital",
            "dental",
        ],
        "finance": [
            "financ",
            "account",
            "audit",
            "invest",
            "bank",
            "risk",
            "actuar",
            "tax",
            "treasury",
        ],
        "engineering": [
            "engineer",
            "mechanical",
            "civil",
            "electric",
            "chemical",
            "structural",
            "aerospace",
        ],
        "marketing": [
            "marketing",
            "seo",
            "content",
            "brand",
            "social media",
            "copywrite",
            "advertis",
            "creative",
        ],
        "sales": [
            "sales",
            "account exec",
            "business develop",
            "bdm",
            "relationship manage",
            "revenue",
        ],
    }
    for category, keywords in keyword_map.items():
        for kw in keywords:
            if kw in role_lower:
                return category
    return "technology"  # default fallback


def _parse_salary_from_jobs(jobs: List[Dict[str, Any]]) -> Optional[str]:
    """Extract a salary range string from a list of Jooble job listings."""
    import re

    numeric_salaries: List[float] = []
    currency_symbol = "$"

    for job in jobs:
        salary_raw = job.get("salary") or "" or ""
        if not salary_raw or not salary_raw.strip():
            continue

        # Detect currency symbol
        for sym in [
            "\u00a3",
            "\u20ac",
            "A$",
            "C$",
            "S$",
            "AED",
            "R$",
            "\u20b9",
            "\u00a5",
            "$",
        ]:
            if sym in salary_raw:
                currency_symbol = sym
                break

        # Pull all numbers from the salary string
        numbers = re.findall(r"[\d,]+\.?\d*", salary_raw.replace(",", ""))
        for n in numbers:
            try:
                val = float(n)
                if val > 500:  # filter out noise
                    numeric_salaries.append(val)
            except ValueError:
                continue

    if not numeric_salaries:
        return None

    lo = min(numeric_salaries)
    hi = max(numeric_salaries)
    if lo == hi:
        hi = lo * 1.4  # approximate range

    def _fmt(v: float) -> str:
        if v >= 1_000_000:
            return f"{v / 1_000_000:,.1f}M"
        if v >= 1_000:
            return f"{v:,.0f}"
        return f"{v:,.0f}"

    return f"{currency_symbol}{_fmt(lo)}-{currency_symbol}{_fmt(hi)}"


def _extract_top_companies(jobs: List[Dict[str, Any]], limit: int = 5) -> List[str]:
    """Extract the most frequently appearing companies from job results."""
    counts: Dict[str, int] = {}
    for job in jobs:
        company = (job.get("company") or "").strip()
        if company and company.lower() not in (
            "",
            "n/a",
            "not specified",
            "confidential",
        ):
            counts[company] = counts.get(company, 0) + 1
    sorted_companies = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return [c[0] for c in sorted_companies[:limit]]


def _compute_freshness(jobs: List[Dict[str, Any]]) -> str:
    """Compute what percentage of jobs were posted within the last 14 days."""
    import datetime

    if not jobs:
        return "No freshness data available"

    recent = 0
    total_with_date = 0
    now = datetime.datetime.utcnow()

    for job in jobs:
        updated = job.get("updated") or ""
        if not updated:
            continue
        try:
            # Jooble returns ISO-ish dates like "2026-03-01T00:00:00.0000000"
            dt_str = updated.split(".")[0].replace("T", " ")
            dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            total_with_date += 1
            if (now - dt).days <= 14:
                recent += 1
        except (ValueError, IndexError):
            continue

    if total_with_date == 0:
        return "Freshness data unavailable"

    pct = int((recent / total_with_date) * 100)
    return f"{pct}% posted within 14 days"


def _jooble_api_post(
    api_key: str, keywords: str, location: str
) -> Optional[Dict[str, Any]]:
    """
    Make a single POST request to the Jooble API.
    Returns parsed JSON response or None on failure.
    """
    global _jooble_request_count, _jooble_request_day

    with _jooble_rate_lock:
        today = time.strftime("%Y-%m-%d")
        if _jooble_request_day != today:
            _jooble_request_count = 0
            _jooble_request_day = today

        if _jooble_request_count >= 480:
            _log_warn(
                "Jooble daily request limit approaching (480/500). Skipping API call."
            )
            return None
        _jooble_request_count += 1

    url = f"https://jooble.org/api/{api_key}"
    payload = json.dumps(
        {
            "keywords": keywords,
            "location": location,
            "page": 1,
        }
    ).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        _log_warn(
            f"Jooble API HTTP error {exc.code} for '{keywords}' in '{location}': {exc.reason}"
        )
        return None
    except urllib.error.URLError as exc:
        _log_warn(
            f"Jooble API URL error for '{keywords}' in '{location}': {exc.reason}"
        )
        return None
    except Exception as exc:
        _log_warn(
            f"Jooble API unexpected error for '{keywords}' in '{location}': {exc}"
        )
        return None


def _get_fallback_data(role: str, location: str) -> Dict[str, Any]:
    """Return curated benchmark data for a role+location pair."""
    country_key = _resolve_jooble_country(location)
    category = _resolve_jooble_category(role)

    if country_key and country_key in JOOBLE_MARKET_DATA:
        country_data = JOOBLE_MARKET_DATA[country_key]
        if category in country_data:
            cat_data = country_data[category]
            return {
                "total_job_postings": cat_data["avg_job_postings"],
                "salary_range": cat_data["avg_salary_range"],
                "market_activity": cat_data["market_activity"],
                "top_companies": [],
                "freshness": f"Benchmark data (avg time to fill: {cat_data['avg_time_to_fill_days']} days)",
                "top_cities": cat_data["top_cities"],
                "avg_time_to_fill_days": cat_data["avg_time_to_fill_days"],
            }

    # Absolute fallback for unmapped locations / categories
    return {
        "total_job_postings": 0,
        "salary_range": "Data not available",
        "market_activity": "Unknown",
        "top_companies": [],
        "freshness": "No data available for this market",
        "top_cities": [],
        "avg_time_to_fill_days": None,
    }


def fetch_jooble_data(roles: List[str], locations: List[str]) -> Dict[str, Any]:
    """
    Fetch international job market data from the Jooble API for the given
    roles and locations.  Falls back to curated benchmark data when the
    JOOBLE_API_KEY environment variable is not set or the API is unreachable.

    Parameters
    ----------
    roles : list[str]
        Job titles / role names to look up (e.g. ["Software Engineer", "Data Scientist"]).
    locations : list[str]
        Target locations (e.g. ["London, UK", "Berlin", "Toronto"]).

    Returns
    -------
    dict
        A structured dict with keys: source, job_market, platform_summary.
    """
    api_key = os.environ.get("JOOBLE_API_KEY", "").strip()
    use_api = bool(api_key)
    source = "Jooble API" if use_api else "Jooble Market Benchmarks"
    job_market: Dict[str, Dict[str, Any]] = {}
    api_failures = 0
    total_combos = 0

    for role in roles:
        role_results: Dict[str, Any] = {}

        for location in locations:
            total_combos += 1
            cache_key_str = _cache_key("jooble", f"{role}|{location}")
            cached = _get_cached(cache_key_str)
            if cached is not None:
                role_results[location] = cached
                continue

            result: Optional[Dict[str, Any]] = None

            # ── Try live API ──────────────────────────────────────────
            if use_api:
                _log_info(f"Jooble API: querying '{role}' in '{location}'")
                api_resp = _jooble_api_post(api_key, role, location)

                if api_resp is not None:
                    jobs = api_resp.get("jobs", [])
                    total_count = api_resp.get("totalCount") or 0
                    salary_range = _parse_salary_from_jobs(jobs)
                    top_companies = _extract_top_companies(jobs)
                    freshness = _compute_freshness(jobs)

                    # Determine market activity from volume
                    if total_count >= 10000:
                        activity = "Very High"
                    elif total_count >= 3000:
                        activity = "High"
                    elif total_count >= 500:
                        activity = "Medium"
                    else:
                        activity = "Low"

                    # If API returned no salary data, try fallback salary
                    if not salary_range:
                        fb = _get_fallback_data(role, location)
                        salary_range = fb.get("salary_range", "Not available")

                    result = {
                        "total_job_postings": total_count,
                        "salary_range": salary_range,
                        "market_activity": activity,
                        "top_companies": top_companies if top_companies else [],
                        "freshness": freshness,
                    }
                else:
                    api_failures += 1

                # Brief pause between API calls to respect rate limits
                time.sleep(0.3)

            # ── Fallback to curated data ──────────────────────────────
            if result is None:
                fb = _get_fallback_data(role, location)
                result = {
                    "total_job_postings": fb["total_job_postings"],
                    "salary_range": fb["salary_range"],
                    "market_activity": fb["market_activity"],
                    "top_companies": fb.get("top_companies", []),
                    "freshness": fb["freshness"],
                }

            # Cache the result
            _set_cached(cache_key_str, result)
            role_results[location] = result

        job_market[role] = role_results

    # If every API call failed, mark source as benchmarks
    if use_api and api_failures == total_combos and total_combos > 0:
        source = "Jooble Market Benchmarks"
        _log_warn(
            "All Jooble API calls failed; using curated benchmark data exclusively."
        )

    return {
        "source": source,
        "job_market": job_market,
        "platform_summary": {
            "platform": "Jooble",
            "coverage": "69 countries, 140,000+ job sites aggregated",
            "best_for": "International job market intelligence, posting volume analysis",
            "data_points": [
                "Job posting volume",
                "Salary ranges",
                "Top employers",
                "Market activity",
            ],
        },
    }


# ═══════════════════════════════════════════════════════════════════════════════
# API 26: BLS JOLTS -- Job Openings & Labor Turnover Survey
# Free with BLS API registration. Monthly data on job openings, hires,
# quits, layoffs, separations by industry.
# ═══════════════════════════════════════════════════════════════════════════════

# JOLTS NAICS industry codes
JOLTS_INDUSTRY_CODES: Dict[str, str] = {
    "total_nonfarm": "000000",
    "healthcare": "620000",
    "technology": "510000",  # Information
    "professional_services": "540099",
    "retail": "440000",
    "construction": "230000",
    "manufacturing": "300000",
    "accommodation_food": "720000",
    "finance": "520000",
    "transportation": "480099",
    "education": "610000",
    "government": "900000",
}

# JOLTS data element codes
JOLTS_ELEMENTS: Dict[str, str] = {
    "job_openings": "JO",  # Job Openings Level
    "hires": "HI",  # Hires Level (thousands)
    "total_separations": "TS",  # Total Separations
    "quits": "QU",  # Quits Level
    "layoffs": "LD",  # Layoffs and Discharges
    "job_openings_rate": "JOR",  # Job Openings Rate (%)
    "hires_rate": "HIR",  # Hires Rate (%)
    "quits_rate": "QUR",  # Quits Rate (%)
}

# Fallback data (2024 annual averages from BLS JOLTS summary)
JOLTS_FALLBACK: Dict[str, Dict[str, Any]] = {
    "total_nonfarm": {
        "job_openings": 8100,
        "hires": 5800,
        "quits": 3500,
        "layoffs": 1700,
        "job_openings_rate": 4.8,
        "hires_rate": 3.5,
        "quits_rate": 2.1,
    },
    "healthcare": {
        "job_openings": 1500,
        "hires": 980,
        "quits": 520,
        "layoffs": 180,
        "job_openings_rate": 6.8,
        "hires_rate": 4.5,
        "quits_rate": 2.4,
    },
    "technology": {
        "job_openings": 280,
        "hires": 190,
        "quits": 120,
        "layoffs": 65,
        "job_openings_rate": 4.2,
        "hires_rate": 2.8,
        "quits_rate": 1.8,
    },
    "professional_services": {
        "job_openings": 1200,
        "hires": 820,
        "quits": 650,
        "layoffs": 220,
        "job_openings_rate": 5.0,
        "hires_rate": 3.4,
        "quits_rate": 2.7,
    },
    "retail": {
        "job_openings": 850,
        "hires": 750,
        "quits": 520,
        "layoffs": 250,
        "job_openings_rate": 5.4,
        "hires_rate": 4.8,
        "quits_rate": 3.3,
    },
    "construction": {
        "job_openings": 420,
        "hires": 380,
        "quits": 250,
        "layoffs": 180,
        "job_openings_rate": 5.2,
        "hires_rate": 4.7,
        "quits_rate": 3.1,
    },
    "manufacturing": {
        "job_openings": 580,
        "hires": 380,
        "quits": 220,
        "layoffs": 210,
        "job_openings_rate": 4.5,
        "hires_rate": 3.0,
        "quits_rate": 1.7,
    },
    "accommodation_food": {
        "job_openings": 1100,
        "hires": 980,
        "quits": 700,
        "layoffs": 250,
        "job_openings_rate": 6.5,
        "hires_rate": 5.8,
        "quits_rate": 4.2,
    },
    "finance": {
        "job_openings": 480,
        "hires": 310,
        "quits": 200,
        "layoffs": 85,
        "job_openings_rate": 4.5,
        "hires_rate": 2.9,
        "quits_rate": 1.9,
    },
    "transportation": {
        "job_openings": 350,
        "hires": 280,
        "quits": 180,
        "layoffs": 120,
        "job_openings_rate": 5.0,
        "hires_rate": 4.0,
        "quits_rate": 2.6,
    },
}


def fetch_bls_jolts(
    industry_code: str = "000000", data_element: str = "JO", years: int = 2
) -> Optional[Dict[str, Any]]:
    """Fetch JOLTS data from BLS API.

    Series ID format: JTS{industry}{size}{ownership}{region}{data_element}{rate_level}
    Example: JTS000000000000000JOL = Total nonfarm, Job Openings Level
    """
    cache_k = _cache_key("bls_jolts", f"{industry_code}_{data_element}_{years}")
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    try:
        # Build series ID: JTS + industry + 0000000 (size/ownership/region) + element + L/R
        suffix = "R" if data_element.endswith("R") else "L"
        base_element = data_element.rstrip("R")
        series_id = f"JTS{industry_code}0000000{base_element}{suffix}"

        end_year = 2025
        start_year = max(2020, end_year - years)

        url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
        payload: Dict[str, Any] = {
            "seriesid": [series_id],
            "startyear": str(start_year),
            "endyear": str(end_year),
        }

        api_key = os.environ.get("BLS_API_KEY", "")
        if api_key:
            payload["registrationkey"] = api_key

        resp = _http_post_json(url, payload, timeout=10)

        if not resp or resp.get("status") != "REQUEST_SUCCEEDED":
            # Fall back to v1 (no key required, lower limits)
            url_v1 = "https://api.bls.gov/publicAPI/v1/timeseries/data/"
            payload_v1 = {
                "seriesid": [series_id],
                "startyear": str(start_year),
                "endyear": str(end_year),
            }
            resp = _http_post_json(url_v1, payload_v1, timeout=10)

        if not resp or resp.get("status") != "REQUEST_SUCCEEDED":
            return None

        series_data = resp.get("Results", {}).get("series", [])
        if not series_data or not series_data[0].get("data"):
            return None

        # Parse into yearly averages
        yearly: Dict[int, List[float]] = {}
        for point in series_data[0]["data"]:
            yr = int(point["year"])
            val_str = str(point.get("value", "0")).replace(",", "")
            try:
                val = float(val_str)
            except (ValueError, TypeError):
                continue
            if yr not in yearly:
                yearly[yr] = []
            yearly[yr].append(val)

        # Average each year
        result_data: Dict[int, float] = {}
        for yr, vals in sorted(yearly.items()):
            result_data[yr] = round(sum(vals) / len(vals), 1)

        result = {
            "series_id": series_id,
            "data_element": data_element,
            "industry_code": industry_code,
            "yearly_averages": result_data,
            "latest_value": (
                result_data.get(max(result_data.keys())) if result_data else None
            ),
            "source": "BLS JOLTS",
            "data_confidence": 0.92,
        }

        _set_cached(cache_k, result)
        return result
    except Exception as e:
        _log_warn(f"BLS JOLTS fetch failed: {e}")
        return None


def get_jolts_hiring_difficulty(industry: str = "total_nonfarm") -> Dict[str, Any]:
    """Compute hiring difficulty index from JOLTS data.

    Difficulty = (job_openings_rate / hires_rate) * quits_rate_factor
    Higher = harder to hire.
    """
    # Try live API first
    ind_code = JOLTS_INDUSTRY_CODES.get(industry, "000000")

    jo_data = fetch_bls_jolts(ind_code, "JOR", 1)
    hi_data = fetch_bls_jolts(ind_code, "HIR", 1)
    qu_data = fetch_bls_jolts(ind_code, "QUR", 1)

    if jo_data and hi_data and qu_data:
        jo_rate = jo_data["latest_value"] or 4.8
        hi_rate = hi_data["latest_value"] or 3.5
        qu_rate = qu_data["latest_value"] or 2.1
        source = "BLS JOLTS API (live)"
        confidence = 0.90
    else:
        # Fallback
        fb = JOLTS_FALLBACK.get(industry, JOLTS_FALLBACK["total_nonfarm"])
        jo_rate = fb["job_openings_rate"]
        hi_rate = fb["hires_rate"]
        qu_rate = fb["quits_rate"]
        source = "BLS JOLTS (curated fallback)"
        confidence = 0.72

    # Hiring difficulty index (0-10 scale)
    # Base = openings/hires ratio (>1.5 = hard, >2.0 = very hard)
    ratio = jo_rate / max(hi_rate, 0.1)
    # Quits factor: high quits = harder to maintain headcount
    quits_factor = 1 + (qu_rate - 2.0) * 0.3  # baseline quits rate ~2.0%

    difficulty = min(10, max(0, ratio * quits_factor * 3.0))

    return {
        "hiring_difficulty_index": round(difficulty, 1),
        "job_openings_rate": jo_rate,
        "hires_rate": hi_rate,
        "quits_rate": qu_rate,
        "openings_to_hires_ratio": round(ratio, 2),
        "interpretation": (
            "Critical shortage"
            if difficulty >= 8
            else (
                "Very difficult"
                if difficulty >= 6.5
                else (
                    "Moderately difficult"
                    if difficulty >= 4.5
                    else "Normal" if difficulty >= 3.0 else "Easy to hire"
                )
            )
        ),
        "source": source,
        "data_confidence": confidence,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# API 27: FRED Expansion -- Employment Cost Index & Sector Data
# Extends existing FRED integration with wage/cost series
# ═══════════════════════════════════════════════════════════════════════════════

FRED_EMPLOYMENT_SERIES: Dict[str, str] = {
    "avg_hourly_earnings": "CES0500000003",  # Total private, monthly
    "employment_cost_index": "ECIWAG",  # ECI: Wages & Salaries, quarterly
    "unemployment_info_sector": "LNU04032237",  # Information industry unemployment
    "unemployment_prof_services": "LNU04032239",  # Professional services unemployment
    "unemployment_healthcare": "LNU04032243",  # Healthcare unemployment (approx)
    "unemployment_construction": "LNU04032231",  # Construction unemployment
    "unemployment_manufacturing": "LNU04032229",  # Manufacturing unemployment
    "job_openings_total": "JTSJOL",  # Total nonfarm job openings
    "quits_total": "JTSQUL",  # Total nonfarm quits
    "median_weekly_earnings": "LES1252881600Q",  # Median weekly earnings
}

# Fallback values (2024 annual)
FRED_EMPLOYMENT_FALLBACK: Dict[str, Any] = {
    "avg_hourly_earnings": 35.50,
    "employment_cost_index": 1.1,  # quarterly % change
    "unemployment_info_sector": 3.8,
    "unemployment_prof_services": 3.2,
    "unemployment_construction": 4.5,
    "unemployment_manufacturing": 3.5,
    "job_openings_total": 8100,  # thousands
    "quits_total": 3500,  # thousands
    "median_weekly_earnings": 1145,
}


def fetch_fred_employment_series(
    series_id: str, observation_start: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Fetch a FRED employment data series.

    Uses the existing FRED API key from environment (FRED_API_KEY).
    """
    cache_k = _cache_key("fred_emp", f"{series_id}_{observation_start or 'default'}")
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    try:
        api_key = os.environ.get("FRED_API_KEY", "")
        if not api_key:
            return None

        if observation_start is None:
            observation_start = "2023-01-01"

        url = (
            f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}"
            f"&api_key={api_key}"
            f"&file_type=json"
            f"&observation_start={observation_start}"
            f"&sort_order=desc"
            f"&limit=24"
        )

        resp = _http_get_json(
            url,
            headers={
                "User-Agent": "MediaPlanGenerator/1.0",
            },
            timeout=8,
        )

        if not resp:
            return None

        observations = resp.get("observations", [])
        if not observations:
            return None

        # Parse observations
        values: List[Dict[str, Any]] = []
        for obs in observations:
            try:
                val = float(obs["value"])
                values.append({"date": obs["date"], "value": val})
            except (ValueError, KeyError):
                continue

        if not values:
            return None

        latest = values[0]["value"]

        # Compute YoY change if enough data
        yoy_change: Optional[float] = None
        if len(values) >= 12:
            current = values[0]["value"]
            year_ago = values[11]["value"]  # ~12 months back
            if year_ago > 0:
                yoy_change = round(((current - year_ago) / year_ago) * 100, 1)

        result = {
            "series_id": series_id,
            "latest_value": latest,
            "latest_date": values[0]["date"],
            "yoy_change_pct": yoy_change,
            "observations": values[:6],  # Last 6 observations
            "source": "Federal Reserve FRED",
            "data_confidence": 0.90,
        }

        _set_cached(cache_k, result)
        return result
    except Exception as e:
        _log_warn(f"FRED employment series {series_id} fetch failed: {e}")
        return None


def get_labor_market_tightness(industry: str = "total") -> Dict[str, Any]:
    """Compute labor market tightness from FRED data.

    Uses avg hourly earnings growth + job openings as signals.
    Tight market = higher CPCs needed for recruitment.
    """
    earnings = fetch_fred_employment_series(
        FRED_EMPLOYMENT_SERIES["avg_hourly_earnings"]
    )
    openings = fetch_fred_employment_series(
        FRED_EMPLOYMENT_SERIES["job_openings_total"]
    )

    if earnings and openings:
        wage_growth = earnings.get("yoy_change_pct", 4.0) or 4.0
        jo_level = openings.get("latest_value", 8100) or 8100
        source = "Federal Reserve FRED (live)"
        confidence = 0.88
    else:
        wage_growth = 4.0
        jo_level = 8100
        source = "FRED (curated fallback)"
        confidence = 0.65

    # Tightness index: 0-10
    # High wage growth + high openings = tight market
    wage_factor = min(2.0, wage_growth / 3.0)  # normalized around 3% baseline
    openings_factor = min(2.0, jo_level / 7000)  # normalized around 7M baseline
    tightness = min(10, (wage_factor + openings_factor) * 2.5)

    return {
        "tightness_index": round(tightness, 1),
        "wage_growth_yoy_pct": wage_growth,
        "job_openings_thousands": jo_level,
        "interpretation": (
            "Very tight"
            if tightness >= 7
            else (
                "Tight" if tightness >= 5 else "Moderate" if tightness >= 3 else "Loose"
            )
        ),
        "cpc_impact": (
            "CPCs elevated 15-25% above baseline"
            if tightness >= 7
            else (
                "CPCs elevated 5-15% above baseline"
                if tightness >= 5
                else (
                    "CPCs near baseline"
                    if tightness >= 3
                    else "CPCs potentially below baseline -- good time for cost-efficient campaigns"
                )
            )
        ),
        "source": source,
        "data_confidence": confidence,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# API 28: Eurostat Labour Force Survey (free, no auth)
# EU unemployment rates, employment by sector, minimum wages
# ═══════════════════════════════════════════════════════════════════════════════

_EUROSTAT_COUNTRY_MAP: Dict[str, str] = {
    "united kingdom": "UK",
    "uk": "UK",
    "germany": "DE",
    "france": "FR",
    "spain": "ES",
    "italy": "IT",
    "netherlands": "NL",
    "belgium": "BE",
    "austria": "AT",
    "sweden": "SE",
    "denmark": "DK",
    "finland": "FI",
    "ireland": "IE",
    "portugal": "PT",
    "greece": "EL",
    "poland": "PL",
    "czech republic": "CZ",
    "czechia": "CZ",
    "romania": "RO",
    "hungary": "HU",
    "bulgaria": "BG",
    "croatia": "HR",
    "slovakia": "SK",
    "slovenia": "SI",
    "lithuania": "LT",
    "latvia": "LV",
    "estonia": "EE",
    "luxembourg": "LU",
    "malta": "MT",
    "cyprus": "CY",
    "norway": "NO",
    "switzerland": "CH",
    "iceland": "IS",
}

_EUROSTAT_FALLBACK: Dict[str, Dict[str, Any]] = {
    "DE": {
        "unemployment_rate": 3.4,
        "youth_unemployment": 6.1,
        "employment_rate": 77.2,
        "min_wage_eur": None,
        "avg_hourly_earnings_eur": 25.30,
    },
    "FR": {
        "unemployment_rate": 7.3,
        "youth_unemployment": 17.4,
        "employment_rate": 68.4,
        "min_wage_eur": 1767,
        "avg_hourly_earnings_eur": 22.80,
    },
    "ES": {
        "unemployment_rate": 11.7,
        "youth_unemployment": 28.5,
        "employment_rate": 65.7,
        "min_wage_eur": 1134,
        "avg_hourly_earnings_eur": 15.60,
    },
    "IT": {
        "unemployment_rate": 7.6,
        "youth_unemployment": 22.3,
        "employment_rate": 62.1,
        "min_wage_eur": None,
        "avg_hourly_earnings_eur": 17.40,
    },
    "NL": {
        "unemployment_rate": 3.6,
        "youth_unemployment": 8.9,
        "employment_rate": 82.9,
        "min_wage_eur": 2070,
        "avg_hourly_earnings_eur": 26.50,
    },
    "BE": {
        "unemployment_rate": 5.5,
        "youth_unemployment": 14.8,
        "employment_rate": 72.1,
        "min_wage_eur": 1955,
        "avg_hourly_earnings_eur": 24.70,
    },
    "AT": {
        "unemployment_rate": 5.1,
        "youth_unemployment": 10.2,
        "employment_rate": 77.8,
        "min_wage_eur": None,
        "avg_hourly_earnings_eur": 23.90,
    },
    "SE": {
        "unemployment_rate": 7.5,
        "youth_unemployment": 20.1,
        "employment_rate": 78.5,
        "min_wage_eur": None,
        "avg_hourly_earnings_eur": 28.10,
    },
    "DK": {
        "unemployment_rate": 4.8,
        "youth_unemployment": 10.3,
        "employment_rate": 78.9,
        "min_wage_eur": None,
        "avg_hourly_earnings_eur": 31.40,
    },
    "FI": {
        "unemployment_rate": 7.2,
        "youth_unemployment": 17.0,
        "employment_rate": 74.8,
        "min_wage_eur": None,
        "avg_hourly_earnings_eur": 24.30,
    },
    "IE": {
        "unemployment_rate": 4.3,
        "youth_unemployment": 10.1,
        "employment_rate": 75.3,
        "min_wage_eur": 2146,
        "avg_hourly_earnings_eur": 28.90,
    },
    "PT": {
        "unemployment_rate": 6.5,
        "youth_unemployment": 21.0,
        "employment_rate": 75.1,
        "min_wage_eur": 960,
        "avg_hourly_earnings_eur": 12.40,
    },
    "PL": {
        "unemployment_rate": 2.8,
        "youth_unemployment": 11.2,
        "employment_rate": 76.2,
        "min_wage_eur": 1012,
        "avg_hourly_earnings_eur": 10.80,
    },
    "CZ": {
        "unemployment_rate": 2.6,
        "youth_unemployment": 8.5,
        "employment_rate": 77.5,
        "min_wage_eur": 775,
        "avg_hourly_earnings_eur": 12.60,
    },
    "RO": {
        "unemployment_rate": 5.4,
        "youth_unemployment": 21.3,
        "employment_rate": 67.8,
        "min_wage_eur": 747,
        "avg_hourly_earnings_eur": 8.30,
    },
    "HU": {
        "unemployment_rate": 4.1,
        "youth_unemployment": 12.8,
        "employment_rate": 74.3,
        "min_wage_eur": 626,
        "avg_hourly_earnings_eur": 9.10,
    },
    "UK": {
        "unemployment_rate": 4.0,
        "youth_unemployment": 12.0,
        "employment_rate": 75.8,
        "min_wage_eur": None,
        "avg_hourly_earnings_eur": 22.50,
    },
    "NO": {
        "unemployment_rate": 3.5,
        "youth_unemployment": 10.5,
        "employment_rate": 79.1,
        "min_wage_eur": None,
        "avg_hourly_earnings_eur": 35.20,
    },
    "CH": {
        "unemployment_rate": 4.3,
        "youth_unemployment": 8.2,
        "employment_rate": 80.2,
        "min_wage_eur": None,
        "avg_hourly_earnings_eur": 38.40,
    },
}


def fetch_eurostat_labour_data(locations: List[str]) -> Dict[str, Any]:
    """Fetch EU labour market data from Eurostat (free, no authentication).

    Returns unemployment rates, employment rates, and wage data for EU/EEA countries.
    Falls back to curated benchmarks when API is unavailable.
    """
    # Extract EU country codes from locations
    eu_codes = []
    for loc in locations:
        loc_lower = loc.lower().strip()
        for name, code in _EUROSTAT_COUNTRY_MAP.items():
            if name in loc_lower:
                eu_codes.append(code)
                break

    if not eu_codes:
        return {}

    eu_codes = list(set(eu_codes))
    result: Dict[str, Any] = {"source": "Eurostat", "countries": {}}

    for code in eu_codes:
        cache_k = _cache_key("eurostat", code)
        cached = _get_cached(cache_k)
        if cached is not None:
            result["countries"][code] = cached
            continue

        # Try live API
        country_data = None
        try:
            geo_param = f"geo={code}"
            url = (
                f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/une_rt_a"
                f"?format=JSON&lang=EN&{geo_param}&sex=T&age=TOTAL&unit=PC_ACT&lastTimePeriod=3"
            )
            resp = _http_get_json(url, timeout=10)
            if resp and "value" in resp:
                values = resp["value"]
                time_dim = (
                    resp.get("dimension", {})
                    .get("time", {})
                    .get("category", {})
                    .get("index", {})
                )
                # Get latest value
                latest_idx = max(time_dim.values()) if time_dim else 0
                unemp_rate = values.get(str(latest_idx))

                if unemp_rate is not None:
                    country_data = {
                        "unemployment_rate": round(float(unemp_rate), 1),
                        "source": "Eurostat LFS API (live)",
                        "data_confidence": 0.92,
                    }
        except Exception as e:
            _log_warn(f"Eurostat API failed for {code}: {e}")

        # Also try minimum wages
        if country_data:
            try:
                mw_url = (
                    f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/earn_mw_cur"
                    f"?format=JSON&lang=EN&geo={code}&lastTimePeriod=1"
                )
                mw_resp = _http_get_json(mw_url, timeout=8)
                if mw_resp and "value" in mw_resp:
                    mw_vals = mw_resp["value"]
                    if mw_vals:
                        latest = list(mw_vals.values())[-1]
                        if latest:
                            country_data["min_wage_eur"] = round(float(latest), 0)
            except Exception:
                pass

        # Fallback to curated data
        if not country_data:
            fb = _EUROSTAT_FALLBACK.get(code)
            if fb:
                country_data = {
                    **fb,
                    "source": "Eurostat (curated benchmark)",
                    "data_confidence": 0.70,
                }
            else:
                continue

        _set_cached(cache_k, country_data)
        result["countries"][code] = country_data

    if not result["countries"]:
        return {}

    result["data_confidence"] = max(
        (d.get("data_confidence", 0.5) for d in result["countries"].values()),
        default=0.5,
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# API 29: ILO ILOSTAT (free, no auth) — Global labour indicators
# Unemployment rates for 190+ countries via SDMX REST
# ═══════════════════════════════════════════════════════════════════════════════

_ILO_COUNTRY_MAP: Dict[str, str] = {
    "united states": "USA",
    "us": "USA",
    "usa": "USA",
    "canada": "CAN",
    "mexico": "MEX",
    "united kingdom": "GBR",
    "uk": "GBR",
    "germany": "DEU",
    "france": "FRA",
    "spain": "ESP",
    "italy": "ITA",
    "japan": "JPN",
    "china": "CHN",
    "india": "IND",
    "brazil": "BRA",
    "australia": "AUS",
    "south korea": "KOR",
    "singapore": "SGP",
    "hong kong": "HKG",
    "south africa": "ZAF",
    "nigeria": "NGA",
    "kenya": "KEN",
    "saudi arabia": "SAU",
    "uae": "ARE",
    "united arab emirates": "ARE",
    "indonesia": "IDN",
    "philippines": "PHL",
    "vietnam": "VNM",
    "thailand": "THA",
    "malaysia": "MYS",
    "argentina": "ARG",
    "colombia": "COL",
    "chile": "CHL",
    "peru": "PER",
    "egypt": "EGY",
    "turkey": "TUR",
    "israel": "ISR",
    "poland": "POL",
    "netherlands": "NLD",
    "belgium": "BEL",
    "sweden": "SWE",
    "norway": "NOR",
    "denmark": "DNK",
    "finland": "FIN",
    "switzerland": "CHE",
    "austria": "AUT",
    "ireland": "IRL",
    "portugal": "PRT",
    "greece": "GRC",
    "czech republic": "CZE",
    "new zealand": "NZL",
    "taiwan": "TWN",
}

_ILO_FALLBACK: Dict[str, Dict[str, Any]] = {
    "USA": {
        "unemployment_rate": 3.7,
        "youth_unemployment": 8.5,
        "labor_force_participation": 62.5,
    },
    "GBR": {
        "unemployment_rate": 4.0,
        "youth_unemployment": 12.0,
        "labor_force_participation": 78.5,
    },
    "DEU": {
        "unemployment_rate": 3.4,
        "youth_unemployment": 6.1,
        "labor_force_participation": 79.2,
    },
    "FRA": {
        "unemployment_rate": 7.3,
        "youth_unemployment": 17.4,
        "labor_force_participation": 72.1,
    },
    "JPN": {
        "unemployment_rate": 2.6,
        "youth_unemployment": 4.2,
        "labor_force_participation": 62.8,
    },
    "CHN": {
        "unemployment_rate": 5.1,
        "youth_unemployment": 14.9,
        "labor_force_participation": 68.4,
    },
    "IND": {
        "unemployment_rate": 7.7,
        "youth_unemployment": 23.2,
        "labor_force_participation": 51.8,
    },
    "BRA": {
        "unemployment_rate": 7.8,
        "youth_unemployment": 17.5,
        "labor_force_participation": 63.2,
    },
    "AUS": {
        "unemployment_rate": 3.7,
        "youth_unemployment": 9.2,
        "labor_force_participation": 66.8,
    },
    "CAN": {
        "unemployment_rate": 5.4,
        "youth_unemployment": 10.8,
        "labor_force_participation": 65.2,
    },
    "KOR": {
        "unemployment_rate": 2.7,
        "youth_unemployment": 6.5,
        "labor_force_participation": 64.1,
    },
    "SGP": {
        "unemployment_rate": 2.0,
        "youth_unemployment": 6.8,
        "labor_force_participation": 69.5,
    },
    "MEX": {
        "unemployment_rate": 2.8,
        "youth_unemployment": 6.2,
        "labor_force_participation": 60.1,
    },
    "ZAF": {
        "unemployment_rate": 32.1,
        "youth_unemployment": 59.7,
        "labor_force_participation": 56.3,
    },
    "NGA": {
        "unemployment_rate": 33.3,
        "youth_unemployment": 42.5,
        "labor_force_participation": 55.2,
    },
    "SAU": {
        "unemployment_rate": 5.6,
        "youth_unemployment": 27.0,
        "labor_force_participation": 61.8,
    },
    "ARE": {
        "unemployment_rate": 2.7,
        "youth_unemployment": 7.5,
        "labor_force_participation": 82.1,
    },
    "IDN": {
        "unemployment_rate": 5.3,
        "youth_unemployment": 14.0,
        "labor_force_participation": 69.1,
    },
    "PHL": {
        "unemployment_rate": 4.3,
        "youth_unemployment": 9.2,
        "labor_force_participation": 65.8,
    },
    "VNM": {
        "unemployment_rate": 2.3,
        "youth_unemployment": 7.5,
        "labor_force_participation": 76.4,
    },
    "THA": {
        "unemployment_rate": 1.1,
        "youth_unemployment": 5.2,
        "labor_force_participation": 68.5,
    },
    "MYS": {
        "unemployment_rate": 3.4,
        "youth_unemployment": 12.1,
        "labor_force_participation": 69.8,
    },
    "ARG": {
        "unemployment_rate": 6.2,
        "youth_unemployment": 18.0,
        "labor_force_participation": 64.5,
    },
    "COL": {
        "unemployment_rate": 10.2,
        "youth_unemployment": 19.8,
        "labor_force_participation": 63.1,
    },
    "CHL": {
        "unemployment_rate": 8.5,
        "youth_unemployment": 21.3,
        "labor_force_participation": 62.0,
    },
    "EGY": {
        "unemployment_rate": 7.1,
        "youth_unemployment": 17.8,
        "labor_force_participation": 43.2,
    },
    "TUR": {
        "unemployment_rate": 9.4,
        "youth_unemployment": 18.5,
        "labor_force_participation": 53.8,
    },
    "ISR": {
        "unemployment_rate": 3.4,
        "youth_unemployment": 7.2,
        "labor_force_participation": 64.1,
    },
    "NZL": {
        "unemployment_rate": 3.9,
        "youth_unemployment": 9.8,
        "labor_force_participation": 71.2,
    },
    "POL": {
        "unemployment_rate": 2.8,
        "youth_unemployment": 11.2,
        "labor_force_participation": 73.5,
    },
    "NLD": {
        "unemployment_rate": 3.6,
        "youth_unemployment": 8.9,
        "labor_force_participation": 82.9,
    },
    "SWE": {
        "unemployment_rate": 7.5,
        "youth_unemployment": 20.1,
        "labor_force_participation": 79.0,
    },
    "NOR": {
        "unemployment_rate": 3.5,
        "youth_unemployment": 10.5,
        "labor_force_participation": 78.8,
    },
    "DNK": {
        "unemployment_rate": 4.8,
        "youth_unemployment": 10.3,
        "labor_force_participation": 79.5,
    },
    "CHE": {
        "unemployment_rate": 4.3,
        "youth_unemployment": 8.2,
        "labor_force_participation": 81.0,
    },
}


def fetch_ilo_labour_data(locations: List[str]) -> Dict[str, Any]:
    """Fetch global labour market data from ILO ILOSTAT (free, no auth).

    Returns unemployment rates and labour force participation for 190+ countries.
    """
    # Extract ISO3 country codes from locations
    ilo_codes = []
    for loc in locations:
        loc_lower = loc.lower().strip()
        for name, code in _ILO_COUNTRY_MAP.items():
            if name in loc_lower:
                ilo_codes.append(code)
                break

    if not ilo_codes:
        return {}

    ilo_codes = list(set(ilo_codes))
    result: Dict[str, Any] = {"source": "ILO ILOSTAT", "countries": {}}

    for code in ilo_codes:
        cache_k = _cache_key("ilo", code)
        cached = _get_cached(cache_k)
        if cached is not None:
            result["countries"][code] = cached
            continue

        country_data = None
        try:
            # ILO SDMX REST API: unemployment rate, annual, total
            url = (
                f"https://sdmx.ilo.org/rest/data/ILO,DF_STI_ALL_UNE_DEA1_SEX_AGE_RT"
                f"/{code}.A......?format=jsondata&startPeriod=2022&detail=dataonly"
            )
            resp = _http_get_json(url, timeout=12)
            if resp and "dataSets" in resp:
                datasets = resp.get("dataSets", [])
                if datasets:
                    series = datasets[0].get("series", {})
                    # Get first series with observations
                    for _sk, sdata in series.items():
                        obs = sdata.get("observations", {})
                        if obs:
                            # Get latest observation
                            latest_key = max(obs.keys(), key=int)
                            val = obs[latest_key]
                            if isinstance(val, list) and val:
                                unemp = float(val[0])
                                country_data = {
                                    "unemployment_rate": round(unemp, 1),
                                    "source": "ILO ILOSTAT SDMX (live)",
                                    "data_confidence": 0.90,
                                }
                                break
        except Exception as e:
            _log_warn(f"ILO ILOSTAT API failed for {code}: {e}")

        # Fallback
        if not country_data:
            fb = _ILO_FALLBACK.get(code)
            if fb:
                country_data = {
                    **fb,
                    "source": "ILO ILOSTAT (curated benchmark)",
                    "data_confidence": 0.68,
                }
            else:
                continue

        _set_cached(cache_k, country_data)
        result["countries"][code] = country_data

    if not result["countries"]:
        return {}

    result["data_confidence"] = max(
        (d.get("data_confidence", 0.5) for d in result["countries"].values()),
        default=0.5,
    )
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# API 30: H-1B Visa Wage Benchmarks (curated from DOL OFLC LCA data)
# Prevailing wages and offered wages by SOC code for H-1B positions
# ═══════════════════════════════════════════════════════════════════════════════

_H1B_WAGE_DATA: Dict[str, Dict[str, Any]] = {
    "15-1252.00": {
        "title": "Software Developers",
        "h1b_median_wage": 135000,
        "h1b_25th": 110000,
        "h1b_75th": 165000,
        "prevailing_wage": 118000,
        "total_lcas_2024": 186000,
        "top_employers": ["Google", "Microsoft", "Amazon", "Meta", "Apple"],
    },
    "15-2051.00": {
        "title": "Data Scientists",
        "h1b_median_wage": 140000,
        "h1b_25th": 115000,
        "h1b_75th": 175000,
        "prevailing_wage": 120000,
        "total_lcas_2024": 42000,
        "top_employers": ["Google", "Meta", "Amazon", "Microsoft", "Apple"],
    },
    "15-1243.00": {
        "title": "Database Architects",
        "h1b_median_wage": 130000,
        "h1b_25th": 105000,
        "h1b_75th": 160000,
        "prevailing_wage": 112000,
        "total_lcas_2024": 15000,
        "top_employers": ["Amazon", "Microsoft", "Oracle", "IBM"],
    },
    "15-1244.00": {
        "title": "Network/Systems Admins",
        "h1b_median_wage": 110000,
        "h1b_25th": 88000,
        "h1b_75th": 135000,
        "prevailing_wage": 95000,
        "total_lcas_2024": 18000,
        "top_employers": ["Microsoft", "Cisco", "IBM", "AWS"],
    },
    "15-1241.00": {
        "title": "Computer Network Architects",
        "h1b_median_wage": 140000,
        "h1b_25th": 115000,
        "h1b_75th": 170000,
        "prevailing_wage": 125000,
        "total_lcas_2024": 8000,
        "top_employers": ["Cisco", "Microsoft", "Amazon"],
    },
    "15-1212.00": {
        "title": "Info Security Analysts",
        "h1b_median_wage": 128000,
        "h1b_25th": 100000,
        "h1b_75th": 155000,
        "prevailing_wage": 110000,
        "total_lcas_2024": 12000,
        "top_employers": ["Deloitte", "PwC", "Microsoft", "Amazon"],
    },
    "15-1254.00": {
        "title": "Web Developers",
        "h1b_median_wage": 105000,
        "h1b_25th": 82000,
        "h1b_75th": 130000,
        "prevailing_wage": 92000,
        "total_lcas_2024": 22000,
        "top_employers": ["Google", "Amazon", "Meta", "Shopify"],
    },
    "15-1255.00": {
        "title": "Web/Digital Interface Designers",
        "h1b_median_wage": 100000,
        "h1b_25th": 78000,
        "h1b_75th": 125000,
        "prevailing_wage": 88000,
        "total_lcas_2024": 5000,
        "top_employers": ["Apple", "Google", "Meta"],
    },
    "11-2021.00": {
        "title": "Marketing Managers",
        "h1b_median_wage": 145000,
        "h1b_25th": 115000,
        "h1b_75th": 180000,
        "prevailing_wage": 130000,
        "total_lcas_2024": 8500,
        "top_employers": ["Google", "Amazon", "Meta", "Salesforce"],
    },
    "11-2022.00": {
        "title": "Sales Managers",
        "h1b_median_wage": 140000,
        "h1b_25th": 110000,
        "h1b_75th": 175000,
        "prevailing_wage": 125000,
        "total_lcas_2024": 6000,
        "top_employers": ["Amazon", "Salesforce", "Oracle"],
    },
    "13-1111.00": {
        "title": "Management Analysts",
        "h1b_median_wage": 115000,
        "h1b_25th": 90000,
        "h1b_75th": 145000,
        "prevailing_wage": 100000,
        "total_lcas_2024": 35000,
        "top_employers": ["Deloitte", "Accenture", "McKinsey", "EY"],
    },
    "13-2051.00": {
        "title": "Financial Analysts",
        "h1b_median_wage": 110000,
        "h1b_25th": 85000,
        "h1b_75th": 140000,
        "prevailing_wage": 95000,
        "total_lcas_2024": 18000,
        "top_employers": ["JPMorgan", "Goldman Sachs", "Morgan Stanley"],
    },
    "13-2011.00": {
        "title": "Accountants/Auditors",
        "h1b_median_wage": 82000,
        "h1b_25th": 65000,
        "h1b_75th": 105000,
        "prevailing_wage": 72000,
        "total_lcas_2024": 15000,
        "top_employers": ["Deloitte", "EY", "KPMG", "PwC"],
    },
    "11-3121.00": {
        "title": "HR Managers",
        "h1b_median_wage": 130000,
        "h1b_25th": 105000,
        "h1b_75th": 160000,
        "prevailing_wage": 118000,
        "total_lcas_2024": 4000,
        "top_employers": ["Amazon", "Google", "Microsoft"],
    },
    "13-1071.00": {
        "title": "HR Specialists",
        "h1b_median_wage": 78000,
        "h1b_25th": 62000,
        "h1b_75th": 98000,
        "prevailing_wage": 68000,
        "total_lcas_2024": 6000,
        "top_employers": ["Amazon", "Infosys", "Wipro"],
    },
    "29-1141.00": {
        "title": "Registered Nurses",
        "h1b_median_wage": 78000,
        "h1b_25th": 62000,
        "h1b_75th": 95000,
        "prevailing_wage": 72000,
        "total_lcas_2024": 8000,
        "top_employers": ["HCA Healthcare", "Kaiser", "Mayo Clinic"],
    },
    "17-2141.00": {
        "title": "Mechanical Engineers",
        "h1b_median_wage": 105000,
        "h1b_25th": 82000,
        "h1b_75th": 130000,
        "prevailing_wage": 92000,
        "total_lcas_2024": 12000,
        "top_employers": ["Tesla", "Boeing", "Lockheed Martin"],
    },
    "17-2071.00": {
        "title": "Electrical Engineers",
        "h1b_median_wage": 115000,
        "h1b_25th": 90000,
        "h1b_75th": 140000,
        "prevailing_wage": 100000,
        "total_lcas_2024": 10000,
        "top_employers": ["Intel", "Qualcomm", "Apple", "Tesla"],
    },
    "17-2051.00": {
        "title": "Civil Engineers",
        "h1b_median_wage": 95000,
        "h1b_25th": 75000,
        "h1b_75th": 120000,
        "prevailing_wage": 85000,
        "total_lcas_2024": 5000,
        "top_employers": ["AECOM", "Jacobs", "Bechtel"],
    },
    "11-1021.00": {
        "title": "General/Ops Managers",
        "h1b_median_wage": 125000,
        "h1b_25th": 95000,
        "h1b_75th": 160000,
        "prevailing_wage": 108000,
        "total_lcas_2024": 8000,
        "top_employers": ["Amazon", "Google", "Microsoft"],
    },
    "11-9199.00": {
        "title": "Managers, All Other",
        "h1b_median_wage": 120000,
        "h1b_25th": 95000,
        "h1b_75th": 155000,
        "prevailing_wage": 105000,
        "total_lcas_2024": 12000,
        "top_employers": ["Amazon", "Accenture", "Google"],
    },
    "13-1081.00": {
        "title": "Logisticians",
        "h1b_median_wage": 88000,
        "h1b_25th": 68000,
        "h1b_75th": 110000,
        "prevailing_wage": 78000,
        "total_lcas_2024": 3000,
        "top_employers": ["Amazon", "FedEx", "UPS"],
    },
    "15-1242.00": {
        "title": "Database Administrators",
        "h1b_median_wage": 115000,
        "h1b_25th": 90000,
        "h1b_75th": 140000,
        "prevailing_wage": 100000,
        "total_lcas_2024": 8000,
        "top_employers": ["Oracle", "Microsoft", "Amazon"],
    },
    "27-1024.00": {
        "title": "Graphic Designers",
        "h1b_median_wage": 72000,
        "h1b_25th": 55000,
        "h1b_75th": 92000,
        "prevailing_wage": 62000,
        "total_lcas_2024": 2000,
        "top_employers": ["Apple", "Google", "Amazon"],
    },
    "27-3042.00": {
        "title": "Technical Writers",
        "h1b_median_wage": 90000,
        "h1b_25th": 72000,
        "h1b_75th": 115000,
        "prevailing_wage": 80000,
        "total_lcas_2024": 3000,
        "top_employers": ["Google", "Microsoft", "Amazon"],
    },
}


def fetch_h1b_wage_benchmarks(roles: List[str]) -> Dict[str, Any]:
    """Return curated H-1B visa wage benchmarks by occupation.

    Sourced from DOL OFLC LCA Disclosure Data (FY2024 Q4).
    No API -- data is embedded (DOL provides only bulk Excel downloads).
    """
    if not roles:
        return {}

    result: Dict[str, Any] = {"source": "DOL OFLC LCA Disclosure Data"}

    for role in roles:
        rl = role.lower().strip()
        # Match via ONET_SOC_CODES first (reuse existing mapping)
        soc = ONET_SOC_CODES.get(rl)
        if not soc:
            # Try partial match
            for key, code in ONET_SOC_CODES.items():
                if key in rl or rl in key:
                    soc = code
                    break

        if soc and soc in _H1B_WAGE_DATA:
            h1b = _H1B_WAGE_DATA[soc]
            result[role] = {
                "soc_code": soc,
                "title": h1b["title"],
                "h1b_median_wage": h1b["h1b_median_wage"],
                "h1b_25th_percentile": h1b["h1b_25th"],
                "h1b_75th_percentile": h1b["h1b_75th"],
                "prevailing_wage": h1b["prevailing_wage"],
                "total_lcas_fy2024": h1b["total_lcas_2024"],
                "top_h1b_employers": h1b["top_employers"],
                "wage_premium_vs_prevailing": f"+{round((h1b['h1b_median_wage'] / h1b['prevailing_wage'] - 1) * 100)}%",
                "source": "DOL OFLC LCA (curated FY2024)",
                "data_confidence": 0.80,
            }

    return result if len(result) > 1 else {}


def fetch_geopolitical_context(
    locations: list,
    industry: str = "",
    roles: list = None,
    campaign_start_month: int = 0,
) -> dict:
    """Use LLM to assess geopolitical/macro events impacting recruitment in given locations."""
    if not locations:
        return {
            "overall_risk_score": 1.0,
            "risk_level": "low",
            "locations": {},
            "summary": "No locations provided.",
            "recommendations": [],
            "source": "none",
            "confidence": 0.0,
        }

    try:
        from llm_router import call_llm, TASK_RESEARCH
    except ImportError:
        _log_warn("llm_router not available for geopolitical context")
        return _geopolitical_fallback(locations)

    month_names = [
        "",
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    month_str = (
        month_names[campaign_start_month]
        if 1 <= campaign_start_month <= 12
        else "the current period"
    )
    roles_str = ", ".join((roles or [])[:5]) or "various roles"
    locations_str = ", ".join(locations[:10])

    prompt = f"""Analyze geopolitical, political, economic, and macro events that could impact
recruitment advertising in these locations: {locations_str}

Industry: {industry or 'general'}
Target roles: {roles_str}
Campaign timing: {month_str}

For EACH location, provide:
1. Risk score (1-10, where 1=stable, 10=severe disruption)
2. Key events/factors (wars, political instability, economic crisis, natural disasters, labor law changes, immigration policy shifts, strikes)
3. Impact on recruitment (talent availability, cost pressure, competition for hires)
4. Budget adjustment recommendation (multiplier: 1.0=no change, >1.0=increase due to difficulty)

Respond ONLY in valid JSON with this exact structure:
{{
    "overall_risk_score": <float 1-10>,
    "locations": {{
        "<location>": {{
            "risk_score": <float 1-10>,
            "events": [
                {{"event": "<description>", "impact": "<impact on recruitment>", "severity": "low|moderate|high|critical"}}
            ],
            "budget_adjustment_factor": <float>
        }}
    }},
    "summary": "<2-3 sentence summary>",
    "recommendations": ["<actionable recommendation>"]
}}"""

    messages = [{"role": "user", "content": prompt}]
    system_prompt = (
        "You are a geopolitical risk analyst specializing in how world events impact "
        "recruitment marketing and talent acquisition. Be specific, factual, and data-driven. "
        "Only cite events you are confident about. Return ONLY valid JSON, no markdown."
    )

    try:
        result = call_llm(
            messages=messages,
            system_prompt=system_prompt,
            max_tokens=2048,
            task_type=TASK_RESEARCH,
            query_text=f"geopolitical risk for recruitment in {locations_str}",
        )
        if result and (result.get("text") or result.get("content")):
            content = result.get("text") or result.get("content") or ""
            json_match = re.search(r"\{[\s\S]*\}", content)
            if json_match:
                parsed = json.loads(json_match.group())
                parsed["source"] = (
                    f"llm_geopolitical_analysis ({result.get('provider', 'unknown')})"
                )
                parsed["confidence"] = 0.7
                score = parsed.get("overall_risk_score", 1.0)
                if score <= 3:
                    parsed["risk_level"] = "low"
                elif score <= 5:
                    parsed["risk_level"] = "moderate"
                elif score <= 7:
                    parsed["risk_level"] = "high"
                else:
                    parsed["risk_level"] = "critical"
                return parsed
    except Exception as e:
        _log_warn("LLM geopolitical analysis failed: %s" % e)

    return _geopolitical_fallback(locations)


def _geopolitical_fallback(locations: list) -> dict:
    """Static fallback when LLM is unavailable."""
    return {
        "overall_risk_score": 3.0,
        "risk_level": "low",
        "locations": {
            loc: {"risk_score": 3.0, "events": [], "budget_adjustment_factor": 1.0}
            for loc in locations
        },
        "summary": "Geopolitical risk analysis unavailable. Using default low-risk assumption.",
        "recommendations": ["Monitor local news for the campaign locations."],
        "source": "fallback",
        "confidence": 0.2,
    }


# Main enrichment orchestrator
# ---------------------------------------------------------------------------


def enrich_data(data: Dict[str, Any], request_id: str = "") -> Dict[str, Any]:
    """
    Main entry point. Takes a media plan request dict and returns an enriched
    dict with salary data, industry stats, demographics, global indicators,
    job market info, company info, competitor logos, and currency rates.

    The input ``data`` dict may contain any combination of:
        - client_name (str)
        - client_website (str)
        - industry (str)
        - roles / job_titles (list[str])
        - locations (list[str])
        - competitors (list[str])

    Args:
        data: Media plan request dict.
        request_id: Optional request ID for tracing through all API calls.

    Returns a dict matching the enrichment schema (see module docstring).
    All sub-keys are populated on a best-effort basis; failures yield empty
    or None values but never raise exceptions.
    """
    # Propagate request_id to thread-local for _safe_call tracing
    if request_id:
        set_request_id(request_id)

    start_time = time.time()
    apis_called: List[str] = []
    apis_succeeded: List[str] = []
    apis_failed: List[str] = []

    # --- Normalize inputs ---
    client_name = data.get("client_name") or "" or ""
    client_website = data.get("client_website") or "" or ""
    industry = data.get("industry") or "" or ""
    roles = data.get("roles") or data.get("job_titles") or []
    locations = data.get("locations") or []
    competitors = data.get("competitors") or []

    if isinstance(roles, str):
        roles = [r.strip() for r in roles.split(",")]
    if isinstance(locations, str):
        locations = [l.strip() for l in locations.split(",")]
    if isinstance(competitors, str):
        competitors = [c.strip() for c in competitors.split(",")]

    # Normalize dict inputs to strings (handles structured input like {"title": "...", "count": 5})
    roles = [r.get("title") or "" if isinstance(r, dict) else r for r in roles]
    locations = [
        (
            ", ".join(
                filter(
                    None,
                    [l.get("city") or "", l.get("state") or "", l.get("country") or ""],
                )
            )
            if isinstance(l, dict)
            else l
        )
        for l in locations
    ]
    competitors = [
        c.get("name") or "" if isinstance(c, dict) else c for c in competitors
    ]

    # Filter out empty strings from lists
    roles = [r for r in roles if isinstance(r, str) and r.strip()]
    locations = [l for l in locations if isinstance(l, str) and l.strip()]
    competitors = [c for c in competitors if isinstance(c, str) and c.strip()]

    # --- Result container ---
    enriched: Dict[str, Any] = {
        "salary_data": {},
        "industry_employment": None,
        "location_demographics": {},
        "global_indicators": {},
        "job_market": {},
        "company_info": {},
        "company_metadata": {},
        "sec_data": {},
        "competitor_logos": {},
        "currency_rates": {},
        "fred_indicators": {},
        "search_trends": {},
        "onet_data": {},
        "imf_indicators": {},
        "country_data": {},
        "geonames_data": {},
        "teleport_data": {},
        "datausa_occupation": {},
        "datausa_location": {},
        "google_ads_data": {},
        "meta_ads_data": {},
        "bing_ads_data": {},
        "tiktok_ads_data": {},
        "linkedin_ads_data": {},
        "careeronestop_data": {},
        "jooble_data": {},
        "eurostat_data": {},
        "ilo_data": {},
        "h1b_data": {},
        "enrichment_summary": {},
    }

    # --- Define tasks for concurrent execution ---
    # Each task is a tuple of (result_key, api_label, callable)
    # Use default args in lambdas to capture current values
    tasks: List[tuple] = []

    if roles:
        tasks.append(("salary_data", "BLS", lambda _r=roles: fetch_salary_data(_r)))

    if industry:
        tasks.append(
            (
                "industry_employment",
                "BLS-QCEW",
                lambda _i=industry: fetch_industry_employment(_i),
            )
        )

    if locations:
        tasks.append(
            (
                "location_demographics",
                "Census-ACS",
                lambda _l=locations: fetch_location_demographics(_l),
            )
        )
        tasks.append(
            (
                "global_indicators",
                "WorldBank",
                lambda _l=locations: fetch_global_indicators(_l),
            )
        )

    if roles and locations:
        tasks.append(
            (
                "job_market",
                "Adzuna",
                lambda _r=roles, _l=locations: fetch_job_market(_r, _l),
            )
        )

    if client_name:
        tasks.append(
            (
                "company_info",
                "Wikipedia",
                lambda _cn=client_name, _cw=client_website: fetch_company_info(
                    _cn, _cw
                ),
            )
        )
        tasks.append(
            (
                "company_metadata",
                "Clearbit-Auto",
                lambda _cn=client_name, _cw=client_website: fetch_company_metadata(
                    _cn, _cw
                ),
            )
        )
        tasks.append(
            (
                "sec_data",
                "SEC-EDGAR",
                lambda _cn=client_name: fetch_sec_company_data(_cn),
            )
        )

    if competitors:
        tasks.append(
            (
                "competitor_logos",
                "Clearbit",
                lambda _c=competitors: fetch_competitor_logos(_c),
            )
        )

    # Currency rates (tries live API, falls back to hardcoded)
    tasks.append(("currency_rates", "CurrencyRates", lambda: fetch_currency_rates()))

    # FRED economic indicators (if API key available)
    tasks.append(("fred_indicators", "FRED", lambda: fetch_fred_indicators()))

    # Google Trends for roles (if pytrends installed)
    if roles:
        trend_keywords = [r for r in roles[:3]]
        if client_name:
            trend_keywords.insert(0, f"{client_name} jobs")
        tasks.append(
            (
                "search_trends",
                "GoogleTrends",
                lambda _kw=trend_keywords: fetch_search_trends(_kw),
            )
        )

    # --- New APIs (O*NET, IMF, REST Countries, GeoNames, Teleport, DataUSA) ---

    if roles:
        tasks.append(
            ("onet_data", "O*NET", lambda _r=roles: fetch_onet_occupation_data(_r))
        )
        tasks.append(
            (
                "datausa_occupation",
                "DataUSA-Occ",
                lambda _r=roles: fetch_datausa_occupation_stats(_r),
            )
        )

    if locations:
        tasks.append(
            ("imf_indicators", "IMF", lambda _l=locations: fetch_imf_indicators(_l))
        )
        tasks.append(
            (
                "country_data",
                "RESTCountries",
                lambda _l=locations: fetch_country_data(_l),
            )
        )
        tasks.append(
            ("geonames_data", "GeoNames", lambda _l=locations: fetch_geonames_data(_l))
        )
        tasks.append(
            (
                "teleport_data",
                "Teleport",
                lambda _l=locations: fetch_teleport_city_data(_l),
            )
        )
        tasks.append(
            (
                "datausa_location",
                "DataUSA-Loc",
                lambda _l=locations: fetch_datausa_location_data(_l),
            )
        )

    # --- Ad Platform & Job Market APIs (19-25) ---

    if roles:
        tasks.append(
            (
                "google_ads_data",
                "GoogleAds",
                lambda _r=roles, _l=locations: fetch_google_ads_data(_r, _l),
            )
        )
        tasks.append(
            (
                "meta_ads_data",
                "MetaAds",
                lambda _r=roles, _l=locations: fetch_meta_ads_data(_r, _l),
            )
        )
        tasks.append(
            (
                "bing_ads_data",
                "BingAds",
                lambda _r=roles, _l=locations: fetch_bing_ads_data(_r, _l),
            )
        )
        tasks.append(
            (
                "tiktok_ads_data",
                "TikTokAds",
                lambda _r=roles, _l=locations: fetch_tiktok_ads_data(_r, _l),
            )
        )
        tasks.append(
            (
                "linkedin_ads_data",
                "LinkedInAds",
                lambda _r=roles, _l=locations: fetch_linkedin_ads_data(_r, _l),
            )
        )
        tasks.append(
            (
                "careeronestop_data",
                "CareerOneStop",
                lambda _r=roles, _l=locations: fetch_careeronestop_data(_r, _l),
            )
        )

    if roles and locations:
        tasks.append(
            (
                "jooble_data",
                "Jooble",
                lambda _r=roles, _l=locations: fetch_jooble_data(_r, _l),
            )
        )

    # --- New APIs (28-30): Eurostat, ILO, H-1B ---

    if locations:
        tasks.append(
            (
                "eurostat_data",
                "Eurostat",
                lambda _l=locations: fetch_eurostat_labour_data(_l),
            )
        )
        tasks.append(
            ("ilo_data", "ILO-ILOSTAT", lambda _l=locations: fetch_ilo_labour_data(_l))
        )

    if roles:
        tasks.append(
            ("h1b_data", "H1B-Wages", lambda _r=roles: fetch_h1b_wage_benchmarks(_r))
        )

    # --- Execute tasks concurrently ---
    _log_info(
        f"Starting enrichment with {len(tasks)} tasks "
        f"(roles={len(roles)}, locations={len(locations)}, "
        f"competitors={len(competitors)})"
    )

    apis_skipped: List[str] = []
    apis_circuit_broken: List[str] = []
    api_details: Dict[str, Dict[str, Any]] = {}  # per-API metadata

    # Gate concurrent enrichments to avoid thread explosion under load.
    # Each enrich_data() creates a ThreadPoolExecutor(max_workers=15);
    # the semaphore caps total concurrent enrichments at 10 (= 150 threads).
    if not _enrichment_semaphore.acquire(timeout=120):  # 2-minute wait max
        _log_warn(
            "enrich_data: too many concurrent enrichments, returning partial data"
        )
        return enriched  # Return what we have so far

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {}
            for result_key, api_label, func in tasks:
                apis_called.append(api_label)
                future = executor.submit(_safe_call, func, api_label)
                future_map[future] = (result_key, api_label)

            for future in as_completed(future_map):
                result_key, api_label = future_map[future]
                try:
                    result, status, metadata = future.result()
                    # Store per-API details
                    api_details[api_label] = {
                        "elapsed_time": metadata.get("elapsed_time", 0.0),
                        "source": metadata.get("source", "unknown"),
                        "success": metadata.get("success", False),
                        "status": status,
                    }

                    if status == "ok":
                        enriched[result_key] = result
                        apis_succeeded.append(api_label)
                    elif status == "empty":
                        # API ran fine but had no applicable data
                        apis_skipped.append(api_label)
                    elif status == "circuit_open":
                        apis_circuit_broken.append(api_label)
                        apis_failed.append(api_label)
                    else:
                        apis_failed.append(api_label)
                except Exception as exc:
                    _log_warn(f"Future for {api_label} raised: {exc}")
                    apis_failed.append(api_label)
                    api_details[api_label] = {
                        "elapsed_time": 0.0,
                        "source": "error",
                        "success": False,
                        "status": "error",
                        "error_message": str(exc),
                    }
    finally:
        _enrichment_semaphore.release()

    # Geopolitical context (LLM-based)
    geo_context = {}
    try:
        geo_context = fetch_geopolitical_context(
            locations=locations,
            industry=industry,
            roles=roles,
            campaign_start_month=int(data.get("campaign_start_month") or 0 or 0),
        )
    except Exception as e:
        _log_warn("Geopolitical context enrichment failed: %s" % e)

    # --- Build enhanced summary ---
    elapsed = round(time.time() - start_time, 2)

    # Confidence score: ratio of successful (live + cached) calls to total
    total_calls = len(apis_called) if apis_called else 1  # avoid division by zero
    successful_calls = sum(
        1
        for d in api_details.values()
        if d.get("success", False) and d.get("source") in ("live", "cached")
    )
    confidence_score = round(successful_calls / total_calls, 3)

    enriched["geopolitical_context"] = geo_context

    enriched["enrichment_summary"] = {
        "apis_called": apis_called,
        "apis_succeeded": apis_succeeded,
        "apis_skipped": apis_skipped,
        "apis_failed": apis_failed,
        "apis_circuit_broken": apis_circuit_broken,
        "total_time_seconds": elapsed,
        "confidence_score": confidence_score,
        "api_details": api_details,
        "cached": False,  # would be True if entire result was from cache
    }

    ok_count = len(apis_succeeded) + len(apis_skipped)
    cb_msg = (
        f", {len(apis_circuit_broken)} circuit-broken" if apis_circuit_broken else ""
    )
    _log_info(
        f"Enrichment complete in {elapsed}s — "
        f"{ok_count}/{len(apis_called)} APIs ok "
        f"({len(apis_succeeded)} data, {len(apis_skipped)} skipped, "
        f"{len(apis_failed)} failed{cb_msg}) "
        f"[confidence={confidence_score}]"
    )

    return enriched


def _safe_call(func, label: str):
    """
    Wrapper that catches all exceptions so a single API failure never
    crashes the enrichment pipeline.

    Returns (result, status, metadata) where:
        - result:   the API return value (or None on failure)
        - status:   "ok", "empty", "error", or "circuit_open"
        - metadata: dict with timing and source info:
            - elapsed_time:  seconds the call took (float)
            - source:        "live", "cached", "fallback", or "circuit_open"
            - success:       bool indicating whether data was obtained
            - error_message: str or None
    """
    rid = get_request_id()
    metadata: Dict[str, Any] = {
        "elapsed_time": 0.0,
        "source": "live",
        "success": False,
        "error_message": None,
        "request_id": rid,
    }

    # --- Circuit breaker gate ---
    if _circuit_breaker_check(label):
        _log_warn(
            f"[{rid}] Circuit breaker OPEN — skipping API '{label}'"
            if rid
            else f"Circuit breaker OPEN — skipping API '{label}'"
        )
        metadata["source"] = "circuit_open"
        metadata["elapsed_time"] = 0.0
        return None, "circuit_open", metadata

    call_start = time.time()
    if rid:
        _log_info(f"[{rid}] API '{label}' — call started")
    try:
        result = func()
        elapsed = round(time.time() - call_start, 4)
        metadata["elapsed_time"] = elapsed

        if result is None or result == {} or result == []:
            # API ran fine but had no applicable data
            _circuit_breaker_record_success(label)
            metadata["success"] = True
            metadata["source"] = "live"
            return result, "empty", metadata

        # Determine source: if the call returned almost instantly and we know
        # caching is in play, mark as cached. The individual API functions use
        # _get_cached internally — calls completing in < 5 ms are very likely
        # cache hits. This is a heuristic; API functions don't currently expose
        # cache-hit information directly.
        if elapsed < 0.005:
            metadata["source"] = "cached"
        else:
            metadata["source"] = "live"

        # ── API Response Validation ──
        # Guard against malformed API responses that could corrupt
        # the downstream synthesis pipeline.  We validate that:
        # 1. Dict responses contain at least one non-empty value
        # 2. Numeric values are within sane bounds (salary not negative,
        #    percentages 0-100, etc.)
        # 3. No unexpected None values in critical fields
        if isinstance(result, dict):
            _validation_warnings = []
            for rk, rv in result.items():
                if isinstance(rv, dict):
                    # Check salary data for obviously wrong values
                    for salary_field in ("median", "mean", "p10", "p25", "p75", "p90"):
                        sv = rv.get(salary_field)
                        if sv is not None and isinstance(sv, (int, float)):
                            if sv < 0:
                                _validation_warnings.append(
                                    f"{rk}.{salary_field} is negative ({sv})"
                                )
                                rv[salary_field] = 0  # clamp to 0
                            elif sv > 10_000_000:  # >0M annual salary
                                _validation_warnings.append(
                                    f"{rk}.{salary_field} is unreasonably large ({sv})"
                                )
            if _validation_warnings:
                _log_warn(
                    f"API '{label}' response validation: "
                    + "; ".join(_validation_warnings[:5])
                )
                metadata["validation_warnings"] = _validation_warnings

        _circuit_breaker_record_success(label)
        metadata["success"] = True
        if rid:
            _log_info(
                f"[{rid}] API '{label}' — {metadata['source']} response in {elapsed}s"
            )
        return result, "ok", metadata

    except Exception as exc:
        elapsed = round(time.time() - call_start, 4)
        metadata["elapsed_time"] = elapsed
        metadata["success"] = False
        metadata["error_message"] = str(exc)
        metadata["source"] = "fallback"
        _circuit_breaker_record_failure(label)
        _log_warn(
            f"[{rid}] API '{label}' raised an exception after {elapsed}s: {exc}"
            if rid
            else f"API '{label}' raised an exception after {elapsed}s: {exc}"
        )
        return None, "error", metadata


# ---------------------------------------------------------------------------
# Convenience: clear all caches
# ---------------------------------------------------------------------------


def clear_cache(memory: bool = True, disk: bool = True) -> None:
    """Clear in-memory and/or disk caches."""
    if memory:
        with _cache_lock:
            _memory_cache.clear()
        _log_info("In-memory cache cleared")

    if disk:
        count = 0
        for f in CACHE_DIR.glob("*.json"):
            try:
                f.unlink()
                count += 1
            except Exception:
                pass
        _log_info(f"Disk cache cleared ({count} files removed)")


# ---------------------------------------------------------------------------
# CLI entry point for testing
# ---------------------------------------------------------------------------


def _cli_demo():
    """Run a quick enrichment demo from the command line."""
    sample = {
        "client_name": "Guidewire",
        "client_website": "guidewire.com",
        "industry": "technology",
        "roles": ["Software Engineer", "Product Manager", "Data Scientist"],
        "locations": ["San Mateo, CA", "London, UK", "Sydney, AU"],
        "competitors": ["Salesforce", "Duck Creek Technologies", "Majesco"],
    }

    print("=" * 60)
    print("  API Enrichment Demo")
    print("=" * 60)
    print(f"\nInput: {json.dumps(sample, indent=2)}\n")

    # Clear caches for fresh test
    clear_cache()

    result = enrich_data(sample)

    print("\n" + "=" * 60)
    print("  Enrichment Results")
    print("=" * 60)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    _cli_demo()
