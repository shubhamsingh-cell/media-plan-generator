"""
api_enrichment.py — Comprehensive API Enrichment System for AI Media Planner

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

All API calls:
    - Use only urllib.request (stdlib, no third-party dependencies)
    - Have a 5-second timeout per call
    - Are cached in-memory and on disk (24-hour TTL)
    - Fail gracefully (never crash the generation pipeline)
    - Run concurrently via ThreadPoolExecutor (max 6 workers)

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
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Constants & configuration
# ---------------------------------------------------------------------------

API_TIMEOUT = 8  # seconds per HTTP call (increased from 5 for reliability)
CACHE_TTL = 86400  # 24 hours in seconds
MAX_WORKERS = 6
CACHE_DIR = Path(__file__).resolve().parent / "data" / "api_cache"

# Ensure cache directory exists at import time
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Create a permissive SSL context for API calls (some free APIs have
# certificate issues on certain platforms). We still verify by default
# but fall back to unverified if the first attempt fails.
_DEFAULT_SSL_CTX = ssl.create_default_context()
_UNVERIFIED_SSL_CTX = ssl._create_unverified_context()

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
    "delivery driver": "53-3031",
    "store associate": "41-2031",
    "cashier": "41-2011",
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
    "manufacturing": "31",
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
    "USA": "us", "GBR": "gb", "CAN": "ca", "AUS": "au", "DEU": "de",
    "FRA": "fr", "IND": "in", "NLD": "nl", "BRA": "br", "POL": "pl",
    "SGP": "sg", "ZAF": "za", "AUT": "at", "NZL": "nz", "ITA": "it",
    "ESP": "es", "MEX": "mx",
}

# US state abbreviations for detecting US locations
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
}

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
MAX_MEMORY_CACHE_SIZE = 500  # Prevent unbounded memory growth


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _log_warn(msg: str) -> None:
    """Write a warning to stderr (never crashes)."""
    try:
        print(f"[api_enrichment WARN] {msg}", file=sys.stderr)
    except Exception:
        pass


def _log_info(msg: str) -> None:
    """Write an info message to stderr."""
    try:
        print(f"[api_enrichment INFO] {msg}", file=sys.stderr)
    except Exception:
        pass


def _cache_key(api_name: str, params: str) -> str:
    """Generate a deterministic cache key from API name and param string."""
    raw = f"{api_name}:{params}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _get_cached(key: str) -> Optional[Any]:
    """Check in-memory cache, then file cache. Returns None on miss."""
    # In-memory
    if key in _memory_cache:
        entry = _memory_cache[key]
        if time.time() - entry["ts"] < CACHE_TTL:
            return entry["data"]
        else:
            del _memory_cache[key]

    # File-based
    cache_file = CACHE_DIR / f"{key}.json"
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as fh:
                entry = json.load(fh)
            if time.time() - entry.get("ts", 0) < CACHE_TTL:
                _memory_cache[key] = entry  # promote to memory
                return entry["data"]
            else:
                cache_file.unlink(missing_ok=True)
        except Exception:
            pass

    return None


def _set_cached(key: str, data: Any) -> None:
    """Store data in both in-memory and file caches."""
    # Evict oldest entries if cache is full
    if len(_memory_cache) >= MAX_MEMORY_CACHE_SIZE:
        sorted_keys = sorted(
            _memory_cache.keys(),
            key=lambda k: _memory_cache[k].get("ts", 0)
        )
        for k in sorted_keys[:MAX_MEMORY_CACHE_SIZE // 5]:
            del _memory_cache[k]
    entry = {"ts": time.time(), "data": data}
    _memory_cache[key] = entry

    cache_file = CACHE_DIR / f"{key}.json"
    try:
        with open(cache_file, "w", encoding="utf-8") as fh:
            json.dump(entry, fh, ensure_ascii=False)
    except Exception as exc:
        _log_warn(f"Failed to write cache file {cache_file}: {exc}")


def _http_get_json(url: str, headers: Optional[Dict[str, str]] = None,
                   timeout: int = API_TIMEOUT) -> Optional[Any]:
    """
    Perform an HTTP GET and return parsed JSON, or None on any failure.
    Tries verified SSL first, falls back to unverified if needed.
    """
    req = urllib.request.Request(url, method="GET")
    req.add_header("User-Agent", "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)")
    req.add_header("Accept", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)

    for ctx in (_DEFAULT_SSL_CTX, _UNVERIFIED_SSL_CTX):
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except ssl.SSLError:
            continue  # retry with unverified context
        except Exception as exc:
            _log_warn(f"HTTP GET failed for {url}: {exc}")
            return None
    return None


def _http_post_json(url: str, payload: Any,
                    headers: Optional[Dict[str, str]] = None,
                    timeout: int = API_TIMEOUT) -> Optional[Any]:
    """Perform an HTTP POST with a JSON body and return parsed JSON."""
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("User-Agent", "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)

    for ctx in (_DEFAULT_SSL_CTX, _UNVERIFIED_SSL_CTX):
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        except ssl.SSLError:
            continue
        except Exception as exc:
            _log_warn(f"HTTP POST failed for {url}: {exc}")
            return None
    return None


def _parse_country_from_location(location: str) -> Optional[str]:
    """
    Attempt to extract an ISO-3 country code from a location string.
    Examples:
        'San Mateo, CA'  -> 'USA'  (US state detected)
        'London, UK'     -> 'GBR'
        'Sydney, Australia' -> 'AUS'
        'Seattle WA'     -> 'USA'  (no comma, space-separated)
    """
    if not location:
        return None

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
    clean = re.sub(r"\s+(inc|llc|ltd|corp|co|technologies|technology|software|group|solutions)$",
                   "", clean, flags=re.IGNORECASE).strip()
    slug = clean.replace(" ", "")
    return f"{slug}.com"


def _extract_state_abbr(location: str) -> Optional[str]:
    """Extract US state abbreviation from a location string."""
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

    series_mean = f"OEUN0000000000000{soc_clean}04"    # annual mean wage
    series_median = f"OEUN0000000000000{soc_clean}13"  # annual median wage
    series_p10 = f"OEUN0000000000000{soc_clean}11"     # annual 10th pct
    series_p90 = f"OEUN0000000000000{soc_clean}15"     # annual 90th pct

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
                msg = str(resp.get("message", ""))
            _log_warn(f"BLS {version} failed for SOC {soc_code}: {msg}")
            resp = None

    if not resp:
        _log_warn(f"BLS request failed on all endpoints for SOC {soc_code}")
        return None

    result: Dict[str, Any] = {"source": "BLS OES"}
    series_list = resp.get("Results", {}).get("series", [])

    for series in series_list:
        sid = series.get("seriesID", "")
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
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "FL": "12", "GA": "13", "HI": "15", "ID": "16",
    "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21", "LA": "22",
    "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
    "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39", "OK": "40",
    "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46", "TN": "47",
    "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53", "WV": "54",
    "WI": "55", "WY": "56", "DC": "11",
}


def _http_get_text(url: str, timeout: int = API_TIMEOUT) -> Optional[str]:
    """Perform HTTP GET and return raw text, or None on failure."""
    req = urllib.request.Request(url, method="GET")
    req.add_header("User-Agent", "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com)")
    for ctx in (_DEFAULT_SSL_CTX, _UNVERIFIED_SSL_CTX):
        try:
            with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                return resp.read().decode("utf-8")
        except ssl.SSLError:
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

    # Map industry to NAICS code
    industry_lower = industry.lower().replace(" ", "_")
    naics = NAICS_CODES.get(industry_lower)
    if not naics:
        # Try partial matching — check each key
        for key, code in NAICS_CODES.items():
            if key in industry_lower or industry_lower in key:
                naics = code
                break
    if not naics:
        # Try word-level matching
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

                    area = row.get("area_fips", "")
                    # National total row + private ownership
                    if area == "US000" and row.get("own_code", "") == "5":
                        try:
                            emp = int(row.get("annual_avg_emplvl", "0") or
                                      row.get("month1_emplvl", "0"))
                            wages = int(row.get("annual_avg_wkly_wage", "0") or "0")
                            estabs = int(row.get("annual_avg_estabs", "0") or
                                         row.get("qtrly_estabs", "0"))

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
        url = (f"https://api.census.gov/data/{acs_year}/acs/acs5"
               "?get=NAME,B01001_001E,B19013_001E&for=state:*")

        try:
            resp = _http_get_json(url, timeout=10)
            if not resp or not isinstance(resp, list) or len(resp) < 2:
                _log_warn(f"Census ACS {acs_year} state data request failed, trying older year")
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


def _country_label(iso3: str) -> str:
    """Convert ISO-3 code to a short human-readable label (e.g. 'GBR' -> 'UK')."""
    reverse_map = {v: k for k, v in COUNTRY_CODES.items() if len(k) == 2}
    return reverse_map.get(iso3, iso3).upper()


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
    try:
        req = urllib.request.Request(clearbit_url, method="HEAD")
        req.add_header("User-Agent", "MediaPlanGenerator/1.0")
        with urllib.request.urlopen(req, timeout=3, context=_DEFAULT_SSL_CTX) as resp:
            if resp.status == 200:
                _set_cached(cache_k, clearbit_url)
                return clearbit_url
    except Exception:
        try:
            with urllib.request.urlopen(req, timeout=3, context=_UNVERIFIED_SSL_CTX) as resp:
                if resp.status == 200:
                    _set_cached(cache_k, clearbit_url)
                    return clearbit_url
        except Exception:
            pass

    # Strategy 2: Google Favicons API (always works, lower resolution)
    google_url = f"https://www.google.com/s2/favicons?domain={domain}&sz=128"
    _set_cached(cache_k, google_url)
    return google_url


def fetch_competitor_logos(competitors: List[str],
                          client_website: Optional[str] = None) -> Dict[str, str]:
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

def fetch_job_market(roles: List[str], locations: List[str]) -> Dict[str, Any]:
    """
    Fetch job market data from Adzuna (if API keys are available).
    Returns posting counts, average salaries, and competition levels.
    """
    app_id = os.environ.get("ADZUNA_APP_ID", "")
    app_key = os.environ.get("ADZUNA_APP_KEY", "")

    if not app_id or not app_key:
        _log_info("Adzuna API keys not set; skipping job market enrichment")
        return {}

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

        params = urllib.parse.urlencode({
            "app_id": app_id,
            "app_key": app_key,
            "what": role,
            "results_per_page": "1",
            "content-type": "application/json",
        })
        url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1?{params}"

        try:
            resp = _http_get_json(url)
            if resp:
                count = resp.get("count", 0)
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

def fetch_company_info(client_name: str,
                       client_website: Optional[str] = None) -> Dict[str, Any]:
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
                extract = resp.get("extract", "")
                if extract and len(extract) > 30:
                    # Verify the article is about a company/organization, not
                    # some unrelated topic. Check for business-related terms.
                    extract_lower = extract.lower()
                    is_company_article = any(term in extract_lower for term in [
                        "company", "corporation", "inc.", "ltd", "software",
                        "founded", "headquartered", "business", "firm",
                        "enterprise", "organization", "provider", "platform",
                        "technology", "services", "solutions", "startup",
                        "subsidiary", "group", "brand", "manufacturer",
                        "hospital", "clinic", "bank", "financial",
                        "retailer", "store", "chain", "restaurant",
                    ])
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
                title = sr.get("title", "")
                if not title:
                    continue
                encoded = urllib.parse.quote(title.replace(" ", "_"), safe="()_")
                summary_url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}"
                try:
                    resp = _http_get_json(summary_url)
                    if resp and resp.get("type") == "standard":
                        extract = resp.get("extract", "")
                        if extract and len(extract) > 30:
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

def fetch_company_metadata(company_name: str,
                           client_website: Optional[str] = None) -> Optional[Dict[str, Any]]:
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
                if item.get("name", "").lower() == company_name.lower():
                    best = item
                    break

            domain = best.get("domain", "")

            # If client_website is provided and Clearbit returned a different
            # domain, prefer the client_website as it's more reliable
            if client_website:
                cw = client_website.strip().lower()
                if cw.startswith("http"):
                    parsed = urllib.parse.urlparse(cw)
                    cw = parsed.hostname or cw
                # Only override if Clearbit domain looks wrong
                if domain and cw and domain != cw:
                    _log_info(f"Clearbit returned domain '{domain}' but "
                              f"client_website is '{cw}'; using client_website")
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
        req.add_header("User-Agent", "MediaPlanGenerator/1.0 (media-plan-generator.onrender.com; contact@joveo.com)")
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
        "", company_lower, flags=re.IGNORECASE
    ).strip()

    # Build search terms: original name + any aliases
    search_terms = [clean_name]
    for alias_key, alias_values in _COMPANY_ALIASES.items():
        if alias_key == clean_name or clean_name.startswith(alias_key):
            search_terms.extend(alias_values)

    best_match = None
    best_score = 0
    for _key, entry in tickers.items():
        title = entry.get("title", "").lower()
        ticker = entry.get("ticker", "").lower()

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
            if title.startswith(term + " ") or title.startswith(term + ",") or title.startswith(term + "."):
                score = 85
                if score > best_score:
                    best_match = entry
                    best_score = score
            # Title contains search term as a whole word
            elif re.search(r'\b' + re.escape(term) + r'\b', title):
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
            "ticker": best_match.get("ticker", ""),
            "cik": str(best_match.get("cik_str", "")),
            "company_name": best_match.get("title", ""),
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
    "unemployment_rate": "UNRATE",       # US unemployment rate
    "cpi_inflation": "CPIAUCSL",         # Consumer Price Index
    "fed_funds_rate": "FEDFUNDS",        # Federal funds rate
    "job_openings": "JTSJOL",            # Job openings (JOLTS)
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
                val = obs.get("value", "")
                if val and val != ".":
                    result[label] = {
                        "value": float(val),
                        "date": obs.get("date", ""),
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

def fetch_search_trends(keywords: List[str]) -> Dict[str, Any]:
    """
    Fetch Google Trends interest data for given keywords.
    Requires the 'pytrends' package (pip install pytrends).
    Returns relative search interest scores.
    """
    try:
        from pytrends.request import TrendReq
    except ImportError:
        _log_info("pytrends not installed; skipping Google Trends")
        return {}

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
# Main enrichment orchestrator
# ---------------------------------------------------------------------------

def enrich_data(data: Dict[str, Any]) -> Dict[str, Any]:
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

    Returns a dict matching the enrichment schema (see module docstring).
    All sub-keys are populated on a best-effort basis; failures yield empty
    or None values but never raise exceptions.
    """
    start_time = time.time()
    apis_called: List[str] = []
    apis_succeeded: List[str] = []
    apis_failed: List[str] = []

    # --- Normalize inputs ---
    client_name = data.get("client_name", "") or ""
    client_website = data.get("client_website", "") or ""
    industry = data.get("industry", "") or ""
    roles = data.get("roles") or data.get("job_titles") or []
    locations = data.get("locations") or []
    competitors = data.get("competitors") or []

    if isinstance(roles, str):
        roles = [r.strip() for r in roles.split(",")]
    if isinstance(locations, str):
        locations = [l.strip() for l in locations.split(",")]
    if isinstance(competitors, str):
        competitors = [c.strip() for c in competitors.split(",")]

    # Filter out empty strings from lists
    roles = [r for r in roles if r.strip()]
    locations = [l for l in locations if l.strip()]
    competitors = [c for c in competitors if c.strip()]

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
        "enrichment_summary": {},
    }

    # --- Define tasks for concurrent execution ---
    # Each task is a tuple of (result_key, api_label, callable)
    # Use default args in lambdas to capture current values
    tasks: List[tuple] = []

    if roles:
        tasks.append(("salary_data", "BLS",
                       lambda _r=roles: fetch_salary_data(_r)))

    if industry:
        tasks.append(("industry_employment", "BLS-QCEW",
                       lambda _i=industry: fetch_industry_employment(_i)))

    if locations:
        tasks.append(("location_demographics", "Census-ACS",
                       lambda _l=locations: fetch_location_demographics(_l)))
        tasks.append(("global_indicators", "WorldBank",
                       lambda _l=locations: fetch_global_indicators(_l)))

    if roles and locations:
        tasks.append(("job_market", "Adzuna",
                       lambda _r=roles, _l=locations: fetch_job_market(_r, _l)))

    if client_name:
        tasks.append(("company_info", "Wikipedia",
                       lambda _cn=client_name, _cw=client_website: fetch_company_info(_cn, _cw)))
        tasks.append(("company_metadata", "Clearbit-Auto",
                       lambda _cn=client_name, _cw=client_website: fetch_company_metadata(_cn, _cw)))
        tasks.append(("sec_data", "SEC-EDGAR",
                       lambda _cn=client_name: fetch_sec_company_data(_cn)))

    if competitors:
        tasks.append(("competitor_logos", "Clearbit",
                       lambda _c=competitors: fetch_competitor_logos(_c)))

    # Currency rates (tries live API, falls back to hardcoded)
    tasks.append(("currency_rates", "CurrencyRates",
                  lambda: fetch_currency_rates()))

    # FRED economic indicators (if API key available)
    tasks.append(("fred_indicators", "FRED",
                  lambda: fetch_fred_indicators()))

    # Google Trends for roles (if pytrends installed)
    if roles:
        trend_keywords = [r for r in roles[:3]]
        if client_name:
            trend_keywords.insert(0, f"{client_name} jobs")
        tasks.append(("search_trends", "GoogleTrends",
                       lambda _kw=trend_keywords: fetch_search_trends(_kw)))

    # --- Execute tasks concurrently ---
    _log_info(f"Starting enrichment with {len(tasks)} tasks "
              f"(roles={len(roles)}, locations={len(locations)}, "
              f"competitors={len(competitors)})")

    apis_skipped: List[str] = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {}
        for result_key, api_label, func in tasks:
            apis_called.append(api_label)
            future = executor.submit(_safe_call, func, api_label)
            future_map[future] = (result_key, api_label)

        for future in as_completed(future_map):
            result_key, api_label = future_map[future]
            try:
                result, status = future.result()
                if status == "ok":
                    enriched[result_key] = result
                    apis_succeeded.append(api_label)
                elif status == "empty":
                    # API ran fine but had no applicable data (e.g. WorldBank for US)
                    apis_skipped.append(api_label)
                else:
                    apis_failed.append(api_label)
            except Exception as exc:
                _log_warn(f"Future for {api_label} raised: {exc}")
                apis_failed.append(api_label)

    # --- Build summary ---
    elapsed = round(time.time() - start_time, 2)
    enriched["enrichment_summary"] = {
        "apis_called": apis_called,
        "apis_succeeded": apis_succeeded,
        "apis_skipped": apis_skipped,
        "apis_failed": apis_failed,
        "total_time_seconds": elapsed,
        "cached": False,  # would be True if entire result was from cache
    }

    ok_count = len(apis_succeeded) + len(apis_skipped)
    _log_info(f"Enrichment complete in {elapsed}s — "
              f"{ok_count}/{len(apis_called)} APIs ok "
              f"({len(apis_succeeded)} data, {len(apis_skipped)} skipped, "
              f"{len(apis_failed)} failed)")

    return enriched


def _safe_call(func, label: str):
    """
    Wrapper that catches all exceptions so a single API failure never
    crashes the enrichment pipeline.
    Returns (result, status) where status is "ok", "empty", or "error".
    """
    try:
        result = func()
        if result is None:
            return None, "error"
        if result == {} or result == []:
            return result, "empty"  # API ran fine but had no applicable data
        return result, "ok"
    except Exception as exc:
        _log_warn(f"API '{label}' raised an exception: {exc}")
        return None, "error"


# ---------------------------------------------------------------------------
# Convenience: clear all caches
# ---------------------------------------------------------------------------

def clear_cache(memory: bool = True, disk: bool = True) -> None:
    """Clear in-memory and/or disk caches."""
    global _memory_cache
    if memory:
        _memory_cache = {}
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
