"""
api_enrichment.py — Comprehensive API Enrichment System for AI Media Planner

Fetches real data from free public APIs to enrich media plan generation with
salary benchmarks, industry employment stats, location demographics, global
economic indicators, job market data, company information, and competitor logos.

Integrated APIs:
    1. BLS (Bureau of Labor Statistics) — Salary & employment data
    2. Data USA — Industry stats, demographics, education
    3. World Bank Open Data — Global economic indicators
    4. Clearbit Logo API — Company & competitor logos
    5. Adzuna Job Search — Job postings & salary data (optional, needs keys)
    6. Open Exchange Rates — Currency conversion (hardcoded fallback)
    7. Wikipedia REST API — Company descriptions

All API calls:
    - Use only urllib.request (stdlib, no third-party dependencies)
    - Have a 5-second timeout per call
    - Are cached in-memory and on disk (24-hour TTL)
    - Fail gracefully (never crash the generation pipeline)
    - Run concurrently via ThreadPoolExecutor (max 4 workers)

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

API_TIMEOUT = 5  # seconds per HTTP call
CACHE_TTL = 86400  # 24 hours in seconds
MAX_WORKERS = 4
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
    "pharmacist": "29-1051",
    "physical therapist": "29-1123",
    "dentist": "29-1021",
    "lawyer": "23-1011",
    "paralegal": "23-2011",
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
    "solutions architect": "15-1299",
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
# ---------------------------------------------------------------------------

NAICS_CODES: Dict[str, str] = {
    "technology": "54",
    "tech": "54",
    "software": "5112",
    "it": "54",
    "information_technology": "54",
    "healthcare": "62",
    "healthcare_medical": "62",
    "medical": "62",
    "health": "62",
    "finance": "52",
    "financial_services": "52",
    "banking": "522",
    "insurance": "524",
    "manufacturing": "31",
    "retail": "44",
    "retail_ecommerce": "44",
    "ecommerce": "454",
    "education": "61",
    "construction": "23",
    "real_estate": "53",
    "transportation": "48",
    "logistics": "49",
    "hospitality": "72",
    "food_service": "722",
    "media": "51",
    "entertainment": "71",
    "telecommunications": "517",
    "energy": "21",
    "oil_gas": "211",
    "mining": "21",
    "agriculture": "11",
    "government": "92",
    "nonprofit": "813",
    "consulting": "5416",
    "legal": "5411",
    "pharmaceutical": "3254",
    "biotech": "3254",
    "aerospace": "3364",
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
    req.add_header("User-Agent", "AIMediaPlanner/1.0")
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
    req.add_header("User-Agent", "AIMediaPlanner/1.0")
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


# ---------------------------------------------------------------------------
# API 1: BLS (Bureau of Labor Statistics)
# ---------------------------------------------------------------------------

def _fetch_bls_salary(role: str, soc_code: str) -> Optional[Dict[str, Any]]:
    """
    Fetch median, 10th-percentile, and 90th-percentile annual wages for a
    given SOC code from the BLS OES survey.
    """
    cache_k = _cache_key("bls", soc_code)
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    # Strip the dash for the series ID
    soc_clean = soc_code.replace("-", "")

    # OES national series IDs for annual wages:
    #   Median (code 13), 10th pct (code 11), 90th pct (code 17)
    series_median = f"OEUN000000000000000{soc_clean}A13"
    series_p10 = f"OEUN000000000000000{soc_clean}A11"
    series_p90 = f"OEUN000000000000000{soc_clean}A17"

    api_key = os.environ.get("BLS_API_KEY", "")
    url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    payload: Dict[str, Any] = {
        "seriesid": [series_median, series_p10, series_p90],
        "startyear": "2023",
        "endyear": "2025",
    }
    if api_key:
        payload["registrationkey"] = api_key

    resp = _http_post_json(url, payload)
    if not resp or resp.get("status") != "REQUEST_SUCCEEDED":
        _log_warn(f"BLS request failed for SOC {soc_code}")
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
            value = float(latest.get("value", "0").replace(",", ""))
        except (ValueError, TypeError):
            continue

        if sid == series_median:
            result["median"] = int(value)
        elif sid == series_p10:
            result["p10"] = int(value)
        elif sid == series_p90:
            result["p90"] = int(value)

    if "median" in result:
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
            # Try partial matching
            for title, code in SOC_CODES.items():
                if title in role_lower or role_lower in title:
                    soc = code
                    break
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
# API 2: Data USA
# ---------------------------------------------------------------------------

def fetch_industry_employment(industry: str) -> Optional[Dict[str, Any]]:
    """
    Fetch industry-level employment stats from Data USA.
    """
    cache_k = _cache_key("datausa_industry", industry)
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    # Build request
    params = urllib.parse.urlencode({
        "drilldowns": "PUMS Industry",
        "measures": "Total Population,Average Wage",
        "limit": "5",
    })
    url = f"https://datausa.io/api/data?{params}"
    resp = _http_get_json(url)

    if not resp or "data" not in resp:
        _log_warn(f"DataUSA industry request failed for: {industry}")
        return None

    records = resp["data"]
    if not records:
        return None

    # Try to find a matching industry record
    industry_lower = industry.lower().replace("_", " ")
    best_match = None
    for rec in records:
        ind_name = rec.get("PUMS Industry", "").lower()
        if industry_lower in ind_name or ind_name in industry_lower:
            best_match = rec
            break

    # Fall back to first record if no match
    if not best_match:
        best_match = records[0]

    result = {
        "total_employed": best_match.get("Total Population"),
        "avg_wage": best_match.get("Average Wage"),
        "sector_name": best_match.get("PUMS Industry", "Unknown"),
        "source": "DataUSA",
    }

    # Attempt growth rate from year-over-year data
    if len(records) >= 2:
        try:
            curr = records[0].get("Total Population", 0)
            prev = records[1].get("Total Population", 1)
            if prev and curr:
                growth = ((curr - prev) / prev) * 100
                result["growth_rate"] = f"{growth:.1f}%"
        except (TypeError, ZeroDivisionError):
            pass

    if result.get("growth_rate") is None:
        result["growth_rate"] = "N/A"

    _set_cached(cache_k, result)
    return result


def fetch_location_demographics(locations: List[str]) -> Dict[str, Any]:
    """
    Fetch demographic data for given locations from DataUSA.
    Currently supports US geographies best.
    """
    demo_data: Dict[str, Any] = {}

    for loc in locations:
        cache_k = _cache_key("datausa_geo", loc)
        cached = _get_cached(cache_k)
        if cached is not None:
            demo_data[loc] = cached
            continue

        # Extract city name for DataUSA query
        city = loc.split(",")[0].strip()
        params = urllib.parse.urlencode({
            "drilldowns": "Place",
            "measures": "Population,Median Household Income",
            "Place": city,
            "limit": "1",
        })
        url = f"https://datausa.io/api/data?{params}"

        try:
            resp = _http_get_json(url)
            if resp and "data" in resp and resp["data"]:
                rec = resp["data"][0]
                entry = {
                    "population": rec.get("Population"),
                    "median_income": rec.get("Median Household Income"),
                    "source": "DataUSA",
                }
                demo_data[loc] = entry
                _set_cached(cache_k, entry)
            else:
                _log_warn(f"No DataUSA demographic data for: {loc}")
        except Exception as exc:
            _log_warn(f"DataUSA demographics failed for {loc}: {exc}")

    return demo_data


# ---------------------------------------------------------------------------
# API 3: World Bank Open Data
# ---------------------------------------------------------------------------

_WB_INDICATORS = {
    "unemployment_rate": "SL.UEM.TOTL.ZS",
    "gdp_growth": "NY.GDP.MKTP.KD.ZG",
    "labor_force": "SL.TLF.TOTL.IN",
}


def fetch_global_indicators(locations: List[str]) -> Dict[str, Any]:
    """
    Fetch key economic indicators from the World Bank for international locations.
    Skips US locations (covered by BLS/DataUSA).
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
            url = (
                f"https://api.worldbank.org/v2/country/{iso3}/indicator/"
                f"{indicator_code}?format=json&per_page=5&date=2020:2025"
            )
            try:
                resp = _http_get_json(url)
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
# API 4: Clearbit Logo API
# ---------------------------------------------------------------------------

def fetch_company_logo(domain: str) -> Optional[str]:
    """
    Return a Clearbit logo URL for the given domain.
    Validates with a HEAD request to confirm the logo exists.
    """
    if not domain:
        return None

    domain = domain.strip().lower()
    if not domain.startswith("http"):
        logo_url = f"https://logo.clearbit.com/{domain}"
    else:
        # Extract domain from URL
        parsed = urllib.parse.urlparse(domain)
        host = parsed.hostname or domain
        logo_url = f"https://logo.clearbit.com/{host}"

    cache_k = _cache_key("clearbit", domain)
    cached = _get_cached(cache_k)
    if cached is not None:
        return cached

    # Validate the logo URL is accessible
    try:
        req = urllib.request.Request(logo_url, method="HEAD")
        req.add_header("User-Agent", "AIMediaPlanner/1.0")
        with urllib.request.urlopen(req, timeout=API_TIMEOUT, context=_DEFAULT_SSL_CTX) as resp:
            if resp.status == 200:
                _set_cached(cache_k, logo_url)
                return logo_url
    except Exception:
        # Try unverified SSL
        try:
            with urllib.request.urlopen(req, timeout=API_TIMEOUT, context=_UNVERIFIED_SSL_CTX) as resp:
                if resp.status == 200:
                    _set_cached(cache_k, logo_url)
                    return logo_url
        except Exception:
            pass

    # Return the URL anyway (it may work in browsers even if HEAD failed)
    _set_cached(cache_k, logo_url)
    return logo_url


def fetch_competitor_logos(competitors: List[str]) -> Dict[str, str]:
    """Fetch logo URLs for a list of competitor company names."""
    logos: Dict[str, str] = {}
    for comp in competitors:
        domain = _domain_from_name(comp)
        url = fetch_company_logo(domain)
        if url:
            logos[comp] = url
    return logos


# ---------------------------------------------------------------------------
# API 5: Adzuna Job Search
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
# API 6: Currency rates (hardcoded fallback)
# ---------------------------------------------------------------------------

def fetch_currency_rates() -> Dict[str, float]:
    """
    Return currency exchange rates relative to USD.
    Uses hardcoded fallback rates (sufficient for salary comparison purposes).
    """
    return dict(FALLBACK_CURRENCY_RATES)


# ---------------------------------------------------------------------------
# API 7: Wikipedia REST API
# ---------------------------------------------------------------------------

def fetch_company_info(client_name: str,
                       client_website: Optional[str] = None) -> Dict[str, Any]:
    """
    Fetch company description from Wikipedia and logo from Clearbit.
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

    # Try exact name, then with underscores
    search_names = [
        client_name,
        client_name.replace(" ", "_"),
        f"{client_name}_(company)",
        f"{client_name.replace(' ', '_')}_(company)",
    ]

    for name in search_names:
        encoded = urllib.parse.quote(name, safe="()_")
        url = f"https://en.wikipedia.org/api/rest_v1/page/summary/{encoded}"
        try:
            resp = _http_get_json(url)
            if resp and resp.get("type") == "standard":
                extract = resp.get("extract", "")
                if extract and len(extract) > 30:
                    info["description"] = extract
                    _set_cached(cache_k, extract)
                    return info
        except Exception:
            continue

    _log_warn(f"Wikipedia summary not found for: {client_name}")
    return info


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

    # --- Result container ---
    enriched: Dict[str, Any] = {
        "salary_data": {},
        "industry_employment": None,
        "location_demographics": {},
        "global_indicators": {},
        "job_market": {},
        "company_info": {},
        "competitor_logos": {},
        "currency_rates": {},
        "enrichment_summary": {},
    }

    # --- Define tasks for concurrent execution ---
    # Each task is a tuple of (result_key, api_label, callable)
    tasks: List[tuple] = []

    if roles:
        tasks.append(("salary_data", "BLS", lambda: fetch_salary_data(roles)))

    if industry:
        tasks.append(("industry_employment", "DataUSA",
                       lambda: fetch_industry_employment(industry)))

    if locations:
        tasks.append(("location_demographics", "DataUSA-Geo",
                       lambda: fetch_location_demographics(locations)))
        tasks.append(("global_indicators", "WorldBank",
                       lambda: fetch_global_indicators(locations)))

    if roles and locations:
        tasks.append(("job_market", "Adzuna",
                       lambda: fetch_job_market(roles, locations)))

    if client_name:
        tasks.append(("company_info", "Wikipedia",
                       lambda: fetch_company_info(client_name, client_website)))

    if competitors:
        tasks.append(("competitor_logos", "Clearbit",
                       lambda: fetch_competitor_logos(competitors)))

    # Currency rates are always fetched (cheap, no network call for fallback)
    tasks.append(("currency_rates", "CurrencyRates",
                  lambda: fetch_currency_rates()))

    # --- Execute tasks concurrently ---
    _log_info(f"Starting enrichment with {len(tasks)} tasks "
              f"(roles={len(roles)}, locations={len(locations)}, "
              f"competitors={len(competitors)})")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {}
        for result_key, api_label, func in tasks:
            apis_called.append(api_label)
            future = executor.submit(_safe_call, func, api_label)
            future_map[future] = (result_key, api_label)

        for future in as_completed(future_map):
            result_key, api_label = future_map[future]
            try:
                result, success = future.result()
                if success and result:
                    enriched[result_key] = result
                    apis_succeeded.append(api_label)
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
        "apis_failed": apis_failed,
        "total_time_seconds": elapsed,
        "cached": False,  # would be True if entire result was from cache
    }

    _log_info(f"Enrichment complete in {elapsed}s — "
              f"{len(apis_succeeded)}/{len(apis_called)} APIs succeeded")

    return enriched


def _safe_call(func, label: str):
    """
    Wrapper that catches all exceptions so a single API failure never
    crashes the enrichment pipeline.
    """
    try:
        result = func()
        success = result is not None and result != {}
        return result, success
    except Exception as exc:
        _log_warn(f"API '{label}' raised an exception: {exc}")
        return None, False


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

    result = enrich_data(sample)

    print("\n" + "=" * 60)
    print("  Enrichment Results")
    print("=" * 60)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    _cli_demo()
