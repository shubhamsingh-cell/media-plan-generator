"""H-1B/LCA salary intelligence from DOL OFLC public disclosure data.

Provides city-level salary benchmarks for competitive compensation analysis.
Data sourced from certified H-1B Labor Condition Applications (FY2024-2025).

Usage:
    from h1b_data import query_h1b_salaries, get_h1b_top_employers

    result = query_h1b_salaries("software engineer", "san francisco")
    employers = get_h1b_top_employers("data scientist", "new york")
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load H-1B salary intelligence data from JSON
# ---------------------------------------------------------------------------

_H1B_DATA: Dict[str, Any] = {}
_DATA_FILE = Path(__file__).parent / "data" / "h1b_salary_intelligence.json"


def _load_h1b_data() -> Dict[str, Any]:
    """Load H-1B salary data from JSON file.

    Returns cached data on subsequent calls. Thread-safe because dict
    assignment is atomic in CPython and we only read after initial load.
    """
    global _H1B_DATA
    if _H1B_DATA:
        return _H1B_DATA
    try:
        with open(_DATA_FILE, encoding="utf-8") as f:
            _H1B_DATA = json.load(f)
        logger.info(
            "H-1B salary intelligence loaded: %d roles",
            len(_H1B_DATA.get("roles", {})),
        )
    except FileNotFoundError:
        logger.warning("H-1B data file not found: %s", _DATA_FILE)
        _H1B_DATA = {"roles": {}}
    except (json.JSONDecodeError, OSError) as exc:
        logger.error("Failed to load H-1B data: %s", exc, exc_info=True)
        _H1B_DATA = {"roles": {}}
    return _H1B_DATA


# ---------------------------------------------------------------------------
# Role name normalization and matching
# ---------------------------------------------------------------------------

_ROLE_ALIASES: Dict[str, str] = {
    # Software engineering
    "software engineer": "software_engineer",
    "software developer": "software_engineer",
    "swe": "software_engineer",
    "sde": "software_engineer",
    "backend engineer": "software_engineer",
    "frontend engineer": "software_engineer",
    "full stack engineer": "software_engineer",
    "fullstack engineer": "software_engineer",
    "full-stack engineer": "software_engineer",
    "senior software engineer": "software_engineer",
    "staff software engineer": "software_engineer",
    "principal engineer": "software_engineer",
    # Data science
    "data scientist": "data_scientist",
    "ml engineer": "data_scientist",
    "machine learning engineer": "data_scientist",
    "ai engineer": "data_scientist",
    "research scientist": "data_scientist",
    # Product management
    "product manager": "product_manager",
    "pm": "product_manager",
    "senior product manager": "product_manager",
    "group product manager": "product_manager",
    "technical product manager": "product_manager",
    # Data analysis
    "data analyst": "data_analyst",
    "business analyst": "data_analyst",
    "analytics analyst": "data_analyst",
    "bi analyst": "data_analyst",
    "business intelligence analyst": "data_analyst",
    "statistician": "data_analyst",
    # Finance
    "financial analyst": "financial_analyst",
    "finance analyst": "financial_analyst",
    "investment analyst": "financial_analyst",
    "equity analyst": "financial_analyst",
    # Mechanical engineering
    "mechanical engineer": "mechanical_engineer",
    "mech engineer": "mechanical_engineer",
    # Electrical engineering
    "electrical engineer": "electrical_engineer",
    "ee": "electrical_engineer",
    "electronics engineer": "electrical_engineer",
    "hardware engineer": "electrical_engineer",
    # Nursing
    "registered nurse": "registered_nurse",
    "rn": "registered_nurse",
    "nurse": "registered_nurse",
    "staff nurse": "registered_nurse",
    # Accounting
    "accountant": "accountant",
    "auditor": "accountant",
    "cpa": "accountant",
    "senior accountant": "accountant",
    "tax accountant": "accountant",
    # Marketing
    "marketing manager": "marketing_manager",
    "digital marketing manager": "marketing_manager",
    "marketing director": "marketing_manager",
    # Project management
    "project manager": "project_manager",
    "program manager": "project_manager",
    "pmo": "project_manager",
    "scrum master": "project_manager",
    # Civil engineering
    "civil engineer": "civil_engineer",
    "structural engineer": "civil_engineer",
    # HR
    "hr manager": "hr_manager",
    "human resources manager": "hr_manager",
    "people manager": "hr_manager",
    "talent acquisition manager": "hr_manager",
    # Consulting
    "management consultant": "management_consultant",
    "consultant": "management_consultant",
    "strategy consultant": "management_consultant",
    "business consultant": "management_consultant",
    "management analyst": "management_consultant",
    # UX/Design
    "ux designer": "ux_designer",
    "ui designer": "ux_designer",
    "product designer": "ux_designer",
    "web designer": "ux_designer",
    "interaction designer": "ux_designer",
}

# Metro name normalization
_METRO_ALIASES: Dict[str, str] = {
    "sf": "san_francisco",
    "san fran": "san_francisco",
    "bay area": "san_francisco",
    "san francisco": "san_francisco",
    "san francisco, ca": "san_francisco",
    "nyc": "new_york",
    "new york": "new_york",
    "new york city": "new_york",
    "new york, ny": "new_york",
    "manhattan": "new_york",
    "san jose": "san_jose",
    "silicon valley": "san_jose",
    "san jose, ca": "san_jose",
    "seattle": "seattle",
    "seattle, wa": "seattle",
    "austin": "austin",
    "austin, tx": "austin",
    "boston": "boston",
    "boston, ma": "boston",
    "los angeles": "los_angeles",
    "la": "los_angeles",
    "los angeles, ca": "los_angeles",
    "chicago": "chicago",
    "chicago, il": "chicago",
    "washington dc": "washington_dc",
    "washington, dc": "washington_dc",
    "dc": "washington_dc",
    "washington d.c.": "washington_dc",
    "denver": "denver",
    "denver, co": "denver",
    "dallas": "dallas",
    "dallas, tx": "dallas",
    "dfw": "dallas",
    "dallas-fort worth": "dallas",
    "atlanta": "atlanta",
    "atlanta, ga": "atlanta",
    "detroit": "detroit",
    "detroit, mi": "detroit",
    "minneapolis": "minneapolis",
    "minneapolis, mn": "minneapolis",
    "phoenix": "phoenix",
    "phoenix, az": "phoenix",
    "san diego": "san_diego",
    "san diego, ca": "san_diego",
    "raleigh": "raleigh",
    "raleigh, nc": "raleigh",
    "research triangle": "raleigh",
    "portland": "portland",
    "portland, or": "portland",
    "pittsburgh": "pittsburgh",
    "pittsburgh, pa": "pittsburgh",
    "charlotte": "charlotte",
    "charlotte, nc": "charlotte",
    "houston": "houston",
    "houston, tx": "houston",
}


def _normalize_role(role: str) -> Optional[str]:
    """Normalize a role string to a canonical H-1B role key.

    Args:
        role: Raw role title (e.g., "Senior Software Engineer", "SWE").

    Returns:
        Canonical role key or None if no match found.
    """
    role_lower = role.lower().strip()

    # Direct alias match
    if role_lower in _ROLE_ALIASES:
        return _ROLE_ALIASES[role_lower]

    # Substring match in aliases
    for alias, key in _ROLE_ALIASES.items():
        if alias in role_lower or role_lower in alias:
            return key

    # Word overlap match
    role_words = set(re.split(r"\W+", role_lower))
    best_key: Optional[str] = None
    best_score = 0
    for alias, key in _ROLE_ALIASES.items():
        alias_words = set(re.split(r"\W+", alias))
        overlap = len(role_words & alias_words)
        if overlap > best_score:
            best_score = overlap
            best_key = key
    if best_score >= 1:
        return best_key

    return None


def _normalize_metro(location: str) -> Optional[str]:
    """Normalize a location string to a canonical metro key.

    Args:
        location: Raw location (e.g., "San Francisco, CA", "NYC").

    Returns:
        Canonical metro key or None if no match found.
    """
    loc_lower = location.lower().strip()
    # Remove state suffixes like ", CA" for matching
    loc_clean = re.sub(r",?\s*(usa?|united states)$", "", loc_lower).strip()

    # Direct alias match
    if loc_clean in _METRO_ALIASES:
        return _METRO_ALIASES[loc_clean]

    # Try without trailing state abbreviation
    loc_no_state = re.sub(r",\s*[a-z]{2}$", "", loc_clean).strip()
    if loc_no_state in _METRO_ALIASES:
        return _METRO_ALIASES[loc_no_state]

    # Substring match
    for alias, key in _METRO_ALIASES.items():
        if alias in loc_clean or loc_clean in alias:
            return key

    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def query_h1b_salaries(role: str, location: str = "") -> Dict[str, Any]:
    """Query H-1B/LCA salary intelligence for a role and optional metro.

    Returns salary percentiles (P10, P25, median, P75, P90), sample size,
    top H-1B employers, and city-level breakdowns when available.

    Args:
        role: Job title (e.g., "Software Engineer", "Data Scientist").
        location: Optional metro area (e.g., "San Francisco", "NYC").

    Returns:
        Dict with salary data, or error message if role not found.
    """
    data = _load_h1b_data()
    roles = data.get("roles", {})

    role_key = _normalize_role(role)
    if not role_key or role_key not in roles:
        # Return available roles for guidance
        available = [r.get("title", k) for k, r in roles.items()]
        return {
            "error": f"No H-1B salary data found for '{role}'",
            "available_roles": available,
            "source": "DOL OFLC LCA Disclosure Data",
        }

    role_data = roles[role_key]
    result: Dict[str, Any] = {
        "source": "DOL OFLC LCA Disclosure Data (FY2024-2025)",
        "role": role_data.get("title", role),
        "soc_code": role_data.get("soc_code", ""),
    }

    # National data always included
    national = role_data.get("national", {})
    result["national"] = {
        "median_salary": national.get("median"),
        "p25": national.get("p25"),
        "p75": national.get("p75"),
        "p10": national.get("p10"),
        "p90": national.get("p90"),
        "total_h1b_lcas": national.get("sample"),
    }

    metros = role_data.get("metros", {})

    # If location specified, return that metro's data
    if location:
        metro_key = _normalize_metro(location)
        if metro_key and metro_key in metros:
            metro = metros[metro_key]
            result["metro"] = {
                "city": metro_key.replace("_", " ").title(),
                "median_salary": metro.get("median"),
                "p25": metro.get("p25"),
                "p75": metro.get("p75"),
                "p10": metro.get("p10"),
                "p90": metro.get("p90"),
                "sample_size": metro.get("sample"),
                "top_h1b_employers": metro.get("top_employers", []),
            }
            # Calculate premium vs national
            nat_median = national.get("median", 0)
            metro_median = metro.get("median", 0)
            if nat_median and metro_median:
                premium_pct = round((metro_median / nat_median - 1) * 100, 1)
                result["metro"][
                    "premium_vs_national"
                ] = f"{'+' if premium_pct > 0 else ''}{premium_pct}%"
        else:
            result["metro_not_found"] = location
            # Include top metros as reference
            result["available_metros"] = _get_top_metros(metros, n=5)
    else:
        # No location: include top 5 metros by median salary
        result["top_metros"] = _get_top_metros(metros, n=8)

    return result


def _get_top_metros(metros: Dict[str, Any], n: int = 5) -> List[Dict[str, Any]]:
    """Get top N metros by median salary from metros dict."""
    sorted_metros = sorted(
        metros.items(),
        key=lambda x: x[1].get("median", 0),
        reverse=True,
    )
    return [
        {
            "city": k.replace("_", " ").title(),
            "median": v.get("median"),
            "p25": v.get("p25"),
            "p75": v.get("p75"),
            "sample": v.get("sample"),
            "top_employers": v.get("top_employers", [])[:3],
        }
        for k, v in sorted_metros[:n]
    ]


def get_h1b_top_employers(role: str, location: str = "") -> Dict[str, Any]:
    """Get top H-1B sponsoring employers for a role and location.

    Args:
        role: Job title.
        location: Optional metro area.

    Returns:
        Dict with employer list and context.
    """
    data = _load_h1b_data()
    roles = data.get("roles", {})

    role_key = _normalize_role(role)
    if not role_key or role_key not in roles:
        return {"error": f"No H-1B employer data for '{role}'"}

    role_data = roles[role_key]
    result: Dict[str, Any] = {
        "source": "DOL OFLC LCA Disclosure Data",
        "role": role_data.get("title", role),
    }

    metros = role_data.get("metros", {})

    if location:
        metro_key = _normalize_metro(location)
        if metro_key and metro_key in metros:
            metro = metros[metro_key]
            result["city"] = metro_key.replace("_", " ").title()
            result["top_employers"] = metro.get("top_employers", [])
            result["median_salary"] = metro.get("median")
            result["sample_size"] = metro.get("sample")
            return result

    # National top employers from the national H-1B data
    # Aggregate unique employers across all metros
    all_employers: Dict[str, int] = {}
    for metro in metros.values():
        for emp in metro.get("top_employers", []):
            all_employers[emp] = all_employers.get(emp, 0) + 1
    sorted_employers = sorted(all_employers.items(), key=lambda x: x[1], reverse=True)
    result["top_employers"] = [e[0] for e in sorted_employers[:10]]
    result["coverage"] = "National (all metros)"
    return result


def compare_h1b_salaries_across_cities(
    role: str, cities: Optional[List[str]] = None
) -> Dict[str, Any]:
    """Compare H-1B salaries for a role across multiple cities.

    Args:
        role: Job title.
        cities: List of city names. If None, returns all available.

    Returns:
        Dict with city-by-city comparison.
    """
    data = _load_h1b_data()
    roles = data.get("roles", {})

    role_key = _normalize_role(role)
    if not role_key or role_key not in roles:
        return {"error": f"No H-1B data for '{role}'"}

    role_data = roles[role_key]
    metros = role_data.get("metros", {})
    national = role_data.get("national", {})

    result: Dict[str, Any] = {
        "source": "DOL OFLC LCA Disclosure Data (FY2024-2025)",
        "role": role_data.get("title", role),
        "national_median": national.get("median"),
        "cities": [],
    }

    if cities:
        for city in cities:
            metro_key = _normalize_metro(city)
            if metro_key and metro_key in metros:
                metro = metros[metro_key]
                nat_med = national.get("median", 1)
                m_med = metro.get("median", 0)
                result["cities"].append(
                    {
                        "city": metro_key.replace("_", " ").title(),
                        "median": m_med,
                        "p25": metro.get("p25"),
                        "p75": metro.get("p75"),
                        "premium_vs_national": (
                            f"{round((m_med / nat_med - 1) * 100, 1)}%"
                            if nat_med
                            else "N/A"
                        ),
                        "top_employers": metro.get("top_employers", [])[:3],
                    }
                )
    else:
        # All available metros
        for k, v in sorted(
            metros.items(), key=lambda x: x[1].get("median", 0), reverse=True
        ):
            nat_med = national.get("median", 1)
            m_med = v.get("median", 0)
            result["cities"].append(
                {
                    "city": k.replace("_", " ").title(),
                    "median": m_med,
                    "p25": v.get("p25"),
                    "p75": v.get("p75"),
                    "premium_vs_national": (
                        f"{round((m_med / nat_med - 1) * 100, 1)}%"
                        if nat_med
                        else "N/A"
                    ),
                    "top_employers": v.get("top_employers", [])[:3],
                }
            )

    return result


def get_available_h1b_roles() -> List[str]:
    """Return list of available role titles in the H-1B dataset."""
    data = _load_h1b_data()
    return [v.get("title", k) for k, v in data.get("roles", {}).items()]


def get_available_h1b_metros(role: str = "") -> List[str]:
    """Return list of available metro areas, optionally for a specific role."""
    data = _load_h1b_data()
    roles = data.get("roles", {})

    if role:
        role_key = _normalize_role(role)
        if role_key and role_key in roles:
            metros = roles[role_key].get("metros", {})
            return [k.replace("_", " ").title() for k in metros]

    # All unique metros across all roles
    all_metros: set = set()
    for role_data in roles.values():
        for k in role_data.get("metros", {}):
            all_metros.add(k.replace("_", " ").title())
    return sorted(all_metros)
