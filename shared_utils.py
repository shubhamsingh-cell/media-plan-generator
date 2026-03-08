"""Shared utility functions used across multiple modules.

Consolidates duplicated logic that previously existed in multiple files:
- Budget parsing (was in app.py x2, ppt_generator.py x1)
- Industry label map (was in app.py, ppt_generator.py)
- Location standardization helpers (was inline in app.py)

Single source of truth. All modules import from here.
"""

from __future__ import annotations

import re
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Industry Label Map  (single source of truth)
# ─────────────────────────────────────────────────────────────

INDUSTRY_LABEL_MAP: Dict[str, str] = {
    "healthcare_medical": "Healthcare & Medical",
    "blue_collar_trades": "Blue Collar / Skilled Trades",
    "maritime_marine": "Maritime & Marine",
    "military_recruitment": "Military Recruitment",
    "tech_engineering": "Technology & Engineering",
    "general_entry_level": "General / Entry-Level",
    "legal_services": "Legal Services",
    "finance_banking": "Finance & Banking",
    "mental_health": "Mental Health & Behavioral",
    "retail_consumer": "Retail & Consumer",
    "aerospace_defense": "Aerospace & Defense",
    "pharma_biotech": "Pharma & Biotech",
    "energy_utilities": "Energy & Utilities",
    "insurance": "Insurance",
    "telecommunications": "Telecommunications",
    "automotive": "Automotive & Manufacturing",
    "food_beverage": "Food & Beverage",
    "logistics_supply_chain": "Logistics & Supply Chain",
    "hospitality_travel": "Hospitality & Travel",
    "media_entertainment": "Media & Entertainment",
    "construction_real_estate": "Construction & Real Estate",
    "education": "Education",
}

# ─────────────────────────────────────────────────────────────
# Budget Parsing  (single source of truth)
# ─────────────────────────────────────────────────────────────

_SUFFIX_MULTIPLIERS = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}


def parse_budget(budget_input, *, default: float = 100_000.0) -> float:
    """Parse a budget string or number into a float value.

    Handles all formats the platform encounters:
      - Numeric:        50000, 250000.0
      - Currency:       "$50,000", "$250,000 - $500,000"
      - Suffixed:       "50K", "1.5M", "2B"
      - Range:          "$50,000 - $250,000"  -> midpoint
      - Text:           "< $50,000", "million", "500k"
      - Comparison:     "< $50,000"  -> 50000
      - None/empty:     returns *default*

    Parameters
    ----------
    budget_input : str, int, float, or None
        Raw budget value from any source (form field, API, chat).
    default : float
        Fallback value when parsing fails entirely. Default 100,000.

    Returns
    -------
    float
        Parsed numeric budget value, always > 0 when input is non-empty.
    """
    # Already numeric
    if isinstance(budget_input, (int, float)):
        return float(budget_input) if budget_input > 0 else default

    if not budget_input:
        return default

    bstr = str(budget_input).strip()
    if not bstr:
        return default

    # Clean currency symbols and whitespace
    clean = bstr.replace(",", "").replace("$", "").replace("USD", "").replace("usd", "").strip()

    # 1. K/M/B suffix:  "50K", "1.5M", "2B"
    km_match = re.match(r'^[<>~\s]*([\d.]+)\s*([KkMmBb])\b', clean)
    if km_match:
        num_part = float(km_match.group(1))
        suffix = km_match.group(2).upper()
        return num_part * _SUFFIX_MULTIPLIERS[suffix]

    # 2. Extract all numbers >= 1000 (filter noise like "3 months")
    all_nums = re.findall(r'[\d]+', clean)
    parsed_nums = [int(n) for n in all_nums if int(n) >= 1000]

    if len(parsed_nums) >= 2:
        # Range like "$250,000 - $500,000" -> midpoint
        return (parsed_nums[0] + parsed_nums[1]) / 2.0
    elif len(parsed_nums) == 1:
        return float(parsed_nums[0])

    # 3. Decimal numbers (e.g., "1.5" without suffix, or small values)
    decimal_match = re.search(r'([\d.]+)', clean)
    if decimal_match:
        val = float(decimal_match.group(1))
        if val > 0:
            return val

    # 4. Text-based keywords
    lower = clean.lower()
    if "million" in lower or "1m" in lower:
        return 1_000_000.0
    if "500k" in lower:
        return 500_000.0
    if "250k" in lower:
        return 250_000.0
    if "100k" in lower:
        return 100_000.0

    # 5. Final fallback
    return default


def parse_budget_display(budget_input) -> Optional[float]:
    """Parse budget for display contexts where None is acceptable.

    Unlike parse_budget(), returns None instead of a default when
    the input is genuinely unparseable or empty. Used by PPT generator
    where missing budget should suppress the slide rather than show
    a fabricated number.
    """
    if isinstance(budget_input, (int, float)):
        return float(budget_input) if budget_input > 0 else None
    if not budget_input:
        return None
    result = parse_budget(budget_input, default=0.0)
    return result if result > 0 else None


# ─────────────────────────────────────────────────────────────
# Location Standardization Constants
# ─────────────────────────────────────────────────────────────

COUNTRY_CANONICAL: Dict[str, str] = {
    "us": "United States", "usa": "United States", "united states": "United States",
    "united states of america": "United States", "u.s.": "United States",
    "u.s.a.": "United States",
    "uk": "United Kingdom", "united kingdom": "United Kingdom",
    "great britain": "United Kingdom",
    "uae": "United Arab Emirates", "united arab emirates": "United Arab Emirates",
    "india": "India", "china": "China", "japan": "Japan", "germany": "Germany",
    "france": "France", "canada": "Canada", "australia": "Australia", "brazil": "Brazil",
    "mexico": "Mexico", "singapore": "Singapore", "ireland": "Ireland", "israel": "Israel",
    "south korea": "South Korea", "netherlands": "Netherlands",
    "the netherlands": "Netherlands",
    "switzerland": "Switzerland", "sweden": "Sweden", "spain": "Spain", "italy": "Italy",
    "poland": "Poland", "philippines": "Philippines", "new zealand": "New Zealand",
    "south africa": "South Africa", "saudi arabia": "Saudi Arabia",
    "hong kong": "Hong Kong",
    "taiwan": "Taiwan", "indonesia": "Indonesia", "malaysia": "Malaysia",
    "thailand": "Thailand",
    "vietnam": "Vietnam", "norway": "Norway", "denmark": "Denmark", "finland": "Finland",
    "belgium": "Belgium", "austria": "Austria", "portugal": "Portugal",
    "czech republic": "Czech Republic",
    "romania": "Romania", "colombia": "Colombia", "argentina": "Argentina",
    "chile": "Chile",
    "peru": "Peru", "egypt": "Egypt", "nigeria": "Nigeria", "kenya": "Kenya",
    "pakistan": "Pakistan",
    "bangladesh": "Bangladesh", "sri lanka": "Sri Lanka", "costa rica": "Costa Rica",
}

US_STATES_CANONICAL: Dict[str, str] = {
    "ca": "California", "california": "California",
    "ny": "New York", "new york": "New York",
    "tx": "Texas", "texas": "Texas",
    "fl": "Florida", "florida": "Florida",
    "il": "Illinois", "illinois": "Illinois",
    "pa": "Pennsylvania", "pennsylvania": "Pennsylvania",
    "oh": "Ohio", "ohio": "Ohio",
    "ga": "Georgia", "georgia": "Georgia",
    "nc": "North Carolina", "north carolina": "North Carolina",
    "mi": "Michigan", "michigan": "Michigan",
    "nj": "New Jersey", "new jersey": "New Jersey",
    "va": "Virginia", "virginia": "Virginia",
    "wa": "Washington", "washington": "Washington",
    "az": "Arizona", "arizona": "Arizona",
    "ma": "Massachusetts", "massachusetts": "Massachusetts",
    "tn": "Tennessee", "tennessee": "Tennessee",
    "in": "Indiana", "indiana": "Indiana",
    "mo": "Missouri", "missouri": "Missouri",
    "md": "Maryland", "maryland": "Maryland",
    "wi": "Wisconsin", "wisconsin": "Wisconsin",
    "co": "Colorado", "colorado": "Colorado",
    "mn": "Minnesota", "minnesota": "Minnesota",
    "sc": "South Carolina", "south carolina": "South Carolina",
    "al": "Alabama", "alabama": "Alabama",
    "la": "Louisiana", "louisiana": "Louisiana",
    "ky": "Kentucky", "kentucky": "Kentucky",
    "or": "Oregon", "oregon": "Oregon",
    "ok": "Oklahoma", "oklahoma": "Oklahoma",
    "ct": "Connecticut", "connecticut": "Connecticut",
    "ut": "Utah", "utah": "Utah",
    "ia": "Iowa", "iowa": "Iowa",
    "nv": "Nevada", "nevada": "Nevada",
    "ar": "Arkansas", "arkansas": "Arkansas",
    "ms": "Mississippi", "mississippi": "Mississippi",
    "ks": "Kansas", "kansas": "Kansas",
    "nm": "New Mexico", "new mexico": "New Mexico",
    "ne": "Nebraska", "nebraska": "Nebraska",
    "id": "Idaho", "idaho": "Idaho",
    "wv": "West Virginia", "west virginia": "West Virginia",
    "hi": "Hawaii", "hawaii": "Hawaii",
    "nh": "New Hampshire", "new hampshire": "New Hampshire",
    "me": "Maine", "maine": "Maine",
    "mt": "Montana", "montana": "Montana",
    "ri": "Rhode Island", "rhode island": "Rhode Island",
    "de": "Delaware", "delaware": "Delaware",
    "sd": "South Dakota", "south dakota": "South Dakota",
    "nd": "North Dakota", "north dakota": "North Dakota",
    "ak": "Alaska", "alaska": "Alaska",
    "vt": "Vermont", "vermont": "Vermont",
    "wy": "Wyoming", "wyoming": "Wyoming",
    "dc": "Washington, D.C.", "washington dc": "Washington, D.C.",
    "washington d.c.": "Washington, D.C.",
}


def standardize_location(loc_str: str) -> str:
    """Standardize a location string: proper casing for city, state, country.

    Handles comma-separated parts like "san francisco, ca, us" -> "San Francisco, California, United States".
    """
    if not isinstance(loc_str, str) or not loc_str.strip():
        return loc_str
    parts = [p.strip() for p in loc_str.split(",")]
    standardized = []
    for part in parts:
        lower = part.lower().strip()
        if lower in COUNTRY_CANONICAL:
            standardized.append(COUNTRY_CANONICAL[lower])
        elif lower in US_STATES_CANONICAL:
            standardized.append(US_STATES_CANONICAL[lower])
        else:
            standardized.append(part.strip().title())
    return ", ".join(standardized)
