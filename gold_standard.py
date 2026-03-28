from __future__ import annotations

"""
Gold Standard Quality Gates for Media Plan Generator.

Enforces world-class quality standards across all generated media plans:
1. City-level supply-demand data
2. Security clearance segmentation
3. Competitor mapping per city/role
4. Difficulty level framework (junior/mid/senior/staff)
5. Channel strategy with traditional + non-traditional splits
6. Multi-tier budget breakdowns (creative/media/contingency)
7. Activation event calendars (seasonal hiring waves)

Each gate is a pure function that enriches ``data`` in-place or returns
enrichment dicts.  app.py calls ``apply_all_quality_gates(data)`` after
enrichment and budget allocation, before Excel/PPT generation.
"""

import datetime
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. City-Level Supply-Demand Data
# ---------------------------------------------------------------------------

# Median salary multipliers by metro (relative to national average = 1.0)
_CITY_SALARY_MULTIPLIERS: dict[str, float] = {
    "san francisco": 1.45,
    "new york": 1.38,
    "seattle": 1.32,
    "boston": 1.28,
    "los angeles": 1.22,
    "washington": 1.25,
    "chicago": 1.10,
    "austin": 1.12,
    "denver": 1.15,
    "atlanta": 1.05,
    "dallas": 1.08,
    "houston": 1.06,
    "miami": 1.04,
    "phoenix": 0.98,
    "minneapolis": 1.08,
    "philadelphia": 1.12,
    "san diego": 1.18,
    "portland": 1.14,
    "nashville": 1.02,
    "raleigh": 1.06,
    "charlotte": 1.00,
    "detroit": 0.95,
    "st louis": 0.92,
    "kansas city": 0.94,
    "indianapolis": 0.93,
    "columbus": 0.96,
    "pittsburgh": 0.94,
    "tampa": 0.97,
    "orlando": 0.95,
    "salt lake city": 1.02,
    "richmond": 0.99,
    "sacramento": 1.10,
    "san antonio": 0.92,
}

# Hiring difficulty by metro (1-10 scale, 10 = hardest)
_CITY_HIRING_DIFFICULTY: dict[str, float] = {
    "san francisco": 8.5,
    "new york": 7.8,
    "seattle": 8.2,
    "boston": 7.5,
    "los angeles": 7.0,
    "washington": 7.2,
    "chicago": 6.5,
    "austin": 7.8,
    "denver": 7.0,
    "atlanta": 6.2,
    "dallas": 6.0,
    "houston": 5.8,
    "miami": 5.5,
    "phoenix": 5.0,
    "minneapolis": 6.0,
    "philadelphia": 5.8,
    "detroit": 4.5,
    "tampa": 5.2,
}

# Supply classification labels
_SUPPLY_TIERS: list[tuple[float, str]] = [
    (8.0, "critically_scarce"),
    (6.5, "tight"),
    (4.5, "balanced"),
    (0.0, "abundant"),
]


def enrich_city_level_data(data: dict) -> dict:
    """Produce per-city salary, hiring difficulty, and supply segmentation.

    Reads ``data['locations']`` and ``data['_enriched']`` for API-sourced
    data, fills gaps with the built-in metro benchmarks.

    Returns:
        Dict keyed by city name with salary_multiplier, hiring_difficulty,
        supply_tier, and salary_range_estimate.
    """
    locations_raw = data.get("locations") or []
    enriched = data.get("_enriched") or {}
    synthesized = data.get("_synthesized") or {}

    # Try to get a national average salary from enrichment
    national_avg_salary: float = 0.0
    salary_range_str = str(
        synthesized.get("salary_range") or enriched.get("salary_range") or ""
    )
    salary_match = re.search(r"\$?([\d,]+)", salary_range_str.replace(",", ""))
    if salary_match:
        try:
            national_avg_salary = float(salary_match.group(1))
        except (ValueError, TypeError):
            pass
    if national_avg_salary <= 0:
        national_avg_salary = 75_000.0  # Fallback US average

    city_data: dict[str, dict[str, Any]] = {}

    for loc in (locations_raw if isinstance(locations_raw, list) else []):
        city_name = ""
        if isinstance(loc, str):
            city_name = loc.split(",")[0].strip()
        elif isinstance(loc, dict):
            city_name = str(loc.get("city") or loc.get("name") or "").strip()
        if not city_name:
            continue

        city_key = city_name.lower()
        multiplier = _CITY_SALARY_MULTIPLIERS.get(city_key, 1.0)
        difficulty = _CITY_HIRING_DIFFICULTY.get(city_key, 5.5)

        # Determine supply tier
        supply_tier = "balanced"
        for threshold, label in _SUPPLY_TIERS:
            if difficulty >= threshold:
                supply_tier = label
                break

        est_salary = round(national_avg_salary * multiplier)

        city_data[city_name] = {
            "salary_multiplier": multiplier,
            "estimated_salary": est_salary,
            "salary_range": f"${est_salary - 10_000:,.0f} - ${est_salary + 15_000:,.0f}",
            "hiring_difficulty": round(difficulty, 1),
            "supply_tier": supply_tier,
            "cost_of_living_index": round(multiplier * 100, 1),
        }

    return city_data


# ---------------------------------------------------------------------------
# 2. Security Clearance Segmentation
# ---------------------------------------------------------------------------

_DEFENSE_KEYWORDS: set[str] = {
    "defense",
    "military",
    "dod",
    "government",
    "federal",
    "intelligence",
    "cleared",
    "clearance",
    "classified",
    "secret",
    "top secret",
    "ts/sci",
    "aerospace",
    "pentagon",
    "army",
    "navy",
    "air force",
    "marine",
    "coast guard",
    "cia",
    "nsa",
    "fbi",
    "dhs",
    "homeland",
}

_CLEARANCE_TYPES: list[dict[str, Any]] = [
    {
        "level": "Top Secret / SCI",
        "code": "TS_SCI",
        "salary_premium_pct": 25,
        "time_to_fill_weeks": 16,
        "candidate_pool_reduction_pct": 85,
        "budget_multiplier": 2.5,
        "channels": [
            "ClearanceJobs",
            "ClearedConnections",
            "Intelligence Careers",
            "USAJobs",
        ],
    },
    {
        "level": "Top Secret",
        "code": "TS",
        "salary_premium_pct": 18,
        "time_to_fill_weeks": 12,
        "candidate_pool_reduction_pct": 75,
        "budget_multiplier": 2.0,
        "channels": ["ClearanceJobs", "USAJobs", "Indeed (cleared filter)", "LinkedIn"],
    },
    {
        "level": "Secret",
        "code": "SECRET",
        "salary_premium_pct": 10,
        "time_to_fill_weeks": 8,
        "candidate_pool_reduction_pct": 50,
        "budget_multiplier": 1.5,
        "channels": ["ClearanceJobs", "USAJobs", "Indeed", "LinkedIn"],
    },
    {
        "level": "Public Trust",
        "code": "PUBLIC_TRUST",
        "salary_premium_pct": 5,
        "time_to_fill_weeks": 6,
        "candidate_pool_reduction_pct": 20,
        "budget_multiplier": 1.2,
        "channels": ["USAJobs", "Indeed", "LinkedIn", "GovernmentJobs.com"],
    },
]


_CLEARANCE_ELIGIBLE_INDUSTRIES: set[str] = {
    "defense",
    "aerospace",
    "government",
    "intelligence",
    "federal",
    "military",
    "aerospace_defense",
    "government_federal",
    "defense_aerospace",
    "national_security",
}


def _is_clearance_eligible_industry(industry: str) -> bool:
    """Check if the industry qualifies for security clearance segmentation.

    Only defense, aerospace, government, intelligence, federal, and military
    industries should show clearance data. Healthcare, retail, tech, etc.
    must never show clearance sections.
    """
    industry_lower = industry.lower().strip()
    # Direct match
    if any(eligible in industry_lower for eligible in _CLEARANCE_ELIGIBLE_INDUSTRIES):
        return True
    return False


def detect_clearance_requirements(data: dict) -> dict[str, Any] | None:
    """Detect if the plan involves defense/government roles needing clearance.

    Only returns clearance segmentation when the industry is defense-related
    (defense, aerospace, government, intelligence, federal, military).
    Non-defense industries (healthcare, retail, tech, etc.) are skipped
    even if keywords like 'secret' or 'classified' appear in the brief.

    Returns:
        Clearance segmentation dict if defense-related, else None.
    """
    industry = str(data.get("industry") or "").lower()
    brief = str(data.get("use_case") or data.get("brief") or "").lower()
    client = str(data.get("client_name") or "").lower()
    roles_raw = data.get("target_roles") or data.get("roles") or []

    # Gate 1: Industry must be clearance-eligible
    # This prevents healthcare, retail, tech, etc. from showing clearance data
    if not _is_clearance_eligible_industry(industry):
        return None

    # Collect all text to scan
    all_text = f"{industry} {brief} {client}"
    for r in (roles_raw if isinstance(roles_raw, list) else []):
        if isinstance(r, str):
            all_text += f" {r.lower()}"
        elif isinstance(r, dict):
            all_text += f" {str(r.get('title') or '').lower()}"

    # Gate 2: Check for defense keywords in the combined text
    matches = [kw for kw in _DEFENSE_KEYWORDS if kw in all_text]
    if not matches:
        return None

    # Determine the likely clearance level
    if any(kw in all_text for kw in ("ts/sci", "sci", "compartmented")):
        primary_clearance = _CLEARANCE_TYPES[0]
    elif any(kw in all_text for kw in ("top secret",)):
        primary_clearance = _CLEARANCE_TYPES[1]
    elif any(kw in all_text for kw in ("secret", "classified", "cleared")):
        primary_clearance = _CLEARANCE_TYPES[2]
    else:
        primary_clearance = _CLEARANCE_TYPES[3]

    return {
        "is_defense_related": True,
        "detected_keywords": matches[:5],
        "primary_clearance": primary_clearance,
        "all_clearance_tiers": _CLEARANCE_TYPES,
        "recommendations": [
            f"Primary clearance level: {primary_clearance['level']} -- "
            f"expect {primary_clearance['time_to_fill_weeks']} week average time-to-fill",
            f"Budget multiplier: {primary_clearance['budget_multiplier']}x due to "
            f"{primary_clearance['candidate_pool_reduction_pct']}% smaller candidate pool",
            f"Salary premium: +{primary_clearance['salary_premium_pct']}% over commercial equivalent",
            f"Recommended channels: {', '.join(primary_clearance['channels'])}",
        ],
    }


# ---------------------------------------------------------------------------
# 3. Competitor Mapping Per City/Role
# ---------------------------------------------------------------------------

# Industry-to-competitor mapping (top employers by sector + metro)
# Each industry has city-level employer lists so competitor output varies by location.
_INDUSTRY_TOP_EMPLOYERS: dict[str, dict[str, list[str]]] = {
    "technology": {
        "_national": ["Google", "Microsoft", "Amazon", "Meta", "Apple", "Netflix"],
        "san francisco": ["Salesforce", "Uber", "Stripe", "Airbnb", "Figma", "Slack"],
        "seattle": ["Amazon", "Microsoft", "Boeing", "Expedia", "Zillow", "Tableau"],
        "new york": [
            "Google",
            "JPMorgan",
            "Bloomberg",
            "Goldman Sachs",
            "Meta",
            "Datadog",
        ],
        "austin": ["Dell", "Tesla", "Oracle", "Samsung", "Indeed", "Bumble"],
        "boston": ["HubSpot", "Wayfair", "Akamai", "Toast", "DraftKings", "Rapid7"],
        "los angeles": ["Snap", "SpaceX", "Hulu", "TikTok", "Riot Games", "Scopely"],
        "denver": ["Arrow Electronics", "Ping Identity", "Ibotta", "Guild Education"],
        "chicago": ["Grubhub", "Groupon", "Braintree", "Avant", "ActiveCampaign"],
        "atlanta": ["NCR", "Mailchimp", "Cardlytics", "OneTrust", "Calendly"],
        "dallas": [
            "AT&T",
            "Texas Instruments",
            "Sabre",
            "Match Group",
            "Tyler Technologies",
        ],
        "miami": ["Chewy", "CareCloud", "Kaseya", "Magic Leap"],
        "phoenix": ["GoDaddy", "Axon", "Carvana", "InfusionSoft"],
        "washington": ["Amazon (HQ2)", "Palantir", "MicroStrategy", "Appian"],
        "minneapolis": [
            "Target (tech)",
            "UnitedHealth (tech)",
            "Best Buy (tech)",
            "C.H. Robinson",
        ],
        "philadelphia": ["Comcast", "Susquehanna", "SEI Investments", "Sidecar"],
        "detroit": ["Rocket Companies", "StockX", "Duo Security", "Ford (tech)"],
    },
    "healthcare_medical": {
        "_national": [
            "UnitedHealth",
            "HCA Healthcare",
            "Kaiser Permanente",
            "CVS Health",
            "Ascension",
        ],
        "boston": ["Mass General", "Boston Children's", "Dana-Farber", "Brigham"],
        "houston": ["MD Anderson", "Memorial Hermann", "Houston Methodist", "Baylor"],
        "chicago": ["Northwestern Medicine", "Advocate", "Rush", "Lurie Children's"],
        "new york": ["NYU Langone", "Mount Sinai", "NewYork-Presbyterian", "Northwell"],
        "los angeles": ["Cedars-Sinai", "UCLA Health", "Keck Medicine", "City of Hope"],
        "atlanta": ["Emory Healthcare", "Piedmont", "Grady Health", "Wellstar"],
        "dallas": [
            "UT Southwestern",
            "Baylor Scott & White",
            "Parkland",
            "Medical City",
        ],
        "denver": ["UCHealth", "SCL Health", "Children's Colorado", "National Jewish"],
        "phoenix": ["Mayo Clinic AZ", "Banner Health", "HonorHealth", "Dignity Health"],
        "seattle": ["UW Medicine", "Virginia Mason", "Swedish Health", "Providence"],
        "san francisco": [
            "UCSF Health",
            "Stanford Health",
            "Sutter Health",
            "Dignity Health",
        ],
        "miami": [
            "Baptist Health",
            "Jackson Health",
            "Mount Sinai Miami",
            "Cleveland Clinic FL",
        ],
        "philadelphia": ["Penn Medicine", "Jefferson Health", "Temple Health", "CHOP"],
        "minneapolis": ["Mayo Clinic", "Allina Health", "Fairview", "HealthPartners"],
    },
    "finance_banking": {
        "_national": ["JPMorgan", "Goldman Sachs", "Morgan Stanley", "Bank of America"],
        "new york": ["Citadel", "Two Sigma", "BlackRock", "Citi", "BNY Mellon"],
        "charlotte": ["Bank of America", "Wells Fargo", "Truist", "Ally Financial"],
        "chicago": ["Citadel", "CME Group", "Northern Trust", "Morningstar"],
        "san francisco": ["Charles Schwab", "Visa", "Wells Fargo", "First Republic"],
        "boston": ["Fidelity", "State Street", "Wellington", "Putnam"],
        "dallas": ["CBRE", "Comerica", "Hilltop Holdings", "NexBank"],
        "atlanta": [
            "SunTrust",
            "Intercontinental Exchange",
            "Invesco",
            "Global Payments",
        ],
        "denver": ["TIAA", "Janus Henderson", "Arrow Financial", "CoBank"],
    },
    "retail_consumer": {
        "_national": ["Walmart", "Amazon", "Target", "Costco", "Home Depot"],
        "new york": ["Macy's", "Ralph Lauren", "Estee Lauder", "L'Oreal USA"],
        "chicago": ["Walgreens", "McDonald's", "Kellogg's", "Kraft Heinz"],
        "san francisco": ["Gap", "Levi's", "Williams-Sonoma", "Restoration Hardware"],
        "dallas": ["7-Eleven", "Neiman Marcus", "JCPenney", "Tuesday Morning"],
        "atlanta": ["Home Depot", "Coca-Cola", "Arby's", "Genuine Parts"],
        "minneapolis": ["Target", "Best Buy", "General Mills", "3M"],
    },
    "aerospace_defense": {
        "_national": [
            "Lockheed Martin",
            "Raytheon",
            "Northrop Grumman",
            "Boeing",
            "General Dynamics",
        ],
        "washington": ["Booz Allen", "Leidos", "SAIC", "ManTech"],
        "huntsville": ["Boeing", "Northrop Grumman", "Raytheon", "Dynetics"],
        "dallas": ["Lockheed Martin", "L3Harris", "Bell Textron", "Elbit Systems"],
        "san diego": ["General Atomics", "Northrop Grumman", "BAE Systems"],
        "denver": ["Lockheed Martin (Space)", "Ball Aerospace", "Raytheon"],
    },
    "transportation": {
        "_national": ["UPS", "FedEx", "XPO Logistics", "J.B. Hunt", "C.H. Robinson"],
        "chicago": ["United Airlines", "Boeing", "Hub Group", "Echo Global"],
        "dallas": ["Southwest Airlines", "American Airlines", "BNSF Railway"],
        "atlanta": ["Delta Air Lines", "UPS", "Norfolk Southern", "Ryder"],
        "seattle": ["Alaska Airlines", "Expeditors", "TOTE Maritime"],
        "miami": ["Ryder", "World Fuel Services", "Brightline"],
    },
    "manufacturing": {
        "_national": [
            "General Electric",
            "3M",
            "Honeywell",
            "Caterpillar",
            "Deere & Co",
        ],
        "detroit": ["Ford", "GM", "Stellantis", "BorgWarner", "Lear"],
        "chicago": ["Abbott", "Caterpillar", "Illinois Tool Works", "Baxter"],
        "houston": ["Dow Chemical", "LyondellBasell", "Baker Hughes"],
        "minneapolis": ["3M", "Medtronic", "General Mills", "Polaris"],
        "pittsburgh": ["US Steel", "PPG Industries", "Alcoa", "Wabtec"],
        "phoenix": [
            "ON Semiconductor",
            "Microchip Technology",
            "Benchmark Electronics",
        ],
        "portland": ["Intel", "Daimler Trucks NA", "Precision Castparts", "FLIR"],
        "san francisco": ["Tesla Fremont", "Lam Research", "Applied Materials", "KLA"],
        "dallas": ["Toyota NA", "Celanese", "Jacobs Engineering", "Lennox"],
    },
    "energy": {
        "_national": [
            "ExxonMobil",
            "Chevron",
            "ConocoPhillips",
            "NextEra Energy",
            "Duke Energy",
        ],
        "houston": [
            "ExxonMobil",
            "Chevron",
            "ConocoPhillips",
            "Phillips 66",
            "Halliburton",
        ],
        "dallas": [
            "Energy Transfer",
            "Pioneer Natural Resources",
            "Vistra",
            "Targa Resources",
        ],
        "denver": ["Xcel Energy", "Ovintiv", "SM Energy", "Antero Resources"],
        "san francisco": ["PG&E", "Sunrun", "SunPower", "ChargePoint"],
        "pittsburgh": ["EQT", "CNX Resources", "Westinghouse", "Consol Energy"],
        "chicago": ["Exelon", "Invenergy", "NiSource", "Ameren"],
    },
    "education": {
        "_national": ["Pearson", "McGraw-Hill", "Chegg", "Coursera", "2U"],
        "boston": [
            "Harvard",
            "MIT",
            "Northeastern",
            "Houghton Mifflin",
            "Boston University",
        ],
        "new york": ["Columbia", "NYU", "Scholastic", "Kaplan", "EdX"],
        "san francisco": [
            "Stanford",
            "Udemy",
            "Coursera",
            "Khan Academy",
            "Lambda School",
        ],
        "chicago": ["University of Chicago", "Northwestern", "Loyola", "DePaul"],
        "austin": ["UT Austin", "Aceable", "A Cloud Guru", "Enroll.com"],
        "los angeles": ["USC", "UCLA", "Caltech", "GoGuardian", "Age of Learning"],
    },
    "logistics": {
        "_national": ["FedEx", "UPS", "XPO Logistics", "C.H. Robinson", "DHL Americas"],
        "atlanta": ["UPS", "Delta Air Lines", "Manhattan Associates", "Veritiv"],
        "dallas": [
            "Southwest Airlines",
            "American Airlines",
            "BNSF Railway",
            "Transplace",
        ],
        "chicago": ["United Airlines", "Hub Group", "Echo Global", "Coyote Logistics"],
        "seattle": ["Amazon Logistics", "Expeditors", "Alaska Airlines", "Convoy"],
        "houston": ["Sysco", "AIT Worldwide", "Enterprise Products Logistics"],
        "minneapolis": [
            "C.H. Robinson",
            "Target Logistics",
            "Penske MN",
            "Digi International",
        ],
    },
}

# Aliases for flexible industry matching (user input -> canonical key)
_INDUSTRY_ALIASES: dict[str, str] = {
    "tech": "technology",
    "software": "technology",
    "it": "technology",
    "information technology": "technology",
    "saas": "technology",
    "health": "healthcare",
    "healthcare_medical": "healthcare",
    "medical": "healthcare",
    "hospital": "healthcare",
    "pharma": "healthcare",
    "pharmaceutical": "healthcare",
    "biotech": "healthcare",
    "banking": "finance",
    "finance_banking": "finance",
    "financial": "finance",
    "financial services": "finance",
    "fintech": "finance",
    "insurance": "finance",
    "consumer": "retail",
    "retail_consumer": "retail",
    "ecommerce": "retail",
    "e-commerce": "retail",
    "cpg": "retail",
    "food": "retail",
    "defense": "aerospace_defense",
    "aerospace": "aerospace_defense",
    "military": "aerospace_defense",
    "government": "aerospace_defense",
    "industrial": "manufacturing",
    "automotive": "manufacturing",
    "oil": "energy",
    "gas": "energy",
    "oil and gas": "energy",
    "renewables": "energy",
    "utilities": "energy",
    "supply chain": "logistics",
    "transportation": "logistics",
    "shipping": "logistics",
    "freight": "logistics",
    "edtech": "education",
    "higher education": "education",
}


def _resolve_industry_key(raw_industry: str) -> str:
    """Resolve a raw industry string to a canonical key in _INDUSTRY_TOP_EMPLOYERS.

    Uses alias table first, then substring matching, then falls back to empty string.
    """
    industry = raw_industry.strip().lower()
    # 1) Direct alias lookup
    if industry in _INDUSTRY_ALIASES:
        return _INDUSTRY_ALIASES[industry]
    # 2) Direct key match
    if industry in _INDUSTRY_TOP_EMPLOYERS:
        return industry
    # 3) Substring match (e.g. "healthcare" in "healthcare_medical")
    for key in _INDUSTRY_TOP_EMPLOYERS:
        if key in industry or industry in key:
            return key
    # 4) Alias substring match (e.g. "tech startup" contains "tech")
    for alias, canonical in _INDUSTRY_ALIASES.items():
        if alias in industry:
            return canonical
    return ""


# ── Role-type difficulty profiles ──
# Each role pattern maps to a full difficulty profile with base difficulty,
# seniority override, time-to-fill, and supply level.  This replaces generic
# seniority-only classification with role-specific intelligence.
_ROLE_DIFFICULTY_MAP: dict[str, dict[str, Any]] = {
    # -- Technology --
    "software engineer": {
        "seniority": "mid",
        "base_difficulty": 7,
        "avg_ttf_days": 42,
        "supply_level": "moderate",
    },
    "senior software engineer": {
        "seniority": "senior",
        "base_difficulty": 8,
        "avg_ttf_days": 55,
        "supply_level": "scarce",
    },
    "staff engineer": {
        "seniority": "staff",
        "base_difficulty": 9,
        "avg_ttf_days": 75,
        "supply_level": "very_scarce",
    },
    "principal engineer": {
        "seniority": "staff",
        "base_difficulty": 9,
        "avg_ttf_days": 75,
        "supply_level": "very_scarce",
    },
    "data scientist": {
        "seniority": "mid-senior",
        "base_difficulty": 8,
        "avg_ttf_days": 50,
        "supply_level": "scarce",
    },
    "ml engineer": {
        "seniority": "senior",
        "base_difficulty": 9,
        "avg_ttf_days": 60,
        "supply_level": "very_scarce",
    },
    "machine learning engineer": {
        "seniority": "senior",
        "base_difficulty": 9,
        "avg_ttf_days": 60,
        "supply_level": "very_scarce",
    },
    "ai engineer": {
        "seniority": "senior",
        "base_difficulty": 9,
        "avg_ttf_days": 60,
        "supply_level": "very_scarce",
    },
    "devops engineer": {
        "seniority": "mid-senior",
        "base_difficulty": 7.5,
        "avg_ttf_days": 45,
        "supply_level": "moderate-scarce",
    },
    "sre": {
        "seniority": "mid-senior",
        "base_difficulty": 7.5,
        "avg_ttf_days": 45,
        "supply_level": "moderate-scarce",
    },
    "site reliability engineer": {
        "seniority": "mid-senior",
        "base_difficulty": 7.5,
        "avg_ttf_days": 45,
        "supply_level": "moderate-scarce",
    },
    "platform engineer": {
        "seniority": "mid-senior",
        "base_difficulty": 7.5,
        "avg_ttf_days": 45,
        "supply_level": "moderate-scarce",
    },
    "cloud engineer": {
        "seniority": "mid-senior",
        "base_difficulty": 7.5,
        "avg_ttf_days": 45,
        "supply_level": "moderate-scarce",
    },
    "security engineer": {
        "seniority": "mid-senior",
        "base_difficulty": 8,
        "avg_ttf_days": 50,
        "supply_level": "scarce",
    },
    "frontend developer": {
        "seniority": "mid",
        "base_difficulty": 6,
        "avg_ttf_days": 35,
        "supply_level": "moderate",
    },
    "frontend engineer": {
        "seniority": "mid",
        "base_difficulty": 6,
        "avg_ttf_days": 35,
        "supply_level": "moderate",
    },
    "backend developer": {
        "seniority": "mid",
        "base_difficulty": 7,
        "avg_ttf_days": 40,
        "supply_level": "moderate",
    },
    "backend engineer": {
        "seniority": "mid",
        "base_difficulty": 7,
        "avg_ttf_days": 40,
        "supply_level": "moderate",
    },
    "full stack developer": {
        "seniority": "mid",
        "base_difficulty": 6.5,
        "avg_ttf_days": 38,
        "supply_level": "moderate",
    },
    "fullstack developer": {
        "seniority": "mid",
        "base_difficulty": 6.5,
        "avg_ttf_days": 38,
        "supply_level": "moderate",
    },
    "data engineer": {
        "seniority": "mid-senior",
        "base_difficulty": 7.5,
        "avg_ttf_days": 45,
        "supply_level": "moderate-scarce",
    },
    "qa engineer": {
        "seniority": "mid",
        "base_difficulty": 5,
        "avg_ttf_days": 30,
        "supply_level": "moderate",
    },
    "product manager": {
        "seniority": "mid-senior",
        "base_difficulty": 6.5,
        "avg_ttf_days": 40,
        "supply_level": "moderate",
    },
    "ux designer": {
        "seniority": "mid",
        "base_difficulty": 6,
        "avg_ttf_days": 35,
        "supply_level": "moderate",
    },
    "solutions architect": {
        "seniority": "senior",
        "base_difficulty": 8,
        "avg_ttf_days": 55,
        "supply_level": "scarce",
    },
    # -- Healthcare --
    "nurse": {
        "seniority": "mid",
        "base_difficulty": 6,
        "avg_ttf_days": 30,
        "supply_level": "moderate",
    },
    "registered nurse": {
        "seniority": "mid",
        "base_difficulty": 6,
        "avg_ttf_days": 30,
        "supply_level": "moderate",
    },
    "nurse practitioner": {
        "seniority": "senior",
        "base_difficulty": 7.5,
        "avg_ttf_days": 45,
        "supply_level": "moderate-scarce",
    },
    "physician": {
        "seniority": "senior",
        "base_difficulty": 9,
        "avg_ttf_days": 90,
        "supply_level": "very_scarce",
    },
    "surgeon": {
        "seniority": "senior",
        "base_difficulty": 9.5,
        "avg_ttf_days": 120,
        "supply_level": "extremely_scarce",
    },
    "pharmacist": {
        "seniority": "mid",
        "base_difficulty": 6,
        "avg_ttf_days": 35,
        "supply_level": "moderate",
    },
    "medical assistant": {
        "seniority": "entry",
        "base_difficulty": 3.5,
        "avg_ttf_days": 18,
        "supply_level": "abundant",
    },
    "physical therapist": {
        "seniority": "mid",
        "base_difficulty": 6.5,
        "avg_ttf_days": 40,
        "supply_level": "moderate",
    },
    # -- Sales --
    "sdr": {
        "seniority": "entry",
        "base_difficulty": 4,
        "avg_ttf_days": 20,
        "supply_level": "abundant",
    },
    "bdr": {
        "seniority": "entry",
        "base_difficulty": 4,
        "avg_ttf_days": 20,
        "supply_level": "abundant",
    },
    "sales development": {
        "seniority": "entry",
        "base_difficulty": 4,
        "avg_ttf_days": 20,
        "supply_level": "abundant",
    },
    "account executive": {
        "seniority": "mid",
        "base_difficulty": 5.5,
        "avg_ttf_days": 30,
        "supply_level": "moderate",
    },
    "sales manager": {
        "seniority": "mid-senior",
        "base_difficulty": 6,
        "avg_ttf_days": 35,
        "supply_level": "moderate",
    },
    "sales director": {
        "seniority": "senior",
        "base_difficulty": 7.5,
        "avg_ttf_days": 50,
        "supply_level": "moderate-scarce",
    },
    # -- Marketing --
    "marketing manager": {
        "seniority": "mid",
        "base_difficulty": 5,
        "avg_ttf_days": 30,
        "supply_level": "moderate",
    },
    "marketing coordinator": {
        "seniority": "entry",
        "base_difficulty": 3.5,
        "avg_ttf_days": 18,
        "supply_level": "abundant",
    },
    "content writer": {
        "seniority": "mid",
        "base_difficulty": 4.5,
        "avg_ttf_days": 25,
        "supply_level": "moderate",
    },
    "growth marketing": {
        "seniority": "mid-senior",
        "base_difficulty": 6.5,
        "avg_ttf_days": 38,
        "supply_level": "moderate",
    },
    # -- Finance --
    "accountant": {
        "seniority": "mid",
        "base_difficulty": 5,
        "avg_ttf_days": 28,
        "supply_level": "moderate",
    },
    "financial analyst": {
        "seniority": "mid",
        "base_difficulty": 5.5,
        "avg_ttf_days": 30,
        "supply_level": "moderate",
    },
    "actuary": {
        "seniority": "senior",
        "base_difficulty": 8.5,
        "avg_ttf_days": 65,
        "supply_level": "very_scarce",
    },
    "investment banker": {
        "seniority": "mid-senior",
        "base_difficulty": 7,
        "avg_ttf_days": 45,
        "supply_level": "moderate-scarce",
    },
    # -- Operations / General --
    "project manager": {
        "seniority": "mid",
        "base_difficulty": 5,
        "avg_ttf_days": 28,
        "supply_level": "moderate",
    },
    "operations manager": {
        "seniority": "mid",
        "base_difficulty": 5,
        "avg_ttf_days": 28,
        "supply_level": "moderate",
    },
    "hr manager": {
        "seniority": "mid",
        "base_difficulty": 5,
        "avg_ttf_days": 28,
        "supply_level": "moderate",
    },
    "recruiter": {
        "seniority": "mid",
        "base_difficulty": 5,
        "avg_ttf_days": 25,
        "supply_level": "moderate",
    },
    "customer success": {
        "seniority": "mid",
        "base_difficulty": 4.5,
        "avg_ttf_days": 25,
        "supply_level": "moderate",
    },
    # -- Hourly / Entry --
    "cashier": {
        "seniority": "entry",
        "base_difficulty": 2.5,
        "avg_ttf_days": 12,
        "supply_level": "abundant",
    },
    "warehouse associate": {
        "seniority": "entry",
        "base_difficulty": 3,
        "avg_ttf_days": 14,
        "supply_level": "abundant",
    },
    "retail associate": {
        "seniority": "entry",
        "base_difficulty": 3,
        "avg_ttf_days": 14,
        "supply_level": "abundant",
    },
    "customer service": {
        "seniority": "entry",
        "base_difficulty": 3,
        "avg_ttf_days": 15,
        "supply_level": "abundant",
    },
    # -- Executive (VP+) --
    "vp": {
        "seniority": "executive",
        "base_difficulty": 9.5,
        "avg_ttf_days": 120,
        "supply_level": "extremely_scarce",
    },
    "vice president": {
        "seniority": "executive",
        "base_difficulty": 9.5,
        "avg_ttf_days": 120,
        "supply_level": "extremely_scarce",
    },
    "cto": {
        "seniority": "executive",
        "base_difficulty": 10,
        "avg_ttf_days": 150,
        "supply_level": "extremely_scarce",
    },
    "cfo": {
        "seniority": "executive",
        "base_difficulty": 10,
        "avg_ttf_days": 150,
        "supply_level": "extremely_scarce",
    },
    "ceo": {
        "seniority": "executive",
        "base_difficulty": 10,
        "avg_ttf_days": 180,
        "supply_level": "extremely_scarce",
    },
}

# Location-based difficulty modifiers (added to role base_difficulty)
_LOCATION_DIFFICULTY_MODIFIERS: dict[str, float] = {
    "san francisco": 1.5,
    "new york": 1.5,
    "nyc": 1.5,
    "manhattan": 1.5,
    "austin": 1.0,
    "seattle": 1.0,
    "boston": 1.0,
    "los angeles": 0.5,
    "washington": 0.5,
    "denver": 0.5,
    "chicago": 0.0,
    "atlanta": 0.0,
    "dallas": 0.0,
    "houston": 0.0,
    "miami": 0.0,
    "phoenix": -0.5,
    "detroit": -0.5,
    "st louis": -0.5,
    "kansas city": -0.5,
    "indianapolis": -0.5,
    "remote": -0.5,
}


def _lookup_role_difficulty(role_title: str) -> dict[str, Any] | None:
    """Look up a role title against _ROLE_DIFFICULTY_MAP.

    Uses longest-match-first to prefer "senior software engineer" over
    "software engineer" when both could match.

    Returns:
        A copy of the matching profile dict, or None if no match.
    """
    title_lower = role_title.lower().strip()
    # Sort keys by length descending so more specific patterns match first
    for pattern in sorted(_ROLE_DIFFICULTY_MAP, key=len, reverse=True):
        if pattern in title_lower:
            return dict(_ROLE_DIFFICULTY_MAP[pattern])
    # S27: Fuzzy fallback -- partial word match for unmatched roles
    title_words = set(title_lower.split())
    for pattern in sorted(_ROLE_DIFFICULTY_MAP, key=len, reverse=True):
        pattern_words = set(pattern.split())
        if pattern_words & title_words:  # any overlapping words
            return dict(_ROLE_DIFFICULTY_MAP[pattern])
    return None


def _get_location_modifier(locations: list[str]) -> tuple[float, str]:
    """Return (modifier, matched_location) for the highest-modifier location.

    If no known location matches, returns (0.0, "").
    """
    best_mod = 0.0
    best_loc = ""
    for loc in locations:
        loc_lower = loc.lower().strip()
        for city, mod in _LOCATION_DIFFICULTY_MODIFIERS.items():
            if city in loc_lower:
                if mod > best_mod or not best_loc:
                    best_mod = mod
                    best_loc = city
    return best_mod, best_loc


def _get_role_difficulty_modifier(role_title: str) -> tuple[float, int]:
    """Return (difficulty_modifier, time_to_fill_modifier) for a role title.

    Derives modifiers from _ROLE_DIFFICULTY_MAP relative to a mid-level
    baseline (difficulty=5, ttf=35).
    """
    profile = _lookup_role_difficulty(role_title)
    if profile:
        return (profile["base_difficulty"] - 5.0, profile["avg_ttf_days"] - 35)
    return (0.0, 0)


def build_competitor_map(data: dict, city_data: dict) -> dict[str, Any]:
    """Build per-city/role competitor mapping with role-based differentiation.

    Args:
        data: Plan generation data dict.
        city_data: Output from enrich_city_level_data().

    Returns:
        Dict with per-city competitor lists, hiring intensity estimates,
        and per-role difficulty scores within each city.
    """
    raw_industry = str(data.get("industry") or "general_entry_level")
    roles_raw = data.get("target_roles") or data.get("roles") or []
    enriched = data.get("_enriched") or {}

    # Resolve industry via alias table + substring matching
    resolved_key = _resolve_industry_key(raw_industry)
    industry_employers: dict[str, list[str]] = (
        _INDUSTRY_TOP_EMPLOYERS.get(resolved_key, {}) if resolved_key else {}
    )

    national_competitors = industry_employers.get("_national", [])

    # ── Parse role titles for difficulty modifiers ──
    role_titles: list[str] = []
    for r in (roles_raw if isinstance(roles_raw, list) else [str(roles_raw)]):
        if isinstance(r, str) and r.strip():
            role_titles.append(r.strip())
        elif isinstance(r, dict):
            t = str(r.get("title") or "").strip()
            if t:
                role_titles.append(t)

    competitor_map: dict[str, Any] = {}
    for city_name in city_data:
        city_key = city_name.lower()
        local_competitors = industry_employers.get(city_key, [])

        # Merge local-first + national, dedup -- local employers appear first
        # S27: Prepend "(National)" when competitors are industry-level only (no city-specific data)
        if not local_competitors and national_competitors:
            all_competitors = [f"(National) {c}" for c in national_competitors[:8]]
        else:
            all_competitors = list(
                dict.fromkeys(local_competitors + national_competitors)
            )[:8]

        base_difficulty = city_data[city_name].get("hiring_difficulty", 5.5)

        # ── Per-role difficulty within this city ──
        role_difficulties: list[dict[str, Any]] = []
        for role_title in role_titles:
            diff_mod, ttf_mod = _get_role_difficulty_modifier(role_title)
            adjusted = max(1.0, min(10.0, base_difficulty + diff_mod))
            role_difficulties.append(
                {
                    "role": role_title,
                    "difficulty": round(adjusted, 1),
                    "difficulty_out_of_10": f"{round(adjusted, 1)}/10",
                    "time_to_fill_modifier_days": ttf_mod,
                }
            )

        # City-level aggregate difficulty = average of role difficulties (or base)
        if role_difficulties:
            avg_difficulty = sum(rd["difficulty"] for rd in role_difficulties) / len(
                role_difficulties
            )
        else:
            avg_difficulty = base_difficulty

        intensity = (
            "high"
            if avg_difficulty >= 7.0
            else ("moderate" if avg_difficulty >= 5.0 else "low")
        )

        competitor_map[city_name] = {
            "top_employers": all_competitors,
            "local_competitors": local_competitors,
            "national_competitors": national_competitors[:5],
            "hiring_intensity": intensity,
            "avg_difficulty": round(avg_difficulty, 1),
            "role_difficulties": role_difficulties,
            "estimated_competing_postings": _estimate_competing_postings(
                avg_difficulty, len(roles_raw)
            ),
        }

    # Also add a national-level entry
    if national_competitors:
        competitor_map["_national"] = {
            "top_employers": national_competitors,
            "hiring_intensity": "moderate",
        }

    return competitor_map


def _estimate_competing_postings(difficulty: float, num_roles: int) -> int:
    """Estimate how many competing job postings exist for similar roles."""
    base = int(difficulty * 150)
    return max(50, base * max(1, num_roles))


# ---------------------------------------------------------------------------
# 4. Difficulty Level Framework
# ---------------------------------------------------------------------------

_SENIORITY_KEYWORDS: dict[str, list[str]] = {
    "intern": ["intern", "trainee", "apprentice", "co-op", "student"],
    "junior": ["junior", "jr", "entry", "associate", "assistant", "i ", " i,"],
    "mid": ["mid", "intermediate", " ii ", " ii,", "specialist"],
    "senior": ["senior", "sr", "lead", "principal", " iii ", " iii,", "staff"],
    "director": ["director", "head of", "vp", "vice president"],
    "executive": ["chief", "cto", "cfo", "coo", "cio", "ceo", "partner", "evp", "svp"],
}

_DIFFICULTY_PROFILES: dict[str, dict[str, Any]] = {
    "intern": {
        "complexity_score": 1,
        "avg_time_to_fill_days": 14,
        "budget_weight": 0.4,
        "channel_emphasis": "campus_recruiting",
        "description": "Entry-level / internship -- high applicant volume, fast fill",
    },
    "junior": {
        "complexity_score": 2,
        "avg_time_to_fill_days": 25,
        "budget_weight": 0.6,
        "channel_emphasis": "job_boards",
        "description": "Junior / early career -- moderate volume, standard process",
    },
    "mid": {
        "complexity_score": 4,
        "avg_time_to_fill_days": 35,
        "budget_weight": 1.0,
        "channel_emphasis": "balanced",
        "description": "Mid-level -- balanced sourcing across channels",
    },
    "senior": {
        "complexity_score": 6,
        "avg_time_to_fill_days": 50,
        "budget_weight": 1.8,
        "channel_emphasis": "niche_boards",
        "description": "Senior / lead -- passive sourcing heavy, niche channels",
    },
    "director": {
        "complexity_score": 8,
        "avg_time_to_fill_days": 70,
        "budget_weight": 2.5,
        "channel_emphasis": "executive_search",
        "description": "Director / VP -- executive channels, headhunters",
    },
    "executive": {
        "complexity_score": 10,
        "avg_time_to_fill_days": 90,
        "budget_weight": 3.5,
        "channel_emphasis": "executive_search",
        "description": "C-suite / executive -- retained search firms, network",
    },
}


def classify_difficulty(data: dict) -> list[dict[str, Any]]:
    """Classify each role by seniority and difficulty using role-type profiles.

    Uses _ROLE_DIFFICULTY_MAP for role-type-specific base difficulty, then
    applies location modifiers from _LOCATION_DIFFICULTY_MODIFIERS.  Falls
    back to seniority-keyword detection when no role-type profile matches.

    Returns:
        List of dicts with role title, seniority, complexity, time-to-fill,
        supply level, and location modifier details.
    """
    roles_raw = data.get("target_roles") or data.get("roles") or []
    # Gather locations for location modifier
    locations: list[str] = []
    for loc_key in ("locations", "location", "cities"):
        loc_val = data.get(loc_key)
        if loc_val:
            if isinstance(loc_val, list):
                locations.extend(str(v) for v in loc_val if v)
            else:
                locations.append(str(loc_val))

    loc_modifier, loc_matched = _get_location_modifier(locations)

    results: list[dict[str, Any]] = []

    for r in (roles_raw if isinstance(roles_raw, list) else [str(roles_raw)]):
        title = ""
        if isinstance(r, str):
            title = r.strip()
        elif isinstance(r, dict):
            title = str(r.get("title") or "").strip()
        if not title:
            continue

        # 1) Try role-type-specific profile first
        role_profile = _lookup_role_difficulty(title)

        if role_profile:
            # Role-type profile found -- use its values directly
            base_diff = float(role_profile["base_difficulty"])
            adjusted_difficulty = max(1.0, min(10.0, base_diff + loc_modifier))
            seniority = str(role_profile["seniority"])
            ttf = int(role_profile["avg_ttf_days"])
            supply = str(role_profile["supply_level"])

            # Derive budget weight and channel emphasis from difficulty
            if adjusted_difficulty >= 9:
                budget_weight = 3.0
                channel_emphasis = "executive_search"
            elif adjusted_difficulty >= 7:
                budget_weight = 1.8
                channel_emphasis = "niche_boards"
            elif adjusted_difficulty >= 5:
                budget_weight = 1.0
                channel_emphasis = "balanced"
            else:
                budget_weight = 0.6
                channel_emphasis = "job_boards"

            # Upgrade supply level if location modifier pushes difficulty up
            if loc_modifier >= 1.0 and supply == "moderate":
                supply = "moderate-scarce"
            elif loc_modifier >= 1.5 and supply in ("moderate-scarce", "scarce"):
                supply = "very_scarce"

            # Build description
            description = (
                f"{seniority.title()} level -- "
                f"difficulty {adjusted_difficulty:.1f}/10, "
                f"~{ttf} day fill time"
            )

            results.append(
                {
                    "role_title": title,
                    "seniority_level": seniority,
                    "complexity_score": round(adjusted_difficulty, 1),
                    "avg_time_to_fill_days": ttf,
                    "budget_weight": budget_weight,
                    "channel_emphasis": channel_emphasis,
                    "supply_level": supply,
                    "location_modifier": loc_modifier,
                    "location_matched": loc_matched,
                    "description": description,
                    "role_profile_matched": True,
                }
            )
        else:
            # 2) Fallback: seniority keyword detection + generic profiles
            title_lower = f" {title.lower()} "
            detected_level = "mid"

            for level, keywords in _SENIORITY_KEYWORDS.items():
                matched = False
                for kw in keywords:
                    if kw in title_lower:
                        detected_level = level
                        matched = True
                        break
                if matched:
                    break

            profile = _DIFFICULTY_PROFILES[detected_level]
            base_complexity = float(profile["complexity_score"])
            adjusted_complexity = max(1.0, min(10.0, base_complexity + loc_modifier))
            ttf = int(profile["avg_time_to_fill_days"])

            # Derive supply level from adjusted complexity
            if adjusted_complexity >= 9:
                supply = "very_scarce"
            elif adjusted_complexity >= 7:
                supply = "scarce"
            elif adjusted_complexity >= 5:
                supply = "moderate"
            else:
                supply = "abundant"

            results.append(
                {
                    "role_title": title,
                    "seniority_level": detected_level,
                    "complexity_score": round(adjusted_complexity, 1),
                    "avg_time_to_fill_days": ttf,
                    "budget_weight": profile["budget_weight"],
                    "channel_emphasis": profile["channel_emphasis"],
                    "supply_level": supply,
                    "location_modifier": loc_modifier,
                    "location_matched": loc_matched,
                    "description": profile["description"],
                    "role_profile_matched": False,
                }
            )

    return results


# ---------------------------------------------------------------------------
# 5. Channel Strategy with Traditional + Non-Traditional Splits
# ---------------------------------------------------------------------------

_TRADITIONAL_CHANNELS: dict[str, dict[str, Any]] = {
    "Indeed": {
        "type": "job_board",
        "reach": "mass",
        "best_for": ["hourly", "mid", "junior"],
    },
    "LinkedIn": {
        "type": "professional_network",
        "reach": "professional",
        "best_for": ["mid", "senior", "executive"],
    },
    "ZipRecruiter": {
        "type": "job_board",
        "reach": "mass",
        "best_for": ["hourly", "junior", "mid"],
    },
    "Glassdoor": {
        "type": "employer_branding",
        "reach": "professional",
        "best_for": ["mid", "senior"],
    },
    "CareerBuilder": {
        "type": "job_board",
        "reach": "mass",
        "best_for": ["hourly", "junior"],
    },
    "Monster": {"type": "job_board", "reach": "mass", "best_for": ["junior", "mid"]},
}

_NON_TRADITIONAL_CHANNELS: dict[str, dict[str, Any]] = {
    "GitHub Jobs / ReadMe": {
        "type": "developer",
        "reach": "niche",
        "best_for": ["technology"],
        "industry": "technology",
    },
    "Stack Overflow Talent": {
        "type": "developer",
        "reach": "niche",
        "best_for": ["technology"],
        "industry": "technology",
    },
    "AngelList / Wellfound": {
        "type": "startup",
        "reach": "niche",
        "best_for": ["technology", "startup"],
    },
    "Behance / Dribbble": {
        "type": "design",
        "reach": "niche",
        "best_for": ["creative", "design"],
    },
    "Meetup.com Sponsorships": {
        "type": "events",
        "reach": "local",
        "best_for": ["technology", "creative"],
    },
    "Reddit (r/forhire, industry subs)": {
        "type": "community",
        "reach": "niche",
        "best_for": ["technology", "creative"],
    },
    "Discord Communities": {
        "type": "community",
        "reach": "niche",
        "best_for": ["technology", "gaming"],
    },
    "Slack Communities (e.g., #jobs)": {
        "type": "community",
        "reach": "niche",
        "best_for": ["technology"],
    },
    "TikTok Recruitment": {
        "type": "social",
        "reach": "gen_z",
        "best_for": ["hourly", "retail", "hospitality"],
    },
    "Handshake": {
        "type": "campus",
        "reach": "campus",
        "best_for": ["intern", "junior"],
    },
    "Hired.com": {
        "type": "marketplace",
        "reach": "professional",
        "best_for": ["technology", "senior"],
    },
    "Hacker News (Who's Hiring)": {
        "type": "community",
        "reach": "niche",
        "best_for": ["technology"],
    },
    "Health eCareers": {
        "type": "niche_board",
        "reach": "niche",
        "best_for": ["healthcare"],
        "industry": "healthcare_medical",
    },
    "Nurse.com": {
        "type": "niche_board",
        "reach": "niche",
        "best_for": ["healthcare"],
        "industry": "healthcare_medical",
    },
    "ClearanceJobs": {
        "type": "niche_board",
        "reach": "niche",
        "best_for": ["defense"],
        "industry": "aerospace_defense",
    },
    "eFinancialCareers": {
        "type": "niche_board",
        "reach": "niche",
        "best_for": ["finance"],
        "industry": "finance_banking",
    },
    "Dice": {
        "type": "niche_board",
        "reach": "niche",
        "best_for": ["technology"],
        "industry": "technology",
    },
}


def build_channel_strategy(
    data: dict, difficulty_results: list[dict]
) -> dict[str, Any]:
    """Build channel strategy with traditional vs non-traditional split.

    Args:
        data: Plan generation data dict.
        difficulty_results: Output from classify_difficulty().

    Returns:
        Dict with traditional_channels, non_traditional_channels, split_pct, recommendations.
    """
    industry = str(data.get("industry") or "").lower()
    # Normalize seniority levels: expand "mid-senior" to both "mid" and "senior",
    # map "staff" -> "senior", "entry" -> "junior" for channel best_for matching.
    seniority_levels: list[str] = []
    for d in difficulty_results:
        raw = str(d.get("seniority_level") or "mid")
        if "-" in raw:
            seniority_levels.extend(raw.split("-"))
        elif raw == "staff":
            seniority_levels.append("senior")
        elif raw == "entry":
            seniority_levels.append("junior")
        else:
            seniority_levels.append(raw)
    seniority_levels = list(set(seniority_levels))  # dedup

    # Pick relevant traditional channels
    trad_picks: list[dict[str, Any]] = []
    for name, info in _TRADITIONAL_CHANNELS.items():
        relevance = sum(1 for s in seniority_levels if s in info["best_for"])
        if relevance > 0 or not seniority_levels:
            trad_picks.append({"name": name, "relevance_score": relevance, **info})

    # Pick relevant non-traditional channels
    nontrad_picks: list[dict[str, Any]] = []
    for name, info in _NON_TRADITIONAL_CHANNELS.items():
        # Match by industry
        ch_industry = info.get("industry") or ""
        industry_match = (
            ch_industry in industry or industry in ch_industry if ch_industry else True
        )
        # Match by seniority
        seniority_match = (
            any(s in info["best_for"] for s in seniority_levels) or not seniority_levels
        )
        if industry_match and seniority_match:
            nontrad_picks.append({"name": name, **info})

    # Calculate recommended split
    # More senior = more non-traditional (sourcing), more junior = more traditional (volume)
    avg_complexity = (
        sum(d["complexity_score"] for d in difficulty_results)
        / max(len(difficulty_results), 1)
        if difficulty_results
        else 4.0
    )
    # Scale: complexity 1-3 -> 80/20 traditional, 4-6 -> 65/35, 7-10 -> 50/50
    if avg_complexity <= 3:
        trad_pct, nontrad_pct = 80, 20
    elif avg_complexity <= 6:
        trad_pct, nontrad_pct = 65, 35
    else:
        trad_pct, nontrad_pct = 50, 50

    return {
        "traditional_channels": sorted(
            trad_picks, key=lambda x: x.get("relevance_score", 0), reverse=True
        )[:6],
        "non_traditional_channels": nontrad_picks[:8],
        "recommended_split": {
            "traditional_pct": trad_pct,
            "non_traditional_pct": nontrad_pct,
        },
        "avg_role_complexity": round(avg_complexity, 1),
        "strategy_note": (
            f"Recommended {trad_pct}/{nontrad_pct} traditional/non-traditional split "
            f"based on average role complexity of {avg_complexity:.1f}/10."
        ),
    }


# ---------------------------------------------------------------------------
# 6. Multi-Tier Budget Breakdowns
# ---------------------------------------------------------------------------


def compute_budget_tiers(data: dict) -> dict[str, Any]:
    """Split total budget into creative, media, and contingency tiers.

    Standard industry splits:
    - Media spend: 65-75% (job ads, programmatic, boards)
    - Creative/content: 15-20% (employer branding, video, copy)
    - Contingency/reserve: 10-15% (market shifts, surge hiring)

    Adjusts based on industry and hiring difficulty.

    Returns:
        Dict with tier_breakdown, per_channel_tiers, and recommendations.
    """
    budget_alloc = data.get("_budget_allocation") or {}
    meta = budget_alloc.get("metadata") or {}
    total_budget = float(meta.get("total_budget") or 0)
    synthesized = data.get("_synthesized") or {}
    enriched = data.get("_enriched") or {}

    if total_budget <= 0:
        # Try to parse from data directly
        from shared_utils import parse_budget

        budget_str = str(data.get("budget") or data.get("budget_range") or "")
        total_budget = parse_budget(budget_str)

    if total_budget <= 0:
        return {"error": "No budget available for tier breakdown"}

    # Determine difficulty to adjust splits
    difficulty_str = (
        (
            str(
                synthesized.get("hiring_difficulty")
                or enriched.get("hiring_difficulty")
                or "moderate"
            )
            or "moderate"
        )
        .lower()
        .strip()
    )

    if (
        "high" in difficulty_str
        or "hard" in difficulty_str
        or "critical" in difficulty_str
    ):
        media_pct, creative_pct, contingency_pct = 0.70, 0.18, 0.12
    elif "low" in difficulty_str or "easy" in difficulty_str:
        media_pct, creative_pct, contingency_pct = 0.75, 0.15, 0.10
    else:
        media_pct, creative_pct, contingency_pct = 0.72, 0.17, 0.11

    media_budget = round(total_budget * media_pct, 2)
    creative_budget = round(total_budget * creative_pct, 2)
    contingency_budget = round(total_budget * contingency_pct, 2)

    # Creative sub-allocation
    creative_sub = {
        "job_ad_copywriting": round(creative_budget * 0.30, 2),
        "employer_brand_content": round(creative_budget * 0.25, 2),
        "video_production": round(creative_budget * 0.20, 2),
        "landing_pages": round(creative_budget * 0.15, 2),
        "social_media_content": round(creative_budget * 0.10, 2),
    }

    # Contingency sub-allocation
    contingency_sub = {
        "market_surge_reserve": round(contingency_budget * 0.40, 2),
        "underperformance_reallocation": round(contingency_budget * 0.30, 2),
        "new_channel_testing": round(contingency_budget * 0.20, 2),
        "emergency_hiring_spikes": round(contingency_budget * 0.10, 2),
    }

    return {
        "total_budget": total_budget,
        "tier_breakdown": {
            "media_spend": {
                "amount": media_budget,
                "pct": round(media_pct * 100, 1),
                "description": "Direct job advertising, programmatic, boards, social ads",
            },
            "creative_content": {
                "amount": creative_budget,
                "pct": round(creative_pct * 100, 1),
                "description": "Employer branding, ad creative, video, landing pages",
                "sub_allocation": creative_sub,
            },
            "contingency_reserve": {
                "amount": contingency_budget,
                "pct": round(contingency_pct * 100, 1),
                "description": "Market shifts, surge hiring, channel testing",
                "sub_allocation": contingency_sub,
            },
        },
        "recommendations": [
            f"Media spend: ${media_budget:,.0f} ({media_pct*100:.0f}%) -- direct advertising budget",
            f"Creative: ${creative_budget:,.0f} ({creative_pct*100:.0f}%) -- invest in employer brand content",
            f"Contingency: ${contingency_budget:,.0f} ({contingency_pct*100:.0f}%) -- reserve for market changes",
            "Review and reallocate contingency funds monthly based on performance data",
        ],
    }


# ---------------------------------------------------------------------------
# 7. Activation Event Calendars
# ---------------------------------------------------------------------------

_HIRING_EVENTS_CALENDAR: dict[int, dict[str, Any]] = {
    1: {
        "season": "New Year Surge",
        "hiring_intensity": "high",
        "events": [
            "New Year job search peak",
            "Budget cycle kickoff",
            "College winter graduates",
        ],
        "recommendation": "Front-load budget -- January sees 25-30% more job searches",
    },
    2: {
        "season": "Early Spring",
        "hiring_intensity": "high",
        "events": [
            "Spring career fairs",
            "Industry conferences begin",
            "Tax season (finance)",
        ],
        "recommendation": "Invest in campus recruiting and career fair sponsorships",
    },
    3: {
        "season": "Spring Peak",
        "hiring_intensity": "very_high",
        "events": [
            "March Madness (brand visibility)",
            "SXSW (tech)",
            "Spring career fairs peak",
        ],
        "recommendation": "Maximum ad spend -- spring is the highest hiring season",
    },
    4: {
        "season": "Q2 Kickoff",
        "hiring_intensity": "high",
        "events": [
            "Q2 budget releases",
            "Earth Day (sustainability hiring)",
            "Internship postings peak",
        ],
        "recommendation": "Launch internship programs and summer hire campaigns",
    },
    5: {
        "season": "Pre-Summer",
        "hiring_intensity": "moderate",
        "events": [
            "May graduations",
            "Memorial Day weekend lull",
            "Summer internship starts",
        ],
        "recommendation": "Target new graduates; reduce spend heading into summer",
    },
    6: {
        "season": "Summer Slowdown Start",
        "hiring_intensity": "moderate",
        "events": ["Summer hiring for seasonal roles", "Healthcare conference season"],
        "recommendation": "Shift to passive sourcing and employer branding",
    },
    7: {
        "season": "Mid-Summer",
        "hiring_intensity": "low",
        "events": [
            "Summer vacation lull",
            "Back-to-school prep (education)",
            "AWS re:Invent prep (tech)",
        ],
        "recommendation": "Lowest cost-per-click -- good time for brand awareness campaigns",
    },
    8: {
        "season": "Late Summer",
        "hiring_intensity": "moderate",
        "events": [
            "Back to work wave",
            "Fall conference planning",
            "Q3 budget reviews",
        ],
        "recommendation": "Ramp up spend -- candidates return from vacation",
    },
    9: {
        "season": "Fall Surge",
        "hiring_intensity": "very_high",
        "events": [
            "HR Tech Conference",
            "Fall campus recruiting",
            "Dreamforce (Salesforce)",
        ],
        "recommendation": "Second biggest hiring wave -- maximize programmatic spend",
    },
    10: {
        "season": "October Peak",
        "hiring_intensity": "high",
        "events": [
            "Grace Hopper (diversity/tech)",
            "LinkedIn Talent Connect",
            "Open enrollment (healthcare)",
        ],
        "recommendation": "Invest in diversity-focused channels and employer branding",
    },
    11: {
        "season": "Pre-Holiday",
        "hiring_intensity": "moderate",
        "events": [
            "Holiday seasonal hiring (retail)",
            "Black Friday/Cyber Monday",
            "Year-end budget spend",
        ],
        "recommendation": "Retail: maximum seasonal spend. Others: use remaining budget strategically",
    },
    12: {
        "season": "Year End",
        "hiring_intensity": "low",
        "events": ["Holiday slowdown", "Year-end reviews", "New year planning"],
        "recommendation": "Minimal active recruiting -- focus on pipeline building for January",
    },
}


_INDUSTRY_MONTHLY_EVENTS: dict[str, dict[int, list[str]]] = {
    "healthcare": {
        1: ["Healthcare staffing surge", "New insurance cycles begin"],
        2: ["HIMSS prep", "Nursing recruitment drives"],
        3: ["HIMSS Global Conference", "ANA policy summit"],
        4: ["National Public Health Week", "AACN NTI prep"],
        5: ["Nursing Week (May 6-12)", "AACN NTI Conference"],
        6: ["Healthcare summer rotations", "AMA Annual Meeting"],
        7: ["Travel nursing peak", "Medical residency transition"],
        8: ["Fall clinical rotations begin", "Back-to-campus nursing"],
        9: ["Healthcare compliance deadlines", "Fall hiring ramp"],
        10: ["APHA Annual Meeting", "Open enrollment staffing"],
        11: ["AHA Annual Meeting", "Year-end clinical hiring"],
        12: ["Holiday coverage staffing", "New year credentialing"],
    },
    "tech": {
        1: ["CES", "New year hiring surge"],
        2: ["MWC Barcelona", "Spring career fairs"],
        3: ["SXSW Interactive", "GDC"],
        4: ["RSA Conference", "Q2 budget releases"],
        5: ["Google I/O", "May graduations"],
        6: ["WWDC", "Summer intern starts"],
        7: ["AWS Summit season", "Mid-year reviews"],
        8: ["Back to work wave", "Fall planning"],
        9: ["Dreamforce", "Fall campus recruiting"],
        10: ["Grace Hopper Celebration", "GitHub Universe"],
        11: ["Web Summit", "Year-end budget spend"],
        12: ["AWS re:Invent", "Year-end reviews"],
    },
    "finance": {
        1: ["Tax season begins", "Banking conference season"],
        2: ["Compliance deadline prep", "Spring career fairs"],
        3: ["End of Q1 close", "SXSW fintech track"],
        4: ["Tax filing deadline", "Q2 hiring budgets release"],
        5: ["Annual shareholder meetings", "May graduations"],
        6: ["Mid-year compliance reviews", "Summer associate starts"],
        7: ["Mid-year reviews", "Q3 planning"],
        8: ["Back to work wave", "Fall recruiting prep"],
        9: ["Fall campus recruiting", "Sibos prep"],
        10: ["Money 20/20", "Sibos"],
        11: ["Year-end audit prep", "Budget finalization"],
        12: ["Year-end close", "Bonus cycle planning"],
    },
    "retail": {
        1: ["NRF Big Show", "Post-holiday analysis"],
        2: ["Spring merchandise planning", "Career fairs"],
        3: ["Spring hiring ramp", "Easter prep staffing"],
        4: ["Spring season peak", "Summer hiring plans"],
        5: ["Memorial Day prep", "Summer staffing begins"],
        6: ["Summer season kicks off", "Back-to-school planning"],
        7: ["Back-to-school hiring", "Prime Day staffing"],
        8: ["Back-to-school peak", "Fall merchandise planning"],
        9: ["Holiday hiring begins", "Fall season transition"],
        10: ["Holiday staffing ramp", "Peak season prep"],
        11: ["Black Friday/Cyber Monday", "Maximum seasonal hiring"],
        12: ["Holiday peak staffing", "Post-holiday planning"],
    },
    "defense": {
        1: ["SHOT Show", "DoD budget cycle begins"],
        2: ["WEST Conference", "Defense career fairs"],
        3: ["Satellite conference", "Spring hiring ramp"],
        4: ["Sea-Air-Space Expo", "Q2 contract awards"],
        5: ["Military spouse hiring month", "May graduations"],
        6: ["Summer intern starts", "Mid-year reviews"],
        7: ["Farnborough/Paris Air Show", "Q3 planning"],
        8: ["DoD fiscal year-end prep", "Fall recruiting"],
        9: ["DoD FY-end spending", "DSEI (London)"],
        10: ["AUSA Annual Meeting", "New FY contracts"],
        11: ["Veteran hiring month", "Year-end clearance hiring"],
        12: ["Year-end reviews", "New year planning"],
    },
}


def _get_industry_key(industry: str) -> str:
    """Map industry string to a key in _INDUSTRY_MONTHLY_EVENTS."""
    industry = industry.lower()
    if "health" in industry or "nurs" in industry or "medical" in industry:
        return "healthcare"
    if "tech" in industry or "software" in industry or "it_" in industry:
        return "tech"
    if "financ" in industry or "bank" in industry or "insurance" in industry:
        return "finance"
    if "retail" in industry or "ecommerce" in industry or "consumer" in industry:
        return "retail"
    if "defense" in industry or "aerospace" in industry or "government" in industry:
        return "defense"
    return ""


def build_activation_calendar(data: dict) -> dict[str, Any]:
    """Build activation event calendar based on campaign start month and industry.

    Returns:
        Dict with monthly timeline, key events, and timing recommendations.
    """
    campaign_month = int(data.get("campaign_start_month") or 0)
    if campaign_month < 1 or campaign_month > 12:
        campaign_month = datetime.datetime.now().month

    industry = str(data.get("industry") or "").lower()
    ind_key = _get_industry_key(industry)
    ind_monthly = _INDUSTRY_MONTHLY_EVENTS.get(ind_key, {})

    # Build 6-month forward calendar
    timeline: list[dict[str, Any]] = []
    for offset in range(6):
        month_num = ((campaign_month - 1 + offset) % 12) + 1
        month_info = _HIRING_EVENTS_CALENDAR[month_num]
        month_name = datetime.date(2026, month_num, 1).strftime("%B")

        # Adjust budget weight based on hiring intensity
        intensity_weights = {
            "very_high": 1.3,
            "high": 1.1,
            "moderate": 1.0,
            "low": 0.7,
        }
        budget_weight = intensity_weights.get(month_info["hiring_intensity"], 1.0)

        # Use industry-specific events when available, fall back to generic
        month_events = ind_monthly.get(month_num, month_info["events"])

        timeline.append(
            {
                "month": month_num,
                "month_name": month_name,
                "offset_from_start": offset,
                "season": month_info["season"],
                "hiring_intensity": month_info["hiring_intensity"],
                "budget_weight": budget_weight,
                "key_events": month_events,
                "recommendation": month_info["recommendation"],
            }
        )

    # Industry-specific events summary (for reference)
    industry_events: list[str] = []
    if "tech" in industry:
        industry_events = [
            "CES (Jan)",
            "SXSW (Mar)",
            "Google I/O (May)",
            "AWS re:Invent (Dec)",
        ]
    elif "health" in industry:
        industry_events = [
            "HIMSS (Mar)",
            "AACN NTI (May)",
            "ANA Policy Summit (Mar)",
            "Nursing Week (May)",
        ]
    elif "finance" in industry:
        industry_events = [
            "Money 20/20 (Oct)",
            "Sibos (Oct)",
            "Tax season surge (Jan-Apr)",
        ]
    elif "retail" in industry:
        industry_events = [
            "NRF Big Show (Jan)",
            "Black Friday prep (Sep-Nov)",
            "Back-to-school (Jul-Aug)",
        ]
    elif "defense" in industry or "aerospace" in industry:
        industry_events = ["AUSA (Oct)", "Sea-Air-Space (Apr)", "SHOT Show (Jan)"]

    # NOTE: Activation calendar uses hardcoded industry events above.
    # These are curated conference/event dates that rarely change year-to-year.
    # If dynamic event data becomes available, replace the hardcoded lists.
    return {
        "campaign_start_month": campaign_month,
        "timeline": timeline,
        "industry_events": industry_events,
        "budget_phasing_note": (
            "Budget should be weighted toward high-intensity months. "
            "Front-load spend in the first 2 months for maximum visibility."
        ),
    }


# ---------------------------------------------------------------------------
# Master orchestrator
# ---------------------------------------------------------------------------


def apply_all_quality_gates(data: dict) -> dict[str, Any]:
    """Apply all 7 Gold Standard quality gates to the plan data.

    Enriches ``data`` in-place with ``_gold_standard`` key containing
    all gate outputs.  Individual gates that fail are logged but do not
    block the pipeline.

    Args:
        data: The full generation data dict (after enrichment + budget allocation).

    Returns:
        The consolidated gold_standard dict (also stored at data['_gold_standard']).
    """
    gold: dict[str, Any] = {}

    # Gate 1: City-level supply-demand
    try:
        city_data = enrich_city_level_data(data)
        if city_data:
            gold["city_level_data"] = city_data
            logger.info(
                "Gold Standard Gate 1: City-level data for %d cities", len(city_data)
            )
    except Exception as e:
        logger.error(
            "Gold Standard Gate 1 (city-level data) failed: %s", e, exc_info=True
        )

    # Gate 2: Security clearance segmentation
    try:
        clearance = detect_clearance_requirements(data)
        if clearance:
            gold["clearance_segmentation"] = clearance
            logger.info(
                "Gold Standard Gate 2: Defense detected, clearance=%s",
                clearance["primary_clearance"]["level"],
            )
    except Exception as e:
        logger.error("Gold Standard Gate 2 (clearance) failed: %s", e, exc_info=True)

    # Gate 3: Competitor mapping
    try:
        city_data_for_competitors = gold.get("city_level_data") or {}
        competitor_map = build_competitor_map(data, city_data_for_competitors)
        if competitor_map:
            gold["competitor_mapping"] = competitor_map
            logger.info(
                "Gold Standard Gate 3: Competitor map for %d locations",
                len(competitor_map),
            )
    except Exception as e:
        logger.error("Gold Standard Gate 3 (competitors) failed: %s", e, exc_info=True)

    # Gate 4: Difficulty level framework
    try:
        difficulty_results = classify_difficulty(data)
        if difficulty_results:
            gold["difficulty_framework"] = difficulty_results
            logger.info(
                "Gold Standard Gate 4: Classified %d roles by difficulty",
                len(difficulty_results),
            )
    except Exception as e:
        logger.error("Gold Standard Gate 4 (difficulty) failed: %s", e, exc_info=True)

    # Gate 5: Channel strategy with splits
    try:
        difficulty_for_channels = gold.get("difficulty_framework") or []
        channel_strategy = build_channel_strategy(data, difficulty_for_channels)
        if channel_strategy:
            gold["channel_strategy"] = channel_strategy
            logger.info(
                "Gold Standard Gate 5: Channel strategy %d/%d split",
                channel_strategy.get("recommended_split", {}).get("traditional_pct", 0),
                channel_strategy.get("recommended_split", {}).get(
                    "non_traditional_pct", 0
                ),
            )
    except Exception as e:
        logger.error(
            "Gold Standard Gate 5 (channel strategy) failed: %s", e, exc_info=True
        )

    # Gate 6: Multi-tier budget breakdowns
    try:
        budget_tiers = compute_budget_tiers(data)
        if budget_tiers and "error" not in budget_tiers:
            gold["budget_tiers"] = budget_tiers
            logger.info("Gold Standard Gate 6: Budget tiers computed")
    except Exception as e:
        logger.error("Gold Standard Gate 6 (budget tiers) failed: %s", e, exc_info=True)

    # Gate 7: Activation event calendar
    try:
        calendar = build_activation_calendar(data)
        if calendar:
            gold["activation_calendar"] = calendar
            logger.info(
                "Gold Standard Gate 7: %d-month activation calendar from month %d",
                len(calendar.get("timeline") or []),
                calendar.get("campaign_start_month", 0),
            )
    except Exception as e:
        logger.error("Gold Standard Gate 7 (calendar) failed: %s", e, exc_info=True)

    # Store on data for downstream consumers (Excel/PPT generators)
    data["_gold_standard"] = gold
    logger.info(
        "Gold Standard: %d of 7 gates produced data (%s)",
        len(gold),
        ", ".join(gold.keys()),
    )

    return gold
