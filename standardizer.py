"""Canonical taxonomy standardizer for the Media Plan Generator.

Provides unified naming conventions across all subsystems — frontend HTML,
api_enrichment, data_synthesizer, knowledge base JSON files, nova,
channels_db, and regional_hiring_intelligence.

Every public function handles None/empty input gracefully and performs
case-insensitive matching.  This module has **zero** external dependencies
(stdlib only).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

__all__ = [
    # Industry
    "CANONICAL_INDUSTRIES",
    "normalize_industry",
    # Roles
    "CANONICAL_ROLES",
    "normalize_role",
    "get_soc_code",
    "get_role_tier",
    # Location
    "COUNTRY_MAP",
    "US_STATE_MAP",
    "REGION_MAP",
    "normalize_location",
    # Platform / Channel
    "PLATFORM_ALIASES",
    "normalize_platform",
    # Metrics
    "METRIC_ALIASES",
    "normalize_metric",
]


# ═══════════════════════════════════════════════════════════════════════════════
# A.  CANONICAL INDUSTRIES  (17 entries)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Sources unified:
#   - templates/index.html  data-industry attributes
#   - api_enrichment.py     NAICS_CODES keys
#   - data_synthesizer.py   _INDUSTRY_TO_KB_KEY keys
#   - recruitment_industry_knowledge.json  industry_specific_benchmarks keys
#   - recruitment_benchmarks_deep.json     industry_benchmarks keys  (deep_bench_key)
#   - platform_intelligence_deep.json      industry_benchmarks keys
#   - channels_db.json                     industries keys           (channels_key)
#   - nova.py                          _INDUSTRY_KEYWORDS keys
#
# Each entry has:
#   aliases        -- variant names that map to this canonical key
#   naics          -- NAICS code prefix
#   kb_key         -- key into recruitment_industry_knowledge.json
#   deep_bench_key -- key into recruitment_benchmarks_deep.json
#   channels_key   -- key into channels_db.json (only present when != deep_bench_key)
#   label          -- human-readable display name
#   soc_prefix     -- SOC code prefix for BLS lookups

CANONICAL_INDUSTRIES: Dict[str, Dict[str, Any]] = {
    "healthcare": {
        "aliases": [
            "healthcare_medical", "health_care", "medical", "health",
            "hospital", "clinical", "healthcare_b2b",
        ],
        "naics": "62",
        "kb_key": "healthcare",
        "deep_bench_key": "healthcare_medical",
        "label": "Healthcare & Medical",
        "soc_prefix": "29",
    },
    "pharma_biotech": {
        "aliases": [
            "pharmaceutical", "pharma", "biotech", "life_sciences",
            "biopharmaceutical", "drug_development",
        ],
        "naics": "3254",
        "kb_key": "healthcare",
        "deep_bench_key": "pharma_biotech",
        "label": "Pharma & Biotech",
        "soc_prefix": "19",
    },
    "mental_health": {
        "aliases": [
            "behavioral_health", "counseling", "psychology", "psychiatry",
            "therapy", "social_work",
        ],
        "naics": "62",
        "kb_key": "healthcare",
        "deep_bench_key": "healthcare_medical",
        "label": "Mental Health & Counseling",
        "soc_prefix": "21",
    },
    "technology": {
        "aliases": [
            "tech_engineering", "tech", "software", "it",
            "information_technology", "saas", "fintech",
            "technology_engineering", "professional_services",
            "media_entertainment", "media", "entertainment",
        ],
        "naics": "54",
        "kb_key": "technology",
        "deep_bench_key": "technology_engineering",
        "channels_key": "tech_engineering",
        "label": "Technology & Engineering",
        "soc_prefix": "15",
    },
    "telecommunications": {
        "aliases": [
            "telecom", "telco", "wireless", "broadband",
        ],
        "naics": "517",
        "kb_key": "technology",
        "deep_bench_key": "telecommunications",
        "label": "Telecommunications",
        "soc_prefix": "15",
    },
    "finance": {
        "aliases": [
            "finance_banking", "financial_services", "banking",
            "accounting", "finserv",
        ],
        "naics": "52",
        "kb_key": "financial_services",
        "deep_bench_key": "finance_banking",
        "label": "Finance & Banking",
        "soc_prefix": "13",
    },
    "insurance": {
        "aliases": [
            "insurtech", "underwriting", "actuarial",
        ],
        "naics": "524",
        "kb_key": "financial_services",
        "deep_bench_key": "insurance",
        "label": "Insurance",
        "soc_prefix": "13",
    },
    "retail": {
        "aliases": [
            "retail_consumer", "retail_ecommerce", "ecommerce",
            "e_commerce", "shopping", "store",
        ],
        "naics": "44",
        "kb_key": "retail_hospitality",
        "deep_bench_key": "retail_consumer",
        "label": "Retail & Consumer",
        "soc_prefix": "41",
    },
    "hospitality": {
        "aliases": [
            "hospitality_travel", "hospitality_food", "food_service",
            "food_beverage", "restaurant", "hotel", "tourism", "travel",
        ],
        "naics": "72",
        "kb_key": "retail_hospitality",
        "deep_bench_key": "hospitality_travel",
        "label": "Hospitality & Food Service",
        "soc_prefix": "35",
    },
    "construction": {
        "aliases": [
            "construction_real_estate", "real_estate", "building",
            "contractor",
        ],
        "naics": "23",
        "kb_key": "construction_infrastructure",
        "deep_bench_key": "construction_real_estate",
        "label": "Construction & Real Estate",
        "soc_prefix": "47",
    },
    "transportation": {
        "aliases": [
            "transportation_logistics", "logistics", "logistics_supply_chain",
            "supply_chain", "trucking", "shipping", "blue_collar",
            "blue_collar_trades", "rideshare", "maritime", "maritime_marine",
        ],
        "naics": "48",
        "kb_key": "transportation_logistics",
        "deep_bench_key": "logistics_supply_chain",
        "label": "Transportation & Logistics",
        "soc_prefix": "53",
    },
    "manufacturing": {
        "aliases": [
            "industrial", "production", "factory", "semiconductor",
            "automotive", "automotive_manufacturing",
        ],
        "naics": "33",
        "kb_key": "manufacturing",
        "deep_bench_key": "automotive_manufacturing",
        "channels_key": "automotive",
        "label": "Manufacturing & Automotive",
        "soc_prefix": "51",
    },
    "aerospace_defense": {
        "aliases": [
            "aerospace", "defense", "military_recruitment",
            "military", "mil",
        ],
        "naics": "3364",
        "kb_key": "manufacturing",
        "deep_bench_key": "aerospace_defense",
        "label": "Aerospace & Defense",
        "soc_prefix": "17",
    },
    "energy": {
        "aliases": [
            "energy_utilities", "oil_gas", "mining", "renewable",
            "solar", "utility", "utilities",
        ],
        "naics": "21",
        "kb_key": "government_utilities",
        "deep_bench_key": "energy_utilities",
        "label": "Energy & Utilities",
        "soc_prefix": "47",
    },
    "government": {
        "aliases": [
            "government_public", "public_sector", "federal",
            "nonprofit", "education",
        ],
        "naics": "92",
        "kb_key": "government_utilities",
        "deep_bench_key": "government",
        "channels_key": "military_recruitment",
        "label": "Government & Public Sector",
        "soc_prefix": "33",
    },
    "legal": {
        "aliases": [
            "legal_services", "law", "attorney", "law_firm",
        ],
        "naics": "5411",
        "kb_key": "financial_services",
        "deep_bench_key": "legal_services",
        "label": "Legal Services",
        "soc_prefix": "23",
    },
    "general": {
        "aliases": [
            "general_entry_level", "entry_level", "other", "unknown",
            "unspecified", "mixed",
        ],
        "naics": "44",
        "kb_key": "retail_hospitality",
        "deep_bench_key": "retail_consumer",
        "label": "General / Entry-Level",
        "soc_prefix": "",
    },
}

# Pre-build reverse lookup: alias -> canonical key  (case-insensitive)
_INDUSTRY_ALIAS_MAP: Dict[str, str] = {}
for _canon_key, _meta in CANONICAL_INDUSTRIES.items():
    _INDUSTRY_ALIAS_MAP[_canon_key] = _canon_key
    for _alias in _meta["aliases"]:
        _INDUSTRY_ALIAS_MAP[_alias.lower()] = _canon_key


def normalize_industry(raw: str | None) -> str:
    """Return the canonical industry key for *raw*.

    Matching order:
      1. Exact canonical key match (case-insensitive).
      2. Exact alias match (case-insensitive).
      3. Substring / token match against aliases.
      4. Fallback to ``"general"``.

    Parameters
    ----------
    raw : str or None
        Free-form industry string from any subsystem.

    Returns
    -------
    str
        One of the 17 canonical keys (including ``"general"``).
    """
    if not raw or not isinstance(raw, str):
        return "general"

    key = raw.strip().lower().replace("-", "_").replace(" ", "_")

    # 1 + 2: direct lookup covers canonical keys AND aliases
    if key in _INDUSTRY_ALIAS_MAP:
        return _INDUSTRY_ALIAS_MAP[key]

    # 3: substring / token scan
    for alias, canon in _INDUSTRY_ALIAS_MAP.items():
        if alias in key or key in alias:
            return canon

    return "general"


def get_channels_key(canonical_industry: str) -> str:
    """Return the channels_db.json lookup key for *canonical_industry*.

    Falls back to ``deep_bench_key`` if no explicit ``channels_key`` is set,
    which is the common case (only 3 industries have a mismatch between
    deep_bench_key and the channels_db naming convention).

    Returns empty string if *canonical_industry* is not found.
    """
    meta = CANONICAL_INDUSTRIES.get(canonical_industry, {})
    return meta.get("channels_key", meta.get("deep_bench_key", ""))


# ═══════════════════════════════════════════════════════════════════════════════
# B.  CANONICAL ROLES  (~30 entries)
# ═══════════════════════════════════════════════════════════════════════════════
#
# Tiers:  executive | professional | skilled | entry

CANONICAL_ROLES: Dict[str, Dict[str, Any]] = {
    # --- Technology / Engineering ---
    "software_engineer": {
        "aliases": ["software developer", "programmer", "coder", "swe", "sde",
                     "software eng", "full stack developer", "frontend engineer",
                     "backend engineer", "mobile developer",
                     "software development engineer"],
        "soc": "15-1252",
        "tier": "professional",
        "category": "technology_it",
    },
    "web_developer": {
        "aliases": ["web dev", "front end developer", "wordpress developer"],
        "soc": "15-1254",
        "tier": "professional",
        "category": "technology_it",
    },
    "data_scientist": {
        "aliases": ["data analyst", "machine learning engineer", "ai engineer",
                     "ai/ml engineer", "ml engineer", "data science"],
        "soc": "15-2051",
        "tier": "professional",
        "category": "data_analytics",
    },
    "data_engineer": {
        "aliases": ["etl developer", "data platform engineer",
                     "analytics engineer"],
        "soc": "15-1252",
        "tier": "professional",
        "category": "data_analytics",
    },
    "devops_engineer": {
        "aliases": ["site reliability engineer", "sre", "platform engineer",
                     "systems administrator", "cloud engineer",
                     "cloud architect", "infrastructure engineer"],
        "soc": "15-1244",
        "tier": "professional",
        "category": "technology_it",
    },
    "cybersecurity_analyst": {
        "aliases": ["security engineer", "information security analyst",
                     "security analyst", "infosec", "penetration tester"],
        "soc": "15-1212",
        "tier": "professional",
        "category": "technology_it",
    },
    "network_engineer": {
        "aliases": ["network administrator", "network architect",
                     "systems engineer"],
        "soc": "15-1241",
        "tier": "professional",
        "category": "technology_it",
    },
    "qa_engineer": {
        "aliases": ["quality assurance", "test engineer", "sdet",
                     "qa analyst", "software tester"],
        "soc": "15-1253",
        "tier": "professional",
        "category": "technology_it",
    },
    # --- Design / Creative ---
    "designer": {
        "aliases": ["ux designer", "ui designer", "graphic designer",
                     "product designer", "visual designer", "ui/ux"],
        "soc": "27-1024",
        "tier": "professional",
        "category": "creative_media",
    },
    # --- Business / Management ---
    "product_manager": {
        "aliases": ["program manager", "project manager",
                     "technical program manager", "tpm", "pm"],
        "soc": "11-2021",
        "tier": "professional",
        "category": "professional_white_collar",
    },
    "operations_manager": {
        "aliases": ["branch manager", "general manager", "facility manager",
                     "plant manager"],
        "soc": "11-1021",
        "tier": "professional",
        "category": "operations_supply_chain",
    },
    "business_analyst": {
        "aliases": ["management consultant", "strategy analyst",
                     "sap consultant", "solutions architect",
                     "consulting analyst"],
        "soc": "13-1111",
        "tier": "professional",
        "category": "professional_white_collar",
    },
    # --- Executive ---
    "executive": {
        "aliases": ["chief executive officer", "ceo", "chief technology officer",
                     "cto", "chief financial officer", "cfo",
                     "vice president", "vp", "c-suite", "director"],
        "soc": "11-1011",
        "tier": "executive",
        "category": "executive_csuite",
    },
    # --- Sales / Marketing ---
    "sales_manager": {
        "aliases": ["account manager", "account executive",
                     "business development", "bdr", "sdr",
                     "sales representative", "sales associate"],
        "soc": "11-2022",
        "tier": "professional",
        "category": "sales_marketing",
    },
    "marketing_manager": {
        "aliases": ["marketing specialist", "seo specialist",
                     "content manager", "social media manager",
                     "brand manager", "digital marketing"],
        "soc": "11-2021",
        "tier": "professional",
        "category": "sales_marketing",
    },
    # --- Finance ---
    "financial_analyst": {
        "aliases": ["investment banking analyst", "risk manager",
                     "quantitative developer", "quant", "teller"],
        "soc": "13-2051",
        "tier": "professional",
        "category": "professional_white_collar",
    },
    "accountant": {
        "aliases": ["auditor", "bookkeeper", "tax preparer",
                     "cpa", "controller"],
        "soc": "13-2011",
        "tier": "professional",
        "category": "professional_white_collar",
    },
    # --- HR ---
    "recruiter": {
        "aliases": ["talent acquisition", "hr specialist",
                     "human resources specialist", "sourcer",
                     "recruiting coordinator"],
        "soc": "13-1071",
        "tier": "professional",
        "category": "human_resources",
    },
    "hr_manager": {
        "aliases": ["human resources manager", "people operations",
                     "head of hr", "hr director", "hr business partner"],
        "soc": "11-3121",
        "tier": "professional",
        "category": "human_resources",
    },
    # --- Healthcare ---
    "registered_nurse": {
        "aliases": ["nurse", "rn", "lpn", "nursing", "travel nurse",
                     "charge nurse", "staff nurse"],
        "soc": "29-1141",
        "tier": "professional",
        "category": "healthcare_clinical",
    },
    "physician": {
        "aliases": ["doctor", "md", "physician assistant", "pa",
                     "surgeon", "specialist"],
        "soc": "29-1218",
        "tier": "professional",
        "category": "healthcare_clinical",
    },
    "medical_support": {
        "aliases": ["cna", "certified nursing assistant", "medical assistant",
                     "dental assistant", "phlebotomist", "caregiver",
                     "home health aide", "emt", "paramedic",
                     "medical technologist", "medical coder"],
        "soc": "31-9092",
        "tier": "skilled",
        "category": "healthcare_clinical",
    },
    # --- Legal ---
    "lawyer": {
        "aliases": ["attorney", "counsel", "paralegal",
                     "compliance officer", "legal assistant"],
        "soc": "23-1011",
        "tier": "professional",
        "category": "legal_compliance",
    },
    # --- Education ---
    "teacher": {
        "aliases": ["professor", "instructor", "educator", "tutor",
                     "principal", "academic"],
        "soc": "25-2031",
        "tier": "professional",
        "category": "education_academia",
    },
    # --- Writing / Content ---
    "writer": {
        "aliases": ["technical writer", "content writer", "copywriter",
                     "editor", "journalist", "communications specialist"],
        "soc": "27-3042",
        "tier": "professional",
        "category": "creative_media",
    },
    # --- Engineering (non-software) ---
    "mechanical_engineer": {
        "aliases": ["electrical engineer", "civil engineer",
                     "chemical engineer", "structural engineer",
                     "process engineer"],
        "soc": "17-2141",
        "tier": "professional",
        "category": "engineering_manufacturing",
    },
    # --- Blue collar / Skilled trades ---
    "truck_driver": {
        "aliases": ["cdl driver", "cdl-a driver", "long haul driver",
                     "delivery driver", "bus driver", "courier",
                     "dispatcher"],
        "soc": "53-3032",
        "tier": "skilled",
        "category": "transportation_logistics",
    },
    "warehouse_worker": {
        "aliases": ["warehouse associate", "warehouse manager",
                     "forklift operator", "picker packer",
                     "material handler", "package handler",
                     "dock worker", "mover", "stocker",
                     "shipping clerk", "receiving clerk"],
        "soc": "53-7065",
        "tier": "entry",
        "category": "transportation_logistics",
    },
    "construction_worker": {
        "aliases": ["construction laborer", "general laborer", "laborer",
                     "carpenter", "electrician", "plumber",
                     "hvac technician", "welder", "machinist",
                     "painter", "roofer", "ironworker",
                     "crane operator", "heavy equipment operator",
                     "concrete worker"],
        "soc": "47-2061",
        "tier": "skilled",
        "category": "skilled_trades",
    },
    "maintenance_technician": {
        "aliases": ["maintenance worker", "janitor", "custodian",
                     "housekeeper", "landscaper", "pest control technician",
                     "auto mechanic", "automotive technician",
                     "diesel mechanic"],
        "soc": "49-9071",
        "tier": "skilled",
        "category": "skilled_trades",
    },
    "retail_associate": {
        "aliases": ["store associate", "cashier", "retail manager",
                     "merchandiser", "store manager"],
        "soc": "41-2031",
        "tier": "entry",
        "category": "sales_marketing",
    },
    "food_service_worker": {
        "aliases": ["cook", "chef", "server", "bartender", "dishwasher",
                     "food service", "barista", "line cook"],
        "soc": "35-3023",
        "tier": "entry",
        "category": "hospitality_food_service",
    },
    "security_guard": {
        "aliases": ["security officer", "loss prevention",
                     "security specialist"],
        "soc": "33-9032",
        "tier": "entry",
        "category": "government_public_sector",
    },
    "production_worker": {
        "aliases": ["assembly worker", "machine operator",
                     "quality inspector", "production associate",
                     "line worker", "factory worker"],
        "soc": "51-9199",
        "tier": "entry",
        "category": "engineering_manufacturing",
    },
}

# Pre-build reverse lookup:  lowered alias -> canonical role key
_ROLE_ALIAS_MAP: Dict[str, str] = {}
for _rkey, _rmeta in CANONICAL_ROLES.items():
    _ROLE_ALIAS_MAP[_rkey.replace("_", " ")] = _rkey
    _ROLE_ALIAS_MAP[_rkey] = _rkey
    for _ralias in _rmeta["aliases"]:
        _ROLE_ALIAS_MAP[_ralias.lower()] = _rkey


def normalize_role(raw: str | None) -> str:
    """Return the canonical role key for *raw*.

    Matching order:
      1. Exact match against canonical keys (underscored and spaced forms).
      2. Exact alias match.
      3. Substring match — longest alias that appears inside *raw*, or
         *raw* that appears inside an alias.
      4. Returns the original string (lowered, underscored) if nothing
         matched.

    Parameters
    ----------
    raw : str or None
        Free-form role / job title string.

    Returns
    -------
    str
        Canonical role key or cleaned original.
    """
    if not raw or not isinstance(raw, str):
        return ""

    cleaned = raw.strip().lower()

    # 1 + 2: direct lookup
    if cleaned in _ROLE_ALIAS_MAP:
        return _ROLE_ALIAS_MAP[cleaned]

    underscored = cleaned.replace(" ", "_").replace("-", "_")
    if underscored in _ROLE_ALIAS_MAP:
        return _ROLE_ALIAS_MAP[underscored]

    # 3: substring scan — prefer longest matching alias
    best_match: str = ""
    best_len: int = 0
    for alias, canon in _ROLE_ALIAS_MAP.items():
        if len(alias) < 3:
            continue  # skip very short aliases to avoid false positives
        if alias in cleaned and len(alias) > best_len:
            best_match = canon
            best_len = len(alias)
        elif cleaned in alias and len(cleaned) > best_len:
            best_match = canon
            best_len = len(cleaned)

    if best_match:
        return best_match

    # 4: no match — return cleaned form
    return re.sub(r"\s+", "_", underscored)


def get_soc_code(role: str | None) -> str:
    """Return the SOC code for *role* after normalisation.

    Returns an empty string when no mapping exists.
    """
    if not role:
        return ""
    canon = normalize_role(role)
    meta = CANONICAL_ROLES.get(canon)
    return meta["soc"] if meta else ""


def get_role_tier(role: str | None) -> str:
    """Return the tier (executive / professional / skilled / entry) for *role*.

    Returns an empty string when no mapping exists.
    """
    if not role:
        return ""
    canon = normalize_role(role)
    meta = CANONICAL_ROLES.get(canon)
    return meta["tier"] if meta else ""


# ═══════════════════════════════════════════════════════════════════════════════
# C.  LOCATION RESOLVER
# ═══════════════════════════════════════════════════════════════════════════════

# -- Country map:  lowercase variant -> canonical dict ---
_COUNTRY_CANONICAL: Dict[str, Dict[str, str]] = {
    "USA": {"name": "United States", "iso2": "us", "iso3": "USA"},
    "GBR": {"name": "United Kingdom", "iso2": "gb", "iso3": "GBR"},
    "CAN": {"name": "Canada", "iso2": "ca", "iso3": "CAN"},
    "AUS": {"name": "Australia", "iso2": "au", "iso3": "AUS"},
    "DEU": {"name": "Germany", "iso2": "de", "iso3": "DEU"},
    "FRA": {"name": "France", "iso2": "fr", "iso3": "FRA"},
    "IND": {"name": "India", "iso2": "in", "iso3": "IND"},
    "JPN": {"name": "Japan", "iso2": "jp", "iso3": "JPN"},
    "CHN": {"name": "China", "iso2": "cn", "iso3": "CHN"},
    "BRA": {"name": "Brazil", "iso2": "br", "iso3": "BRA"},
    "MEX": {"name": "Mexico", "iso2": "mx", "iso3": "MEX"},
    "KOR": {"name": "South Korea", "iso2": "kr", "iso3": "KOR"},
    "ITA": {"name": "Italy", "iso2": "it", "iso3": "ITA"},
    "ESP": {"name": "Spain", "iso2": "es", "iso3": "ESP"},
    "NLD": {"name": "Netherlands", "iso2": "nl", "iso3": "NLD"},
    "SWE": {"name": "Sweden", "iso2": "se", "iso3": "SWE"},
    "CHE": {"name": "Switzerland", "iso2": "ch", "iso3": "CHE"},
    "SGP": {"name": "Singapore", "iso2": "sg", "iso3": "SGP"},
    "IRL": {"name": "Ireland", "iso2": "ie", "iso3": "IRL"},
    "ISR": {"name": "Israel", "iso2": "il", "iso3": "ISR"},
    "NZL": {"name": "New Zealand", "iso2": "nz", "iso3": "NZL"},
    "ZAF": {"name": "South Africa", "iso2": "za", "iso3": "ZAF"},
    "ARE": {"name": "United Arab Emirates", "iso2": "ae", "iso3": "ARE"},
    "POL": {"name": "Poland", "iso2": "pl", "iso3": "POL"},
    "NOR": {"name": "Norway", "iso2": "no", "iso3": "NOR"},
    "DNK": {"name": "Denmark", "iso2": "dk", "iso3": "DNK"},
    "FIN": {"name": "Finland", "iso2": "fi", "iso3": "FIN"},
    "BEL": {"name": "Belgium", "iso2": "be", "iso3": "BEL"},
    "AUT": {"name": "Austria", "iso2": "at", "iso3": "AUT"},
    "PRT": {"name": "Portugal", "iso2": "pt", "iso3": "PRT"},
    "ARG": {"name": "Argentina", "iso2": "ar", "iso3": "ARG"},
    "COL": {"name": "Colombia", "iso2": "co", "iso3": "COL"},
    "CHL": {"name": "Chile", "iso2": "cl", "iso3": "CHL"},
    "PHL": {"name": "Philippines", "iso2": "ph", "iso3": "PHL"},
    "MYS": {"name": "Malaysia", "iso2": "my", "iso3": "MYS"},
    "THA": {"name": "Thailand", "iso2": "th", "iso3": "THA"},
    "IDN": {"name": "Indonesia", "iso2": "id", "iso3": "IDN"},
    "VNM": {"name": "Vietnam", "iso2": "vn", "iso3": "VNM"},
    "NGA": {"name": "Nigeria", "iso2": "ng", "iso3": "NGA"},
    "EGY": {"name": "Egypt", "iso2": "eg", "iso3": "EGY"},
    "SAU": {"name": "Saudi Arabia", "iso2": "sa", "iso3": "SAU"},
    "PAK": {"name": "Pakistan", "iso2": "pk", "iso3": "PAK"},
    "BGD": {"name": "Bangladesh", "iso2": "bd", "iso3": "BGD"},
    "TWN": {"name": "Taiwan", "iso2": "tw", "iso3": "TWN"},
    "CZE": {"name": "Czech Republic", "iso2": "cz", "iso3": "CZE"},
    "ROU": {"name": "Romania", "iso2": "ro", "iso3": "ROU"},
    "HUN": {"name": "Hungary", "iso2": "hu", "iso3": "HUN"},
    "GRC": {"name": "Greece", "iso2": "gr", "iso3": "GRC"},
    "TUR": {"name": "Turkey", "iso2": "tr", "iso3": "TUR"},
    "KEN": {"name": "Kenya", "iso2": "ke", "iso3": "KEN"},
}

# Build COUNTRY_MAP: every known variant -> canonical entry
COUNTRY_MAP: Dict[str, Dict[str, str]] = {}

# Extra aliases not derivable from canonical table
_EXTRA_COUNTRY_ALIASES: Dict[str, str] = {
    "america": "USA",
    "britain": "GBR",
    "england": "GBR",
    "uk": "GBR",
    "gb": "GBR",
    "deutschland": "DEU",
    "holland": "NLD",
    "korea": "KOR",
    "uae": "ARE",
}

for _iso3, _cmeta in _COUNTRY_CANONICAL.items():
    _entry = dict(_cmeta)
    COUNTRY_MAP[_iso3.lower()] = _entry
    COUNTRY_MAP[_cmeta["iso2"].lower()] = _entry
    COUNTRY_MAP[_cmeta["name"].lower()] = _entry

for _alias, _iso3 in _EXTRA_COUNTRY_ALIASES.items():
    if _iso3 in _COUNTRY_CANONICAL:
        COUNTRY_MAP[_alias.lower()] = dict(_COUNTRY_CANONICAL[_iso3])


# -- US State map --
_US_STATES_RAW: List[tuple] = [
    ("AL", "Alabama"), ("AK", "Alaska"), ("AZ", "Arizona"),
    ("AR", "Arkansas"), ("CA", "California"), ("CO", "Colorado"),
    ("CT", "Connecticut"), ("DE", "Delaware"), ("FL", "Florida"),
    ("GA", "Georgia"), ("HI", "Hawaii"), ("ID", "Idaho"),
    ("IL", "Illinois"), ("IN", "Indiana"), ("IA", "Iowa"),
    ("KS", "Kansas"), ("KY", "Kentucky"), ("LA", "Louisiana"),
    ("ME", "Maine"), ("MD", "Maryland"), ("MA", "Massachusetts"),
    ("MI", "Michigan"), ("MN", "Minnesota"), ("MS", "Mississippi"),
    ("MO", "Missouri"), ("MT", "Montana"), ("NE", "Nebraska"),
    ("NV", "Nevada"), ("NH", "New Hampshire"), ("NJ", "New Jersey"),
    ("NM", "New Mexico"), ("NY", "New York"), ("NC", "North Carolina"),
    ("ND", "North Dakota"), ("OH", "Ohio"), ("OK", "Oklahoma"),
    ("OR", "Oregon"), ("PA", "Pennsylvania"), ("RI", "Rhode Island"),
    ("SC", "South Carolina"), ("SD", "South Dakota"), ("TN", "Tennessee"),
    ("TX", "Texas"), ("UT", "Utah"), ("VT", "Vermont"),
    ("VA", "Virginia"), ("WA", "Washington"), ("WV", "West Virginia"),
    ("WI", "Wisconsin"), ("WY", "Wyoming"), ("DC", "District of Columbia"),
]

US_STATE_MAP: Dict[str, Dict[str, str]] = {}
for _abbr, _full in _US_STATES_RAW:
    _entry = {"abbr": _abbr, "full": _full}
    US_STATE_MAP[_abbr.lower()] = _entry
    US_STATE_MAP[_full.lower()] = _entry


# -- Region map (matches regional_hiring_intelligence.json region keys) --
REGION_MAP: Dict[str, str] = {
    # us_northeast
    "CT": "us_northeast", "ME": "us_northeast", "MA": "us_northeast",
    "NH": "us_northeast", "RI": "us_northeast", "VT": "us_northeast",
    "NJ": "us_northeast", "NY": "us_northeast", "PA": "us_northeast",
    "DE": "us_northeast", "MD": "us_northeast", "DC": "us_northeast",
    # us_southeast
    "AL": "us_southeast", "FL": "us_southeast", "GA": "us_southeast",
    "KY": "us_southeast", "MS": "us_southeast", "NC": "us_southeast",
    "SC": "us_southeast", "TN": "us_southeast", "VA": "us_southeast",
    "WV": "us_southeast", "LA": "us_southeast", "AR": "us_southeast",
    # us_midwest
    "IL": "us_midwest", "IN": "us_midwest", "IA": "us_midwest",
    "KS": "us_midwest", "MI": "us_midwest", "MN": "us_midwest",
    "MO": "us_midwest", "NE": "us_midwest", "ND": "us_midwest",
    "OH": "us_midwest", "SD": "us_midwest", "WI": "us_midwest",
    # us_southwest
    "AZ": "us_southwest", "CO": "us_southwest", "NM": "us_southwest",
    "OK": "us_southwest", "TX": "us_southwest", "UT": "us_southwest",
    "NV": "us_southwest",
    # us_west_coast
    "AK": "us_west_coast", "CA": "us_west_coast", "HI": "us_west_coast",
    "OR": "us_west_coast", "WA": "us_west_coast",
    "ID": "us_west_coast", "MT": "us_west_coast", "WY": "us_west_coast",
}

# City -> (state_abbr, market_key) for regional_hiring_intelligence.json
_CITY_TO_MARKET: Dict[str, tuple] = {
    "boston": ("MA", "boston_ma"),
    "hartford": ("CT", "hartford_ct"),
    "providence": ("RI", "providence_ri"),
    "portland": ("ME", "portland_me"),  # also OR — disambiguated by state
    "new york": ("NY", "nyc_ny"),
    "nyc": ("NY", "nyc_ny"),
    "manhattan": ("NY", "nyc_ny"),
    "brooklyn": ("NY", "nyc_ny"),
    "philadelphia": ("PA", "philadelphia_pa"),
    "newark": ("NJ", "newark_nj"),
    "pittsburgh": ("PA", "pittsburgh_pa"),
    "baltimore": ("MD", "baltimore_md"),
    "washington": ("DC", "washington_dc"),
    "charlotte": ("NC", "charlotte_nc"),
    "raleigh": ("NC", "raleigh_nc"),
    "atlanta": ("GA", "atlanta_ga"),
    "miami": ("FL", "miami_fl"),
    "tampa": ("FL", "tampa_fl"),
    "orlando": ("FL", "orlando_fl"),
    "jacksonville": ("FL", "jacksonville_fl"),
    "nashville": ("TN", "nashville_tn"),
    "memphis": ("TN", "memphis_tn"),
    "birmingham": ("AL", "birmingham_al"),
    "new orleans": ("LA", "new_orleans_la"),
    "chicago": ("IL", "chicago_il"),
    "detroit": ("MI", "detroit_mi"),
    "cleveland": ("OH", "cleveland_oh"),
    "columbus": ("OH", "columbus_oh"),
    "indianapolis": ("IN", "indianapolis_in"),
    "milwaukee": ("WI", "milwaukee_wi"),
    "minneapolis": ("MN", "minneapolis_mn"),
    "kansas city": ("MO", "kansas_city_mo"),
    "st louis": ("MO", "st_louis_mo"),
    "saint louis": ("MO", "st_louis_mo"),
    "des moines": ("IA", "des_moines_ia"),
    "omaha": ("NE", "omaha_ne"),
    "houston": ("TX", "houston_tx"),
    "dallas": ("TX", "dallas_tx"),
    "austin": ("TX", "austin_tx"),
    "san antonio": ("TX", "san_antonio_tx"),
    "phoenix": ("AZ", "phoenix_az"),
    "denver": ("CO", "denver_co"),
    "salt lake city": ("UT", "salt_lake_city_ut"),
    "las vegas": ("NV", "las_vegas_nv"),
    "albuquerque": ("NM", "albuquerque_nm"),
    "el paso": ("TX", "el_paso_tx"),
    "los angeles": ("CA", "los_angeles_ca"),
    "san francisco": ("CA", "san_francisco_ca"),
    "san diego": ("CA", "san_diego_ca"),
    "seattle": ("WA", "seattle_wa"),
    # Canadian markets
    "toronto": ("ON", "toronto_on"),
    "ottawa": ("ON", "ottawa_on"),
    "montreal": ("QC", "montreal_qc"),
    "vancouver": ("BC", "vancouver_bc"),
    "calgary": ("AB", "calgary_ab"),
    "edmonton": ("AB", "edmonton_ab"),
    # UK markets
    "london": ("", "london"),
}

# Portland OR override
_PORTLAND_OR_MARKET = ("OR", "portland_or")

# Canadian province abbreviations (for Toronto, ON etc.)
_CA_PROVINCE_MAP: Dict[str, str] = {
    "on": "Ontario", "qc": "Quebec", "bc": "British Columbia",
    "ab": "Alberta", "mb": "Manitoba", "sk": "Saskatchewan",
    "ns": "Nova Scotia", "nb": "New Brunswick", "pe": "Prince Edward Island",
    "nl": "Newfoundland and Labrador", "nt": "Northwest Territories",
    "yt": "Yukon", "nu": "Nunavut",
}

# Two-letter codes that are BOTH US state abbreviations AND country ISO-2
# codes.  When these appear after a recognised city, prefer state.
_AMBIGUOUS_2LETTER: set = {
    "ar", "ca", "co", "de", "id", "il", "in",
}


def normalize_location(raw: str | None) -> Dict[str, str]:
    """Parse a free-form location string into a structured dict.

    Handles formats such as:
      - ``"Boston, MA"``
      - ``"New York, NY, United States"``
      - ``"London, UK"``
      - ``"California"``
      - ``"United States"``
      - ``"Toronto, ON"``

    Correctly disambiguates two-letter codes that are both US state
    abbreviations and ISO-2 country codes (CA, CO, IL, etc.) by
    using city context.

    Parameters
    ----------
    raw : str or None
        Location string from any subsystem.

    Returns
    -------
    dict
        Keys: original, city, state, state_full, country, country_iso3,
        country_iso2, region_key, market_key.  Missing values are empty
        strings.
    """
    result: Dict[str, str] = {
        "original": raw or "",
        "city": "",
        "state": "",
        "state_full": "",
        "country": "",
        "country_iso3": "",
        "country_iso2": "",
        "region_key": "",
        "market_key": "",
    }

    if not raw or not isinstance(raw, str):
        return result

    cleaned = raw.strip()
    parts = [p.strip() for p in cleaned.split(",")]
    lower_parts = [p.lower() for p in parts]
    num_parts = len(parts)

    # ------------------------------------------------------------------
    # Phase 1: Detect city from the first comma-separated part
    # ------------------------------------------------------------------
    city_str = ""
    city_lower = ""
    market_info: Optional[tuple] = None  # (state_abbr, market_key)

    if num_parts >= 2:
        candidate = parts[0].strip()
        cand_lower = candidate.lower()
        # Check market database first
        if cand_lower in _CITY_TO_MARKET:
            market_info = _CITY_TO_MARKET[cand_lower]
            city_str = candidate
            city_lower = cand_lower
        elif cand_lower not in US_STATE_MAP:
            # Not a state name, so treat as city even if not in market DB
            city_str = candidate
            city_lower = cand_lower
    elif num_parts == 1:
        # Single token — could be a city, state, or country
        pass

    # ------------------------------------------------------------------
    # Phase 2: Detect state / province
    # ------------------------------------------------------------------
    state_entry: Optional[Dict[str, str]] = None
    is_us_context = False  # True when we have strong evidence this is US

    # If market DB gave us a state, use it
    if market_info:
        mk_state, _ = market_info
        if mk_state:
            st = US_STATE_MAP.get(mk_state.lower())
            if st:
                state_entry = st
                is_us_context = True

    # Scan remaining parts for state (skip first part if it was a city)
    if not state_entry:
        scan_parts = lower_parts[1:] if city_str else lower_parts
        for lp in scan_parts:
            if lp in US_STATE_MAP:
                state_entry = US_STATE_MAP[lp]
                is_us_context = True
                break
            # Canadian province
            if lp in _CA_PROVINCE_MAP:
                break  # handled in country phase

    # Single-token: might be a state name (full name only, not abbr)
    if not state_entry and num_parts == 1:
        full_lower = cleaned.lower()
        # Only match full state names, not 2-letter codes (those are ambiguous)
        if len(full_lower) > 2 and full_lower in US_STATE_MAP:
            state_entry = US_STATE_MAP[full_lower]
            is_us_context = True

    # ------------------------------------------------------------------
    # Phase 3: Detect country
    # ------------------------------------------------------------------
    country_entry: Optional[Dict[str, str]] = None

    # Explicit country part — scan from the end (rightmost = most likely)
    for lp in reversed(lower_parts):
        # Skip 2-letter codes that are ambiguous with US states
        # when we already have US context
        if len(lp) == 2 and lp in _AMBIGUOUS_2LETTER and is_us_context:
            continue
        # Skip 2-letter codes that are ambiguous when city is known
        if len(lp) == 2 and lp in _AMBIGUOUS_2LETTER and city_str:
            # If this code is also a state, prefer state interpretation
            if lp in US_STATE_MAP:
                if not state_entry:
                    state_entry = US_STATE_MAP[lp]
                    is_us_context = True
                continue
        # Only accept as country if it is in COUNTRY_MAP and NOT ambiguous
        if len(lp) > 2 and lp in COUNTRY_MAP:
            country_entry = COUNTRY_MAP[lp]
            break
        if len(lp) <= 2 and lp in COUNTRY_MAP and lp not in _AMBIGUOUS_2LETTER:
            country_entry = COUNTRY_MAP[lp]
            break
        # Handle "uk", "gb" etc. from extra aliases
        if lp in COUNTRY_MAP and lp not in _AMBIGUOUS_2LETTER:
            country_entry = COUNTRY_MAP[lp]
            break

    # Canadian province detection -> sets country to Canada
    if not country_entry:
        for lp in lower_parts:
            if lp in _CA_PROVINCE_MAP:
                country_entry = COUNTRY_MAP.get("canada")
                break

    # Infer US when state found but country not
    if state_entry and not country_entry:
        country_entry = COUNTRY_MAP.get("united states")

    # Single-token fallback: try as country
    if not country_entry and not state_entry and num_parts == 1:
        full_lower = cleaned.lower()
        if full_lower in COUNTRY_MAP:
            country_entry = COUNTRY_MAP[full_lower]

    # ------------------------------------------------------------------
    # Phase 4: Market key and Portland disambiguation
    # ------------------------------------------------------------------
    market_key = ""
    if market_info:
        mk_state_code, market_key = market_info
        # Portland disambiguation
        if city_lower == "portland" and state_entry:
            if state_entry["abbr"] == "OR":
                _, market_key = _PORTLAND_OR_MARKET

    # ------------------------------------------------------------------
    # Phase 5: Region key
    # ------------------------------------------------------------------
    region_key = ""
    if state_entry:
        region_key = REGION_MAP.get(state_entry["abbr"], "")

    # ------------------------------------------------------------------
    # Populate result
    # ------------------------------------------------------------------
    if city_str:
        result["city"] = city_str
    if state_entry:
        result["state"] = state_entry["abbr"]
        result["state_full"] = state_entry["full"]
    if country_entry:
        result["country"] = country_entry["name"]
        result["country_iso3"] = country_entry["iso3"]
        result["country_iso2"] = country_entry["iso2"]
    result["region_key"] = region_key
    result["market_key"] = market_key

    return result


# ═══════════════════════════════════════════════════════════════════════════════
# D.  PLATFORM / CHANNEL NORMALIZER
# ═══════════════════════════════════════════════════════════════════════════════

PLATFORM_ALIASES: Dict[str, List[str]] = {
    "indeed": ["indeed.com", "Indeed", "indeed_sponsored"],
    "linkedin": ["LinkedIn", "linkedin.com", "linkedin_jobs",
                 "linkedin_recruiter"],
    "ziprecruiter": ["ZipRecruiter", "zip recruiter", "zip_recruiter",
                     "ziprecruiter.com"],
    "google": ["Google Ads", "google_search_ads", "google_display_ads",
               "google ads", "adwords", "google_ads", "google for jobs",
               "google_for_jobs"],
    "meta": ["Facebook", "facebook", "Meta", "facebook_ads",
             "meta_facebook_ads", "instagram", "Instagram",
             "meta_ads", "fb"],
    "microsoft_bing": ["Bing", "bing", "Microsoft Bing",
                       "microsoft_bing_ads", "bing_ads", "microsoft_ads",
                       "microsoft ads"],
    "tiktok": ["TikTok", "tik tok", "tiktok_ads", "tik_tok"],
    "glassdoor": ["Glassdoor", "glassdoor.com"],
    "monster": ["Monster", "monster.com"],
    "careerbuilder": ["CareerBuilder", "career builder", "career_builder",
                      "careerbuilder.com"],
    "programmatic": ["programmatic_display", "programmatic display",
                     "programmatic_ads", "dsp"],
    "craigslist": ["Craigslist", "craigslist.org"],
    "handshake": ["Handshake", "handshake.com"],
    "dice": ["Dice", "dice.com"],
    "stackoverflow": ["Stack Overflow", "stack overflow",
                      "stackoverflow.com"],
    "snagajob": ["Snagajob", "snag a job", "snagajob.com"],
    "jooble": ["Jooble", "jooble.org"],
    "adzuna": ["Adzuna", "adzuna.com"],
    "simplyhired": ["SimplyHired", "simply hired", "simplyhired.com"],
    "appcast": ["Appcast", "appcast.io"],
    "joveo": ["Joveo", "joveo.com"],
    "pandologic": ["PandoLogic", "pandologic.com"],
    "recruitics": ["Recruitics", "recruitics.com"],
    "talroo": ["Talroo", "talroo.com"],
    "neuvoo": ["Neuvoo", "talent.com", "Talent.com"],
    "twitter": ["Twitter", "X", "x.com", "twitter_ads"],
    "snap": ["Snapchat", "snapchat", "snap_ads"],
    "reddit": ["Reddit", "reddit.com", "reddit_ads"],
    "spotify": ["Spotify", "spotify_ads"],
}

# Pre-build reverse lookup
_PLATFORM_ALIAS_MAP: Dict[str, str] = {}
for _pkey, _paliases in PLATFORM_ALIASES.items():
    _PLATFORM_ALIAS_MAP[_pkey.lower()] = _pkey
    for _pa in _paliases:
        _PLATFORM_ALIAS_MAP[_pa.lower()] = _pkey


def normalize_platform(raw: str | None) -> str:
    """Return the canonical platform key for *raw*.

    Parameters
    ----------
    raw : str or None
        Platform / channel name from any subsystem.

    Returns
    -------
    str
        Canonical platform key, or the original lowered string if no
        match found.
    """
    if not raw or not isinstance(raw, str):
        return ""

    key = raw.strip().lower()

    if key in _PLATFORM_ALIAS_MAP:
        return _PLATFORM_ALIAS_MAP[key]

    # Strip trailing ".com" / ".org" etc. and retry
    stripped = re.sub(r"\.(com|org|io|net)$", "", key)
    if stripped in _PLATFORM_ALIAS_MAP:
        return _PLATFORM_ALIAS_MAP[stripped]

    # Substring check (e.g., "indeed sponsored" contains "indeed")
    for alias, canon in _PLATFORM_ALIAS_MAP.items():
        if len(alias) >= 4 and alias in key:
            return canon

    return key


# ═══════════════════════════════════════════════════════════════════════════════
# E.  METRIC NORMALIZER
# ═══════════════════════════════════════════════════════════════════════════════

METRIC_ALIASES: Dict[str, List[str]] = {
    "cpc": ["cost_per_click", "avg_cpc", "CPC", "cost per click",
             "cost-per-click", "average_cpc"],
    "cpa": ["cost_per_application", "avg_cpa", "CPA",
             "cost per application", "cost-per-application",
             "cost_per_apply", "cost per apply", "cost_per_app"],
    "cph": ["cost_per_hire", "CPH", "cost per hire", "cost-per-hire",
             "hiring_cost", "hiring cost"],
    "cpm": ["cost_per_mille", "CPM", "cost per thousand",
             "cost-per-thousand", "cost per mille"],
    "ctr": ["click_through_rate", "CTR", "click through rate",
             "click-through-rate", "clickthrough_rate"],
    "apply_rate": ["application_rate", "conversion_rate", "cvr",
                    "apply rate", "application rate", "conversion rate",
                    "apply_conversion"],
    "time_to_fill": ["ttf", "days_to_fill", "time to fill",
                      "time-to-fill", "days to fill", "time to hire",
                      "time-to-hire", "tth"],
    "time_to_hire": ["hiring_time", "hire_time", "recruitment_cycle"],
    "quality_of_hire": ["qoh", "hire_quality", "quality of hire"],
    "roi": ["return_on_investment", "return on investment",
             "roas", "return on ad spend"],
    "budget": ["spend", "total_spend", "ad_spend", "investment",
               "total_budget", "media_spend"],
    "salary": ["compensation", "pay", "wage", "earnings", "income",
               "total_compensation", "base_salary"],
}

_METRIC_ALIAS_MAP: Dict[str, str] = {}
for _mkey, _maliases in METRIC_ALIASES.items():
    _METRIC_ALIAS_MAP[_mkey.lower()] = _mkey
    for _ma in _maliases:
        _METRIC_ALIAS_MAP[_ma.lower()] = _mkey


def normalize_metric(raw: str | None) -> str:
    """Return the canonical metric key for *raw*.

    Parameters
    ----------
    raw : str or None
        Metric name from any subsystem.

    Returns
    -------
    str
        Canonical metric key, or the original lowered string if no
        match found.
    """
    if not raw or not isinstance(raw, str):
        return ""

    key = raw.strip().lower()

    if key in _METRIC_ALIAS_MAP:
        return _METRIC_ALIAS_MAP[key]

    # Normalise separators and retry
    normalised = key.replace("-", "_").replace(" ", "_")
    if normalised in _METRIC_ALIAS_MAP:
        return _METRIC_ALIAS_MAP[normalised]

    return key
