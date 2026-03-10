#!/usr/bin/env python3
"""AI Media Planner - Standalone HTTP server with real research data."""

import json
import os
import io
import datetime
import sys
import re
import zipfile
import uuid
import time
try:
    import fcntl
except ImportError:
    fcntl = None  # Windows compatibility: file locking handled below
import logging
import threading
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse
from pathlib import Path
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, PieChart, DoughnutChart, Reference
from openpyxl.chart.label import DataLabelList
from openpyxl.chart.series import DataPoint
from openpyxl.drawing.image import Image as XlImage

from shared_utils import (
    parse_budget,
    INDUSTRY_LABEL_MAP as _SHARED_INDUSTRY_LABEL_MAP,
    standardize_location as _shared_standardize_location,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# KNOWLEDGE BASE LOADER
# ═══════════════════════════════════════════════════════════════════════════════

_knowledge_base = None

# Mapping from legacy industry keys (used in INDUSTRY_NAICS_MAP) to KB keys
# in data/recruitment_industry_knowledge.json -> industry_specific_benchmarks
_INDUSTRY_KEY_TO_KB_KEY = {
    "healthcare": "healthcare",
    "healthcare_medical": "healthcare",
    "mental_health": "healthcare",
    "technology": "technology",
    "tech_engineering": "technology",
    "telecom": "technology",
    "telecommunications": "technology",
    "retail": "retail_hospitality",
    "retail_consumer": "retail_hospitality",
    "hospitality": "retail_hospitality",
    "hospitality_travel": "retail_hospitality",
    "food_beverage": "retail_hospitality",
    "construction": "construction_infrastructure",
    "construction_real_estate": "construction_infrastructure",
    "transportation": "transportation_logistics",
    "logistics_supply_chain": "transportation_logistics",
    "maritime": "transportation_logistics",
    "maritime_marine": "transportation_logistics",
    "rideshare": "transportation_logistics",
    "manufacturing": "manufacturing",
    "automotive": "manufacturing",
    "blue_collar": "transportation_logistics",
    "blue_collar_trades": "transportation_logistics",
    "aerospace": "manufacturing",
    "aerospace_defense": "manufacturing",
    "pharma": "healthcare",
    "pharma_biotech": "healthcare",
    "finance": "financial_services",
    "finance_banking": "financial_services",
    "insurance": "financial_services",
    "government": "government_utilities",
    "military_recruitment": "government_utilities",
    "energy": "government_utilities",
    "energy_utilities": "government_utilities",
    "education": "government_utilities",
    "professional_services": "technology",
    "legal_services": "financial_services",
    "nonprofit": "government_utilities",
    "general": "retail_hospitality",
    "general_entry_level": "retail_hospitality",
    "media_entertainment": "technology",
}


def load_knowledge_base() -> dict:
    """Load and merge all knowledge base files into unified dict.

    Loads 8 JSON data files from the ``data/`` directory, each into its own
    section key.  The ``core`` section (recruitment_industry_knowledge.json) is
    additionally merged into the top level for backward compatibility so that
    existing code referencing ``kb["benchmarks"]``, ``kb["salary_trends"]``,
    etc. continues to work.

    Uses a module-level cache so the files are read at most once.

    Returns:
        Merged dict with section keys + backward-compat top-level keys,
        or a minimal dict on failure.
    """
    global _knowledge_base
    if _knowledge_base is not None:
        return _knowledge_base

    files = {
        "core":                   "recruitment_industry_knowledge.json",
        "platform_intelligence":  "platform_intelligence_deep.json",
        "recruitment_benchmarks": "recruitment_benchmarks_deep.json",
        "recruitment_strategy":   "recruitment_strategy_intelligence.json",
        "regional_hiring":        "regional_hiring_intelligence.json",
        "supply_ecosystem":       "supply_ecosystem_intelligence.json",
        "workforce_trends":       "workforce_trends_intelligence.json",
        "white_papers":           "industry_white_papers.json",
        "joveo_2026_benchmarks":  "joveo_2026_benchmarks.json",
    }

    kb = {}
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    loaded_count = 0
    for section_key, filename in files.items():
        fpath = os.path.join(data_dir, filename)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                kb[section_key] = json.load(f)
                loaded_count += 1
                logger.info("KB loaded %s (%s)", section_key, filename)
        except FileNotFoundError:
            kb[section_key] = {}
            logger.warning("KB file not found: %s", filename)
        except json.JSONDecodeError as e:
            kb[section_key] = {}
            logger.error("KB JSON error in %s: %s", filename, e)
        except Exception as e:
            kb[section_key] = {}
            logger.error("KB load error for %s: %s", filename, e)

    # Backward compatibility: merge core keys to top level so existing
    # code that accesses kb["benchmarks"], kb["salary_trends"], etc. still works
    core = kb.get("core", {})
    for k, v in core.items():
        if k not in kb:  # don't overwrite section keys
            kb[k] = v

    # ── Data Freshness Validation ──
    # Check last_updated metadata in each KB section and warn if data is
    # older than 90 days.  This prevents silently serving stale benchmarks
    # when JSON files haven't been regenerated in a long time.
    stale_sections = []
    try:
        today = datetime.datetime.now()
        max_age_days = 90
        for section_key, section_data in kb.items():
            if not isinstance(section_data, dict):
                continue
            # Check top-level and nested metadata for last_updated
            last_updated_str = None
            if isinstance(section_data.get("metadata"), dict):
                last_updated_str = section_data["metadata"].get("last_updated")
            if not last_updated_str:
                last_updated_str = section_data.get("last_updated")
            if last_updated_str and isinstance(last_updated_str, str):
                try:
                    lu_date = datetime.datetime.strptime(
                        last_updated_str[:10], "%Y-%m-%d"
                    )
                    age_days = (today - lu_date).days
                    if age_days > max_age_days:
                        stale_sections.append(
                            (section_key, last_updated_str, age_days)
                        )
                except (ValueError, TypeError):
                    pass
        if stale_sections:
            for skey, sdate, sage in stale_sections:
                logger.warning(
                    "KB DATA FRESHNESS WARNING: '%s' last updated %s "
                    "(%d days ago, threshold=%d days)",
                    skey, sdate, sage, max_age_days,
                )
            kb["_freshness_warnings"] = [
                {"section": s, "last_updated": d, "age_days": a}
                for s, d, a in stale_sections
            ]
    except Exception as e:
        logger.warning("KB freshness check failed (non-fatal): %s", e)

    logger.info("Knowledge base loaded: %d/%d files, %d total keys",
                loaded_count, len(files), len(kb))
    _knowledge_base = kb
    return kb



# ═══════════════════════════════════════════════════════════════════════════════
# NAICS-BASED INDUSTRY CLASSIFICATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════
INDUSTRY_NAICS_MAP = {
    # Technology / Software (NAICS 51)
    "technology": {"naics": "51", "sector": "Technology & Software", "bls_sector": "Information", 
                   "talent_profile": "Professional / Technical Workforce",
                   "legacy_key": "tech_engineering",
                   "keywords": ["tech", "software", "saas", "cloud", "ai", "data", "cyber", "digital", "it services", "information technology"]},
    # Healthcare (NAICS 62)
    "healthcare": {"naics": "62", "sector": "Healthcare & Life Sciences", "bls_sector": "Health Care",
                   "talent_profile": "Clinical & Allied Health Workforce",
                   "legacy_key": "healthcare_medical",
                   "keywords": ["health", "hospital", "medical", "pharma", "biotech", "clinical", "nursing", "patient care", "life sciences"]},
    # Finance (NAICS 52)
    "finance": {"naics": "52", "sector": "Financial Services & Insurance", "bls_sector": "Financial Activities",
                "talent_profile": "Professional / Financial Workforce",
                "legacy_key": "finance_banking",
                "keywords": ["bank", "finance", "insurance", "investment", "capital", "wealth", "fintech", "credit", "mortgage", "securities"]},
    # Manufacturing (NAICS 31-33)
    "manufacturing": {"naics": "31-33", "sector": "Manufacturing & Industrial", "bls_sector": "Manufacturing",
                      "talent_profile": "Skilled Trades & Production Workforce",
                      "legacy_key": "automotive",
                      "keywords": ["manufactur", "industrial", "production", "assembly", "factory"]},
    # Retail / E-commerce (NAICS 44-45)
    "retail": {"naics": "44-45", "sector": "Retail & E-Commerce", "bls_sector": "Retail Trade",
               "talent_profile": "General / Entry-Level & Hourly Workforce",
               "legacy_key": "retail_consumer",
               "keywords": ["retail", "e-commerce", "ecommerce", "store", "shop", "consumer", "merchandise", "grocery"]},
    # Transportation / Logistics (NAICS 48-49)  
    "transportation": {"naics": "48-49", "sector": "Transportation & Logistics", "bls_sector": "Transportation and Warehousing",
                       "talent_profile": "Operations & Logistics Workforce",
                       "legacy_key": "logistics_supply_chain",
                       "keywords": ["transport", "logistics", "shipping", "delivery", "freight", "trucking", "courier", "warehouse", "supply chain", "fleet"]},
    # Hospitality (NAICS 72)
    "hospitality": {"naics": "72", "sector": "Hospitality & Tourism", "bls_sector": "Leisure and Hospitality",
                    "talent_profile": "Service & Hospitality Workforce",
                    "legacy_key": "hospitality_travel",
                    "keywords": ["hotel", "hospitality", "restaurant", "food service", "tourism", "travel", "lodging", "resort", "dining"]},
    # Education (NAICS 61)
    "education": {"naics": "61", "sector": "Education & Training", "bls_sector": "Education",
                  "talent_profile": "Academic & Educational Workforce",
                  "legacy_key": "education",
                  "keywords": ["education", "school", "university", "college", "academic", "teaching", "k-12", "k12", "training", "teacher", "principal", "isd"]},
    # Professional Services (NAICS 54)
    "professional_services": {"naics": "54", "sector": "Professional & Business Services", "bls_sector": "Professional and Business Services",
                              "talent_profile": "Professional / Consulting Workforce",
                              "legacy_key": "legal_services",
                              "keywords": ["consulting", "professional service", "accounting", "legal", "audit", "advisory", "staffing", "management consulting"]},
    # Construction (NAICS 23)
    "construction": {"naics": "23", "sector": "Construction & Real Estate", "bls_sector": "Construction",
                     "talent_profile": "Skilled Trades & Construction Workforce",
                     "legacy_key": "construction_real_estate",
                     "keywords": ["construction", "building", "real estate", "property", "architecture", "engineering firm", "contractor"]},
    # Energy (NAICS 21, 22)
    "energy": {"naics": "21-22", "sector": "Energy & Utilities", "bls_sector": "Mining and Logging",
               "talent_profile": "Technical & Field Operations Workforce",
               "legacy_key": "energy_utilities",
               "keywords": ["energy", "oil", "gas", "solar", "renewable", "electric", "utility", "power", "mining", "petroleum"]},
    # Government (NAICS 92)
    "government": {"naics": "92", "sector": "Government & Public Administration", "bls_sector": "Government",
                   "talent_profile": "Public Sector Workforce",
                   "legacy_key": "military_recruitment",
                   "keywords": ["government", "federal", "state agency", "municipal", "public sector", "military", "defense", "civil service", "army", "navy", "air force", "marines"]},
    # Telecommunications (NAICS 51 sub)
    "telecom": {"naics": "51", "sector": "Telecommunications", "bls_sector": "Information",
                "talent_profile": "Technical & Engineering Workforce",
                "legacy_key": "telecommunications",
                "keywords": ["telecom", "wireless", "broadband", "5g", "network", "communications"]},
    # Agriculture (NAICS 11)
    "agriculture": {"naics": "11", "sector": "Agriculture & Food Production", "bls_sector": "Agriculture",
                    "talent_profile": "Agricultural & Seasonal Workforce",
                    "legacy_key": "food_beverage",
                    "keywords": ["agriculture", "farming", "food production", "agri", "crop", "livestock"]},
    # Nonprofit (NAICS 81)
    "nonprofit": {"naics": "81", "sector": "Nonprofit & Social Services", "bls_sector": "Other Services",
                  "talent_profile": "Mission-Driven Workforce",
                  "legacy_key": "general_entry_level",
                  "keywords": ["nonprofit", "non-profit", "ngo", "charity", "foundation", "social services"]},
    # General / Entry-Level (catch-all)
    "general": {"naics": "00", "sector": "General / Multi-Industry", "bls_sector": "Total Nonfarm",
                "talent_profile": "General / Entry-Level & Hourly Workforce",
                "legacy_key": "general_entry_level",
                "keywords": ["general", "entry level", "entry-level", "hourly", "temp", "seasonal", "staffing agency"]},
    # Rideshare / Gig Economy (NAICS 48 sub)
    "rideshare": {"naics": "48", "sector": "Rideshare & Gig Economy", "bls_sector": "Transportation and Warehousing",
                  "talent_profile": "Gig & Independent Contractor Workforce",
                  "legacy_key": "logistics_supply_chain",
                  "keywords": ["rideshare", "ride-share", "gig", "uber", "lyft", "doordash", "instacart", "platform", "on-demand"]},
    # Blue Collar / Skilled Trades (NAICS 23/31-33 crossover)
    "blue_collar": {"naics": "31-33", "sector": "Blue Collar / Skilled Trades", "bls_sector": "Manufacturing",
                    "talent_profile": "Skilled Trades & Production Workforce",
                    "legacy_key": "blue_collar_trades",
                    "keywords": ["blue collar", "skilled trade", "welder", "electrician", "plumber", "mechanic", "hvac", "trade"]},
    # Maritime / Marine (NAICS 48 sub)
    "maritime": {"naics": "48", "sector": "Maritime & Marine", "bls_sector": "Transportation and Warehousing",
                 "talent_profile": "Maritime & Marine Workforce",
                 "legacy_key": "maritime_marine",
                 "keywords": ["maritime", "marine", "shipping", "naval", "shipyard", "port", "offshore", "vessel", "seafar"]},
    # Aerospace & Defense (NAICS 33 sub)
    "aerospace": {"naics": "33", "sector": "Aerospace & Defense", "bls_sector": "Manufacturing",
                  "talent_profile": "Aerospace & Defense Workforce",
                  "legacy_key": "aerospace_defense",
                  "keywords": ["aerospace", "defense", "aviation", "aircraft", "missile", "satellite", "space"]},
    # Pharma & Biotech (NAICS 32 sub)
    "pharma": {"naics": "32", "sector": "Pharma & Biotech", "bls_sector": "Manufacturing",
               "talent_profile": "Pharmaceutical & Research Workforce",
               "legacy_key": "pharma_biotech",
               "keywords": ["pharma", "biotech", "drug", "vaccine", "clinical trial", "biolog", "therapeutic"]},
    # Insurance (NAICS 52 sub)
    "insurance": {"naics": "52", "sector": "Insurance", "bls_sector": "Financial Activities",
                  "talent_profile": "Professional / Insurance Workforce",
                  "legacy_key": "insurance",
                  "keywords": ["insurance", "underwriter", "actuar", "claims", "policy"]},
    # Mental Health (NAICS 62 sub)
    "mental_health": {"naics": "62", "sector": "Mental Health & Behavioral", "bls_sector": "Health Care",
                      "talent_profile": "Behavioral & Mental Health Workforce",
                      "legacy_key": "mental_health",
                      "keywords": ["mental health", "behavioral", "counselor", "therapist", "psycholog", "psychiatr", "substance abuse"]},
    # Media & Entertainment (NAICS 71)
    "media_entertainment": {"naics": "71", "sector": "Media & Entertainment", "bls_sector": "Leisure and Hospitality",
                            "talent_profile": "Creative & Media Workforce",
                            "legacy_key": "media_entertainment",
                            "keywords": ["media", "entertainment", "film", "broadcast", "streaming", "gaming", "music", "content"]},
    # Food & Beverage (NAICS 72 sub / 31)
    "food_beverage": {"naics": "31", "sector": "Food & Beverage", "bls_sector": "Manufacturing",
                      "talent_profile": "Food Service & Production Workforce",
                      "legacy_key": "food_beverage",
                      "keywords": ["food", "beverage", "brewery", "distillery", "catering", "bakery", "meat", "dairy"]},
}

# Mapping from legacy industry keys (used in channels_db, research.py) to NAICS map keys
# Priority entries that should NOT be overridden by later NAICS map entries
_LEGACY_PRIORITY = {
    "general_entry_level": "general",
    "logistics_supply_chain": "transportation",
    "automotive": "manufacturing",
    "food_beverage": "food_beverage",
}
_LEGACY_TO_NAICS_KEY = {}
for _nk, _nv in INDUSTRY_NAICS_MAP.items():
    _lk = _nv.get("legacy_key", "")
    if _lk and _lk not in _LEGACY_TO_NAICS_KEY:
        _LEGACY_TO_NAICS_KEY[_lk] = _nk
# Apply priority overrides
for _lk, _nk in _LEGACY_PRIORITY.items():
    if _nk in INDUSTRY_NAICS_MAP:
        _LEGACY_TO_NAICS_KEY[_lk] = _nk

def classify_industry(raw_industry: str, company_name: str = "", roles: list = None) -> dict:
    """
    Classify an industry input into the correct NAICS sector with talent profile.
    Uses fuzzy keyword matching against the input string AND company name AND roles.
    
    Handles both:
    - Legacy keys (e.g., "healthcare_medical", "tech_engineering") from the frontend dropdown
    - Free-text industry names (e.g., "Technology", "Healthcare") from API/manual input
    
    Returns the full industry profile dict including legacy_key for backward compatibility.
    """
    if not raw_industry:
        raw_industry = ""
    
    # Step 1: Check if the input is already a legacy key (from frontend dropdown)
    raw_stripped = raw_industry.strip()
    if raw_stripped in _LEGACY_TO_NAICS_KEY:
        return INDUSTRY_NAICS_MAP[_LEGACY_TO_NAICS_KEY[raw_stripped]]
    
    # Step 2: Check if the input directly matches a NAICS map key
    raw_lower = raw_stripped.lower()
    if raw_lower in INDUSTRY_NAICS_MAP:
        return INDUSTRY_NAICS_MAP[raw_lower]
    
    # Step 3: Fuzzy keyword matching against input + company name + roles
    search_text = f"{raw_industry} {company_name} {' '.join(roles or [])}".lower()
    
    best_match = None
    best_score = 0
    
    for key, profile in INDUSTRY_NAICS_MAP.items():
        score = 0
        for kw in profile["keywords"]:
            if kw in search_text:
                # Longer keyword matches are weighted higher
                score += len(kw)
        if score > best_score:
            best_score = score
            best_match = profile
    
    if best_match and best_score >= 3:
        return best_match
    
    # Step 4: Fallback - try to match the raw industry string directly against sector names
    if raw_lower:  # Only attempt if we have a non-empty industry string
        for key, profile in INDUSTRY_NAICS_MAP.items():
            if key in raw_lower or raw_lower in profile["sector"].lower():
                return profile
    
    # Final fallback
    return {
        "naics": "00", 
        "sector": raw_industry.strip() if raw_industry and raw_industry.strip() else "General / Multi-Industry",
        "bls_sector": "Total Nonfarm",
        "talent_profile": "General / Mixed Workforce",
        "legacy_key": "general_entry_level",
        "keywords": []
    }



BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")

# --- Request logging helpers ---
REQUEST_LOG_FILE = os.path.join(DATA_DIR, "request_log.json")
REQUEST_LOG_LOCK = REQUEST_LOG_FILE + ".lock"

def load_request_log():
    try:
        with open(REQUEST_LOG_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_request_log(log):
    with open(REQUEST_LOG_FILE, "w") as f:
        json.dump(log, f, indent=2, default=str)

def _cleanup_old_docs(docs_dir, max_files=200):
    """Remove oldest generated docs if directory exceeds max_files."""
    try:
        files = sorted(
            [f for f in os.listdir(docs_dir) if f.endswith('.zip')],
            key=lambda f: os.path.getmtime(os.path.join(docs_dir, f))
        )
        while len(files) > max_files:
            oldest = files.pop(0)
            os.remove(os.path.join(docs_dir, oldest))
    except OSError:
        pass

def log_request(data, status, file_size=0, generation_time=0, error_msg=None, doc_filename=None):
    """Append a log entry with file locking to prevent corruption from concurrent writes."""
    entry = {
        "id": str(uuid.uuid4())[:8],
        "timestamp": datetime.datetime.now().isoformat(),
        "requester_name": data.get("requester_name", "Unknown"),
        "requester_email": data.get("requester_email", "Unknown"),
        "client_name": data.get("client_name") or data.get("company_name", "Unknown"),
        "industry": data.get("industry", "Unknown"),
        "budget": data.get("budget", "Not specified"),
        "roles": data.get("target_roles") or data.get("roles", []),
        "locations": data.get("locations", []),
        "work_environment": data.get("work_environment", "Not specified"),
        "status": status,
        "file_size_bytes": file_size,
        "generation_time_seconds": round(generation_time, 2),
        "error_message": error_msg,
        "doc_filename": doc_filename,
        "enrichment_apis": data.get("_enriched", {}).get("enrichment_summary", {}).get("apis_succeeded", []) if isinstance(data.get("_enriched"), dict) else [],
    }
    # Use file locking to prevent concurrent write corruption
    try:
        os.makedirs(os.path.dirname(REQUEST_LOG_LOCK), exist_ok=True)
        with open(REQUEST_LOG_LOCK, "w") as lock_fd:
            try:
                if fcntl:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX)
                log = load_request_log()
                log.append(entry)
                # Keep only last 1000 entries to prevent unbounded growth
                if len(log) > 1000:
                    log = log[-1000:]
                save_request_log(log)
            finally:
                if fcntl:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
    except OSError as e:
        logger.error("Failed to write request log: %s", e)
    return entry

# Import research module for real data
sys.path.insert(0, BASE_DIR)
import research

# ── Canonical taxonomy standardizer ──
# Normalizes industry, role, location, platform, and metric names across
# the entire pipeline so that every subsystem uses the same canonical keys.
# Without this, industry/role/location strings from the frontend can arrive
# in dozens of variant forms and silently miss KB lookups downstream.
try:
    from standardizer import (
        normalize_industry as std_normalize_industry,
        normalize_role as std_normalize_role,
        normalize_location as std_normalize_location,
        normalize_platform as std_normalize_platform,
        normalize_metric as std_normalize_metric,
        CANONICAL_INDUSTRIES,
        CANONICAL_ROLES,
        get_soc_code as std_get_soc_code,
        get_role_tier as std_get_role_tier,
    )
    _STANDARDIZER_AVAILABLE = True
    logger.info("standardizer loaded successfully")
except ImportError as e:
    _STANDARDIZER_AVAILABLE = False
    logger.warning("standardizer import failed: %s", e)

try:
    from ppt_generator import generate_pptx, INDUSTRY_ALLOC_PROFILES
    logger.info("ppt_generator loaded successfully")
except ImportError as e:
    logger.warning("ppt_generator import failed: %s", e)
    generate_pptx = None
    INDUSTRY_ALLOC_PROFILES = None

try:
    from api_enrichment import enrich_data
    logger.info("api_enrichment loaded successfully")
except ImportError as e:
    logger.warning("api_enrichment import failed: %s", e)
    enrich_data = None

try:
    from data_synthesizer import synthesize as data_synthesize
    logger.info("data_synthesizer loaded successfully")
except ImportError as e:
    logger.warning("data_synthesizer import failed: %s", e)
    data_synthesize = None

try:
    from budget_engine import calculate_budget_allocation
    logger.info("budget_engine loaded successfully")
except ImportError as e:
    logger.warning("budget_engine import failed: %s", e)
    calculate_budget_allocation = None

# v3: Trend engine and collar intelligence for new Excel worksheets
try:
    import trend_engine as _trend_engine_mod
    _HAS_TREND_ENGINE = True
    logger.info("trend_engine loaded for app.py Excel worksheets")
except ImportError:
    _HAS_TREND_ENGINE = False

try:
    import collar_intelligence as _collar_intel_mod
    _HAS_COLLAR_INTEL = True
    logger.info("collar_intelligence loaded for app.py Excel worksheets")
except ImportError:
    _HAS_COLLAR_INTEL = False

# ═══════════════════════════════════════════════════════════════════════════════
# ROLE-TIER CLASSIFICATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

ROLE_TIER_KEYWORDS = {
    "executive": {
        "tier": "Executive / Leadership",
        "keywords": ["ceo", "cfo", "cto", "cio", "cmo", "vp ", "vice president", "svp", "evp", "chief", "president", "director", "head of"],
        "funnel": {"awareness": 0.10, "consideration": 0.25, "engagement": 0.35, "conversion": 0.30},
        "channels": ["Executive search firms", "LinkedIn Recruiter", "Board networks", "Industry conferences"],
        "cpa_multiplier": 3.0,
        "time_to_fill_days": 90,
    },
    "professional": {
        "tier": "Professional / White-Collar",
        "keywords": ["engineer", "developer", "analyst", "manager", "consultant", "architect", "scientist", "specialist",
                     "accountant", "auditor", "attorney", "lawyer", "planner", "strategist", "designer", "product", "data",
                     "researcher", "coordinator", "administrator", "compliance", "underwriter"],
        "funnel": {"awareness": 0.20, "consideration": 0.30, "engagement": 0.30, "conversion": 0.20},
        "channels": ["LinkedIn", "Indeed", "Glassdoor", "Company careers page", "Industry job boards"],
        "cpa_multiplier": 1.5,
        "time_to_fill_days": 45,
    },
    "clinical": {
        "tier": "Clinical / Licensed",
        "keywords": ["nurse", "rn ", "lpn", "cna", "physician", "doctor", "therapist", "pharmacist", "paramedic",
                     "emt", "medical assistant", "dental", "radiology", "sonograph", "surgical tech", "respiratory"],
        "funnel": {"awareness": 0.15, "consideration": 0.25, "engagement": 0.35, "conversion": 0.25},
        "channels": ["NurseFly", "Vivian Health", "Health eCareers", "Indeed Healthcare", "Hospital networks"],
        "cpa_multiplier": 2.0,
        "time_to_fill_days": 60,
    },
    "skilled_trades": {
        "tier": "Skilled Trades / Technical",
        "keywords": ["technician", "mechanic", "electrician", "plumber", "welder", "hvac", "machinist",
                     "installer", "maintenance", "repair", "inspector", "quality", "foreman", "carpenter",
                     "aircraft mechanic", "solar panel", "wind turbine"],
        "funnel": {"awareness": 0.25, "consideration": 0.25, "engagement": 0.25, "conversion": 0.25},
        "channels": ["Trade schools", "Union halls", "Indeed Blue", "Craigslist", "Local job fairs"],
        "cpa_multiplier": 1.2,
        "time_to_fill_days": 30,
    },
    "hourly": {
        "tier": "Hourly / Entry-Level",
        "keywords": ["associate", "clerk", "cashier", "barista", "crew", "team member", "warehouse", "package handler",
                     "picker", "packer", "stocker", "janitor", "custodian", "housekeeper", "dishwasher",
                     "food prep", "line cook", "server", "host", "busser", "retail associate", "store associate",
                     "front desk", "receptionist", "call center", "customer service rep", "csr"],
        "funnel": {"awareness": 0.35, "consideration": 0.25, "engagement": 0.20, "conversion": 0.20},
        "channels": ["Indeed", "Snagajob", "Jobcase", "Facebook Jobs", "Walk-in/Referral", "Text-to-Apply"],
        "cpa_multiplier": 0.5,
        "time_to_fill_days": 14,
    },
    "gig": {
        "tier": "Gig / Independent Contractor",
        "keywords": ["driver partner", "delivery partner", "delivery driver", "courier", "rider", "dasher",
                     "shopper", "freelance", "contractor", "1099", "independent", "flex", "on-demand"],
        "funnel": {"awareness": 0.40, "consideration": 0.20, "engagement": 0.15, "conversion": 0.25},
        "channels": ["Programmatic ads", "Social media (Facebook/Instagram)", "Referral bonuses", "Google Ads", "SMS campaigns"],
        "cpa_multiplier": 0.3,
        "time_to_fill_days": 7,
    },
    "education": {
        "tier": "Education / Academic",
        "keywords": ["teacher", "professor", "instructor", "principal", "superintendent", "dean", "librarian",
                     "counselor", "aide", "paraprofessional", "substitute", "tutor", "curriculum", "coach"],
        "funnel": {"awareness": 0.20, "consideration": 0.30, "engagement": 0.30, "conversion": 0.20},
        "channels": ["SchoolSpring", "K12JobSpot", "HigherEdJobs", "State education boards", "University career offices"],
        "cpa_multiplier": 1.0,
        "time_to_fill_days": 45,
    },
}


def classify_role_tier(role_title: str) -> dict:
    """Classify a role into a tier based on keyword matching. Returns the tier profile."""
    role_lower = role_title.lower().strip()

    best_match = None
    best_score = 0

    for tier_key, tier_data in ROLE_TIER_KEYWORDS.items():
        score = 0
        for kw in tier_data["keywords"]:
            if kw in role_lower:
                score += len(kw)  # Longer keyword matches score higher
        if score > best_score:
            best_score = score
            best_match = tier_data

    if best_match and best_score >= 2:
        return best_match

    # Default to professional if no match
    return ROLE_TIER_KEYWORDS["professional"]


# Load global supply data
GLOBAL_SUPPLY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "global_supply.json")
global_supply_data = {}
try:
    with open(GLOBAL_SUPPLY_PATH, "r") as f:
        global_supply_data = json.load(f)
except (FileNotFoundError, json.JSONDecodeError, OSError):
    pass

_channels_db_cache = None
_joveo_publishers_cache = None

def load_channels_db():
    global _channels_db_cache
    if _channels_db_cache is not None:
        return _channels_db_cache
    try:
        with open(os.path.join(DATA_DIR, "channels_db.json"), "r") as f:
            _channels_db_cache = json.load(f)
            return _channels_db_cache
    except (FileNotFoundError, json.JSONDecodeError, OSError) as e:
        logger.error("Failed to load channels_db.json: %s", e)
        return {}

def load_joveo_publishers():
    global _joveo_publishers_cache
    if _joveo_publishers_cache is not None:
        return _joveo_publishers_cache
    path = os.path.join(DATA_DIR, "joveo_publishers.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            _joveo_publishers_cache = json.load(f)
            return _joveo_publishers_cache
    except FileNotFoundError:
        logger.warning("joveo_publishers.json not found at %s", path)
        return {}
    except json.JSONDecodeError as e:
        logger.error("joveo_publishers.json JSON parse error: %s", e)
        return {}
    except OSError as e:
        logger.error("Failed to read joveo_publishers.json: %s", e)
        return {}


def fetch_client_logo(client_name, client_website=""):
    """Try to fetch a client logo using the client website URL (preferred) or name-based domain guess."""
    domain = ""

    # 1. Try to extract domain from the provided client website URL (most accurate)
    if client_website:
        try:
            parsed = urlparse(client_website if "://" in client_website else f"https://{client_website}")
            domain = parsed.hostname or ""
            # Remove www. prefix for cleaner domain
            if domain.startswith("www."):
                domain = domain[4:]
        except Exception:
            pass

    # 2. Fallback: guess domain from client name
    if not domain and client_name:
        name = client_name.lower().strip()
        for suffix in [" inc", " inc.", " llc", " ltd", " corp", " corporation", " co", " company", " group", " international"]:
            name = name.replace(suffix, "")
        domain = re.sub(r'[^a-z0-9]', '', name) + ".com"

    if not domain:
        return None, None

    # Try Clearbit Logo API (high quality, free for logos), then Google favicon
    logo_urls = [
        f"https://logo.clearbit.com/{domain}",
        f"https://www.google.com/s2/favicons?domain={domain}&sz=128",
    ]

    for url in logo_urls:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            response = urllib.request.urlopen(req, timeout=5)
            if response.status == 200:
                img_data = response.read()
                if len(img_data) > 500:  # Ensure it's not a tiny placeholder
                    return img_data, url
        except Exception:
            continue
    return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# INDUSTRY-SPECIFIC NICHE CHANNELS MAPPING
# ═══════════════════════════════════════════════════════════════════════════════
INDUSTRY_NICHE_CHANNELS = {
    "technology": ["Stack Overflow Jobs", "GitHub Jobs", "HackerNews (Who's Hiring)", "AngelList/Wellfound", "Dice", "Built In", "Triplebyte"],
    "tech_engineering": ["Stack Overflow Jobs", "GitHub Jobs", "HackerNews (Who's Hiring)", "AngelList/Wellfound", "Dice", "Built In", "Triplebyte"],
    "healthcare": ["NurseFly", "Vivian Health", "Health eCareers", "Doximity", "PracticeLink", "Medscape Career Center", "AllNurses"],
    "healthcare_medical": ["NurseFly", "Vivian Health", "Health eCareers", "Doximity", "PracticeLink", "Medscape Career Center", "AllNurses"],
    "finance": ["eFinancialCareers", "Wall Street Oasis", "Selby Jennings", "The Muse (Finance)", "Vault.com"],
    "finance_banking": ["eFinancialCareers", "Wall Street Oasis", "Selby Jennings", "The Muse (Finance)", "Vault.com"],
    "hospitality": ["Hcareers", "Poached", "Culinary Agents", "Harri", "OysterLink"],
    "hospitality_travel": ["Hcareers", "Poached", "Culinary Agents", "Harri", "OysterLink"],
    "education": ["SchoolSpring", "K12JobSpot", "HigherEdJobs", "Chronicle Vitae", "TeachAway", "Teach.org"],
    "manufacturing": ["ManufacturingJobs.com", "iHireManufacturing", "JobsInManufacturing", "IndustryWeek Careers"],
    "transportation": ["CDLJobs.com", "TruckingJobs.com", "FleetOwner Careers", "Transport Topics Jobs", "DriveForMe"],
    "energy": ["Rigzone", "EnergyJobline", "Solar Energy Jobs", "NABCEP Job Board", "Oil and Gas Job Search"],
    "energy_utilities": ["Rigzone", "EnergyJobline", "Solar Energy Jobs", "NABCEP Job Board", "Oil and Gas Job Search"],
    "retail": ["RetailCareersNow", "AllRetailJobs", "iHireRetail", "NRF Job Board"],
    "retail_consumer": ["RetailCareersNow", "AllRetailJobs", "iHireRetail", "NRF Job Board"],
    "professional_services": ["Vault.com", "Management Consulted", "Fishbowl", "WetFeet", "Idealist (for nonprofits)"],
    "construction": ["ConstructionJobs.com", "iHireConstruction", "AGC Career Center", "BuildingTeams"],
    "construction_real_estate": ["ConstructionJobs.com", "iHireConstruction", "AGC Career Center", "BuildingTeams"],
    "rideshare": ["Ridester", "The Rideshare Guy", "Gridwise community", "Driver forums", "Facebook gig groups"],
    "government": ["USAJobs", "GovernmentJobs.com", "ClearanceJobs", "Intelligence Careers"],
    "nonprofit": ["Idealist", "Work for Good", "DevEx", "NGOJobBoard", "Foundation List"],
    "blue_collar_trades": ["TradesmenInternational", "CraftHire", "PeopleReady", "iHireConstruction", "CDLJobs.com"],
    "maritime_marine": ["RigZone", "SeaCareer", "MarineLink Jobs", "AllMarinerJobs", "gCaptain Jobs"],
    "military_recruitment": ["RecruitMilitary", "Military.com", "ClearanceJobs", "Hire Heroes USA", "USAJobs.gov"],
    "legal_services": ["LawJobs.com", "Robert Half Legal", "Above the Law", "NALP Job Board", "Lawcrossing.com"],
    "mental_health": ["Psychology Today Jobs", "NASW Career Center", "APA PsycCareers", "SocialWorkJobBank", "iHireTherapy"],
    "aerospace_defense": ["ClearanceJobs", "ClearedJobs.Net", "Aviation Job Search", "SpaceCareers.uk", "Airswift"],
    "pharma_biotech": ["PharmiWeb.com", "BioSpace", "MedReps", "Science Careers (AAAS)", "Nature Careers"],
    "insurance": ["InsuranceJobs.com", "GreatInsuranceJobs.com", "Actuarial Careers Inc", "SOA Career Center"],
    "telecommunications": ["WirelessEstimator Jobs", "FierceTelecom Careers", "Light Reading Jobs", "IEEE Job Site"],
    "automotive": ["AutomotiveJobFinder", "iHireEngineering", "SAE International Jobs", "AutoCareers"],
    "food_beverage": ["FoodIndustryJobs.com", "iHireHospitality", "FoodProcessing.com Careers", "IFT Career Center"],
    "logistics_supply_chain": ["SupplyChainRecruit.com", "LogisticsJobShop.com", "3PL Jobs", "WarehouseJobs.com"],
    "media_entertainment": ["ProductionHub", "Mandy.com", "EntertainmentCareers.net", "MediaBistro"],
    "general_entry_level": ["Snagajob", "Wonolo", "Instawork", "Jobcase", "College Recruiter"],
}


def _verify_plan_data(data):
    """Use Gemini/secondary LLM to verify key facts in plan data before output.

    Checks channel recommendations, benchmark numbers, and location appropriateness.
    Returns verification status dict. Non-blocking -- failures return 'skipped'.
    """
    try:
        from llm_router import call_llm, TASK_VERIFICATION
    except ImportError:
        return {"status": "skipped", "reason": "llm_router_unavailable"}

    synthesized = data.get("_synthesized", {})
    if not isinstance(synthesized, dict):
        return {"status": "skipped", "reason": "no_synthesized_data"}

    # Build a concise snapshot of key plan data points for verification
    client = data.get("client_name", "Client")
    industry = data.get("industry_label", data.get("industry", ""))
    locations = data.get("locations", [])
    roles = data.get("roles", [])
    budget = data.get("budget", "N/A")
    budget_alloc = data.get("_budget_allocation", {})

    # Extract key numbers to verify
    check_items = []
    if isinstance(budget_alloc, dict):
        meta = budget_alloc.get("metadata", {})
        if meta:
            check_items.append(f"Total budget: ${meta.get('total_budget', 'N/A')}")
        ch_allocs = budget_alloc.get("channel_allocations", {})
        for ch_name, ch_data in list(ch_allocs.items())[:5]:
            if isinstance(ch_data, dict):
                check_items.append(
                    f"{ch_name}: ${ch_data.get('dollar_amount', 0):.0f}, "
                    f"CPC=${ch_data.get('cpc', 0):.2f}, "
                    f"est clicks={ch_data.get('projected_clicks', 0)}"
                )

    if not check_items:
        return {"status": "skipped", "reason": "no_data_to_verify"}

    prompt = f"""Verify this recruitment media plan data for reasonableness.

Client: {client}
Industry: {industry}
Locations: {', '.join(str(l) for l in locations[:5])}
Roles: {', '.join(str(r) for r in roles[:5])}
Budget: {budget}

Key allocations:
{chr(10).join(check_items)}

Check:
1. Are CPC values reasonable for the industry/channels? (typical ranges: Indeed $0.10-2.00, LinkedIn $1.50-8.00, Google $0.50-5.00)
2. Are click/application projections mathematically consistent with budget and CPC?
3. Are channel recommendations appropriate for this industry?

Return ONLY valid JSON:
{{"verified": true, "issues": [], "severity": "none"}}
OR
{{"verified": false, "issues": ["issue1", "issue2"], "severity": "minor|major"}}"""

    try:
        result = call_llm(
            messages=[{"role": "user", "content": prompt}],
            system_prompt="You are a recruitment advertising data verifier. Return ONLY valid JSON.",
            max_tokens=512,
            task_type=TASK_VERIFICATION,
            query_text="verify media plan data",
            preferred_providers=["gemini"],
        )
        if result and (result.get("text") or result.get("content")):
            content = result.get("text") or result.get("content", "")
            import re as _re
            json_match = _re.search(r'\{[\s\S]*?\}', content)
            if json_match:
                parsed = json.loads(json_match.group())
                return {
                    "status": "verified" if parsed.get("verified") else "issues_found",
                    "issues": parsed.get("issues", []),
                    "severity": parsed.get("severity", "none"),
                    "provider": result.get("provider", "unknown"),
                }
    except Exception:
        pass

    return {"status": "skipped", "reason": "verification_failed"}


def generate_excel(data):
    # Normalize budget: frontend sends "budget_range" but code reads "budget"
    if data.get("budget_range") and not data.get("budget"):
        data["budget"] = data["budget_range"]

    # Null safety for all input fields
    for key, default in [("client_name", "Client"), ("company_name", "Client"), ("industry", "general_entry_level"), ("budget", "Not specified"), ("work_environment", "hybrid")]:
        if not data.get(key):
            data[key] = default
    # Ensure list fields are actual lists
    for key in ["locations", "roles", "target_roles", "campaign_goals", "competitors"]:
        val = data.get(key)
        if val is None:
            data[key] = []
        elif isinstance(val, str):
            data[key] = [val]

    # ── Input Standardization ──
    # Uses shared_utils for location standardization (single source of truth)
    if isinstance(data.get("locations"), list):
        data["locations"] = [_shared_standardize_location(loc) if isinstance(loc, str) else loc for loc in data["locations"]]
    # Standardize role titles (title case)
    for role_key in ("target_roles", "roles"):
        if isinstance(data.get(role_key), list):
            data[role_key] = [
                r.strip().title() if isinstance(r, str) else r
                for r in data[role_key]
            ]
    # Standardize company name (title case if all lower/upper)
    _cn = data.get("client_name", "")
    if isinstance(_cn, str) and (_cn == _cn.lower() or _cn == _cn.upper()) and len(_cn) > 3:
        data["client_name"] = _cn.title()

    db = load_channels_db()
    joveo_pubs = load_joveo_publishers()
    gs = global_supply_data  # global supply reference

    # Pass publisher data and channels DB into data dict so PPT generator can access them
    data["_joveo_publishers"] = joveo_pubs
    data["_channels_db"] = db
    # Get global supply data via research module for international locations
    global_research = research.get_global_supply_data(
        data.get("locations", ["United States"]),
        data.get("industry", "general_entry_level"),
    )

    # Industry label mapping (single source of truth in shared_utils.py)
    industry_label_map = _SHARED_INDUSTRY_LABEL_MAP

    wb = Workbook()

    # Set workbook metadata for GEO/SEO discoverability
    wb.properties.title = f"Recruitment Media Plan - {data.get('client_name', 'Client')}"
    wb.properties.creator = "Nova AI by Joveo"
    wb.properties.subject = f"AI-generated recruitment media plan for {data.get('client_name', 'Client')}"
    wb.properties.keywords = f"recruitment media plan, {data.get('industry', '').replace('_', ' ').title()}, job advertising"
    wb.properties.description = f"Generated by Nova AI Media Plan Generator. Data from 25 APIs, 91+ platforms."
    wb.properties.category = "Recruitment Advertising"
    wb.properties.lastModifiedBy = "Nova AI by Joveo"

    # Styles
    header_font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    header_fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
    subheader_font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
    subheader_fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
    section_font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
    section_fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
    body_font = Font(name="Calibri", size=10)
    wrap_alignment = Alignment(wrap_text=True, vertical="top")
    center_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin_border = Border(
        left=Side(style="thin", color="CCCCCC"),
        right=Side(style="thin", color="CCCCCC"),
        top=Side(style="thin", color="CCCCCC"),
        bottom=Side(style="thin", color="CCCCCC"),
    )
    accent_fill = PatternFill(start_color="E8F0FE", end_color="E8F0FE", fill_type="solid")

    # ── Joveo-inspired color palette ──
    NAVY = "1B2A4A"
    BLUE = "0A66C9"
    MEDIUM_BLUE = "004082"
    LIGHT_BLUE = "D1E8FF"
    ACCENT = "0A66C9"          # Joveo blue (accent)
    ACCENT_LIGHT = "5BA3E6"    # Light Joveo blue
    ACCENT_PALE = "E8F0FE"     # Pale Joveo blue
    OFF_WHITE = "F2F2F0"
    WARM_GRAY = "EBE6E0"
    GREEN_GOOD = "2E7D32"
    AMBER_WARN = "F57C00"

    # LinkedIn-style fills
    accent_fill = PatternFill(start_color=ACCENT, end_color=ACCENT, fill_type="solid")
    accent_light_fill = PatternFill(start_color=ACCENT_LIGHT, end_color=ACCENT_LIGHT, fill_type="solid")
    accent_pale_fill = PatternFill(start_color=ACCENT_PALE, end_color=ACCENT_PALE, fill_type="solid")
    off_white_fill = PatternFill(start_color=OFF_WHITE, end_color=OFF_WHITE, fill_type="solid")
    warm_gray_fill = PatternFill(start_color=WARM_GRAY, end_color=WARM_GRAY, fill_type="solid")
    light_blue_fill = PatternFill(start_color=LIGHT_BLUE, end_color=LIGHT_BLUE, fill_type="solid")
    green_fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
    amber_fill = PatternFill(start_color="FFF3E0", end_color="FFF3E0", fill_type="solid")
    accent_border = Border(
        left=Side(style="medium", color=ACCENT),
        right=Side(style="thin", color=WARM_GRAY),
        top=Side(style="thin", color=WARM_GRAY),
        bottom=Side(style="thin", color=WARM_GRAY),
    )
    accent_bottom_border = Border(bottom=Side(style="medium", color=ACCENT))

    def style_section_header(ws, row, col_start, col_end, title):
        """Style a section header with navy text and blue accent border."""
        ws.merge_cells(start_row=row, start_column=col_start, end_row=row, end_column=col_end)
        cell = ws.cell(row=row, column=col_start, value=title)
        cell.font = Font(name="Calibri", bold=True, size=14, color=NAVY)
        cell.border = Border(
            bottom=Side(style="medium", color=ACCENT),
            left=Side(style="thick", color=ACCENT),
        )
        return cell

    def style_body_cell(ws, row, col, val=""):
        cell = ws.cell(row=row, column=col, value=val)
        cell.font = body_font
        cell.alignment = wrap_alignment
        cell.border = thin_border
        return cell

    locations = data.get("locations", ["United States"])

    # Determine display currency based on locations
    # Single country = use that country's currency; multiple countries = default to USD
    CURRENCY_SYMBOLS = {
        "USD": "$", "GBP": "£", "EUR": "€", "JPY": "¥", "INR": "₹",
        "AUD": "A$", "CAD": "C$", "SGD": "S$", "CHF": "CHF ", "AED": "AED ",
        "ZAR": "R", "BRL": "R$", "MXN": "MX$", "PLN": "PLN ", "SEK": "SEK ",
        "DKK": "DKK ", "NOK": "NOK ", "NZD": "NZ$", "HKD": "HK$", "KRW": "₩",
        "ILS": "₪", "CNY": "¥", "PHP": "₱", "MYR": "RM ", "THB": "฿",
    }
    display_currency = "$"  # Default USD
    display_currency_code = "USD"
    if len(locations) == 1:
        loc_info = research.get_location_info(locations[0])
        loc_currency = loc_info.get("currency", "USD")
        display_currency = CURRENCY_SYMBOLS.get(loc_currency, "$")
        display_currency_code = loc_currency

    industry = data.get("industry", "general_entry_level")
    roles_raw = data.get("target_roles") or data.get("roles", [])
    # Normalize: roles can be list-of-strings or list-of-dicts with "title" key
    roles = []
    for r in roles_raw:
        if isinstance(r, dict):
            roles.append(r.get("title") or r.get("role") or str(r))
        else:
            roles.append(str(r))
    # Write normalized string list back so all downstream data.get("roles") gets strings
    data["roles"] = roles
    data["target_roles"] = roles

    # ── Role-Tier Classification ──
    role_tiers = {}
    for role in roles:
        role_tiers[role] = classify_role_tier(role)
    # Compute aggregate tier profile (weighted by role count per tier)
    tier_groups = {}
    for role, tier_info in role_tiers.items():
        tier_name = tier_info["tier"]
        if tier_name not in tier_groups:
            tier_groups[tier_name] = {"count": 0, "roles": [], "tier_info": tier_info}
        tier_groups[tier_name]["count"] += 1
        tier_groups[tier_name]["roles"].append(role)
    # Store in data for downstream use
    data["_role_tiers"] = role_tiers
    data["_tier_groups"] = tier_groups


    # Channel category preferences (support list-of-strings, list-of-dicts, and dict formats)
    ch_cats_raw = data.get("channel_categories", {})
    if isinstance(ch_cats_raw, list):
        ch_cats = {}
        for item in ch_cats_raw:
            if isinstance(item, dict):
                name = item.get("name", "")
                enabled = item.get("enabled", True)
                ch_cats[name] = enabled
            else:
                ch_cats[str(item)] = True
    else:
        ch_cats = ch_cats_raw
    include_regional = ch_cats.get("regional_boards", True)
    include_global = ch_cats.get("global_boards", True)
    include_niche = ch_cats.get("niche_boards", True)
    include_social = ch_cats.get("social_media", True)
    include_programmatic = ch_cats.get("programmatic_dsp", True)
    include_employer_brand = ch_cats.get("employer_branding", False)
    include_apac = ch_cats.get("apac_regional", False)
    include_emea = ch_cats.get("emea_regional", False)

    niche_key = db.get("industries", {}).get(industry, {}).get("niche_channel_key", "")

    # ── Sheet 1: Overview ──
    ws_overview = wb.active
    ws_overview.title = "Overview"
    ws_overview.sheet_properties.tabColor = "1B2A4A"
    ws_overview.column_dimensions["A"].width = 5
    ws_overview.column_dimensions["B"].width = 40
    ws_overview.column_dimensions["C"].width = 60

    ws_overview.merge_cells("B2:C2")
    title_cell = ws_overview["B2"]
    _overview_client = data.get("client_name", "").strip()
    title_cell.value = f"Media Plan — {_overview_client}" if _overview_client else "AI Media Planner — Overview"
    title_cell.font = Font(name="Calibri", bold=True, size=18, color="1B2A4A")

    # Try to fetch and insert client logo (prefer website URL for accuracy)
    client_name = data.get("client_name", "")
    client_website = data.get("client_website", "")
    logo_data, logo_src = fetch_client_logo(client_name, client_website) if (client_name or client_website) else (None, None)
    if logo_data:
        try:
            logo_stream = io.BytesIO(logo_data)
            logo_img = XlImage(logo_stream)
            logo_img.width = 80
            logo_img.height = 80
            ws_overview.add_image(logo_img, "D2")
            ws_overview.column_dimensions["D"].width = 15
        except Exception:
            pass  # Silently skip if image insertion fails

    job_cat_labels = data.get("job_category_labels", [])
    client_competitors = data.get("competitors", [])
    # CRITICAL: Filter out self from client-specified competitors (prevents self-as-competitor bug)
    _company_name_for_filter = (data.get("client_name", "") or "").lower().strip()
    if _company_name_for_filter and client_competitors:
        client_competitors = [c for c in client_competitors if _company_name_for_filter not in c.lower()]

    overview_items = [
        ("Client Name", data.get("client_name", "")),
        ("Client Website", data.get("client_website", "") or "Not specified"),
        ("Client's Use Case", data.get("use_case", "") or "Not specified"),
        ("Industry", data.get("industry_label", industry_label_map.get(industry, industry))),
        ("NAICS Sector", f"{data.get('bls_sector', 'General')} (NAICS {data.get('naics_code', '00')})"),
        ("Talent Profile", data.get("talent_profile", "General / Mixed Workforce")),
        ("Job Categories", ", ".join(job_cat_labels) if job_cat_labels else "Not specified"),
        ("Target Locations", ", ".join(locations)),
        ("Target Roles", ", ".join(roles)),
        ("Target Demographic", data.get("target_demographic", "")),
        ("Budget Range", data.get("budget_range", "") or data.get("budget", "Not specified")),
        ("Campaign Duration", data.get("campaign_duration", "") or "Not specified"),
        ("Hire Volume", data.get("hire_volume", "") or "Not specified"),
        ("Key Competitors", ", ".join(client_competitors) if client_competitors else "Not specified"),
    ]

    row = 4
    for label, value in overview_items:
        ws_overview.cell(row=row, column=2, value=label).font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
        ws_overview.cell(row=row, column=2).border = thin_border
        ws_overview.cell(row=row, column=3, value=value).font = body_font
        ws_overview.cell(row=row, column=3).alignment = wrap_alignment
        ws_overview.cell(row=row, column=3).border = thin_border
        row += 1

    # Joveo supply network summary
    row += 1
    ws_overview.cell(row=row, column=2, value="Joveo Supply Network").font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
    row += 1
    total_pubs = joveo_pubs.get("total_active_publishers", 1238)
    _total_pubs_str = f"{total_pubs:,}" if isinstance(total_pubs, (int, float)) else str(total_pubs)
    ws_overview.cell(row=row, column=2, value=f"  Active Supply Partners: {_total_pubs_str}+").font = body_font
    row += 1
    ws_overview.cell(row=row, column=2, value=f"  Countries Covered: 200+").font = body_font
    row += 1
    ws_overview.cell(row=row, column=2, value=f"  Regions: Americas, Europe, APAC, LATAM, MEA, Africa").font = body_font

    # Geopolitical Risk Assessment (if risk_level is not "low")
    _synth_data = data.get("_synthesized", {}) if isinstance(data, dict) else {}
    _geo_ctx = _synth_data.get("geopolitical_context", {}) if isinstance(_synth_data, dict) else {}
    if isinstance(_geo_ctx, dict) and _geo_ctx.get("risk_level", "low") != "low":
        row += 2
        _risk_color = "C0392B" if _geo_ctx.get("risk_level") in ("high", "critical") else "D47A1A"
        ws_overview.cell(row=row, column=2, value="Geopolitical Risk Assessment").font = Font(name="Calibri", bold=True, size=12, color=_risk_color)
        row += 1
        _risk_level = _geo_ctx.get("risk_level", "moderate").upper()
        _risk_score = _geo_ctx.get("overall_risk_score", 0)
        ws_overview.cell(row=row, column=2, value=f"  Overall Risk Level:").font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
        ws_overview.cell(row=row, column=3, value=f"{_risk_level} ({_risk_score:.1f}/10)").font = Font(name="Calibri", bold=True, size=11, color=_risk_color)
        row += 1
        _geo_summary = _geo_ctx.get("summary", "")
        if _geo_summary:
            ws_overview.cell(row=row, column=2, value=f"  Summary:").font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
            ws_overview.cell(row=row, column=3, value=str(_geo_summary)[:200]).font = body_font
            ws_overview.cell(row=row, column=3).alignment = wrap_alignment
            row += 1
        _geo_recs = _geo_ctx.get("recommendations", [])
        if _geo_recs:
            ws_overview.cell(row=row, column=2, value=f"  Key Recommendations:").font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
            ws_overview.cell(row=row, column=3, value="; ".join(str(r)[:80] for r in _geo_recs[:3])).font = body_font
            ws_overview.cell(row=row, column=3).alignment = wrap_alignment
            row += 1

    row += 2
    ws_overview.cell(row=row, column=2, value="Plan Sections").font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
    row += 1
    # Determine if any location is international (not US-only)
    has_international = False
    for loc in locations:
        loc_info = research.get_location_info(loc)
        if loc_info.get("is_international"):
            has_international = True
            break

    sections = ["Market Trends", "Labour Market Intelligence", "Competitor Analysis", "Channel Strategy", "Traditional Channels", "Non-Traditional Channels"]
    if data.get("job_categories"):
        sections.append("Job Category Insights")
    if has_international or data.get("include_global_supply"):
        sections.append("Global Supply Strategy")
    if data.get("include_dei"):
        sections.append("DEI & Diversity Channels")
    if data.get("include_innovative"):
        sections.append("Innovative Channels 2025+")
    if data.get("include_budget_guide"):
        sections.append("Budget & Pricing Guide")
    if data.get("include_educational"):
        sections.append("Educational Partners")
    if data.get("include_events"):
        sections.append("Events & Career Fairs")
    if data.get("include_media_platforms"):
        sections.append("Media/Print Platforms")
    if data.get("include_radio_podcasts"):
        sections.append("Radio/Podcasts")

    for section in sections:
        ws_overview.cell(row=row, column=2, value=f"  {section}").font = body_font
        row += 1

    ws_overview.cell(row=row + 1, column=2, value=f"Generated on: {datetime.datetime.now().strftime('%B %d, %Y')}").font = Font(name="Calibri", italic=True, size=9, color="888888")
    ws_overview.cell(row=row + 2, column=2, value="Powered by Joveo — Programmatic Job Advertising at Scale").font = Font(name="Calibri", italic=True, size=9, color="2E75B6")

    # ── Executive Summary (inserted as FIRST sheet) ──
    ws_exec = wb.create_sheet("Executive Summary")
    ws_exec.sheet_properties.tabColor = "1B2A4A"
    ws_exec.column_dimensions["A"].width = 3
    ws_exec.column_dimensions["B"].width = 22
    ws_exec.column_dimensions["C"].width = 20
    ws_exec.column_dimensions["D"].width = 20
    ws_exec.column_dimensions["E"].width = 20
    ws_exec.column_dimensions["F"].width = 20
    ws_exec.column_dimensions["G"].width = 55

    navy_fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
    metric_fill = PatternFill(start_color=OFF_WHITE, end_color=OFF_WHITE, fill_type="solid")
    blue_accent_border = Border(
        left=Side(style="medium", color=ACCENT),
        right=Side(style="thin", color=WARM_GRAY),
        top=Side(style="thin", color=WARM_GRAY),
        bottom=Side(style="thin", color=WARM_GRAY),
    )

    client_name_val = data.get("client_name") or "CLIENT"
    industry_label_val = data.get("industry_label", industry_label_map.get(industry, industry))

    # Large merged header - Navy background
    ws_exec.merge_cells("B2:G2")
    title_cell_exec = ws_exec["B2"]
    title_cell_exec.value = f"AI MEDIA PLANNER \u2014 {client_name_val.upper()}"
    title_cell_exec.font = Font(name="Calibri", bold=True, size=22, color="FFFFFF")
    title_cell_exec.fill = navy_fill
    title_cell_exec.alignment = Alignment(horizontal="center", vertical="center")
    for c in range(3, 8):
        ws_exec.cell(row=2, column=c).fill = navy_fill
    ws_exec.row_dimensions[2].height = 50

    # Blue accent subtitle bar
    ws_exec.merge_cells("B3:G3")
    ws_exec["B3"].value = f"{industry_label_val}  |  Generated {datetime.datetime.now().strftime('%B %d, %Y')}"
    ws_exec["B3"].font = Font(name="Calibri", bold=True, size=11, color=NAVY)
    ws_exec["B3"].fill = accent_fill
    ws_exec["B3"].alignment = Alignment(horizontal="center", vertical="center")
    for c in range(3, 8):
        ws_exec.cell(row=3, column=c).fill = accent_fill
    ws_exec.row_dimensions[3].height = 30

    # Insert logo on executive summary if available
    if logo_data:
        try:
            logo_stream2 = io.BytesIO(logo_data)
            logo_img2 = XlImage(logo_stream2)
            logo_img2.width = 60
            logo_img2.height = 60
            ws_exec.add_image(logo_img2, "H2")
            ws_exec.column_dimensions["H"].width = 12
        except Exception:
            pass

    # Campaign Snapshot section with blue accent
    exec_row = 5
    style_section_header(ws_exec, exec_row, 2, 7, "Campaign Snapshot")
    exec_row += 1

    # Hero stat - total budget or channel count as large number
    budget_range_val = data.get("budget_range", "") or data.get("budget", "Not specified")
    campaign_duration_val = data.get("campaign_duration", "") or "Not specified"
    loc_count = len(locations)
    role_count = len(roles)
    hire_volume_val = data.get("hire_volume", "") or "Not specified"

    # Large hero stat row
    ws_exec.merge_cells(f"B{exec_row}:C{exec_row}")
    hero_cell = ws_exec.cell(row=exec_row, column=2, value=str(budget_range_val))
    hero_cell.font = Font(name="Calibri", bold=True, size=24, color=ACCENT)
    hero_cell.fill = off_white_fill
    hero_cell.alignment = Alignment(horizontal="center", vertical="center")
    ws_exec.cell(row=exec_row, column=3).fill = off_white_fill
    ws_exec.merge_cells(f"D{exec_row}:E{exec_row}")
    hero_label = ws_exec.cell(row=exec_row, column=4, value="BUDGET RANGE")
    hero_label.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
    hero_label.fill = off_white_fill
    hero_label.alignment = Alignment(horizontal="left", vertical="center")
    ws_exec.cell(row=exec_row, column=5).fill = off_white_fill
    ws_exec.row_dimensions[exec_row].height = 45
    exec_row += 1

    # 2x3 metric cards with off-white backgrounds and blue value highlights
    metric_grid = [
        ("Campaign Duration", campaign_duration_val, "Hire Volume", str(hire_volume_val)),
        ("Target Locations", f"{loc_count} location(s)", "Target Roles", f"{role_count} role(s)" if role_count > 0 else "Roles not specified"),
        ("Industry", industry_label_val, "Talent Profile", data.get("talent_profile", "General / Mixed Workforce")),
    ]
    metric_card_border = Border(
        left=Side(style="thin", color=WARM_GRAY),
        right=Side(style="thin", color=WARM_GRAY),
        top=Side(style="thin", color=WARM_GRAY),
        bottom=Side(style="thin", color=WARM_GRAY),
    )
    for label1, val1, label2, val2 in metric_grid:
        # Left metric card
        cell_l = ws_exec.cell(row=exec_row, column=2, value=label1)
        cell_l.font = Font(name="Calibri", bold=True, size=9, color="666666")
        cell_l.fill = off_white_fill
        cell_l.border = metric_card_border
        cell_v = ws_exec.cell(row=exec_row, column=3, value=val1)
        cell_v.font = Font(name="Calibri", bold=True, size=12, color=NAVY)
        cell_v.fill = off_white_fill
        cell_v.border = metric_card_border
        # Right metric card
        if label2:
            cell_r = ws_exec.cell(row=exec_row, column=4, value=label2)
            cell_r.font = Font(name="Calibri", bold=True, size=9, color="666666")
            cell_r.fill = off_white_fill
            cell_r.border = metric_card_border
            cell_rv = ws_exec.cell(row=exec_row, column=5, value=val2)
            cell_rv.font = Font(name="Calibri", bold=True, size=12, color=NAVY)
            cell_rv.fill = off_white_fill
            cell_rv.border = metric_card_border
        exec_row += 1

    # Plan at a Glance
    exec_row += 1
    style_section_header(ws_exec, exec_row, 2, 7, "Plan at a Glance")
    exec_row += 1
    all_sheet_names = ["Overview", "Market Trends", "Labour Market Intelligence", "Channel Strategy", "Traditional Channels", "Non-Traditional Channels"]
    if data.get("job_categories"):
        all_sheet_names.append("Job Category Insights")
    if has_international or data.get("include_global_supply"):
        all_sheet_names.append("Global Supply Strategy")
    if data.get("include_dei"):
        all_sheet_names.append("DEI & Diversity Channels")
    if data.get("include_innovative"):
        all_sheet_names.append("Innovative Channels 2025+")
    if data.get("include_budget_guide"):
        all_sheet_names.append("Budget & Pricing Guide")
    if data.get("include_educational"):
        all_sheet_names.append("Educational Partners")
    if data.get("include_events"):
        all_sheet_names.append("Events & Career Fairs")
    if data.get("include_media_platforms"):
        all_sheet_names.append("Media & Print Platforms")
    if data.get("include_radio_podcasts"):
        all_sheet_names.append("Radio & Podcasts")
    all_sheet_names.append("Campaign Timeline")
    for sn in all_sheet_names:
        ws_exec.cell(row=exec_row, column=2, value=f"  \u2022  {sn}").font = Font(name="Calibri", size=10, color="333333")
        exec_row += 1

    # Channel Mix Summary
    exec_row += 1
    style_section_header(ws_exec, exec_row, 2, 7, "Channel Mix Summary")
    exec_row += 1
    regional_count = len(data.get("selected_regional", db["traditional_channels"]["regional_local"][:25]))
    niche_count = len(data.get("selected_niche", db["traditional_channels"]["niche_by_industry"].get(niche_key, [])[:25]))
    global_count = len(data.get("selected_global", db["traditional_channels"]["global_reach"][:25]))
    ws_exec.cell(row=exec_row, column=2, value=f"{regional_count} Regional  +  {niche_count} Niche  +  {global_count} Global channels").font = Font(name="Calibri", size=11, color="1B2A4A")
    exec_row += 2


    # ── Company Intelligence ──
    company_intel = research.get_company_intelligence(client_name)
    if company_intel.get("matched"):
        style_section_header(ws_exec, exec_row, 2, 7, "Company Intelligence")
        exec_row += 1
        intel_items = [
            ("Company Size", company_intel.get("size", "N/A")),
            ("Glassdoor Rating", company_intel.get("glassdoor", "N/A")),
            ("Brand Strength", company_intel.get("brand_strength", "N/A")),
            ("Hiring Volume", company_intel.get("hiring_volume", "N/A")),
            ("Benefits Highlight", company_intel.get("benefits_highlight", "N/A")),
            ("Attrition Rate", company_intel.get("attrition", "N/A")),
        ]
        for label, value in intel_items:
            ws_exec.cell(row=exec_row, column=2, value=f"  {label}:").font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
            ws_exec.cell(row=exec_row, column=4, value=value).font = Font(name="Calibri", size=10, color="333333")
            ws_exec.merge_cells(f"B{exec_row}:C{exec_row}")
            ws_exec.merge_cells(f"D{exec_row}:G{exec_row}")
            exec_row += 1
        exec_row += 1

    # Labour Market Summary in Executive Summary
    lm_exec = research.get_labour_market_intelligence(industry, locations)
    lm_ind = lm_exec.get("industry_metrics", {})
    style_section_header(ws_exec, exec_row, 2, 7, "Labour Market Snapshot")
    exec_row += 1
    lm_summary_items = [
        f"Sector: {lm_ind.get('sector_name', '')}",
        f"Employment Growth: {lm_ind.get('projected_growth_2024_2034', '')}",
        f"Talent Shortage: {lm_ind.get('talent_shortage_severity', '')}",
        f"JOLTS Openings Rate: {lm_ind.get('job_openings_rate_jolts', '')}",
        f"Avg Time to Fill: {lm_ind.get('vacancy_fill_time_avg', '')}",
        f"Wage Growth: {lm_ind.get('wage_growth_yoy', '')}",
    ]
    for item in lm_summary_items:
        ws_exec.cell(row=exec_row, column=2, value=f"  {item}").font = Font(name="Calibri", size=10, color="333333")
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        exec_row += 1

    # Enhance with real API data if available
    enriched = data.get("_enriched", {})
    industry_emp = enriched.get("industry_employment")
    if industry_emp:
        exec_row += 1
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        _total_emp = industry_emp.get('total_employed', 'N/A')
        _total_emp_str = f"{_total_emp:,}" if isinstance(_total_emp, (int, float)) else str(_total_emp)
        _avg_wage = industry_emp.get('avg_annual_wage') or industry_emp.get('avg_wage', 0)
        _avg_wage_str = f"${_avg_wage:,.0f}" if isinstance(_avg_wage, (int, float)) else str(_avg_wage)
        _estabs = industry_emp.get('establishments', '')
        _estabs_str = f", {_estabs:,} establishments" if isinstance(_estabs, (int, float)) and _estabs else ""
        ws_exec.cell(row=exec_row, column=2, value=f"Live Data: {industry_emp.get('sector_name', 'Industry')} — {_total_emp_str} employed, Avg annual wage {_avg_wage_str}{_estabs_str} ({industry_emp.get('source', 'BLS QCEW')})").font = Font(name="Calibri", size=10, bold=True, color="2E7D32")
        exec_row += 1

    # SEC EDGAR data — public company status
    sec_data = enriched.get("sec_data")
    if sec_data and sec_data.get("is_public"):
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value=f"Public Company: {sec_data.get('company_name', '')} (Ticker: {sec_data.get('ticker', 'N/A')}) — Source: SEC EDGAR").font = Font(name="Calibri", size=10, bold=True, color="1565C0")
        exec_row += 1

    exec_row += 1


    # ── Seasonal Hiring Calendar ──
    seasonal_advice = research.get_seasonal_hiring_advice(industry)
    style_section_header(ws_exec, exec_row, 2, 7, "Seasonal Hiring Calendar")
    exec_row += 1
    peak_months_str = ", ".join(seasonal_advice.get("peak_months", []))
    ramp_start = seasonal_advice.get("ramp_start", "")
    seasonal_note = seasonal_advice.get("note", "")
    ws_exec.cell(row=exec_row, column=2, value=f"  Peak Hiring Months: {peak_months_str}").font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    exec_row += 1
    ws_exec.cell(row=exec_row, column=2, value=f"  Campaign Ramp-Up Start: {ramp_start}").font = Font(name="Calibri", bold=True, size=10, color="2E75B6")
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    exec_row += 1
    ws_exec.cell(row=exec_row, column=2, value=f"  {seasonal_note}").font = Font(name="Calibri", italic=True, size=10, color="596780")
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    exec_row += 1
    exec_row += 1

    # Competitive Landscape in Executive Summary
    if client_competitors:
        style_section_header(ws_exec, exec_row, 2, 7, "Competitive Landscape")
        exec_row += 1
        ws_exec.cell(row=exec_row, column=2, value=f"Key Competitors: {', '.join(client_competitors)}").font = Font(name="Calibri", size=11, color="333333")
        exec_row += 1
        ws_exec.cell(row=exec_row, column=2, value="Detailed per-competitor intelligence (hiring channels, employer brand, strategies, and recommendations) included in the Market Trends sheet.").font = Font(name="Calibri", italic=True, size=10, color="596780")
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        exec_row += 2

    # ── Budget Summary (Phase 5 enhancements) ──
    _exec_budget = data.get("_budget_allocation", {})

    # Budget Allocation Summary — only show if we have meaningful data (clicks > 0 or hires > 0)
    _exec_total_proj = _exec_budget.get("total_projected", {}) if isinstance(_exec_budget, dict) else {}
    _exec_proj_hires = _exec_total_proj.get("hires", 0) if isinstance(_exec_total_proj, dict) else 0
    _exec_proj_cph = _exec_total_proj.get("cost_per_hire", 0) if isinstance(_exec_total_proj, dict) else 0
    _exec_proj_clicks = _exec_total_proj.get("clicks", 0) if isinstance(_exec_total_proj, dict) else 0
    _exec_proj_apps = _exec_total_proj.get("applications", 0) if isinstance(_exec_total_proj, dict) else 0
    _has_budget_data = any(isinstance(v, (int, float)) and v > 0 for v in [_exec_proj_hires, _exec_proj_clicks, _exec_proj_apps])
    if _has_budget_data:
        style_section_header(ws_exec, exec_row, 2, 7, "Budget Allocation Summary")
        exec_row += 1

        _ba_summary = [
            ("Projected Hires", f"{_exec_proj_hires:,.0f}" if isinstance(_exec_proj_hires, (int, float)) else str(_exec_proj_hires),
             "Projected Cost/Hire", f"${_exec_proj_cph:,.0f}" if isinstance(_exec_proj_cph, (int, float)) and _exec_proj_cph > 0 else "N/A"),
            ("Projected Clicks", f"{_exec_proj_clicks:,.0f}" if isinstance(_exec_proj_clicks, (int, float)) else str(_exec_proj_clicks),
             "Projected Applications", f"{_exec_proj_apps:,.0f}" if isinstance(_exec_proj_apps, (int, float)) else str(_exec_proj_apps)),
        ]
        for label1, val1, label2, val2 in _ba_summary:
            ws_exec.cell(row=exec_row, column=2, value=f"  {label1}:").font = Font(name="Calibri", bold=True, size=10, color=NAVY)
            ws_exec.cell(row=exec_row, column=3, value=val1).font = Font(name="Calibri", size=10, color="333333")
            ws_exec.cell(row=exec_row, column=4, value=f"  {label2}:").font = Font(name="Calibri", bold=True, size=10, color=NAVY)
            ws_exec.merge_cells(f"E{exec_row}:G{exec_row}")
            ws_exec.cell(row=exec_row, column=5, value=val2).font = Font(name="Calibri", size=10, color="333333")
            exec_row += 1

        # Top 3 channel recommendations
        _exec_ch_allocs = _exec_budget.get("channel_allocations", {})
        if isinstance(_exec_ch_allocs, dict) and _exec_ch_allocs:
            exec_row += 1
            ws_exec.cell(row=exec_row, column=2, value="  Top Channel Recommendations:").font = Font(name="Calibri", bold=True, size=10, color=NAVY)
            ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
            exec_row += 1
            # Sort channels by dollar amount
            _sorted_chs = sorted(
                [(ch_name, ch_data) for ch_name, ch_data in _exec_ch_allocs.items() if isinstance(ch_data, dict)],
                key=lambda x: x[1].get("dollar_amount", x[1].get("amount", 0)),
                reverse=True,
            )
            for _ch_name, _ch_data in _sorted_chs[:3]:
                _ch_amt = _ch_data.get("dollar_amount", _ch_data.get("amount", 0))
                _ch_pct = _ch_data.get("percentage", 0)
                _ch_hires = _ch_data.get("projected_hires", 0)
                _ch_display = _ch_name.replace("_", " ").title()
                ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
                _ch_text = f"    {_ch_display}: ${_ch_amt:,.0f} ({_ch_pct:.0f}%)" if isinstance(_ch_amt, (int, float)) and isinstance(_ch_pct, (int, float)) else f"    {_ch_display}"
                if isinstance(_ch_hires, (int, float)) and _ch_hires > 0:
                    _ch_text += f" — ~{_ch_hires:.1f} projected hires"
                ws_exec.cell(row=exec_row, column=2, value=_ch_text).font = Font(name="Calibri", size=10, color="333333")
                exec_row += 1

        exec_row += 1

    # Key Recommendations
    style_section_header(ws_exec, exec_row, 2, 7, "Key Recommendations")
    exec_row += 1

    recommendations = []
    if has_international:
        recommendations.append("International recruitment strategy recommended with local job boards")
    budget_str = str(data.get("budget_range", "") or data.get("budget", ""))
    # Check if budget exceeds $500K
    if any(x in budget_str.lower() for x in ["500k", "500,000", "1m", "1,000,000", "million"]):
        recommendations.append("Multi-channel programmatic approach with performance tracking")
    # Industry-specific recommendation
    industry_rec_map = {
        "healthcare_medical": "Healthcare: Focus on medical-specific job boards and professional associations",
        "tech_engineering": "Technology: Leverage developer communities and tech-focused platforms",
        "blue_collar_trades": "Trades: Prioritize local community boards and vocational training partnerships",
        "finance_banking": "Finance: Target professional networks and financial industry publications",
        "maritime_marine": "Maritime: Use specialized maritime job boards and port-area media",
        "legal_services": "Legal: Focus on bar association boards and legal publications",
        "retail_consumer": "Retail: Leverage high-volume job boards and social media advertising",
        "aerospace_defense": "Aerospace & Defense: Target cleared-talent networks and defense industry boards",
        "pharma_biotech": "Pharma: Use scientific publications and specialized biotech job platforms",
        "mental_health": "Mental Health: Focus on counseling association boards and healthcare networks",
        "energy_utilities": "Energy & Utilities: Target specialized energy boards and professional associations for certified technicians and engineers",
        "insurance": "Insurance: Leverage financial services networks and actuarial professional communities",
        "telecommunications": "Telecommunications: Target STEM talent pools and technical certification platforms",
        "automotive": "Automotive: Focus on manufacturing job boards, trade schools, and EV industry networks",
        "food_beverage": "Food & Beverage: Leverage hospitality networks, culinary schools, and seasonal hiring platforms",
        "logistics_supply_chain": "Logistics: Target warehouse and transportation job boards with high-volume programmatic campaigns",
        "hospitality_travel": "Hospitality: Focus on seasonal hiring platforms, hospitality-specific boards, and local community outreach",
        "media_entertainment": "Media & Entertainment: Target creative talent platforms, portfolio sites, and industry-specific communities",
        "construction_real_estate": "Construction: Focus on trade association boards, apprenticeship programs, and skilled labor networks",
        "education": "Education: Target academic job boards, teacher certification sites, and higher education networks",
    }
    if industry in industry_rec_map:
        recommendations.append(industry_rec_map[industry])
    if data.get("include_dei"):
        recommendations.append("DEI-focused channels included to ensure inclusive hiring")
    # Job category specific recommendations
    jc_keys = data.get("job_categories", [])
    jc_db = db.get("job_categories", {})
    if jc_keys:
        for jck in jc_keys[:2]:
            jc_data = jc_db.get(jck, {})
            if jc_data:
                bp = jc_data.get("best_practices", [])
                if bp:
                    recommendations.append(f"{jc_data.get('label', jck)}: {bp[0]}")
    # Role-specific recommendations (enhanced with tier classification)
    if roles:
        role_str = ", ".join(roles[:5])
        recommendations.append(f"Role-targeted strategy: Focus channels and messaging on {role_str} talent pools for maximum relevance")
        # Tier-based recommendations from classification engine
        _tier_groups = data.get("_tier_groups", {})
        for _tname, _tg in _tier_groups.items():
            _ti = _tg["tier_info"]
            _tier_channels = ", ".join(_ti["channels"][:3])
            _tier_roles = ", ".join(_tg["roles"][:3])
            recommendations.append(f"{_tname} ({_tier_roles}): Recommended channels — {_tier_channels}. Est. time-to-fill: {_ti['time_to_fill_days']} days. CPA multiplier: {_ti['cpa_multiplier']}x")
        # Additional pattern-based recommendations (complementary to tier system)
        role_lower = " ".join(r.lower() for r in roles)
        if any(x in role_lower for x in ["nurse", "physician", "rn", "medical", "clinical", "therapist"]):
            recommendations.append("Clinical talent: Prioritize health-specific boards (Health eCareers, Vivian Health) and hospital system career pages")
        if any(x in role_lower for x in ["engineer", "developer", "software", "devops", "data scientist"]):
            recommendations.append("Tech talent: Invest in developer communities (GitHub, Stack Overflow Talent, Wellfound) and technical assessment platforms")
        if any(x in role_lower for x in ["driver", "warehouse", "mechanic", "technician", "operator"]):
            recommendations.append("High-volume roles: Deploy programmatic CPA-based campaigns for rapid scaling with performance optimization")
        if any(x in role_lower for x in ["executive", "director", "vp", "chief", "president", "c-suite"]):
            recommendations.append("Executive search: Supplement with retained/contingency search firms and premium professional networks")
        if any(x in role_lower for x in ["sales", "account", "business development"]):
            recommendations.append("Sales talent: Leverage LinkedIn Recruiter, sales communities, and performance-based sourcing channels")

    # Always have at least 3 recommendations
    if len(recommendations) < 3:
        recommendations.append("Programmatic job advertising recommended for optimized cost-per-applicant")
    if len(recommendations) < 3:
        recommendations.append("Employer branding investment will improve long-term talent pipeline")

    for rec in recommendations:
        cell_rec = ws_exec.cell(row=exec_row, column=2, value=f"  {rec}")
        cell_rec.font = Font(name="Calibri", size=10, color="333333")
        cell_rec.border = blue_accent_border
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        exec_row += 1

    # ── Recruitment Marketing Benchmarks (2025-2026 Data) ──
    _current_year = datetime.date.today().year
    _bench_year_label = f"{_current_year - 1}-{_current_year}"
    exec_row += 2
    style_section_header(ws_exec, exec_row, 2, 7, f"{_bench_year_label} Recruitment Marketing Benchmarks — CPA / CPC / CPH by Industry & Region")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value=f"Data sourced from {_bench_year_label} industry benchmark reports including Appcast Recruitment Marketing Benchmark (379M clicks, 30M applies analyzed), Recruitics Talent Market Index, and SHRM Benchmarking.").font = Font(name="Calibri", italic=True, size=9, color="596780")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value=f"Key {_bench_year_label} trends: CPCs rose 27% YoY | Overall CPA up 4.8% | Apply rates climbed 35% to 6.1% | Avg programmatic CPH reached $851 | Healthcare remains most expensive to hire").font = Font(name="Calibri", italic=True, size=9, color="2E75B6")

    enrichment_summary = data.get("_enriched", {}).get("enrichment_summary", {})
    if enrichment_summary.get("apis_succeeded"):
        exec_row += 1
        apis_used = ", ".join(enrichment_summary["apis_succeeded"])
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value=f"Live API data sourced from: {apis_used} (fetched {enrichment_summary.get('total_time_seconds', 0):.1f}s ago)").font = Font(name="Calibri", italic=True, size=9, color="0A66C9")

    exec_row += 2

    # CPA/CPC/CPH benchmark data by job category and region — updated with real 2025-2026 data
    cpa_cpc_benchmarks = {
        "healthcare_medical": {
            "label": "Healthcare & Medical",
            "apply_rate": "3.2% - 4.5%",
            "cph_range": "$9,000 - $12,000",
            "yoy_trend": "CPA up; lowest apply rates of any sector (Appcast 2025)",
            "benchmarks": {
                "North America": {"cpa": "$35 - $85", "cpc": "$0.90 - $3.50", "cph": "$9,000 - $12,000", "notes": "Hardest to fill; travel nursing $85+ CPA; clinical shortages persist; CPA exceeds all other sectors"},
                "Europe (UK/DE/FR)": {"cpa": "$28 - $65", "cpc": "$0.70 - $2.80", "cph": "$7,000 - $10,000", "notes": "NHS high volume lower CPA; specialist/surgeon roles premium; burnout-driven churn"},
                "APAC (IN/AU/SG)": {"cpa": "$10 - $40", "cpc": "$0.30 - $1.50", "cph": "$3,000 - $7,000", "notes": "India lowest; Australia/Singapore premium; immigration constraints limiting supply"},
                "LATAM": {"cpa": "$8 - $30", "cpc": "$0.20 - $1.00", "cph": "$2,000 - $5,000", "notes": "Growing market; lower competition; telemedicine roles emerging"},
            }
        },
        "blue_collar_trades": {
            "label": "Blue Collar / Skilled Trades",
            "apply_rate": "4.0% - 5.5%",
            "cph_range": "$3,500 - $5,600",
            "yoy_trend": "Light industrial CPA down 19.7% YoY but up 8.16% MoM (Recruitics Apr 2025)",
            "benchmarks": {
                "North America": {"cpa": "$12 - $35", "cpc": "$0.40 - $1.60", "cph": "$3,500 - $5,600", "notes": "High volume; skilled trades (electricians, plumbers) facing persistent shortages; CDL roles $52+ CPA"},
                "Europe (UK/DE/FR)": {"cpa": "$10 - $30", "cpc": "$0.35 - $1.30", "cph": "$3,000 - $5,000", "notes": "Trades apprenticeships lower CPA; Germany skilled worker visa driving demand"},
                "APAC (IN/AU/SG)": {"cpa": "$4 - $18", "cpc": "$0.10 - $0.70", "cph": "$1,200 - $3,000", "notes": "India high volume, very low CPA; Australia mining/construction premium"},
                "LATAM": {"cpa": "$3 - $15", "cpc": "$0.08 - $0.55", "cph": "$800 - $2,500", "notes": "Manufacturing hubs competitive; Mexico nearshoring driving demand"},
            }
        },
        "tech_engineering": {
            "label": "Technology & Engineering",
            "apply_rate": "6.41%",
            "cph_range": "$6,000 - $22,000",
            "yoy_trend": "Highest apply rate at 6.41% (Appcast 2025); white-collar recession driving surplus; IT CPA down 10.75% YoY (Recruitics)",
            "benchmarks": {
                "North America": {"cpa": "$25 - $75", "cpc": "$1.20 - $4.50", "cph": "$6,000 - $22,000", "notes": "CPAs falling due to white-collar recession; senior/AI roles still $100+; CPCs remain high despite reduced competition"},
                "Europe (UK/DE/FR)": {"cpa": "$20 - $60", "cpc": "$0.90 - $3.50", "cph": "$5,000 - $15,000", "notes": "Berlin/London hotspots; remote-first lowering CPA; AI/ML roles carry premium"},
                "APAC (IN/AU/SG)": {"cpa": "$8 - $35", "cpc": "$0.25 - $1.80", "cph": "$2,000 - $8,000", "notes": "India tech hubs competitive; Singapore premium; GCC market expanding"},
                "LATAM": {"cpa": "$6 - $28", "cpc": "$0.20 - $1.40", "cph": "$1,500 - $6,000", "notes": "Nearshore tech hubs booming; Brazil/Mexico/Colombia key markets"},
            }
        },
        "general_entry_level": {
            "label": "General / Entry-Level",
            "apply_rate": "5.5% - 6.1%",
            "cph_range": "$2,000 - $4,700",
            "yoy_trend": "Overall apply rate 6.1% (up 35% in 2024); Avg US CPH $4,700 (SHRM 2025); Programmatic CPH $851 (Appcast)",
            "benchmarks": {
                "North America": {"cpa": "$10 - $25", "cpc": "$0.35 - $1.30", "cph": "$2,000 - $4,700", "notes": "Highest volume; CPCs up 27% in 2024; Q4 seasonal spikes; every US state saw lower CPA than 2023"},
                "Europe (UK/DE/FR)": {"cpa": "$8 - $22", "cpc": "$0.28 - $1.10", "cph": "$1,800 - $4,000", "notes": "Retail/hospitality most common; UK high street volume roles"},
                "APAC (IN/AU/SG)": {"cpa": "$3 - $14", "cpc": "$0.08 - $0.50", "cph": "$500 - $2,500", "notes": "Massive volume in India; Sun Belt-equivalent regions deliver lower CPA"},
                "LATAM": {"cpa": "$2 - $10", "cpc": "$0.05 - $0.40", "cph": "$400 - $2,000", "notes": "Lowest CPAs globally; scaling opportunity; quality screening critical"},
            }
        },
        "finance_banking": {
            "label": "Finance & Banking",
            "apply_rate": "5.0% - 6.0%",
            "cph_range": "$5,000 - $12,000",
            "yoy_trend": "Finance & Ops CPA surged +33.3% MoM in Jul 2025 (Recruitics); compliance-heavy hiring drives costs",
            "benchmarks": {
                "North America": {"cpa": "$21 - $65", "cpc": "$0.90 - $3.50", "cph": "$5,000 - $12,000", "notes": "Compliance roles premium; fintech competitive; extensive background checks inflate CPH"},
                "Europe (UK/DE/FR)": {"cpa": "$18 - $55", "cpc": "$0.75 - $2.80", "cph": "$4,000 - $10,000", "notes": "London financial district highest CPA; regulatory hiring surging"},
                "APAC (IN/AU/SG)": {"cpa": "$8 - $32", "cpc": "$0.30 - $1.50", "cph": "$2,000 - $6,000", "notes": "Singapore/HK premium; India BPO/fintech ops lower CPA"},
                "LATAM": {"cpa": "$6 - $25", "cpc": "$0.18 - $0.90", "cph": "$1,500 - $4,500", "notes": "Banking sector growing in Brazil/Mexico; digital banking roles emerging"},
            }
        },
        "retail_consumer": {
            "label": "Retail & Consumer",
            "apply_rate": "4.5% - 5.8%",
            "cph_range": "$2,700 - $4,000",
            "yoy_trend": "Retail CPA up 55% YoY but dropped 55.7% MoM in Mar 2025 (Recruitics); 5,800+ store closures in 2025",
            "benchmarks": {
                "North America": {"cpa": "$8 - $21", "cpc": "$0.25 - $1.00", "cph": "$2,700 - $4,000", "notes": "High volume; seasonal Q4 peak; historically ~$21 avg CPA; 64,000 retail jobs shed in 2025"},
                "Europe (UK/DE/FR)": {"cpa": "$7 - $18", "cpc": "$0.20 - $0.85", "cph": "$2,200 - $3,500", "notes": "High street retail competitive; e-commerce fulfillment roles growing"},
                "APAC (IN/AU/SG)": {"cpa": "$3 - $10", "cpc": "$0.08 - $0.40", "cph": "$800 - $2,000", "notes": "E-commerce driving demand in India/SE Asia; quick commerce roles new category"},
                "LATAM": {"cpa": "$2 - $8", "cpc": "$0.05 - $0.35", "cph": "$600 - $1,800", "notes": "Retail expansion across region; large candidate pools keep CPA low"},
            }
        },
        "pharma_biotech": {
            "label": "Pharma & Biotech",
            "apply_rate": "3.8% - 5.2%",
            "cph_range": "$8,000 - $18,000",
            "yoy_trend": "Highly specialized; clinical and R&D roles among most expensive to fill in 2025",
            "benchmarks": {
                "North America": {"cpa": "$40 - $110", "cpc": "$1.50 - $5.00", "cph": "$8,000 - $18,000", "notes": "Highly specialized; clinical/regulatory roles most expensive; credentialing adds to CPH"},
                "Europe (UK/DE/FR)": {"cpa": "$32 - $85", "cpc": "$1.20 - $4.00", "cph": "$6,000 - $14,000", "notes": "Basel/Cambridge/Dublin clusters premium; EMA regulatory talent scarce"},
                "APAC (IN/AU/SG)": {"cpa": "$12 - $45", "cpc": "$0.40 - $2.00", "cph": "$3,000 - $8,000", "notes": "India pharma hub (Hyderabad/Mumbai); R&D roles growing; CDMO expansion"},
                "LATAM": {"cpa": "$8 - $35", "cpc": "$0.25 - $1.40", "cph": "$2,000 - $6,000", "notes": "Clinical trials driving demand in Brazil; biosimilar manufacturing growth"},
            }
        },
        "energy_utilities": {
            "label": "Energy & Utilities",
            "apply_rate": "4.2% - 5.5%",
            "cph_range": "$5,000 - $10,000",
            "yoy_trend": "Renewables/EV sector driving new talent demand; oil/gas roles still carry premium CPA",
            "benchmarks": {
                "North America": {"cpa": "$28 - $70", "cpc": "$0.90 - $3.00", "cph": "$5,000 - $10,000", "notes": "Oil/gas premium; renewables/solar growing fast; energy transition creating new role categories"},
                "Europe (UK/DE/FR)": {"cpa": "$22 - $58", "cpc": "$0.75 - $2.50", "cph": "$4,000 - $8,500", "notes": "Green energy hubs in Germany/Nordics; wind/solar technicians in demand"},
                "APAC (IN/AU/SG)": {"cpa": "$10 - $38", "cpc": "$0.30 - $1.50", "cph": "$2,500 - $6,000", "notes": "Mining and energy in Australia premium; India solar manufacturing scale-up"},
                "LATAM": {"cpa": "$7 - $28", "cpc": "$0.20 - $1.00", "cph": "$1,800 - $5,000", "notes": "Oil/gas hubs in Brazil/Mexico; lithium mining in Chile/Argentina"},
            }
        },
        "logistics_supply_chain": {
            "label": "Logistics & Supply Chain",
            "apply_rate": "4.0% - 5.2%",
            "cph_range": "$4,500 - $8,000",
            "yoy_trend": "CPA up 131% YoY (Recruitics Feb 2025); CDL/last-mile/warehouse automation roles most competitive",
            "benchmarks": {
                "North America": {"cpa": "$15 - $52", "cpc": "$0.40 - $1.80", "cph": "$4,500 - $8,000", "notes": "Transportation roles ~$52 CPA; CDL/last-mile most expensive; warehouse automation roles emerging"},
                "Europe (UK/DE/FR)": {"cpa": "$10 - $35", "cpc": "$0.30 - $1.30", "cph": "$3,500 - $6,500", "notes": "Distribution hubs Germany/Netherlands; Brexit-driven UK driver shortages"},
                "APAC (IN/AU/SG)": {"cpa": "$4 - $18", "cpc": "$0.10 - $0.60", "cph": "$1,200 - $3,500", "notes": "India massive volume; very low CPA; e-commerce logistics booming"},
                "LATAM": {"cpa": "$3 - $14", "cpc": "$0.08 - $0.45", "cph": "$800 - $2,800", "notes": "Manufacturing and nearshoring logistics expansion; Mexico/Colombia key hubs"},
            }
        },
        "automotive": {
            "label": "Automotive & Manufacturing",
            "apply_rate": "4.5% - 5.8%",
            "cph_range": "$5,600 - $9,000",
            "yoy_trend": "Manufacturing CPH $5,611 avg (industry benchmark); EV transition creating new skilled roles",
            "benchmarks": {
                "North America": {"cpa": "$18 - $50", "cpc": "$0.60 - $2.20", "cph": "$5,600 - $9,000", "notes": "EV sector most competitive; skilled trades premium; battery/EV plant buildouts driving demand"},
                "Europe (UK/DE/FR)": {"cpa": "$15 - $42", "cpc": "$0.50 - $1.80", "cph": "$4,500 - $7,500", "notes": "Germany auto hub; EV battery gigafactories scaling; automation technician roles growing"},
                "APAC (IN/AU/SG)": {"cpa": "$5 - $22", "cpc": "$0.15 - $0.85", "cph": "$1,500 - $4,000", "notes": "Japan/Korea OEMs; India manufacturing growth; EV supply chain expansion"},
                "LATAM": {"cpa": "$4 - $18", "cpc": "$0.12 - $0.65", "cph": "$1,000 - $3,500", "notes": "Mexico auto corridor competitive; nearshoring accelerating manufacturing demand"},
            }
        },
        "insurance": {
            "label": "Insurance",
            "apply_rate": "4.8% - 5.8%",
            "cph_range": "$5,000 - $10,000",
            "yoy_trend": "Actuarial and underwriting roles among most expensive to fill; insurtech disrupting talent landscape",
            "benchmarks": {
                "North America": {"cpa": "$25 - $65", "cpc": "$0.85 - $3.20", "cph": "$5,000 - $10,000", "notes": "Actuarial/underwriting most expensive; insurtech creating new role demand"},
                "Europe (UK/DE/FR)": {"cpa": "$20 - $52", "cpc": "$0.70 - $2.60", "cph": "$4,000 - $8,000", "notes": "London/Zurich insurance markets premium; Solvency II compliance roles"},
                "APAC (IN/AU/SG)": {"cpa": "$8 - $30", "cpc": "$0.25 - $1.30", "cph": "$2,000 - $5,000", "notes": "India BPO insurance ops lower CPA; digital insurance roles growing"},
                "LATAM": {"cpa": "$6 - $22", "cpc": "$0.18 - $0.85", "cph": "$1,500 - $4,000", "notes": "Growing insurance market in Brazil; microinsurance roles emerging"},
            }
        },
        "hospitality_travel": {
            "label": "Hospitality & Travel",
            "apply_rate": "4.0% - 5.0%",
            "cph_range": "$2,500 - $4,000",
            "yoy_trend": "CPA surged +225% YoY (Recruitics Jan 2025); seasonal demand swings extreme; 3,000 jobs shed in Jan 2025",
            "benchmarks": {
                "North America": {"cpa": "$8 - $25", "cpc": "$0.22 - $1.00", "cph": "$2,500 - $4,000", "notes": "CPA surging +225% YoY despite high volume; seasonal peaks extreme; turnover-driven churn"},
                "Europe (UK/DE/FR)": {"cpa": "$7 - $20", "cpc": "$0.18 - $0.80", "cph": "$2,000 - $3,500", "notes": "Tourism hubs seasonal demand; Mediterranean summer surges; visa-dependent workforce"},
                "APAC (IN/AU/SG)": {"cpa": "$3 - $12", "cpc": "$0.06 - $0.40", "cph": "$800 - $2,200", "notes": "Massive hospitality sector in SE Asia; Bali/Thailand/Vietnam growth markets"},
                "LATAM": {"cpa": "$2 - $10", "cpc": "$0.04 - $0.35", "cph": "$600 - $1,800", "notes": "Tourism-driven in Caribbean/Mexico; all-inclusive resort hiring surges"},
            }
        },
        "telecommunications": {
            "label": "Telecommunications",
            "apply_rate": "5.0% - 6.0%",
            "cph_range": "$5,000 - $10,000",
            "yoy_trend": "5G deployment and fiber buildouts driving field technician demand; AI/network roles carrying premium",
            "benchmarks": {
                "North America": {"cpa": "$22 - $60", "cpc": "$0.75 - $2.80", "cph": "$5,000 - $10,000", "notes": "5G/fiber technicians in demand; network engineer roles premium; field roles hard to fill"},
                "Europe (UK/DE/FR)": {"cpa": "$18 - $48", "cpc": "$0.60 - $2.20", "cph": "$4,000 - $8,000", "notes": "Fibre rollout in UK/DE driving demand; regulatory/spectrum roles niche"},
                "APAC (IN/AU/SG)": {"cpa": "$6 - $25", "cpc": "$0.18 - $1.00", "cph": "$1,500 - $4,500", "notes": "India Jio/Airtel hiring at scale; APAC 5G rollout accelerating"},
                "LATAM": {"cpa": "$5 - $20", "cpc": "$0.14 - $0.75", "cph": "$1,200 - $3,500", "notes": "Infrastructure buildout across Brazil/Mexico; rural connectivity roles emerging"},
            }
        },
        "food_beverage": {
            "label": "Food & Beverage",
            "apply_rate": "4.8% - 6.0%",
            "cph_range": "$2,000 - $3,500",
            "yoy_trend": "Steepest CPA decline of any sector: down 25.8% YoY (Recruitics 2025); improved labor availability",
            "benchmarks": {
                "North America": {"cpa": "$6 - $18", "cpc": "$0.18 - $0.75", "cph": "$2,000 - $3,500", "notes": "CPA down 25.8% YoY — steepest decline across all sectors; improved part-time labor availability"},
                "Europe (UK/DE/FR)": {"cpa": "$5 - $15", "cpc": "$0.15 - $0.65", "cph": "$1,800 - $3,000", "notes": "QSR/fast casual competitive; seasonal tourist-area demand spikes"},
                "APAC (IN/AU/SG)": {"cpa": "$2 - $8", "cpc": "$0.05 - $0.30", "cph": "$500 - $1,500", "notes": "Massive food delivery workforce in India/SE Asia; gig economy roles"},
                "LATAM": {"cpa": "$2 - $7", "cpc": "$0.04 - $0.25", "cph": "$400 - $1,200", "notes": "Large informal workforce transitioning to formal; QSR expansion rapid"},
            }
        },
        "media_entertainment": {
            "label": "Media & Entertainment",
            "apply_rate": "5.5% - 6.3%",
            "cph_range": "$5,000 - $12,000",
            "yoy_trend": "Content/streaming roles competitive; marketing & advertising apply rate 6.31% (Appcast 2025)",
            "benchmarks": {
                "North America": {"cpa": "$20 - $55", "cpc": "$0.70 - $2.80", "cph": "$5,000 - $12,000", "notes": "Content creation/streaming roles competitive; marketing at 6.31% apply rate; AI content roles emerging"},
                "Europe (UK/DE/FR)": {"cpa": "$16 - $45", "cpc": "$0.55 - $2.20", "cph": "$4,000 - $9,000", "notes": "London/Berlin creative hubs; localization roles growing across EU"},
                "APAC (IN/AU/SG)": {"cpa": "$5 - $22", "cpc": "$0.15 - $0.90", "cph": "$1,500 - $5,000", "notes": "India Bollywood/OTT content scale; gaming industry growth in Japan/Korea"},
                "LATAM": {"cpa": "$4 - $18", "cpc": "$0.12 - $0.70", "cph": "$1,200 - $4,000", "notes": "Spanish-language content production growing; streaming localization demand"},
            }
        },
        "construction_real_estate": {
            "label": "Construction & Real Estate",
            "apply_rate": "3.5% - 4.8%",
            "cph_range": "$4,500 - $8,000",
            "yoy_trend": "Low apply rates like healthcare; skilled trades shortage persists (Appcast 2025); construction among hardest to hire",
            "benchmarks": {
                "North America": {"cpa": "$20 - $55", "cpc": "$0.65 - $2.50", "cph": "$4,500 - $8,000", "notes": "Skilled trades shortage persists; infrastructure bill driving demand; among hardest to hire alongside healthcare"},
                "Europe (UK/DE/FR)": {"cpa": "$16 - $42", "cpc": "$0.50 - $2.00", "cph": "$3,500 - $6,500", "notes": "Green building/retrofit demand; housing crisis driving construction hiring"},
                "APAC (IN/AU/SG)": {"cpa": "$5 - $20", "cpc": "$0.15 - $0.80", "cph": "$1,200 - $3,500", "notes": "India infrastructure megaprojects; Australia mining/construction premium"},
                "LATAM": {"cpa": "$4 - $16", "cpc": "$0.10 - $0.60", "cph": "$800 - $2,800", "notes": "Infrastructure development across region; skilled labor migration common"},
            }
        },
        "education": {
            "label": "Education",
            "apply_rate": "3.8% - 5.0%",
            "cph_range": "$3,500 - $7,000",
            "yoy_trend": "Among lower-apply-rate 'standing-up' sectors (Appcast 2025); teacher shortages driving higher CPAs",
            "benchmarks": {
                "North America": {"cpa": "$18 - $48", "cpc": "$0.55 - $2.20", "cph": "$3,500 - $7,000", "notes": "Teacher shortages driving higher CPA; STEM/special ed most competitive; edtech roles blending with tech CPAs"},
                "Europe (UK/DE/FR)": {"cpa": "$14 - $38", "cpc": "$0.45 - $1.80", "cph": "$3,000 - $6,000", "notes": "UK teacher recruitment crisis; university research roles competitive"},
                "APAC (IN/AU/SG)": {"cpa": "$5 - $20", "cpc": "$0.12 - $0.75", "cph": "$1,000 - $3,500", "notes": "India edtech boom; international school hiring in Singapore/HK"},
                "LATAM": {"cpa": "$3 - $14", "cpc": "$0.08 - $0.50", "cph": "$800 - $2,500", "notes": "Public education hiring constrained; private/international school growth"},
            }
        },
    }

    # Show benchmarks relevant to the client's industry
    client_industry = data.get("industry", "general_entry_level")
    relevant_benchmarks = {}

    # Always show the client's industry first
    if client_industry in cpa_cpc_benchmarks:
        relevant_benchmarks[client_industry] = cpa_cpc_benchmarks[client_industry]

    # Add general entry-level if not already the client's industry
    if client_industry != "general_entry_level" and "general_entry_level" in cpa_cpc_benchmarks:
        relevant_benchmarks["general_entry_level"] = cpa_cpc_benchmarks["general_entry_level"]

    # If industry not in our benchmark data, show general
    if not relevant_benchmarks:
        relevant_benchmarks["general_entry_level"] = cpa_cpc_benchmarks["general_entry_level"]

    # Determine relevant regions based on client locations
    client_locations = data.get("locations", ["United States"])
    relevant_regions = set()
    for loc in client_locations:
        loc_lower = loc.lower()
        if any(x in loc_lower for x in ["united states", "us", "america", "canada", "new york", "california", "texas", "florida", "chicago", "boston", "seattle"]):
            relevant_regions.add("North America")
        elif any(x in loc_lower for x in ["uk", "united kingdom", "germany", "france", "europe", "london", "berlin", "paris", "netherlands", "spain", "italy"]):
            relevant_regions.add("Europe (UK/DE/FR)")
        elif any(x in loc_lower for x in ["india", "australia", "singapore", "japan", "china", "asia", "apac", "hong kong", "korea"]):
            relevant_regions.add("APAC (IN/AU/SG)")
        elif any(x in loc_lower for x in ["brazil", "mexico", "latin", "latam", "colombia", "argentina", "chile"]):
            relevant_regions.add("LATAM")
        else:
            relevant_regions.add("North America")  # Default
    if not relevant_regions:
        relevant_regions.add("North America")

    for ind_key, ind_data in relevant_benchmarks.items():
        # Industry header with apply rate and CPH summary
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value=ind_data["label"]).font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
        exec_row += 1

        # YoY trend line
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value=f"2025 Trend: {ind_data['yoy_trend']}").font = Font(name="Calibri", italic=True, size=9, color="1B6B3A")
        exec_row += 1

        # Industry-level stats row
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value=f"Avg Apply Rate: {ind_data['apply_rate']}  |  Avg CPH (Total): {ind_data['cph_range']}").font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
        exec_row += 1

        # Table headers — now with CPH column
        bench_headers = ["Region", f"Avg CPA ({display_currency_code})", f"Avg CPC ({display_currency_code})", f"Est. CPH ({display_currency_code})", f"{_bench_year_label} Market Intelligence"]
        for i, h in enumerate(bench_headers):
            cell = ws_exec.cell(row=exec_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        exec_row += 1

        for region_name, region_data in ind_data["benchmarks"].items():
            # Only show regions relevant to the client's target locations
            if region_name not in relevant_regions:
                continue

            c1 = ws_exec.cell(row=exec_row, column=2, value=region_name)
            c1.font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
            c1.border = thin_border

            c2 = ws_exec.cell(row=exec_row, column=3, value=region_data["cpa"])
            c2.font = Font(name="Calibri", bold=True, size=10)
            c2.border = thin_border
            c2.alignment = center_alignment

            c3 = ws_exec.cell(row=exec_row, column=4, value=region_data["cpc"])
            c3.font = Font(name="Calibri", bold=True, size=10)
            c3.border = thin_border
            c3.alignment = center_alignment

            c4 = ws_exec.cell(row=exec_row, column=5, value=region_data["cph"])
            c4.font = Font(name="Calibri", bold=True, size=10)
            c4.border = thin_border
            c4.alignment = center_alignment

            c5 = ws_exec.cell(row=exec_row, column=6, value=region_data["notes"])
            c5.font = Font(name="Calibri", italic=True, size=9, color="596780")
            c5.border = thin_border
            c5.alignment = wrap_alignment

            exec_row += 1

        exec_row += 1

    # ── CPQA Section — Joveo's Recommended Metric ──
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    cpqa_cell = ws_exec.cell(row=exec_row, column=2, value="Beyond CPA: Cost Per Qualified Applicant (CPQA) — Joveo's Recommended Metric")
    cpqa_cell.font = Font(name="Calibri", bold=True, size=12, color="1B2A4A")
    cpqa_cell.border = Border(bottom=Side(style="medium", color="2E75B6"))
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Unlike CPC (cost per click) and CPA (cost per application), CPQA measures the cost of attracting candidates who meet predefined qualification standards — shifting focus from volume to value.").font = Font(name="Calibri", size=10, color="596780")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="True CPQA evaluates candidate fit, intent, and potential — not just proximity to a zip code or surface-level criteria. This is the metric that directly correlates with hiring outcomes.").font = Font(name="Calibri", size=10, color="596780")
    exec_row += 1

    cpqa_points = [
        "CPC tells you the cost of getting someone to look at a job. CPA tells you the cost of getting an application. CPQA tells you the cost of getting someone who can actually be hired.",
        "With Joveo's programmatic platform, ML-driven bid optimization targets quality signals across 1,200+ publishers, reducing CPQA by up to 10x vs. manual media buying.",
        "Industry benchmark: An optimized CPQA should be 3-5x higher than CPA but deliver 2-3x better interview-to-hire conversion rates.",
        "Case study impact: Organizations using CPQA-optimized campaigns have achieved CPA reductions from $40 to $4 through intelligent job ad expansion and publisher mix optimization.",
    ]
    for point in cpqa_points:
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value=f"  \u2022  {point}").font = Font(name="Calibri", size=9, color="1B2A4A")
        exec_row += 1

    # ── Source Citations ──
    exec_row += 2
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Data Sources & Citations").font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
    ws_exec.cell(row=exec_row, column=2).border = Border(bottom=Side(style="thin", color="2E75B6"))
    exec_row += 1

    sources = [
        f"[1] Appcast {_current_year - 1} Recruitment Marketing Benchmark Report — 379M clicks, 30M+ applies from 1,300+ US employers (appcast.io/benchmark-report)",
        f"[2] Appcast {_current_year} Benchmark Report — 10th annual, new candidate disposition & global data (prnewswire.com)",
        f"[3] Recruitics Talent Market Index (Monthly {_bench_year_label}) — Billions of data points across all verticals, CPA/CPC by job family (recruitics.com/labor-market-index)",
        f"[4] SHRM {_bench_year_label} Benchmarking Reports — 88 data sets, avg US CPH $4,700, exec CPH $28,000+ (shrm.org/benchmarking)",
        "[5] Joveo Platform Data & CPQA Framework — Cost Per Qualified Applicant methodology across 1,200+ publishers (joveo.com)",
        f"[6] Industry CPH benchmarks aggregated from HR Dive, Engagedly, and industry-specific recruitment cost studies ({_bench_year_label})",
    ]
    for src in sources:
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value=src).font = Font(name="Calibri", italic=True, size=8, color="777777")
        exec_row += 1

    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Rates shown for your target regions. Actual rates may vary based on job specificity, seasonality, competition, and programmatic optimization level.").font = Font(name="Calibri", italic=True, size=8, color="999999")

    # ── Role-Tier Strategy Breakdown ──
    tier_groups = data.get("_tier_groups", {})
    if tier_groups and roles:
        exec_row += 3
        style_section_header(ws_exec, exec_row, 2, 7, "Role-Tier Strategy Breakdown")
        exec_row += 1
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value="Each role is classified into a hiring tier with tailored funnel weights, recommended channels, and expected time-to-fill. This ensures differentiated strategies across executive, professional, clinical, trades, hourly, gig, and education roles.").font = Font(name="Calibri", italic=True, size=9, color="596780")
        exec_row += 2

        tier_headers = ["Role Tier", "Roles", "Recommended Channels", "CPA Multiplier", "Est. Time to Fill", "Funnel Emphasis"]
        for i, h in enumerate(tier_headers):
            cell = ws_exec.cell(row=exec_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        exec_row += 1

        for tier_name, tg in tier_groups.items():
            ti = tg["tier_info"]
            roles_display = ", ".join(tg["roles"][:4])
            if len(tg["roles"]) > 4:
                roles_display += f" (+{len(tg['roles']) - 4} more)"
            channels_display = ", ".join(ti["channels"][:4])
            funnel_info = ti["funnel"]
            # Find the dominant funnel stage
            dominant_stage = max(funnel_info, key=funnel_info.get)
            funnel_display = f"{dominant_stage.title()} ({int(funnel_info[dominant_stage]*100)}%)"

            c1 = ws_exec.cell(row=exec_row, column=2, value=tier_name)
            c1.font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
            c1.border = thin_border

            c2 = ws_exec.cell(row=exec_row, column=3, value=roles_display)
            c2.font = Font(name="Calibri", size=10, color="333333")
            c2.border = thin_border
            c2.alignment = Alignment(wrap_text=True)

            c3 = ws_exec.cell(row=exec_row, column=4, value=channels_display)
            c3.font = Font(name="Calibri", size=9, color="596780")
            c3.border = thin_border
            c3.alignment = Alignment(wrap_text=True)

            c4 = ws_exec.cell(row=exec_row, column=5, value=f"{ti['cpa_multiplier']}x")
            c4.font = Font(name="Calibri", size=10, color="333333")
            c4.border = thin_border
            c4.alignment = center_alignment

            c5 = ws_exec.cell(row=exec_row, column=6, value=f"{ti['time_to_fill_days']} days")
            c5.font = Font(name="Calibri", size=10, color="333333")
            c5.border = thin_border
            c5.alignment = center_alignment

            c6 = ws_exec.cell(row=exec_row, column=7, value=funnel_display)
            c6.font = Font(name="Calibri", size=10, color="596780")
            c6.border = thin_border
            c6.alignment = center_alignment

            # Alternating row fill
            if list(tier_groups.keys()).index(tier_name) % 2 == 0:
                for c in range(2, 8):
                    ws_exec.cell(row=exec_row, column=c).fill = accent_pale_fill

            ws_exec.row_dimensions[exec_row].height = 30
            exec_row += 1

        exec_row += 1
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        ws_exec.cell(row=exec_row, column=2, value="CPA Multiplier reflects relative cost compared to baseline. Time-to-fill estimates are industry medians. Funnel emphasis shows where budget should be concentrated.").font = Font(name="Calibri", italic=True, size=8, color="999999")

    # ── Expected Hiring Funnel Forecast ──
    exec_row += 3
    style_section_header(ws_exec, exec_row, 2, 7, "Expected Hiring Funnel Forecast")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Estimated pipeline volumes based on industry conversion benchmarks and selected channel mix. Actual results depend on job specificity, employer brand, and optimization.").font = Font(name="Calibri", italic=True, size=9, color="596780")
    exec_row += 2

    # Funnel conversion rates by industry (based on 2025 research data)
    funnel_benchmarks = {
        "healthcare_medical": {"impression_to_click": 0.028, "click_to_apply": 0.035, "apply_to_qualified": 0.22, "qualified_to_interview": 0.35, "interview_to_offer": 0.28, "offer_to_hire": 0.82},
        "tech_engineering": {"impression_to_click": 0.035, "click_to_apply": 0.064, "apply_to_qualified": 0.30, "qualified_to_interview": 0.40, "interview_to_offer": 0.25, "offer_to_hire": 0.78},
        "blue_collar_trades": {"impression_to_click": 0.030, "click_to_apply": 0.045, "apply_to_qualified": 0.25, "qualified_to_interview": 0.38, "interview_to_offer": 0.32, "offer_to_hire": 0.85},
        "general_entry_level": {"impression_to_click": 0.032, "click_to_apply": 0.061, "apply_to_qualified": 0.28, "qualified_to_interview": 0.35, "interview_to_offer": 0.30, "offer_to_hire": 0.80},
        "finance_banking": {"impression_to_click": 0.030, "click_to_apply": 0.050, "apply_to_qualified": 0.26, "qualified_to_interview": 0.38, "interview_to_offer": 0.27, "offer_to_hire": 0.80},
        "retail_consumer": {"impression_to_click": 0.033, "click_to_apply": 0.055, "apply_to_qualified": 0.30, "qualified_to_interview": 0.32, "interview_to_offer": 0.35, "offer_to_hire": 0.85},
        "pharma_biotech": {"impression_to_click": 0.025, "click_to_apply": 0.040, "apply_to_qualified": 0.20, "qualified_to_interview": 0.35, "interview_to_offer": 0.25, "offer_to_hire": 0.80},
        "hospitality_travel": {"impression_to_click": 0.035, "click_to_apply": 0.050, "apply_to_qualified": 0.32, "qualified_to_interview": 0.30, "interview_to_offer": 0.38, "offer_to_hire": 0.88},
        "logistics_supply_chain": {"impression_to_click": 0.030, "click_to_apply": 0.048, "apply_to_qualified": 0.25, "qualified_to_interview": 0.35, "interview_to_offer": 0.30, "offer_to_hire": 0.83},
        "energy_utilities": {"impression_to_click": 0.028, "click_to_apply": 0.045, "apply_to_qualified": 0.24, "qualified_to_interview": 0.36, "interview_to_offer": 0.28, "offer_to_hire": 0.82},
        "automotive": {"impression_to_click": 0.030, "click_to_apply": 0.048, "apply_to_qualified": 0.26, "qualified_to_interview": 0.36, "interview_to_offer": 0.30, "offer_to_hire": 0.84},
        "insurance": {"impression_to_click": 0.029, "click_to_apply": 0.050, "apply_to_qualified": 0.25, "qualified_to_interview": 0.37, "interview_to_offer": 0.27, "offer_to_hire": 0.80},
        "food_beverage": {"impression_to_click": 0.034, "click_to_apply": 0.055, "apply_to_qualified": 0.32, "qualified_to_interview": 0.30, "interview_to_offer": 0.36, "offer_to_hire": 0.87},
        "construction_real_estate": {"impression_to_click": 0.027, "click_to_apply": 0.040, "apply_to_qualified": 0.22, "qualified_to_interview": 0.34, "interview_to_offer": 0.30, "offer_to_hire": 0.83},
        "education": {"impression_to_click": 0.028, "click_to_apply": 0.045, "apply_to_qualified": 0.28, "qualified_to_interview": 0.35, "interview_to_offer": 0.30, "offer_to_hire": 0.82},
        "telecommunications": {"impression_to_click": 0.030, "click_to_apply": 0.050, "apply_to_qualified": 0.26, "qualified_to_interview": 0.38, "interview_to_offer": 0.28, "offer_to_hire": 0.80},
        "media_entertainment": {"impression_to_click": 0.032, "click_to_apply": 0.058, "apply_to_qualified": 0.28, "qualified_to_interview": 0.36, "interview_to_offer": 0.26, "offer_to_hire": 0.78},
    }

    fb = funnel_benchmarks.get(client_industry, funnel_benchmarks["general_entry_level"])

    # ── Budget-driven funnel calculation ──
    # Parse budget from the budget_range string (e.g. "$50,000 - $250,000", "< $50,000")
    budget_range_str = str(data.get("budget_range", "") or "")
    budget_midpoint = parse_budget(budget_range_str)

    # Industry-specific Cost-Per-Hire (CPH) ranges for realistic funnel math
    industry_cph_ranges = {
        "healthcare_medical":      (9000, 12000),
        "tech_engineering":        (6000, 14000),
        "blue_collar_trades":      (3500, 5600),
        "general_entry_level":     (2000, 4700),
        "finance_banking":         (5000, 12000),
        "retail_consumer":         (2700, 4000),
        "pharma_biotech":          (8000, 18000),
        "hospitality_travel":      (2500, 4000),
        "logistics_supply_chain":  (4500, 8000),
        "energy_utilities":        (5000, 10000),
        "automotive":              (5600, 9000),
        "insurance":               (5000, 10000),
        "food_beverage":           (2000, 3500),
        "construction_real_estate":(4500, 8000),
        "education":               (3500, 7000),
        "telecommunications":      (5000, 10000),
        "media_entertainment":     (5000, 12000),
        "maritime_marine":         (5000, 10000),
        "military_recruitment":    (4000, 8000),
        "legal_services":          (5000, 12000),
        "mental_health":           (4000, 8000),
        "aerospace_defense":       (6000, 15000),
    }
    cph_low, cph_high = industry_cph_ranges.get(client_industry, (4000, 8000))
    avg_cph = (cph_low + cph_high) / 2

    # Adjust CPH by weighted role-tier CPA multiplier
    role_tiers_data = data.get("_role_tiers", {})
    if role_tiers_data:
        total_multiplier = sum(t["cpa_multiplier"] for t in role_tiers_data.values())
        avg_tier_multiplier = total_multiplier / len(role_tiers_data)
        # Apply tier multiplier to CPH (normalize around 1.0 baseline for professional tier)
        avg_cph = avg_cph * (avg_tier_multiplier / 1.5)  # 1.5 is the professional baseline

    # Calculate realistic projected hires from budget
    est_hires_from_budget = max(1, int(budget_midpoint / avg_cph))

    # Build funnel UPWARD from hires using realistic conversion ratios
    est_hires_val = est_hires_from_budget
    est_offers = int(est_hires_val / fb["offer_to_hire"])
    est_interviews_final = int(est_offers / fb["interview_to_offer"])
    est_qualified = int(est_interviews_final / fb["qualified_to_interview"])
    est_applications = int(est_qualified / fb["apply_to_qualified"])
    est_clicks = int(est_applications / fb["click_to_apply"])
    est_impressions = int(est_clicks / fb["impression_to_click"])

    funnel_stages = [
        ("Impressions", est_impressions, "Job ad views across all selected channels", None),
        ("Clicks", est_clicks, "Candidates who clicked through to job details", f"{fb['impression_to_click']*100:.1f}%"),
        ("Applications", est_applications, "Completed applications submitted", f"{fb['click_to_apply']*100:.1f}%"),
        ("Qualified Applicants", est_qualified, "Applicants meeting minimum qualifications (CPQA target)", f"{fb['apply_to_qualified']*100:.0f}%"),
        ("Interviews", est_interviews_final, "Candidates advancing to interview stage", f"{fb['qualified_to_interview']*100:.0f}%"),
        ("Offers", est_offers, "Offers extended to qualified candidates", f"{fb['interview_to_offer']*100:.0f}%"),
        ("Hires", est_hires_val, f"Projected hires at {display_currency}{avg_cph:,.0f} avg CPH", f"{fb['offer_to_hire']*100:.0f}%"),
    ]

    # Funnel table headers
    funnel_headers = ["Funnel Stage", "Est. Volume", "Conversion Rate", "Description"]
    for i, h in enumerate(funnel_headers):
        cell = ws_exec.cell(row=exec_row, column=2 + i, value=h)
        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
        cell.fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
        cell.border = thin_border
        cell.alignment = center_alignment
    exec_row += 1

    # Funnel color gradient - navy to light blue (LinkedIn style)
    funnel_colors = [NAVY, "1B3D6F", "245694", "2E6EB8", "3886DC", "5A9FE6", "82B8F0"]
    funnel_table_start_row = exec_row  # track where table data begins
    for idx, (stage, volume, desc, rate) in enumerate(funnel_stages):
        c1 = ws_exec.cell(row=exec_row, column=2, value=f"{'  ' * idx}\u25B6 {stage}")
        c1.font = Font(name="Calibri", bold=True, size=10, color=funnel_colors[idx])
        c1.border = thin_border

        # Write volume as number for chart reference
        c2 = ws_exec.cell(row=exec_row, column=3, value=volume)
        c2.font = Font(name="Calibri", bold=True, size=11, color=NAVY)
        c2.border = thin_border
        c2.alignment = center_alignment
        c2.number_format = '#,##0'

        c3 = ws_exec.cell(row=exec_row, column=4, value=rate if rate else "\u2014")
        c3.font = Font(name="Calibri", size=10, color="596780")
        c3.border = thin_border
        c3.alignment = center_alignment

        c4 = ws_exec.cell(row=exec_row, column=5, value=desc)
        c4.font = Font(name="Calibri", italic=True, size=9, color="596780")
        c4.border = thin_border
        c4.alignment = wrap_alignment

        # Alternating row fill with pale blue
        if idx % 2 == 0:
            for c in range(2, 6):
                ws_exec.cell(row=exec_row, column=c).fill = accent_pale_fill

        exec_row += 1

    funnel_table_end_row = exec_row - 1

    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value=f"Conversion rates based on {_bench_year_label} industry benchmarks (Appcast, Recruitics). CPQA stage is where Joveo's ML optimization delivers highest impact.").font = Font(name="Calibri", italic=True, size=8, color="999999")

    # ── Budget Sufficiency Warning ──
    budget_warning = ""
    hire_volume_raw = data.get("hire_volume", "")
    if hire_volume_raw and budget_midpoint:
        try:
            # Parse target hires from hire_volume string (e.g. "50", "50 hires", "100+")
            _hv_nums = re.findall(r'\d+', str(hire_volume_raw))
            target_hires = int(_hv_nums[0]) if _hv_nums else 0
            if target_hires > 0 and budget_midpoint > 0:
                # Use industry-average CPH to estimate achievable hires
                estimated_hires_possible = budget_midpoint / max(avg_cph, 1)
                if estimated_hires_possible < target_hires * 0.3:
                    budget_warning = f"\u26a0\ufe0f BUDGET ALERT: At industry-average cost-per-hire (${avg_cph:,.0f}), the ${budget_midpoint:,.0f} budget may only yield ~{int(estimated_hires_possible)} of your {target_hires} target hires ({int(estimated_hires_possible/target_hires*100)}% of goal). Consider increasing budget or phasing the campaign."
        except (ValueError, TypeError, IndexError):
            pass

    if budget_warning:
        exec_row += 2
        ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
        warn_cell = ws_exec.cell(row=exec_row, column=2, value=budget_warning)
        warn_cell.font = Font(name="Calibri", bold=True, size=11, color="FFFFFF")
        warn_cell.fill = PatternFill(start_color="C00000", end_color="C00000", fill_type="solid")
        warn_cell.alignment = Alignment(wrap_text=True, vertical="center")
        warn_cell.border = Border(
            left=Side(style="thick", color="C00000"),
            right=Side(style="thick", color="C00000"),
            top=Side(style="thick", color="C00000"),
            bottom=Side(style="thick", color="C00000"),
        )
        for _wc in range(3, 8):
            ws_exec.cell(row=exec_row, column=_wc).fill = PatternFill(start_color="C00000", end_color="C00000", fill_type="solid")
        ws_exec.row_dimensions[exec_row].height = 50

    # ── Funnel Visualization Chart (horizontal bar chart) ──
    exec_row += 2
    funnel_chart = BarChart()
    funnel_chart.type = "bar"  # horizontal bars
    funnel_chart.style = 10
    funnel_chart.title = "Hiring Funnel — Pipeline Volume by Stage"
    funnel_chart.y_axis.title = None
    funnel_chart.x_axis.title = "Estimated Volume"
    funnel_chart.width = 28
    funnel_chart.height = 14

    # Data reference: column 3 (volumes) from funnel_table_start_row to funnel_table_end_row
    funnel_data_ref = Reference(ws_exec, min_col=3, min_row=funnel_table_start_row, max_row=funnel_table_end_row)
    funnel_cats_ref = Reference(ws_exec, min_col=2, min_row=funnel_table_start_row, max_row=funnel_table_end_row)
    funnel_chart.add_data(funnel_data_ref, titles_from_data=False)
    funnel_chart.set_categories(funnel_cats_ref)
    funnel_chart.shape = 4
    funnel_chart.legend = None

    # Apply navy-to-light-blue gradient colors to individual bars
    bar_colors_hex = ["1B2A4A", "1B3D6F", "245694", "2E6EB8", "3886DC", "5A9FE6", "82B8F0"]
    series = funnel_chart.series[0]
    for bar_idx in range(len(funnel_stages)):
        pt = DataPoint(idx=bar_idx)
        pt.graphicalProperties.solidFill = bar_colors_hex[bar_idx]
        series.data_points.append(pt)

    ws_exec.add_chart(funnel_chart, f"B{exec_row}")
    exec_row += 17  # Space for chart height

    # ── Channel Contribution Forecast ──
    exec_row += 3
    style_section_header(ws_exec, exec_row, 2, 7, "Channel Contribution Forecast")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Estimated % of qualified pipeline contributed by each selected channel category, based on programmatic recruitment benchmarks.").font = Font(name="Calibri", italic=True, size=9, color="596780")
    exec_row += 2

    ch_cats2_raw = data.get("channel_categories", {})
    if isinstance(ch_cats2_raw, list):
        ch_cats2 = {}
        for item in ch_cats2_raw:
            if isinstance(item, dict):
                ch_cats2[item.get("name", "")] = item.get("enabled", True)
            else:
                ch_cats2[str(item)] = True
    else:
        ch_cats2 = ch_cats2_raw

    # ── Industry-aware channel allocation profiles ──
    _INDUSTRY_ALLOC = {
        "healthcare_medical":     {"programmatic_dsp": 22, "global_boards": 15, "niche_boards": 30, "social_media": 10, "regional_boards": 10, "employer_branding": 8, "apac_regional": 3, "emea_regional": 2},
        "tech_engineering":       {"programmatic_dsp": 30, "global_boards": 15, "niche_boards": 20, "social_media": 18, "regional_boards": 5,  "employer_branding": 7, "apac_regional": 3, "emea_regional": 2},
        "finance_banking":        {"programmatic_dsp": 25, "global_boards": 18, "niche_boards": 25, "social_media": 10, "regional_boards": 7,  "employer_branding": 10, "apac_regional": 3, "emea_regional": 2},
        "retail_consumer":        {"programmatic_dsp": 38, "global_boards": 22, "niche_boards": 8,  "social_media": 20, "regional_boards": 7,  "employer_branding": 3, "apac_regional": 1, "emea_regional": 1},
        "hospitality_travel":     {"programmatic_dsp": 38, "global_boards": 22, "niche_boards": 8,  "social_media": 20, "regional_boards": 7,  "employer_branding": 3, "apac_regional": 1, "emea_regional": 1},
        "general_entry_level":    {"programmatic_dsp": 40, "global_boards": 22, "niche_boards": 8,  "social_media": 15, "regional_boards": 10, "employer_branding": 3, "apac_regional": 1, "emea_regional": 1},
        "blue_collar_trades":     {"programmatic_dsp": 35, "global_boards": 20, "niche_boards": 10, "social_media": 15, "regional_boards": 15, "employer_branding": 3, "apac_regional": 1, "emea_regional": 1},
        "aerospace_defense":      {"programmatic_dsp": 20, "global_boards": 15, "niche_boards": 30, "social_media": 8,  "regional_boards": 10, "employer_branding": 12, "apac_regional": 3, "emea_regional": 2},
        "pharma_biotech":         {"programmatic_dsp": 22, "global_boards": 15, "niche_boards": 28, "social_media": 10, "regional_boards": 8,  "employer_branding": 12, "apac_regional": 3, "emea_regional": 2},
        "education":              {"programmatic_dsp": 20, "global_boards": 18, "niche_boards": 28, "social_media": 12, "regional_boards": 10, "employer_branding": 7, "apac_regional": 3, "emea_regional": 2},
        "legal_services":         {"programmatic_dsp": 22, "global_boards": 18, "niche_boards": 28, "social_media": 8,  "regional_boards": 8,  "employer_branding": 11, "apac_regional": 3, "emea_regional": 2},
        "automotive":             {"programmatic_dsp": 30, "global_boards": 18, "niche_boards": 18, "social_media": 10, "regional_boards": 15, "employer_branding": 5, "apac_regional": 2, "emea_regional": 2},
        "energy_utilities":       {"programmatic_dsp": 25, "global_boards": 15, "niche_boards": 25, "social_media": 8,  "regional_boards": 15, "employer_branding": 7, "apac_regional": 3, "emea_regional": 2},
        "mental_health":          {"programmatic_dsp": 22, "global_boards": 18, "niche_boards": 28, "social_media": 10, "regional_boards": 8,  "employer_branding": 9, "apac_regional": 3, "emea_regional": 2},
        "logistics_supply_chain": {"programmatic_dsp": 35, "global_boards": 20, "niche_boards": 12, "social_media": 10, "regional_boards": 15, "employer_branding": 5, "apac_regional": 2, "emea_regional": 1},
        "insurance":              {"programmatic_dsp": 25, "global_boards": 18, "niche_boards": 25, "social_media": 10, "regional_boards": 7,  "employer_branding": 10, "apac_regional": 3, "emea_regional": 2},
        "maritime_marine":        {"programmatic_dsp": 20, "global_boards": 15, "niche_boards": 30, "social_media": 8,  "regional_boards": 15, "employer_branding": 7, "apac_regional": 3, "emea_regional": 2},
        # C3 FIX: Add missing industry channel profiles
        "construction_real_estate":{"programmatic_dsp": 30, "global_boards": 18, "niche_boards": 15, "social_media": 10, "regional_boards": 18, "employer_branding": 5, "apac_regional": 2, "emea_regional": 2},
        "telecommunications":    {"programmatic_dsp": 28, "global_boards": 15, "niche_boards": 22, "social_media": 12, "regional_boards": 8,  "employer_branding": 10, "apac_regional": 3, "emea_regional": 2},
        "rideshare":              {"programmatic_dsp": 42, "global_boards": 22, "niche_boards": 5,  "social_media": 18, "regional_boards": 8,  "employer_branding": 3, "apac_regional": 1, "emea_regional": 1},
        "professional_services":  {"programmatic_dsp": 22, "global_boards": 18, "niche_boards": 25, "social_media": 10, "regional_boards": 8,  "employer_branding": 12, "apac_regional": 3, "emea_regional": 2},
        "government":             {"programmatic_dsp": 18, "global_boards": 20, "niche_boards": 28, "social_media": 5,  "regional_boards": 15, "employer_branding": 8, "apac_regional": 3, "emea_regional": 3},
        "military_recruitment":   {"programmatic_dsp": 15, "global_boards": 15, "niche_boards": 35, "social_media": 8,  "regional_boards": 12, "employer_branding": 10, "apac_regional": 3, "emea_regional": 2},
        "manufacturing":          {"programmatic_dsp": 32, "global_boards": 18, "niche_boards": 15, "social_media": 10, "regional_boards": 18, "employer_branding": 4, "apac_regional": 2, "emea_regional": 1},
        "telecom":                {"programmatic_dsp": 28, "global_boards": 15, "niche_boards": 22, "social_media": 12, "regional_boards": 8,  "employer_branding": 10, "apac_regional": 3, "emea_regional": 2},
        "food_beverage":          {"programmatic_dsp": 40, "global_boards": 22, "niche_boards": 5,  "social_media": 20, "regional_boards": 8,  "employer_branding": 3, "apac_regional": 1, "emea_regional": 1},
        "real_estate":            {"programmatic_dsp": 25, "global_boards": 18, "niche_boards": 20, "social_media": 12, "regional_boards": 15, "employer_branding": 5, "apac_regional": 3, "emea_regional": 2},
    }
    _DEFAULT_ALLOC = {"programmatic_dsp": 35, "global_boards": 20, "niche_boards": 15, "social_media": 12, "regional_boards": 8, "employer_branding": 5, "apac_regional": 3, "emea_regional": 2}
    _ind_key = data.get("industry", "general_entry_level")
    _ap = dict(_INDUSTRY_ALLOC.get(_ind_key, _DEFAULT_ALLOC))

    # Budget-size adjustment
    _bstr = str(data.get("budget", "") or "")
    try:
        _bnums = re.findall(r'[\d]+', _bstr.replace(",", "").replace("$", "").strip())
        _bval = int(_bnums[0]) if _bnums and int(_bnums[0]) >= 100 else None
    except (ValueError, IndexError):
        _bval = None
    if _bval is not None:
        if _bval < 50000:
            _ap["employer_branding"] = max(1, _ap["employer_branding"] - 3)
            _ap["apac_regional"] = max(0, _ap["apac_regional"] - 2)
            _ap["emea_regional"] = max(0, _ap["emea_regional"] - 1)
            _ap["programmatic_dsp"] += 4; _ap["global_boards"] += 2
        elif _bval > 500000:
            _ap["employer_branding"] += 4; _ap["regional_boards"] += 2; _ap["social_media"] += 2
            _ap["programmatic_dsp"] = max(5, _ap["programmatic_dsp"] - 5)
            _ap["global_boards"] = max(5, _ap["global_boards"] - 3)

    # Role seniority adjustment
    _rl = data.get("roles", []) or data.get("target_roles", []) or []
    if _rl:
        _rt = " ".join(r.lower() for r in _rl)
        _sr = sum(1 for k in ["executive","director","vp","chief","president","c-suite","senior","head of","principal"] if k in _rt)
        _jr = sum(1 for k in ["intern","entry","junior","associate","trainee","assistant","coordinator"] if k in _rt)
        if _sr > _jr:
            _ap["niche_boards"] += 4; _ap["employer_branding"] += 3
            _ap["social_media"] = max(2, _ap["social_media"] - 3); _ap["programmatic_dsp"] = max(5, _ap["programmatic_dsp"] - 4)
        elif _jr > _sr:
            _ap["social_media"] += 5; _ap["global_boards"] += 3
            _ap["niche_boards"] = max(2, _ap["niche_boards"] - 4); _ap["programmatic_dsp"] = max(5, _ap["programmatic_dsp"] - 2)
    for _k2 in _ap:
        _ap[_k2] = max(1, _ap[_k2])

    # Industry-specific channel descriptions
    _NICHE_DESC = {
        "healthcare_medical": "Health eCareers, Doximity, Vivian Health — clinical talent; highest quality match for medical roles",
        "tech_engineering": "Dice, BuiltIn, Stack Overflow Talent — developer & engineering talent; skills-matched candidates",
        "finance_banking": "eFinancialCareers, CFA Career Center — financial professional talent; CPA/CFA certified candidates",
        "aerospace_defense": "ClearedJobs.net, Aviation Job Search — cleared & aerospace talent; security-vetted pipeline",
        "pharma_biotech": "BioSpace, Nature Careers, MedReps — scientific & clinical talent; research-credentialed candidates",
        "education": "HigherEdJobs, K12JobSpot, SchoolSpring — academic talent; certified educator pipeline",
    }
    _niche_d = _NICHE_DESC.get(_ind_key, "Specialized boards with higher quality match; lower volume but better CPQA")
    _SOCIAL_DESC = {
        "tech_engineering": "LinkedIn Ads, Reddit, Discord, GitHub — developer community reach; passive tech talent engagement",
        "retail_consumer": "Facebook Jobs, Instagram, TikTok, Snapchat — high-volume social reach; mobile-first hourly candidates",
        "hospitality_travel": "Facebook Jobs, Instagram, TikTok — high-volume social reach; seasonal & hourly workforce",
        "blue_collar_trades": "Facebook Jobs, TikTok, Nextdoor — local trade worker reach; mobile-first blue-collar candidates",
    }
    _social_d = _SOCIAL_DESC.get(_ind_key, "Facebook Jobs, Instagram, TikTok — passive candidate reach; employer brand amplification")

    channel_allocations = []
    if ch_cats2.get("programmatic_dsp", True):
        channel_allocations.append(("Programmatic & DSP", _ap["programmatic_dsp"], "ML-optimized bidding; highest volume driver; real-time publisher optimization", "#2E75B6", "Highest"))
    if ch_cats2.get("global_boards", True):
        channel_allocations.append(("Global Job Boards", _ap["global_boards"], "Indeed, ZipRecruiter, Glassdoor — broad reach; consistent applicant flow", "#1B6B3A", "High"))
    if ch_cats2.get("niche_boards", True):
        channel_allocations.append(("Niche & Industry Boards", _ap["niche_boards"], _niche_d, "#0A66C9", "Medium-High"))
    if ch_cats2.get("social_media", True):
        channel_allocations.append(("Social Media Channels", _ap["social_media"], _social_d, "#ED7D31", "Medium"))
    if ch_cats2.get("regional_boards", True):
        channel_allocations.append(("Regional & Local Boards", _ap["regional_boards"], "Geo-targeted local reach; strong for hourly & blue-collar roles", "#4472C4", "Medium"))
    if ch_cats2.get("employer_branding", False):
        channel_allocations.append(("Employer Branding", _ap["employer_branding"], "Glassdoor, Comparably — long-term brand building; 1.7x higher InMail acceptance", "#00B0F0", "Long-term"))
    if ch_cats2.get("apac_regional", False):
        channel_allocations.append(("APAC Regional", _ap["apac_regional"], "JobStreet, Naukri, Seek — APAC market-specific reach", "#FFC000", "Regional"))
    if ch_cats2.get("emea_regional", False):
        channel_allocations.append(("EMEA Regional", _ap["emea_regional"], "StepStone, Totaljobs, Reed — EMEA market-specific reach", "#FF6B6B", "Regional"))

    # Normalize allocations to 100%
    total_alloc = sum(a[1] for a in channel_allocations) if channel_allocations else 100
    normalized = [(name, round(pct/total_alloc*100), desc, color, impact) for name, pct, desc, color, impact in channel_allocations]

    # Channel table headers
    ch_headers = ["Channel Category", "Budget Allocation", "Impact Level", "Strategy & Expected Contribution"]
    for i, h in enumerate(ch_headers):
        cell = ws_exec.cell(row=exec_row, column=2 + i, value=h)
        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
        cell.fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
        cell.border = thin_border
        cell.alignment = center_alignment
    exec_row += 1

    for idx, (name, pct, desc, color, impact) in enumerate(normalized):
        c1 = ws_exec.cell(row=exec_row, column=2, value=name)
        c1.font = Font(name="Calibri", bold=True, size=10, color=color.replace("#", ""))
        c1.border = thin_border

        # Visual bar representation using Unicode block chars
        bar_len = max(1, pct // 5)
        bar = "\u2588" * bar_len + f"  {pct}%"
        c2 = ws_exec.cell(row=exec_row, column=3, value=bar)
        c2.font = Font(name="Calibri", bold=True, size=10, color=color.replace("#", ""))
        c2.border = thin_border
        c2.alignment = Alignment(horizontal="left", vertical="center")

        c3 = ws_exec.cell(row=exec_row, column=4, value=impact)
        c3.font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
        c3.border = thin_border
        c3.alignment = center_alignment

        c4 = ws_exec.cell(row=exec_row, column=5, value=desc)
        c4.font = Font(name="Calibri", italic=True, size=9, color="596780")
        c4.border = thin_border
        c4.alignment = wrap_alignment

        if idx % 2 == 0:
            for c in range(2, 6):
                ws_exec.cell(row=exec_row, column=c).fill = accent_pale_fill
        exec_row += 1

    # ── Donut Chart for Channel Distribution ──
    if normalized:
        exec_row += 2
        # Write chart data in a hidden area (columns H-I) for the donut chart reference
        donut_data_start = exec_row
        ws_exec.cell(row=exec_row, column=8, value="Channel").font = Font(name="Calibri", bold=True, size=9, color="999999")
        ws_exec.cell(row=exec_row, column=9, value="Allocation %").font = Font(name="Calibri", bold=True, size=9, color="999999")
        exec_row += 1
        donut_colors = [NAVY, BLUE, ACCENT, LIGHT_BLUE, WARM_GRAY, MEDIUM_BLUE, ACCENT_LIGHT, "A8D4FF"]
        for ch_idx, (ch_name, ch_pct, _desc, _color, _impact) in enumerate(normalized):
            ws_exec.cell(row=exec_row, column=8, value=ch_name).font = Font(name="Calibri", size=8, color="999999")
            ws_exec.cell(row=exec_row, column=9, value=ch_pct).font = Font(name="Calibri", size=8, color="999999")
            exec_row += 1
        donut_data_end = exec_row - 1

        donut_chart = DoughnutChart()
        donut_chart.title = "Budget Allocation by Channel"
        donut_chart.style = 10
        donut_chart.width = 18
        donut_chart.height = 12

        donut_data_ref = Reference(ws_exec, min_col=9, min_row=donut_data_start, max_row=donut_data_end)
        donut_cats_ref = Reference(ws_exec, min_col=8, min_row=donut_data_start + 1, max_row=donut_data_end)
        donut_chart.add_data(donut_data_ref, titles_from_data=True)
        donut_chart.set_categories(donut_cats_ref)

        # Apply LinkedIn-style colors to donut segments
        if donut_chart.series:
            donut_series = donut_chart.series[0]
            for seg_idx in range(len(normalized)):
                seg_pt = DataPoint(idx=seg_idx)
                color_idx = seg_idx % len(donut_colors)
                seg_pt.graphicalProperties.solidFill = donut_colors[color_idx]
                donut_series.data_points.append(seg_pt)

        # Data labels showing percentages
        donut_chart.dataLabels = DataLabelList()
        donut_chart.dataLabels.showPercent = True
        donut_chart.dataLabels.showCatName = True
        donut_chart.dataLabels.showVal = False

        # Place chart after the table
        chart_anchor_row = donut_data_start - 1
        ws_exec.add_chart(donut_chart, f"B{chart_anchor_row}")
        exec_row = chart_anchor_row + 16  # Space for chart

    # ── Employer Branding ROI ──
    exec_row += 3
    style_section_header(ws_exec, exec_row, 2, 7, "Employer Branding ROI — Why Brand Investment Multiplies Hiring Outcomes")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value=f"Data from LinkedIn's {_bench_year_label} Hiring Value analysis shows that candidates who engage with an employer's brand before applying deliver significantly better hiring outcomes.").font = Font(name="Calibri", italic=True, size=9, color="596780")
    exec_row += 2

    eb_metrics = [
        ("1.7x", "Higher Outreach Acceptance", "Candidates exposed to employer branding are 1.7x more likely to accept recruiter InMails and respond to outreach messages.", "#2E75B6"),
        ("5.9x", "Higher Conversion to Hire", "Brand-engaged applicants are 5.9x more likely to convert from application to hire compared to non-engaged candidates.", "#00B050"),
        ("2.2x", "Faster Promotion Rate", "Employer brand-influenced hires show 2.2x higher promotion rates, indicating better role fit and long-term alignment.", "#0A66C9"),
        ("1.4x", "Higher Demand Talent", "Brand-influenced hires are 1.4x more likely to be in-demand candidates (higher InMail volume), signaling you're attracting competitive talent.", "#ED7D31"),
        ("82%", "First-Year Retention", "Brand-engaged hires show stronger first-year retention rates, reducing costly early-stage turnover and rehiring costs.", "#1B6B3A"),
    ]

    for idx, (metric, label, desc, color) in enumerate(eb_metrics):
        # Metric value in large font with blue background
        c1 = ws_exec.cell(row=exec_row, column=2, value=metric)
        c1.font = Font(name="Calibri", bold=True, size=18, color=NAVY)
        c1.fill = accent_fill
        c1.border = thin_border
        c1.alignment = center_alignment

        c2 = ws_exec.cell(row=exec_row, column=3, value=label)
        c2.font = Font(name="Calibri", bold=True, size=11, color=NAVY)
        c2.border = thin_border
        c2.alignment = Alignment(vertical="center")

        ws_exec.merge_cells(f"D{exec_row}:F{exec_row}")
        c3 = ws_exec.cell(row=exec_row, column=4, value=desc)
        c3.font = Font(name="Calibri", size=10, color="596780")
        c3.border = thin_border
        c3.alignment = wrap_alignment

        ws_exec.row_dimensions[exec_row].height = 40

        if idx % 2 == 0:
            for c in range(3, 7):
                ws_exec.cell(row=exec_row, column=c).fill = off_white_fill
        else:
            for c in range(3, 7):
                ws_exec.cell(row=exec_row, column=c).fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
        exec_row += 1

    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    rec_cell = ws_exec.cell(row=exec_row, column=2, value="  RECOMMENDATION: Allocate 5-10% of total recruitment marketing budget to employer branding for sustained pipeline quality improvement.")
    rec_cell.font = Font(name="Calibri", bold=True, italic=True, size=10, color=NAVY)
    rec_cell.fill = accent_light_fill
    rec_cell.border = Border(left=Side(style="thick", color=ACCENT), bottom=Side(style="thin", color=ACCENT))
    for c in range(3, 8):
        ws_exec.cell(row=exec_row, column=c).fill = accent_light_fill

    # ── Quality of Hire Expected Outcomes ──
    exec_row += 3
    style_section_header(ws_exec, exec_row, 2, 7, "Quality of Hire — Expected Outcomes by Channel Type")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Not all channels deliver equal quality. This framework shows expected quality-of-hire outcomes by sourcing channel, helping optimize spend toward highest-quality pipelines.").font = Font(name="Calibri", italic=True, size=9, color="596780")
    exec_row += 2

    # Quality metrics table
    qoh_headers = ["Channel Type", "Avg Time-to-Fill", "First-Year Retention", "Quality Score", "Best For"]
    for i, h in enumerate(qoh_headers):
        cell = ws_exec.cell(row=exec_row, column=2 + i, value=h)
        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
        cell.fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
        cell.border = thin_border
        cell.alignment = center_alignment
    exec_row += 1

    # v3.5: Dynamic QoH table from budget engine channel allocations
    _CHANNEL_QUALITY_META = {
        "programmatic": ("18-25 days", "78-85%", 4, "High-volume hiring; ML optimization improves quality over time"),
        "programmatic_dsp": ("18-25 days", "78-85%", 4, "High-volume hiring; ML optimization improves quality over time"),
        "job_board": ("22-30 days", "72-80%", 3, "Broad reach; consistent pipeline for standard roles"),
        "niche_board": ("25-35 days", "82-90%", 5, "Specialized roles; highest quality match; lower volume"),
        "social_media": ("20-30 days", "70-78%", 3, "Passive candidates; employer brand-driven; younger demographics"),
        "social": ("20-30 days", "70-78%", 3, "Passive candidates; employer brand-driven; younger demographics"),
        "referral": ("15-20 days", "88-95%", 5, "Highest retention; fastest fill; limited scale"),
        "employer_branding": ("30-45 days", "85-92%", 5, "Long-term pipeline; highest quality but slower ROI"),
        "events": ("25-40 days", "75-82%", 3, "Face-to-face engagement; strong for campus/entry-level"),
        "staffing_agency": ("10-15 days", "60-70%", 2, "Fastest fill; lowest retention; highest per-hire cost"),
        "search_engine": ("20-28 days", "74-82%", 3, "Intent-driven candidates via Google/Bing job ads"),
        "display": ("25-35 days", "65-75%", 2, "Brand awareness and passive candidate re-engagement"),
    }
    _qoh_budget = data.get("_budget_allocation", {})
    _qoh_ch_allocs = _qoh_budget.get("channel_allocations", {}) if isinstance(_qoh_budget, dict) else {}
    if _qoh_ch_allocs and isinstance(_qoh_ch_allocs, dict):
        qoh_data = []
        for ch_name, ch_data in _qoh_ch_allocs.items():
            if not isinstance(ch_data, dict):
                continue
            category = ch_data.get("category", "job_board")
            meta = _CHANNEL_QUALITY_META.get(category, _CHANNEL_QUALITY_META.get("job_board"))
            label = ch_name.replace("_", " ").title()
            if "joveo" in ch_name.lower() or category in ("programmatic", "programmatic_dsp"):
                if "programmatic" not in label.lower():
                    label = f"Programmatic ({label})"
            stars = "\u2605" * meta[2] + "\u2606" * (5 - meta[2])
            qoh_data.append((label, meta[0], meta[1], stars, meta[3]))
        qoh_data.sort(key=lambda x: x[3].count("\u2605"), reverse=True)
        qoh_data = qoh_data[:8]
    else:
        # Fallback: original hardcoded table (only when budget engine produced nothing)
        qoh_data = [
            ("Programmatic (Joveo)", "18-25 days", "78-85%", "\u2605\u2605\u2605\u2605\u2606", "High-volume hiring; ML optimization improves quality over time"),
            ("Direct Job Boards", "22-30 days", "72-80%", "\u2605\u2605\u2605\u2606\u2606", "Broad reach; consistent pipeline for standard roles"),
            ("Niche/Industry Boards", "25-35 days", "82-90%", "\u2605\u2605\u2605\u2605\u2605", "Specialized roles; highest quality match; lower volume"),
            ("Social Media", "20-30 days", "70-78%", "\u2605\u2605\u2605\u2606\u2606", "Passive candidates; employer brand-driven; younger demographics"),
            ("Employee Referrals", "15-20 days", "88-95%", "\u2605\u2605\u2605\u2605\u2605", "Highest retention; fastest fill; limited scale"),
            ("Employer Branding", "30-45 days", "85-92%", "\u2605\u2605\u2605\u2605\u2605", "Long-term pipeline; highest quality but slower ROI"),
            ("Events & Career Fairs", "25-40 days", "75-82%", "\u2605\u2605\u2605\u2606\u2606", "Face-to-face engagement; strong for campus/entry-level"),
            ("Staffing Agencies", "10-15 days", "60-70%", "\u2605\u2605\u2606\u2606\u2606", "Fastest fill; lowest retention; highest per-hire cost"),
        ]

    for idx, (ch_type, ttf, retention, quality, best_for) in enumerate(qoh_data):
        c1 = ws_exec.cell(row=exec_row, column=2, value=ch_type)
        is_joveo = "Joveo" in ch_type
        c1.font = Font(name="Calibri", bold=True, size=10, color="2E75B6" if is_joveo else "1B2A4A")
        c1.border = thin_border

        c2 = ws_exec.cell(row=exec_row, column=3, value=ttf)
        c2.font = Font(name="Calibri", size=10, color="1B2A4A")
        c2.border = thin_border
        c2.alignment = center_alignment

        c3 = ws_exec.cell(row=exec_row, column=4, value=retention)
        c3.font = Font(name="Calibri", bold=True, size=10, color="1B6B3A")
        c3.border = thin_border
        c3.alignment = center_alignment

        c4 = ws_exec.cell(row=exec_row, column=5, value=quality)
        c4.font = Font(name="Calibri", size=12, color=ACCENT)
        c4.border = thin_border
        c4.alignment = center_alignment

        c5 = ws_exec.cell(row=exec_row, column=6, value=best_for)
        c5.font = Font(name="Calibri", italic=True, size=9, color="596780")
        c5.border = thin_border
        c5.alignment = wrap_alignment

        if is_joveo:
            for c in range(2, 7):
                ws_exec.cell(row=exec_row, column=c).fill = accent_light_fill
        elif idx % 2 == 0:
            for c in range(2, 7):
                ws_exec.cell(row=exec_row, column=c).fill = off_white_fill
        exec_row += 1

    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    joveo_rec = ws_exec.cell(row=exec_row, column=2, value="  Joveo's programmatic approach combines the speed of automation with quality optimization (CPQA), delivering the best balance of time-to-fill, retention, and cost efficiency.")
    joveo_rec.font = Font(name="Calibri", bold=True, italic=True, size=10, color=NAVY)
    joveo_rec.fill = accent_light_fill
    joveo_rec.border = Border(left=Side(style="thick", color=ACCENT))
    for c in range(3, 8):
        ws_exec.cell(row=exec_row, column=c).fill = accent_light_fill

    # ── Quality & ROI Metrics (2x2 card grid) ──
    exec_row += 3
    style_section_header(ws_exec, exec_row, 2, 7, "Quality & ROI Metrics")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Key performance indicators estimated from your channel mix, industry benchmarks, and campaign parameters.").font = Font(name="Calibri", italic=True, size=9, color="596780")
    exec_row += 2

    # Calculate estimated metrics -- v3.5: prefer budget engine total_projected
    _qm_budget = data.get("_budget_allocation", {})
    _qm_total_proj = _qm_budget.get("total_projected", {}) if isinstance(_qm_budget, dict) else {}
    if isinstance(_qm_total_proj, dict) and _qm_total_proj.get("hires", 0) > 0:
        # Budget engine projections (highest accuracy)
        est_total_reach = est_impressions  # Keep impressions from earlier calculation
        est_hires = int(_qm_total_proj.get("hires", 0))
        est_applies = int(_qm_total_proj.get("applications", 0))
        _qm_cpa = _qm_total_proj.get("cost_per_application")
        est_cpa = round(float(_qm_cpa), 2) if isinstance(_qm_cpa, (int, float)) and _qm_cpa > 0 else round(budget_midpoint / max(est_applies, 1), 2)
        _qm_cph = _qm_total_proj.get("cost_per_hire")
        est_cph_display = round(float(_qm_cph), 0) if isinstance(_qm_cph, (int, float)) and _qm_cph > 0 else round(budget_midpoint / max(est_hires, 1), 0)
        _qm_ch_allocs = _qm_budget.get("channel_allocations", {})
        num_channels = len([c for c in _qm_ch_allocs.values()
                            if isinstance(c, dict) and c.get("percentage", 0) > 0]) if isinstance(_qm_ch_allocs, dict) else (regional_count + niche_count + global_count)
    else:
        # Fallback: original local calculations
        est_total_reach = est_impressions
        num_channels = regional_count + niche_count + global_count
        est_hires = funnel_stages[-1][1] if funnel_stages else 0
        est_applies = funnel_stages[2][1] if len(funnel_stages) > 2 else 0
        est_cpa = round(budget_midpoint / max(est_applies, 1), 2) if est_applies > 0 else 0
        est_cph_display = round(budget_midpoint / max(est_hires, 1), 0) if est_hires > 0 else avg_cph
    channel_diversity = min(round(num_channels / 10 * 100, 0), 100)  # score out of 100

    quality_metrics = [
        (f"{est_total_reach:,.0f}", "Estimated Total Reach", "Total impressions across all selected channels"),
        (f"{display_currency}{est_cpa:,.2f}", "Est. Cost Per Application", "Budget-calibrated CPA based on industry benchmarks"),
        (f"{display_currency}{est_cph_display:,.0f}", "Est. Cost Per Hire", f"Based on {client_industry.replace('_', ' ').title()} industry benchmarks"),
        (f"{est_hires:,}", "Projected Hires", f"Across {num_channels} channels in {len(normalized) if normalized else 0} categories"),
    ]

    # 2x2 grid layout: row 1 has metrics 0,1; row 2 has metrics 2,3
    card_border = Border(
        left=Side(style="thin", color=WARM_GRAY),
        right=Side(style="thin", color=WARM_GRAY),
        top=Side(style="thin", color=WARM_GRAY),
        bottom=Side(style="thin", color=WARM_GRAY),
    )
    for grid_row_idx in range(2):
        # Large number row
        for grid_col_idx in range(2):
            m_idx = grid_row_idx * 2 + grid_col_idx
            if m_idx >= len(quality_metrics):
                break
            val, lbl, ctx = quality_metrics[m_idx]
            col_start = 2 + grid_col_idx * 3  # B or E

            # Merge for large metric number
            ws_exec.merge_cells(start_row=exec_row, start_column=col_start, end_row=exec_row, end_column=col_start + 2)
            num_cell = ws_exec.cell(row=exec_row, column=col_start, value=val)
            num_cell.font = Font(name="Calibri", bold=True, size=22, color=NAVY)
            num_cell.fill = accent_fill
            num_cell.alignment = Alignment(horizontal="center", vertical="center")
            num_cell.border = card_border
            for cc in range(col_start + 1, col_start + 3):
                ws_exec.cell(row=exec_row, column=cc).fill = accent_fill
                ws_exec.cell(row=exec_row, column=cc).border = card_border
        ws_exec.row_dimensions[exec_row].height = 40
        exec_row += 1

        # Label row
        for grid_col_idx in range(2):
            m_idx = grid_row_idx * 2 + grid_col_idx
            if m_idx >= len(quality_metrics):
                break
            val, lbl, ctx = quality_metrics[m_idx]
            col_start = 2 + grid_col_idx * 3

            ws_exec.merge_cells(start_row=exec_row, start_column=col_start, end_row=exec_row, end_column=col_start + 2)
            lbl_cell = ws_exec.cell(row=exec_row, column=col_start, value=lbl)
            lbl_cell.font = Font(name="Calibri", bold=True, size=11, color="FFFFFF")
            lbl_cell.fill = navy_fill
            lbl_cell.alignment = Alignment(horizontal="center", vertical="center")
            lbl_cell.border = card_border
            for cc in range(col_start + 1, col_start + 3):
                ws_exec.cell(row=exec_row, column=cc).fill = navy_fill
                ws_exec.cell(row=exec_row, column=cc).border = card_border
        exec_row += 1

        # Context row
        for grid_col_idx in range(2):
            m_idx = grid_row_idx * 2 + grid_col_idx
            if m_idx >= len(quality_metrics):
                break
            val, lbl, ctx = quality_metrics[m_idx]
            col_start = 2 + grid_col_idx * 3

            ws_exec.merge_cells(start_row=exec_row, start_column=col_start, end_row=exec_row, end_column=col_start + 2)
            ctx_cell = ws_exec.cell(row=exec_row, column=col_start, value=ctx)
            ctx_cell.font = Font(name="Calibri", italic=True, size=9, color="596780")
            ctx_cell.fill = off_white_fill
            ctx_cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            ctx_cell.border = card_border
            for cc in range(col_start + 1, col_start + 3):
                ws_exec.cell(row=exec_row, column=cc).fill = off_white_fill
                ws_exec.cell(row=exec_row, column=cc).border = card_border
        exec_row += 1

        # Spacer between card rows
        if grid_row_idx == 0:
            exec_row += 1

    # ── Peer Industry Benchmark Comparison ──
    exec_row += 3
    style_section_header(ws_exec, exec_row, 2, 7, "Peer Industry Benchmark Comparison — How Your Industry Compares")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Your industry's recruitment marketing costs compared to peer industries and the all-industry average. Helps identify relative competitiveness and budget calibration.").font = Font(name="Calibri", italic=True, size=9, color="596780")
    exec_row += 2

    # Peer comparison data (all North America for consistency)
    peer_industries = {
        "healthcare_medical": {"label": "Healthcare", "cpa": "$35 - $85", "cpc": "$0.90 - $3.50", "cph": "$9K - $12K", "apply_rate": "3.2 - 4.5%", "difficulty": "\u2605\u2605\u2605\u2605\u2605"},
        "tech_engineering": {"label": "Technology", "cpa": "$25 - $75", "cpc": "$1.20 - $4.50", "cph": "$6K - $22K", "apply_rate": "6.41%", "difficulty": "\u2605\u2605\u2605\u2605\u2606"},
        "retail_consumer": {"label": "Retail", "cpa": "$8 - $21", "cpc": "$0.25 - $1.00", "cph": "$2.7K - $4K", "apply_rate": "4.5 - 5.8%", "difficulty": "\u2605\u2605\u2606\u2606\u2606"},
        "finance_banking": {"label": "Finance", "cpa": "$21 - $65", "cpc": "$0.90 - $3.50", "cph": "$5K - $12K", "apply_rate": "5.0 - 6.0%", "difficulty": "\u2605\u2605\u2605\u2606\u2606"},
        "logistics_supply_chain": {"label": "Logistics", "cpa": "$15 - $52", "cpc": "$0.40 - $1.80", "cph": "$4.5K - $8K", "apply_rate": "4.0 - 5.2%", "difficulty": "\u2605\u2605\u2605\u2606\u2606"},
        "hospitality_travel": {"label": "Hospitality", "cpa": "$8 - $25", "cpc": "$0.22 - $1.00", "cph": "$2.5K - $4K", "apply_rate": "4.0 - 5.0%", "difficulty": "\u2605\u2605\u2605\u2606\u2606"},
        "pharma_biotech": {"label": "Pharma", "cpa": "$40 - $110", "cpc": "$1.50 - $5.00", "cph": "$8K - $18K", "apply_rate": "3.8 - 5.2%", "difficulty": "\u2605\u2605\u2605\u2605\u2605"},
        "general_entry_level": {"label": "General", "cpa": "$10 - $25", "cpc": "$0.35 - $1.30", "cph": "$2K - $4.7K", "apply_rate": "5.5 - 6.1%", "difficulty": "\u2605\u2605\u2606\u2606\u2606"},
    }

    # Table headers with split-panel look
    peer_headers = ["Industry", f"Avg CPA ({display_currency_code})", f"Avg CPC ({display_currency_code})", "Est. CPH", "Apply Rate", "Difficulty", "vs. Avg"]
    for i, h in enumerate(peer_headers):
        cell = ws_exec.cell(row=exec_row, column=2 + i, value=h)
        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
        cell.fill = navy_fill
        cell.border = thin_border
        cell.alignment = center_alignment
    exec_row += 1

    # Difficulty numeric map for delta comparison
    difficulty_score = {
        "\u2605\u2605\u2605\u2605\u2605": 5, "\u2605\u2605\u2605\u2605\u2606": 4,
        "\u2605\u2605\u2605\u2606\u2606": 3, "\u2605\u2605\u2606\u2606\u2606": 2,
        "\u2605\u2606\u2606\u2606\u2606": 1,
    }
    avg_difficulty = 3  # All-industry average reference

    for ind_key, pdata in peer_industries.items():
        is_client = ind_key == client_industry

        if is_client:
            label_val = f"\u25B6 {pdata['label']} (YOUR INDUSTRY)"
        else:
            label_val = pdata["label"]

        c1 = ws_exec.cell(row=exec_row, column=2, value=label_val)
        c1.border = thin_border

        if is_client:
            # Client row: blue highlight with navy text
            c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
            c1.fill = accent_fill
        else:
            c1.font = Font(name="Calibri", bold=False, size=10, color=NAVY)

        fields = [pdata["cpa"], pdata["cpc"], pdata["cph"], pdata["apply_rate"], pdata["difficulty"]]
        for fi, fval in enumerate(fields):
            cell = ws_exec.cell(row=exec_row, column=3 + fi, value=fval)
            cell.border = thin_border
            cell.alignment = center_alignment
            if is_client:
                cell.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                cell.fill = accent_fill
            else:
                cell.font = Font(name="Calibri", size=10, color=NAVY)

        # Delta column - compare difficulty to average
        d_score = difficulty_score.get(pdata["difficulty"], 3)
        if d_score < avg_difficulty:
            delta_text = f"\u25B2 Easier"
            delta_color = GREEN_GOOD
            delta_fill_val = green_fill
        elif d_score > avg_difficulty:
            delta_text = f"\u25BC Harder"
            delta_color = AMBER_WARN
            delta_fill_val = amber_fill
        else:
            delta_text = "\u2014 Average"
            delta_color = "666666"
            delta_fill_val = off_white_fill

        delta_cell = ws_exec.cell(row=exec_row, column=8, value=delta_text)
        delta_cell.font = Font(name="Calibri", bold=True, size=10, color=delta_color)
        delta_cell.border = thin_border
        delta_cell.alignment = center_alignment
        delta_cell.fill = delta_fill_val

        # Non-client alternating rows
        if not is_client:
            row_idx_in_peers = list(peer_industries.keys()).index(ind_key)
            if row_idx_in_peers % 2 == 0:
                for c in range(2, 8):
                    if ws_exec.cell(row=exec_row, column=c).fill == PatternFill():
                        ws_exec.cell(row=exec_row, column=c).fill = off_white_fill

        exec_row += 1

    # All-industry average row
    ws_exec.cell(row=exec_row, column=2, value="ALL-INDUSTRY AVG").font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
    ws_exec.cell(row=exec_row, column=2).fill = navy_fill
    ws_exec.cell(row=exec_row, column=2).border = thin_border
    avg_values = ["$10 - $45", "$0.35 - $2.50", "$4.7K (SHRM)", "5.5 - 6.1%", "\u2605\u2605\u2605\u2606\u2606", "\u2014"]
    for fi, fval in enumerate(avg_values):
        cell = ws_exec.cell(row=exec_row, column=3 + fi, value=fval)
        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
        cell.fill = navy_fill
        cell.border = thin_border
        cell.alignment = center_alignment
    exec_row += 1

    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Your industry row is highlighted in blue. \u25B2 = easier than average (green), \u25BC = harder than average (amber). Use this to calibrate budget expectations.").font = Font(name="Calibri", italic=True, size=8, color="999999")

    # Move Executive Summary to first position
    wb.move_sheet("Executive Summary", offset=-(len(wb.sheetnames) - 1))

    # ── Sheet 2: Market Trends ──
    ws_trends = wb.create_sheet("Market Trends")
    ws_trends.sheet_properties.tabColor = "2E75B6"
    ws_trends.column_dimensions["A"].width = 5
    ws_trends.column_dimensions["B"].width = 35
    for i, loc in enumerate(locations):
        ws_trends.column_dimensions[get_column_letter(3 + i)].width = 55

    ws_trends.merge_cells(start_row=2, start_column=2, end_row=2, end_column=2 + len(locations))
    ws_trends["B2"].value = "Market Trends & Labor Market Analysis"
    ws_trends["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
    ws_trends["B2"].border = accent_bottom_border

    row = 4
    headers = ["Market Trends Factor"] + locations
    for i, h in enumerate(headers):
        cell = ws_trends.cell(row=row, column=2 + i, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_alignment
        cell.border = thin_border

    # USE RESEARCH MODULE for real market trends
    # Pass BLS enrichment salary data so roles get real salary ranges instead of generic fallback
    enrichment_salary_data = data.get("_enriched", {}).get("salary_data", {})
    market_trends = research.get_market_trends(locations, industry, roles, enrichment_salary_data=enrichment_salary_data)

    for trend in market_trends:
        row += 1
        style_body_cell(ws_trends, row, 2, trend.get("factor", ""))
        ws_trends.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
        for i, loc in enumerate(locations):
            desc = trend.get("descriptions", {}).get(loc, "")
            style_body_cell(ws_trends, row, 3 + i, desc)
        # Alternate row shading
        if (row % 2) == 0:
            for c in range(2, 3 + len(locations)):
                ws_trends.cell(row=row, column=c).fill = accent_fill

    # Competitor section
    row += 3
    style_section_header(ws_trends, row, 2, 2 + len(locations), "Competitor Analysis")

    row += 2
    comp_headers = ["Competitor Category", "Key Competitors", "Hiring Focus & Threat Level"]
    for i, h in enumerate(comp_headers):
        cell = ws_trends.cell(row=row, column=2 + i, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    # Widen columns for competitor detail
    ws_trends.column_dimensions[get_column_letter(3)].width = 70
    ws_trends.column_dimensions[get_column_letter(4)].width = 70

    # USE RESEARCH MODULE for real competitor data
    company_name = data.get("company_name", "") or data.get("client_name", "")
    competitors = research.get_competitors(industry, locations, company_name=company_name)

    for comp in competitors:
        row += 1
        style_body_cell(ws_trends, row, 2, comp.get("category", ""))
        ws_trends.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
        style_body_cell(ws_trends, row, 3, comp.get("competitors", ""))
        style_body_cell(ws_trends, row, 4, comp.get("threat", ""))
        # Row height for long text
        ws_trends.row_dimensions[row].height = 80

    # Client-specified competitors section — per-competitor differentiated intelligence
    # CRITICAL: Filter out self from client-specified competitors
    _client_name_lower = (data.get("client_name", "") or "").lower().strip()
    if _client_name_lower and client_competitors:
        client_competitors = [c for c in client_competitors if _client_name_lower not in c.lower()]
    if client_competitors:
        comp_intel = research.get_client_competitor_intelligence(client_competitors, industry)

        row += 3
        style_section_header(ws_trends, row, 2, 5, "Client-Identified Competitor Intelligence")
        row += 1
        ws_trends.cell(row=row, column=2, value="Detailed competitive intelligence for each client-specified competitor, including hiring channels, employer brand analysis, and strategic recommendations.").font = Font(name="Calibri", italic=True, size=10, color="596780")
        ws_trends.merge_cells(start_row=row, start_column=2, end_row=row, end_column=5)
        row += 2

        ci_headers = ["Competitor", "Hiring Channels & Brand", "Recruitment Strategies", "Strategic Recommendation"]
        for i, h in enumerate(ci_headers):
            cell = ws_trends.cell(row=row, column=2 + i, value=h)
            cell.font = header_font
            cell.fill = PatternFill(start_color="CE9047", end_color="CE9047", fill_type="solid")
            cell.alignment = center_alignment
            cell.border = thin_border
        ws_trends.column_dimensions[get_column_letter(5)].width = 60

        for ci in comp_intel:
            row += 1
            # Competitor name + size
            comp_label = ci["competitor"]
            if ci.get("company_size") and "Research" not in ci["company_size"]:
                comp_label += f"\n({ci['company_size']})"
            style_body_cell(ws_trends, row, 2, comp_label)
            ws_trends.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)

            # Channels + brand + Glassdoor
            channels_brand = f"Channels: {ci['primary_hiring_channels']}\n\nBrand: {ci['employer_brand_strength']}\n\nGlassdoor: {ci['glassdoor_rating']}"
            if ci.get("talent_focus"):
                channels_brand += f"\n\nTalent Focus: {ci['talent_focus']}"
            style_body_cell(ws_trends, row, 3, channels_brand)

            # Recruitment strategies
            style_body_cell(ws_trends, row, 4, ci.get("known_recruitment_strategies", ""))

            # Strategic recommendation
            style_body_cell(ws_trends, row, 5, ci.get("strategic_recommendation", ""))

            ws_trends.row_dimensions[row].height = 120

    # ── Sheet: Labour Market Intelligence ──
    labour_data = research.get_labour_market_intelligence(industry, locations)
    ws_labour = wb.create_sheet("Labour Market Intelligence")
    ws_labour.sheet_properties.tabColor = "438765"
    ws_labour.column_dimensions["A"].width = 5
    ws_labour.column_dimensions["B"].width = 40
    ws_labour.column_dimensions["C"].width = 60
    ws_labour.column_dimensions["D"].width = 60

    ws_labour.merge_cells("B2:D2")
    ws_labour["B2"].value = "Labour Market Intelligence"
    ws_labour["B2"].font = Font(name="Calibri", bold=True, size=18, color="1B2A4A")

    lm_row = 3
    ws_labour.cell(row=lm_row, column=2, value="BLS/JOLTS-style curated data for the selected industry and target locations. Use this intelligence to inform recruitment strategy, budget allocation, and candidate messaging.").font = Font(name="Calibri", italic=True, size=10, color="596780")
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")

    # ── Section 1: National Economic Summary ──
    lm_row += 2
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
    ws_labour.cell(row=lm_row, column=2, value="National Economic Snapshot (US)").font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    ws_labour.cell(row=lm_row, column=2).fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
    lm_row += 1

    national = labour_data.get("national_summary", {})
    national_items = [
        ("Total Nonfarm Job Openings", national.get("total_nonfarm_openings", "")),
        ("National Unemployment Rate", national.get("national_unemployment_rate", "")),
        ("Labour Force Participation Rate", national.get("labour_force_participation", "")),
        ("U-6 Underemployment Rate", national.get("u6_underemployment", "")),
        ("National Quits Rate (JOLTS)", national.get("national_quits_rate", "")),
        ("National Openings Rate (JOLTS)", national.get("national_openings_rate", "")),
        ("Avg Hourly Earnings (All Workers)", national.get("avg_hourly_earnings_all", "")),
        ("Wage Growth YoY", national.get("avg_hourly_earnings_yoy_change", "")),
        ("Jobs-to-Unemployed Ratio", national.get("jobs_to_unemployed_ratio", "")),
    ]
    for label, value in national_items:
        ws_labour.cell(row=lm_row, column=2, value=label).font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
        ws_labour.cell(row=lm_row, column=2).border = thin_border
        ws_labour.cell(row=lm_row, column=3, value=value).font = body_font
        ws_labour.cell(row=lm_row, column=3).border = thin_border
        lm_row += 1

    # ── Section 2: Industry-Specific Metrics ──
    lm_row += 1
    ind_metrics = labour_data.get("industry_metrics", {})
    sector_name = ind_metrics.get("sector_name", industry_label_map.get(industry, industry))
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
    ws_labour.cell(row=lm_row, column=2, value=f"Industry Focus: {sector_name}").font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    ws_labour.cell(row=lm_row, column=2).fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
    lm_row += 1

    ind_items = [
        ("BLS Sector Code", ind_metrics.get("bls_sector_code", "")),
        ("Total US Employment", ind_metrics.get("total_employment_us", "")),
        ("Projected Growth (2024-2034)", ind_metrics.get("projected_growth_2024_2034", "")),
        ("Annual Job Openings", ind_metrics.get("annual_openings", "")),
        ("Median Annual Wage", ind_metrics.get("median_annual_wage", "")),
        ("JOLTS Openings Rate", ind_metrics.get("job_openings_rate_jolts", "")),
        ("JOLTS Quits Rate", ind_metrics.get("quits_rate_jolts", "")),
        ("JOLTS Hires Rate", ind_metrics.get("hires_rate_jolts", "")),
        ("JOLTS Layoffs Rate", ind_metrics.get("layoffs_rate_jolts", "")),
        ("Avg Time to Fill Vacancy", ind_metrics.get("vacancy_fill_time_avg", "")),
        ("Talent Shortage Severity", ind_metrics.get("talent_shortage_severity", "")),
        ("Wage Growth YoY", ind_metrics.get("wage_growth_yoy", "")),
        ("Unionization Rate", ind_metrics.get("unionization_rate", "")),
        ("Remote Work %", ind_metrics.get("remote_work_pct", "")),
    ]
    for label, value in ind_items:
        ws_labour.cell(row=lm_row, column=2, value=label).font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
        ws_labour.cell(row=lm_row, column=2).border = thin_border
        c = ws_labour.cell(row=lm_row, column=3, value=value)
        c.font = body_font
        c.border = thin_border
        c.alignment = wrap_alignment
        # Highlight severity
        if "CRITICAL" in str(value).upper():
            c.font = Font(name="Calibri", bold=True, size=10, color="CC0000")
        elif "HIGH" in str(value).upper() and "shortage" in label.lower():
            c.font = Font(name="Calibri", bold=True, size=10, color="CE9047")
        lm_row += 1

    # ── Section 3: Key Industry Trends ──
    lm_row += 1
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
    ws_labour.cell(row=lm_row, column=2, value="Key Industry Trends & Hiring Implications").font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
    ws_labour.cell(row=lm_row, column=2).fill = PatternFill(start_color="438765", end_color="438765", fill_type="solid")
    lm_row += 1

    for i, trend in enumerate(ind_metrics.get("key_trends", []), 1):
        ws_labour.cell(row=lm_row, column=2, value=f"Trend {i}").font = Font(name="Calibri", bold=True, size=10, color="438765")
        ws_labour.cell(row=lm_row, column=2).border = thin_border
        c = ws_labour.cell(row=lm_row, column=3, value=trend)
        c.font = body_font
        c.alignment = wrap_alignment
        c.border = thin_border
        ws_labour.merge_cells(f"C{lm_row}:D{lm_row}")
        ws_labour.row_dimensions[lm_row].height = 35
        lm_row += 1

    # ── Section 4: Location-Specific Context ──
    loc_contexts = labour_data.get("location_contexts", [])
    if loc_contexts:
        lm_row += 1
        ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
        ws_labour.cell(row=lm_row, column=2, value="Location-Specific Labour Market Context").font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
        ws_labour.cell(row=lm_row, column=2).fill = PatternFill(start_color="CE9047", end_color="CE9047", fill_type="solid")
        lm_row += 1

        loc_headers = ["Location", "Unemployment", "Median Salary", "Population", "COLI", "Notes"]
        for i, h in enumerate(loc_headers):
            cell = ws_labour.cell(row=lm_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color="CE9047", end_color="CE9047", fill_type="solid")
            cell.alignment = center_alignment
            cell.border = thin_border
        ws_labour.column_dimensions["E"].width = 15
        ws_labour.column_dimensions["F"].width = 15
        ws_labour.column_dimensions["G"].width = 60
        lm_row += 1

        for lctx in loc_contexts:
            style_body_cell(ws_labour, lm_row, 2, lctx.get("location", ""))
            ws_labour.cell(row=lm_row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_labour, lm_row, 3, lctx.get("unemployment_rate", ""))
            style_body_cell(ws_labour, lm_row, 4, lctx.get("median_salary", ""))
            style_body_cell(ws_labour, lm_row, 5, lctx.get("population", ""))
            style_body_cell(ws_labour, lm_row, 6, str(lctx.get("coli", "")))
            style_body_cell(ws_labour, lm_row, 7, lctx.get("context_note", ""))
            ws_labour.row_dimensions[lm_row].height = 35
            lm_row += 1

    # Real-time salary data from BLS
    enriched = data.get("_enriched", {})
    salary_data = enriched.get("salary_data", {})
    if salary_data:
        lm_row += 2
        style_section_header(ws_labour, lm_row, 2, 7, "Real-Time Salary Benchmarks (BLS)")
        lm_row += 1
        # Headers
        for ci, hdr in enumerate(["Role", "Mean / Median Salary", "10th %ile", "90th %ile", "Source"], start=2):
            c = ws_labour.cell(row=lm_row, column=ci, value=hdr)
            c.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            c.fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
            c.alignment = center_alignment
        lm_row += 1
        for role_name, sdata in salary_data.items():
            ws_labour.cell(row=lm_row, column=2, value=role_name).font = body_font
            # Show median if available, otherwise mean
            _med = sdata.get('median') or sdata.get('mean', 0)
            _label = "Median" if sdata.get('median') else "Mean"
            ws_labour.cell(row=lm_row, column=3, value=f"${_med:,.0f} ({_label})" if isinstance(_med, (int, float)) else str(_med)).font = Font(name="Calibri", size=10, bold=True, color="2E7D32")
            _p10 = sdata.get('p10', 0)
            ws_labour.cell(row=lm_row, column=4, value=f"${_p10:,.0f}" if isinstance(_p10, (int, float)) else str(_p10)).font = body_font
            _p90 = sdata.get('p90', 0)
            ws_labour.cell(row=lm_row, column=5, value=f"${_p90:,.0f}" if isinstance(_p90, (int, float)) else str(_p90)).font = body_font
            ws_labour.cell(row=lm_row, column=6, value=sdata.get("source", "BLS")).font = Font(name="Calibri", italic=True, size=9, color="596780")
            for ci in range(2, 7):
                ws_labour.cell(row=lm_row, column=ci).border = thin_border
                ws_labour.cell(row=lm_row, column=ci).alignment = center_alignment
            lm_row += 1

    location_demos = enriched.get("location_demographics", {})
    if location_demos:
        lm_row += 2
        style_section_header(ws_labour, lm_row, 2, 7, "Location Demographics (Live Data)")
        lm_row += 1
        for ci, hdr in enumerate(["Location", "Population", "Median Income", "Geo Level", "Matched Place", "Source"], start=2):
            c = ws_labour.cell(row=lm_row, column=ci, value=hdr)
            c.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            c.fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
            c.alignment = center_alignment
        lm_row += 1
        for loc_name, ldata in location_demos.items():
            ws_labour.cell(row=lm_row, column=2, value=loc_name).font = body_font
            pop = ldata.get("population")
            ws_labour.cell(row=lm_row, column=3, value=f"{pop:,.0f}" if isinstance(pop, (int, float)) else ("N/A" if not pop else str(pop))).font = body_font
            inc = ldata.get("median_income")
            ws_labour.cell(row=lm_row, column=4, value=f"${inc:,.0f}" if isinstance(inc, (int, float)) else ("N/A" if not inc else str(inc))).font = body_font
            # Show the geographic level (city, metro, state/country) so users know what the population number represents
            geo_level = ldata.get("geo_level", "Unknown")
            ws_labour.cell(row=lm_row, column=5, value=geo_level).font = body_font
            # Show the matched place name (from Census ACS state_name, or WorldBank country)
            matched_place = ldata.get("matched_place", "") or ldata.get("state_name", "") or ldata.get("country", "")
            ws_labour.cell(row=lm_row, column=6, value=matched_place).font = body_font
            ws_labour.cell(row=lm_row, column=7, value=ldata.get("source", "")).font = Font(name="Calibri", italic=True, size=9, color="596780")
            for ci in range(2, 8):
                ws_labour.cell(row=lm_row, column=ci).border = thin_border
                ws_labour.cell(row=lm_row, column=ci).alignment = center_alignment
            lm_row += 1

    global_indicators = enriched.get("global_indicators", {})
    if global_indicators:
        lm_row += 2
        style_section_header(ws_labour, lm_row, 2, 7, "Global Economic Indicators (World Bank)")
        lm_row += 1
        for ci, hdr in enumerate(["Country", "Unemployment Rate", "GDP Growth", "Labor Force", "Source"], start=2):
            c = ws_labour.cell(row=lm_row, column=ci, value=hdr)
            c.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            c.fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
            c.alignment = center_alignment
        lm_row += 1
        for country, gdata in global_indicators.items():
            ws_labour.cell(row=lm_row, column=2, value=country).font = body_font
            unemp = gdata.get("unemployment_rate")
            ws_labour.cell(row=lm_row, column=3, value=f"{unemp}%" if unemp else "N/A").font = body_font
            gdp = gdata.get("gdp_growth")
            ws_labour.cell(row=lm_row, column=4, value=f"{gdp}%" if gdp else "N/A").font = body_font
            lf = gdata.get("labor_force")
            ws_labour.cell(row=lm_row, column=5, value=f"{lf:,.0f}" if isinstance(lf, (int, float)) else ("N/A" if not lf else str(lf))).font = body_font
            ws_labour.cell(row=lm_row, column=6, value=gdata.get("source", "World Bank")).font = Font(name="Calibri", italic=True, size=9, color="596780")
            for ci in range(2, 7):
                ws_labour.cell(row=lm_row, column=ci).border = thin_border
                ws_labour.cell(row=lm_row, column=ci).alignment = center_alignment
            lm_row += 1


    # ── Section: Hiring Compliance & Regulatory Notes ──
    hiring_regs = research.get_hiring_regulations(locations)
    if hiring_regs:
        lm_row += 2
        ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
        ws_labour.cell(row=lm_row, column=2, value="Hiring Compliance & Regulatory Notes").font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
        ws_labour.cell(row=lm_row, column=2).fill = PatternFill(start_color="CC0000", end_color="CC0000", fill_type="solid")
        lm_row += 1

        ws_labour.cell(row=lm_row, column=2, value="Applicable hiring regulations by state/jurisdiction. Ensure job postings and hiring processes comply with all listed requirements.").font = Font(name="Calibri", italic=True, size=9, color="596780")
        ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
        lm_row += 1

        for reg_entry in hiring_regs:
            reg_state = reg_entry.get("state", "")
            reg_loc = reg_entry.get("location", "")
            reg_list = reg_entry.get("regulations", [])
            compliance_note = reg_entry.get("compliance_note", "")

            ws_labour.cell(row=lm_row, column=2, value=f"{reg_state} ({reg_loc})").font = Font(name="Calibri", bold=True, size=11, color="CC0000")
            ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
            lm_row += 1

            for reg in reg_list:
                ws_labour.cell(row=lm_row, column=2, value=f"  \u2022  {reg}").font = Font(name="Calibri", size=10, color="333333")
                ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
                lm_row += 1

            ws_labour.cell(row=lm_row, column=2, value=compliance_note).font = Font(name="Calibri", italic=True, size=9, color="596780")
            ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")
            lm_row += 1

    lm_row += 1
    ws_labour.cell(row=lm_row, column=2, value="Data Sources: BLS Occupational Employment & Wage Statistics, JOLTS (Job Openings & Labor Turnover Survey), BLS Employment Projections, industry reports. Curated reference data as of 2024.").font = Font(name="Calibri", italic=True, size=9, color="888888")
    ws_labour.merge_cells(f"B{lm_row}:D{lm_row}")

    # ── Sheet 3: Channel Strategy ──
    ws_strategy = wb.create_sheet("Channel Strategy")
    ws_strategy.sheet_properties.tabColor = "2E75B6"
    ws_strategy.column_dimensions["A"].width = 5
    ws_strategy.column_dimensions["B"].width = 30
    ws_strategy.column_dimensions["C"].width = 50
    ws_strategy.column_dimensions["D"].width = 50
    ws_strategy.column_dimensions["E"].width = 20
    ws_strategy.column_dimensions["F"].width = 45

    ws_strategy.merge_cells("B2:F2")
    ws_strategy["B2"].value = "Channel Strategy"
    ws_strategy["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
    ws_strategy["B2"].border = accent_bottom_border

    row = 4
    strat_headers = ["Channel", "Reasoning", "How to Use", "KPIs / Metrics", "Niche / Non-Traditional Channels"]
    for i, h in enumerate(strat_headers):
        cell = ws_strategy.cell(row=row, column=2 + i, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_alignment
        cell.border = thin_border

    # Look up industry-specific niche channels for this plan
    _niche_channels_for_industry = INDUSTRY_NICHE_CHANNELS.get(industry, INDUSTRY_NICHE_CHANNELS.get("general_entry_level", []))

    selected_strategies = data.get("channel_strategies", [])
    if not selected_strategies:
        all_strats = db.get("channel_strategies", {})
        selected_strategies = all_strats.get("awareness", []) + all_strats.get("hiring", [])

    _niche_idx = 0
    for strat in selected_strategies:
        row += 1
        style_body_cell(ws_strategy, row, 2, strat.get("channel", ""))
        ws_strategy.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
        style_body_cell(ws_strategy, row, 3, strat.get("reasoning", ""))
        style_body_cell(ws_strategy, row, 4, strat.get("usage", ""))
        style_body_cell(ws_strategy, row, 5, strat.get("kpis", "Reach, Engagement, CTR, Conversions"))
        # Populate Niche / Non-Traditional Channels column with 2-3 industry-specific niche channels per row
        if _niche_channels_for_industry and _niche_idx < len(_niche_channels_for_industry):
            _end_idx = min(_niche_idx + 3, len(_niche_channels_for_industry))
            _niche_slice = _niche_channels_for_industry[_niche_idx:_end_idx]
            style_body_cell(ws_strategy, row, 6, ", ".join(_niche_slice))
            ws_strategy.cell(row=row, column=6).font = Font(name="Calibri", size=10, color="0A66C9")
            _niche_idx = _end_idx

    # ── Industry-Specific Niche Channel Recommendations ──
    row += 3
    style_section_header(ws_strategy, row, 2, 6, "Industry-Specific Niche Channel Recommendations")
    row += 1
    ws_strategy.merge_cells(f"B{row}:F{row}")
    ws_strategy.cell(row=row, column=2, value=f"Recommended niche and non-traditional channels for the {data.get('industry_label', industry.replace('_', ' ').title())} industry. These specialized platforms offer higher-quality candidates for targeted roles.").font = Font(name="Calibri", italic=True, size=10, color="596780")
    row += 1

    _niche_rec_headers = ["Channel", "Type", "Best For"]
    for i, h in enumerate(_niche_rec_headers):
        cell = ws_strategy.cell(row=row, column=2 + i, value=h)
        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
        cell.fill = PatternFill(start_color="0A66C9", end_color="0A66C9", fill_type="solid")
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    for _nch in _niche_channels_for_industry:
        style_body_cell(ws_strategy, row, 2, _nch)
        ws_strategy.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10, color="0A66C9")
        style_body_cell(ws_strategy, row, 3, "Niche / Specialized Board")
        style_body_cell(ws_strategy, row, 4, f"Specialized {data.get('industry_label', industry.replace('_', ' ').title())} talent")
        row += 1

    # ── Bar Chart: Channel Effectiveness Score ──
    row += 3
    style_section_header(ws_strategy, row, 2, 5, "Channel Effectiveness Scores")
    row += 1

    bar_data_start = row
    bar_headers_list = ["Channel", "Effectiveness Score (0-100)"]
    for i, h in enumerate(bar_headers_list):
        cell = ws_strategy.cell(row=row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    # Dynamic channel effectiveness from budget engine ROI scores (0-10 -> 0-100)
    # Falls back to generic defaults only if budget_allocation is empty.
    _eff_budget = data.get("_budget_allocation", {})
    _eff_ch_allocs = _eff_budget.get("channel_allocations", {}) if isinstance(_eff_budget, dict) else {}
    if _eff_ch_allocs and isinstance(_eff_ch_allocs, dict):
        bar_items = []
        for _eff_ch_name, _eff_ch_data in _eff_ch_allocs.items():
            if isinstance(_eff_ch_data, dict):
                _eff_roi = _eff_ch_data.get("roi_score", _eff_ch_data.get("roi", 5))
                _eff_score = min(int(round(float(_eff_roi) * 10)), 100) if isinstance(_eff_roi, (int, float)) else 50
                _eff_label = _eff_ch_name.replace("_", " ").title()
                bar_items.append((_eff_label, _eff_score))
        bar_items.sort(key=lambda x: x[1], reverse=True)
        bar_items = bar_items[:8]  # top 8 channels
    else:
        # Fallback: generic defaults (only used if budget engine produced no data)
        bar_items = [
            ("Indeed", 85),
            ("LinkedIn", 90),
            ("Google Ads", 75),
            ("Facebook/Meta", 70),
            ("Programmatic DSP", 80),
            ("Niche Job Boards", 88),
            ("Social Media", 65),
            ("Events/Career Fairs", 72),
        ]
    for ch_name, score in bar_items:
        style_body_cell(ws_strategy, row, 2, ch_name)
        style_body_cell(ws_strategy, row, 3, score)
        row += 1

    bar_data_end = row - 1

    # Create BarChart
    bar_chart = BarChart()
    bar_chart.type = "bar"  # horizontal bars
    bar_chart.title = "Channel Effectiveness Score (0-100)"
    bar_chart.width = 18
    bar_chart.height = 10
    bar_chart.style = 10
    bar_chart.y_axis.title = None
    bar_chart.x_axis.title = "Score"

    bar_data_ref = Reference(ws_strategy, min_col=3, min_row=bar_data_start, max_row=bar_data_end)
    bar_cats = Reference(ws_strategy, min_col=2, min_row=bar_data_start + 1, max_row=bar_data_end)
    bar_chart.add_data(bar_data_ref, titles_from_data=True)
    bar_chart.set_categories(bar_cats)
    bar_chart.shape = 4

    # Color the bars with primary blue
    if bar_chart.series:
        bar_chart.series[0].graphicalProperties.solidFill = "2E75B6"

    ws_strategy.add_chart(bar_chart, f"B{row + 1}")


    # ── Campus Recruiting Recommendations ──
    campus_recs = research.get_campus_recruiting_recommendations(locations, roles, industry)
    if campus_recs:
        row += 18  # Space for the bar chart above
        style_section_header(ws_strategy, row, 2, 5, "Campus Recruiting Recommendations")
        row += 1
        ws_strategy.cell(row=row, column=2, value="Recommended universities based on target locations. Leverage campus career fairs, on-campus events, and university job boards.").font = Font(name="Calibri", italic=True, size=9, color="596780")
        ws_strategy.merge_cells(f"B{row}:E{row}")
        row += 1

        campus_headers = ["University", "Programs", "Enrollment", "Recruiting Channel"]
        for i, h in enumerate(campus_headers):
            cell = ws_strategy.cell(row=row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color="438765", end_color="438765", fill_type="solid")
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        seen_unis = set()
        for rec in campus_recs:
            uni_name = rec.get("university", "")
            if uni_name in seen_unis:
                continue
            seen_unis.add(uni_name)
            style_body_cell(ws_strategy, row, 2, uni_name)
            ws_strategy.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_strategy, row, 3, rec.get("programs", ""))
            style_body_cell(ws_strategy, row, 4, rec.get("enrollment", ""))
            style_body_cell(ws_strategy, row, 5, rec.get("recruiting_channel", ""))
            ws_strategy.row_dimensions[row].height = 30
            row += 1

    # ── Sheet 4: Traditional Channels ──
    ws_trad = wb.create_sheet("Traditional Channels")
    ws_trad.sheet_properties.tabColor = "4472C4"
    ws_trad.column_dimensions["A"].width = 5
    ws_trad.column_dimensions["B"].width = 30
    ws_trad.column_dimensions["C"].width = 30
    ws_trad.column_dimensions["D"].width = 30
    ws_trad.column_dimensions["E"].width = 5
    ws_trad.column_dimensions["F"].width = 35

    ws_trad.merge_cells("B2:F2")
    ws_trad["B2"].value = "Traditional Channels"
    ws_trad["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
    ws_trad["B2"].border = accent_bottom_border

    roles_str = f" | Target Roles: {', '.join(roles[:5])}" if roles else ""
    _trad_pubs = joveo_pubs.get('total_active_publishers', 10238)
    _trad_pubs_str = f"{_trad_pubs:,}" if isinstance(_trad_pubs, (int, float)) else str(_trad_pubs)
    ws_trad["B3"].value = f"Target: {', '.join(locations)}{roles_str} | Joveo Supply Network: {_trad_pubs_str}+ active publishers"
    ws_trad["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

    row = 5
    ws_trad["B4"].value = "Job Boards (PPC + Slot/Posting + DE&I specific + Niche)"
    ws_trad["B4"].font = Font(name="Calibri", bold=True, size=12, color="2E75B6")

    cat_headers = ["Regional/Local Reach", "*Niche", "Global Reach", "", "Location-Specific"]
    for i, h in enumerate(cat_headers):
        cell = ws_trad.cell(row=row, column=2 + i, value=h)
        cell.font = section_font
        cell.fill = section_fill
        cell.alignment = center_alignment
        cell.border = thin_border

    niche_key = db.get("industries", {}).get(industry, {}).get("niche_channel_key", "")

    regional = data.get("selected_regional", db["traditional_channels"]["regional_local"][:25]) if include_regional else []
    _db_niche = db["traditional_channels"]["niche_by_industry"].get(niche_key, [])[:25]
    # Fallback to INDUSTRY_NICHE_CHANNELS if DB niche list is empty
    if not _db_niche:
        _db_niche = INDUSTRY_NICHE_CHANNELS.get(industry, INDUSTRY_NICHE_CHANNELS.get("general_entry_level", []))
    niche_channels = data.get("selected_niche", _db_niche) if include_niche else []
    global_channels = data.get("selected_global", db["traditional_channels"]["global_reach"][:25]) if include_global else []

    # USE RESEARCH MODULE for real location-specific boards
    location_boards = research.get_location_boards(locations)

    max_rows = max(len(regional), len(niche_channels), len(global_channels), len(location_boards))
    for i in range(max_rows):
        row += 1
        if include_regional and i < len(regional):
            style_body_cell(ws_trad, row, 2, regional[i])
        if include_niche and i < len(niche_channels):
            style_body_cell(ws_trad, row, 3, niche_channels[i])
        if include_global and i < len(global_channels):
            style_body_cell(ws_trad, row, 4, global_channels[i])
        if i < len(location_boards):
            style_body_cell(ws_trad, row, 6, location_boards[i])

    # Add Joveo publisher categories summary
    row += 2
    ws_trad.merge_cells(start_row=row, start_column=2, end_row=row, end_column=6)
    ws_trad.cell(row=row, column=2, value="Additional Joveo Supply Partners by Category").font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
    row += 1

    joveo_cats = [
        ("DEI / Diversity Job Boards", "DEI"),
        ("University Job Boards", "University Job Board"),
        ("Government Job Boards", "Govt"),
        ("Healthcare / Niche Health", "Health"),
        ("Technology / Niche Tech", "Tech"),
        ("Community Hiring", "Community Hiring"),
    ]
    for label, cat_key in joveo_cats:
        pubs = joveo_pubs.get("by_category", {}).get(cat_key, [])
        if pubs:
            cell = ws_trad.cell(row=row, column=2, value=label)
            cell.font = Font(name="Calibri", bold=True, size=10, color="2E75B6")
            cell.border = thin_border
            style_body_cell(ws_trad, row, 3, ", ".join(pubs[:15]))
            ws_trad.merge_cells(start_row=row, start_column=3, end_row=row, end_column=6)
            row += 1

    # ── Sheet 5: Non-Traditional Channels ──
    ws_nontrad = wb.create_sheet("Non-Traditional Channels")
    ws_nontrad.sheet_properties.tabColor = "4472C4"
    ws_nontrad.column_dimensions["A"].width = 5
    ws_nontrad.column_dimensions["B"].width = 35
    ws_nontrad.column_dimensions["C"].width = 30

    ws_nontrad.merge_cells("B2:C2")
    ws_nontrad["B2"].value = "Non-Traditional Channels"
    ws_nontrad["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
    ws_nontrad["B2"].border = accent_bottom_border

    ws_nontrad["B3"].value = f"Target: {', '.join(locations)}"
    ws_nontrad["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

    row = 5
    nt = db["non_traditional_channels"]
    categories = [
        ("CPQA (Cost Per Qualified Applicant)", nt.get("cpqa", [])),
        ("Data Partners / Candidate Re-engagement", nt.get("data_partners", [])),
        ("Media Channels", nt.get("media_channels", [])),
    ]
    if include_programmatic:
        categories.append(("DSPs (Demand-Side Platforms)", nt.get("dsps", [])))
    categories.append(("Government Job Boards", nt.get("gov_job_boards", [])))
    categories.append(("Early-Career Channels", nt.get("early_career_channels", [])))
    if include_employer_brand:
        categories.append(("Employer Branding", nt.get("employer_branding", [])))

    # Add industry-specific non-traditional channels
    _industry_nt = nt.get("industry_specific", {})
    _ind_key_nt = data.get("industry", "general_entry_level")
    _ind_nt_channels = _industry_nt.get(_ind_key_nt, [])
    if _ind_nt_channels:
        _ind_label = db.get("industries", {}).get(_ind_key_nt, {}).get("label", _ind_key_nt.replace("_", " ").title())
        categories.append((f"Industry-Specific Channels ({_ind_label})", _ind_nt_channels))

    # Enrich with Joveo supply partner data
    joveo_nt_cats = {
        "Staffing Partners": joveo_pubs.get("by_category", {}).get("Staffing Partner", [])[:10],
        "AI-Powered Sourcing Tools": joveo_pubs.get("by_category", {}).get("AI tool", [])[:10],
        "Influencer Marketing Platforms": joveo_pubs.get("by_category", {}).get("Influencer Marketing", [])[:10],
        "Programmatic Audio Partners": joveo_pubs.get("by_category", {}).get("Programmatic Audio", [])[:10],
    }
    if include_programmatic:
        joveo_nt_cats["DSP Partners (Joveo Network)"] = joveo_pubs.get("by_category", {}).get("DSP", [])[:10]
    if include_social:
        joveo_nt_cats["Social Media Advertising"] = joveo_pubs.get("by_category", {}).get("Social Media", [])[:10]

    for cat_name, channels in categories:
        cell = ws_nontrad.cell(row=row, column=2, value=cat_name)
        cell.font = section_font
        cell.fill = section_fill
        cell.border = thin_border
        ws_nontrad.cell(row=row, column=3).fill = section_fill
        ws_nontrad.cell(row=row, column=3).border = thin_border
        row += 1
        for ch in channels:
            style_body_cell(ws_nontrad, row, 3, ch)
            row += 1
        row += 1

    # Add Joveo-specific non-traditional categories
    for cat_name, pubs in joveo_nt_cats.items():
        if pubs:
            cell = ws_nontrad.cell(row=row, column=2, value=cat_name)
            cell.font = section_font
            cell.fill = section_fill
            cell.border = thin_border
            ws_nontrad.cell(row=row, column=3).fill = section_fill
            ws_nontrad.cell(row=row, column=3).border = thin_border
            row += 1
            for ch in pubs:
                style_body_cell(ws_nontrad, row, 3, ch)
                row += 1
            row += 1

    # Add alternate supply categories from channels_db
    alt_supply = nt.get("alternate_supply", {})
    alt_supply_sections = [
        ("Competitor Supply Channels", nt.get("competitor_supply_channels", [])),
        ("Alternate Staffing Partners", alt_supply.get("staffing_partners", [])),
    ]
    if include_programmatic:
        alt_supply_sections.append(("Alternate DSPs", alt_supply.get("dsps", [])))
    alt_supply_sections.append(("Influencer Marketing", alt_supply.get("influencer_marketing", [])))
    alt_supply_sections.append(("Programmatic Audio", alt_supply.get("programmatic_audio", [])))
    if include_social:
        alt_supply_sections.append(("Social Media Advertising", alt_supply.get("social_media_ads", [])))
    for cat_name, items in alt_supply_sections:
        if items:
            cell = ws_nontrad.cell(row=row, column=2, value=cat_name)
            cell.font = section_font
            cell.fill = section_fill
            cell.border = thin_border
            ws_nontrad.cell(row=row, column=3).fill = section_fill
            ws_nontrad.cell(row=row, column=3).border = thin_border
            row += 1
            for ch in items:
                style_body_cell(ws_nontrad, row, 3, ch)
                row += 1
            row += 1

    # Add APAC local social platforms if any location is in APAC
    apac_social = alt_supply.get("local_social_platforms", {})
    apac_classifieds = alt_supply.get("local_classifieds", {})
    if include_apac and (apac_social or apac_classifieds):
        has_apac_data = False
        for loc in locations:
            loc_lower = loc.strip().lower()
            for country_key in list(apac_social.keys()) + list(apac_classifieds.keys()):
                if country_key in loc_lower or loc_lower in country_key:
                    has_apac_data = True
                    break
        # Also include if any international location is present
        if has_international or has_apac_data:
            if apac_social:
                cell = ws_nontrad.cell(row=row, column=2, value="APAC Local Social Platforms")
                cell.font = section_font
                cell.fill = section_fill
                cell.border = thin_border
                ws_nontrad.cell(row=row, column=3).fill = section_fill
                ws_nontrad.cell(row=row, column=3).border = thin_border
                row += 1
                for country, platforms in apac_social.items():
                    style_body_cell(ws_nontrad, row, 2, f"  {country.replace('_', ' ').title()}")
                    style_body_cell(ws_nontrad, row, 3, ", ".join(platforms))
                    row += 1
                row += 1
            if apac_classifieds:
                cell = ws_nontrad.cell(row=row, column=2, value="APAC Local Classifieds")
                cell.font = section_font
                cell.fill = section_fill
                cell.border = thin_border
                ws_nontrad.cell(row=row, column=3).fill = section_fill
                ws_nontrad.cell(row=row, column=3).border = thin_border
                row += 1
                for country, platforms in apac_classifieds.items():
                    style_body_cell(ws_nontrad, row, 2, f"  {country.replace('_', ' ').title()}")
                    style_body_cell(ws_nontrad, row, 3, ", ".join(platforms))
                    row += 1
                row += 1

    # ── Sheet 6: Global Supply Strategy (if international or explicitly requested) ──
    if has_international or data.get("include_global_supply"):
        ws_global = wb.create_sheet("Global Supply Strategy")
        ws_global.sheet_properties.tabColor = "00B050"
        ws_global.column_dimensions["A"].width = 5
        ws_global.column_dimensions["B"].width = 25
        ws_global.column_dimensions["C"].width = 25
        ws_global.column_dimensions["D"].width = 20
        ws_global.column_dimensions["E"].width = 15
        ws_global.column_dimensions["F"].width = 20
        ws_global.column_dimensions["G"].width = 30

        ws_global.merge_cells("B2:G2")
        ws_global["B2"].value = "Global Supply Strategy"
        ws_global["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_global["B2"].border = accent_bottom_border
        ws_global["B3"].value = f"Markets: {', '.join(locations)}"
        ws_global["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

        row = 5
        # Country-Specific Job Boards
        cell = ws_global.cell(row=row, column=2, value="Country-Specific Job Boards")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 8):
            ws_global.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_global.cell(row=row, column=c).border = thin_border
        row += 1

        gs_headers = ["Country", "Board Name", "Billing Model", "Category", "Tier", "Monthly Spend"]
        for i, h in enumerate(gs_headers):
            cell = ws_global.cell(row=row, column=2 + i, value=h)
            cell.font = subheader_font
            cell.fill = subheader_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        country_boards = global_research.get("country_boards", [])
        for cb in country_boards:
            country_name = cb.get("country", "")
            board_data = cb.get("data", {})
            boards = board_data.get("boards", [])
            monthly = board_data.get("monthly_spend", "N/A")
            for idx, board in enumerate(boards):
                style_body_cell(ws_global, row, 2, country_name if idx == 0 else "")
                if idx == 0:
                    ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_global, row, 3, board.get("name", ""))
                style_body_cell(ws_global, row, 4, board.get("billing", ""))
                style_body_cell(ws_global, row, 5, board.get("category", ""))
                style_body_cell(ws_global, row, 6, board.get("tier", ""))
                style_body_cell(ws_global, row, 7, monthly if idx == 0 else "")
                if row % 2 == 0:
                    for c in range(2, 8):
                        ws_global.cell(row=row, column=c).fill = accent_fill
                row += 1
            row += 1  # gap between countries

        # Push vs Pull Strategy
        row += 1
        cell = ws_global.cell(row=row, column=2, value="Push vs Pull Strategy Recommendations")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 8):
            ws_global.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_global.cell(row=row, column=c).border = thin_border
        row += 1

        push_pull = global_research.get("push_pull_strategy", {})
        for strategy_type in ["pull_advertising", "push_advertising"]:
            strat = push_pull.get(strategy_type, {})
            if strat:
                cell = ws_global.cell(row=row, column=2, value=strategy_type.replace("_", " ").title())
                cell.font = Font(name="Calibri", bold=True, size=11, color="1F4E79")
                cell.fill = subheader_fill
                cell.border = thin_border
                for c in range(3, 8):
                    ws_global.cell(row=row, column=c).fill = subheader_fill
                    ws_global.cell(row=row, column=c).border = thin_border
                row += 1
                style_body_cell(ws_global, row, 2, "Description")
                ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_global, row, 3, strat.get("description", ""))
                ws_global.merge_cells(start_row=row, start_column=3, end_row=row, end_column=7)
                row += 1
                style_body_cell(ws_global, row, 2, "Best For")
                ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_global, row, 3, strat.get("best_for", ""))
                ws_global.merge_cells(start_row=row, start_column=3, end_row=row, end_column=7)
                row += 1
                style_body_cell(ws_global, row, 2, "Channels")
                ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                channels_list = strat.get("channels", [])
                style_body_cell(ws_global, row, 3, ", ".join(channels_list) if isinstance(channels_list, list) else str(channels_list))
                ws_global.merge_cells(start_row=row, start_column=3, end_row=row, end_column=7)
                row += 1
                style_body_cell(ws_global, row, 2, "KPIs")
                ws_global.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                kpis_list = strat.get("kpis", [])
                style_body_cell(ws_global, row, 3, ", ".join(kpis_list) if isinstance(kpis_list, list) else str(kpis_list))
                ws_global.merge_cells(start_row=row, start_column=3, end_row=row, end_column=7)
                row += 1
                row += 1

        # NOTE: Commission Tiers intentionally excluded — internal Joveo data, not for client-facing output

    # ── Sheet 7: DEI & Diversity Channels (optional) ──
    if data.get("include_dei"):
        ws_dei = wb.create_sheet("DEI & Diversity Channels")
        ws_dei.sheet_properties.tabColor = "0A66C9"
        ws_dei.column_dimensions["A"].width = 5
        ws_dei.column_dimensions["B"].width = 30
        ws_dei.column_dimensions["C"].width = 35
        ws_dei.column_dimensions["D"].width = 25
        ws_dei.column_dimensions["E"].width = 30

        ws_dei.merge_cells("B2:E2")
        ws_dei["B2"].value = "DEI & Diversity Channels"
        ws_dei["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_dei["B2"].border = accent_bottom_border
        ws_dei["B3"].value = f"Target Markets: {', '.join(locations)}"
        ws_dei["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

        row = 5
        # DEI Boards by Region
        cell = ws_dei.cell(row=row, column=2, value="DEI-Focused Job Boards by Region")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 6):
            ws_dei.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_dei.cell(row=row, column=c).border = thin_border
        row += 1

        dei_headers = ["Board Name", "Focus Area", "Regions Covered"]
        for i, h in enumerate(dei_headers):
            cell = ws_dei.cell(row=row, column=2 + i, value=h)
            cell.font = subheader_font
            cell.fill = subheader_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        dei_by_country = gs.get("dei_boards_by_country", {})
        for region_name, boards in dei_by_country.items():
            # Section header for each region
            cell = ws_dei.cell(row=row, column=2, value=region_name)
            cell.font = section_font
            cell.fill = section_fill
            cell.border = thin_border
            for c in range(3, 5):
                ws_dei.cell(row=row, column=c).fill = section_fill
                ws_dei.cell(row=row, column=c).border = thin_border
            row += 1

            board_list = boards if isinstance(boards, list) else boards.get("boards", boards) if isinstance(boards, dict) else []
            if isinstance(board_list, list):
                for board in board_list:
                    if isinstance(board, dict):
                        style_body_cell(ws_dei, row, 2, board.get("name", ""))
                        style_body_cell(ws_dei, row, 3, board.get("focus", ""))
                        style_body_cell(ws_dei, row, 4, board.get("regions", region_name))
                        if row % 2 == 0:
                            for c in range(2, 5):
                                ws_dei.cell(row=row, column=c).fill = accent_fill
                        row += 1
            row += 1  # gap between regions

        # Women-Specific Boards
        row += 1
        cell = ws_dei.cell(row=row, column=2, value="Women-Specific Job Boards")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 6):
            ws_dei.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_dei.cell(row=row, column=c).border = thin_border
        row += 1

        women_headers = ["Board Name", "Focus", "Regions"]
        for i, h in enumerate(women_headers):
            cell = ws_dei.cell(row=row, column=2 + i, value=h)
            cell.font = subheader_font
            cell.fill = subheader_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        women_boards = gs.get("women_specific_boards", [])
        for board in women_boards:
            style_body_cell(ws_dei, row, 2, board.get("name", ""))
            style_body_cell(ws_dei, row, 3, board.get("focus", ""))
            style_body_cell(ws_dei, row, 4, board.get("regions", ""))
            if row % 2 == 0:
                for c in range(2, 5):
                    ws_dei.cell(row=row, column=c).fill = accent_fill
            row += 1

        # Industry-Specific Diversity Channels from channels_db
        row += 2
        cell = ws_dei.cell(row=row, column=2, value="Industry-Specific Diversity Channels")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 6):
            ws_dei.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_dei.cell(row=row, column=c).border = thin_border
        row += 1

        dei_channels = db.get("traditional_channels", {}).get("niche_by_industry", {}).get("diversity_dei", [])
        for ch in dei_channels:
            style_body_cell(ws_dei, row, 2, ch)
            style_body_cell(ws_dei, row, 3, "Multi-diversity / DEI-focused")
            style_body_cell(ws_dei, row, 4, "US / Global")
            row += 1

    # ── Sheet 8: Innovative Channels 2025+ (optional) ──
    if data.get("include_innovative"):
        ws_innov = wb.create_sheet("Innovative Channels 2025+")
        ws_innov.sheet_properties.tabColor = "FF6600"
        ws_innov.column_dimensions["A"].width = 5
        ws_innov.column_dimensions["B"].width = 30
        ws_innov.column_dimensions["C"].width = 50
        ws_innov.column_dimensions["D"].width = 45
        ws_innov.column_dimensions["E"].width = 20
        ws_innov.column_dimensions["F"].width = 35

        ws_innov.merge_cells("B2:F2")
        ws_innov["B2"].value = "Innovative Channels 2025+"
        ws_innov["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_innov["B2"].border = accent_bottom_border
        ws_innov["B3"].value = "Emerging recruitment channels: CTV, DOOH, Retail Media, Gaming, Podcasts, Messaging Apps & more"
        ws_innov["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

        row = 5
        innov_headers = ["Channel", "Description", "Best Use Case", "Billing Model", "Best For / Industries"]
        for i, h in enumerate(innov_headers):
            cell = ws_innov.cell(row=row, column=2 + i, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        innovative = gs.get("innovative_channels_2025", {})
        for channel_key, channel_data in innovative.items():
            if isinstance(channel_data, dict):
                style_body_cell(ws_innov, row, 2, channel_key.replace("_", " ").title())
                ws_innov.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_innov, row, 3, channel_data.get("description", ""))
                style_body_cell(ws_innov, row, 4, channel_data.get("use_case", ""))
                style_body_cell(ws_innov, row, 5, channel_data.get("billing", ""))
                best_for = channel_data.get("best_for", [])
                style_body_cell(ws_innov, row, 6, ", ".join(best_for) if isinstance(best_for, list) else str(best_for))
                ws_innov.row_dimensions[row].height = 50
                if row % 2 == 0:
                    for c in range(2, 7):
                        ws_innov.cell(row=row, column=c).fill = accent_fill
                row += 1

        # Platforms sub-section
        row += 2
        cell = ws_innov.cell(row=row, column=2, value="Platform Details by Channel")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 7):
            ws_innov.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_innov.cell(row=row, column=c).border = thin_border
        row += 1

        for i, h in enumerate(["Channel", "Platforms"]):
            cell = ws_innov.cell(row=row, column=2 + i, value=h)
            cell.font = subheader_font
            cell.fill = subheader_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        for channel_key, channel_data in innovative.items():
            if isinstance(channel_data, dict):
                platforms = channel_data.get("platforms", [])
                if platforms:
                    style_body_cell(ws_innov, row, 2, channel_key.replace("_", " ").title())
                    ws_innov.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                    style_body_cell(ws_innov, row, 3, ", ".join(platforms))
                    ws_innov.merge_cells(start_row=row, start_column=3, end_row=row, end_column=6)
                    row += 1

    # ── Sheet 9: Budget & Pricing Guide (optional) ──
    if data.get("include_budget_guide"):
        ws_budget = wb.create_sheet("Budget & Pricing Guide")
        ws_budget.sheet_properties.tabColor = "C00000"
        ws_budget.column_dimensions["A"].width = 5
        ws_budget.column_dimensions["B"].width = 25
        ws_budget.column_dimensions["C"].width = 50
        ws_budget.column_dimensions["D"].width = 25
        ws_budget.column_dimensions["E"].width = 40

        ws_budget.merge_cells("B2:E2")
        ws_budget["B2"].value = "Budget & Pricing Guide"
        ws_budget["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_budget["B2"].border = accent_bottom_border

        row = 4
        # Billing Models
        cell = ws_budget.cell(row=row, column=2, value="Billing Models Explained")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 6):
            ws_budget.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_budget.cell(row=row, column=c).border = thin_border
        row += 1

        billing_headers = ["Model", "Description", "Typical Rate Range"]
        for i, h in enumerate(billing_headers):
            cell = ws_budget.cell(row=row, column=2 + i, value=h)
            cell.font = subheader_font
            cell.fill = subheader_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        billing_models = gs.get("billing_models", {})
        for model_name, model_data in billing_models.items():
            style_body_cell(ws_budget, row, 2, model_name)
            ws_budget.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_budget, row, 3, model_data.get("description", ""))
            style_body_cell(ws_budget, row, 4, model_data.get("typical_range", ""))
            if row % 2 == 0:
                for c in range(2, 5):
                    ws_budget.cell(row=row, column=c).fill = accent_fill
            row += 1

        # NOTE: Commission Tiers intentionally excluded — internal Joveo data, not for client-facing output

        # Monthly Buying Recommendations by Region
        row += 2
        cell = ws_budget.cell(row=row, column=2, value="Monthly Buying by Region")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 6):
            ws_budget.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_budget.cell(row=row, column=c).border = thin_border
        row += 1

        region_headers = ["Region", "Monthly Spend", "Top Countries"]
        for i, h in enumerate(region_headers):
            cell = ws_budget.cell(row=row, column=2 + i, value=h)
            cell.font = subheader_font
            cell.fill = subheader_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        monthly_buying = gs.get("monthly_buying_by_region", {})
        for region_name, region_data in monthly_buying.items():
            style_body_cell(ws_budget, row, 2, region_name)
            ws_budget.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_budget, row, 3, region_data.get("spend", ""))
            top_countries = region_data.get("top_countries", [])
            style_body_cell(ws_budget, row, 4, ", ".join(top_countries))
            if row % 2 == 0:
                for c in range(2, 5):
                    ws_budget.cell(row=row, column=c).fill = accent_fill
            row += 1

        # CPA Rate Benchmarks by region (sourced from channels_db channel_strategies)
        row += 2
        cell = ws_budget.cell(row=row, column=2, value="CPA Rate Benchmarks (Typical Ranges)")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 6):
            ws_budget.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_budget.cell(row=row, column=c).border = thin_border
        row += 1

        # Try to read CPA benchmarks from channels_db; fall back to hardcoded values
        cpa_from_db = db.get("cpa_rate_benchmarks", {})
        cpa_benchmarks = []
        if isinstance(cpa_from_db, dict) and cpa_from_db:
            # channels_db structure: {region: {low_cpa: {range, publishers}, mid_cpa: {...}, high_cpa: {...}}}
            _region_labels = {
                "north_america": "North America (US/Canada)",
                "europe": "Europe (UK/DE/FR/NL)",
                "apac": "APAC (India/Japan/AU/SG)",
                "latam": "LATAM (Brazil/Mexico/Argentina)",
                "mea": "MEA (UAE/South Africa/Kenya)",
            }
            for region_key, region_data in cpa_from_db.items():
                if not isinstance(region_data, dict):
                    continue
                region_label = _region_labels.get(region_key.lower(), region_key.replace("_", " ").title())
                # Build CPA range from low/mid/high tiers
                tier_ranges = []
                tier_publishers = []
                for tier_key in ("low_cpa", "mid_cpa", "high_cpa"):
                    tier = region_data.get(tier_key, {})
                    if isinstance(tier, dict):
                        t_range = tier.get("range", "")
                        if t_range:
                            tier_ranges.append(f"{tier_key.replace('_cpa', '').title()}: {t_range}")
                        t_pubs = tier.get("publishers", [])
                        if isinstance(t_pubs, list):
                            tier_publishers.extend(t_pubs[:3])
                cpa_range_str = " | ".join(tier_ranges) if tier_ranges else "N/A"
                notes_str = f"Key publishers: {', '.join(tier_publishers[:5])}" if tier_publishers else ""
                cpa_benchmarks.append((region_label, cpa_range_str, notes_str))

        # Enrich with platform-level CPA data from ad_platform_analysis synthesis
        _synth_ad = data.get("_synthesized", {}).get("ad_platform_analysis", {})
        if isinstance(_synth_ad, dict) and _synth_ad:
            _plat_cpas = []
            for pname, pdata in _synth_ad.items():
                if not isinstance(pdata, dict) or pname.startswith("_"):
                    continue
                _p_cpa = pdata.get("avg_cpa", pdata.get("cpa", None))
                if isinstance(_p_cpa, (int, float)) and _p_cpa > 0:
                    _plat_cpas.append(f"{pname}: ${_p_cpa:.2f}")
            if _plat_cpas:
                cpa_benchmarks.append(("Platform-Specific (Synthesized)", " | ".join(_plat_cpas[:6]), "CPA from ad platform analysis synthesis"))

        if not cpa_benchmarks:
            cpa_benchmarks = [
                ("North America (US/Canada)", "$15 - $45", "High competition, strong CPC performance on Indeed/ZipRecruiter"),
                ("Europe (UK/DE/FR/NL)", "$12 - $40", "Mixed CPC/Posting models; StepStone, Reed, Totaljobs common"),
                ("APAC (India/Japan/AU/SG)", "$5 - $30", "Lower CPAs in India; premium in Japan/AU/Singapore"),
                ("LATAM (Brazil/Mexico/Argentina)", "$3 - $20", "Cost-effective; CompuTrabajo, OCC Mundial popular"),
                ("MEA (UAE/South Africa/Kenya)", "$5 - $25", "Growing market; Bayt.com, CareerJunction dominant"),
            ]
        for i, h in enumerate(["Region", f"CPA Range ({display_currency_code})", "Notes"]):
            cell = ws_budget.cell(row=row, column=2 + i, value=h)
            cell.font = subheader_font
            cell.fill = subheader_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        # Filter to only show regions relevant to client's locations
        region_map = {
            "North America": ["north america", "us", "united states", "america", "canada"],
            "Europe": ["europe", "uk", "united kingdom", "germany", "france", "london", "berlin", "paris", "netherlands", "spain", "italy"],
            "APAC": ["apac", "india", "australia", "singapore", "japan", "china", "asia", "hong kong", "korea"],
            "LATAM": ["latam", "brazil", "mexico", "latin", "colombia", "argentina", "chile"],
            "MEA": ["mea", "uae", "south africa", "kenya", "middle east", "africa"],
        }
        location_str = " ".join(locations).lower()
        filtered_benchmarks = []
        for region, cpa_range, notes in cpa_benchmarks:
            region_key = region.split("(")[0].strip()
            keywords = region_map.get(region_key, [])
            # Always include platform-specific synthesized data and matching regions
            if region_key.startswith("Platform") or any(kw in location_str for kw in keywords):
                filtered_benchmarks.append((region, cpa_range, notes))
        # Default to North America + any platform data if nothing matched
        if not filtered_benchmarks:
            filtered_benchmarks = [b for b in cpa_benchmarks if "North America" in b[0] or "Platform" in b[0]]
        for region, cpa_range, notes in filtered_benchmarks:
            style_body_cell(ws_budget, row, 2, region)
            ws_budget.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_budget, row, 3, cpa_range)
            style_body_cell(ws_budget, row, 4, notes)
            ws_budget.row_dimensions[row].height = 35
            if row % 2 == 0:
                for c in range(2, 5):
                    ws_budget.cell(row=row, column=c).fill = accent_fill
            row += 1

        # ── Pie Chart: Recommended Budget Allocation ──
        row += 2
        cell = ws_budget.cell(row=row, column=2, value="Recommended Budget Allocation by Channel Type")
        cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
        cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
        for c in range(3, 6):
            ws_budget.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            ws_budget.cell(row=row, column=c).border = thin_border
        row += 1

        # Write pie chart data table
        pie_data_start = row
        pie_headers = ["Channel Type", "Allocation %"]
        for i, h in enumerate(pie_headers):
            cell = ws_budget.cell(row=row, column=2 + i, value=h)
            cell.font = subheader_font
            cell.fill = subheader_fill
            cell.alignment = center_alignment
            cell.border = thin_border
        row += 1

        # Dynamic pie from budget engine channel allocations
        _pie_budget = data.get("_budget_allocation", {})
        _pie_ch_allocs = _pie_budget.get("channel_allocations", {}) if isinstance(_pie_budget, dict) else {}
        if _pie_ch_allocs and isinstance(_pie_ch_allocs, dict):
            pie_items = []
            for _pie_ch_name, _pie_ch_data in _pie_ch_allocs.items():
                if isinstance(_pie_ch_data, dict):
                    _pie_pct = _pie_ch_data.get("percentage", _pie_ch_data.get("pct", 0))
                    if isinstance(_pie_pct, (int, float)) and _pie_pct > 0:
                        _pie_label = _pie_ch_name.replace("_", " ").title()
                        pie_items.append((_pie_label, round(float(_pie_pct))))
            pie_items.sort(key=lambda x: x[1], reverse=True)
            # Ensure percentages sum close to 100
            if not pie_items:
                pie_items = [("Unallocated", 100)]
        else:
            # Fallback: generic defaults (only if budget engine produced no data)
            pie_items = [
                ("Job Boards (Programmatic)", 35),
                ("Social Media Advertising", 20),
                ("Niche/Industry Boards", 15),
                ("Employer Branding", 10),
                ("Events & Career Fairs", 8),
                ("Innovative/Emerging", 7),
                ("DEI Channels", 5),
            ]
        for label, pct in pie_items:
            style_body_cell(ws_budget, row, 2, label)
            style_body_cell(ws_budget, row, 3, pct)
            row += 1

        pie_data_end = row - 1

        # Create PieChart
        pie_chart = PieChart()
        pie_chart.title = "Recommended Budget Allocation by Channel Type"
        pie_chart.width = 18
        pie_chart.height = 12
        pie_chart.style = 10

        pie_labels = Reference(ws_budget, min_col=2, min_row=pie_data_start + 1, max_row=pie_data_end)
        pie_values = Reference(ws_budget, min_col=3, min_row=pie_data_start, max_row=pie_data_end)
        pie_chart.add_data(pie_values, titles_from_data=True)
        pie_chart.set_categories(pie_labels)

        pie_chart.dataLabels = DataLabelList()
        pie_chart.dataLabels.showPercent = True
        pie_chart.dataLabels.showCatName = True

        ws_budget.add_chart(pie_chart, f"B{row + 1}")
        row += 20  # leave space for chart

        # ── Real Dollar Budget Breakdown (from budget_engine) ──
        _bp_budget_alloc = data.get("_budget_allocation", {})
        _bp_ch_allocs = _bp_budget_alloc.get("channel_allocations", {}) if isinstance(_bp_budget_alloc, dict) else {}
        if isinstance(_bp_ch_allocs, dict) and _bp_ch_allocs:
            row += 2
            cell = ws_budget.cell(row=row, column=2, value="Calculated Budget Breakdown (Projected)")
            cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
            cell.fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
            for c in range(3, 6):
                ws_budget.cell(row=row, column=c).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
                ws_budget.cell(row=row, column=c).border = thin_border
            row += 1

            _bp_headers = ["Channel", f"$ Amount ({display_currency_code})", "Projected Hires", "Cost/Hire"]
            for i, h in enumerate(_bp_headers):
                cell = ws_budget.cell(row=row, column=2 + i, value=h)
                cell.font = subheader_font
                cell.fill = subheader_fill
                cell.alignment = center_alignment
                cell.border = thin_border
            row += 1

            for ch_name, ch_data in _bp_ch_allocs.items():
                if not isinstance(ch_data, dict):
                    continue
                _bp_amt = ch_data.get("dollar_amount", ch_data.get("amount", 0))
                _bp_hires = ch_data.get("projected_hires", 0)
                _bp_cph = ch_data.get("cost_per_hire", 0)
                style_body_cell(ws_budget, row, 2, ch_name.replace("_", " ").title())
                ws_budget.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_budget, row, 3, f"${_bp_amt:,.0f}" if isinstance(_bp_amt, (int, float)) else str(_bp_amt))
                style_body_cell(ws_budget, row, 4, f"{_bp_hires:,.1f}" if isinstance(_bp_hires, (int, float)) else str(_bp_hires))
                style_body_cell(ws_budget, row, 5, f"${_bp_cph:,.0f}" if isinstance(_bp_cph, (int, float)) and _bp_cph > 0 else "N/A")
                if row % 2 == 0:
                    for c in range(2, 6):
                        ws_budget.cell(row=row, column=c).fill = accent_fill
                row += 1

            row += 1
            ws_budget.merge_cells(f"B{row}:E{row}")
            ws_budget.cell(row=row, column=2, value="Dollar amounts computed by the budget engine based on industry allocation percentages, role complexity, and location cost multipliers.").font = Font(name="Calibri", italic=True, size=8, color="999999")

    # ── Job Category Insights Sheet (if categories selected) ──
    job_categories = data.get("job_categories", [])
    job_cat_db = db.get("job_categories", {})
    if job_categories and job_cat_db:
        ws_jc = wb.create_sheet("Job Category Insights")
        ws_jc.sheet_properties.tabColor = "00B0F0"
        ws_jc.column_dimensions["A"].width = 3
        ws_jc.column_dimensions["B"].width = 22
        ws_jc.column_dimensions["C"].width = 28
        ws_jc.column_dimensions["D"].width = 28
        ws_jc.column_dimensions["E"].width = 28
        ws_jc.column_dimensions["F"].width = 28

        ws_jc.merge_cells("B2:F2")
        jc_title = ws_jc["B2"]
        jc_title.value = "Job Category Insights & Channel Recommendations"
        jc_title.font = Font(name="Calibri", bold=True, size=18, color="FFFFFF")
        jc_title.fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
        jc_title.alignment = Alignment(horizontal="center", vertical="center")
        for c in range(3, 7):
            ws_jc.cell(row=2, column=c).fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
        ws_jc.row_dimensions[2].height = 40

        ws_jc.merge_cells("B3:F3")
        ws_jc["B3"].value = f"Categories: {', '.join(data.get('job_category_labels', []))}  |  Industry: {data.get('industry_label', industry)}"
        ws_jc["B3"].font = Font(name="Calibri", italic=True, size=10, color="FFFFFF")
        ws_jc["B3"].fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")
        ws_jc["B3"].alignment = Alignment(horizontal="center")
        for c in range(3, 7):
            ws_jc.cell(row=3, column=c).fill = PatternFill(start_color="1B2A4A", end_color="1B2A4A", fill_type="solid")

        jc_row = 5

        for cat_key in job_categories:
            cat = job_cat_db.get(cat_key)
            if not cat:
                continue

            # Category header
            ws_jc.merge_cells(f"B{jc_row}:F{jc_row}")
            cell = ws_jc.cell(row=jc_row, column=2, value=f"{cat.get('icon', '')} {cat.get('label', cat_key)}")
            cell.font = Font(name="Calibri", bold=True, size=14, color="FFFFFF")
            cell.fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
            cell.alignment = Alignment(horizontal="center", vertical="center")
            for c in range(3, 7):
                ws_jc.cell(row=jc_row, column=c).fill = PatternFill(start_color="2E75B6", end_color="2E75B6", fill_type="solid")
            ws_jc.row_dimensions[jc_row].height = 30
            jc_row += 1

            # Description
            ws_jc.merge_cells(f"B{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=2, value=cat.get("description", "")).font = Font(name="Calibri", italic=True, size=10, color="666666")
            jc_row += 1

            # Key Metrics Row
            jc_row += 1
            metric_items = [
                ("Avg CPA Range", cat.get("avg_cpa_range", "N/A")),
                ("Time to Fill", cat.get("avg_time_to_fill", "N/A")),
                ("Awareness %", str(cat.get("strategy_emphasis", {}).get("awareness", "")) + "%" if cat.get("strategy_emphasis") else "N/A"),
                ("Hiring %", str(cat.get("strategy_emphasis", {}).get("hiring", "")) + "%" if cat.get("strategy_emphasis") else "N/A"),
            ]
            for i, (label, val) in enumerate(metric_items):
                cell_l = ws_jc.cell(row=jc_row, column=2 + i, value=label)
                cell_l.font = Font(name="Calibri", bold=True, size=9, color="666666")
                cell_l.fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
                cell_l.border = thin_border
                cell_v = ws_jc.cell(row=jc_row + 1, column=2 + i, value=val)
                cell_v.font = Font(name="Calibri", bold=True, size=12, color="1B2A4A")
                cell_v.fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
                cell_v.border = thin_border
                cell_v.alignment = Alignment(horizontal="center")
            jc_row += 2

            # Example Roles
            jc_row += 1
            ws_jc.cell(row=jc_row, column=2, value="Typical Roles").font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
            example_roles = cat.get("example_roles", [])
            ws_jc.cell(row=jc_row, column=3, value=", ".join(example_roles)).font = body_font
            ws_jc.merge_cells(f"C{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=2).border = thin_border
            ws_jc.cell(row=jc_row, column=3).border = thin_border
            ws_jc.cell(row=jc_row, column=3).alignment = wrap_alignment
            jc_row += 1

            # Recommended Channels Table
            jc_row += 1
            rec_ch = cat.get("recommended_channels", {})
            channel_sections = [
                ("Primary Channels (Joveo Supply)", rec_ch.get("primary", []), "00B050"),
                ("Secondary Channels", rec_ch.get("secondary", []), "4472C4"),
                ("Social & Paid Media", rec_ch.get("social", []), "ED7D31"),
                ("Niche / Specialized", rec_ch.get("niche", []), "0A66C9"),
            ]
            ch_headers = ["Category", "Channels", "Strategy Notes", "Data Source"]
            for i, h in enumerate(ch_headers):
                cell = ws_jc.cell(row=jc_row, column=2 + i, value=h)
                cell.font = header_font
                cell.fill = header_fill
                cell.alignment = center_alignment
                cell.border = thin_border
            jc_row += 1

            for ch_label, ch_list, ch_color in channel_sections:
                if ch_list:
                    cell = ws_jc.cell(row=jc_row, column=2, value=ch_label)
                    cell.font = Font(name="Calibri", bold=True, size=10, color=ch_color)
                    cell.border = thin_border
                    style_body_cell(ws_jc, jc_row, 3, ", ".join(ch_list))
                    # Add strategy notes based on category with industry-aware allocation guidance
                    _strat_emph = cat.get("strategy_emphasis", {})
                    _hiring_pct = _strat_emph.get("hiring", 65)
                    if "Primary" in ch_label:
                        _pri_range = "45-55%" if _hiring_pct >= 65 else "35-45%"
                        style_body_cell(ws_jc, jc_row, 4, f"High-volume, proven ROI — allocate {_pri_range} of budget")
                        style_body_cell(ws_jc, jc_row, 5, "Joveo Supply Repository")
                    elif "Secondary" in ch_label:
                        _sec_range = "15-25%" if _hiring_pct >= 65 else "20-30%"
                        style_body_cell(ws_jc, jc_row, 4, f"Supplementary reach — allocate {_sec_range} of budget")
                        style_body_cell(ws_jc, jc_row, 5, "Past Media Plan Data")
                    elif "Social" in ch_label:
                        _soc_range = "20-30%" if _hiring_pct < 55 else "10-20%"
                        style_body_cell(ws_jc, jc_row, 4, f"Brand awareness + retargeting — allocate {_soc_range} of budget")
                        style_body_cell(ws_jc, jc_row, 5, "Competitor Analysis")
                    else:
                        _niche_range = "15-20%" if _hiring_pct >= 65 else "10-15%"
                        style_body_cell(ws_jc, jc_row, 4, f"Targeted specialists — allocate {_niche_range} of budget")
                        style_body_cell(ws_jc, jc_row, 5, "Industry Research")
                    ws_jc.row_dimensions[jc_row].height = 35
                    jc_row += 1

            # Joveo Supply Fit
            jc_row += 1
            ws_jc.cell(row=jc_row, column=2, value="Joveo Supply Fit").font = Font(name="Calibri", bold=True, size=11, color="00B050")
            ws_jc.cell(row=jc_row, column=2).border = thin_border
            joveo_fit = cat.get("joveo_supply_fit", [])
            ws_jc.cell(row=jc_row, column=3, value=", ".join(joveo_fit)).font = body_font
            ws_jc.cell(row=jc_row, column=3).border = thin_border
            ws_jc.merge_cells(f"C{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=3).alignment = wrap_alignment
            jc_row += 1

            # Competitor Channels
            ws_jc.cell(row=jc_row, column=2, value="Competitor Channels").font = Font(name="Calibri", bold=True, size=11, color="ED7D31")
            ws_jc.cell(row=jc_row, column=2).border = thin_border
            comp_ch = cat.get("competitor_channels", [])
            ws_jc.cell(row=jc_row, column=3, value=", ".join(comp_ch)).font = body_font
            ws_jc.cell(row=jc_row, column=3).border = thin_border
            ws_jc.merge_cells(f"C{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=3).alignment = wrap_alignment
            jc_row += 1

            # Best Practices
            jc_row += 1
            ws_jc.merge_cells(f"B{jc_row}:F{jc_row}")
            ws_jc.cell(row=jc_row, column=2, value="Best Practices & Recommendations").font = Font(name="Calibri", bold=True, size=11, color="1B2A4A")
            jc_row += 1
            for bp in cat.get("best_practices", []):
                ws_jc.cell(row=jc_row, column=2, value=f"  \u2022  {bp}").font = body_font
                ws_jc.merge_cells(f"B{jc_row}:F{jc_row}")
                ws_jc.cell(row=jc_row, column=2).border = thin_border
                jc_row += 1

            jc_row += 2  # gap between categories

    # ── Campaign Timeline Sheet ──
    ws_timeline = wb.create_sheet("Campaign Timeline")
    ws_timeline.sheet_properties.tabColor = "2E75B6"
    ws_timeline.column_dimensions["A"].width = 3
    ws_timeline.column_dimensions["B"].width = 22
    ws_timeline.column_dimensions["C"].width = 16
    ws_timeline.column_dimensions["D"].width = 40
    ws_timeline.column_dimensions["E"].width = 25
    ws_timeline.column_dimensions["F"].width = 28
    ws_timeline.column_dimensions["G"].width = 12

    ws_timeline.merge_cells("B2:G2")
    ws_timeline["B2"].value = "Campaign Timeline"
    ws_timeline["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
    ws_timeline["B2"].border = accent_bottom_border

    # Use actual campaign duration instead of hardcoded "Standard 12 Weeks"
    actual_duration = data.get("campaign_duration", "Not specified")
    if actual_duration and actual_duration != "Not specified":
        duration_display = actual_duration
    else:
        duration_display = "Standard 12 Weeks"
    ws_timeline["B3"].value = f"Client: {data.get('client_name', '')}  |  Duration: {duration_display}"
    ws_timeline["B3"].font = Font(name="Calibri", italic=True, size=10, color="666666")

    tl_row = 5
    style_section_header(ws_timeline, tl_row, 2, 7, "Campaign Phases")
    tl_row += 1

    tl_headers = ["Phase", "Timeline", "Activities", "Channels", "KPIs", "Budget %"]
    for i, h in enumerate(tl_headers):
        cell = ws_timeline.cell(row=tl_row, column=2 + i, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    tl_row += 1

    phase_colors = {
        "Phase 1": PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid"),  # blue
        "Phase 2": PatternFill(start_color="D9EAD3", end_color="D9EAD3", fill_type="solid"),  # green
        "Phase 3": PatternFill(start_color="FCE5CD", end_color="FCE5CD", fill_type="solid"),  # orange
        "Phase 4": PatternFill(start_color="ECFEFF", end_color="ECFEFF", fill_type="solid"),  # pale teal
    }

    # Build timeline phases based on actual campaign_weeks
    cw = data.get("campaign_weeks", 12)
    if cw <= 12:
        phases = [
            ("Phase 1", "Research & Setup", "Weeks 1-2", "Market analysis, account setup, creative development", "N/A", "Setup completion", "5%"),
            ("Phase 2", "Launch & Optimize", f"Weeks 3-{min(6, cw)}", "Initial campaign launch, A/B testing, bid optimization", "All programmatic", "CPC, CPA, Apply rate", "30%"),
            ("Phase 3", "Scale & Expand", f"Weeks {min(7, cw)}-{cw}", "Increase spend on top performers, add new channels", "Top 5 channels", "Cost per hire, Quality of hire", "40%"),
            ("Phase 4", "Sustain & Refine", "Ongoing", "Maintain performance, quarterly reviews, seasonal adjustments", "Proven channels", "ROI, Time to fill", "25%"),
        ]
    elif cw <= 26:
        phases = [
            ("Phase 1", "Research & Setup", "Weeks 1-3", "Market analysis, account setup, creative development, employer brand audit", "N/A", "Setup completion", "5%"),
            ("Phase 2", "Launch & Optimize", f"Weeks 4-{cw // 3}", "Initial campaign launch, A/B testing, bid optimization", "All programmatic", "CPC, CPA, Apply rate", "25%"),
            ("Phase 3", "Scale & Expand", f"Weeks {cw // 3 + 1}-{2 * cw // 3}", "Increase spend on top performers, add new channels, expand geo-targeting", "Top 5 channels", "Cost per hire, Quality of hire", "40%"),
            ("Phase 4", "Sustain & Refine", f"Weeks {2 * cw // 3 + 1}-{cw}", "Maintain performance, quarterly reviews, seasonal adjustments, ROI reporting", "Proven channels", "ROI, Time to fill", "30%"),
        ]
    else:
        # Long campaigns (6+ months)
        phases = [
            ("Phase 1", "Research & Setup", "Weeks 1-4", "Market analysis, account setup, creative development, employer brand audit", "N/A", "Setup completion", "5%"),
            ("Phase 2", "Launch & Optimize", f"Weeks 5-{cw // 4}", "Initial campaign launch, A/B testing, bid optimization, talent pipeline building", "All programmatic", "CPC, CPA, Apply rate", "20%"),
            ("Phase 3", "Scale & Expand", f"Weeks {cw // 4 + 1}-{cw // 2}", "Increase spend on top performers, add new channels, expand geo-targeting", "Top 5 channels", "Cost per hire, Quality of hire", "40%"),
            ("Phase 4", "Sustain & Refine", f"Weeks {cw // 2 + 1}-{cw}", "Maintain performance, quarterly reviews, seasonal adjustments, advanced ROI reporting", "Proven channels", "ROI, Time to fill, Retention", "35%"),
        ]

    for phase_key, phase_name, timeline, activities, channels, kpis, budget_pct in phases:
        phase_fill = phase_colors.get(phase_key, accent_fill)
        vals = [f"{phase_key}: {phase_name}", timeline, activities, channels, kpis, budget_pct]
        for i, v in enumerate(vals):
            cell = ws_timeline.cell(row=tl_row, column=2 + i, value=v)
            cell.font = body_font
            cell.alignment = wrap_alignment
            cell.border = thin_border
            cell.fill = phase_fill
        ws_timeline.cell(row=tl_row, column=2).font = Font(name="Calibri", bold=True, size=10)
        ws_timeline.row_dimensions[tl_row].height = 45
        tl_row += 1

    # Key Milestones section
    tl_row += 2
    style_section_header(ws_timeline, tl_row, 2, 7, "Key Milestones")
    tl_row += 1

    milestone_headers = ["Milestone", "Description"]
    for i, h in enumerate(milestone_headers):
        cell = ws_timeline.cell(row=tl_row, column=2 + i, value=h)
        cell.font = subheader_font
        cell.fill = subheader_fill
        cell.alignment = center_alignment
        cell.border = thin_border
    tl_row += 1

    # Build milestones based on actual campaign_weeks
    if cw <= 12:
        milestones = [
            ("Week 1", "Kick-off meeting & channel setup"),
            ("Week 2", "Creative assets approved, campaigns live"),
            ("Week 4", "First performance review"),
            (f"Week {max(6, cw // 2)}", "Mid-campaign optimization review"),
            (f"Week {cw}", "Full performance report & recommendations"),
        ]
    elif cw <= 26:
        milestones = [
            ("Week 1", "Kick-off meeting & channel setup"),
            ("Week 3", "Creative assets approved, campaigns live"),
            ("Week 6", "First performance review"),
            (f"Week {cw // 2}", "Mid-campaign optimization review"),
            (f"Week {3 * cw // 4}", "Quarterly performance report"),
            (f"Week {cw}", "Full performance report & recommendations"),
        ]
    else:
        milestones = [
            ("Week 1", "Kick-off meeting & channel setup"),
            ("Week 4", "Creative assets approved, campaigns live"),
            ("Week 8", "First performance review"),
            (f"Week {cw // 4}", "Q1 performance report"),
            (f"Week {cw // 2}", "Mid-campaign optimization review"),
            (f"Week {3 * cw // 4}", "Q3 performance report"),
            (f"Week {cw}", "Full performance report & recommendations"),
        ]
    for ms_label, ms_desc in milestones:
        cell_l = ws_timeline.cell(row=tl_row, column=2, value=ms_label)
        cell_l.font = Font(name="Calibri", bold=True, size=10)
        cell_l.border = thin_border
        cell_d = ws_timeline.cell(row=tl_row, column=3, value=ms_desc)
        cell_d.font = body_font
        cell_d.border = thin_border
        ws_timeline.merge_cells(f"C{tl_row}:G{tl_row}")
        if tl_row % 2 == 0:
            cell_l.fill = accent_fill
            cell_d.fill = accent_fill
        tl_row += 1

    # ── Optional: Educational Partners ──
    if data.get("include_educational"):
        ws_edu = wb.create_sheet("Educational Partners")
        ws_edu.sheet_properties.tabColor = "70AD47"
        ws_edu.column_dimensions["A"].width = 5
        ws_edu.column_dimensions["B"].width = 45
        ws_edu.column_dimensions["C"].width = 70
        ws_edu.merge_cells("B2:C2")
        ws_edu["B2"].value = "Educational Partners & Training Programs"
        ws_edu["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_edu["B2"].border = accent_bottom_border
        row = 4
        for i, h in enumerate(["Institution", "Talent Focus / Strategic Fit"]):
            cell = ws_edu.cell(row=row, column=2 + i, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = thin_border

        # USE RESEARCH MODULE for real universities
        partners = research.get_educational_partners(locations, industry)

        for p in partners:
            row += 1
            style_body_cell(ws_edu, row, 2, p.get("institution", ""))
            ws_edu.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_edu, row, 3, p.get("fit", ""))
            ws_edu.row_dimensions[row].height = 45

        # Add Joveo university job board partners
        uni_pubs = joveo_pubs.get("by_category", {}).get("University Job Board", [])
        if uni_pubs:
            row += 2
            ws_edu.merge_cells(start_row=row, start_column=2, end_row=row, end_column=3)
            ws_edu.cell(row=row, column=2, value="Joveo University Job Board Partners (for campus recruitment)").font = Font(name="Calibri", bold=True, size=12, color="2E75B6")
            row += 1
            for i, h in enumerate(["Platform", "Type"]):
                cell = ws_edu.cell(row=row, column=2 + i, value=h)
                cell.font = subheader_font
                cell.fill = subheader_fill
                cell.alignment = center_alignment
                cell.border = thin_border
            row += 1
            for pub in uni_pubs[:20]:
                style_body_cell(ws_edu, row, 2, pub)
                style_body_cell(ws_edu, row, 3, "University Job Board — Campus Recruiting Pipeline")
                row += 1

    # ── Optional: Events & Career Fairs ──
    if data.get("include_events"):
        ws_events = wb.create_sheet("Events & Career Fairs")
        ws_events.sheet_properties.tabColor = "70AD47"
        for col, w in [("A",5),("B",40),("C",22),("D",22),("E",45),("F",18),("G",18)]:
            ws_events.column_dimensions[col].width = w
        ws_events.merge_cells("B2:G2")
        ws_events["B2"].value = "Events & Career Fairs"
        ws_events["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_events["B2"].border = accent_bottom_border
        row = 4
        for i, h in enumerate(["Primary Partners", "Location", "Type", "Branding & Recruitment Impact", "Reach", "Budget Est."]):
            cell = ws_events.cell(row=row, column=2 + i, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = thin_border

        # USE RESEARCH MODULE for real events
        events = research.get_events(locations, industry)

        for evt in events:
            row += 1
            style_body_cell(ws_events, row, 2, evt.get("partner", ""))
            ws_events.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
            style_body_cell(ws_events, row, 3, evt.get("location", ""))
            style_body_cell(ws_events, row, 4, evt.get("type", ""))
            style_body_cell(ws_events, row, 5, evt.get("impact", ""))
            style_body_cell(ws_events, row, 6, evt.get("reach", ""))
            style_body_cell(ws_events, row, 7, evt.get("budget", ""))

    # ── Optional: Radio/Podcasts ──
    if data.get("include_radio_podcasts"):
        ws_radio = wb.create_sheet("Radio & Podcasts")
        ws_radio.sheet_properties.tabColor = "ED7D31"
        for col, w in [("A",5),("B",45),("C",25),("D",30),("E",35)]:
            ws_radio.column_dimensions[col].width = w
        ws_radio.merge_cells("B2:E2")
        ws_radio["B2"].value = "Radio & Podcasts"
        ws_radio["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_radio["B2"].border = accent_bottom_border
        row = 4
        for i, h in enumerate(["Channel / Station", "Weekly Listeners / Downloads", "Format / Genre", "Audience Type"]):
            cell = ws_radio.cell(row=row, column=2 + i, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = center_alignment
            cell.border = thin_border

        # USE RESEARCH MODULE for real radio/podcast data
        radio_data = research.get_radio_podcasts(locations, industry)

        # Separate local radio and podcasts
        local_stations = [r for r in radio_data if "downloads" not in r.get("listeners", "").lower()]
        podcasts = [r for r in radio_data if "downloads" in r.get("listeners", "").lower()]

        if local_stations:
            row += 1
            cell = ws_radio.cell(row=row, column=2, value="LOCAL RADIO STATIONS")
            cell.font = section_font
            cell.fill = section_fill
            cell.border = thin_border
            for c in range(3, 6):
                ws_radio.cell(row=row, column=c).fill = section_fill
                ws_radio.cell(row=row, column=c).border = thin_border

            for station in local_stations:
                row += 1
                style_body_cell(ws_radio, row, 2, station.get("name", ""))
                ws_radio.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_radio, row, 3, station.get("listeners", ""))
                style_body_cell(ws_radio, row, 4, station.get("genre", ""))
                style_body_cell(ws_radio, row, 5, station.get("audience", ""))

        if podcasts:
            row += 2
            cell = ws_radio.cell(row=row, column=2, value="INDUSTRY PODCASTS")
            cell.font = section_font
            cell.fill = section_fill
            cell.border = thin_border
            for c in range(3, 6):
                ws_radio.cell(row=row, column=c).fill = section_fill
                ws_radio.cell(row=row, column=c).border = thin_border

            for pod in podcasts:
                row += 1
                style_body_cell(ws_radio, row, 2, pod.get("name", ""))
                ws_radio.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10)
                style_body_cell(ws_radio, row, 3, pod.get("listeners", ""))
                style_body_cell(ws_radio, row, 4, pod.get("genre", ""))
                style_body_cell(ws_radio, row, 5, pod.get("audience", ""))

    # ── Optional: Media/Print Platforms ──
    if data.get("include_media_platforms"):
        ws_media = wb.create_sheet("Media & Print Platforms")
        ws_media.sheet_properties.tabColor = "ED7D31"
        for col, w in [("A",5),("B",40),("C",18),("D",32),("E",22),("F",18),("G",30)]:
            ws_media.column_dimensions[col].width = w

        # Comprehensive media/print platform data by industry
        media_platforms_db = {
            "healthcare_medical": {
                "label": "Healthcare & Medical",
                "trade_publications": [
                    ("JAMA Network", "Physicians, Surgeons, Researchers", "US + Global", "Employer profiles, classified ads, sponsored features"),
                    ("New England Journal of Medicine (NEJM)", "Physicians, Hospital Leaders", "Global", "Career center listings, display ads"),
                    ("American Journal of Nursing (AJN)", "RNs, NPs, Nurse Leaders", "US", "Print + digital job ads, career guides"),
                    ("Modern Healthcare", "Hospital Administrators, C-Suite", "US", "Sponsored content, job board integration"),
                    ("Becker's Hospital Review", "Health System Executives", "US", "Job listings, executive recruitment features"),
                    ("Nursing Times", "Nurses, Midwives, HCAs", "UK + Europe", "Recruitment supplements, career events"),
                ],
                "digital_media": [
                    ("Medscape", "1M+ Physicians, NPs, PAs (verified)", "Global", "Targeted recruitment ads by specialty & geography"),
                    ("Doximity", "80%+ of US Physicians", "US", "InMail recruiting, employer profiles, telehealth talent"),
                    ("Nurse.com", "RNs, LPNs, Travel Nurses", "US", "Job alerts, continuing education + recruitment combos"),
                    ("Health eCareers", "Allied Health, Clinical, Admin", "US", "Niche job board, resume database access"),
                    ("PracticeLink", "Physicians in active job search", "US", "Physician-only recruitment platform"),
                ],
                "ooh_print": [
                    ("Hospital Cafeteria / Break Room Flyers", "Current staff referrals", "Local", "Low-cost, high-trust referral channel"),
                    ("Medical Conference Programs (AAMC, AACN)", "Conference attendees by specialty", "US + Global", "Event program ads, booth placements"),
                    ("Nursing School Campus Boards", "New grad nurses, students", "Local", "Career fair flyers, campus partnerships"),
                    ("Transit Ads in Hospital Districts", "Local healthcare workers", "Regional", "Bus wraps, metro ads near medical centers"),
                ],
                "broadcast_audio": [
                    ("Healthcare NOW Radio", "Healthcare professionals, HIT", "US", "Sponsored segments, recruitment spots"),
                    ("The Nurses You Know Podcast", "Nursing professionals", "US", "Sponsorship, employer spotlight episodes"),
                    ("Spotify/Pandora Geo-targeted", "Commuting healthcare workers", "Regional", "Audio ads targeting healthcare interest segments"),
                ],
            },
            "technology_it": {
                "label": "Technology & IT",
                "trade_publications": [
                    ("IEEE Spectrum", "Engineers, Computer Scientists", "Global", "Career ads, employer sponsored features"),
                    ("Communications of the ACM", "Software Engineers, CS Researchers", "Global", "Career center, recruitment display ads"),
                    ("Wired Magazine", "Tech professionals, early adopters", "US + Global", "Employer brand features, career content"),
                    ("MIT Technology Review", "Tech leaders, AI/ML researchers", "Global", "Sponsored recruitment content, job listings"),
                    ("InfoWorld / Computerworld", "IT Managers, Developers, DevOps", "US", "Tech career guides, recruitment ads"),
                ],
                "digital_media": [
                    ("Stack Overflow Talent", "10M+ Monthly developers", "Global", "Targeted developer recruitment, company profiles, skills-based matching"),
                    ("GitHub (Employer Branding)", "100M+ Developer accounts", "Global", "Repository sponsorships, team pages, open-source community engagement"),
                    ("Hacker News (Who's Hiring)", "Senior engineers, startup talent", "Global", "Monthly hiring threads, YC company pages"),
                    ("Dev.to", "2M+ Developer community", "Global", "Employer listings, sponsored posts, community engagement"),
                    ("TechCrunch / The Verge Careers", "Tech industry professionals", "Global", "Sponsored job placements, brand articles"),
                    ("Dice.com", "IT professionals, contractors", "US", "Tech-specific job board, skills matching"),
                ],
                "ooh_print": [
                    ("Tech Conference Programs (CES, AWS re:Invent, Google I/O)", "Developers, CTOs, Architects", "Global", "Event sponsorships, booth recruitment"),
                    ("Tech Hub Billboard / Transit (SF, NYC, Austin, Seattle)", "Local tech workers", "Regional", "High-visibility employer brand ads"),
                    ("Hackathon Sponsorships", "Junior to mid developers", "Regional", "Direct talent pipeline + brand building"),
                    ("University CS Department Boards", "CS/Engineering students", "Local", "Campus recruiting, internship postings"),
                ],
                "broadcast_audio": [
                    ("Software Engineering Daily Podcast", "Senior developers, architects", "Global", "Recruitment sponsorships, employer spotlights"),
                    ("Changelog Podcast Network", "Open source developers", "Global", "Targeted dev recruitment, sponsor reads"),
                    ("Spotify Tech Playlist Targeting", "Tech workers by interest", "US + Global", "Audio ads targeting tech/coding playlists"),
                ],
            },
            "tech_engineering": {
                "label": "Technology & Engineering",
                "trade_publications": [
                    ("IEEE Spectrum", "Engineers, Computer Scientists", "Global", "Career ads, employer sponsored features"),
                    ("Communications of the ACM", "Software Engineers, CS Researchers", "Global", "Career center, recruitment display ads"),
                    ("Wired Magazine", "Tech professionals, early adopters", "US + Global", "Employer brand features, career content"),
                    ("MIT Technology Review", "Tech leaders, AI/ML researchers", "Global", "Sponsored recruitment content, job listings"),
                ],
                "digital_media": [
                    ("Stack Overflow Talent", "10M+ Monthly developers", "Global", "Targeted developer recruitment, company profiles, skills-based matching"),
                    ("GitHub (Employer Branding)", "100M+ Developer accounts", "Global", "Repository sponsorships, team pages, open-source engagement"),
                    ("Hacker News (Who's Hiring)", "Senior engineers, startup talent", "Global", "Monthly hiring threads"),
                    ("Dice.com", "IT professionals, contractors", "US", "Tech-specific job board, skills matching"),
                ],
                "ooh_print": [
                    ("Tech Conference Programs (CES, AWS re:Invent)", "Developers, CTOs, Architects", "Global", "Event sponsorships, booth recruitment"),
                    ("University CS Department Boards", "CS/Engineering students", "Local", "Campus recruiting, internship postings"),
                ],
                "broadcast_audio": [
                    ("Software Engineering Daily Podcast", "Senior developers, architects", "Global", "Recruitment sponsorships"),
                    ("Spotify Tech Playlist Targeting", "Tech workers by interest", "Global", "Audio ads targeting tech playlists"),
                ],
            },
            "blue_collar_trades": {
                "label": "Blue Collar & Skilled Trades",
                "trade_publications": [
                    ("Contractor Magazine", "Plumbing, HVAC, Mechanical contractors", "US", "Classified job ads, workforce features"),
                    ("Electrical Contractor (EC&M)", "Electricians, Electrical contractors", "US", "Career section, recruitment ads"),
                    ("Welding Journal (AWS)", "Welders, fabricators, inspectors", "US + Global", "Certified welder recruitment ads"),
                    ("Fine Homebuilding / JLC", "Carpenters, builders, remodelers", "US", "Help wanted ads, workforce content"),
                    ("Plumbing & Mechanical Magazine", "Plumbers, pipefitters", "US", "Trade recruitment, apprenticeship ads"),
                ],
                "digital_media": [
                    ("Tradesmen International", "All skilled trades", "US", "Skilled labor marketplace, staffing"),
                    ("iHireConstruction / iHireMaintenance", "Construction & maintenance workers", "US", "Niche job boards for trades"),
                    ("SkillsUSA / Trade School Job Boards", "Vocational students, apprentices", "US", "Early career pipeline, school partnerships"),
                    ("Facebook Local Groups (Trade Work)", "Local tradespeople", "Regional", "Community-based recruiting, local job posts"),
                    ("NextDoor Neighborhood Ads", "Local residents seeking work", "Regional", "Hyperlocal recruitment for trades"),
                ],
                "ooh_print": [
                    ("Home Depot / Lowe's Community Boards", "DIY-ers, handymen, trade workers", "Local", "Flyer postings, partnership programs"),
                    ("Union Halls & Trade School Bulletin Boards", "Apprentices, journeymen", "Local", "Direct trade community access"),
                    ("Truck Stop / Gas Station Postings", "CDL drivers, field workers", "Regional", "Highway corridor reach"),
                    ("Local Newspaper Classifieds", "Blue collar job seekers", "Local", "Traditional but trusted channel"),
                    ("Community Recreation Centers", "Local job seekers", "Local", "Flyers, job fair postings"),
                ],
                "broadcast_audio": [
                    ("Local AM/FM Radio (Drive Time)", "Commuting tradespeople", "Regional", "30-60 sec recruitment spots during commute hours"),
                    ("Country / Classic Rock Station Sponsorships", "Blue collar demographics", "Regional", "Recruitment spots on trade-friendly formats"),
                    ("iHeartRadio Local Targeting", "Regional workers by zip code", "Regional", "Digital audio ads targeted by location"),
                ],
            },
            "logistics_supply_chain": {
                "label": "Logistics & Supply Chain",
                "trade_publications": [
                    ("Supply Chain Management Review", "SCM directors, VP Operations", "US + Global", "Executive recruitment, employer profiles"),
                    ("Logistics Management", "Logistics managers, warehouse directors", "US", "Career section, recruitment display ads"),
                    ("Transport Topics", "Trucking, fleet operators, CDL drivers", "US", "Driver recruitment ads, fleet hiring features"),
                    ("Journal of Commerce (JOC)", "Shipping, freight, customs professionals", "Global", "Trade recruitment, sponsored content"),
                    ("Inbound Logistics", "Supply chain, procurement professionals", "US", "Recruitment ads, workforce articles"),
                ],
                "digital_media": [
                    ("CDLjobs.com", "CDL truck drivers", "US", "Driver-specific job board, 500K+ monthly visitors"),
                    ("FreightWaves", "Freight, trucking, logistics professionals", "US", "News site, career section, employer brand"),
                    ("SupplyChainBrain", "SC professionals all levels", "US + Global", "Job board, webinar sponsorships"),
                    ("Indeed CDL Driver Campaigns", "CDL-A/B drivers, delivery drivers", "US", "Programmatic driver recruitment"),
                    ("Facebook Driver Groups", "OTR, local, regional drivers", "US", "Community-based driver recruiting"),
                ],
                "ooh_print": [
                    ("Truck Stop Advertising (Pilot/Flying J, Love's)", "Long-haul truck drivers", "US Nationwide", "Poster ads, digital screens at fuel islands"),
                    ("Warehouse District Billboards", "Warehouse workers, forklift operators", "Regional", "High-traffic logistics corridor visibility"),
                    ("Distribution Center Break Rooms", "Current warehouse employees (referrals)", "Local", "Internal referral flyers, QR code postings"),
                    ("CDL School Campus Boards", "New CDL holders, students", "Regional", "Direct pipeline from driving schools"),
                ],
                "broadcast_audio": [
                    ("Road Dog Trucking (SiriusXM Ch 146)", "OTR truck drivers", "US Nationwide", "Recruitment spots during long hauls"),
                    ("TruckersMP / Trucker Path App Ads", "Active truck drivers", "US", "In-app recruitment ads"),
                    ("Spotify/Pandora Geo-targeted (Warehouse zones)", "Warehouse workers by geography", "Regional", "Audio ads near fulfillment centers"),
                ],
            },
            "retail_consumer": {
                "label": "Retail & Consumer",
                "trade_publications": [
                    ("Retail Dive", "Retail executives, buyers, merchandisers", "US", "Recruitment display ads, employer profiles"),
                    ("NRF SmartBrief / Stores Magazine", "NRF members, retail leaders", "US", "Career content, sponsored recruitment"),
                    ("Chain Store Age", "Chain retail management", "US", "Recruitment ads, workforce features"),
                    ("Progressive Grocer", "Grocery retail managers, buyers", "US", "Supermarket industry recruitment"),
                ],
                "digital_media": [
                    ("Snagajob / Snag", "Hourly retail & service workers", "US", "Hourly job platform, 100M+ job seekers"),
                    ("RetailCareers.com", "Retail management, corporate retail", "US + Global", "Retail-specific job board"),
                    ("Facebook Local Job Posts", "Local hourly job seekers", "Regional", "Free + paid job postings, high engagement"),
                    ("TikTok Recruitment (Retail brands)", "Gen Z retail workers", "US + Global", "Short-form employer brand content"),
                    ("Google Local Jobs", "In-store, hourly, seasonal workers", "Regional", "High-intent local job search"),
                ],
                "ooh_print": [
                    ("In-Store 'Now Hiring' Signage", "Walk-in candidates, shoppers", "Local", "Highest-converting retail recruitment channel"),
                    ("Shopping Mall Directory Boards", "Foot traffic job seekers", "Local", "Multi-store hiring event ads"),
                    ("Local Community Bulletin Boards", "Neighborhood residents", "Local", "Free/low-cost community reach"),
                    ("Bus Stop / Transit Shelter Ads", "Commuters, entry-level workers", "Regional", "High-visibility in retail corridors"),
                    ("College Campus Boards (Part-time)", "Students seeking part-time work", "Local", "Campus partnerships for seasonal hires"),
                ],
                "broadcast_audio": [
                    ("Local Radio (Pop, Urban, Top 40)", "Younger workforce 18-35", "Regional", "Drive-time recruitment spots"),
                    ("Spotify Free Tier Ads", "Younger demographics", "Regional", "Audio/display ads between songs"),
                    ("Retail podcast sponsorships", "Retail professionals", "US", "Employer brand content"),
                ],
            },
            "hospitality_travel": {
                "label": "Hospitality & Travel",
                "trade_publications": [
                    ("Hotel Management", "Hotel GMs, front office, F&B directors", "US", "Career section, recruitment ads"),
                    ("Restaurant Business / Nation's Restaurant News", "Restaurant managers, chefs, FOH", "US", "Recruitment classifieds, workforce features"),
                    ("Travel Weekly", "Travel agents, tour operators", "US + Global", "Career ads, industry recruitment"),
                    ("Lodging Magazine", "Hospitality management, revenue mgrs", "US", "Recruitment display ads"),
                ],
                "digital_media": [
                    ("Hcareers", "Hotel, restaurant, casino, resort jobs", "US + Global", "Hospitality-specific job board"),
                    ("Poached Jobs", "Restaurant, bar, hotel staff", "US", "Culinary & hospitality talent marketplace"),
                    ("Culinary Agents", "Chefs, cooks, sommeliers, F&B", "US", "Food service recruitment platform"),
                    ("Hosco", "Hospitality students & professionals", "Global", "International hospitality careers"),
                    ("Instagram / TikTok (Behind the scenes)", "Younger hospitality workers", "Global", "Employer brand, day-in-the-life content"),
                ],
                "ooh_print": [
                    ("Culinary School Campus Boards", "Culinary students, externs", "Local", "Chef and kitchen pipeline"),
                    ("Hotel Lobby / Employee Entrance Postings", "Walk-in applicants, current referrals", "Local", "High-conversion referral channel"),
                    ("Tourism District Billboards / Transit", "Service workers in tourism areas", "Regional", "Seasonal hiring visibility"),
                    ("Restaurant Supply Store Boards", "Kitchen staff, restaurant workers", "Local", "Industry-adjacent community reach"),
                ],
                "broadcast_audio": [
                    ("Food & restaurant podcasts (Bon Appétit, Eater)", "Culinary professionals, foodies", "US + Global", "Sponsorship, employer spotlight"),
                    ("Local radio — morning drive", "Service industry shift workers", "Regional", "Recruitment spots before shift start"),
                ],
            },
            "finance_banking": {
                "label": "Finance & Banking",
                "trade_publications": [
                    ("Financial Times", "Finance professionals, banking executives", "Global", "Recruitment display ads, career section"),
                    ("Wall Street Journal", "Financial analysts, fund managers", "Global", "Career section, executive recruitment"),
                    ("American Banker", "Banking professionals, compliance, risk", "US", "Industry recruitment, employer profiles"),
                    ("The Economist", "Economists, policy analysts, finance leaders", "Global", "Career ads, sponsored employer content"),
                    ("Barron's", "Investment professionals, wealth managers", "US", "Career section, premium recruitment ads"),
                ],
                "digital_media": [
                    ("eFinancialCareers", "Investment banking, asset management", "Global", "Finance-specific job board, 2M+ monthly visitors"),
                    ("Wall Street Oasis", "IB analysts, PE/VC professionals", "Global", "Community + job board, employer AMAs"),
                    ("Bloomberg Careers", "Financial data, analytics professionals", "Global", "Premium finance recruitment"),
                    ("Investopedia Career Center", "Financial advisors, analysts", "US", "Career content, job listings"),
                    ("The Muse / Vault Finance", "Early-career finance professionals", "US", "Employer profiles, career guides"),
                ],
                "ooh_print": [
                    ("Financial District Transit Ads (NYC, London, HK)", "Finance commuters", "Regional", "High-visibility in financial hubs"),
                    ("CFA/CPA Exam Prep Center Boards", "Finance candidates pursuing credentials", "US + Global", "Targeted credential-track talent"),
                    ("University Business School Boards", "MBA, finance students", "Regional", "On-campus recruitment, career fair ads"),
                ],
                "broadcast_audio": [
                    ("Bloomberg Radio / TV", "Finance professionals, traders", "Global", "Recruitment sponsorships, career segments"),
                    ("Financial podcasts (Odd Lots, Money Stuff)", "Finance enthusiasts, professionals", "Global", "Sponsor reads, employer features"),
                    ("CNBC / Fox Business Digital", "Finance decision-makers", "US", "Recruitment display + video ads"),
                ],
            },
            "aerospace_defense": {
                "label": "Aerospace & Defense",
                "trade_publications": [
                    ("Aviation Week & Space Technology", "Aerospace engineers, program managers", "Global", "Career center, recruitment display ads"),
                    ("Defense News", "Defense contractors, military-transition", "US + Global", "Recruitment features, career section"),
                    ("Jane's Defence Weekly", "Defense & security professionals", "Global", "Premium recruitment listings"),
                    ("SpaceNews", "Space industry engineers, scientists", "Global", "Space sector recruitment ads"),
                ],
                "digital_media": [
                    ("ClearedJobs.net", "Security-cleared professionals", "US", "Cleared talent job board, virtual career fairs"),
                    ("Military.com", "Veterans, military-transition professionals", "US", "Largest military community, job board"),
                    ("Defense One", "Defense policy, technology professionals", "US", "Sponsored recruitment content"),
                    ("AIAA Career Center", "Aerospace engineers, researchers", "Global", "Professional association job board"),
                    ("AviationJobSearch", "Pilots, aviation maintenance, aerospace", "Global", "Aviation-specific recruitment platform"),
                ],
                "ooh_print": [
                    ("Defense Industry Conference Programs (AUSA, AAAA)", "Defense professionals, military leaders", "US + Global", "Conference recruitment booths, program ads"),
                    ("Military Base Transition Centers", "Separating service members", "US", "High-quality veteran pipeline"),
                    ("Aerospace Museum / Air Show Programs", "Aviation enthusiasts, STEM talent", "Regional", "Brand visibility + passive recruitment"),
                ],
                "broadcast_audio": [
                    ("The Aerospace Executive Podcast", "Aerospace leaders, engineers", "US", "Recruitment sponsorships"),
                    ("Defense & Aerospace Report", "Defense industry professionals", "US", "Sponsor reads, career features"),
                ],
            },
            "pharma_biotech": {
                "label": "Pharma & Biotech",
                "trade_publications": [
                    ("Nature", "Scientists, researchers, PhD professionals", "Global", "Career section, recruitment ads, employer profiles"),
                    ("The Lancet", "Clinical researchers, physicians", "Global", "Medical recruitment, career features"),
                    ("Science Magazine (AAAS)", "STEM researchers, postdocs", "Global", "Career center, employer ads"),
                    ("STAT News", "Biopharma, health, science reporters & pros", "US", "Sponsored employer content, job listings"),
                    ("Genetic Engineering & Biotechnology News (GEN)", "Biotech lab professionals", "US + Global", "Niche biotech recruitment ads"),
                ],
                "digital_media": [
                    ("BioSpace", "Life science professionals all levels", "US", "Biotech/pharma job board, 1M+ monthly visitors"),
                    ("PharmiWeb", "Pharmaceutical, CRO, medical device", "Global", "Pharma-specific recruitment platform"),
                    ("Endpoints News", "Biopharma executives, clinical teams", "US", "Sponsored content, employer brand features"),
                    ("ResearchGate Jobs", "Academic researchers, PhD scientists", "Global", "Research community recruitment"),
                    ("MedReps", "Medical sales, device reps, pharma sales", "US", "Sales-focused life science job board"),
                ],
                "ooh_print": [
                    ("Biotech Hub Transit Ads (Boston, SF, San Diego, RTP)", "Life science commuters", "Regional", "High-visibility in biotech clusters"),
                    ("Scientific Conference Programs (ASCO, ASH, JPM)", "Researchers, clinical professionals", "Global", "Conference career fair ads, booth recruitment"),
                    ("University Lab / Science Building Boards", "Postdocs, PhD students, researchers", "Local", "Academic pipeline for industry positions"),
                ],
                "broadcast_audio": [
                    ("This Week in Virology / Science podcasts", "Scientists, researchers", "Global", "Recruitment sponsorships in science media"),
                    ("STAT News Podcast", "Biopharma professionals", "US", "Industry recruitment sponsor reads"),
                ],
            },
            "energy_utilities": {
                "label": "Energy & Utilities",
                "trade_publications": [
                    ("Power Engineering", "Power plant operators, engineers", "US", "Career section, recruitment ads"),
                    ("Oil & Gas Journal", "Petroleum engineers, field operators", "Global", "O&G industry recruitment"),
                    ("Renewable Energy World", "Solar, wind, energy storage pros", "Global", "Clean energy recruitment ads"),
                    ("Utility Dive", "Utility executives, grid engineers", "US", "Career content, sponsored recruitment"),
                    ("T&D World", "Transmission & distribution professionals", "US", "Utility workforce recruitment"),
                ],
                "digital_media": [
                    ("Rigzone", "Oil & gas professionals globally", "Global", "O&G job board, 1.5M+ monthly visitors"),
                    ("EnergyJobline", "All energy sector professionals", "Global", "Cross-energy recruitment platform"),
                    ("CleanTechnica Jobs", "Renewable energy professionals", "Global", "Clean energy job board"),
                    ("Utility Jobs Online", "Electric, gas, water utility workers", "US", "Utility-specific recruitment"),
                ],
                "ooh_print": [
                    ("Energy Conference Programs (CERAWeek, Solar Power Intl)", "Energy executives, engineers", "Global", "Conference recruitment, booth presence"),
                    ("Oil Field / Pipeline Corridor Billboards", "Field workers, operators", "Regional", "Reach workers in energy production areas"),
                    ("Trade Union Halls (IBEW, UWUA)", "Lineworkers, electricians, utility workers", "US", "Union-pathway recruitment"),
                ],
                "broadcast_audio": [
                    ("Energy podcasts (The Energy Gang, Columbia Energy Exchange)", "Energy professionals, policy makers", "US + Global", "Recruitment sponsorships"),
                    ("Local radio in energy production regions (Permian, Bakken)", "Oilfield workers, plant operators", "Regional", "Geo-targeted recruitment spots"),
                ],
            },
            "education": {
                "label": "Education",
                "trade_publications": [
                    ("The Chronicle of Higher Education", "University faculty, administrators", "US", "Premier higher ed recruitment publication"),
                    ("Education Week", "K-12 teachers, administrators, policy", "US", "K-12 recruitment ads, career section"),
                    ("Inside Higher Ed", "College faculty, staff, administrators", "US", "Job board, employer profiles"),
                    ("Times Higher Education (THE)", "International university faculty", "Global", "Global academic recruitment"),
                ],
                "digital_media": [
                    ("HigherEdJobs", "College & university positions", "US", "Largest higher ed job board"),
                    ("SchoolSpring / Frontline Education", "K-12 teachers, staff", "US", "K-12 school recruitment platform"),
                    ("Teach Away", "International teaching positions", "Global", "International school recruitment"),
                    ("Indeed Education Campaigns", "All education roles", "US", "Broad education recruitment"),
                ],
                "ooh_print": [
                    ("Education Conference Programs (ISTE, ASCD, AERA)", "Educators, administrators", "US + Global", "Conference career fair recruitment"),
                    ("College of Education Bulletin Boards", "Student teachers, new graduates", "Local", "Pipeline for new teacher hires"),
                    ("School District Office Boards", "Current staff referrals, substitutes", "Local", "Community recruitment reach"),
                ],
                "broadcast_audio": [
                    ("Education podcasts (Cult of Pedagogy, EdSurge)", "Teachers, instructional designers", "US", "Recruitment sponsorships"),
                    ("NPR / Public Radio Underwriting", "Educated demographics, teachers", "Regional", "Premium audience, employer brand"),
                ],
            },
            "construction_real_estate": {
                "label": "Construction & Real Estate",
                "trade_publications": [
                    ("Engineering News-Record (ENR)", "Construction executives, PMs, engineers", "US + Global", "Industry recruitment leader, career center"),
                    ("Construction Dive", "Construction managers, superintendents", "US", "Recruitment ads, workforce features"),
                    ("Builder Magazine", "Homebuilders, developers", "US", "Residential construction recruitment"),
                    ("Real Estate Forum / GlobeSt", "CRE professionals, brokers, developers", "US", "Commercial RE recruitment"),
                ],
                "digital_media": [
                    ("ConstructionJobs.com", "All construction trades & management", "US", "Construction-specific job board"),
                    ("iHireConstruction", "Laborers, operators, supervisors", "US", "Niche construction recruitment"),
                    ("Procore Community Jobs", "Construction tech, project management", "US", "Construction software community"),
                    ("Zillow / Realtor.com Careers", "Real estate agents, property managers", "US", "Real estate recruitment"),
                ],
                "ooh_print": [
                    ("Construction Supply Store Boards (Home Depot Pro)", "Contractors, construction workers", "Local", "Industry-adjacent community reach"),
                    ("Job Site Fence Banners / QR Codes", "Passersby, local trade workers", "Local", "High-visibility at active construction sites"),
                    ("Union Halls (Carpenters, Laborers, Operating Engineers)", "Union tradespeople", "Regional", "Union apprenticeship pipeline"),
                ],
                "broadcast_audio": [
                    ("Construction-focused radio (AM talk, sports)", "Construction worker commuters", "Regional", "Drive-time recruitment spots"),
                    ("Construction podcasts (ConTech Crew)", "Construction professionals", "US", "Recruitment sponsorships"),
                ],
            },
            "food_beverage": {
                "label": "Food & Beverage",
                "trade_publications": [
                    ("Food Processing Magazine", "Food manufacturing, QA, plant managers", "US", "Industry recruitment, career section"),
                    ("Beverage Industry", "Beverage production, distribution", "US", "Beverage sector recruitment"),
                    ("Food Engineering", "Food plant engineers, operations", "US", "Engineering recruitment in food mfg"),
                    ("QSR Magazine", "Quick service restaurant management", "US", "Restaurant industry recruitment"),
                ],
                "digital_media": [
                    ("Poached Jobs", "Restaurant, bar, culinary talent", "US", "Hospitality talent marketplace"),
                    ("FoodGrads", "Food science, food safety graduates", "US + Canada", "Entry-level food industry recruitment"),
                    ("CareerBuilder Food & Bev", "Food manufacturing, plant workers", "US", "Broad food industry recruitment"),
                    ("Facebook Local Restaurant Groups", "Line cooks, servers, kitchen staff", "Regional", "Community-based recruiting"),
                ],
                "ooh_print": [
                    ("Food Industry Trade Show Programs (IFT, NRA Show)", "Food scientists, chefs, F&B execs", "US + Global", "Conference recruitment and brand visibility"),
                    ("Restaurant Supply Store Boards", "Kitchen staff, restaurant workers", "Local", "Industry community reach"),
                    ("Food Plant Employee Entrance Postings", "Current employee referrals", "Local", "Referral-based internal recruitment"),
                ],
                "broadcast_audio": [
                    ("Food podcasts (Bon Appétit, Gastropod)", "Culinary professionals, food scientists", "US + Global", "Recruitment sponsorships"),
                    ("Local radio near food production facilities", "Plant workers, line operators", "Regional", "Geo-targeted recruitment spots"),
                ],
            },
            "telecommunications": {
                "label": "Telecommunications",
                "trade_publications": [
                    ("Light Reading", "Telecom engineers, network architects", "Global", "Carrier & vendor recruitment ads"),
                    ("FierceTelecom / FierceWireless", "Telecom executives, 5G professionals", "US + Global", "Recruitment features, career section"),
                    ("RCR Wireless News", "Wireless industry professionals", "Global", "Wireless/5G recruitment ads"),
                ],
                "digital_media": [
                    ("TelecomCareers.net", "All telecom roles", "US", "Telecom-specific job board"),
                    ("Dice.com (Telecom/Network)", "Network engineers, telecom IT", "US", "Tech job board with telecom filter"),
                    ("LinkedIn Telecom Groups", "Telecom professionals by specialty", "Global", "Group recruiting, sponsored posts"),
                ],
                "ooh_print": [
                    ("MWC / CES Conference Programs", "Telecom executives, engineers", "Global", "Event recruitment, booth hiring"),
                    ("Telecom facility area billboards", "Tower technicians, field engineers", "Regional", "Local recruitment near infrastructure"),
                ],
                "broadcast_audio": [
                    ("Telecom podcasts (Light Reading, Fierce)", "Telecom professionals", "Global", "Recruitment sponsorships"),
                ],
            },
            "media_entertainment": {
                "label": "Media & Entertainment",
                "trade_publications": [
                    ("Variety", "Entertainment industry professionals", "Global", "Recruitment ads, career classifieds"),
                    ("The Hollywood Reporter", "Film, TV, media executives", "Global", "Entertainment career section"),
                    ("Broadcasting & Cable", "Broadcast, streaming professionals", "US", "Media industry recruitment"),
                    ("AdAge / AdWeek", "Marketing, advertising, creative pros", "US + Global", "Agency & brand recruitment"),
                ],
                "digital_media": [
                    ("Mandy.com", "Actors, crew, production staff", "Global", "Entertainment production job board"),
                    ("ProductionHub", "Film & video production professionals", "US", "Production crew recruitment"),
                    ("Mediabistro", "Media, journalism, content professionals", "US", "Media industry job board"),
                    ("CreativePool / Behance Jobs", "Designers, animators, creative talent", "Global", "Creative talent marketplace"),
                ],
                "ooh_print": [
                    ("Film/Media Festival Programs (Sundance, SXSW, NAB Show)", "Creative & production professionals", "Global", "Event recruitment, networking"),
                    ("Studio Lot / Production Office Boards", "Production crew, assistants", "Local", "Industry insider recruitment"),
                ],
                "broadcast_audio": [
                    ("Entertainment industry podcasts (The Business, Scriptnotes)", "Writers, producers, industry pros", "US + Global", "Recruitment sponsorships"),
                ],
            },
            "insurance": {
                "label": "Insurance",
                "trade_publications": [
                    ("Insurance Journal", "Insurance agents, underwriters, adjusters", "US", "Insurance industry recruitment ads"),
                    ("Best's Review (AM Best)", "Insurance executives, actuaries", "US + Global", "Premium insurance career listings"),
                    ("National Underwriter", "P&C, life/health underwriters", "US", "Underwriting recruitment classifieds"),
                ],
                "digital_media": [
                    ("InsuranceJobs.com", "All insurance roles", "US", "Insurance-specific job board"),
                    ("The Institutes Career Center", "Certified insurance professionals (CPCU, ARM)", "US", "Credential-based recruitment"),
                    ("Actuarial Outpost / GoActuary", "Actuaries, actuarial students", "US + Global", "Actuarial recruitment platform"),
                ],
                "ooh_print": [
                    ("Insurance Industry Conference Programs (RIMS, CPCU)", "Insurance professionals", "US + Global", "Conference career fairs, program ads"),
                    ("Actuarial exam prep center boards", "Aspiring actuaries", "US", "Pipeline recruitment for actuarial talent"),
                ],
                "broadcast_audio": [
                    ("Insurance podcasts (Insurance Journal, Carrier Mgmt)", "Insurance professionals", "US", "Recruitment sponsorships"),
                ],
            },
            "legal_services": {
                "label": "Legal Services",
                "trade_publications": [
                    ("The American Lawyer", "BigLaw attorneys, firm management", "US", "Legal recruitment ads, lateral classifieds"),
                    ("National Law Journal", "Attorneys, judges, legal professionals", "US", "Career section, recruitment display ads"),
                    ("ABA Journal", "All practicing attorneys (ABA members)", "US", "Bar association recruitment listings"),
                    ("Law360", "In-house counsel, litigators, transactional", "US", "Legal news + career center"),
                ],
                "digital_media": [
                    ("LawCrossing", "Attorney, paralegal, legal staff", "US", "Legal-specific job aggregator"),
                    ("Robert Half Legal / Special Counsel", "Legal professionals, temp-to-perm", "US", "Legal staffing + recruitment"),
                    ("Above the Law", "Associates, law students, in-house counsel", "US", "Legal career content, employer features"),
                    ("NALP / Law School Career Centers", "Law students, recent JD graduates", "US", "Pipeline from law schools"),
                ],
                "ooh_print": [
                    ("Bar Association Event Programs (ABA, State Bars)", "Practicing attorneys", "US", "CLE event career fairs, program ads"),
                    ("Law School Campus Boards (Top 50)", "Law students, 1L-3L", "Regional", "On-campus legal recruitment"),
                ],
                "broadcast_audio": [
                    ("Legal podcasts (Strict Scrutiny, Lawyer 2 Lawyer)", "Attorneys, legal professionals", "US", "Recruitment sponsorships"),
                ],
            },
            "automotive": {
                "label": "Automotive & Manufacturing",
                "trade_publications": [
                    ("Automotive News", "Auto industry executives, dealers", "US + Global", "Automotive recruitment leader"),
                    ("SAE International Publications", "Automotive engineers, EV specialists", "Global", "Engineering recruitment, career center"),
                    ("IndustryWeek", "Manufacturing plant managers, directors", "US", "Manufacturing recruitment ads"),
                    ("Assembly Magazine", "Manufacturing, assembly professionals", "US", "Plant-level recruitment ads"),
                ],
                "digital_media": [
                    ("AutoJobs.com", "Dealership, automotive service, manufacturing", "US", "Auto industry job board"),
                    ("iHireManufacturing", "Manufacturing all levels", "US", "Manufacturing-specific recruitment"),
                    ("Engineering.com Jobs", "Mechanical, manufacturing engineers", "Global", "Engineering recruitment platform"),
                    ("DealerSocket / Hireology (Dealers)", "Dealership staff recruitment", "US", "Automotive retail recruitment"),
                ],
                "ooh_print": [
                    ("Auto Shows (NAIAS, LA Auto Show) Programs", "Automotive professionals, enthusiasts", "US + Global", "Event recruitment, brand visibility"),
                    ("Manufacturing Plant Corridor Boards", "Current employees (referrals), temp workers", "Local", "Internal referral recruitment"),
                    ("Vocational School Auto Programs", "Auto tech students, mechanics", "Local", "Technician pipeline recruitment"),
                ],
                "broadcast_audio": [
                    ("Automotive podcasts (Autoline, The Drive)", "Auto industry professionals", "US + Global", "Recruitment sponsorships"),
                    ("Local radio near manufacturing plants", "Manufacturing workers, shift employees", "Regional", "Geo-targeted recruitment spots"),
                ],
            },
            "military_recruitment": {
                "label": "Military Recruitment",
                "trade_publications": [
                    ("Military Times (Army/Navy/Air Force/Marine Times)", "Active duty, veterans, military families", "US", "Premier military recruitment publication"),
                    ("Stars and Stripes", "Overseas military, DoD civilians", "Global", "Overseas military community recruitment"),
                    ("G.I. Jobs Magazine", "Transitioning service members", "US", "Military-to-civilian career guide"),
                ],
                "digital_media": [
                    ("Military.com", "5M+ Veterans, active duty, families", "US", "Largest military job board & community"),
                    ("Hire Heroes USA", "Veteran job seekers", "US", "Nonprofit veteran career placement"),
                    ("RecruitMilitary", "Military-experienced professionals", "US", "Veteran job fairs + job board"),
                    ("USAJOBS (Federal)", "Veterans seeking federal employment", "US", "Government career portal"),
                ],
                "ooh_print": [
                    ("Military Base Transition Assistance (TAP) Centers", "Separating service members", "US + Global", "Direct military-to-civilian pipeline"),
                    ("VFW / American Legion Posts", "Veterans community", "US", "Community-based veteran recruitment"),
                    ("Military Installation Bulletin Boards", "Active duty, dependents", "US", "On-base job postings"),
                ],
                "broadcast_audio": [
                    ("Armed Forces Radio (AFN)", "Active duty service members worldwide", "Global", "Overseas military recruitment"),
                    ("Veteran podcasts (Borne the Battle, Jocko Podcast)", "Veterans, military-transition", "US", "Recruitment sponsorships, employer spotlights"),
                ],
            },
            "maritime_marine": {
                "label": "Maritime & Marine",
                "trade_publications": [
                    ("Maritime Executive", "Ship officers, port management, naval architects", "Global", "Maritime career section, recruitment ads"),
                    ("Marine Log", "Shipbuilding, marine engineering", "US", "Marine industry recruitment"),
                    ("TradeWinds", "Shipping, offshore, maritime executives", "Global", "Shipping industry career classifieds"),
                    ("WorkBoat Magazine", "Workboat operators, inland waterway pros", "US", "Domestic maritime recruitment"),
                ],
                "digital_media": [
                    ("MarineLink / Maritime Jobs", "All maritime roles globally", "Global", "Maritime-specific job board"),
                    ("Crew4Yachts / dockwalk", "Yacht crew, superyacht positions", "Global", "Luxury maritime recruitment"),
                    ("GCAPTAIN", "Maritime professionals, ship captains", "Global", "Maritime news + career section"),
                ],
                "ooh_print": [
                    ("Port Authority / Seamen's Church Boards", "Mariners, dockworkers, port staff", "Regional", "Direct maritime community recruitment"),
                    ("Maritime Academy Campus Boards", "Maritime cadets, new graduates", "US", "Maritime officer pipeline"),
                    ("Ship Chandlery / Maritime Supply Stores", "Active mariners", "Regional", "Industry community postings"),
                ],
                "broadcast_audio": [
                    ("Maritime podcasts (Maritime Podcast, The Shipping Podcast)", "Shipping & maritime professionals", "Global", "Recruitment sponsorships"),
                ],
            },
        }
        # Add general/entry-level as fallback
        media_platforms_db["general_entry_level"] = {
            "label": "General / Entry-Level",
            "trade_publications": [
                ("Local Newspapers (Classifieds Section)", "General job seekers, all levels", "Local", "Traditional recruitment, broad reach"),
                ("Community Newspapers / Weeklies", "Local residents, hourly workers", "Local", "Affordable, community-trusted channel"),
            ],
            "digital_media": [
                ("Indeed Sponsored Jobs", "All job seekers, 250M+ monthly visitors", "Global", "Largest job site, broad programmatic reach"),
                ("Facebook Jobs", "Hourly, entry-level, local job seekers", "Global", "Social recruiting, community groups"),
                ("Google for Jobs", "High-intent job searchers", "Global", "Organic + paid local job visibility"),
                ("Craigslist Jobs", "Local hourly, entry-level, gig workers", "US", "Low-cost local recruitment"),
            ],
            "ooh_print": [
                ("Community Bulletin Boards (Libraries, Rec Centers)", "Local job seekers", "Local", "Free community reach"),
                ("Public Transit Ads (Bus, Subway, Shelters)", "Commuting workers, entry-level", "Regional", "High-frequency visibility"),
                ("Grocery Store / Laundromat Boards", "Neighborhood residents seeking work", "Local", "Hyperlocal community reach"),
            ],
            "broadcast_audio": [
                ("Local Radio (Mix of formats)", "General audience by demographics", "Regional", "Broad recruitment reach, drive-time slots"),
                ("Spotify/Pandora Geo-targeted Ads", "Younger demographics by location", "Regional", "Digital audio recruitment ads"),
            ],
        }

        # Get the platform data for the client's industry (with fallback)
        ind_key = industry
        if ind_key not in media_platforms_db:
            # Try mapping some common aliases
            alias_map = {"niche_industry": "general_entry_level", "mental_health": "healthcare_medical"}
            ind_key = alias_map.get(ind_key, "general_entry_level")
        ind_platforms = media_platforms_db.get(ind_key, media_platforms_db["general_entry_level"])
        ind_label = ind_platforms.get("label", industry.replace("_", " ").title())

        # Get locations for regional context
        locs = data.get("locations", [])
        loc_context = ", ".join(locs[:3]) if locs else "US"

        ws_media.merge_cells("B2:G2")
        ws_media["B2"].value = f"Media & Print Platforms — {ind_label}"
        ws_media["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_media["B2"].border = accent_bottom_border

        ws_media.merge_cells("B3:G3")
        ws_media["B3"].value = f"Recommended recruitment marketing channels for {ind_label} roles in {loc_context}. These platforms complement digital programmatic channels to reach passive candidates and build employer brand."
        ws_media["B3"].font = Font(name="Calibri", italic=True, size=9, color="596780")
        ws_media["B3"].alignment = Alignment(wrap_text=True, vertical="top")

        category_labels = {
            "trade_publications": ("📰 TRADE PUBLICATIONS & JOURNALS", "2E75B6"),
            "digital_media": ("💻 DIGITAL MEDIA PLATFORMS", "1B6B3A"),
            "ooh_print": ("🏗️ OUT-OF-HOME & PRINT", "ED7D31"),
            "broadcast_audio": ("🎙️ BROADCAST & AUDIO", "0A66C9"),
            "specialty": ("⭐ SPECIALTY & EMERGING", "FFC000"),
        }

        row = 5
        for cat_key, (cat_label, cat_color) in category_labels.items():
            platforms = ind_platforms.get(cat_key, [])
            if not platforms:
                continue

            # Category header
            ws_media.merge_cells(f"B{row}:G{row}")
            cell = ws_media.cell(row=row, column=2, value=cat_label)
            cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
            cell.fill = PatternFill(start_color=cat_color, end_color=cat_color, fill_type="solid")
            cell.alignment = Alignment(vertical="center")
            for c in range(3, 8):
                ws_media.cell(row=row, column=c).fill = PatternFill(start_color=cat_color, end_color=cat_color, fill_type="solid")
            row += 1

            # Column headers
            for i, h in enumerate(["Platform / Channel", "Type", "Target Audience", "Reach", "Use Case", "Recommendation"]):
                cell = ws_media.cell(row=row, column=2 + i, value=h)
                cell.font = Font(name="Calibri", bold=True, size=10, color="1B2A4A")
                cell.fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
                cell.alignment = Alignment(horizontal="center", vertical="center")
                cell.border = thin_border
            row += 1

            # Platform rows
            for idx, plat in enumerate(platforms):
                name, audience, reach, use_case = plat
                fill_color = "FFFFFF" if idx % 2 == 0 else "F2F6FA"
                row_fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")

                type_label = cat_key.replace("_", " ").title()

                # Recommendation logic based on locations
                rec = "✅ Recommended"
                if "Global" in reach and any(loc for loc in locs if any(c in loc.lower() for c in ["uk","london","india","germany","singapore","japan","australia"])):
                    rec = "⭐ High Priority (International)"
                elif "Local" in reach and locs:
                    rec = f"✅ Target: {locs[0]}" if locs else "✅ Recommended"

                for ci, val in enumerate([name, type_label, audience, reach, use_case, rec]):
                    cell = ws_media.cell(row=row, column=2 + ci, value=val)
                    cell.font = Font(name="Calibri", size=10, bold=(ci==0), color="1B2A4A" if ci==0 else "333333")
                    cell.fill = row_fill
                    cell.alignment = Alignment(wrap_text=True, vertical="top")
                    cell.border = thin_border
                row += 1
            row += 1  # Gap between categories

        # Footer with guidance
        ws_media.merge_cells(f"B{row}:G{row}")
        cell = ws_media.cell(row=row, column=2, value="💡 Recommendation: Combine 2-3 print/OOH channels with digital programmatic for optimal passive + active candidate coverage. Allocate 10-15% of total recruitment budget to media/print channels for employer brand lift.")
        cell.font = Font(name="Calibri", italic=True, size=9, color="596780")
        cell.alignment = Alignment(wrap_text=True)

    # ═══════════════════════════════════════════════════════════════════════════
    # PHASE 5: SYNTHESIZED DATA & BUDGET SHEETS
    # ═══════════════════════════════════════════════════════════════════════════

    # ── Sheet: Ad Platform Analysis ──
    _synth = data.get("_synthesized", {})
    _ad_plat = _synth.get("ad_platform_analysis", {})
    if isinstance(_ad_plat, dict) and _ad_plat:
        ws_adplat = wb.create_sheet("Ad Platform Analysis")
        ws_adplat.sheet_properties.tabColor = "0A66C9"
        ws_adplat.column_dimensions["A"].width = 3
        ws_adplat.column_dimensions["B"].width = 22
        ws_adplat.column_dimensions["C"].width = 14
        ws_adplat.column_dimensions["D"].width = 14
        ws_adplat.column_dimensions["E"].width = 14
        ws_adplat.column_dimensions["F"].width = 18
        ws_adplat.column_dimensions["G"].width = 14
        ws_adplat.column_dimensions["H"].width = 18
        ws_adplat.column_dimensions["I"].width = 22

        ws_adplat.merge_cells("B2:I2")
        ws_adplat["B2"].value = "Ad Platform Analysis"
        ws_adplat["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_adplat["B2"].border = accent_bottom_border

        adp_row = 4
        # Recommendation row
        _top_platform = ""
        _top_score = 0.0
        # FIX: Synthesizer returns platforms as top-level keys (not wrapped in "platforms")
        _platforms_list = {k: v for k, v in _ad_plat.items()
                          if isinstance(v, dict) and not k.startswith("_")}
        if isinstance(_platforms_list, dict):
            for pname, pdata in _platforms_list.items():
                if isinstance(pdata, dict) and pdata.get("fit_score", 0) > _top_score:
                    _top_score = pdata.get("fit_score", 0)
                    _top_platform = pname
        if _top_platform:
            ws_adplat.merge_cells(f"B{adp_row}:I{adp_row}")
            _rec_cell = ws_adplat.cell(row=adp_row, column=2, value=f"Top Recommendation: {_top_platform} (Fit Score: {_top_score:.0%})")
            _rec_cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
            _rec_cell.fill = PatternFill(start_color="2E7D32", end_color="2E7D32", fill_type="solid")
            _rec_cell.alignment = center_alignment
            for c in range(3, 10):
                ws_adplat.cell(row=adp_row, column=c).fill = PatternFill(start_color="2E7D32", end_color="2E7D32", fill_type="solid")
            adp_row += 2

        # Headers
        adp_headers = ["Platform", "CPC", "CPM", "CPA", "Audience Reach", "Fit Score", "ROI Projection", "Daily Budget Range"]
        for i, h in enumerate(adp_headers):
            cell = ws_adplat.cell(row=adp_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        adp_row += 1

        if isinstance(_platforms_list, dict):
            for pidx, (pname, pdata) in enumerate(_platforms_list.items()):
                if not isinstance(pdata, dict) or pname.startswith("_"):
                    continue
                _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if pidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                _fit = pdata.get("fit_score", 0)
                _cpc_val = pdata.get("avg_cpc", pdata.get("cpc", "N/A"))
                _cpm_val = pdata.get("avg_cpm", pdata.get("cpm", "N/A"))
                _cpa_val = pdata.get("avg_cpa", pdata.get("cpa", "N/A"))
                _reach_val = pdata.get("estimated_reach", pdata.get("audience_reach", "N/A"))
                _roi_val = pdata.get("roi_projection", "N/A")
                _daily_val = pdata.get("daily_budget_range", pdata.get("recommended_daily_budget", "N/A"))

                # Format monetary values
                if isinstance(_cpc_val, (int, float)):
                    _cpc_val = f"${_cpc_val:.2f}"
                if isinstance(_cpm_val, (int, float)):
                    _cpm_val = f"${_cpm_val:.2f}"
                if isinstance(_cpa_val, (int, float)):
                    _cpa_val = f"${_cpa_val:.2f}"
                if isinstance(_reach_val, (int, float)):
                    _reach_val = f"{_reach_val:,.0f}"
                if isinstance(_roi_val, (int, float)):
                    _roi_val = f"{_roi_val:.1f}x"
                if isinstance(_daily_val, (list, tuple)) and len(_daily_val) == 2:
                    _daily_val = f"${_daily_val[0]:,.0f} - ${_daily_val[1]:,.0f}"
                elif isinstance(_daily_val, dict):
                    _daily_val = f"${_daily_val.get('min', 0):,.0f} - ${_daily_val.get('max', 0):,.0f}"

                for ci, val in enumerate([pname, _cpc_val, _cpm_val, _cpa_val, _reach_val, f"{_fit:.0%}" if isinstance(_fit, (int, float)) else str(_fit), _roi_val, _daily_val]):
                    cell = ws_adplat.cell(row=adp_row, column=2 + ci, value=val)
                    cell.font = Font(name="Calibri", size=10, bold=(ci == 0))
                    cell.fill = _row_fill
                    cell.border = thin_border
                    cell.alignment = center_alignment if ci > 0 else wrap_alignment

                # Color-code fit score cell
                fit_cell = ws_adplat.cell(row=adp_row, column=7)
                if isinstance(_fit, (int, float)):
                    if _fit >= 0.7:
                        fit_cell.fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
                        fit_cell.font = Font(name="Calibri", bold=True, size=10, color="2E7D32")
                    elif _fit >= 0.4:
                        fit_cell.fill = PatternFill(start_color="FFF3E0", end_color="FFF3E0", fill_type="solid")
                        fit_cell.font = Font(name="Calibri", bold=True, size=10, color="F57C00")
                    else:
                        fit_cell.fill = PatternFill(start_color="FFEBEE", end_color="FFEBEE", fill_type="solid")
                        fit_cell.font = Font(name="Calibri", bold=True, size=10, color="C62828")
                adp_row += 1

        # Summary note
        adp_row += 1
        ws_adplat.merge_cells(f"B{adp_row}:I{adp_row}")
        ws_adplat.cell(row=adp_row, column=2, value="Fit scores are computed from industry match, audience alignment, cost efficiency, and historical conversion benchmarks. Higher scores indicate better expected ROI.").font = Font(name="Calibri", italic=True, size=9, color="596780")

    # ── Sheet: Salary Intelligence ──
    _sal_intel = _synth.get("salary_intelligence", {})
    if isinstance(_sal_intel, dict) and _sal_intel:
        ws_salary = wb.create_sheet("Salary Intelligence")
        ws_salary.sheet_properties.tabColor = "0A66C9"
        ws_salary.column_dimensions["A"].width = 3
        ws_salary.column_dimensions["B"].width = 30
        ws_salary.column_dimensions["C"].width = 14
        ws_salary.column_dimensions["D"].width = 14
        ws_salary.column_dimensions["E"].width = 14
        ws_salary.column_dimensions["F"].width = 14
        ws_salary.column_dimensions["G"].width = 14
        ws_salary.column_dimensions["H"].width = 14
        ws_salary.column_dimensions["I"].width = 14

        ws_salary.merge_cells("B2:I2")
        ws_salary["B2"].value = "Salary Intelligence"
        ws_salary["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_salary["B2"].border = accent_bottom_border

        sal_row = 4
        ws_salary.merge_cells(f"B{sal_row}:I{sal_row}")
        ws_salary.cell(row=sal_row, column=2, value="Fused salary data from multiple API sources with confidence scoring. Use for competitive offer benchmarking and budget calibration.").font = Font(name="Calibri", italic=True, size=9, color="596780")
        sal_row += 2

        sal_headers = ["Role", "Min", "P25", "Median", "P75", "Max", "Sources", "Confidence"]
        for i, h in enumerate(sal_headers):
            cell = ws_salary.cell(row=sal_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        sal_row += 1

        # _sal_intel can be a dict with role keys, or have a "roles" sub-dict
        _sal_roles = _sal_intel.get("roles", _sal_intel) if isinstance(_sal_intel, dict) else {}
        if isinstance(_sal_roles, dict):
            for sidx, (role_name, role_data) in enumerate(_sal_roles.items()):
                if not isinstance(role_data, dict):
                    continue
                # Skip meta keys
                if role_name in ("summary", "overall", "metadata", "data_quality"):
                    continue
                _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if sidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                _conf = role_data.get("confidence", role_data.get("confidence_score", 0))
                _srcs = role_data.get("sources", role_data.get("source_count", "N/A"))
                if isinstance(_srcs, list):
                    _srcs = len(_srcs)

                _sal_vals = [
                    role_name,
                    f"${role_data.get('min', role_data.get('p10', 0)):,.0f}" if isinstance(role_data.get('min', role_data.get('p10', 0)), (int, float)) else str(role_data.get('min', 'N/A')),
                    f"${role_data.get('p25', 0):,.0f}" if isinstance(role_data.get('p25', 0), (int, float)) else str(role_data.get('p25', 'N/A')),
                    f"${role_data.get('median', role_data.get('p50', 0)):,.0f}" if isinstance(role_data.get('median', role_data.get('p50', 0)), (int, float)) else str(role_data.get('median', 'N/A')),
                    f"${role_data.get('p75', 0):,.0f}" if isinstance(role_data.get('p75', 0), (int, float)) else str(role_data.get('p75', 'N/A')),
                    f"${role_data.get('max', role_data.get('p90', 0)):,.0f}" if isinstance(role_data.get('max', role_data.get('p90', 0)), (int, float)) else str(role_data.get('max', 'N/A')),
                    str(_srcs),
                    f"{_conf:.0%}" if isinstance(_conf, (int, float)) else str(_conf),
                ]
                for ci, val in enumerate(_sal_vals):
                    cell = ws_salary.cell(row=sal_row, column=2 + ci, value=val)
                    cell.font = Font(name="Calibri", size=10, bold=(ci == 0))
                    cell.fill = _row_fill
                    cell.border = thin_border
                    cell.alignment = center_alignment if ci > 0 else wrap_alignment

                # Highlight low-confidence rows
                if isinstance(_conf, (int, float)) and _conf < 0.5:
                    for c in range(2, 10):
                        ws_salary.cell(row=sal_row, column=c).fill = PatternFill(start_color="FFF3E0", end_color="FFF3E0", fill_type="solid")
                    conf_cell = ws_salary.cell(row=sal_row, column=9)
                    conf_cell.font = Font(name="Calibri", bold=True, size=10, color="F57C00")
                sal_row += 1

        sal_row += 1
        ws_salary.merge_cells(f"B{sal_row}:I{sal_row}")
        ws_salary.cell(row=sal_row, column=2, value="Rows highlighted in amber indicate low confidence (< 50%). Consider supplementing with additional market research for those roles.").font = Font(name="Calibri", italic=True, size=9, color="596780")

    # ── Sheet: Market Demand Analysis ──
    _mkt_demand = _synth.get("job_market_demand", {})
    if isinstance(_mkt_demand, dict) and _mkt_demand:
        ws_demand = wb.create_sheet("Market Demand Analysis")
        ws_demand.sheet_properties.tabColor = "ED7D31"
        ws_demand.column_dimensions["A"].width = 3
        ws_demand.column_dimensions["B"].width = 28
        ws_demand.column_dimensions["C"].width = 16
        ws_demand.column_dimensions["D"].width = 16
        ws_demand.column_dimensions["E"].width = 16
        ws_demand.column_dimensions["F"].width = 18
        ws_demand.column_dimensions["G"].width = 14
        ws_demand.column_dimensions["H"].width = 16

        ws_demand.merge_cells("B2:H2")
        ws_demand["B2"].value = "Market Demand Analysis"
        ws_demand["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_demand["B2"].border = accent_bottom_border

        dem_row = 4
        ws_demand.merge_cells(f"B{dem_row}:H{dem_row}")
        ws_demand.cell(row=dem_row, column=2, value="Job market demand signals fused from job posting APIs, search trend data, and talent pool analysis. Temperature indicates hiring difficulty.").font = Font(name="Calibri", italic=True, size=9, color="596780")
        dem_row += 2

        dem_headers = ["Role", "Job Postings", "Search Interest", "Talent Pool", "Competition Index", "Temperature", "Trend"]
        for i, h in enumerate(dem_headers):
            cell = ws_demand.cell(row=dem_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        dem_row += 1

        _dem_roles = _mkt_demand.get("roles", _mkt_demand) if isinstance(_mkt_demand, dict) else {}
        if isinstance(_dem_roles, dict):
            for didx, (role_name, role_data) in enumerate(_dem_roles.items()):
                if not isinstance(role_data, dict):
                    continue
                if role_name in ("summary", "overall", "metadata", "data_quality"):
                    continue
                _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if didx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")

                _postings = role_data.get("total_postings", role_data.get("job_postings", role_data.get("posting_count", "N/A")))
                _search = role_data.get("search_volume_monthly", role_data.get("search_interest", role_data.get("search_volume", "N/A")))
                _pool = role_data.get("talent_pool_estimate", role_data.get("talent_pool", role_data.get("talent_supply", "N/A")))
                _comp = role_data.get("competition_index", role_data.get("competition", "N/A"))
                _temp = role_data.get("market_temperature", role_data.get("temperature", "N/A"))
                _trend = role_data.get("trend_direction", role_data.get("trend", role_data.get("demand_trend", "N/A")))

                if isinstance(_postings, (int, float)):
                    _postings = f"{_postings:,.0f}"
                if isinstance(_search, (int, float)):
                    _search = f"{_search:,.0f}"
                if isinstance(_pool, (int, float)):
                    _pool = f"{_pool:,.0f}"
                if isinstance(_comp, (int, float)):
                    _comp = f"{_comp:.2f}"

                for ci, val in enumerate([role_name, str(_postings), str(_search), str(_pool), str(_comp), str(_temp).title(), str(_trend).title()]):
                    cell = ws_demand.cell(row=dem_row, column=2 + ci, value=val)
                    cell.font = Font(name="Calibri", size=10, bold=(ci == 0))
                    cell.fill = _row_fill
                    cell.border = thin_border
                    cell.alignment = center_alignment if ci > 0 else wrap_alignment

                # Color-code temperature
                temp_cell = ws_demand.cell(row=dem_row, column=7)
                _temp_lower = str(_temp).lower()
                if "hot" in _temp_lower:
                    temp_cell.fill = PatternFill(start_color="FFEBEE", end_color="FFEBEE", fill_type="solid")
                    temp_cell.font = Font(name="Calibri", bold=True, size=10, color="C62828")
                elif "warm" in _temp_lower:
                    temp_cell.fill = PatternFill(start_color="FFF3E0", end_color="FFF3E0", fill_type="solid")
                    temp_cell.font = Font(name="Calibri", bold=True, size=10, color="E65100")
                elif "cool" in _temp_lower:
                    temp_cell.fill = PatternFill(start_color="E3F2FD", end_color="E3F2FD", fill_type="solid")
                    temp_cell.font = Font(name="Calibri", bold=True, size=10, color="1565C0")
                elif "cold" in _temp_lower:
                    temp_cell.fill = PatternFill(start_color="ECEFF1", end_color="ECEFF1", fill_type="solid")
                    temp_cell.font = Font(name="Calibri", bold=True, size=10, color="78909C")
                dem_row += 1

        dem_row += 1
        ws_demand.merge_cells(f"B{dem_row}:H{dem_row}")
        ws_demand.cell(row=dem_row, column=2, value="Temperature: Hot = high demand/low supply (hardest to hire), Cold = low demand/high supply (easiest). Competition Index reflects employer competition for the same talent.").font = Font(name="Calibri", italic=True, size=9, color="596780")

    # ── Sheet: Location Intelligence ──
    _loc_profiles = _synth.get("location_profiles", {})
    if isinstance(_loc_profiles, dict) and _loc_profiles:
        ws_loc = wb.create_sheet("Location Intelligence")
        ws_loc.sheet_properties.tabColor = "0A66C9"
        ws_loc.column_dimensions["A"].width = 3
        ws_loc.column_dimensions["B"].width = 26
        ws_loc.column_dimensions["C"].width = 16
        ws_loc.column_dimensions["D"].width = 16
        ws_loc.column_dimensions["E"].width = 18
        ws_loc.column_dimensions["F"].width = 16
        ws_loc.column_dimensions["G"].width = 16
        ws_loc.column_dimensions["H"].width = 16
        ws_loc.column_dimensions["I"].width = 18

        ws_loc.merge_cells("B2:I2")
        ws_loc["B2"].value = "Location Intelligence"
        ws_loc["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_loc["B2"].border = accent_bottom_border

        loc_row = 4
        ws_loc.merge_cells(f"B{loc_row}:I{loc_row}")
        ws_loc.cell(row=loc_row, column=2, value="Per-location economic and workforce profiles fused from Census, GeoNames, Teleport, REST Countries, IMF, and World Bank data sources.").font = Font(name="Calibri", italic=True, size=9, color="596780")
        loc_row += 2

        loc_headers = ["Location", "Population", "Workforce Est.", "Cost of Living", "Timezone", "Quality of Life", "Infrastructure", "Sources"]
        for i, h in enumerate(loc_headers):
            cell = ws_loc.cell(row=loc_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        loc_row += 1

        for lidx, (loc_name, loc_data) in enumerate(_loc_profiles.items()):
            if not isinstance(loc_data, dict):
                continue
            _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if lidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")

            _loc_pop = loc_data.get("population", "N/A")
            _loc_wf = loc_data.get("workforce_estimate", "N/A")
            _loc_col = loc_data.get("cost_of_living", {})
            if isinstance(_loc_col, dict):
                _col_idx = _loc_col.get("index", _loc_col.get("cost_index", ""))
                _col_display = f"{_col_idx:.1f}" if isinstance(_col_idx, (int, float)) else str(_col_idx) if _col_idx else "N/A"
            else:
                _col_display = str(_loc_col) if _loc_col else "N/A"
            _loc_tz = loc_data.get("timezone", "N/A")
            _loc_qol = loc_data.get("quality_of_life_score", "N/A")
            _loc_infra = loc_data.get("infrastructure_score", "N/A")
            _loc_meta = loc_data.get("_meta", {})
            _loc_src_count = _loc_meta.get("source_count", 0) if isinstance(_loc_meta, dict) else 0

            if isinstance(_loc_pop, (int, float)):
                _loc_pop = f"{_loc_pop:,.0f}"
            if isinstance(_loc_wf, (int, float)):
                _loc_wf = f"{_loc_wf:,.0f}"
            if isinstance(_loc_qol, (int, float)):
                _loc_qol = f"{_loc_qol:.1f}"
            if isinstance(_loc_infra, (int, float)):
                _loc_infra = f"{_loc_infra:.2f}"

            _loc_vals = [
                loc_data.get("location", loc_name),
                str(_loc_pop),
                str(_loc_wf),
                _col_display,
                str(_loc_tz),
                str(_loc_qol),
                str(_loc_infra),
                str(_loc_src_count),
            ]
            for ci, val in enumerate(_loc_vals):
                cell = ws_loc.cell(row=loc_row, column=2 + ci, value=val)
                cell.font = Font(name="Calibri", size=10, bold=(ci == 0))
                cell.fill = _row_fill
                cell.border = thin_border
                cell.alignment = center_alignment if ci > 0 else wrap_alignment
            loc_row += 1

            # Show economic indicators if available
            _econ = loc_data.get("economic_indicators", {})
            _wb_ind = loc_data.get("world_bank_indicators", {})
            if (isinstance(_econ, dict) and _econ) or (isinstance(_wb_ind, dict) and _wb_ind):
                _gdp_g = _econ.get("gdp_growth", _wb_ind.get("gdp_growth", None)) if isinstance(_econ, dict) else (_wb_ind.get("gdp_growth", None) if isinstance(_wb_ind, dict) else None)
                _unemp = _econ.get("unemployment", _wb_ind.get("unemployment_rate", None)) if isinstance(_econ, dict) else (_wb_ind.get("unemployment_rate", None) if isinstance(_wb_ind, dict) else None)
                _infl = _econ.get("inflation", None) if isinstance(_econ, dict) else None
                econ_parts = []
                if isinstance(_gdp_g, (int, float)):
                    econ_parts.append(f"GDP Growth: {_gdp_g:.1f}%")
                if isinstance(_unemp, (int, float)):
                    econ_parts.append(f"Unemployment: {_unemp:.1f}%")
                if isinstance(_infl, (int, float)):
                    econ_parts.append(f"Inflation: {_infl:.1f}%")
                if econ_parts:
                    ws_loc.merge_cells(f"C{loc_row}:I{loc_row}")
                    ws_loc.cell(row=loc_row, column=2, value="").fill = _row_fill
                    ws_loc.cell(row=loc_row, column=3, value=f"Economic: {' | '.join(econ_parts)}").font = Font(name="Calibri", italic=True, size=9, color="596780")
                    loc_row += 1

            # Show country info if available
            _country = loc_data.get("country_info", {})
            if isinstance(_country, dict) and _country:
                _currencies = _country.get("currencies", {})
                _languages = _country.get("languages", {})
                _region = _country.get("region", "")
                country_parts = []
                if _region:
                    country_parts.append(f"Region: {_region}")
                if isinstance(_currencies, dict) and _currencies:
                    curr_names = [v.get("name", k) if isinstance(v, dict) else str(v) for k, v in list(_currencies.items())[:3]]
                    country_parts.append(f"Currency: {', '.join(curr_names)}")
                if isinstance(_languages, dict) and _languages:
                    lang_names = list(_languages.values())[:3] if all(isinstance(v, str) for v in _languages.values()) else list(_languages.keys())[:3]
                    country_parts.append(f"Languages: {', '.join(str(l) for l in lang_names)}")
                if country_parts:
                    ws_loc.merge_cells(f"C{loc_row}:I{loc_row}")
                    ws_loc.cell(row=loc_row, column=2, value="").fill = _row_fill
                    ws_loc.cell(row=loc_row, column=3, value=f"Country: {' | '.join(country_parts)}").font = Font(name="Calibri", italic=True, size=9, color="596780")
                    loc_row += 1

        loc_row += 1
        ws_loc.merge_cells(f"B{loc_row}:I{loc_row}")
        ws_loc.cell(row=loc_row, column=2, value="Sources include US Census Bureau, DataUSA, GeoNames, Teleport QoL Index, REST Countries, IMF WEO, and World Bank. Source count indicates number of independent data sources contributing to each profile.").font = Font(name="Calibri", italic=True, size=9, color="596780")

    # ── Sheet: Competitive Landscape ──
    _comp_intel = _synth.get("competitive_intelligence", {})
    if isinstance(_comp_intel, dict) and _comp_intel:
        ws_comp = wb.create_sheet("Competitive Landscape")
        ws_comp.sheet_properties.tabColor = "0891B2"
        ws_comp.column_dimensions["A"].width = 3
        ws_comp.column_dimensions["B"].width = 28
        ws_comp.column_dimensions["C"].width = 30
        ws_comp.column_dimensions["D"].width = 20
        ws_comp.column_dimensions["E"].width = 22
        ws_comp.column_dimensions["F"].width = 18
        ws_comp.column_dimensions["G"].width = 18

        ws_comp.merge_cells("B2:G2")
        ws_comp["B2"].value = "Competitive Landscape"
        ws_comp["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_comp["B2"].border = accent_bottom_border

        comp_row = 4

        # Company profile section
        _comp_profile = _comp_intel.get("company_profile", {})
        if isinstance(_comp_profile, dict) and _comp_profile:
            style_section_header(ws_comp, comp_row, 2, 7, "Company Profile")
            comp_row += 2

            _profile_fields = [
                ("Company Name", _comp_profile.get("name", "N/A")),
                ("Description", _comp_profile.get("description", "N/A")),
                ("Domain", _comp_profile.get("domain", "N/A")),
                ("SEC Ticker", _comp_profile.get("sec_ticker", "N/A") or "N/A"),
                ("SIC Industry", _comp_profile.get("sec_sic_description", "N/A") or "N/A"),
                ("Publicly Traded", "Yes" if _comp_profile.get("is_public") else "No"),
            ]
            for fidx, (flabel, fval) in enumerate(_profile_fields):
                if fval and str(fval) != "N/A" and str(fval).strip():
                    _pf_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if fidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                    c1 = ws_comp.cell(row=comp_row, column=2, value=flabel)
                    c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                    c1.fill = _pf_fill
                    c1.border = thin_border
                    ws_comp.merge_cells(f"C{comp_row}:G{comp_row}")
                    c2 = ws_comp.cell(row=comp_row, column=3, value=str(fval)[:200])
                    c2.font = Font(name="Calibri", size=10)
                    c2.fill = _pf_fill
                    c2.border = thin_border
                    c2.alignment = wrap_alignment
                    comp_row += 1

        # Competitors table
        _competitors = _comp_intel.get("competitors", {})
        if isinstance(_competitors, dict) and _competitors:
            comp_row += 1
            style_section_header(ws_comp, comp_row, 2, 7, "Competitor Companies")
            comp_row += 2

            comp_tbl_headers = ["Competitor", "Domain", "Logo URL"]
            for i, h in enumerate(comp_tbl_headers):
                cell = ws_comp.cell(row=comp_row, column=2 + i, value=h)
                cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                cell.border = thin_border
                cell.alignment = center_alignment
            comp_row += 1

            for cidx, (cname, cdata) in enumerate(_competitors.items()):
                if not isinstance(cdata, dict):
                    continue
                _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if cidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                _c_vals = [
                    cdata.get("name", cname),
                    cdata.get("domain", "N/A"),
                    cdata.get("logo_url", "N/A"),
                ]
                for ci, val in enumerate(_c_vals):
                    cell = ws_comp.cell(row=comp_row, column=2 + ci, value=str(val))
                    cell.font = Font(name="Calibri", size=10, bold=(ci == 0))
                    cell.fill = _row_fill
                    cell.border = thin_border
                    cell.alignment = wrap_alignment
                comp_row += 1

        # Industry hiring trends
        _hiring = _comp_intel.get("hiring_trends", {})
        if isinstance(_hiring, dict) and _hiring:
            comp_row += 1
            style_section_header(ws_comp, comp_row, 2, 7, "Industry Hiring Trends")
            comp_row += 2

            _trend_fields = [
                ("Total Employment", _hiring.get("employment_count")),
                ("Avg Weekly Wage", _hiring.get("average_weekly_wage")),
                ("Establishments", _hiring.get("establishment_count")),
                ("Data Source", _hiring.get("source")),
            ]
            for fidx, (flabel, fval) in enumerate(_trend_fields):
                if fval is not None:
                    _tf_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if fidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                    c1 = ws_comp.cell(row=comp_row, column=2, value=flabel)
                    c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                    c1.fill = _tf_fill
                    c1.border = thin_border
                    if isinstance(fval, (int, float)):
                        fval_str = f"{fval:,.0f}" if fval > 100 else f"${fval:,.2f}"
                    else:
                        fval_str = str(fval)
                    ws_comp.merge_cells(f"C{comp_row}:D{comp_row}")
                    c2 = ws_comp.cell(row=comp_row, column=3, value=fval_str)
                    c2.font = Font(name="Calibri", size=10)
                    c2.fill = _tf_fill
                    c2.border = thin_border
                    comp_row += 1

            # Market context
            _mkt_ctx = _hiring.get("market_context", {})
            if isinstance(_mkt_ctx, dict) and _mkt_ctx:
                comp_row += 1
                _mkt_state = _mkt_ctx.get("market_state", "N/A")
                _ai_adoption = _mkt_ctx.get("ai_adoption", "N/A")
                ws_comp.merge_cells(f"B{comp_row}:G{comp_row}")
                ws_comp.cell(row=comp_row, column=2, value=f"Market State: {_mkt_state} | AI Adoption in Recruiting: {_ai_adoption}").font = Font(name="Calibri", italic=True, size=10, color="596780")
                comp_row += 1

        # Market positioning
        _mkt_pos = _comp_intel.get("market_positioning", {})
        if isinstance(_mkt_pos, dict) and _mkt_pos:
            comp_row += 1
            style_section_header(ws_comp, comp_row, 2, 7, "Market Positioning")
            comp_row += 2
            _pos_items = [
                ("Industry Sector", _mkt_pos.get("industry_sector", "N/A")),
                ("Publicly Traded", "Yes" if _mkt_pos.get("is_public_company") else "No"),
                ("Known Competitors", str(_mkt_pos.get("competitor_count", 0))),
                ("SEC Filings Available", "Yes" if _mkt_pos.get("has_sec_filings") else "No"),
            ]
            for fidx, (plabel, pval) in enumerate(_pos_items):
                _mp_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if fidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                c1 = ws_comp.cell(row=comp_row, column=2, value=plabel)
                c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                c1.fill = _mp_fill
                c1.border = thin_border
                ws_comp.merge_cells(f"C{comp_row}:D{comp_row}")
                c2 = ws_comp.cell(row=comp_row, column=3, value=str(pval))
                c2.font = Font(name="Calibri", size=10)
                c2.fill = _mp_fill
                c2.border = thin_border
                comp_row += 1

        # --- Clearbit Company Intelligence ---
        _clearbit_co = _comp_intel.get("company_clearbit", {})
        if isinstance(_clearbit_co, dict) and _clearbit_co:
            comp_row += 2
            style_section_header(ws_comp, comp_row, 2, 7, "Company Intelligence (Clearbit)")
            comp_row += 2
            _cb_fields = [
                ("Domain", _clearbit_co.get("domain", "N/A")),
                ("Industry", _clearbit_co.get("industry", "N/A")),
                ("Employee Count", f"{_clearbit_co.get('employee_count', 'N/A'):,}" if isinstance(_clearbit_co.get("employee_count"), (int, float)) else str(_clearbit_co.get("employee_count", "N/A"))),
                ("Annual Revenue", f"${_clearbit_co.get('annual_revenue', 0):,.0f}" if isinstance(_clearbit_co.get("annual_revenue"), (int, float)) and _clearbit_co.get("annual_revenue") else "N/A"),
            ]
            _cb_tags = _clearbit_co.get("tags", [])
            if isinstance(_cb_tags, list) and _cb_tags:
                _cb_fields.append(("Tags", ", ".join(str(t) for t in _cb_tags[:8])))
            _cb_tech = _clearbit_co.get("tech_stack", [])
            if isinstance(_cb_tech, list) and _cb_tech:
                _cb_fields.append(("Tech Stack", ", ".join(str(t) for t in _cb_tech[:10])))

            for fidx, (flabel, fval) in enumerate(_cb_fields):
                if fval and str(fval) != "N/A" and str(fval).strip():
                    _pf_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if fidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                    c1 = ws_comp.cell(row=comp_row, column=2, value=flabel)
                    c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                    c1.fill = _pf_fill
                    c1.border = thin_border
                    ws_comp.merge_cells(f"C{comp_row}:G{comp_row}")
                    c2 = ws_comp.cell(row=comp_row, column=3, value=str(fval)[:300])
                    c2.font = Font(name="Calibri", size=10)
                    c2.fill = _pf_fill
                    c2.border = thin_border
                    c2.alignment = wrap_alignment
                    comp_row += 1

        # --- Wikipedia Company Summary ---
        _wiki_co = _comp_intel.get("company_wikipedia", {})
        if isinstance(_wiki_co, dict) and _wiki_co:
            _wiki_desc = _wiki_co.get("description", "")
            _wiki_founded = _wiki_co.get("founded")
            _wiki_hq = _wiki_co.get("headquarters")
            _wiki_url = _wiki_co.get("url")
            if _wiki_desc or _wiki_founded or _wiki_hq:
                comp_row += 2
                style_section_header(ws_comp, comp_row, 2, 7, "Company Background (Wikipedia)")
                comp_row += 2
                _wiki_fields = []
                if _wiki_desc:
                    _wiki_fields.append(("Description", str(_wiki_desc)[:400]))
                if _wiki_founded:
                    _wiki_fields.append(("Founded", str(_wiki_founded)))
                if _wiki_hq:
                    _wiki_fields.append(("Headquarters", str(_wiki_hq)))
                if _wiki_url:
                    _wiki_fields.append(("Wikipedia URL", str(_wiki_url)))

                for fidx, (flabel, fval) in enumerate(_wiki_fields):
                    _pf_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if fidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                    c1 = ws_comp.cell(row=comp_row, column=2, value=flabel)
                    c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                    c1.fill = _pf_fill
                    c1.border = thin_border
                    ws_comp.merge_cells(f"C{comp_row}:G{comp_row}")
                    c2 = ws_comp.cell(row=comp_row, column=3, value=fval)
                    c2.font = Font(name="Calibri", size=10)
                    c2.fill = _pf_fill
                    c2.border = thin_border
                    c2.alignment = wrap_alignment
                    comp_row += 1

        # --- SEC EDGAR Filings ---
        _sec_co = _comp_intel.get("company_sec", {})
        if isinstance(_sec_co, dict) and _sec_co:
            _sec_cik = _sec_co.get("cik")
            _sec_filings = _sec_co.get("filings", [])
            _sec_sic = _sec_co.get("sic_code")
            _sec_fy = _sec_co.get("fiscal_year_end")
            if _sec_cik or _sec_filings:
                comp_row += 2
                style_section_header(ws_comp, comp_row, 2, 7, "SEC EDGAR Filings")
                comp_row += 2

                _sec_meta = [
                    ("CIK", str(_sec_cik) if _sec_cik else "N/A"),
                    ("SIC Code", str(_sec_sic) if _sec_sic else "N/A"),
                    ("Fiscal Year End", str(_sec_fy) if _sec_fy else "N/A"),
                ]
                for fidx, (flabel, fval) in enumerate(_sec_meta):
                    if fval != "N/A":
                        _pf_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if fidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                        c1 = ws_comp.cell(row=comp_row, column=2, value=flabel)
                        c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                        c1.fill = _pf_fill
                        c1.border = thin_border
                        ws_comp.merge_cells(f"C{comp_row}:D{comp_row}")
                        c2 = ws_comp.cell(row=comp_row, column=3, value=fval)
                        c2.font = Font(name="Calibri", size=10)
                        c2.fill = _pf_fill
                        c2.border = thin_border
                        comp_row += 1

                if isinstance(_sec_filings, list) and _sec_filings:
                    comp_row += 1
                    _filing_headers = ["Filing Type", "Date", "Description"]
                    for i, h in enumerate(_filing_headers):
                        cell = ws_comp.cell(row=comp_row, column=2 + i, value=h)
                        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                        cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                        cell.border = thin_border
                        cell.alignment = center_alignment
                    comp_row += 1

                    for fidx, filing in enumerate(_sec_filings[:5]):
                        if not isinstance(filing, dict):
                            continue
                        _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if fidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                        _f_vals = [
                            filing.get("form", filing.get("type", "N/A")),
                            filing.get("filed", filing.get("date", "N/A")),
                            filing.get("description", filing.get("primaryDocDescription", "N/A")),
                        ]
                        for ci, val in enumerate(_f_vals):
                            cell = ws_comp.cell(row=comp_row, column=2 + ci, value=str(val)[:100])
                            cell.font = Font(name="Calibri", size=10)
                            cell.fill = _row_fill
                            cell.border = thin_border
                            cell.alignment = wrap_alignment if ci == 2 else center_alignment
                        comp_row += 1

        # --- Competitor Logos (enriched) ---
        _comp_logos = _comp_intel.get("competitor_logos", {})
        if isinstance(_comp_logos, dict) and _comp_logos:
            comp_row += 2
            style_section_header(ws_comp, comp_row, 2, 7, "Competitor Brand Assets")
            comp_row += 2

            _logo_headers = ["Competitor", "Logo URL", "Domain"]
            for i, h in enumerate(_logo_headers):
                cell = ws_comp.cell(row=comp_row, column=2 + i, value=h)
                cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                cell.border = thin_border
                cell.alignment = center_alignment
            comp_row += 1

            for cidx, (cname, cdata) in enumerate(_comp_logos.items()):
                _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if cidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                if isinstance(cdata, dict):
                    _logo_url = cdata.get("logo", cdata.get("url", "N/A"))
                    _domain = cdata.get("domain", "N/A")
                elif isinstance(cdata, str):
                    _logo_url = cdata
                    _domain = "N/A"
                else:
                    continue
                for ci, val in enumerate([cname, str(_logo_url), str(_domain)]):
                    cell = ws_comp.cell(row=comp_row, column=2 + ci, value=val)
                    cell.font = Font(name="Calibri", size=10, bold=(ci == 0))
                    cell.fill = _row_fill
                    cell.border = thin_border
                    cell.alignment = wrap_alignment
                comp_row += 1

        comp_row += 1
        ws_comp.merge_cells(f"B{comp_row}:G{comp_row}")
        ws_comp.cell(row=comp_row, column=2, value="Data sourced from Wikipedia, Clearbit, SEC EDGAR filings, BLS QCEW, and internal knowledge base. Competitor data based on industry classification and domain analysis.").font = Font(name="Calibri", italic=True, size=9, color="596780")

    # ── Sheet: Budget Allocation ──
    _budget_alloc = data.get("_budget_allocation", {})
    if isinstance(_budget_alloc, dict) and _budget_alloc:
        ws_ba = wb.create_sheet("Budget Allocation")
        ws_ba.sheet_properties.tabColor = "1B6B3A"
        ws_ba.column_dimensions["A"].width = 3
        ws_ba.column_dimensions["B"].width = 24
        ws_ba.column_dimensions["C"].width = 10
        ws_ba.column_dimensions["D"].width = 16
        ws_ba.column_dimensions["E"].width = 16
        ws_ba.column_dimensions["F"].width = 18
        ws_ba.column_dimensions["G"].width = 16
        ws_ba.column_dimensions["H"].width = 12
        ws_ba.column_dimensions["I"].width = 12
        ws_ba.column_dimensions["J"].width = 14
        ws_ba.column_dimensions["K"].width = 12

        ws_ba.merge_cells("B2:K2")
        ws_ba["B2"].value = "Budget Allocation & Projected Outcomes"
        ws_ba["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_ba["B2"].border = accent_bottom_border

        ba_row = 4

        # Summary hero metrics
        _total_proj = _budget_alloc.get("total_projected", {})
        _bstr_display = str(data.get("budget", "") or "")
        try:
            _bnums_d = re.findall(r'[\d]+', _bstr_display.replace(",", "").replace("$", "").strip())
            _total_budget_val = float(_bnums_d[0]) if _bnums_d else 0
        except (ValueError, IndexError):
            _total_budget_val = 0

        _proj_hires = _total_proj.get("hires", 0)
        _proj_cph = _total_proj.get("cost_per_hire", 0)

        _hero_items = [
            ("TOTAL BUDGET", f"${_total_budget_val:,.0f}" if _total_budget_val > 0 else str(data.get("budget", "N/A"))),
            ("PROJECTED HIRES", f"{_proj_hires:,.0f}" if isinstance(_proj_hires, (int, float)) else str(_proj_hires)),
            ("PROJECTED COST/HIRE", f"${_proj_cph:,.0f}" if isinstance(_proj_cph, (int, float)) and _proj_cph > 0 else "N/A"),
        ]
        for hidx, (hlabel, hval) in enumerate(_hero_items):
            col_start = 2 + hidx * 3
            col_end = col_start + 2
            ws_ba.merge_cells(start_row=ba_row, start_column=col_start, end_row=ba_row, end_column=col_end)
            hcell = ws_ba.cell(row=ba_row, column=col_start, value=hval)
            hcell.font = Font(name="Calibri", bold=True, size=18, color="FFFFFF")
            hcell.fill = PatternFill(start_color=ACCENT, end_color=ACCENT, fill_type="solid")
            hcell.alignment = Alignment(horizontal="center", vertical="center")
            for cc in range(col_start, col_end + 1):
                ws_ba.cell(row=ba_row, column=cc).fill = PatternFill(start_color=ACCENT, end_color=ACCENT, fill_type="solid")
            ws_ba.merge_cells(start_row=ba_row + 1, start_column=col_start, end_row=ba_row + 1, end_column=col_end)
            lcell = ws_ba.cell(row=ba_row + 1, column=col_start, value=hlabel)
            lcell.font = Font(name="Calibri", bold=True, size=9, color="FFFFFF")
            lcell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
            lcell.alignment = Alignment(horizontal="center", vertical="center")
            for cc in range(col_start, col_end + 1):
                ws_ba.cell(row=ba_row + 1, column=cc).fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
        ws_ba.row_dimensions[ba_row].height = 40
        ba_row += 3

        # Channel allocation table
        ba_row += 1
        style_section_header(ws_ba, ba_row, 2, 11, "Channel-Level Budget Allocation")
        ba_row += 2

        ba_ch_headers = ["Channel", "%", "$ Amount", "Proj. Clicks", "Proj. Applications", "Proj. Hires", "CPC", "CPA", "Cost/Hire", "ROI Score"]
        for i, h in enumerate(ba_ch_headers):
            cell = ws_ba.cell(row=ba_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        ba_row += 1

        _ch_allocs = _budget_alloc.get("channel_allocations", {})
        _total_spend = 0
        _total_clicks = 0
        _total_apps = 0
        _total_hires_ch = 0

        if isinstance(_ch_allocs, dict):
            for chidx, (ch_name, ch_data) in enumerate(_ch_allocs.items()):
                if not isinstance(ch_data, dict):
                    continue
                _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if chidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                _pct = ch_data.get("percentage", 0)
                _amt = ch_data.get("dollar_amount", ch_data.get("amount", 0))
                _clicks = ch_data.get("projected_clicks", 0)
                _apps = ch_data.get("projected_applications", 0)
                _hires = ch_data.get("projected_hires", 0)
                _cpc = ch_data.get("cpc", ch_data.get("cost_per_click", 0))
                _cpa = ch_data.get("cpa", ch_data.get("cost_per_application", 0))
                _cph = ch_data.get("cost_per_hire", 0)
                _roi = ch_data.get("roi_score", ch_data.get("roi", "N/A"))

                _total_spend += _amt if isinstance(_amt, (int, float)) else 0
                _total_clicks += _clicks if isinstance(_clicks, (int, float)) else 0
                _total_apps += _apps if isinstance(_apps, (int, float)) else 0
                _total_hires_ch += _hires if isinstance(_hires, (int, float)) else 0

                _ch_vals = [
                    ch_name.replace("_", " ").title(),
                    f"{_pct:.0f}%" if isinstance(_pct, (int, float)) else str(_pct),
                    f"${_amt:,.0f}" if isinstance(_amt, (int, float)) else str(_amt),
                    f"{_clicks:,.0f}" if isinstance(_clicks, (int, float)) else str(_clicks),
                    f"{_apps:,.0f}" if isinstance(_apps, (int, float)) else str(_apps),
                    f"{_hires:,.1f}" if isinstance(_hires, (int, float)) else str(_hires),
                    f"${_cpc:.2f}" if isinstance(_cpc, (int, float)) else str(_cpc),
                    f"${_cpa:.2f}" if isinstance(_cpa, (int, float)) else str(_cpa),
                    f"${_cph:,.0f}" if isinstance(_cph, (int, float)) and _cph > 0 else "N/A",
                    f"{_roi:.2f}" if isinstance(_roi, (int, float)) else str(_roi),
                ]
                for ci, val in enumerate(_ch_vals):
                    cell = ws_ba.cell(row=ba_row, column=2 + ci, value=val)
                    cell.font = Font(name="Calibri", size=10, bold=(ci == 0))
                    cell.fill = _row_fill
                    cell.border = thin_border
                    cell.alignment = center_alignment if ci > 0 else wrap_alignment
                ba_row += 1

        # Summary totals row
        _total_vals = ["TOTAL", "100%", f"${_total_spend:,.0f}", f"{_total_clicks:,.0f}", f"{_total_apps:,.0f}", f"{_total_hires_ch:,.1f}", "", "", f"${_total_spend / max(_total_hires_ch, 1):,.0f}" if _total_hires_ch > 0 else "N/A", ""]
        for ci, val in enumerate(_total_vals):
            cell = ws_ba.cell(row=ba_row, column=2 + ci, value=val)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        ba_row += 2

        # Sufficiency assessment
        _suff = _budget_alloc.get("sufficiency", {})
        if isinstance(_suff, dict) and _suff:
            style_section_header(ws_ba, ba_row, 2, 11, "Budget Sufficiency Assessment")
            ba_row += 2
            _suff_grade = _suff.get("grade", _suff.get("rating", "N/A"))
            _suff_msg = _suff.get("message", _suff.get("assessment", ""))
            _suff_color = "2E7D32" if str(_suff_grade).upper() in ("A", "B", "SUFFICIENT", "GOOD") else "F57C00" if str(_suff_grade).upper() in ("C", "MODERATE", "MARGINAL") else "C62828"
            ws_ba.merge_cells(f"B{ba_row}:K{ba_row}")
            _suff_cell = ws_ba.cell(row=ba_row, column=2, value=f"Grade: {_suff_grade}  |  {_suff_msg}")
            _suff_cell.font = Font(name="Calibri", bold=True, size=11, color=_suff_color)
            ba_row += 1

        # Warnings
        _warnings = _budget_alloc.get("warnings", [])
        if _warnings and isinstance(_warnings, list):
            ba_row += 1
            for w in _warnings:
                ws_ba.merge_cells(f"B{ba_row}:K{ba_row}")
                ws_ba.cell(row=ba_row, column=2, value=f"  Warning: {w}").font = Font(name="Calibri", size=10, color="C62828")
                ba_row += 1

        # Budget Reality Check section
        _ba_suff = _budget_alloc.get("sufficiency", {})
        ba_reality = _ba_suff.get("budget_reality_check", {}) if isinstance(_ba_suff, dict) else {}
        if ba_reality:
            tier = ba_reality.get("feasibility_tier", "")
            if tier in ("impossible", "severely_underfunded", "underfunded"):
                ba_row += 1
                style_section_header(ws_ba, ba_row, 2, 11, "BUDGET REALITY CHECK")
                # Override the section header color to red for urgency
                for col_idx in range(2, 12):
                    _rc = ws_ba.cell(row=ba_row, column=col_idx)
                    if _rc.value:
                        _rc.font = Font(name="Calibri", bold=True, size=13, color="FF0000")
                ba_row += 2

                _rc_label = ba_reality.get("feasibility_label", "WARNING")
                _rc_msg = ba_reality.get("feasibility_message", "")
                _rc_color = "C00000" if tier == "impossible" else "E65100" if tier == "severely_underfunded" else "F57C00"

                ws_ba.merge_cells(f"B{ba_row}:K{ba_row}")
                _lbl_cell = ws_ba.cell(row=ba_row, column=2, value=_rc_label)
                _lbl_cell.font = Font(name="Calibri", bold=True, size=12, color="FFFFFF")
                _lbl_cell.fill = PatternFill(start_color=_rc_color, end_color=_rc_color, fill_type="solid")
                _lbl_cell.alignment = Alignment(horizontal="center", vertical="center")
                for _cc in range(2, 12):
                    ws_ba.cell(row=ba_row, column=_cc).fill = PatternFill(start_color=_rc_color, end_color=_rc_color, fill_type="solid")
                ba_row += 1

                ws_ba.merge_cells(f"B{ba_row}:K{ba_row}")
                _msg_cell = ws_ba.cell(row=ba_row, column=2, value=_rc_msg)
                _msg_cell.font = Font(name="Calibri", size=10, color=_rc_color)
                _msg_cell.alignment = Alignment(wrap_text=True, vertical="center")
                ws_ba.row_dimensions[ba_row].height = 45
                ba_row += 2

                _rc_details = [
                    ("Budget per Hire:", f"${ba_reality.get('budget_per_hire', 0):,.0f}"),
                    ("Industry Avg CPH:", f"${ba_reality.get('industry_avg_cph', 0):,.0f}"),
                    ("Realistic Hires at This Budget:", str(ba_reality.get('realistic_hires', 'N/A'))),
                    ("Target Hires:", str(ba_reality.get('target_hires', 'N/A'))),
                    ("Recommended Min Budget:", f"${ba_reality.get('min_viable_budget', 0):,.0f}"),
                ]
                for _dl, _dv in _rc_details:
                    ws_ba.cell(row=ba_row, column=2, value=f"  {_dl}").font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                    ws_ba.merge_cells(f"D{ba_row}:F{ba_row}")
                    ws_ba.cell(row=ba_row, column=4, value=_dv).font = Font(name="Calibri", size=10, color="333333")
                    ba_row += 1
                ba_row += 1

        # Optimization recommendations
        _recs = _budget_alloc.get("recommendations", [])
        if _recs and isinstance(_recs, list):
            ba_row += 1
            style_section_header(ws_ba, ba_row, 2, 11, "Optimization Recommendations")
            ba_row += 2
            for ridx, rec in enumerate(_recs):
                if isinstance(rec, dict):
                    rec_text = rec.get("recommendation", rec.get("message", str(rec)))
                else:
                    rec_text = str(rec)
                ws_ba.merge_cells(f"B{ba_row}:K{ba_row}")
                ws_ba.cell(row=ba_row, column=2, value=f"  {ridx + 1}. {rec_text}").font = Font(name="Calibri", size=10, color="333333")
                ba_row += 1

    # ── Sheet: Campaign Projections (structured estimates) ──
    if isinstance(_budget_alloc, dict) and _budget_alloc:
        try:
            ws_proj = wb.create_sheet("Campaign Projections")
            ws_proj.sheet_properties.tabColor = "0A66C9"
            ws_proj.column_dimensions["A"].width = 3
            ws_proj.column_dimensions["B"].width = 30
            ws_proj.column_dimensions["C"].width = 18
            ws_proj.column_dimensions["D"].width = 16
            ws_proj.column_dimensions["E"].width = 16
            ws_proj.column_dimensions["F"].width = 16
            ws_proj.column_dimensions["G"].width = 16
            ws_proj.column_dimensions["H"].width = 16
            ws_proj.column_dimensions["I"].width = 16
            ws_proj.column_dimensions["J"].width = 16
            ws_proj.column_dimensions["K"].width = 16
            ws_proj.column_dimensions["L"].width = 16

            _proj_navy_fill = PatternFill(start_color="08294A", end_color="08294A", fill_type="solid")
            _proj_white_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
            _proj_alt_fill = PatternFill(start_color="F5F5F5", end_color="F5F5F5", fill_type="solid")
            _proj_header_font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            _proj_data_font = Font(name="Calibri", size=10)
            _proj_data_bold_font = Font(name="Calibri", bold=True, size=10)

            # Sheet title
            ws_proj.merge_cells("B2:L2")
            ws_proj["B2"].value = "Campaign Projections & Estimates"
            ws_proj["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
            ws_proj["B2"].border = accent_bottom_border

            ws_proj.merge_cells("B3:L3")
            ws_proj["B3"].value = "All projections generated by Joveo's budget allocation engine based on industry benchmarks, channel CPC data, and historical conversion rates."
            ws_proj["B3"].font = Font(name="Calibri", italic=True, size=9, color="596780")

            proj_row = 5

            # ── TABLE 1: Campaign Summary Projections ──
            _proj_total = _budget_alloc.get("total_projected", {})
            if not isinstance(_proj_total, dict):
                _proj_total = {}
            _proj_meta = _budget_alloc.get("metadata", {})
            if not isinstance(_proj_meta, dict):
                _proj_meta = {}
            _proj_tb = _proj_meta.get("total_budget", 0)

            style_section_header(ws_proj, proj_row, 2, 6, "Campaign Summary Projections")
            proj_row += 2

            # Header row
            for ci, hdr in enumerate(["Metric", "Value", "Confidence", "Source", "Notes"]):
                cell = ws_proj.cell(row=proj_row, column=2 + ci, value=hdr)
                cell.font = _proj_header_font
                cell.fill = _proj_navy_fill
                cell.border = thin_border
                cell.alignment = center_alignment
            proj_row += 1

            _proj_clicks = _proj_total.get("clicks", 0)
            _proj_apps = _proj_total.get("applications", 0)
            _proj_hires = _proj_total.get("hires", 0)
            _proj_cpc_avg = _proj_total.get("cost_per_click", 0)
            _proj_cpa_avg = _proj_total.get("cost_per_application", 0)
            _proj_cph_avg = _proj_total.get("cost_per_hire", 0)
            _proj_util = ((_proj_clicks * _proj_cpc_avg) / _proj_tb * 100) if _proj_tb > 0 and _proj_cpc_avg > 0 and _proj_clicks > 0 else 100.0

            _summary_rows = [
                ("Total Budget", f"${_proj_tb:,.0f}" if _proj_tb > 0 else "N/A", "-", "User Input", "Campaign investment amount"),
                ("Projected Total Clicks", f"{_proj_clicks:,.0f}" if _proj_clicks else "N/A", "Medium", "Budget Engine", "Aggregate across all channels"),
                ("Projected Total Applications", f"{_proj_apps:,.0f}" if _proj_apps else "N/A", "Medium", "Budget Engine", "Based on channel apply rates"),
                ("Projected Total Hires", f"{_proj_hires:,.0f}" if _proj_hires else "N/A", "Medium", "Budget Engine", "Based on tier-blended hire rates"),
                ("Avg Cost Per Click", f"${_proj_cpc_avg:,.2f}" if _proj_cpc_avg > 0 else "N/A", "Medium", "Budget Engine", "Weighted average across channels"),
                ("Avg Cost Per Application", f"${_proj_cpa_avg:,.2f}" if _proj_cpa_avg > 0 else "N/A", "Medium", "Budget Engine", "Total budget / projected applications"),
                ("Avg Cost Per Hire", f"${_proj_cph_avg:,.0f}" if _proj_cph_avg > 0 else "N/A", "Medium", "Budget Engine", "Total budget / projected hires"),
                ("Budget Utilization", f"{min(_proj_util, 100):.1f}%", "High", "Budget Engine", "Percentage of budget allocated to channels"),
            ]

            for ridx, (metric, val, conf, src, note) in enumerate(_summary_rows):
                _rfill = _proj_white_fill if ridx % 2 == 0 else _proj_alt_fill
                for ci, cell_val in enumerate([metric, val, conf, src, note]):
                    cell = ws_proj.cell(row=proj_row, column=2 + ci, value=cell_val)
                    cell.font = _proj_data_bold_font if ci == 0 else _proj_data_font
                    cell.fill = _rfill
                    cell.border = thin_border
                    cell.alignment = center_alignment if ci > 0 else wrap_alignment
                proj_row += 1

            proj_row += 2

            # ── TABLE 2: Channel-by-Channel Projections ──
            _proj_ch_allocs = _budget_alloc.get("channel_allocations", {})
            if not isinstance(_proj_ch_allocs, dict):
                _proj_ch_allocs = {}

            if _proj_ch_allocs:
                style_section_header(ws_proj, proj_row, 2, 12, "Channel-by-Channel Projections")
                proj_row += 2

                _ch_headers = ["Channel", "Budget", "% Share", "CPC", "Clicks", "Applications", "Hires", "CPA", "Cost/Hire", "ROI Score", "Confidence"]
                for ci, hdr in enumerate(_ch_headers):
                    cell = ws_proj.cell(row=proj_row, column=2 + ci, value=hdr)
                    cell.font = _proj_header_font
                    cell.fill = _proj_navy_fill
                    cell.border = thin_border
                    cell.alignment = center_alignment
                proj_row += 1

                _ch_total_spend = 0
                _ch_total_clicks = 0
                _ch_total_apps = 0
                _ch_total_hires = 0

                # Sort channels by dollar amount descending
                _sorted_chs = sorted(
                    _proj_ch_allocs.items(),
                    key=lambda x: x[1].get("dollar_amount", x[1].get("dollars", 0)) if isinstance(x[1], dict) else 0,
                    reverse=True
                )

                for chidx, (ch_name, ch_data) in enumerate(_sorted_chs):
                    if not isinstance(ch_data, dict):
                        continue
                    _rfill = _proj_white_fill if chidx % 2 == 0 else _proj_alt_fill
                    _ch_dollars = ch_data.get("dollar_amount", ch_data.get("dollars", 0))
                    _ch_pct = ch_data.get("percentage", 0)
                    _ch_cpc = ch_data.get("cpc", 0)
                    _ch_clicks = ch_data.get("projected_clicks", 0)
                    _ch_apps = ch_data.get("projected_applications", 0)
                    _ch_hires = ch_data.get("projected_hires", 0)
                    _ch_cpa = ch_data.get("cpa", ch_data.get("cost_per_application", 0))
                    _ch_cph = ch_data.get("cost_per_hire", 0)
                    _ch_roi = ch_data.get("roi_score", 0)
                    _ch_conf = ch_data.get("confidence", "low").title()

                    _ch_total_spend += _ch_dollars if isinstance(_ch_dollars, (int, float)) else 0
                    _ch_total_clicks += _ch_clicks if isinstance(_ch_clicks, (int, float)) else 0
                    _ch_total_apps += _ch_apps if isinstance(_ch_apps, (int, float)) else 0
                    _ch_total_hires += _ch_hires if isinstance(_ch_hires, (int, float)) else 0

                    _ch_vals = [
                        ch_name.replace("_", " ").title(),
                        f"${_ch_dollars:,.0f}" if isinstance(_ch_dollars, (int, float)) else str(_ch_dollars),
                        f"{_ch_pct:.1f}%" if isinstance(_ch_pct, (int, float)) else str(_ch_pct),
                        f"${_ch_cpc:.2f}" if isinstance(_ch_cpc, (int, float)) and _ch_cpc > 0 else "N/A",
                        f"{_ch_clicks:,.0f}" if isinstance(_ch_clicks, (int, float)) else str(_ch_clicks),
                        f"{_ch_apps:,.0f}" if isinstance(_ch_apps, (int, float)) else str(_ch_apps),
                        f"{_ch_hires:,.1f}" if isinstance(_ch_hires, (int, float)) else str(_ch_hires),
                        f"${_ch_cpa:.2f}" if isinstance(_ch_cpa, (int, float)) and _ch_cpa > 0 else "N/A",
                        f"${_ch_cph:,.0f}" if isinstance(_ch_cph, (int, float)) and _ch_cph > 0 else "N/A",
                        f"{_ch_roi:.1f}/10" if isinstance(_ch_roi, (int, float)) else str(_ch_roi),
                        _ch_conf,
                    ]
                    for ci, val in enumerate(_ch_vals):
                        cell = ws_proj.cell(row=proj_row, column=2 + ci, value=val)
                        cell.font = _proj_data_bold_font if ci == 0 else _proj_data_font
                        cell.fill = _rfill
                        cell.border = thin_border
                        cell.alignment = center_alignment if ci > 0 else wrap_alignment
                    proj_row += 1

                # Totals row
                _ch_totals_vals = [
                    "TOTAL",
                    f"${_ch_total_spend:,.0f}",
                    "100%",
                    "",
                    f"{_ch_total_clicks:,.0f}",
                    f"{_ch_total_apps:,.0f}",
                    f"{_ch_total_hires:,.1f}",
                    f"${_ch_total_spend / max(_ch_total_apps, 1):,.2f}" if _ch_total_apps > 0 else "N/A",
                    f"${_ch_total_spend / max(_ch_total_hires, 1):,.0f}" if _ch_total_hires > 0 else "N/A",
                    "",
                    "",
                ]
                for ci, val in enumerate(_ch_totals_vals):
                    cell = ws_proj.cell(row=proj_row, column=2 + ci, value=val)
                    cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                    cell.fill = _proj_navy_fill
                    cell.border = thin_border
                    cell.alignment = center_alignment
                proj_row += 2

            # ── TABLE 3: Role-by-Role Budget Distribution ──
            _proj_role_allocs = _budget_alloc.get("role_allocations", {})
            if not isinstance(_proj_role_allocs, dict):
                _proj_role_allocs = {}

            if _proj_role_allocs:
                proj_row += 1
                style_section_header(ws_proj, proj_row, 2, 8, "Role-by-Role Budget Distribution")
                proj_row += 2

                _role_headers = ["Role", "Budget Share", "Dollar Amount", "Tier", "Openings", "$ Per Opening", "Notes"]
                for ci, hdr in enumerate(_role_headers):
                    cell = ws_proj.cell(row=proj_row, column=2 + ci, value=hdr)
                    cell.font = _proj_header_font
                    cell.fill = _proj_navy_fill
                    cell.border = thin_border
                    cell.alignment = center_alignment
                proj_row += 1

                _sorted_roles = sorted(
                    _proj_role_allocs.items(),
                    key=lambda x: x[1].get("dollar_amount", 0) if isinstance(x[1], dict) else 0,
                    reverse=True
                )

                for ridx, (role_name, role_data) in enumerate(_sorted_roles):
                    if not isinstance(role_data, dict):
                        continue
                    _rfill = _proj_white_fill if ridx % 2 == 0 else _proj_alt_fill
                    _r_share = role_data.get("budget_share", 0)
                    _r_dollars = role_data.get("dollar_amount", 0)
                    _r_tier = role_data.get("tier", "default").replace("_", " ").title()
                    _r_openings = role_data.get("openings", role_data.get("headcount", 1))
                    _r_per_opening = _r_dollars / max(_r_openings, 1) if _r_dollars > 0 else 0
                    _r_mult = role_data.get("multiplier", 1.0)

                    _r_notes = ""
                    if _r_mult > 1.2:
                        _r_notes = "High-priority (elevated spend multiplier)"
                    elif _r_mult < 0.8:
                        _r_notes = "Lower complexity (reduced spend multiplier)"

                    _role_vals = [
                        role_name.strip().title(),
                        f"{_r_share * 100:.1f}%",
                        f"${_r_dollars:,.0f}",
                        _r_tier,
                        str(int(_r_openings)),
                        f"${_r_per_opening:,.0f}",
                        _r_notes,
                    ]
                    for ci, val in enumerate(_role_vals):
                        cell = ws_proj.cell(row=proj_row, column=2 + ci, value=val)
                        cell.font = _proj_data_bold_font if ci == 0 else _proj_data_font
                        cell.fill = _rfill
                        cell.border = thin_border
                        cell.alignment = center_alignment if ci > 0 else wrap_alignment
                    proj_row += 1

                proj_row += 1

            # ── TABLE 4: Budget Feasibility Assessment ──
            _proj_suff = _budget_alloc.get("sufficiency", {})
            if not isinstance(_proj_suff, dict):
                _proj_suff = {}

            if _proj_suff:
                proj_row += 1
                style_section_header(ws_proj, proj_row, 2, 6, "Budget Feasibility Assessment")
                proj_row += 2

                for ci, hdr in enumerate(["Assessment", "Value", "Details", "Status", "Action Required"]):
                    cell = ws_proj.cell(row=proj_row, column=2 + ci, value=hdr)
                    cell.font = _proj_header_font
                    cell.fill = _proj_navy_fill
                    cell.border = thin_border
                    cell.alignment = center_alignment
                proj_row += 1

                _suff_sufficient = _proj_suff.get("sufficient", False)
                _suff_bpo = _proj_suff.get("budget_per_opening", 0)
                _suff_avg_cph = _proj_suff.get("industry_avg_cost_per_hire", 0)
                _suff_gap = _proj_suff.get("gap_amount", 0)
                _suff_proj_hires = _proj_suff.get("total_projected_hires", 0)
                _suff_openings = _proj_meta.get("total_openings", 0)

                # Determine feasibility label
                if _suff_sufficient and _suff_bpo >= _suff_avg_cph:
                    _feas_label = "Well Funded"
                    _feas_status = "PASS"
                elif _suff_sufficient:
                    _feas_label = "Adequate"
                    _feas_status = "PASS"
                else:
                    _feas_label = "Underfunded"
                    _feas_status = "WARNING"

                _assess_rows = [
                    ("Feasibility Rating", _feas_label, f"Budget is {'sufficient' if _suff_sufficient else 'below recommended levels'} for target openings", _feas_status, "Review if WARNING"),
                    ("Budget Per Opening", f"${_suff_bpo:,.0f}" if _suff_bpo > 0 else "N/A", f"Total budget divided by {_suff_openings} openings" if _suff_openings > 0 else "Based on total budget allocation", "INFO", "-"),
                    ("Industry Avg CPH", f"${_suff_avg_cph:,.0f}" if _suff_avg_cph > 0 else "N/A", f"Average cost-per-hire for {_proj_meta.get('industry', 'general').replace('_', ' ').title()}", "BENCHMARK", "-"),
                    ("Projected Hires", f"{_suff_proj_hires:,.0f}" if _suff_proj_hires else "N/A", f"Against target of {_suff_openings}" if _suff_openings > 0 else "Budget engine projection", "INFO", "Monitor during campaign"),
                    ("Budget Gap", f"${_suff_gap:,.0f}" if _suff_gap > 0 else "$0 (No Gap)", "Additional budget needed to meet all openings at industry avg CPH" if _suff_gap > 0 else "Budget covers projected needs", "WARNING" if _suff_gap > 0 else "PASS", "Consider increasing budget" if _suff_gap > 0 else "-"),
                ]

                for ridx, (assess, val, detail, status, action) in enumerate(_assess_rows):
                    _rfill = _proj_white_fill if ridx % 2 == 0 else _proj_alt_fill
                    # Color-code the status
                    _status_color = "2E7D32" if status == "PASS" else "F57C00" if status == "WARNING" else "333333"

                    for ci, cell_val in enumerate([assess, val, detail, status, action]):
                        cell = ws_proj.cell(row=proj_row, column=2 + ci, value=cell_val)
                        if ci == 3:
                            cell.font = Font(name="Calibri", bold=True, size=10, color=_status_color)
                        elif ci == 0:
                            cell.font = _proj_data_bold_font
                        else:
                            cell.font = _proj_data_font
                        cell.fill = _rfill
                        cell.border = thin_border
                        cell.alignment = center_alignment if ci > 0 else wrap_alignment
                    proj_row += 1

                # Warnings subsection
                _proj_warnings = _budget_alloc.get("warnings", [])
                if _proj_warnings and isinstance(_proj_warnings, list):
                    proj_row += 1
                    ws_proj.merge_cells(f"B{proj_row}:F{proj_row}")
                    ws_proj.cell(row=proj_row, column=2, value="Warnings").font = Font(name="Calibri", bold=True, size=11, color="C62828")
                    proj_row += 1
                    for w in _proj_warnings:
                        ws_proj.merge_cells(f"B{proj_row}:L{proj_row}")
                        ws_proj.cell(row=proj_row, column=2, value=f"  {w}").font = Font(name="Calibri", size=10, color="C62828")
                        ws_proj.row_dimensions[proj_row].height = 30
                        ws_proj.cell(row=proj_row, column=2).alignment = wrap_alignment
                        proj_row += 1

                # Recommendations subsection
                _proj_recs = _budget_alloc.get("recommendations", [])
                if _proj_recs and isinstance(_proj_recs, list):
                    proj_row += 1
                    ws_proj.merge_cells(f"B{proj_row}:F{proj_row}")
                    ws_proj.cell(row=proj_row, column=2, value="Optimization Recommendations").font = Font(name="Calibri", bold=True, size=11, color="2E7D32")
                    proj_row += 1
                    for ridx, rec in enumerate(_proj_recs):
                        if isinstance(rec, dict):
                            rec_text = rec.get("recommendation", rec.get("message", str(rec)))
                        else:
                            rec_text = str(rec)
                        ws_proj.merge_cells(f"B{proj_row}:L{proj_row}")
                        ws_proj.cell(row=proj_row, column=2, value=f"  {ridx + 1}. {rec_text}").font = Font(name="Calibri", size=10, color="333333")
                        ws_proj.row_dimensions[proj_row].height = 30
                        ws_proj.cell(row=proj_row, column=2).alignment = wrap_alignment
                        proj_row += 1

            # ── Optimization Suggestions ──
            _proj_opt_sugg = _budget_alloc.get("optimization_suggestions", [])
            if _proj_opt_sugg and isinstance(_proj_opt_sugg, list):
                proj_row += 2
                ws_proj.merge_cells(f"B{proj_row}:L{proj_row}")
                ws_proj.cell(row=proj_row, column=2, value="Optimization Suggestions").font = Font(name="Calibri", bold=True, size=11, color=ACCENT)
                proj_row += 1
                for sidx, sugg in enumerate(_proj_opt_sugg):
                    ws_proj.merge_cells(f"B{proj_row}:L{proj_row}")
                    ws_proj.cell(row=proj_row, column=2, value=f"  {sidx + 1}. {sugg}").font = Font(name="Calibri", size=10, color="333333")
                    ws_proj.row_dimensions[proj_row].height = 30
                    ws_proj.cell(row=proj_row, column=2).alignment = wrap_alignment
                    proj_row += 1

            # Footer note
            proj_row += 2
            ws_proj.merge_cells(f"B{proj_row}:L{proj_row}")
            ws_proj.cell(row=proj_row, column=2, value="Projections are estimates based on industry benchmarks, historical channel performance data, and Joveo's ML models. Actual results may vary based on market conditions, job posting quality, and campaign optimization.").font = Font(name="Calibri", italic=True, size=9, color="596780")
            ws_proj.row_dimensions[proj_row].height = 30
            ws_proj.cell(row=proj_row, column=2).alignment = wrap_alignment

        except Exception:
            pass  # Graceful fallback: skip Campaign Projections sheet if data is malformed

    # ── Sheet: Workforce Trends ──
    _workforce = _synth.get("workforce_insights", {})
    if isinstance(_workforce, dict) and _workforce:
        try:
            ws_wf = wb.create_sheet("Workforce Trends")
            ws_wf.sheet_properties.tabColor = "7B1FA2"
            ws_wf.column_dimensions["A"].width = 3
            ws_wf.column_dimensions["B"].width = 30
            ws_wf.column_dimensions["C"].width = 22
            ws_wf.column_dimensions["D"].width = 22
            ws_wf.column_dimensions["E"].width = 22
            ws_wf.column_dimensions["F"].width = 22
            ws_wf.column_dimensions["G"].width = 22

            ws_wf.merge_cells("B2:G2")
            ws_wf["B2"].value = "Workforce Trends & Insights"
            ws_wf["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
            ws_wf["B2"].border = accent_bottom_border

            wf_row = 4
            ws_wf.merge_cells(f"B{wf_row}:G{wf_row}")
            ws_wf.cell(row=wf_row, column=2, value="Workforce intelligence derived from industry research, white papers, and knowledge base data. Use these insights to tailor messaging, channel selection, and employer brand positioning.").font = Font(name="Calibri", italic=True, size=9, color="596780")
            wf_row += 2

            # --- Gen-Z Insights ---
            _gen_z = _workforce.get("gen_z_insights", {})
            if isinstance(_gen_z, dict) and _gen_z:
                style_section_header(ws_wf, wf_row, 2, 7, "Gen-Z Workforce Insights")
                wf_row += 2

                _gz_share = _gen_z.get("workforce_share")
                if _gz_share:
                    ws_wf.merge_cells(f"B{wf_row}:G{wf_row}")
                    ws_wf.cell(row=wf_row, column=2, value=f"Gen-Z Workforce Share: {_gz_share}").font = Font(name="Calibri", bold=True, size=11, color=ACCENT)
                    wf_row += 2

                # Platform Usage
                _gz_platforms = _gen_z.get("job_search_platforms", {})
                if isinstance(_gz_platforms, dict) and _gz_platforms:
                    ws_wf.cell(row=wf_row, column=2, value="Job Search Platform Preferences").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                    wf_row += 1
                    for i, h in enumerate(["Platform", "Usage Rate"]):
                        cell = ws_wf.cell(row=wf_row, column=2 + i, value=h)
                        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                        cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                        cell.border = thin_border
                        cell.alignment = center_alignment
                    wf_row += 1
                    for pidx, (pname, pval) in enumerate(_gz_platforms.items()):
                        _wf_rf = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if pidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                        c1 = ws_wf.cell(row=wf_row, column=2, value=str(pname))
                        c1.font = Font(name="Calibri", bold=True, size=10)
                        c1.fill = _wf_rf
                        c1.border = thin_border
                        _pval_str = f"{pval}" if not isinstance(pval, (int, float)) else f"{pval:.0%}" if pval <= 1 else f"{pval}%"
                        c2 = ws_wf.cell(row=wf_row, column=3, value=_pval_str)
                        c2.font = Font(name="Calibri", size=10)
                        c2.fill = _wf_rf
                        c2.border = thin_border
                        c2.alignment = center_alignment
                        wf_row += 1
                    wf_row += 1

                # Mobile vs Desktop
                _mobile = _gen_z.get("mobile_vs_desktop", {})
                if isinstance(_mobile, dict) and _mobile:
                    ws_wf.cell(row=wf_row, column=2, value="Mobile vs Desktop Behavior").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                    wf_row += 1
                    for mkey, mval in _mobile.items():
                        _wf_mf = PatternFill(start_color="E8F0FE", end_color="E8F0FE", fill_type="solid")
                        c1 = ws_wf.cell(row=wf_row, column=2, value=str(mkey).replace("_", " ").title())
                        c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                        c1.fill = _wf_mf
                        c1.border = thin_border
                        ws_wf.merge_cells(f"C{wf_row}:D{wf_row}")
                        c2 = ws_wf.cell(row=wf_row, column=3, value=str(mval))
                        c2.font = Font(name="Calibri", size=10)
                        c2.fill = _wf_mf
                        c2.border = thin_border
                        c2.alignment = wrap_alignment
                        wf_row += 1
                    wf_row += 1

                # Social Media Habits
                _social = _gen_z.get("social_media_habits", {})
                if isinstance(_social, dict) and _social:
                    ws_wf.cell(row=wf_row, column=2, value="Social Media Habits").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                    wf_row += 1
                    for skey, sval in _social.items():
                        c1 = ws_wf.cell(row=wf_row, column=2, value=str(skey).replace("_", " ").title())
                        c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                        c1.border = thin_border
                        ws_wf.merge_cells(f"C{wf_row}:G{wf_row}")
                        c2 = ws_wf.cell(row=wf_row, column=3, value=str(sval)[:300])
                        c2.font = Font(name="Calibri", size=10)
                        c2.border = thin_border
                        c2.alignment = wrap_alignment
                        wf_row += 1
                    wf_row += 1

                # Workplace Expectations
                _expectations = _gen_z.get("workplace_expectations", {})
                if isinstance(_expectations, dict) and _expectations:
                    style_section_header(ws_wf, wf_row, 2, 7, "Gen-Z Workplace Expectations")
                    wf_row += 2
                    for exp_key, exp_data in _expectations.items():
                        if not isinstance(exp_data, dict) or not exp_data:
                            continue
                        ws_wf.cell(row=wf_row, column=2, value=str(exp_key).replace("_", " ").title()).font = Font(name="Calibri", bold=True, size=11, color=ACCENT)
                        wf_row += 1
                        for ekey, eval_val in exp_data.items():
                            _wf_ef = PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                            c1 = ws_wf.cell(row=wf_row, column=2, value=str(ekey).replace("_", " ").title())
                            c1.font = Font(name="Calibri", size=10, color="333333")
                            c1.fill = _wf_ef
                            c1.border = thin_border
                            ws_wf.merge_cells(f"C{wf_row}:G{wf_row}")
                            c2 = ws_wf.cell(row=wf_row, column=3, value=str(eval_val)[:300])
                            c2.font = Font(name="Calibri", size=10)
                            c2.fill = _wf_ef
                            c2.border = thin_border
                            c2.alignment = wrap_alignment
                            wf_row += 1
                        wf_row += 1

                # Salary Expectations
                _salary_exp = _gen_z.get("salary_expectations", {})
                if isinstance(_salary_exp, dict) and _salary_exp:
                    ws_wf.cell(row=wf_row, column=2, value="Gen-Z Salary Expectations").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                    wf_row += 1
                    for skey, sval in _salary_exp.items():
                        c1 = ws_wf.cell(row=wf_row, column=2, value=str(skey).replace("_", " ").title())
                        c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                        c1.border = thin_border
                        ws_wf.merge_cells(f"C{wf_row}:D{wf_row}")
                        c2 = ws_wf.cell(row=wf_row, column=3, value=str(sval))
                        c2.font = Font(name="Calibri", size=10)
                        c2.border = thin_border
                        wf_row += 1
                    wf_row += 1

                # Tenure & Job Hopping
                _tenure = _gen_z.get("tenure", {})
                if isinstance(_tenure, dict) and _tenure:
                    ws_wf.cell(row=wf_row, column=2, value="Gen-Z Tenure & Job Hopping").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                    wf_row += 1
                    for tkey, tval in _tenure.items():
                        c1 = ws_wf.cell(row=wf_row, column=2, value=str(tkey).replace("_", " ").title())
                        c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                        c1.border = thin_border
                        ws_wf.merge_cells(f"C{wf_row}:D{wf_row}")
                        c2 = ws_wf.cell(row=wf_row, column=3, value=str(tval))
                        c2.font = Font(name="Calibri", size=10)
                        c2.border = thin_border
                        c2.alignment = wrap_alignment
                        wf_row += 1
                    wf_row += 1

            # --- Employer Branding ---
            _eb = _workforce.get("employer_branding", {})
            if isinstance(_eb, dict) and _eb:
                style_section_header(ws_wf, wf_row, 2, 7, "Employer Branding Intelligence")
                wf_row += 2
                _roi_data = _eb.get("roi_data", {})
                if isinstance(_roi_data, dict) and _roi_data:
                    ws_wf.cell(row=wf_row, column=2, value="Employer Branding ROI").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                    wf_row += 1
                    for rkey, rval in _roi_data.items():
                        c1 = ws_wf.cell(row=wf_row, column=2, value=str(rkey).replace("_", " ").title())
                        c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                        c1.border = thin_border
                        ws_wf.merge_cells(f"C{wf_row}:G{wf_row}")
                        c2 = ws_wf.cell(row=wf_row, column=3, value=str(rval)[:300])
                        c2.font = Font(name="Calibri", size=10)
                        c2.border = thin_border
                        c2.alignment = wrap_alignment
                        wf_row += 1
                    wf_row += 1
                _bp = _eb.get("best_practices", {})
                if isinstance(_bp, dict) and _bp:
                    ws_wf.cell(row=wf_row, column=2, value="Employer Branding Best Practices").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                    wf_row += 1
                    for bkey, bval in _bp.items():
                        c1 = ws_wf.cell(row=wf_row, column=2, value=str(bkey).replace("_", " ").title())
                        c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                        c1.border = thin_border
                        ws_wf.merge_cells(f"C{wf_row}:G{wf_row}")
                        c2 = ws_wf.cell(row=wf_row, column=3, value=str(bval)[:300])
                        c2.font = Font(name="Calibri", size=10)
                        c2.border = thin_border
                        c2.alignment = wrap_alignment
                        wf_row += 1
                    wf_row += 1
                _ch_eff = _eb.get("channel_effectiveness", {})
                if isinstance(_ch_eff, dict) and _ch_eff:
                    ws_wf.cell(row=wf_row, column=2, value="Employer Brand Channel Effectiveness").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                    wf_row += 1
                    for ckey, cval in _ch_eff.items():
                        c1 = ws_wf.cell(row=wf_row, column=2, value=str(ckey).replace("_", " ").title())
                        c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                        c1.border = thin_border
                        ws_wf.merge_cells(f"C{wf_row}:G{wf_row}")
                        c2 = ws_wf.cell(row=wf_row, column=3, value=str(cval)[:300])
                        c2.font = Font(name="Calibri", size=10)
                        c2.border = thin_border
                        c2.alignment = wrap_alignment
                        wf_row += 1
                    wf_row += 1

            # --- Supply Partner Trends ---
            _sp_trends = _workforce.get("supply_partner_trends", {})
            if isinstance(_sp_trends, dict) and _sp_trends:
                style_section_header(ws_wf, wf_row, 2, 7, "Supply Partner Trends")
                wf_row += 2
                for spkey, spval in _sp_trends.items():
                    c1 = ws_wf.cell(row=wf_row, column=2, value=str(spkey).replace("_", " ").title())
                    c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                    c1.border = thin_border
                    ws_wf.merge_cells(f"C{wf_row}:G{wf_row}")
                    c2 = ws_wf.cell(row=wf_row, column=3, value=str(spval)[:300])
                    c2.font = Font(name="Calibri", size=10)
                    c2.border = thin_border
                    c2.alignment = wrap_alignment
                    wf_row += 1
                wf_row += 1

            # --- Job Type Trends ---
            _jt_trends = _workforce.get("job_type_trends", {})
            if isinstance(_jt_trends, dict) and _jt_trends:
                style_section_header(ws_wf, wf_row, 2, 7, "Job Type Trends")
                wf_row += 2
                for jtkey, jtval in _jt_trends.items():
                    c1 = ws_wf.cell(row=wf_row, column=2, value=str(jtkey).replace("_", " ").title())
                    c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                    c1.border = thin_border
                    ws_wf.merge_cells(f"C{wf_row}:G{wf_row}")
                    c2 = ws_wf.cell(row=wf_row, column=3, value=str(jtval)[:300])
                    c2.font = Font(name="Calibri", size=10)
                    c2.border = thin_border
                    c2.alignment = wrap_alignment
                    wf_row += 1
                wf_row += 1

            # Footer
            ws_wf.merge_cells(f"B{wf_row}:G{wf_row}")
            ws_wf.cell(row=wf_row, column=2, value="Data sourced from recruitment industry knowledge base, workforce trends intelligence, and employer branding research. Gen-Z data reflects 2024-2026 behavioral studies.").font = Font(name="Calibri", italic=True, size=9, color="596780")

        except Exception as _wf_err:
            logger.warning("Workforce Trends sheet generation failed: %s", _wf_err)

    # ── Sheet: Sources & References ──
    _wf_for_refs = _synth.get("workforce_insights", {})
    _relevant_research = _wf_for_refs.get("relevant_research", []) if isinstance(_wf_for_refs, dict) else []
    if isinstance(_relevant_research, list) and _relevant_research:
        try:
            ws_refs = wb.create_sheet("Sources & References")
            ws_refs.sheet_properties.tabColor = "455A64"
            ws_refs.column_dimensions["A"].width = 3
            ws_refs.column_dimensions["B"].width = 5
            ws_refs.column_dimensions["C"].width = 40
            ws_refs.column_dimensions["D"].width = 22
            ws_refs.column_dimensions["E"].width = 12
            ws_refs.column_dimensions["F"].width = 55
            ws_refs.column_dimensions["G"].width = 14

            ws_refs.merge_cells("B2:G2")
            ws_refs["B2"].value = "Sources & References"
            ws_refs["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
            ws_refs["B2"].border = accent_bottom_border

            ref_row = 4
            ws_refs.merge_cells(f"B{ref_row}:G{ref_row}")
            ws_refs.cell(row=ref_row, column=2, value="Industry research reports and white papers referenced in this media plan. Reports are selected for relevance to your industry, target roles, and recruitment strategy.").font = Font(name="Calibri", italic=True, size=9, color="596780")
            ref_row += 2

            style_section_header(ws_refs, ref_row, 2, 7, "Research Reports & White Papers")
            ref_row += 2

            _ref_headers = ["#", "Report Title", "Publisher", "Year", "Key Findings", "Findings Count"]
            for i, h in enumerate(_ref_headers):
                cell = ws_refs.cell(row=ref_row, column=2 + i, value=h)
                cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                cell.border = thin_border
                cell.alignment = center_alignment
            ref_row += 1

            for ridx, report in enumerate(_relevant_research):
                if not isinstance(report, dict):
                    continue
                _ref_rf = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if ridx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                _r_title = report.get("title", "N/A")
                _r_publisher = report.get("publisher", "N/A")
                _r_year = report.get("year", "N/A")
                _r_findings = report.get("top_findings", [])
                _r_count = report.get("finding_count", 0)
                if isinstance(_r_findings, list) and _r_findings:
                    _findings_display = "; ".join(str(f)[:120] for f in _r_findings[:3])
                else:
                    _findings_display = "N/A"
                _r_vals = [
                    ridx + 1, str(_r_title), str(_r_publisher),
                    str(_r_year) if _r_year else "N/A", _findings_display,
                    str(_r_count) if isinstance(_r_count, (int, float)) else "N/A",
                ]
                for ci, val in enumerate(_r_vals):
                    cell = ws_refs.cell(row=ref_row, column=2 + ci, value=val)
                    cell.font = Font(name="Calibri", size=10, bold=(ci == 1))
                    cell.fill = _ref_rf
                    cell.border = thin_border
                    cell.alignment = center_alignment if ci in (0, 3, 5) else wrap_alignment
                ws_refs.row_dimensions[ref_row].height = 45
                ref_row += 1

            ref_row += 1
            ws_refs.merge_cells(f"B{ref_row}:G{ref_row}")
            ws_refs.cell(row=ref_row, column=2, value="Reports are ranked by relevance to your industry and recruitment objectives. Findings are summarized from original publications.").font = Font(name="Calibri", italic=True, size=9, color="596780")

        except Exception as _ref_err:
            logger.warning("Sources & References sheet generation failed: %s", _ref_err)

    # ── Sheet: Regional Intelligence ──
    _loc_profiles = _synth.get("location_profiles", {})
    if isinstance(_loc_profiles, dict) and _loc_profiles:
        _has_regional = any(
            isinstance(lp, dict) and isinstance(lp.get("regional_intelligence"), dict)
            for lp in _loc_profiles.values()
        )
        if _has_regional:
            try:
                ws_reg = wb.create_sheet("Regional Intelligence")
                ws_reg.sheet_properties.tabColor = "00695C"
                ws_reg.column_dimensions["A"].width = 3
                ws_reg.column_dimensions["B"].width = 24
                ws_reg.column_dimensions["C"].width = 20
                ws_reg.column_dimensions["D"].width = 26
                ws_reg.column_dimensions["E"].width = 26
                ws_reg.column_dimensions["F"].width = 28
                ws_reg.column_dimensions["G"].width = 28
                ws_reg.column_dimensions["H"].width = 22

                ws_reg.merge_cells("B2:H2")
                ws_reg["B2"].value = "Regional Intelligence"
                ws_reg["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
                ws_reg["B2"].border = accent_bottom_border

                reg_row = 4
                ws_reg.merge_cells(f"B{reg_row}:H{reg_row}")
                ws_reg.cell(row=reg_row, column=2, value="Per-market hiring intelligence including top job boards, dominant industries, talent dynamics, hiring regulations, cultural norms, and CPA benchmarks. Data sourced from regional hiring knowledge base.").font = Font(name="Calibri", italic=True, size=9, color="596780")
                reg_row += 2

                for loc_name, loc_data in _loc_profiles.items():
                    if not isinstance(loc_data, dict):
                        continue
                    _ri = loc_data.get("regional_intelligence", {})
                    if not isinstance(_ri, dict) or not _ri:
                        continue

                    style_section_header(ws_reg, reg_row, 2, 8, str(loc_name))
                    reg_row += 1
                    _ri_region = _ri.get("region", "N/A")
                    _ri_market = _ri.get("market", "N/A")
                    ws_reg.merge_cells(f"B{reg_row}:H{reg_row}")
                    ws_reg.cell(row=reg_row, column=2, value=f"Region: {_ri_region}  |  Market: {_ri_market}").font = Font(name="Calibri", italic=True, size=10, color="596780")
                    reg_row += 2

                    # Top Job Boards
                    _top_boards = _ri.get("top_job_boards", [])
                    if isinstance(_top_boards, list) and _top_boards:
                        ws_reg.cell(row=reg_row, column=2, value="Top Job Boards").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                        reg_row += 1
                        for i, h in enumerate(["Platform", "Specialty", "Pricing Model", "Monthly Traffic", "Notes"]):
                            cell = ws_reg.cell(row=reg_row, column=2 + i, value=h)
                            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                            cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                            cell.border = thin_border
                            cell.alignment = center_alignment
                        reg_row += 1
                        for bidx, board in enumerate(_top_boards):
                            _reg_rf = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if bidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                            if isinstance(board, dict):
                                _b_vals = [board.get("name", board.get("platform", "N/A")), board.get("specialty", board.get("focus", "N/A")), board.get("pricing_model", board.get("pricing", "N/A")), board.get("monthly_traffic", board.get("traffic", "N/A")), board.get("notes", board.get("description", ""))]
                            elif isinstance(board, str):
                                _b_vals = [board, "", "", "", ""]
                            else:
                                continue
                            for ci, val in enumerate(_b_vals):
                                _val_str = f"{val:,.0f}" if isinstance(val, (int, float)) and ci == 3 else str(val)[:150] if val else ""
                                cell = ws_reg.cell(row=reg_row, column=2 + ci, value=_val_str)
                                cell.font = Font(name="Calibri", size=10, bold=(ci == 0))
                                cell.fill = _reg_rf
                                cell.border = thin_border
                                cell.alignment = wrap_alignment
                            reg_row += 1
                        reg_row += 1

                    # Dominant Industries
                    _dom_ind = _ri.get("dominant_industries", [])
                    if isinstance(_dom_ind, list) and _dom_ind:
                        ws_reg.cell(row=reg_row, column=2, value="Dominant Industries").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                        reg_row += 1
                        _ind_names = [ind.get("name", ind.get("industry", str(ind))) if isinstance(ind, dict) else str(ind) for ind in _dom_ind]
                        ws_reg.merge_cells(f"B{reg_row}:H{reg_row}")
                        ws_reg.cell(row=reg_row, column=2, value=", ".join(str(d) for d in _ind_names[:15])).font = Font(name="Calibri", size=10, color="333333")
                        ws_reg.cell(row=reg_row, column=2).border = thin_border
                        reg_row += 2

                    # Talent Dynamics
                    _talent_dyn = _ri.get("talent_dynamics", {})
                    if isinstance(_talent_dyn, dict) and _talent_dyn:
                        ws_reg.cell(row=reg_row, column=2, value="Talent Dynamics").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                        reg_row += 1
                        for tkey, tval in _talent_dyn.items():
                            _td_f = PatternFill(start_color="E8F0FE", end_color="E8F0FE", fill_type="solid")
                            c1 = ws_reg.cell(row=reg_row, column=2, value=str(tkey).replace("_", " ").title())
                            c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                            c1.fill = _td_f
                            c1.border = thin_border
                            ws_reg.merge_cells(f"C{reg_row}:H{reg_row}")
                            c2 = ws_reg.cell(row=reg_row, column=3, value=str(tval)[:300])
                            c2.font = Font(name="Calibri", size=10)
                            c2.fill = _td_f
                            c2.border = thin_border
                            c2.alignment = wrap_alignment
                            reg_row += 1
                        reg_row += 1

                    # Hiring Regulations
                    _hire_regs = _ri.get("hiring_regulations", {})
                    if isinstance(_hire_regs, dict) and _hire_regs:
                        ws_reg.cell(row=reg_row, column=2, value="Hiring Regulations").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                        reg_row += 1
                        for rkey, rval in _hire_regs.items():
                            c1 = ws_reg.cell(row=reg_row, column=2, value=str(rkey).replace("_", " ").title())
                            c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                            c1.border = thin_border
                            ws_reg.merge_cells(f"C{reg_row}:H{reg_row}")
                            c2 = ws_reg.cell(row=reg_row, column=3, value=str(rval)[:300])
                            c2.font = Font(name="Calibri", size=10)
                            c2.border = thin_border
                            c2.alignment = wrap_alignment
                            reg_row += 1
                        reg_row += 1

                    # Cultural Norms
                    _cult_norms = _ri.get("cultural_norms", {})
                    if isinstance(_cult_norms, dict) and _cult_norms:
                        ws_reg.cell(row=reg_row, column=2, value="Cultural Norms").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                        reg_row += 1
                        for ckey, cval in _cult_norms.items():
                            c1 = ws_reg.cell(row=reg_row, column=2, value=str(ckey).replace("_", " ").title())
                            c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                            c1.border = thin_border
                            ws_reg.merge_cells(f"C{reg_row}:H{reg_row}")
                            c2 = ws_reg.cell(row=reg_row, column=3, value=str(cval)[:300])
                            c2.font = Font(name="Calibri", size=10)
                            c2.border = thin_border
                            c2.alignment = wrap_alignment
                            reg_row += 1
                        reg_row += 1

                    # CPA Benchmarks
                    _cpa_bench = _ri.get("cpa_benchmark", {})
                    if isinstance(_cpa_bench, dict) and _cpa_bench:
                        ws_reg.cell(row=reg_row, column=2, value="CPA Benchmarks").font = Font(name="Calibri", bold=True, size=11, color=NAVY)
                        reg_row += 1
                        for i, h in enumerate(["Metric", "Value"]):
                            cell = ws_reg.cell(row=reg_row, column=2 + i, value=h)
                            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                            cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                            cell.border = thin_border
                            cell.alignment = center_alignment
                        reg_row += 1
                        for cbkey, cbval in _cpa_bench.items():
                            _cb_f = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
                            c1 = ws_reg.cell(row=reg_row, column=2, value=str(cbkey).replace("_", " ").title())
                            c1.font = Font(name="Calibri", bold=True, size=10, color="2E7D32")
                            c1.fill = _cb_f
                            c1.border = thin_border
                            _cbval_str = f"${cbval:,.2f}" if isinstance(cbval, (int, float)) else str(cbval)
                            c2 = ws_reg.cell(row=reg_row, column=3, value=_cbval_str)
                            c2.font = Font(name="Calibri", size=10, color="2E7D32")
                            c2.fill = _cb_f
                            c2.border = thin_border
                            c2.alignment = center_alignment
                            reg_row += 1
                        reg_row += 1

                    reg_row += 1  # Extra spacing between locations

                # Footer
                ws_reg.merge_cells(f"B{reg_row}:H{reg_row}")
                ws_reg.cell(row=reg_row, column=2, value="Regional intelligence sourced from regional hiring knowledge base, local market research, and industry-specific talent data. CPA benchmarks are market-specific estimates.").font = Font(name="Calibri", italic=True, size=9, color="596780")

            except Exception as _reg_err:
                logger.warning("Regional Intelligence sheet generation failed: %s", _reg_err)

    # ── Sheet: Data Confidence ──
    _conf_scores = _synth.get("confidence_scores", {})
    _data_quality = _synth.get("data_quality", {})
    if isinstance(_conf_scores, dict) and _conf_scores:
        ws_conf = wb.create_sheet("Data Confidence")
        ws_conf.sheet_properties.tabColor = "F57C00"
        ws_conf.column_dimensions["A"].width = 3
        ws_conf.column_dimensions["B"].width = 30
        ws_conf.column_dimensions["C"].width = 14
        ws_conf.column_dimensions["D"].width = 14
        ws_conf.column_dimensions["E"].width = 30

        ws_conf.merge_cells("B2:E2")
        ws_conf["B2"].value = "Data Confidence Report"
        ws_conf["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_conf["B2"].border = accent_bottom_border

        conf_row = 4

        # Overall quality grade
        _overall = _conf_scores.get("overall", _conf_scores.get("overall_score", 0))
        if isinstance(_overall, (int, float)):
            if _overall >= 0.9:
                _grade = "A"
            elif _overall >= 0.8:
                _grade = "B"
            elif _overall >= 0.7:
                _grade = "C"
            elif _overall >= 0.5:
                _grade = "D"
            else:
                _grade = "F"
            _grade_color = "2E7D32" if _grade in ("A", "B") else "F57C00" if _grade == "C" else "C62828"
        else:
            _grade = str(_overall)
            _grade_color = "333333"

        ws_conf.merge_cells(f"B{conf_row}:C{conf_row}")
        _grade_cell = ws_conf.cell(row=conf_row, column=2, value=f"Overall Data Quality Grade: {_grade}")
        _grade_cell.font = Font(name="Calibri", bold=True, size=18, color="FFFFFF")
        _grade_cell.fill = PatternFill(start_color=_grade_color, end_color=_grade_color, fill_type="solid")
        _grade_cell.alignment = Alignment(horizontal="center", vertical="center")
        for cc in range(3, 4):
            ws_conf.cell(row=conf_row, column=cc).fill = PatternFill(start_color=_grade_color, end_color=_grade_color, fill_type="solid")
        ws_conf.merge_cells(f"D{conf_row}:E{conf_row}")
        _score_cell = ws_conf.cell(row=conf_row, column=4, value=f"Score: {_overall:.0%}" if isinstance(_overall, (int, float)) else f"Score: {_overall}")
        _score_cell.font = Font(name="Calibri", bold=True, size=14, color=_grade_color)
        _score_cell.alignment = Alignment(horizontal="center", vertical="center")
        ws_conf.row_dimensions[conf_row].height = 45
        conf_row += 2

        # Per-section confidence table
        conf_headers = ["Section", "Score", "Grade", "Sources Used"]
        for i, h in enumerate(conf_headers):
            cell = ws_conf.cell(row=conf_row, column=2 + i, value=h)
            cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
            cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
            cell.border = thin_border
            cell.alignment = center_alignment
        conf_row += 1

        _sections = _conf_scores.get("per_section", _conf_scores.get("sections", _conf_scores))
        if isinstance(_sections, dict):
            for cidx, (sec_name, sec_data) in enumerate(_sections.items()):
                if sec_name in ("overall", "overall_score", "sections", "per_section", "metadata", "data_quality_grade"):
                    continue
                _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if cidx % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")

                if isinstance(sec_data, dict):
                    _sec_score = sec_data.get("score", sec_data.get("confidence", 0))
                    _sec_sources = sec_data.get("sources", sec_data.get("apis_used", []))
                elif isinstance(sec_data, (int, float)):
                    _sec_score = sec_data
                    _sec_sources = []
                else:
                    continue

                if isinstance(_sec_score, (int, float)):
                    if _sec_score >= 0.9:
                        _sec_grade = "A"
                    elif _sec_score >= 0.8:
                        _sec_grade = "B"
                    elif _sec_score >= 0.7:
                        _sec_grade = "C"
                    elif _sec_score >= 0.5:
                        _sec_grade = "D"
                    else:
                        _sec_grade = "F"
                else:
                    _sec_grade = "N/A"

                if isinstance(_sec_sources, list):
                    _src_display = ", ".join(str(s) for s in _sec_sources[:5])
                    if len(_sec_sources) > 5:
                        _src_display += f" (+{len(_sec_sources) - 5} more)"
                else:
                    _src_display = str(_sec_sources)

                _sec_vals = [
                    sec_name.replace("_", " ").title(),
                    f"{_sec_score:.0%}" if isinstance(_sec_score, (int, float)) else str(_sec_score),
                    _sec_grade,
                    _src_display,
                ]
                for ci, val in enumerate(_sec_vals):
                    cell = ws_conf.cell(row=conf_row, column=2 + ci, value=val)
                    cell.font = Font(name="Calibri", size=10, bold=(ci == 0))
                    cell.fill = _row_fill
                    cell.border = thin_border
                    cell.alignment = center_alignment if ci in (1, 2) else wrap_alignment

                # Color-code grade
                grade_cell = ws_conf.cell(row=conf_row, column=4)
                if _sec_grade in ("A", "B"):
                    grade_cell.font = Font(name="Calibri", bold=True, size=10, color="2E7D32")
                elif _sec_grade == "C":
                    grade_cell.font = Font(name="Calibri", bold=True, size=10, color="F57C00")
                elif _sec_grade in ("D", "F"):
                    grade_cell.font = Font(name="Calibri", bold=True, size=10, color="C62828")
                conf_row += 1

        # API failure report
        conf_row += 1
        _failed_apis = []
        if isinstance(_data_quality, dict):
            _failed_apis = _data_quality.get("failed_apis", _data_quality.get("circuit_broken", []))
        if not _failed_apis:
            # Try enrichment summary
            _enr_sum = data.get("_enriched", {}).get("enrichment_summary", {})
            if isinstance(_enr_sum, dict):
                _failed_apis = _enr_sum.get("apis_failed", [])

        if _failed_apis and isinstance(_failed_apis, list):
            style_section_header(ws_conf, conf_row, 2, 5, "API Failures & Circuit Breakers")
            conf_row += 2
            for api_name in _failed_apis:
                if isinstance(api_name, dict):
                    api_display = api_name.get("name", api_name.get("api", str(api_name)))
                    api_reason = api_name.get("reason", api_name.get("error", ""))
                else:
                    api_display = str(api_name)
                    api_reason = ""
                ws_conf.cell(row=conf_row, column=2, value=f"  {api_display}").font = Font(name="Calibri", size=10, color="C62828")
                if api_reason:
                    ws_conf.cell(row=conf_row, column=4, value=str(api_reason)).font = Font(name="Calibri", italic=True, size=9, color="999999")
                    ws_conf.merge_cells(f"D{conf_row}:E{conf_row}")
                conf_row += 1
        else:
            ws_conf.merge_cells(f"B{conf_row}:E{conf_row}")
            ws_conf.cell(row=conf_row, column=2, value="All APIs responded successfully. No circuit breakers triggered.").font = Font(name="Calibri", size=10, color="2E7D32")

    # ── v3: Collar Strategy Worksheet ──
    # When roles span both blue/white collar types, generate a dedicated comparison sheet
    if _HAS_COLLAR_INTEL:
        try:
            _roles_list = data.get("roles", data.get("target_roles", []))
            _industry_key = data.get("industry", "general_entry_level")
            if _roles_list and isinstance(_roles_list, list) and len(_roles_list) >= 2:
                _collar_results = []
                for r in _roles_list[:15]:
                    r_str = r if isinstance(r, str) else (r.get("title", r.get("role", str(r))) if isinstance(r, dict) else str(r))
                    try:
                        cr = _collar_intel_mod.classify_collar(role=r_str, industry=_industry_key)
                        _collar_results.append((r_str, cr))
                    except Exception:
                        _collar_results.append((r_str, {"collar_type": "white_collar", "confidence": 0.3}))

                # Check if we have a collar mix (not all the same type)
                _collar_types_found = set(cr.get("collar_type", "") for _, cr in _collar_results)
                if len(_collar_types_found) >= 2 or "blue_collar" in _collar_types_found:
                    ws_collar = wb.create_sheet("Collar Strategy")
                    ws_collar.sheet_properties.tabColor = "0891B2"  # Teal
                    ws_collar.column_dimensions["A"].width = 3
                    ws_collar.column_dimensions["B"].width = 28
                    ws_collar.column_dimensions["C"].width = 18
                    ws_collar.column_dimensions["D"].width = 14
                    ws_collar.column_dimensions["E"].width = 25
                    ws_collar.column_dimensions["F"].width = 30

                    ws_collar.merge_cells("B2:F2")
                    ws_collar["B2"].value = "Blue Collar vs White Collar Strategy Analysis"
                    ws_collar["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
                    ws_collar["B2"].border = accent_bottom_border

                    ws_collar.merge_cells("B3:F3")
                    ws_collar["B3"].value = "Differentiated recruitment approach based on collar type classification"
                    ws_collar["B3"].font = Font(name="Calibri", italic=True, size=10, color="596780")

                    # Role classification table
                    _crow = 5
                    _collar_headers = ["Role", "Collar Type", "Confidence", "Method", "Channel Strategy"]
                    for ci, h in enumerate(_collar_headers):
                        cell = ws_collar.cell(row=_crow, column=2 + ci, value=h)
                        cell.font = Font(name="Calibri", bold=True, size=10, color="FFFFFF")
                        cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                        cell.border = thin_border
                        cell.alignment = center_alignment
                    _crow += 1

                    for ci, (r_str, cr) in enumerate(_collar_results):
                        _row_fill = PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid") if ci % 2 == 0 else PatternFill(start_color="F2F6FA", end_color="F2F6FA", fill_type="solid")
                        ct = cr.get("collar_type", "unknown")
                        conf = cr.get("confidence", 0)
                        method = cr.get("method", "")
                        strategy = cr.get("channel_strategy", "")
                        if isinstance(strategy, dict):
                            strategy = ", ".join(f"{k}: {v}" for k, v in list(strategy.items())[:3])
                        elif isinstance(strategy, list):
                            strategy = ", ".join(str(s) for s in strategy[:3])

                        vals = [r_str, ct.replace("_", " ").title(), f"{conf:.0%}", str(method), str(strategy)[:60]]
                        for vi, val in enumerate(vals):
                            cell = ws_collar.cell(row=_crow, column=2 + vi, value=val)
                            cell.font = Font(name="Calibri", size=10)
                            cell.fill = _row_fill
                            cell.border = thin_border
                            cell.alignment = wrap_alignment
                        # Color the collar type cell
                        ct_cell = ws_collar.cell(row=_crow, column=3)
                        if "blue" in ct:
                            ct_cell.font = Font(name="Calibri", bold=True, size=10, color="0A66C9")
                        elif "white" in ct:
                            ct_cell.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                        elif "grey" in ct:
                            ct_cell.font = Font(name="Calibri", bold=True, size=10, color="596780")
                        # Color confidence
                        conf_cell = ws_collar.cell(row=_crow, column=4)
                        if conf >= 0.8:
                            conf_cell.font = Font(name="Calibri", bold=True, size=10, color="2E7D32")
                        elif conf >= 0.5:
                            conf_cell.font = Font(name="Calibri", size=10, color="F57C00")
                        else:
                            conf_cell.font = Font(name="Calibri", italic=True, size=10, color="C62828")
                        _crow += 1

                    # Strategy comparison section
                    _crow += 2
                    ws_collar.merge_cells(f"B{_crow}:F{_crow}")
                    ws_collar.cell(row=_crow, column=2, value="Strategy Comparison by Collar Type").font = Font(name="Calibri", bold=True, size=14, color=NAVY)
                    ws_collar.cell(row=_crow, column=2).border = accent_bottom_border
                    _crow += 2

                    for ct_key in ["blue_collar", "white_collar"]:
                        if ct_key in _collar_intel_mod.COLLAR_STRATEGY:
                            strat = _collar_intel_mod.COLLAR_STRATEGY[ct_key]
                            ws_collar.merge_cells(f"B{_crow}:C{_crow}")
                            ws_collar.cell(row=_crow, column=2, value=ct_key.replace("_", " ").title()).font = Font(name="Calibri", bold=True, size=12, color="0A66C9" if "blue" in ct_key else NAVY)
                            _crow += 1
                            for sk in ["preferred_platforms", "messaging_tone", "avg_cpa_range", "avg_cpc_range", "time_to_fill_benchmark_days", "mobile_apply_pct"]:
                                sv = strat.get(sk, "")
                                if sv:
                                    label = sk.replace("_", " ").title()
                                    if isinstance(sv, list):
                                        sv = ", ".join(str(s) for s in sv[:5])
                                    ws_collar.cell(row=_crow, column=2, value=f"  {label}").font = Font(name="Calibri", size=10, color="596780")
                                    ws_collar.cell(row=_crow, column=4, value=str(sv)).font = Font(name="Calibri", bold=True, size=10)
                                    ws_collar.merge_cells(f"D{_crow}:F{_crow}")
                                    _crow += 1
                            _crow += 1

                    logger.info("Collar Strategy worksheet created with %d roles", len(_collar_results))
        except Exception as e:
            logger.warning("Collar Strategy worksheet creation failed: %s", e)

    # ── v3: CPC/CPA Trends Worksheet ──
    if _HAS_TREND_ENGINE:
        try:
            _industry_key = data.get("industry", "general_entry_level")
            ws_trends_v3 = wb.create_sheet("CPC CPA Trends")
            ws_trends_v3.sheet_properties.tabColor = "0A66C9"
            ws_trends_v3.column_dimensions["A"].width = 3
            ws_trends_v3.column_dimensions["B"].width = 22
            ws_trends_v3.column_dimensions["C"].width = 14
            ws_trends_v3.column_dimensions["D"].width = 14
            ws_trends_v3.column_dimensions["E"].width = 14
            ws_trends_v3.column_dimensions["F"].width = 14
            ws_trends_v3.column_dimensions["G"].width = 14
            ws_trends_v3.column_dimensions["H"].width = 14

            ws_trends_v3.merge_cells("B2:H2")
            ws_trends_v3["B2"].value = f"CPC/CPA Trend Analysis - {data.get('industry_label', _industry_key.replace('_', ' ').title())}"
            ws_trends_v3["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
            ws_trends_v3["B2"].border = accent_bottom_border

            ws_trends_v3.merge_cells("B3:H3")
            ws_trends_v3["B3"].value = "4-year historical trend with seasonal and collar-type adjustments"
            ws_trends_v3["B3"].font = Font(name="Calibri", italic=True, size=10, color="596780")

            _trow = 5
            _platforms = ["google", "meta_fb", "indeed", "linkedin", "programmatic"]
            _plat_labels = {"google": "Google Ads", "meta_fb": "Meta (Facebook)", "indeed": "Indeed", "linkedin": "LinkedIn", "programmatic": "Programmatic DSP"}

            for plat in _platforms:
                plat_label = _plat_labels.get(plat, plat.replace("_", " ").title())
                ws_trends_v3.merge_cells(f"B{_trow}:H{_trow}")
                ws_trends_v3.cell(row=_trow, column=2, value=plat_label).font = Font(name="Calibri", bold=True, size=12, color=NAVY)
                ws_trends_v3.cell(row=_trow, column=2).border = accent_bottom_border
                _trow += 1

                # Header row
                _trend_headers = ["Metric", "2022", "2023", "2024", "2025", "Trend", "YoY %"]
                for ci, h in enumerate(_trend_headers):
                    cell = ws_trends_v3.cell(row=_trow, column=2 + ci, value=h)
                    cell.font = Font(name="Calibri", bold=True, size=9, color="FFFFFF")
                    cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                    cell.border = thin_border
                    cell.alignment = center_alignment
                _trow += 1

                for metric in ["cpc", "cpa"]:
                    try:
                        trend = _trend_engine_mod.get_trend(
                            platform=plat, industry=_industry_key,
                            metric=metric, years_back=4,
                        )
                        if not trend or not isinstance(trend, dict):
                            continue

                        history = trend.get("history", [])
                        # Build year->value lookup from history list
                        _year_vals = {h["year"]: h.get("value", 0) for h in history if isinstance(h, dict) and "year" in h}
                        direction = trend.get("trend_direction", "stable")
                        yoy = trend.get("avg_yoy_change_pct", 0)

                        ws_trends_v3.cell(row=_trow, column=2, value=metric.upper()).font = Font(name="Calibri", bold=True, size=10)
                        ws_trends_v3.cell(row=_trow, column=2).border = thin_border

                        for yi, year in enumerate([2022, 2023, 2024, 2025]):
                            val = _year_vals.get(year, 0)
                            if isinstance(val, (int, float)) and val > 0:
                                cell = ws_trends_v3.cell(row=_trow, column=3 + yi, value=f"${val:.2f}")
                            else:
                                cell = ws_trends_v3.cell(row=_trow, column=3 + yi, value="-")
                            cell.font = Font(name="Calibri", size=10)
                            cell.border = thin_border
                            cell.alignment = center_alignment

                        # Trend direction
                        trend_cell = ws_trends_v3.cell(row=_trow, column=7, value=direction.title())
                        trend_cell.border = thin_border
                        trend_cell.alignment = center_alignment
                        if direction == "rising":
                            trend_cell.font = Font(name="Calibri", bold=True, size=10, color="C62828")
                        elif direction == "falling":
                            trend_cell.font = Font(name="Calibri", bold=True, size=10, color="2E7D32")
                        else:
                            trend_cell.font = Font(name="Calibri", size=10, color="596780")

                        # YoY %
                        yoy_cell = ws_trends_v3.cell(row=_trow, column=8,
                                                      value=f"{'+' if yoy > 0 else ''}{yoy:.1f}%")
                        yoy_cell.border = thin_border
                        yoy_cell.alignment = center_alignment
                        if yoy > 5:
                            yoy_cell.font = Font(name="Calibri", bold=True, size=10, color="C62828")
                        elif yoy < -5:
                            yoy_cell.font = Font(name="Calibri", bold=True, size=10, color="2E7D32")
                        else:
                            yoy_cell.font = Font(name="Calibri", size=10)

                        _trow += 1
                    except Exception:
                        pass
                _trow += 1

            # Seasonal factors section
            _trow += 1
            ws_trends_v3.merge_cells(f"B{_trow}:H{_trow}")
            ws_trends_v3.cell(row=_trow, column=2, value="Monthly Seasonal CPC Multipliers").font = Font(name="Calibri", bold=True, size=14, color=NAVY)
            ws_trends_v3.cell(row=_trow, column=2).border = accent_bottom_border
            _trow += 2

            _month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            # Two half-year blocks: Jan-Jun then Jul-Dec
            for _half, _m_start in [(0, 0), (1, 6)]:
                if _half == 1:
                    _trow += 1  # Spacing between halves
                # Header
                ws_trends_v3.cell(row=_trow, column=2, value="Collar Type").font = Font(name="Calibri", bold=True, size=9, color="FFFFFF")
                ws_trends_v3.cell(row=_trow, column=2).fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                ws_trends_v3.cell(row=_trow, column=2).border = thin_border
                for mi, mn in enumerate(_month_names[_m_start:_m_start + 6]):
                    cell = ws_trends_v3.cell(row=_trow, column=3 + mi, value=mn)
                    cell.font = Font(name="Calibri", bold=True, size=9, color="FFFFFF")
                    cell.fill = PatternFill(start_color=NAVY, end_color=NAVY, fill_type="solid")
                    cell.border = thin_border
                    cell.alignment = center_alignment
                _trow += 1

                for collar in ["blue_collar", "white_collar"]:
                    ws_trends_v3.cell(row=_trow, column=2, value=collar.replace("_", " ").title()).font = Font(name="Calibri", bold=True, size=10)
                    ws_trends_v3.cell(row=_trow, column=2).border = thin_border
                    for mi in range(6):
                        try:
                            sa = _trend_engine_mod.get_seasonal_adjustment(collar, _m_start + mi + 1)
                            mult = sa.get("multiplier", 1.0) if isinstance(sa, dict) else sa
                            cell = ws_trends_v3.cell(row=_trow, column=3 + mi, value=f"{mult:.2f}x")
                            cell.border = thin_border
                            cell.alignment = center_alignment
                            if mult > 1.1:
                                cell.font = Font(name="Calibri", bold=True, size=10, color="C62828")
                                cell.fill = PatternFill(start_color="FFF3E0", end_color="FFF3E0", fill_type="solid")
                            elif mult < 0.9:
                                cell.font = Font(name="Calibri", bold=True, size=10, color="2E7D32")
                                cell.fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
                            else:
                                cell.font = Font(name="Calibri", size=10)
                        except Exception:
                            pass
                    _trow += 1

            logger.info("CPC/CPA Trends worksheet created")
        except Exception as e:
            logger.warning("CPC/CPA Trends worksheet creation failed: %s", e)

    # ── Universal data source attribution footer ──
    # Add a "Data Sources" footer to each main content sheet so every printed/shared
    # page carries provenance info.  This is a critical trust signal for Fortune 500
    # clients and procurement teams who review deliverables.
    _attribution_sheets = [
        "Market Trends", "Labour Market Intelligence", "Channel Strategy",
        "Traditional Channels", "Non-Traditional Channels",
    ]
    _enrichment_summary = data.get("_enriched", {}).get("enrichment_summary", {}) if isinstance(data.get("_enriched"), dict) else {}
    _apis_used_list = _enrichment_summary.get("apis_succeeded", []) if _enrichment_summary else []
    _apis_used_str = ", ".join(_apis_used_list[:8]) if _apis_used_list else ""

    for _sheet_name in _attribution_sheets:
        if _sheet_name not in wb.sheetnames:
            continue
        _ws = wb[_sheet_name]
        _last_row = _ws.max_row + 2
        _ws.merge_cells(f"B{_last_row}:F{_last_row}")
        _ws.cell(row=_last_row, column=2,
                 value="Data sourced from Joveo internal knowledge base, Appcast Recruitment Marketing Benchmark, SHRM Benchmarking, and BLS/JOLTS labor market data."
                ).font = Font(name="Calibri", italic=True, size=9, color="596780")
        if _apis_used_str:
            _last_row += 1
            _ws.merge_cells(f"B{_last_row}:F{_last_row}")
            _ws.cell(row=_last_row, column=2,
                     value=f"Live API data enrichment: {_apis_used_str}"
                    ).font = Font(name="Calibri", italic=True, size=9, color="0A66C9")

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()


ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "")
if not ADMIN_API_KEY:
    logger.warning("ADMIN_API_KEY not set - admin endpoints unprotected (dev mode)")

# ═══════════════════════════════════════════════════════════════════════════════
# OPENAPI 3.0 SPECIFICATION
# ═══════════════════════════════════════════════════════════════════════════════
_OPENAPI_SPEC = {
    "openapi": "3.0.3",
    "info": {
        "title": "AI Media Planner API",
        "description": "Recruitment advertising media plan generator with AI-powered research, "
                       "budget optimization, and strategy deck creation.",
        "version": "3.4.0",
        "contact": {"name": "Joveo Engineering", "url": "https://media-plan-generator.onrender.com"},
        "license": {"name": "Proprietary"},
    },
    "servers": [
        {"url": "https://media-plan-generator.onrender.com", "description": "Production"},
        {"url": "http://localhost:5001", "description": "Local development"},
    ],
    "paths": {
        "/api/generate": {
            "post": {
                "summary": "Generate media plan",
                "description": "Generate a recruitment advertising media plan bundle (Excel + PPT in ZIP). "
                               "Supports synchronous (default) and asynchronous (X-Async: true) modes.",
                "operationId": "generateMediaPlan",
                "tags": ["Generation"],
                "parameters": [
                    {
                        "name": "X-Async",
                        "in": "header",
                        "required": False,
                        "schema": {"type": "string", "enum": ["true"]},
                        "description": "Set to 'true' to run generation asynchronously. "
                                       "Returns a job_id for polling via /api/jobs/{job_id}.",
                    }
                ],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["client_name"],
                                "properties": {
                                    "client_name": {"type": "string", "description": "Company or client name", "example": "Acme Corp"},
                                    "industry": {"type": "string", "description": "Industry vertical (e.g. healthcare, technology)", "example": "healthcare"},
                                    "budget": {"type": "string", "description": "Campaign budget (e.g. '$50,000', '100000')", "example": "$50,000"},
                                    "roles": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "Target roles to hire",
                                        "example": ["Registered Nurse", "Medical Assistant"],
                                    },
                                    "target_roles": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "Alias for roles",
                                    },
                                    "locations": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "Target hiring locations",
                                        "example": ["Dallas, TX", "Houston, TX"],
                                    },
                                    "competitors": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                        "description": "Competitor company names for benchmarking",
                                    },
                                    "campaign_duration": {"type": "string", "description": "Campaign timeline", "example": "3-6 months"},
                                    "hire_volume": {"type": "string", "description": "Number of hires expected"},
                                    "notes": {"type": "string", "description": "Additional context or requirements"},
                                },
                            }
                        }
                    },
                },
                "responses": {
                    "200": {
                        "description": "Synchronous: Binary ZIP containing Excel + PPT. Async: JSON with job_id.",
                        "content": {
                            "application/zip": {
                                "schema": {"type": "string", "format": "binary"},
                            },
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "job_id": {"type": "string"},
                                        "status": {"type": "string", "enum": ["processing"]},
                                        "poll_url": {"type": "string"},
                                    },
                                },
                            },
                        },
                    },
                    "400": {"description": "Invalid input (missing client_name, bad JSON, etc.)"},
                    "413": {"description": "Request body too large (>10MB)"},
                    "429": {"description": "Rate limit exceeded"},
                },
            }
        },
        "/api/chat": {
            "post": {
                "summary": "Nova AI chat",
                "description": "Send a message to the Nova recruitment intelligence chatbot.",
                "operationId": "novaChat",
                "tags": ["Chat"],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["message"],
                                "properties": {
                                    "message": {"type": "string", "description": "User message", "example": "What are the best job boards for healthcare?"},
                                    "conversation_history": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "role": {"type": "string", "enum": ["user", "assistant"]},
                                                "content": {"type": "string"},
                                            },
                                        },
                                        "description": "Previous conversation turns for context",
                                    },
                                },
                            }
                        }
                    },
                },
                "responses": {
                    "200": {
                        "description": "Chat response",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "response": {"type": "string"},
                                        "sources": {"type": "array", "items": {"type": "string"}},
                                        "confidence": {"type": "number", "format": "float"},
                                        "tools_used": {"type": "array", "items": {"type": "string"}},
                                    },
                                },
                            }
                        },
                    },
                    "429": {"description": "Rate limit exceeded"},
                },
            }
        },
        "/api/health": {
            "get": {
                "summary": "Liveness probe",
                "description": "Lightweight health check for load balancers and uptime monitors.",
                "operationId": "healthLiveness",
                "tags": ["Health"],
                "responses": {
                    "200": {
                        "description": "Service is alive",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "status": {"type": "string", "example": "ok"},
                                        "version": {"type": "string", "example": "3.4.0"},
                                        "timestamp": {"type": "string", "format": "date-time"},
                                    },
                                },
                            }
                        },
                    }
                },
            }
        },
        "/api/health/ready": {
            "get": {
                "summary": "Readiness probe",
                "description": "Deep readiness check (KB loaded, disk, memory, modules).",
                "operationId": "healthReadiness",
                "tags": ["Health"],
                "responses": {
                    "200": {"description": "Service is ready"},
                    "503": {"description": "Service is not ready (degraded)"},
                },
            }
        },
        "/api/health/data-matrix": {
            "get": {
                "summary": "Data matrix status",
                "description": "Background data matrix health monitor status.",
                "operationId": "healthDataMatrix",
                "tags": ["Health"],
                "security": [{"BearerAuth": []}],
                "responses": {
                    "200": {"description": "Data matrix status"},
                    "401": {"description": "Unauthorized"},
                    "503": {"description": "Data matrix degraded or unavailable"},
                },
            }
        },
        "/api/health/auto-qc": {
            "get": {
                "summary": "Auto QC status",
                "description": "Autonomous QC engine test results and self-upgrade status.",
                "operationId": "healthAutoQC",
                "tags": ["Health"],
                "security": [{"BearerAuth": []}],
                "responses": {
                    "200": {"description": "QC engine status"},
                    "401": {"description": "Unauthorized"},
                    "503": {"description": "QC engine degraded or unavailable"},
                },
            }
        },
        "/api/health/slos": {
            "get": {
                "summary": "SLO compliance",
                "description": "Service Level Objective compliance check across all monitored metrics.",
                "operationId": "healthSLOs",
                "tags": ["Health"],
                "security": [{"BearerAuth": []}],
                "responses": {
                    "200": {"description": "SLO compliance results"},
                    "401": {"description": "Unauthorized"},
                    "503": {"description": "Monitoring module unavailable"},
                },
            }
        },
        "/api/health/eval": {
            "get": {
                "summary": "Eval scores",
                "description": "Run the full evaluation framework and return aggregate test scores.",
                "operationId": "healthEval",
                "tags": ["Health"],
                "security": [{"BearerAuth": []}],
                "responses": {
                    "200": {"description": "Evaluation results with per-category scores"},
                    "401": {"description": "Unauthorized"},
                    "503": {"description": "Eval framework unavailable"},
                },
            }
        },
        "/api/jobs/{job_id}": {
            "get": {
                "summary": "Poll async job",
                "description": "Check status of an asynchronous generation job. "
                               "When completed, returns the binary file content.",
                "operationId": "pollJob",
                "tags": ["Generation"],
                "parameters": [
                    {
                        "name": "job_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"},
                        "description": "Job ID returned by async /api/generate",
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Job status or completed binary content",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "job_id": {"type": "string"},
                                        "status": {"type": "string", "enum": ["processing", "completed", "failed"]},
                                        "progress_pct": {"type": "integer"},
                                        "created": {"type": "string", "format": "date-time"},
                                        "elapsed_seconds": {"type": "number"},
                                    },
                                },
                            },
                            "application/zip": {
                                "schema": {"type": "string", "format": "binary"},
                            },
                        },
                    },
                    "404": {"description": "Job not found or expired"},
                },
            }
        },
        "/api/admin/keys": {
            "post": {
                "summary": "Manage API keys",
                "description": "Create, list, or revoke tiered API keys.",
                "operationId": "adminKeys",
                "tags": ["Admin"],
                "security": [{"BearerAuth": []}],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["action"],
                                "properties": {
                                    "action": {"type": "string", "enum": ["create", "list", "revoke"]},
                                    "tier": {"type": "string", "enum": ["free", "pro", "enterprise"]},
                                    "label": {"type": "string", "description": "Human-readable key label"},
                                    "key": {"type": "string", "description": "Key to revoke (for revoke action)"},
                                },
                            }
                        }
                    },
                },
                "responses": {
                    "200": {"description": "Key management result"},
                    "401": {"description": "Unauthorized"},
                },
            }
        },
        "/api/admin/usage": {
            "get": {
                "summary": "Per-key usage dashboard",
                "description": "View rate limit usage statistics for all API keys.",
                "operationId": "adminUsage",
                "tags": ["Admin"],
                "security": [{"BearerAuth": []}],
                "responses": {
                    "200": {"description": "Usage statistics per key"},
                    "401": {"description": "Unauthorized"},
                },
            }
        },
    },
    "components": {
        "securitySchemes": {
            "BearerAuth": {
                "type": "http",
                "scheme": "bearer",
                "description": "Admin API key passed as Bearer token. "
                               "Set ADMIN_API_KEY env var on the server.",
            }
        }
    },
    "tags": [
        {"name": "Generation", "description": "Media plan generation (sync and async)"},
        {"name": "Chat", "description": "Nova AI recruitment intelligence chatbot"},
        {"name": "Health", "description": "Health checks, readiness probes, monitoring"},
        {"name": "Admin", "description": "Administrative endpoints (require Bearer token)"},
    ],
}

# ═══════════════════════════════════════════════════════════════════════════════
# SWAGGER UI HTML (self-contained, loads from CDN)
# ═══════════════════════════════════════════════════════════════════════════════
_SWAGGER_UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AI Media Planner - API Docs</title>
<link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
<style>
body { margin: 0; padding: 0; }
#swagger-ui .topbar { display: none; }
</style>
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
SwaggerUIBundle({
  url: "/api/docs/openapi.json",
  dom_id: "#swagger-ui",
  deepLinking: true,
  presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
  layout: "BaseLayout",
});
</script>
</body>
</html>"""

# ═══════════════════════════════════════════════════════════════════════════════
# ASYNC GENERATION JOB STORE
# ═══════════════════════════════════════════════════════════════════════════════
_generation_jobs: dict = {}
_generation_jobs_lock = threading.Lock()
_GENERATION_JOB_EXPIRY_SECONDS = 30 * 60  # 30 minutes

def _cleanup_generation_jobs():
    """Background thread to clean up expired async generation jobs."""
    while True:
        try:
            time.sleep(300)  # Every 5 minutes
            now = time.time()
            with _generation_jobs_lock:
                expired = [
                    jid for jid, jdata in _generation_jobs.items()
                    if now - jdata.get("created", 0) > _GENERATION_JOB_EXPIRY_SECONDS
                    or (jdata.get("status") in ("completed", "failed") and now - jdata.get("created", 0) > 600)
                ]
                for jid in expired:
                    _generation_jobs.pop(jid, None)
            if expired:
                logger.info("Cleaned up %d expired generation jobs", len(expired))
        except Exception as e:
            logger.warning("Generation job cleanup error: %s", e)

_job_cleanup_thread = threading.Thread(target=_cleanup_generation_jobs, daemon=True, name="job-cleanup")
_job_cleanup_thread.start()

# ═══════════════════════════════════════════════════════════════════════════════
# TIERED API KEY RATE LIMITING
# ═══════════════════════════════════════════════════════════════════════════════
API_KEY_TIERS = {
    "free":       {"rpm": 5,   "rpd": 50},
    "pro":        {"rpm": 30,  "rpd": 1000},
    "enterprise": {"rpm": 100, "rpd": 10000},
}

# In-memory store: key -> {tier, label, created, revoked, usage_minute: [...], usage_day: [...]}
_api_keys_store: dict = {}
_api_keys_lock = threading.Lock()

# ═══════════════════════════════════════════════════════════════════════════════
# MONITORING & OBSERVABILITY
# ═══════════════════════════════════════════════════════════════════════════════
try:
    from monitoring import (
        configure_logging, get_metrics, health_check_liveness,
        health_check_readiness, GracefulShutdown, get_system_info,
    )
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    _metrics = get_metrics()
    _shutdown = GracefulShutdown(timeout=30.0)
    logger.info("Monitoring module loaded successfully")
except ImportError as _mon_err:
    logger.warning("monitoring module not available: %s", _mon_err)
    _metrics = None
    _shutdown = None

    def health_check_liveness():
        return {"status": "ok", "version": "3.4.0", "timestamp": datetime.datetime.now().isoformat()}

    def health_check_readiness():
        return {"status": "healthy", "version": "3.4.0"}

# Data matrix health monitor (background checks every 12h)
try:
    from data_matrix_monitor import get_data_matrix_monitor
    _data_matrix = get_data_matrix_monitor()
    _data_matrix.start_background()
    logger.info("Data matrix monitor started (checks every 12h)")
except ImportError as _dm_err:
    logger.warning("data_matrix_monitor not available: %s", _dm_err)
    _data_matrix = None

# Autonomous QC engine (twice-daily tests + weekly self-upgrade)
try:
    from auto_qc import get_auto_qc
    _auto_qc = get_auto_qc()
    _auto_qc.start_background()
    logger.info("AutoQC engine started (tests every 12h, self-upgrade weekly)")
except ImportError as _aqc_err:
    logger.warning("auto_qc not available: %s", _aqc_err)
    _auto_qc = None

# Email alert notifications (Resend API)
try:
    import email_alerts as _email_alerts
    if _email_alerts._is_enabled():
        logger.info("Email alerts enabled (Resend API)")
    else:
        logger.debug("Email alerts not configured (RESEND_API_KEY not set)")
except ImportError:
    _email_alerts = None
    logger.debug("email_alerts module not available")

# Grafana Cloud Loki logging (ships WARNING+ logs)
try:
    from grafana_logger import setup_grafana_logging
    if setup_grafana_logging(logging.getLogger()):
        logger.info("Grafana Loki logging enabled")
    else:
        logger.debug("Grafana Loki logging not configured (env vars missing)")
except ImportError:
    logger.debug("grafana_logger module not available")

# Supabase persistent cache (L3 cache layer)
try:
    from supabase_cache import start_cleanup_thread as _supa_start_cleanup
    _supa_start_cleanup(interval_hours=6)
    logger.info("Supabase cache cleanup thread started (every 6h)")
except ImportError:
    logger.debug("supabase_cache module not available")
except Exception as _supa_err:
    logger.debug("Supabase cache not configured: %s", _supa_err)

# Simple in-memory rate limiter
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
# Bounded thread pool for async Slack event processing (max 4 concurrent)
_slack_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="slack-event")
_rate_limit_store = defaultdict(list)
_rate_limit_lock = threading.Lock()
_RATE_LIMIT_WINDOW = 60   # seconds
_RATE_LIMIT_MAX = 10       # requests per window per IP

# Global rate limit for /api/chat to prevent distributed API cost abuse
_GLOBAL_CHAT_RATE_LIMIT_MAX = int(os.environ.get("GLOBAL_CHAT_RATE_LIMIT", "120"))  # per minute across all IPs
_global_chat_timestamps: list = []
_global_chat_lock = threading.Lock()

_ALLOWED_ORIGINS = {
    "http://localhost:10000", "http://localhost:5001", "http://127.0.0.1:10000",
    "http://127.0.0.1:5001", "https://media-plan-generator.onrender.com",
}

class MediaPlanHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        logger.info(format, *args)

    def end_headers(self):
        """Add security + API platform headers to all responses."""
        # ── API Platform headers ──
        self.send_header("X-API-Version", "v1")
        # Request ID: generated per-request in do_GET/do_POST, or fallback
        _rid = getattr(self, "_request_id", None)
        if _rid:
            self.send_header("X-Request-ID", _rid)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
        # Prevent browsers from running inline scripts from injected content
        self.send_header("X-XSS-Protection", "1; mode=block")
        # Strict-Transport-Security: tell browsers to only use HTTPS
        self.send_header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
        # Permissions-Policy: disable sensitive browser APIs not needed by this app
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        # Content-Security-Policy: restrict resource origins (unsafe-inline needed for
        # inline styles/scripts used throughout templates)
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' https://unpkg.com; "
            "style-src 'self' 'unsafe-inline' https://unpkg.com; "
            "img-src 'self' https: data:; "
            "font-src 'self' https: data:; "
            "connect-src 'self' https:; "
            "object-src 'none'; "
            "base-uri 'self'; "
            "form-action 'self';"
        )
        super().end_headers()

    def _get_cors_origin(self):
        """Return the request Origin if it is in the allowlist, else empty string.
        SECURITY: CORS_ALLOW_ALL removed to prevent accidental exposure in production."""
        origin = self.headers.get("Origin", "")
        if origin in _ALLOWED_ORIGINS:
            return origin
        return ""  # No CORS header = browser blocks

    def _check_admin_auth(self):
        """Check for admin API key via Authorization header only.
        Uses hmac.compare_digest for timing-safe comparison to prevent
        timing side-channel attacks on the API key.
        SECURITY: Fails closed -- rejects if ADMIN_API_KEY is not configured."""
        if not ADMIN_API_KEY:
            return False  # No key configured = reject (fail closed)
        auth = self.headers.get("Authorization", "")
        key = None
        if auth.startswith("Bearer "):
            key = auth[7:]
        if not key:
            return False
        import hmac
        return hmac.compare_digest(key, ADMIN_API_KEY)

    def _check_rate_limit(self):
        """Tiered rate limiting: API key tier limits take precedence over per-IP limits.

        If the request carries a valid ``Authorization: Bearer <key>`` whose key
        exists in ``_api_keys_store`` and is not revoked, the per-minute and
        per-day limits from the key's tier are used instead of the default
        per-IP limits.  Otherwise falls back to original IP-based limiting.

        Thread-safe. Cleans up stale entries to prevent unbounded memory growth.
        """
        now = time.time()

        # ── Check for API-key-based tier limits ──
        auth_header = self.headers.get("Authorization", "")
        api_key = None
        if auth_header.startswith("Bearer "):
            api_key = auth_header[7:]

        if api_key:
            with _api_keys_lock:
                key_entry = _api_keys_store.get(api_key)
                if key_entry and not key_entry.get("revoked"):
                    tier_name = key_entry.get("tier", "free")
                    tier_limits = API_KEY_TIERS.get(tier_name, API_KEY_TIERS["free"])

                    # Per-minute check
                    key_entry["usage_minute"] = [
                        t for t in key_entry.get("usage_minute", [])
                        if now - t < 60
                    ]
                    if len(key_entry["usage_minute"]) >= tier_limits["rpm"]:
                        return False

                    # Per-day check
                    key_entry["usage_day"] = [
                        t for t in key_entry.get("usage_day", [])
                        if now - t < 86400
                    ]
                    if len(key_entry["usage_day"]) >= tier_limits["rpd"]:
                        return False

                    key_entry["usage_minute"].append(now)
                    key_entry["usage_day"].append(now)
                    return True
                # If key is invalid/revoked, fall through to IP-based limiting

        # ── Fallback: per-IP rate limiting (original behavior) ──
        client_ip = self.client_address[0]
        with _rate_limit_lock:
            # Purge expired timestamps for this IP
            _rate_limit_store[client_ip] = [
                t for t in _rate_limit_store[client_ip]
                if now - t < _RATE_LIMIT_WINDOW
            ]
            if len(_rate_limit_store[client_ip]) >= _RATE_LIMIT_MAX:
                return False
            _rate_limit_store[client_ip].append(now)
            # Periodic cleanup: evict IPs with no recent requests.
            # Run cleanup when store exceeds 500 IPs (not 1000) to stay lean.
            if len(_rate_limit_store) > 500:
                stale = [
                    ip for ip, ts in list(_rate_limit_store.items())
                    if not ts or now - max(ts) > _RATE_LIMIT_WINDOW * 2
                ]
                for ip in stale:
                    _rate_limit_store.pop(ip, None)
        return True

    def _check_global_chat_rate_limit(self):
        """Global rate limit across all IPs for /api/chat to prevent distributed cost abuse.

        Thread-safe. Returns True if request is allowed, False if limit exceeded.
        """
        now = time.time()
        with _global_chat_lock:
            # Purge expired timestamps
            _global_chat_timestamps[:] = [
                t for t in _global_chat_timestamps
                if now - t < _RATE_LIMIT_WINDOW
            ]
            if len(_global_chat_timestamps) >= _GLOBAL_CHAT_RATE_LIMIT_MAX:
                return False
            _global_chat_timestamps.append(now)
        return True

    def do_GET(self):
        # ── Request ID generation (Feature 6) ──
        try:
            from monitoring import generate_request_id as _gen_rid
            self._request_id = _gen_rid()
        except Exception:
            self._request_id = uuid.uuid4().hex[:12]
        _req_start = time.time()
        if _metrics:
            _metrics.enter_request()
        try:
            self._handle_GET()
        finally:
            if _metrics:
                _metrics.exit_request()
                _latency = (time.time() - _req_start) * 1000
                parsed = urlparse(self.path)
                _metrics.record_request(parsed.path, "GET", 200, _latency)

    def _handle_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        # ── API Versioning: strip /v1 prefix (Feature 4) ──
        if path.startswith("/v1/"):
            path = path[3:]
        if path == "/" or path == "":
            self._serve_file(os.path.join(TEMPLATES_DIR, "index.html"), "text/html")
        elif path in ("/api/health", "/health"):
            # Lightweight liveness probe (fast, for Render.com health checks)
            self._send_json(health_check_liveness())
        elif path in ("/api/health/ready", "/ready"):
            # Deep readiness probe (checks KB, disk, memory, modules)
            result = health_check_readiness()
            status_code = 200 if result.get("status") == "healthy" else 503
            body = json.dumps(result).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/health/data-matrix":
            # Data matrix health monitor (admin-protected)
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            if _data_matrix:
                dm_result = _data_matrix.get_status()
                dm_code = 200 if dm_result.get("status") != "degraded" else 503
                dm_body = json.dumps(dm_result, indent=2).encode("utf-8")
                self.send_response(dm_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(dm_body)))
                self.end_headers()
                self.wfile.write(dm_body)
            else:
                dm_err_body = json.dumps({"error": "Data matrix monitor not available"}).encode("utf-8")
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(dm_err_body)))
                self.end_headers()
                self.wfile.write(dm_err_body)
        elif path == "/api/health/auto-qc":
            # Autonomous QC engine status (admin-protected)
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            if _auto_qc:
                qc_result = _auto_qc.get_status()
                qc_code = 200 if qc_result.get("status") != "degraded" else 503
                qc_body = json.dumps(qc_result, indent=2).encode("utf-8")
                self.send_response(qc_code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(qc_body)))
                self.end_headers()
                self.wfile.write(qc_body)
            else:
                qc_err_body = json.dumps({"error": "AutoQC engine not available"}).encode("utf-8")
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(qc_err_body)))
                self.end_headers()
                self.wfile.write(qc_err_body)
        elif path == "/api/health/orchestrator":
            # Orchestrator cache stats + fallback telemetry (admin-protected)
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            try:
                import data_orchestrator as _do
                orch_data = {
                    "cache_stats": _do.get_cache_stats(),
                    "fallback_telemetry": _do.get_fallback_telemetry(),
                    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                }
                self._send_json(orch_data)
            except Exception as _oe:
                self._send_json({"error": f"Orchestrator unavailable: {_oe}"})
        elif path == "/api/metrics":
            # Metrics endpoint (admin-protected)
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            metrics_data = _metrics.get_metrics() if _metrics else {"error": "Monitoring not available"}
            self._send_json(metrics_data)
        elif path == "/api/nova/metrics":
            # Nova chatbot metrics (admin-protected)
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            try:
                from nova import get_nova_metrics
                self._send_json(get_nova_metrics())
            except Exception as e:
                logger.error("Nova metrics error: %s", e, exc_info=True)
                self._send_json({"error": "Failed to retrieve Nova metrics"})
        elif path == "/api/slack/status":
            # ── Slack Bot Diagnostic Endpoint (admin-protected) ──
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            _slack_bot_token = os.environ.get("SLACK_BOT_TOKEN", "")
            _slack_signing_secret = os.environ.get("SLACK_SIGNING_SECRET", "")
            diag = {
                "slack_bot_token": "SET" if _slack_bot_token else "NOT SET",
                "slack_signing_secret": "SET" if _slack_signing_secret else "NOT SET",
                "anthropic_api_key": "SET" if os.environ.get("ANTHROPIC_API_KEY", "") else "NOT SET",
                "event_endpoint": "/api/slack/events",
                "event_endpoint_url": os.environ.get("BASE_URL", "https://media-plan-generator.onrender.com") + "/api/slack/events",
                "admin_endpoint": "/api/admin/nova",
            }
            # Try to instantiate bot and check auth
            try:
                from nova_slack import get_nova_bot
                bot = get_nova_bot()
                diag["bot_user_id"] = bot.bot_user_id or "NOT AUTHENTICATED (auth.test failed)"
                diag["nova_engine"] = "LOADED" if bot._iq_engine else "NOT LOADED"
                diag["learned_answers_count"] = len(bot.learned_answers.get("answers", []))
                diag["unanswered_count"] = len([q for q in bot.unanswered.get("questions", []) if q.get("status") == "pending"])
            except Exception as e:
                diag["bot_status"] = f"ERROR: {e}"
            # Setup checklist
            checks = []
            if not _slack_bot_token:
                checks.append("MISSING: Set SLACK_BOT_TOKEN env var (xoxb-...) in Render dashboard")
            elif not _slack_bot_token.startswith("xoxb-"):
                checks.append("WRONG FORMAT: SLACK_BOT_TOKEN should start with 'xoxb-'")
            if not _slack_signing_secret:
                checks.append("MISSING: Set SLACK_SIGNING_SECRET env var in Render dashboard")
            if not os.environ.get("ANTHROPIC_API_KEY", ""):
                checks.append("MISSING: Set ANTHROPIC_API_KEY for Nova chatbot intelligence")
            if diag.get("bot_user_id", "").startswith("NOT"):
                checks.append("AUTH FAILED: Bot token invalid or bot not installed to workspace")
            if not checks:
                checks.append("ALL CHECKS PASSED - Slack bot should be operational")
            diag["setup_checklist"] = checks
            diag["required_slack_scopes"] = [
                "app_mentions:read", "chat:write", "channels:history",
                "channels:read", "im:history", "im:read", "im:write", "users:read",
            ]
            diag["required_bot_events"] = ["app_mention", "message.im"]
            self._send_json(diag)
        elif path == "/robots.txt":
            robots_content = (
                "User-agent: *\n"
                "Allow: /\n"
                "Disallow: /api/\n"
                "Disallow: /admin/\n"
                "Disallow: /health\n"
                "Disallow: /ready\n"
                "Disallow: /dashboard\n"
                "\n"
                "# AI crawlers welcome (GEO/AEO optimization)\n"
                "User-agent: GPTBot\n"
                "Allow: /\n"
                "Disallow: /api/\n"
                "\n"
                "User-agent: ChatGPT-User\n"
                "Allow: /\n"
                "Disallow: /api/\n"
                "\n"
                "User-agent: Claude-Web\n"
                "Allow: /\n"
                "Disallow: /api/\n"
                "\n"
                "User-agent: Google-Extended\n"
                "Allow: /\n"
                "Disallow: /api/\n"
                "\n"
                "Sitemap: https://media-plan-generator.onrender.com/sitemap.xml\n"
            )
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()
            self.wfile.write(robots_content.encode("utf-8"))
        elif path == "/sitemap.xml":
            _today = datetime.date.today().isoformat()
            sitemap_content = (
                '<?xml version="1.0" encoding="UTF-8"?>\n'
                '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
                '  <url>\n'
                '    <loc>https://media-plan-generator.onrender.com/</loc>\n'
                f'    <lastmod>{_today}</lastmod>\n'
                '    <changefreq>weekly</changefreq>\n'
                '    <priority>1.0</priority>\n'
                '  </url>\n'
                '</urlset>\n'
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/xml; charset=utf-8")
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()
            self.wfile.write(sitemap_content.encode("utf-8"))
        elif path == "/api/channels":
            db = load_channels_db()
            # Inject the full industry options list for frontend consumption
            db["industry_options"] = [
                {"value": "healthcare_medical", "label": "Healthcare & Medical"},
                {"value": "blue_collar_trades", "label": "Blue Collar / Skilled Trades"},
                {"value": "maritime_marine", "label": "Maritime & Marine"},
                {"value": "military_recruitment", "label": "Military Recruitment"},
                {"value": "tech_engineering", "label": "Technology & Engineering"},
                {"value": "general_entry_level", "label": "General / Entry-Level"},
                {"value": "legal_services", "label": "Legal Services"},
                {"value": "finance_banking", "label": "Finance & Banking"},
                {"value": "mental_health", "label": "Mental Health & Behavioral"},
                {"value": "retail_consumer", "label": "Retail & Consumer"},
                {"value": "aerospace_defense", "label": "Aerospace & Defense"},
                {"value": "pharma_biotech", "label": "Pharma & Biotech"},
                {"value": "energy_utilities", "label": "Energy & Utilities"},
                {"value": "insurance", "label": "Insurance"},
                {"value": "telecommunications", "label": "Telecommunications"},
                {"value": "automotive", "label": "Automotive & Manufacturing"},
                {"value": "food_beverage", "label": "Food & Beverage"},
                {"value": "logistics_supply_chain", "label": "Logistics & Supply Chain"},
                {"value": "hospitality_travel", "label": "Hospitality & Travel"},
                {"value": "media_entertainment", "label": "Media & Entertainment"},
                {"value": "construction_real_estate", "label": "Construction & Real Estate"},
                {"value": "education", "label": "Education"},
            ]
            self._send_json(db)
        elif path == "/api/requests":
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            log = load_request_log()
            # Add download URLs for entries that have doc_filename
            enriched_log = []
            for entry in log:
                e = dict(entry)
                if e.get("doc_filename"):
                    e["doc_download_url"] = f"/api/documents/{e['doc_filename']}"
                enriched_log.append(e)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            response = {
                "total_requests": len(enriched_log),
                "requests": enriched_log[-100:]  # Last 100 entries
            }
            self.wfile.write(json.dumps(response, indent=2, default=str).encode())
        elif path == "/dashboard":
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized - set ADMIN_API_KEY env var and pass ?key=...")
                return
            dashboard_path = os.path.join(TEMPLATES_DIR, "dashboard.html")
            if os.path.exists(dashboard_path):
                with open(dashboard_path, "r") as f:
                    html = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(html.encode())
            else:
                self.send_error(404, "Dashboard page not found")
        elif path == "/api/documents":
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            docs_dir = os.path.join(DATA_DIR, "generated_docs")
            os.makedirs(docs_dir, exist_ok=True)
            documents = []
            for fname in sorted(os.listdir(docs_dir), reverse=True):
                if fname.endswith(".zip"):
                    fpath = os.path.join(docs_dir, fname)
                    try:
                        stat = os.stat(fpath)
                        documents.append({
                            "filename": fname,
                            "size_bytes": stat.st_size,
                            "created": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
                            "download_url": f"/api/documents/{fname}"
                        })
                    except OSError:
                        continue
            response = json.dumps({"total": len(documents), "documents": documents[:100]})
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(response.encode())
        elif path.startswith("/api/documents/"):
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized — admin key required")
                return
            fname = path.split("/")[-1]
            # Security: sanitize filename to prevent path traversal
            fname = re.sub(r'[^\w\.\-]', '', fname)
            if not fname or '..' in fname:
                self.send_error(400, "Invalid filename")
                return
            doc_path = os.path.join(DATA_DIR, "generated_docs", fname)
            # Verify the resolved path is within the docs directory
            docs_dir = os.path.realpath(os.path.join(DATA_DIR, "generated_docs"))
            real_path = os.path.realpath(doc_path)
            if not real_path.startswith(docs_dir):
                self.send_error(403, "Access denied")
                return
            if os.path.exists(doc_path) and os.path.isfile(doc_path):
                try:
                    with open(doc_path, "rb") as df:
                        content = df.read()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/zip")
                    self.send_header("Content-Disposition", f'attachment; filename="{fname}"')
                    self.send_header("Content-Length", str(len(content)))
                    self.end_headers()
                    self.wfile.write(content)
                except Exception as e:
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    logger.error("Document read error: %s", e)
                    self.wfile.write(json.dumps({"error": "Failed to read document"}).encode())
            else:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Document not found"}).encode())
        elif path in ("/admin/nova", "/admin/nova/"):
            # ── Nova Admin Dashboard ──
            if not self._check_admin_auth():
                self.send_response(401)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"<h1>401 Unauthorized</h1><p>Set ADMIN_API_KEY env var and pass ?key=YOUR_KEY</p>")
                return
            nova_html = os.path.join(BASE_DIR, "static", "nova-admin.html")
            if os.path.exists(nova_html):
                with open(nova_html, "r") as f:
                    html = f.read()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(html.encode())
            else:
                self.send_error(404, "Nova admin page not found")
        elif path.startswith("/static/"):
            # Serve static files (JS, CSS, images) from the static/ directory
            # URL-decode the path first to catch encoded traversal (%2e%2e)
            decoded_path = urllib.parse.unquote(path)
            safe_path = decoded_path.lstrip("/")
            # Security: reject directory traversal attempts
            if ".." in safe_path or safe_path.startswith("/") or "\x00" in safe_path:
                self.send_error(403)
                return
            filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), safe_path)
            # Resolve symlinks and verify the real path stays within static/
            static_root = os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "static"))
            real_filepath = os.path.realpath(filepath)
            if not real_filepath.startswith(static_root + os.sep) and real_filepath != static_root:
                self.send_error(403)
                return
            ext = os.path.splitext(filepath)[1].lower()
            content_types = {
                ".js": "application/javascript",
                ".css": "text/css",
                ".html": "text/html",
                ".json": "application/json",
                ".png": "image/png",
                ".jpg": "image/jpeg",
                ".svg": "image/svg+xml",
                ".ico": "image/x-icon",
            }
            ctype = content_types.get(ext, "application/octet-stream")
            if os.path.isfile(filepath):
                try:
                    with open(filepath, "rb") as f:
                        content = f.read()
                    self.send_response(200)
                    self.send_header("Content-Type", f"{ctype}; charset=utf-8")
                    self.send_header("Content-Length", str(len(content)))
                    self.send_header("Cache-Control", "public, max-age=604800")  # 7 days
                    self.end_headers()
                    self.wfile.write(content)
                except FileNotFoundError:
                    self.send_error(404)
            else:
                self.send_error(404)
        # ── Feature 1: Swagger UI + OpenAPI spec ──
        elif path == "/docs":
            body = _SWAGGER_UI_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/docs/openapi.json":
            body = json.dumps(_OPENAPI_SPEC, indent=2).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            self.wfile.write(body)
        # ── Feature 2c: Async job polling ──
        elif path.startswith("/api/jobs/"):
            job_id = path.split("/")[-1]
            if not job_id or not re.match(r'^[a-f0-9]{1,12}$', job_id):
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid job_id"}).encode())
                return
            now = time.time()
            with _generation_jobs_lock:
                job = _generation_jobs.get(job_id)
                if not job:
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Job not found or expired"}).encode())
                    return
                # Auto-expire check
                if now - job["created"] > _GENERATION_JOB_EXPIRY_SECONDS:
                    _generation_jobs.pop(job_id, None)
                    self.send_response(404)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Job expired"}).encode())
                    return
                job_status = job["status"]
                elapsed = round(now - job["created"], 1)
            if job_status == "completed":
                # Return the binary content
                result_bytes = job.get("result_bytes", b"")
                content_type = job.get("result_content_type", "application/zip")
                filename = job.get("result_filename", "result.zip")
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.send_header("Content-Length", str(len(result_bytes)))
                self.end_headers()
                self.wfile.write(result_bytes)
                # Clean up completed job after download
                with _generation_jobs_lock:
                    _generation_jobs.pop(job_id, None)
            elif job_status == "failed":
                err_msg = job.get("error", "Generation failed")
                self._send_json({
                    "job_id": job_id,
                    "status": "failed",
                    "error": err_msg,
                    "created": datetime.datetime.fromtimestamp(job["created"]).isoformat(),
                    "elapsed_seconds": elapsed,
                })
            else:
                self._send_json({
                    "job_id": job_id,
                    "status": "processing",
                    "progress_pct": job.get("progress_pct", 0),
                    "created": datetime.datetime.fromtimestamp(job["created"]).isoformat(),
                    "elapsed_seconds": elapsed,
                })
        # ── Feature 5a: SLO compliance ──
        elif path == "/api/health/slos":
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            try:
                from monitoring import MetricsCollector as _MC
                _mc_inst = get_metrics() if _metrics else None
                if _mc_inst and hasattr(_mc_inst, 'check_slo_compliance'):
                    slo_result = _mc_inst.check_slo_compliance()
                    self._send_json(slo_result)
                else:
                    self._send_json({"error": "SLO compliance check not available"})
            except Exception as _slo_err:
                logger.error("SLO check error: %s", _slo_err, exc_info=True)
                slo_err_body = json.dumps({"error": f"SLO check failed: {_slo_err}"}).encode("utf-8")
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(slo_err_body)))
                self.end_headers()
                self.wfile.write(slo_err_body)
        # ── Feature 5b: Eval scores ──
        elif path == "/api/health/eval":
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            try:
                from eval_framework import EvalSuite
                _ef = EvalSuite()
                eval_result = _ef.run_full_eval()
                self._send_json(eval_result)
            except ImportError:
                self._send_json({"error": "Eval framework not available"})
            except Exception as _eval_err:
                logger.error("Eval framework error: %s", _eval_err, exc_info=True)
                eval_err_body = json.dumps({"error": f"Eval failed: {_eval_err}"}).encode("utf-8")
                self.send_response(503)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(eval_err_body)))
                self.end_headers()
                self.wfile.write(eval_err_body)
        # ── Feature 3c: Per-key usage dashboard ──
        elif path == "/api/admin/usage":
            if not self._check_admin_auth():
                self.send_error(401, "Unauthorized")
                return
            now = time.time()
            usage_data = {}
            with _api_keys_lock:
                for key, entry in _api_keys_store.items():
                    # Mask key for display (show first 8 chars)
                    masked = key[:8] + "..." if len(key) > 8 else key
                    tier_name = entry.get("tier", "free")
                    tier_limits = API_KEY_TIERS.get(tier_name, API_KEY_TIERS["free"])
                    minute_usage = len([t for t in entry.get("usage_minute", []) if now - t < 60])
                    day_usage = len([t for t in entry.get("usage_day", []) if now - t < 86400])
                    usage_data[masked] = {
                        "tier": tier_name,
                        "label": entry.get("label", ""),
                        "revoked": entry.get("revoked", False),
                        "created": entry.get("created", ""),
                        "requests_this_minute": minute_usage,
                        "requests_today": day_usage,
                        "limit_rpm": tier_limits["rpm"],
                        "limit_rpd": tier_limits["rpd"],
                    }
            self._send_json({"keys": usage_data, "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()})
        else:
            self.send_error(404)

    def do_POST(self):
        # ── Request ID generation (Feature 6) ──
        try:
            from monitoring import generate_request_id as _gen_rid
            self._request_id = _gen_rid()
        except Exception:
            self._request_id = uuid.uuid4().hex[:12]
        _req_start = time.time()
        if _metrics:
            _metrics.enter_request()
        try:
            self._handle_POST()
        finally:
            if _metrics:
                _metrics.exit_request()
                _latency = (time.time() - _req_start) * 1000
                parsed = urlparse(self.path)
                _metrics.record_request(parsed.path, "POST", 200, _latency)

    def _handle_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        # ── API Versioning: strip /v1 prefix (Feature 4) ──
        if path.startswith("/v1/"):
            path = path[3:]
        if path == "/api/generate":
            if not self._check_rate_limit():
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Rate limit exceeded. Please try again in a minute."}).encode())
                return
            try:
                content_len = int(self.headers.get("Content-Length", 0))
            except (ValueError, TypeError):
                content_len = 0
            if content_len <= 0:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Empty request body"}).encode())
                return
            if content_len > 10 * 1024 * 1024:  # 10MB limit
                self.send_response(413)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Request too large"}).encode())
                return
            body = self.rfile.read(content_len)
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
                return

            # Validate payload is a JSON object (not array/string/number)
            if not isinstance(data, dict):
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Request body must be a JSON object"}).encode())
                return

            # Sanitize all string inputs: strip HTML/script tags to prevent stored XSS
            def _sanitize_val(val):
                if isinstance(val, str):
                    return re.sub(r'<[^>]+>', '', val).strip()
                if isinstance(val, list):
                    return [_sanitize_val(v) for v in val]
                if isinstance(val, dict):
                    return {k: _sanitize_val(v) for k, v in val.items()}
                return val
            for _skey in list(data.keys()):
                data[_skey] = _sanitize_val(data[_skey])

            # Validate required fields
            client_name_input = (data.get("client_name") or "").strip()
            if not client_name_input:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Client name is required."}).encode())
                return
            data["client_name"] = client_name_input

            # Validate critical input fields (non-blocking: store warnings, don't 400)
            _roles_input = data.get("target_roles") or data.get("roles", [])
            _locs_input = data.get("locations", [])
            _budget_input = str(data.get("budget", "") or data.get("budget_range", "") or "").strip()
            _validation_warnings = []
            if not _roles_input or (isinstance(_roles_input, list) and len(_roles_input) == 0):
                _validation_warnings.append("No target roles specified -- plan will use generic benchmarks")
            if not _locs_input or (isinstance(_locs_input, list) and len(_locs_input) == 0):
                _validation_warnings.append("No locations specified -- plan will use global averages")
            if not _budget_input:
                _validation_warnings.append("No budget specified -- plan will use estimated ranges")
            if _validation_warnings:
                data["_input_warnings"] = _validation_warnings
                logger.info("Input validation warnings: %s", _validation_warnings)

            # ── Feature 2b: Async generation mode ──
            if self.headers.get("X-Async", "").lower() == "true":
                job_id = uuid.uuid4().hex[:12]
                with _generation_jobs_lock:
                    _generation_jobs[job_id] = {
                        "status": "processing",
                        "progress_pct": 0,
                        "created": time.time(),
                        "result_bytes": None,
                        "result_content_type": None,
                        "result_filename": None,
                        "error": None,
                    }
                request_id = getattr(self, "_request_id", None)

                def _async_generate(jid, gen_data, rid):
                    """Run the full sync generation pipeline in a background thread."""
                    try:
                        with _generation_jobs_lock:
                            if jid in _generation_jobs:
                                _generation_jobs[jid]["progress_pct"] = 10

                        # Enrichment
                        enriched = {}
                        if enrich_data is not None:
                            try:
                                enriched = enrich_data(gen_data, request_id=rid) if rid else enrich_data(gen_data)
                                gen_data["_enriched"] = enriched
                            except Exception:
                                gen_data["_enriched"] = {}
                        else:
                            gen_data["_enriched"] = {}

                        with _generation_jobs_lock:
                            if jid in _generation_jobs:
                                _generation_jobs[jid]["progress_pct"] = 30

                        # KB + Synthesis
                        kb = load_knowledge_base()
                        gen_data["_knowledge_base"] = kb
                        if data_synthesize is not None:
                            try:
                                synthesized = data_synthesize(enriched, kb, gen_data)
                                gen_data["_synthesized"] = synthesized
                            except Exception:
                                gen_data["_synthesized"] = {}
                        else:
                            gen_data["_synthesized"] = {}

                        # Copy geopolitical context from enriched to synthesized
                        # (PPT/Excel read from _synthesized; enrichment stores it in _enriched)
                        if isinstance(enriched, dict) and isinstance(gen_data.get("_synthesized"), dict):
                            _geo_from_enriched = enriched.get("geopolitical_context")
                            if _geo_from_enriched and isinstance(_geo_from_enriched, dict):
                                gen_data["_synthesized"]["geopolitical_context"] = _geo_from_enriched

                        with _generation_jobs_lock:
                            if jid in _generation_jobs:
                                _generation_jobs[jid]["progress_pct"] = 50

                        # Industry classification
                        industry_raw = gen_data.get("industry", "")
                        company_name = gen_data.get("client_name", "")
                        roles_list = gen_data.get("target_roles") or gen_data.get("roles", [])
                        if isinstance(roles_list, str):
                            roles_list = [r.strip() for r in roles_list.split(",") if r.strip()]
                        industry_profile = classify_industry(industry_raw, company_name, roles_list)
                        gen_data["industry"] = industry_profile.get("legacy_key", "general_entry_level")
                        if not gen_data.get("industry_label"):
                            gen_data["industry_label"] = industry_profile["sector"]
                        gen_data["talent_profile"] = industry_profile["talent_profile"]
                        gen_data["bls_sector"] = industry_profile["bls_sector"]
                        gen_data["naics_code"] = industry_profile.get("naics", "00")

                        with _generation_jobs_lock:
                            if jid in _generation_jobs:
                                _generation_jobs[jid]["progress_pct"] = 60

                        # Budget Allocation (Phase 4 -- same as sync path)
                        if calculate_budget_allocation is not None:
                            try:
                                _ind_key_ba = gen_data.get("industry", "general_entry_level")
                                _DEFAULT_ALLOC_BA = {"programmatic_dsp": 35, "global_boards": 20, "niche_boards": 15, "social_media": 12, "regional_boards": 8, "employer_branding": 5, "apac_regional": 3, "emea_regional": 2}
                                if INDUSTRY_ALLOC_PROFILES is not None:
                                    channel_pcts = INDUSTRY_ALLOC_PROFILES.get(_ind_key_ba, _DEFAULT_ALLOC_BA)
                                else:
                                    channel_pcts = _DEFAULT_ALLOC_BA

                                _bstr_ba = str(gen_data.get("budget", "") or gen_data.get("budget_range", "") or "").strip()
                                if not _bstr_ba:
                                    _bstr_ba = str(gen_data.get("budget_range", "") or "").strip()
                                _bval_ba = parse_budget(_bstr_ba)

                                _roles_raw = gen_data.get("target_roles") or gen_data.get("roles", [])
                                _roles_for_ba = []
                                for r in (_roles_raw if isinstance(_roles_raw, list) else []):
                                    if isinstance(r, str):
                                        _roles_for_ba.append({"title": r, "count": 1, "tier": gen_data.get("_role_tiers", {}).get(r, {}).get("tier", "Professional")})
                                    elif isinstance(r, dict):
                                        _roles_for_ba.append({"title": r.get("title", ""), "count": int(r.get("count", 1)), "tier": r.get("_tier", "Professional")})

                                _locs_raw = gen_data.get("locations", [])
                                _locs_for_ba = []
                                for loc in (_locs_raw if isinstance(_locs_raw, list) else []):
                                    if isinstance(loc, str):
                                        parts = [p.strip() for p in loc.split(",")]
                                        _locs_for_ba.append({"city": parts[0] if parts else loc, "state": parts[1] if len(parts) > 1 else "", "country": parts[2] if len(parts) > 2 else parts[-1] if len(parts) > 1 else "US"})
                                    elif isinstance(loc, dict):
                                        _locs_for_ba.append({"city": loc.get("city", ""), "state": loc.get("state", ""), "country": loc.get("country", "")})

                                synthesized_for_ba = gen_data.get("_synthesized", {})
                                enriched_for_ba = gen_data.get("_enriched", {})
                                merged_for_ba = {}
                                if isinstance(enriched_for_ba, dict):
                                    merged_for_ba.update(enriched_for_ba)
                                if isinstance(synthesized_for_ba, dict):
                                    merged_for_ba.update(synthesized_for_ba)
                                budget_result = calculate_budget_allocation(
                                    total_budget=_bval_ba,
                                    roles=_roles_for_ba,
                                    locations=_locs_for_ba,
                                    industry=gen_data.get("industry", "General"),
                                    channel_percentages=channel_pcts,
                                    synthesized_data=merged_for_ba,
                                    knowledge_base=kb,
                                    collar_type=gen_data.get("_collar_type", ""),
                                    campaign_start_month=int(gen_data.get("campaign_start_month", 0) or 0),
                                )
                                gen_data["_budget_allocation"] = budget_result
                                logger.info("Async budget allocation complete: %s", list(budget_result.keys()) if isinstance(budget_result, dict) else 'N/A')
                            except Exception as ba_err:
                                logger.warning("Async budget allocation failed (non-fatal): %s", ba_err)
                                gen_data["_budget_allocation"] = {}
                        else:
                            gen_data["_budget_allocation"] = {}

                        with _generation_jobs_lock:
                            if jid in _generation_jobs:
                                _generation_jobs[jid]["progress_pct"] = 70

                        # Gemini/LLM verification of key plan data points
                        try:
                            gen_data["_verification"] = _verify_plan_data(gen_data)
                        except Exception:
                            gen_data["_verification"] = {"status": "skipped", "reason": "verification_error"}

                        # Excel generation
                        excel_bytes = generate_excel(gen_data)

                        with _generation_jobs_lock:
                            if jid in _generation_jobs:
                                _generation_jobs[jid]["progress_pct"] = 80

                        # PPT generation
                        client_name = re.sub(r'[^a-zA-Z0-9_\-]', '_', gen_data.get("client_name") or "Client")
                        pptx_bytes = None
                        if generate_pptx is not None:
                            try:
                                pptx_bytes = generate_pptx(gen_data)
                            except Exception:
                                pptx_bytes = None

                        if pptx_bytes:
                            zip_buffer = io.BytesIO()
                            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                                zf.writestr(f"{client_name}_Media_Plan.xlsx", excel_bytes)
                                zf.writestr(f"{client_name}_Strategy_Deck.pptx", pptx_bytes)
                            result_bytes = zip_buffer.getvalue()
                            result_ct = "application/zip"
                            result_fn = f"{client_name}_Media_Plan_Bundle.zip"
                        else:
                            result_bytes = excel_bytes
                            result_ct = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            result_fn = f"{client_name}_Media_Plan.xlsx"

                        with _generation_jobs_lock:
                            if jid in _generation_jobs:
                                _generation_jobs[jid].update({
                                    "status": "completed",
                                    "progress_pct": 100,
                                    "result_bytes": result_bytes,
                                    "result_content_type": result_ct,
                                    "result_filename": result_fn,
                                })
                        logger.info("Async job %s completed (%d bytes)", jid, len(result_bytes))
                    except Exception as async_err:
                        logger.error("Async job %s failed: %s", jid, async_err, exc_info=True)
                        with _generation_jobs_lock:
                            if jid in _generation_jobs:
                                _generation_jobs[jid].update({
                                    "status": "failed",
                                    "error": str(async_err),
                                })
                        # Send email alert for async generation failure (matches sync path)
                        try:
                            from email_alerts import send_generation_failure_alert
                            _client = gen_data.get("client_name", "Unknown") if gen_data else "Unknown"
                            send_generation_failure_alert(_client, str(async_err), jid)
                        except Exception:
                            pass  # email alerts are best-effort

                t = threading.Thread(
                    target=_async_generate,
                    args=(job_id, data, request_id),
                    daemon=True,
                    name=f"async-gen-{job_id}",
                )
                t.start()
                self._send_json({
                    "job_id": job_id,
                    "status": "processing",
                    "poll_url": f"/api/jobs/{job_id}",
                })
                return

            # ── P0 Fix: Capture timeline, hire_volume, and notes from input ──
            # campaign_duration: normalize from frontend dropdown or API input
            raw_duration = str(data.get("campaign_duration") or data.get("timeline") or "").strip()
            if raw_duration:
                data["campaign_duration"] = raw_duration
            else:
                data["campaign_duration"] = "Not specified"

            # campaign_start_month: int 1-12, 0 or absent = current month
            raw_csm = data.get("campaign_start_month", 0)
            try:
                campaign_start_month = int(raw_csm) if raw_csm else 0
            except (ValueError, TypeError):
                campaign_start_month = 0
            if campaign_start_month < 0 or campaign_start_month > 12:
                campaign_start_month = 0
            if campaign_start_month == 0:
                import datetime as _dt_csm
                campaign_start_month = _dt_csm.datetime.now().month
            data["campaign_start_month"] = campaign_start_month

            # hire_volume: capture from form field or parse from notes
            raw_hire_vol = str(data.get("hire_volume") or "").strip()
            raw_notes = (data.get("notes") or "").strip()

            if raw_hire_vol:
                data["hire_volume"] = raw_hire_vol
            elif raw_notes:
                # Parse hire volume from notes (e.g. "5000+", "10,000+", "500+ teachers")
                hv_match = re.search(r'(\d[\d,]*)\+?\s*(?:hires?|positions?|roles?|openings?|teachers?|nurses?|drivers?|employees?|workers?|associates?|people|staff|headcount)?', raw_notes, re.IGNORECASE)
                if hv_match:
                    hv_num = hv_match.group(1).replace(",", "")
                    try:
                        hv_int = int(hv_num)
                        if hv_int >= 5:  # Only treat as hire volume if >= 5
                            # Preserve the original text snippet for context
                            hv_text = hv_match.group(0).strip()
                            if "+" in raw_notes[hv_match.start():hv_match.end() + 2]:
                                hv_text = f"{hv_int:,}+"
                            else:
                                hv_text = f"{hv_int:,}"
                            data["hire_volume"] = f"{hv_text} hires"
                    except ValueError:
                        pass
            if not data.get("hire_volume"):
                data["hire_volume"] = "Not specified"

            # Store notes for downstream use
            if raw_notes:
                data["notes"] = raw_notes

            # ── Normalize roles early: dict-of-dicts -> list-of-strings ──
            # API/form can send roles as [{"title":"...","location":"..."}] or ["string"]
            for _rkey in ("roles", "target_roles"):
                _rlist = data.get(_rkey, [])
                if isinstance(_rlist, list) and _rlist and isinstance(_rlist[0], dict):
                    data[_rkey] = [
                        (r.get("title") or r.get("role") or str(r)) for r in _rlist
                    ]

            # Compute campaign_weeks from campaign_duration for timeline phasing
            duration_str = str(data.get("campaign_duration", "") or "")
            campaign_weeks = 12  # default
            dur_lower = duration_str.lower()
            # Order matters: check more specific/longer ranges before shorter ones
            if "2-5 year" in dur_lower or "long-term" in dur_lower or "long term" in dur_lower:
                campaign_weeks = 156
            elif "1-2 year" in dur_lower or "2 year" in dur_lower:
                campaign_weeks = 80
            elif "6-12 month" in dur_lower or "9 month" in dur_lower or "12 month" in dur_lower or "1 year" in dur_lower:
                campaign_weeks = 48
            elif "3-6 month" in dur_lower or "4 month" in dur_lower or "5 month" in dur_lower or "6 month" in dur_lower:
                campaign_weeks = 24
            elif "1-3 month" in dur_lower or "1 month" in dur_lower or "2 month" in dur_lower or "3 month" in dur_lower:
                campaign_weeks = 12
            elif "ongoing" in dur_lower:
                campaign_weeks = 52  # annual cycle
            else:
                # Try to parse weeks directly (e.g. "26 weeks", "16 weeks")
                wk_match = re.search(r'(\d+)\s*week', dur_lower)
                if wk_match:
                    campaign_weeks = int(wk_match.group(1))
                else:
                    # Try months (e.g. "6 months")
                    mo_match = re.search(r'(\d+)\s*month', dur_lower)
                    if mo_match:
                        campaign_weeks = int(mo_match.group(1)) * 4
            data["campaign_weeks"] = campaign_weeks

            # ── Phase 0: Canonical Taxonomy Normalization ──
            # Run the standardizer on all input fields BEFORE they enter
            # the enrichment/synthesis pipeline.  This ensures every
            # downstream lookup (KB, channels_db, BLS, etc.) uses
            # consistent canonical keys instead of variant strings.
            if _STANDARDIZER_AVAILABLE:
                try:
                    # -- Normalize industry --
                    raw_ind = data.get("industry", "")
                    canonical_ind = std_normalize_industry(raw_ind)
                    data["_industry_original"] = raw_ind
                    data["_industry_canonical"] = canonical_ind
                    ind_meta = CANONICAL_INDUSTRIES.get(canonical_ind, {})
                    data["_industry_legacy_key"] = ind_meta.get(
                        "deep_bench_key", ind_meta.get("aliases", [""])[0]
                    )
                    data["_industry_kb_key"] = ind_meta.get("kb_key", "")

                    # Update the primary industry key so the enrichment
                    # pipeline (api_enrichment, data_synthesizer, etc.)
                    # receives a canonical key instead of the raw variant.
                    # The classify_industry call downstream will further
                    # refine this to the legacy_key for backward compat.
                    if canonical_ind and canonical_ind != "general":
                        data["industry"] = canonical_ind
                    elif data.get("_industry_legacy_key"):
                        data["industry"] = data["_industry_legacy_key"]

                    # -- Normalize roles (attach SOC + tier metadata) --
                    for role_key in ("target_roles", "roles"):
                        raw_roles = data.get(role_key, [])
                        if isinstance(raw_roles, list):
                            for r in raw_roles:
                                if isinstance(r, dict):
                                    title = r.get("title", "")
                                    r["_canonical_role"] = std_normalize_role(title)
                                    r["_soc_code"] = std_get_soc_code(title)
                                    r["_role_tier"] = std_get_role_tier(title)

                    # -- Normalize locations (attach region/market keys) --
                    raw_locs = data.get("locations", [])
                    if isinstance(raw_locs, list):
                        parsed_locs = []
                        for loc in raw_locs:
                            loc_str = loc if isinstance(loc, str) else ""
                            if isinstance(loc, dict):
                                loc_str = ", ".join(filter(None, [
                                    loc.get("city", ""),
                                    loc.get("state", ""),
                                    loc.get("country", ""),
                                ]))
                            parsed_locs.append(std_normalize_location(loc_str))
                        data["_locations_parsed"] = parsed_locs

                    logger.info(
                        "Standardizer: industry=%s -> canonical=%s (kb_key=%s)",
                        raw_ind, canonical_ind, data.get("_industry_kb_key", ""),
                    )
                except Exception as e:
                    logger.warning("Standardizer normalization failed (non-fatal): %s", e)

            # Enrich data with live API data
            enriched = {}
            if enrich_data is not None:
                try:
                    # Feature 6c: propagate request_id to enrich_data
                    _rid = getattr(self, "_request_id", None)
                    enriched = enrich_data(data, request_id=_rid) if _rid else enrich_data(data)
                    data["_enriched"] = enriched
                    logger.info("API enrichment complete: %s", enriched.get('enrichment_summary', {}))
                except Exception as e:
                    logger.warning("API enrichment failed (non-fatal): %s", e)
                    data["_enriched"] = {}
            else:
                data["_enriched"] = {}

            # ── Phase 2: Load Knowledge Base ──
            kb = load_knowledge_base()
            data["_knowledge_base"] = kb  # Pass KB to PPT for fallback data

            # ── Phase 3: Data Synthesis ──
            if data_synthesize is not None:
                try:
                    synthesized = data_synthesize(enriched, kb, data)
                    data["_synthesized"] = synthesized
                    logger.info("Data synthesis complete: %s", list(synthesized.keys()) if isinstance(synthesized, dict) else 'N/A')
                except Exception as e:
                    logger.warning("Data synthesis failed (non-fatal): %s", e)
                    data["_synthesized"] = {}
            else:
                data["_synthesized"] = {}

            # ── Copy geopolitical context from enriched to synthesized ──
            # PPT/Excel read from _synthesized, but geopolitical_context is produced
            # by api_enrichment.enrich_data() and stored in _enriched. Bridge the gap.
            if isinstance(enriched, dict) and isinstance(data.get("_synthesized"), dict):
                _geo_from_enriched = enriched.get("geopolitical_context")
                if _geo_from_enriched and isinstance(_geo_from_enriched, dict):
                    data["_synthesized"]["geopolitical_context"] = _geo_from_enriched

            # ── NAICS-based Industry Classification ──
            # Classify industry using the NAICS engine, supporting both legacy keys
            # (from frontend dropdown) and free-text industry names
            industry_raw = data.get("industry", "")
            company_name = data.get("client_name", "") or data.get("company_name", "")
            roles_list = data.get("target_roles") or data.get("roles", [])
            if isinstance(roles_list, str):
                roles_list = [r.strip() for r in roles_list.split(",") if r.strip()]
            industry_profile = classify_industry(industry_raw, company_name, roles_list)
            
            # Set the industry key to the legacy key for backward compatibility with
            # research.py, channels_db.json, and all internal lookup tables
            data["industry"] = industry_profile.get("legacy_key", "general_entry_level")
            
            # Set the display label from NAICS classification (prefer frontend-provided label)
            if not data.get("industry_label"):
                data["industry_label"] = industry_profile["sector"]
            
            # Store talent profile and BLS sector for downstream use
            data["talent_profile"] = industry_profile["talent_profile"]
            data["bls_sector"] = industry_profile["bls_sector"]
            data["naics_code"] = industry_profile.get("naics", "00")

            # ── Phase 4: Budget Allocation ──
            if calculate_budget_allocation is not None:
                try:
                    _ind_key_ba = data.get("industry", "general_entry_level")
                    _DEFAULT_ALLOC_BA = {"programmatic_dsp": 35, "global_boards": 20, "niche_boards": 15, "social_media": 12, "regional_boards": 8, "employer_branding": 5, "apac_regional": 3, "emea_regional": 2}
                    # Use the expanded 17-industry profiles from ppt_generator if available,
                    # falling back to the default allocation
                    if INDUSTRY_ALLOC_PROFILES is not None:
                        channel_pcts = INDUSTRY_ALLOC_PROFILES.get(_ind_key_ba, _DEFAULT_ALLOC_BA)
                    else:
                        # Inline fallback if ppt_generator is not importable (5 industries)
                        _INDUSTRY_ALLOC_BA_FALLBACK = {
                            "healthcare_medical":     {"programmatic_dsp": 22, "global_boards": 15, "niche_boards": 30, "social_media": 10, "regional_boards": 10, "employer_branding": 8, "apac_regional": 3, "emea_regional": 2},
                            "tech_engineering":       {"programmatic_dsp": 30, "global_boards": 15, "niche_boards": 20, "social_media": 18, "regional_boards": 5,  "employer_branding": 7, "apac_regional": 3, "emea_regional": 2},
                            "finance_banking":        {"programmatic_dsp": 25, "global_boards": 18, "niche_boards": 25, "social_media": 10, "regional_boards": 7,  "employer_branding": 10, "apac_regional": 3, "emea_regional": 2},
                            "retail_consumer":        {"programmatic_dsp": 38, "global_boards": 22, "niche_boards": 8,  "social_media": 20, "regional_boards": 7,  "employer_branding": 3, "apac_regional": 1, "emea_regional": 1},
                            "general_entry_level":    {"programmatic_dsp": 40, "global_boards": 22, "niche_boards": 8,  "social_media": 15, "regional_boards": 10, "employer_branding": 3, "apac_regional": 1, "emea_regional": 1},
                        }
                        channel_pcts = _INDUSTRY_ALLOC_BA_FALLBACK.get(_ind_key_ba, _DEFAULT_ALLOC_BA)

                    # Parse budget to float — uses shared_utils.parse_budget (single source of truth)
                    _bstr_ba = str(data.get("budget", "") or data.get("budget_range", "") or "").strip()
                    if not _bstr_ba:
                        _bstr_ba = str(data.get("budget_range", "") or "").strip()
                    _bval_ba = parse_budget(_bstr_ba)
                    logger.info("Budget allocation: parsed '%s' -> $%s", _bstr_ba, f"{_bval_ba:,.2f}")

                    # Build role dicts from string list
                    _roles_raw = data.get("target_roles") or data.get("roles", [])
                    _roles_for_ba = []
                    for r in (_roles_raw if isinstance(_roles_raw, list) else []):
                        if isinstance(r, str):
                            _roles_for_ba.append({"title": r, "count": 1, "tier": data.get("_role_tiers", {}).get(r, {}).get("tier", "Professional")})
                        elif isinstance(r, dict):
                            _roles_for_ba.append({"title": r.get("title", ""), "count": int(r.get("count", 1)), "tier": r.get("_tier", "Professional")})

                    # Build location dicts from string list
                    _locs_raw = data.get("locations", [])
                    _locs_for_ba = []
                    for loc in (_locs_raw if isinstance(_locs_raw, list) else []):
                        if isinstance(loc, str):
                            parts = [p.strip() for p in loc.split(",")]
                            _locs_for_ba.append({"city": parts[0] if parts else loc, "state": parts[1] if len(parts) > 1 else "", "country": parts[2] if len(parts) > 2 else parts[-1] if len(parts) > 1 else "US"})
                        elif isinstance(loc, dict):
                            _locs_for_ba.append({"city": loc.get("city", ""), "state": loc.get("state", ""), "country": loc.get("country", "")})

                    synthesized_for_ba = data.get("_synthesized", {})
                    enriched_for_ba = data.get("_enriched", {})
                    # Merge raw enrichment + synthesized so budget engine can
                    # access both raw API keys (google_ads_data, teleport_data)
                    # and synthesized fusion keys (ad_platform_analysis, etc.)
                    merged_for_ba = {}
                    if isinstance(enriched_for_ba, dict):
                        merged_for_ba.update(enriched_for_ba)
                    if isinstance(synthesized_for_ba, dict):
                        merged_for_ba.update(synthesized_for_ba)
                    budget_result = calculate_budget_allocation(
                        total_budget=_bval_ba,
                        roles=_roles_for_ba,
                        locations=_locs_for_ba,
                        industry=data.get("industry", "General"),
                        channel_percentages=channel_pcts,
                        synthesized_data=merged_for_ba,
                        knowledge_base=kb,
                        collar_type=data.get("_collar_type", ""),
                        campaign_start_month=int(data.get("campaign_start_month", 0) or 0),
                    )
                    data["_budget_allocation"] = budget_result
                    logger.info("Budget allocation complete: %s", list(budget_result.keys()) if isinstance(budget_result, dict) else 'N/A')
                except Exception as e:
                    logger.warning("Budget allocation failed (non-fatal): %s", e)
                    data["_budget_allocation"] = {}
            else:
                data["_budget_allocation"] = {}

            # ── Gemini/LLM verification of plan data (same as async path) ──
            try:
                data["_verification"] = _verify_plan_data(data)
            except Exception:
                data["_verification"] = {"status": "skipped", "reason": "verification_error"}

            start_time = time.time()
            try:
                excel_bytes = generate_excel(data)
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                logger.error("Excel generation error: %s", tb)
                generation_time = time.time() - start_time
                log_request(data, "error", generation_time=generation_time, error_msg=str(e))
                try:
                    from email_alerts import send_generation_failure_alert
                    send_generation_failure_alert(
                        client_name=data.get("client_name", "Unknown"),
                        error=str(e),
                        request_id=data.get("_request_id", ""),
                    )
                except Exception:
                    pass
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Generation failed. Please check your inputs and try again."}).encode())
                return

            # Sanitize client_name to ASCII-safe characters (prevents CJK/Unicode crashes in filenames/headers)
            client_name = re.sub(r'[^a-zA-Z0-9_\-]', '_', data.get("client_name") or "Client")

            # Generate Strategy PPT deck
            pptx_bytes = None
            pptx_warning = None
            if generate_pptx is not None:
                try:
                    pptx_bytes = generate_pptx(data)
                    logger.info("PPT generated: %d bytes", len(pptx_bytes))
                except Exception as e:
                    logger.error("PPT generation error: %s", e, exc_info=True)
                    pptx_bytes = None
                    pptx_warning = "Strategy deck (PPT) could not be generated. Excel plan is included."
            else:
                logger.info("PPT generation skipped: ppt_generator not available")

            if pptx_bytes:
                # Bundle both files in a ZIP
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(f"{client_name}_Media_Plan.xlsx", excel_bytes)
                    zf.writestr(f"{client_name}_Strategy_Deck.pptx", pptx_bytes)
                zip_bytes = zip_buffer.getvalue()

                # Save a copy for the document repository
                doc_filename = None
                try:
                    docs_dir = os.path.join(DATA_DIR, "generated_docs")
                    os.makedirs(docs_dir, exist_ok=True)
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    client_slug = re.sub(r'[^a-zA-Z0-9_\-]', '_', data.get("client_name") or "Client")
                    doc_filename = f"{timestamp}_{client_slug}.zip"
                    doc_path = os.path.join(docs_dir, doc_filename)
                    with open(doc_path, "wb") as df:
                        df.write(zip_bytes)
                    logger.info("Document saved: %s (%d bytes)", doc_filename, len(zip_bytes))
                    _cleanup_old_docs(docs_dir)
                except Exception as doc_err:
                    logger.warning("Could not save document copy: %s", doc_err)
                    doc_filename = None

                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Disposition", f'attachment; filename="{client_name}_Media_Plan_Bundle.zip"')
                self.send_header("Content-Length", str(len(zip_bytes)))
                self.end_headers()
                self.wfile.write(zip_bytes)
                generation_time = time.time() - start_time
                log_request(data, "success", file_size=len(zip_bytes), generation_time=generation_time, doc_filename=doc_filename)
                if _metrics:
                    _metrics.record_generation(generation_time)
            else:
                # Fallback to Excel only if PPT fails — save Excel as doc copy
                doc_filename = None
                try:
                    docs_dir = os.path.join(DATA_DIR, "generated_docs")
                    os.makedirs(docs_dir, exist_ok=True)
                    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                    client_slug = re.sub(r'[^a-zA-Z0-9_\-]', '_', data.get("client_name") or "Client")
                    # Wrap the Excel in a ZIP for consistent storage
                    doc_zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(doc_zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                        zf.writestr(f"{client_name}_Media_Plan.xlsx", excel_bytes)
                    doc_filename = f"{timestamp}_{client_slug}.zip"
                    doc_path = os.path.join(docs_dir, doc_filename)
                    with open(doc_path, "wb") as df:
                        df.write(doc_zip_buffer.getvalue())
                    logger.info("Document saved (Excel only): %s", doc_filename)
                except Exception as doc_err:
                    logger.warning("Could not save document copy: %s", doc_err)
                    doc_filename = None

                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                self.send_header("Content-Disposition", f'attachment; filename="{client_name}_Media_Plan.xlsx"')
                self.send_header("Content-Length", str(len(excel_bytes)))
                if pptx_warning:
                    self.send_header("X-PPT-Warning", pptx_warning)
                self.end_headers()
                self.wfile.write(excel_bytes)
                generation_time = time.time() - start_time
                log_request(data, "success", file_size=len(excel_bytes), generation_time=generation_time, doc_filename=doc_filename)
                if _metrics:
                    _metrics.record_generation(generation_time)
        elif path == "/api/chat":
            # ── Nova Chat Endpoint ──
            if not self._check_rate_limit() or not self._check_global_chat_rate_limit():
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Rate limit exceeded. Please wait a moment."}).encode())
                return
            try:
                content_len = int(self.headers.get("Content-Length", 0))
            except (ValueError, TypeError):
                content_len = 0
            if content_len <= 0:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Empty request body"}).encode())
                return
            if content_len > 100 * 1024:  # 100KB limit for chat (4000 char msg + history)
                self.send_response(413)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Request too large"}).encode())
                return
            body = self.rfile.read(content_len)
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                cors_origin = self._get_cors_origin()
                if cors_origin:
                    self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
                return
            try:
                from nova import handle_chat_request
                response = handle_chat_request(data)
                # NOTE: Chat path routing is recorded inside nova.py (_nova_metrics.record_chat)
                # which forwards to MetricsCollector. Do NOT double-count here.
            except Exception as chat_err:
                logger.error("Nova chat error: %s", chat_err, exc_info=True)
                response = {
                    "response": "An error occurred processing your request. Please try again.",
                    "sources": [],
                    "confidence": 0.0,
                    "tools_used": [],
                    "error": "Internal error processing request",
                }
            resp_body = json.dumps(response).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            cors_origin = self._get_cors_origin()
            if cors_origin:
                self.send_header("Access-Control-Allow-Origin", cors_origin)
            self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            self.wfile.write(resp_body)
        elif path == "/api/slack/events":
            # ── Nova Slack Event Webhook ──
            # FIX: Handle Slack retries — return 200 immediately to prevent duplicates
            retry_num = self.headers.get("X-Slack-Retry-Num")
            retry_reason = self.headers.get("X-Slack-Retry-Reason")
            if retry_num:
                logger.info("Slack retry #%s (reason: %s) — acknowledging without reprocessing", retry_num, retry_reason)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("X-Slack-No-Retry", "1")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True}).encode())
                return
            try:
                content_len = int(self.headers.get("Content-Length", 0))
            except (ValueError, TypeError):
                content_len = 0
            if content_len > 1 * 1024 * 1024:  # 1MB limit for Slack events
                self.send_response(413)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Request too large"}).encode())
                return
            body = self.rfile.read(content_len)
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
                return

            # Verify Slack request signature -- SECURITY: never skip on error
            # in production. Only skip if SLACK_SIGNING_SECRET is not configured
            # (i.e., the Slack bot is intentionally disabled).
            _slack_signing_secret = os.environ.get("SLACK_SIGNING_SECRET", "")
            if _slack_signing_secret:
                try:
                    from nova_slack import get_nova_bot
                    _nova_bot = get_nova_bot()
                    _slack_ts = self.headers.get("X-Slack-Request-Timestamp", "")
                    _slack_sig = self.headers.get("X-Slack-Signature", "")
                    if not _nova_bot.verify_slack_signature(_slack_ts, body.decode("utf-8") if isinstance(body, bytes) else body, _slack_sig):
                        logger.warning("Slack signature verification failed")
                        self.send_response(403)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"error": "Invalid signature"}).encode())
                        return
                except ImportError:
                    # nova_slack module not available -- reject the request
                    logger.error("Slack signing secret configured but nova_slack module unavailable")
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Slack integration unavailable"}).encode())
                    return
                except Exception as sig_err:
                    # Signature verification failed due to unexpected error -- reject
                    logger.error("Slack signature verification error: %s", sig_err)
                    self.send_response(403)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Signature verification failed"}).encode())
                    return
            else:
                logger.warning("SLACK_SIGNING_SECRET not set -- rejecting Slack event (configure to enable)")
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Slack signing secret not configured"}).encode())
                return

            # Respond to Slack immediately (within 3 seconds), process async
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Slack-No-Retry", "1")
            self.end_headers()

            # Handle URL verification challenge synchronously (Slack requires immediate response)
            if data.get("type") == "url_verification":
                challenge_resp = json.dumps({"challenge": data.get("challenge", "")}).encode()
                self.wfile.write(challenge_resp)
                return

            self.wfile.write(json.dumps({"ok": True}).encode())

            # Process event asynchronously via bounded thread pool (prevents thread explosion)
            def _process_slack_event_async(event_data):
                try:
                    from nova_slack import handle_slack_event
                    handle_slack_event(event_data)
                    if _metrics:
                        _metrics.record_slack_event()
                except Exception as slack_err:
                    logger.error("Nova Slack async event error: %s", slack_err, exc_info=True)
            try:
                _slack_executor.submit(_process_slack_event_async, data)
            except RuntimeError:
                # Thread pool shut down or full -- process synchronously as fallback
                logger.warning("Slack thread pool unavailable, processing synchronously")
                _process_slack_event_async(data)
        elif path == "/api/admin/nova":
            # ── Nova Admin API (unanswered questions management) ──
            if not self._check_admin_auth():
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Unauthorized"}).encode())
                return
            try:
                content_len = int(self.headers.get("Content-Length", 0))
            except (ValueError, TypeError):
                content_len = 0
            if content_len > 1 * 1024 * 1024:  # 1MB limit for admin
                self.send_response(413)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Request too large"}).encode())
                return
            body = self.rfile.read(content_len)
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
                return
            try:
                from nova_slack import handle_admin_unanswered
                response = handle_admin_unanswered(data)
            except Exception as admin_err:
                logger.error("Nova admin error: %s", admin_err, exc_info=True)
                response = {"error": "Internal error processing request"}
            resp_body = json.dumps(response).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            self.wfile.write(resp_body)
        elif path == "/api/nova/chat":
            # ── Nova Standalone Chat (admin testing) ──
            if not self._check_admin_auth():
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Unauthorized"}).encode())
                return
            try:
                content_len = int(self.headers.get("Content-Length", 0))
            except (ValueError, TypeError):
                content_len = 0
            if content_len > 1 * 1024 * 1024:  # 1MB limit
                self.send_response(413)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Request too large"}).encode())
                return
            body = self.rfile.read(content_len)
            try:
                data = json.loads(body) if body else {}
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
                return
            try:
                from nova_slack import handle_chat_standalone
                response = handle_chat_standalone(data)
            except Exception as chat_err:
                logger.error("Nova chat error: %s", chat_err, exc_info=True)
                response = {"error": "Internal error processing request"}
            resp_body = json.dumps(response).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(resp_body)))
            self.end_headers()
            self.wfile.write(resp_body)
        # ── Feature 3c: Admin API key management ──
        elif path == "/api/admin/keys":
            if not self._check_admin_auth():
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Unauthorized"}).encode())
                return
            try:
                content_len = int(self.headers.get("Content-Length", 0))
            except (ValueError, TypeError):
                content_len = 0
            body = self.rfile.read(content_len) if content_len > 0 else b"{}"
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
                return
            action = data.get("action", "list")
            if action == "create":
                tier = data.get("tier", "free")
                if tier not in API_KEY_TIERS:
                    self._send_json({"error": f"Invalid tier. Must be one of: {list(API_KEY_TIERS.keys())}"})
                    return
                label = data.get("label", "")
                new_key = uuid.uuid4().hex
                with _api_keys_lock:
                    _api_keys_store[new_key] = {
                        "tier": tier,
                        "label": label,
                        "created": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                        "revoked": False,
                        "usage_minute": [],
                        "usage_day": [],
                    }
                self._send_json({
                    "action": "created",
                    "key": new_key,
                    "tier": tier,
                    "label": label,
                    "limits": API_KEY_TIERS[tier],
                })
            elif action == "revoke":
                key_to_revoke = data.get("key", "")
                with _api_keys_lock:
                    if key_to_revoke in _api_keys_store:
                        _api_keys_store[key_to_revoke]["revoked"] = True
                        self._send_json({"action": "revoked", "key": key_to_revoke[:8] + "..."})
                    else:
                        self._send_json({"error": "Key not found"})
            elif action == "list":
                keys_list = []
                with _api_keys_lock:
                    for k, v in _api_keys_store.items():
                        keys_list.append({
                            "key_prefix": k[:8] + "...",
                            "tier": v.get("tier", "free"),
                            "label": v.get("label", ""),
                            "created": v.get("created", ""),
                            "revoked": v.get("revoked", False),
                        })
                self._send_json({"keys": keys_list, "total": len(keys_list)})
            else:
                self._send_json({"error": f"Unknown action '{action}'. Use: create, list, revoke"})
        else:
            self.send_error(404)

    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self.send_response(204)
        cors_origin = self._get_cors_origin()
        if cors_origin:
            self.send_header("Access-Control-Allow-Origin", cors_origin)
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Async")
        self.send_header("Access-Control-Max-Age", "86400")
        self.end_headers()

    def _serve_file(self, filepath, content_type):
        try:
            with open(filepath, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.end_headers()
            self.wfile.write(content)
        except FileNotFoundError:
            self.send_error(404)

    def _send_json(self, data):
        body = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each request in a separate thread so /api/generate (20-60s) does
    not block health checks, static files, or other concurrent requests.
    daemon_threads ensures threads don't block process shutdown."""
    daemon_threads = True
    # Allow port reuse to avoid "Address already in use" on quick restarts
    allow_reuse_address = True


if __name__ == "__main__":
    import signal

    port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 5001))
    server = ThreadedHTTPServer(("0.0.0.0", port), MediaPlanHandler)

    # ── Graceful shutdown on SIGTERM (Render.com sends SIGTERM) ──
    def _handle_signal(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info("Received %s -- initiating graceful shutdown", sig_name)
        if _shutdown:
            _shutdown.request_shutdown()
            _shutdown.wait_for_completion()
        server.shutdown()
        logger.info("Server shut down cleanly")

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # ── Pre-warm knowledge base on startup ──
    try:
        kb = load_knowledge_base()
        logger.info("Knowledge base pre-warmed: %d keys", len(kb))
    except Exception as kb_err:
        logger.error("Knowledge base pre-warm failed: %s", kb_err)

    # ── Startup banner ──
    logger.info("=" * 60)
    logger.info("AI Media Planner v2.2.0")
    logger.info("Port: %d | PID: %d | Threads: daemon", port, os.getpid())
    logger.info("Health: http://localhost:%d/health", port)
    logger.info("Readiness: http://localhost:%d/ready", port)
    logger.info("Metrics: http://localhost:%d/api/metrics", port)
    logger.info("PPTX: %s | API enrichment: %s",
                "available" if generate_pptx else "unavailable",
                "available" if enrich_data else "unavailable")
    logger.info("Data Matrix: %s", "monitoring" if _data_matrix else "unavailable")
    logger.info("AutoQC: %s", "running" if _auto_qc else "unavailable")
    logger.info("API Docs: http://localhost:%d/docs", port)
    logger.info("OpenAPI: http://localhost:%d/api/docs/openapi.json", port)
    logger.info("API Version: v1 | Async generation: enabled")
    logger.info("=" * 60)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received -- shutting down")
        server.shutdown()
    finally:
        logger.info("Server process exiting")
