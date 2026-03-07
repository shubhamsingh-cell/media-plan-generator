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
import urllib.request
import urllib.error
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.chart import BarChart, PieChart, DoughnutChart, Reference
from openpyxl.chart.label import DataLabelList
from openpyxl.chart.series import DataPoint
from openpyxl.drawing.image import Image as XlImage

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
    os.makedirs(os.path.dirname(REQUEST_LOG_LOCK), exist_ok=True)
    lock_fd = open(REQUEST_LOG_LOCK, "w")
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
        lock_fd.close()
    return entry

# Import research module for real data
sys.path.insert(0, BASE_DIR)
import research
try:
    from ppt_generator import generate_pptx
    print("ppt_generator loaded successfully", file=sys.stderr)
except ImportError as e:
    print(f"WARNING: ppt_generator import failed: {e}", file=sys.stderr)
    generate_pptx = None

try:
    from api_enrichment import enrich_data
    print("api_enrichment loaded successfully", file=sys.stderr)
except ImportError as e:
    print(f"WARNING: api_enrichment import failed: {e}", file=sys.stderr)
    enrich_data = None

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

def load_channels_db():
    with open(os.path.join(DATA_DIR, "channels_db.json"), "r") as f:
        return json.load(f)

def load_joveo_publishers():
    path = os.path.join(DATA_DIR, "joveo_publishers.json")
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
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

    db = load_channels_db()
    joveo_pubs = load_joveo_publishers()
    gs = global_supply_data  # global supply reference
    # Get global supply data via research module for international locations
    global_research = research.get_global_supply_data(
        data.get("locations", ["United States"]),
        data.get("industry", "general_entry_level"),
    )

    # Industry label mapping
    industry_label_map = {
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

    wb = Workbook()

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

    # ── LinkedIn-inspired color palette ──
    NAVY = "1B2A4A"
    BLUE = "0A66C9"
    MEDIUM_BLUE = "004082"
    LIGHT_BLUE = "D1E8FF"
    GOLD = "7C3AED"
    LIGHT_GOLD = "A78BFA"
    PALE_GOLD = "EDE9FE"
    OFF_WHITE = "F2F2F0"
    WARM_GRAY = "EBE6E0"
    GREEN_GOOD = "2E7D32"
    AMBER_WARN = "F57C00"

    # LinkedIn-style fills
    gold_fill = PatternFill(start_color=GOLD, end_color=GOLD, fill_type="solid")
    light_gold_fill = PatternFill(start_color=LIGHT_GOLD, end_color=LIGHT_GOLD, fill_type="solid")
    pale_gold_fill = PatternFill(start_color=PALE_GOLD, end_color=PALE_GOLD, fill_type="solid")
    off_white_fill = PatternFill(start_color=OFF_WHITE, end_color=OFF_WHITE, fill_type="solid")
    warm_gray_fill = PatternFill(start_color=WARM_GRAY, end_color=WARM_GRAY, fill_type="solid")
    light_blue_fill = PatternFill(start_color=LIGHT_BLUE, end_color=LIGHT_BLUE, fill_type="solid")
    green_fill = PatternFill(start_color="E8F5E9", end_color="E8F5E9", fill_type="solid")
    amber_fill = PatternFill(start_color="FFF3E0", end_color="FFF3E0", fill_type="solid")
    gold_accent_border = Border(
        left=Side(style="medium", color=GOLD),
        right=Side(style="thin", color=WARM_GRAY),
        top=Side(style="thin", color=WARM_GRAY),
        bottom=Side(style="thin", color=WARM_GRAY),
    )
    gold_bottom_border = Border(bottom=Side(style="medium", color=GOLD))

    def style_section_header(ws, row, col_start, col_end, title):
        """Style a section header with navy text and purple accent border."""
        ws.merge_cells(start_row=row, start_column=col_start, end_row=row, end_column=col_end)
        cell = ws.cell(row=row, column=col_start, value=title)
        cell.font = Font(name="Calibri", bold=True, size=14, color=NAVY)
        cell.border = Border(
            bottom=Side(style="medium", color=GOLD),
            left=Side(style="thick", color=GOLD),
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
    roles = data.get("target_roles") or data.get("roles", [])

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
    title_cell.value = "AI Media Planner — Overview"
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
        ("Client's Use Case", data.get("use_case", "")),
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
        left=Side(style="medium", color=GOLD),
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

    # Purple accent subtitle bar
    ws_exec.merge_cells("B3:G3")
    ws_exec["B3"].value = f"{industry_label_val}  |  Generated {datetime.datetime.now().strftime('%B %d, %Y')}"
    ws_exec["B3"].font = Font(name="Calibri", bold=True, size=11, color=NAVY)
    ws_exec["B3"].fill = gold_fill
    ws_exec["B3"].alignment = Alignment(horizontal="center", vertical="center")
    for c in range(3, 8):
        ws_exec.cell(row=3, column=c).fill = gold_fill
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

    # Campaign Snapshot section with purple accent
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
    hero_cell.font = Font(name="Calibri", bold=True, size=24, color=GOLD)
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

    # 2x3 metric cards with off-white backgrounds and purple value highlights
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
        ws_exec.cell(row=exec_row, column=2, value=f"Live API data sourced from: {apis_used} (fetched {enrichment_summary.get('total_time_seconds', 0):.1f}s ago)").font = Font(name="Calibri", italic=True, size=9, color="7C3AED")

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
                    ws_exec.cell(row=exec_row, column=c).fill = pale_gold_fill

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
    def _parse_budget_midpoint(bstr):
        """Extract a numeric midpoint from budget range strings like '$50,000 - $250,000'."""
        if not bstr:
            return 100000  # default fallback
        nums = re.findall(r'[\d,]+', bstr.replace(",", ""))
        # re-parse with commas removed
        nums = re.findall(r'[\d]+', bstr.replace(",", ""))
        parsed = [int(n) for n in nums if int(n) >= 1000]  # filter out small numbers
        if len(parsed) >= 2:
            return (parsed[0] + parsed[1]) / 2  # midpoint of range
        elif len(parsed) == 1:
            return parsed[0]
        # Handle text-based values
        bstr_lower = bstr.lower()
        if "million" in bstr_lower or "1m" in bstr_lower:
            return 1000000
        if "500k" in bstr_lower or "500,000" in bstr_lower:
            return 500000
        return 100000  # safe default

    budget_midpoint = _parse_budget_midpoint(budget_range_str)

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

        # Alternating row fill with pale purple
        if idx % 2 == 0:
            for c in range(2, 6):
                ws_exec.cell(row=exec_row, column=c).fill = pale_gold_fill

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
    }
    _DEFAULT_ALLOC = {"programmatic_dsp": 35, "global_boards": 20, "niche_boards": 15, "social_media": 12, "regional_boards": 8, "employer_branding": 5, "apac_regional": 3, "emea_regional": 2}
    _ind_key = data.get("industry", "general_entry_level")
    _ap = dict(_INDUSTRY_ALLOC.get(_ind_key, _DEFAULT_ALLOC))

    # Budget-size adjustment
    _bstr = str(data.get("budget", "") or "")
    try:
        _bnums = re.findall(r'[\d]+', _bstr.replace(",", "").replace("$", "").strip())
        _bval = int(_bnums[0]) if _bnums and int(_bnums[0]) >= 1000 else None
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
        channel_allocations.append(("Niche & Industry Boards", _ap["niche_boards"], _niche_d, "#7030A0", "Medium-High"))
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
                ws_exec.cell(row=exec_row, column=c).fill = pale_gold_fill
        exec_row += 1

    # ── Donut Chart for Channel Distribution ──
    if normalized:
        exec_row += 2
        # Write chart data in a hidden area (columns H-I) for the donut chart reference
        donut_data_start = exec_row
        ws_exec.cell(row=exec_row, column=8, value="Channel").font = Font(name="Calibri", bold=True, size=9, color="999999")
        ws_exec.cell(row=exec_row, column=9, value="Allocation %").font = Font(name="Calibri", bold=True, size=9, color="999999")
        exec_row += 1
        donut_colors = [NAVY, BLUE, GOLD, LIGHT_BLUE, WARM_GRAY, MEDIUM_BLUE, LIGHT_GOLD, "A8D4FF"]
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
        ("2.2x", "Faster Promotion Rate", "Employer brand-influenced hires show 2.2x higher promotion rates, indicating better role fit and long-term alignment.", "#7030A0"),
        ("1.4x", "Higher Demand Talent", "Brand-influenced hires are 1.4x more likely to be in-demand candidates (higher InMail volume), signaling you're attracting competitive talent.", "#ED7D31"),
        ("82%", "First-Year Retention", "Brand-engaged hires show stronger first-year retention rates, reducing costly early-stage turnover and rehiring costs.", "#1B6B3A"),
    ]

    for idx, (metric, label, desc, color) in enumerate(eb_metrics):
        # Metric value in large font with purple background
        c1 = ws_exec.cell(row=exec_row, column=2, value=metric)
        c1.font = Font(name="Calibri", bold=True, size=18, color=NAVY)
        c1.fill = gold_fill
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
    rec_cell.fill = light_gold_fill
    rec_cell.border = Border(left=Side(style="thick", color=GOLD), bottom=Side(style="thin", color=GOLD))
    for c in range(3, 8):
        ws_exec.cell(row=exec_row, column=c).fill = light_gold_fill

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
        c4.font = Font(name="Calibri", size=12, color=GOLD)
        c4.border = thin_border
        c4.alignment = center_alignment

        c5 = ws_exec.cell(row=exec_row, column=6, value=best_for)
        c5.font = Font(name="Calibri", italic=True, size=9, color="596780")
        c5.border = thin_border
        c5.alignment = wrap_alignment

        if is_joveo:
            for c in range(2, 7):
                ws_exec.cell(row=exec_row, column=c).fill = light_gold_fill
        elif idx % 2 == 0:
            for c in range(2, 7):
                ws_exec.cell(row=exec_row, column=c).fill = off_white_fill
        exec_row += 1

    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    joveo_rec = ws_exec.cell(row=exec_row, column=2, value="  Joveo's programmatic approach combines the speed of automation with quality optimization (CPQA), delivering the best balance of time-to-fill, retention, and cost efficiency.")
    joveo_rec.font = Font(name="Calibri", bold=True, italic=True, size=10, color=NAVY)
    joveo_rec.fill = light_gold_fill
    joveo_rec.border = Border(left=Side(style="thick", color=GOLD))
    for c in range(3, 8):
        ws_exec.cell(row=exec_row, column=c).fill = light_gold_fill

    # ── Quality & ROI Metrics (2x2 card grid) ──
    exec_row += 3
    style_section_header(ws_exec, exec_row, 2, 7, "Quality & ROI Metrics")
    exec_row += 1
    ws_exec.merge_cells(f"B{exec_row}:G{exec_row}")
    ws_exec.cell(row=exec_row, column=2, value="Key performance indicators estimated from your channel mix, industry benchmarks, and campaign parameters.").font = Font(name="Calibri", italic=True, size=9, color="596780")
    exec_row += 2

    # Calculate estimated metrics
    est_total_reach = est_impressions
    num_channels = regional_count + niche_count + global_count
    est_hires = funnel_stages[-1][1] if funnel_stages else 0
    est_applies = funnel_stages[2][1] if len(funnel_stages) > 2 else 0
    est_cpa = round(budget_midpoint / max(est_applies, 1), 2) if est_applies > 0 else 0  # budget-derived CPA
    channel_diversity = min(round(num_channels / 10 * 100, 0), 100)  # score out of 100
    est_cph_display = round(budget_midpoint / max(est_hires, 1), 0) if est_hires > 0 else avg_cph

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
            num_cell.fill = gold_fill
            num_cell.alignment = Alignment(horizontal="center", vertical="center")
            num_cell.border = card_border
            for cc in range(col_start + 1, col_start + 3):
                ws_exec.cell(row=exec_row, column=cc).fill = gold_fill
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
            # Client row: purple highlight with navy text
            c1.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
            c1.fill = gold_fill
        else:
            c1.font = Font(name="Calibri", bold=False, size=10, color=NAVY)

        fields = [pdata["cpa"], pdata["cpc"], pdata["cph"], pdata["apply_rate"], pdata["difficulty"]]
        for fi, fval in enumerate(fields):
            cell = ws_exec.cell(row=exec_row, column=3 + fi, value=fval)
            cell.border = thin_border
            cell.alignment = center_alignment
            if is_client:
                cell.font = Font(name="Calibri", bold=True, size=10, color=NAVY)
                cell.fill = gold_fill
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
    ws_exec.cell(row=exec_row, column=2, value="Your industry row is highlighted in purple. \u25B2 = easier than average (green), \u25BC = harder than average (amber). Use this to calibrate budget expectations.").font = Font(name="Calibri", italic=True, size=8, color="999999")

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
    ws_trends["B2"].border = gold_bottom_border

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
    ws_strategy["B2"].border = gold_bottom_border

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
            ws_strategy.cell(row=row, column=6).font = Font(name="Calibri", size=10, color="7030A0")
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
        cell.fill = PatternFill(start_color="7030A0", end_color="7030A0", fill_type="solid")
        cell.alignment = center_alignment
        cell.border = thin_border
    row += 1

    for _nch in _niche_channels_for_industry:
        style_body_cell(ws_strategy, row, 2, _nch)
        ws_strategy.cell(row=row, column=2).font = Font(name="Calibri", bold=True, size=10, color="7030A0")
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
    ws_trad["B2"].border = gold_bottom_border

    roles_str = f" | Target Roles: {', '.join(roles[:5])}" if roles else ""
    _trad_pubs = joveo_pubs.get('total_active_publishers', 1238)
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
    ws_nontrad["B2"].border = gold_bottom_border

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
        ws_global["B2"].border = gold_bottom_border
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
        ws_dei.sheet_properties.tabColor = "7030A0"
        ws_dei.column_dimensions["A"].width = 5
        ws_dei.column_dimensions["B"].width = 30
        ws_dei.column_dimensions["C"].width = 35
        ws_dei.column_dimensions["D"].width = 25
        ws_dei.column_dimensions["E"].width = 30

        ws_dei.merge_cells("B2:E2")
        ws_dei["B2"].value = "DEI & Diversity Channels"
        ws_dei["B2"].font = Font(name="Calibri", bold=True, size=16, color=NAVY)
        ws_dei["B2"].border = gold_bottom_border
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
        ws_innov["B2"].border = gold_bottom_border
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
        ws_budget["B2"].border = gold_bottom_border

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
        if cpa_from_db:
            cpa_benchmarks = [
                (region_name, region_info.get("range", "N/A"), region_info.get("notes", ""))
                for region_name, region_info in cpa_from_db.items()
                if isinstance(region_info, dict)
            ]
        else:
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
            if any(kw in location_str for kw in keywords):
                filtered_benchmarks.append((region, cpa_range, notes))
        # Default to North America if nothing matched
        if not filtered_benchmarks:
            filtered_benchmarks = [b for b in cpa_benchmarks if "North America" in b[0]]
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
                ("Niche / Specialized", rec_ch.get("niche", []), "7030A0"),
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
    ws_timeline["B2"].border = gold_bottom_border

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
        "Phase 4": PatternFill(start_color="E4D1F0", end_color="E4D1F0", fill_type="solid"),  # purple
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
        ws_edu["B2"].border = gold_bottom_border
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
        ws_events["B2"].border = gold_bottom_border
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
        ws_radio["B2"].border = gold_bottom_border
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
        ws_media["B2"].border = gold_bottom_border

        ws_media.merge_cells("B3:G3")
        ws_media["B3"].value = f"Recommended recruitment marketing channels for {ind_label} roles in {loc_context}. These platforms complement digital programmatic channels to reach passive candidates and build employer brand."
        ws_media["B3"].font = Font(name="Calibri", italic=True, size=9, color="596780")
        ws_media["B3"].alignment = Alignment(wrap_text=True, vertical="top")

        category_labels = {
            "trade_publications": ("📰 TRADE PUBLICATIONS & JOURNALS", "2E75B6"),
            "digital_media": ("💻 DIGITAL MEDIA PLATFORMS", "1B6B3A"),
            "ooh_print": ("🏗️ OUT-OF-HOME & PRINT", "ED7D31"),
            "broadcast_audio": ("🎙️ BROADCAST & AUDIO", "7030A0"),
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

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()


ADMIN_API_KEY = os.environ.get("ADMIN_API_KEY", "")

# Simple in-memory rate limiter
from collections import defaultdict
_rate_limit_store = defaultdict(list)
_RATE_LIMIT_WINDOW = 60   # seconds
_RATE_LIMIT_MAX = 10       # requests per window per IP

class MediaPlanHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {format % args}", file=sys.stderr)

    def end_headers(self):
        """Add security headers to all responses."""
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
        super().end_headers()

    def _check_admin_auth(self):
        """Check for admin API key in query params or Authorization header."""
        if not ADMIN_API_KEY:
            return True  # No key configured = development mode
        parsed = urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        key = params.get("key", [None])[0]
        if not key:
            auth = self.headers.get("Authorization", "")
            if auth.startswith("Bearer "):
                key = auth[7:]
        return key == ADMIN_API_KEY

    def _check_rate_limit(self):
        """Simple per-IP rate limiting for generate endpoint."""
        client_ip = self.client_address[0]
        now = time.time()
        _rate_limit_store[client_ip] = [
            t for t in _rate_limit_store[client_ip]
            if now - t < _RATE_LIMIT_WINDOW
        ]
        if len(_rate_limit_store[client_ip]) >= _RATE_LIMIT_MAX:
            return False
        _rate_limit_store[client_ip].append(now)
        return True

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "":
            self._serve_file(os.path.join(TEMPLATES_DIR, "index.html"), "text/html")
        elif parsed.path == "/api/health":
            self._send_json({
                "status": "ok",
                "version": "2.1.0",
                "pptx_available": generate_pptx is not None,
                "timestamp": datetime.datetime.now().isoformat()
            })
        elif parsed.path == "/api/channels":
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
        elif parsed.path == "/api/requests":
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
        elif parsed.path == "/dashboard":
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
        elif parsed.path == "/api/documents":
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
        elif parsed.path.startswith("/api/documents/"):
            fname = parsed.path.split("/")[-1]
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
                    print(f"Document read error: {e}", file=sys.stderr)
                    self.wfile.write(json.dumps({"error": "Failed to read document"}).encode())
            else:
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Document not found"}).encode())
        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/generate":
            if not self._check_rate_limit():
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Rate limit exceeded. Please try again in a minute."}).encode())
                return
            try:
                content_len = int(self.headers.get("Content-Length", 0))
            except (ValueError, TypeError):
                content_len = 0
            if content_len > 10 * 1024 * 1024:  # 10MB limit
                self.send_response(413)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Request too large"}).encode())
                return
            body = self.rfile.read(content_len)
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
                return

            # Validate required fields
            client_name_input = (data.get("client_name") or "").strip()
            if not client_name_input:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Client name is required."}).encode())
                return
            data["client_name"] = client_name_input

            # ── P0 Fix: Capture timeline, hire_volume, and notes from input ──
            # campaign_duration: normalize from frontend dropdown or API input
            raw_duration = str(data.get("campaign_duration") or data.get("timeline") or "").strip()
            if raw_duration:
                data["campaign_duration"] = raw_duration
            else:
                data["campaign_duration"] = "Not specified"

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

            # Enrich data with live API data
            enriched = {}
            if enrich_data is not None:
                try:
                    enriched = enrich_data(data)
                    data["_enriched"] = enriched
                    print(f"API enrichment complete: {enriched.get('enrichment_summary', {})}", file=sys.stderr)
                except Exception as e:
                    print(f"API enrichment failed (non-fatal): {e}", file=sys.stderr)
                    data["_enriched"] = {}
            else:
                data["_enriched"] = {}

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

            start_time = time.time()
            try:
                excel_bytes = generate_excel(data)
            except Exception as e:
                import traceback
                tb = traceback.format_exc()
                print(f"Excel generation error: {tb}", file=sys.stderr)
                generation_time = time.time() - start_time
                log_request(data, "error", generation_time=generation_time, error_msg=str(e))
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Generation failed. Please check your inputs and try again."}).encode())
                return

            import re as _re
            # Sanitize client_name to ASCII-safe characters (prevents CJK/Unicode crashes in filenames/headers)
            client_name = _re.sub(r'[^a-zA-Z0-9_\-]', '_', data.get("client_name") or "Client")

            # Generate Strategy PPT deck
            pptx_bytes = None
            if generate_pptx is not None:
                try:
                    pptx_bytes = generate_pptx(data)
                    print(f"PPT generated: {len(pptx_bytes)} bytes", file=sys.stderr)
                except Exception as e:
                    import traceback
                    print(f"PPT generation error: {e}", file=sys.stderr)
                    traceback.print_exc(file=sys.stderr)
                    pptx_bytes = None
            else:
                print("PPT generation skipped: ppt_generator not available", file=sys.stderr)

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
                    print(f"Document saved: {doc_filename} ({len(zip_bytes)} bytes)", file=sys.stderr)
                    _cleanup_old_docs(docs_dir)
                except Exception as doc_err:
                    print(f"WARNING: Could not save document copy: {doc_err}", file=sys.stderr)
                    doc_filename = None

                self.send_response(200)
                self.send_header("Content-Type", "application/zip")
                self.send_header("Content-Disposition", f'attachment; filename="{client_name}_Media_Plan_Bundle.zip"')
                self.send_header("Content-Length", str(len(zip_bytes)))
                self.end_headers()
                self.wfile.write(zip_bytes)
                generation_time = time.time() - start_time
                log_request(data, "success", file_size=len(zip_bytes), generation_time=generation_time, doc_filename=doc_filename)
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
                    print(f"Document saved (Excel only): {doc_filename}", file=sys.stderr)
                except Exception as doc_err:
                    print(f"WARNING: Could not save document copy: {doc_err}", file=sys.stderr)
                    doc_filename = None

                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                self.send_header("Content-Disposition", f'attachment; filename="{client_name}_Media_Plan.xlsx"')
                self.send_header("Content-Length", str(len(excel_bytes)))
                self.end_headers()
                self.wfile.write(excel_bytes)
                generation_time = time.time() - start_time
                log_request(data, "success", file_size=len(excel_bytes), generation_time=generation_time, doc_filename=doc_filename)
        else:
            self.send_error(404)

    def _serve_file(self, filepath, content_type):
        try:
            with open(filepath, "rb") as f:
                content = f.read()
            self.send_response(200)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", sys.argv[1] if len(sys.argv) > 1 else 5001))
    server = HTTPServer(("0.0.0.0", port), MediaPlanHandler)
    print(f"AI Media Planner running at http://localhost:{port}", file=sys.stderr)
    server.serve_forever()
