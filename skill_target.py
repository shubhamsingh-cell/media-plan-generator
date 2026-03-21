#!/usr/bin/env python3
"""
skill_target.py -- Skill-Based Targeting Engine for Recruitment Advertising

Helps recruiters identify the right job boards and channels based on the
specific skills required for a role. Maps 50+ skills to optimal recruitment
channels with rarity scoring, budget allocation, and collar intelligence.

Capabilities:
  - Skill categorization (technical, soft, certifications, domain)
  - Channel recommendations with match scores per skill cluster
  - Platform-specific targeting (LinkedIn, Indeed, GitHub Jobs, etc.)
  - Skill rarity scoring (common, moderate, rare, unicorn)
  - Budget allocation across recommended channels
  - Competitor insight for similar skill profiles
  - Talent pool estimation
  - Collar type classification integration
  - Excel & PowerPoint branded report generation

Design tokens:
    UI:    Deep Obsidian (bg #0a0a0f, cards rgba(15,15,25,0.8), accent #6366f1)
    Excel: Sapphire Blue (Navy #0F172A, Accent #2563EB, Light #DBEAFE, Calibri, col B)
    PPT:   Joveo brand (Port Gore #202058, Blue Violet #5A54BD, Downy #6BB3CD)

Thread-safe, stdlib-only core. Never crashes (all errors return structured dicts).
"""

from __future__ import annotations

import io
import json
import logging
import math
import re
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Optional imports (lazy, try/except) ──────────────────────────────────────

try:
    from collar_intelligence import classify_collar as _classify_collar
    _HAS_COLLAR = True
except ImportError:
    _classify_collar = None
    _HAS_COLLAR = False

try:
    from shared_utils import INDUSTRY_LABEL_MAP, parse_budget
except ImportError:
    INDUSTRY_LABEL_MAP = {}
    def parse_budget(v, *, default=100_000.0):
        try:
            return float(v)
        except Exception:
            return default

try:
    import research as _research
    _HAS_RESEARCH = True
except ImportError:
    _research = None
    _HAS_RESEARCH = False


# =============================================================================
# CONSTANTS & DESIGN TOKENS
# =============================================================================

# Excel palette - Sapphire Blue
_NAVY = "0F172A"
_SAPPHIRE = "2563EB"
_BLUE_LIGHT = "DBEAFE"
_BLUE_PALE = "EFF6FF"
_WHITE = "FFFFFF"
_GREEN = "16A34A"
_GREEN_BG = "DCFCE7"
_AMBER = "D97706"
_AMBER_BG = "FEF3C7"
_RED = "DC2626"
_RED_BG = "FEE2E2"
_MUTED = "78716C"
_WARM_GRAY = "E7E5E4"
_STONE = "1C1917"

COL_START = 2  # Column B

# UI Obsidian palette (for reference / JSON responses)
_UI_BG = "#0a0a0f"
_UI_CARD = "rgba(15,15,25,0.8)"
_UI_ACCENT = "#6366f1"


# =============================================================================
# SKILL KNOWLEDGE BASE (50+ skills)
# =============================================================================

# Skill rarity tiers
RARITY_COMMON = "common"         # >500k professionals
RARITY_MODERATE = "moderate"     # 100k-500k
RARITY_RARE = "rare"             # 10k-100k
RARITY_UNICORN = "unicorn"       # <10k

# Skill categories
CAT_TECHNICAL = "technical"
CAT_SOFT = "soft_skill"
CAT_CERTIFICATION = "certification"
CAT_DOMAIN = "domain_knowledge"

# Channel IDs
CH_LINKEDIN = "LinkedIn"
CH_INDEED = "Indeed"
CH_GITHUB = "GitHub Jobs"
CH_STACKOVERFLOW = "Stack Overflow Jobs"
CH_DICE = "Dice"
CH_GLASSDOOR = "Glassdoor"
CH_ZIPRECRUITER = "ZipRecruiter"
CH_MONSTER = "Monster"
CH_ANGELLIST = "AngelList/Wellfound"
CH_HIRED = "Hired"
CH_TRIPLEBYTE = "Triplebyte"
CH_TOPTAL = "Toptal"
CH_BEHANCE = "Behance"
CH_DRIBBBLE = "Dribbble"
CH_FLEXJOBS = "FlexJobs"
CH_WEWORKREMOTELY = "We Work Remotely"
CH_BUILTIN = "Built In"
CH_CAREERBUILDER = "CareerBuilder"
CH_SNAGAJOB = "Snagajob"
CH_NURSE_COM = "Nurse.com"
CH_HEALTHCAREJOBSITE = "HealthcareJobSite"
CH_CLEARANCEJOBS = "ClearanceJobs"
CH_USAJOBS = "USAJobs"
CH_HANDSHAKE = "Handshake"
CH_FACEBOOK = "Facebook Jobs"
CH_GOOGLE_ADS = "Google Ads (PPC)"
CH_PROGRAMMATIC = "Programmatic (Joveo/Appcast)"

# Master skill database: skill_name -> metadata
SKILL_DATABASE: Dict[str, Dict[str, Any]] = {
    # ── Technical / Engineering ──
    "python": {
        "category": CAT_TECHNICAL, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_DICE, CH_HIRED],
        "talent_pool_base": 800_000, "demand_trend": "growing",
    },
    "javascript": {
        "category": CAT_TECHNICAL, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_BUILTIN],
        "talent_pool_base": 1_200_000, "demand_trend": "stable",
    },
    "typescript": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_BUILTIN],
        "talent_pool_base": 450_000, "demand_trend": "growing",
    },
    "java": {
        "category": CAT_TECHNICAL, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_DICE, CH_STACKOVERFLOW],
        "talent_pool_base": 900_000, "demand_trend": "stable",
    },
    "c++": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_DICE, CH_STACKOVERFLOW, CH_CLEARANCEJOBS],
        "talent_pool_base": 350_000, "demand_trend": "stable",
    },
    "rust": {
        "category": CAT_TECHNICAL, "rarity": RARITY_RARE,
        "channels": [CH_GITHUB, CH_STACKOVERFLOW, CH_HIRED, CH_BUILTIN],
        "talent_pool_base": 45_000, "demand_trend": "growing",
    },
    "golang": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_GITHUB, CH_STACKOVERFLOW, CH_HIRED, CH_LINKEDIN],
        "talent_pool_base": 180_000, "demand_trend": "growing",
    },
    "react": {
        "category": CAT_TECHNICAL, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_BUILTIN],
        "talent_pool_base": 650_000, "demand_trend": "stable",
    },
    "angular": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_DICE, CH_STACKOVERFLOW],
        "talent_pool_base": 280_000, "demand_trend": "declining",
    },
    "vue.js": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_BUILTIN],
        "talent_pool_base": 200_000, "demand_trend": "growing",
    },
    "node.js": {
        "category": CAT_TECHNICAL, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_BUILTIN],
        "talent_pool_base": 700_000, "demand_trend": "stable",
    },
    "aws": {
        "category": CAT_TECHNICAL, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_DICE, CH_INDEED, CH_HIRED],
        "talent_pool_base": 600_000, "demand_trend": "growing",
    },
    "azure": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_DICE, CH_INDEED, CH_GLASSDOOR],
        "talent_pool_base": 400_000, "demand_trend": "growing",
    },
    "gcp": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_DICE, CH_GITHUB, CH_HIRED],
        "talent_pool_base": 200_000, "demand_trend": "growing",
    },
    "kubernetes": {
        "category": CAT_TECHNICAL, "rarity": RARITY_RARE,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_HIRED],
        "talent_pool_base": 95_000, "demand_trend": "growing",
    },
    "docker": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_DICE],
        "talent_pool_base": 350_000, "demand_trend": "stable",
    },
    "terraform": {
        "category": CAT_TECHNICAL, "rarity": RARITY_RARE,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_HIRED],
        "talent_pool_base": 80_000, "demand_trend": "growing",
    },
    "machine_learning": {
        "category": CAT_TECHNICAL, "rarity": RARITY_RARE,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_HIRED, CH_TOPTAL],
        "talent_pool_base": 70_000, "demand_trend": "growing",
    },
    "deep_learning": {
        "category": CAT_TECHNICAL, "rarity": RARITY_RARE,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_HIRED, CH_TOPTAL],
        "talent_pool_base": 35_000, "demand_trend": "growing",
    },
    "llm_ai": {
        "category": CAT_TECHNICAL, "rarity": RARITY_UNICORN,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_HIRED, CH_TOPTAL, CH_ANGELLIST],
        "talent_pool_base": 8_000, "demand_trend": "explosive",
    },
    "data_engineering": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_DICE, CH_GITHUB, CH_HIRED],
        "talent_pool_base": 180_000, "demand_trend": "growing",
    },
    "sql": {
        "category": CAT_TECHNICAL, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_DICE, CH_GLASSDOOR],
        "talent_pool_base": 1_500_000, "demand_trend": "stable",
    },
    "cybersecurity": {
        "category": CAT_TECHNICAL, "rarity": RARITY_RARE,
        "channels": [CH_LINKEDIN, CH_DICE, CH_CLEARANCEJOBS, CH_INDEED],
        "talent_pool_base": 65_000, "demand_trend": "growing",
    },
    "devops": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_GITHUB, CH_STACKOVERFLOW, CH_DICE],
        "talent_pool_base": 250_000, "demand_trend": "growing",
    },
    "ios_swift": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_STACKOVERFLOW, CH_HIRED, CH_BUILTIN],
        "talent_pool_base": 220_000, "demand_trend": "stable",
    },
    "android_kotlin": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_STACKOVERFLOW, CH_HIRED, CH_BUILTIN],
        "talent_pool_base": 280_000, "demand_trend": "stable",
    },
    "salesforce": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_DICE, CH_GLASSDOOR],
        "talent_pool_base": 300_000, "demand_trend": "stable",
    },
    "sap": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_DICE, CH_MONSTER],
        "talent_pool_base": 250_000, "demand_trend": "stable",
    },
    "ui_ux_design": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_DRIBBBLE, CH_BEHANCE, CH_BUILTIN],
        "talent_pool_base": 350_000, "demand_trend": "stable",
    },
    "figma": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_DRIBBBLE, CH_BEHANCE, CH_BUILTIN],
        "talent_pool_base": 300_000, "demand_trend": "growing",
    },
    "product_management": {
        "category": CAT_DOMAIN, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_BUILTIN, CH_GLASSDOOR, CH_ANGELLIST],
        "talent_pool_base": 400_000, "demand_trend": "stable",
    },
    # ── Trades / Blue collar ──
    "welding": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_INDEED, CH_SNAGAJOB, CH_ZIPRECRUITER, CH_FACEBOOK],
        "talent_pool_base": 420_000, "demand_trend": "growing",
    },
    "hvac": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_INDEED, CH_ZIPRECRUITER, CH_SNAGAJOB, CH_FACEBOOK],
        "talent_pool_base": 380_000, "demand_trend": "growing",
    },
    "cdl_driving": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_COMMON,
        "channels": [CH_INDEED, CH_ZIPRECRUITER, CH_SNAGAJOB, CH_FACEBOOK, CH_PROGRAMMATIC],
        "talent_pool_base": 900_000, "demand_trend": "growing",
    },
    "forklift_operation": {
        "category": CAT_TECHNICAL, "rarity": RARITY_COMMON,
        "channels": [CH_INDEED, CH_SNAGAJOB, CH_ZIPRECRUITER, CH_FACEBOOK],
        "talent_pool_base": 700_000, "demand_trend": "stable",
    },
    "electrical_work": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_INDEED, CH_ZIPRECRUITER, CH_SNAGAJOB, CH_FACEBOOK],
        "talent_pool_base": 350_000, "demand_trend": "growing",
    },
    "plumbing": {
        "category": CAT_TECHNICAL, "rarity": RARITY_MODERATE,
        "channels": [CH_INDEED, CH_ZIPRECRUITER, CH_SNAGAJOB, CH_FACEBOOK],
        "talent_pool_base": 320_000, "demand_trend": "growing",
    },
    "cnc_machining": {
        "category": CAT_TECHNICAL, "rarity": RARITY_RARE,
        "channels": [CH_INDEED, CH_ZIPRECRUITER, CH_MONSTER, CH_PROGRAMMATIC],
        "talent_pool_base": 90_000, "demand_trend": "growing",
    },
    # ── Healthcare / Grey collar ──
    "nursing_rn": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_COMMON,
        "channels": [CH_INDEED, CH_NURSE_COM, CH_HEALTHCAREJOBSITE, CH_LINKEDIN],
        "talent_pool_base": 3_100_000, "demand_trend": "growing",
    },
    "nursing_lpn": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_COMMON,
        "channels": [CH_INDEED, CH_NURSE_COM, CH_HEALTHCAREJOBSITE, CH_ZIPRECRUITER],
        "talent_pool_base": 700_000, "demand_trend": "growing",
    },
    "medical_coding": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_MODERATE,
        "channels": [CH_INDEED, CH_LINKEDIN, CH_HEALTHCAREJOBSITE, CH_FLEXJOBS],
        "talent_pool_base": 210_000, "demand_trend": "stable",
    },
    "pharmacy": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_MODERATE,
        "channels": [CH_INDEED, CH_LINKEDIN, CH_HEALTHCAREJOBSITE, CH_GLASSDOOR],
        "talent_pool_base": 320_000, "demand_trend": "stable",
    },
    # ── Certifications ──
    "pmp": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_DICE, CH_GLASSDOOR],
        "talent_pool_base": 400_000, "demand_trend": "stable",
    },
    "cpa": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR, CH_CAREERBUILDER],
        "talent_pool_base": 670_000, "demand_trend": "stable",
    },
    "cissp": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_RARE,
        "channels": [CH_LINKEDIN, CH_DICE, CH_CLEARANCEJOBS, CH_INDEED],
        "talent_pool_base": 55_000, "demand_trend": "growing",
    },
    "aws_certified": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_DICE, CH_INDEED, CH_HIRED],
        "talent_pool_base": 300_000, "demand_trend": "growing",
    },
    "six_sigma": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR, CH_MONSTER],
        "talent_pool_base": 350_000, "demand_trend": "declining",
    },
    "scrum_master": {
        "category": CAT_CERTIFICATION, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR, CH_BUILTIN],
        "talent_pool_base": 280_000, "demand_trend": "stable",
    },
    # ── Soft skills ──
    "leadership": {
        "category": CAT_SOFT, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR],
        "talent_pool_base": 5_000_000, "demand_trend": "stable",
    },
    "communication": {
        "category": CAT_SOFT, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR],
        "talent_pool_base": 8_000_000, "demand_trend": "stable",
    },
    "project_management": {
        "category": CAT_SOFT, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR, CH_BUILTIN],
        "talent_pool_base": 2_000_000, "demand_trend": "stable",
    },
    "negotiation": {
        "category": CAT_SOFT, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR],
        "talent_pool_base": 400_000, "demand_trend": "stable",
    },
    "critical_thinking": {
        "category": CAT_SOFT, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR],
        "talent_pool_base": 3_000_000, "demand_trend": "stable",
    },
    # ── Domain knowledge ──
    "financial_analysis": {
        "category": CAT_DOMAIN, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR, CH_CAREERBUILDER],
        "talent_pool_base": 450_000, "demand_trend": "stable",
    },
    "regulatory_compliance": {
        "category": CAT_DOMAIN, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR, CH_CAREERBUILDER],
        "talent_pool_base": 320_000, "demand_trend": "growing",
    },
    "supply_chain": {
        "category": CAT_DOMAIN, "rarity": RARITY_MODERATE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_GLASSDOOR, CH_MONSTER],
        "talent_pool_base": 380_000, "demand_trend": "growing",
    },
    "clinical_research": {
        "category": CAT_DOMAIN, "rarity": RARITY_RARE,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_HEALTHCAREJOBSITE, CH_GLASSDOOR],
        "talent_pool_base": 85_000, "demand_trend": "growing",
    },
    "digital_marketing": {
        "category": CAT_DOMAIN, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_BUILTIN, CH_GLASSDOOR],
        "talent_pool_base": 600_000, "demand_trend": "stable",
    },
    "content_writing": {
        "category": CAT_DOMAIN, "rarity": RARITY_COMMON,
        "channels": [CH_LINKEDIN, CH_INDEED, CH_FLEXJOBS, CH_WEWORKREMOTELY],
        "talent_pool_base": 500_000, "demand_trend": "stable",
    },
}

# ── Aliases for fuzzy matching ──
_SKILL_ALIASES: Dict[str, str] = {
    "py": "python", "python3": "python", "python 3": "python",
    "js": "javascript", "es6": "javascript", "ecmascript": "javascript",
    "ts": "typescript",
    "c sharp": "java", "c#": "java",
    "go": "golang", "go lang": "golang",
    "k8s": "kubernetes", "kube": "kubernetes",
    "tf": "terraform",
    "ml": "machine_learning", "ai/ml": "machine_learning",
    "ai": "llm_ai", "llm": "llm_ai", "gen ai": "llm_ai", "genai": "llm_ai",
    "large language model": "llm_ai", "generative ai": "llm_ai",
    "dl": "deep_learning", "neural networks": "deep_learning",
    "ux": "ui_ux_design", "ux design": "ui_ux_design", "ui design": "ui_ux_design",
    "product": "product_management", "pm": "product_management",
    "rn": "nursing_rn", "registered nurse": "nursing_rn",
    "lpn": "nursing_lpn", "licensed practical nurse": "nursing_lpn",
    "truck driving": "cdl_driving", "cdl": "cdl_driving",
    "forklift": "forklift_operation",
    "electrician": "electrical_work", "electrical": "electrical_work",
    "plumber": "plumbing",
    "cnc": "cnc_machining", "machining": "cnc_machining",
    "welder": "welding",
    "security": "cybersecurity", "infosec": "cybersecurity",
    "agile": "scrum_master",
    "lean": "six_sigma",
    "accounting": "cpa",
    "compliance": "regulatory_compliance",
    "react.js": "react", "reactjs": "react",
    "angular.js": "angular", "angularjs": "angular",
    "vuejs": "vue.js", "vue": "vue.js",
    "nodejs": "node.js", "node": "node.js",
    "amazon web services": "aws",
    "google cloud": "gcp", "google cloud platform": "gcp",
    "microsoft azure": "azure",
}


# ── Channel metadata ──
CHANNEL_METADATA: Dict[str, Dict[str, Any]] = {
    CH_LINKEDIN: {
        "type": "professional_network", "avg_cpc": 5.50, "avg_cpa": 45.0,
        "best_for": ["white_collar", "grey_collar"],
        "targeting_options": ["skills", "job_title", "company", "industry", "seniority", "education"],
    },
    CH_INDEED: {
        "type": "job_board", "avg_cpc": 0.50, "avg_cpa": 18.0,
        "best_for": ["blue_collar", "white_collar", "grey_collar", "pink_collar"],
        "targeting_options": ["job_title", "location", "salary_range", "experience_level"],
    },
    CH_GITHUB: {
        "type": "developer_community", "avg_cpc": 4.00, "avg_cpa": 55.0,
        "best_for": ["white_collar"],
        "targeting_options": ["language", "framework", "contribution_history"],
    },
    CH_STACKOVERFLOW: {
        "type": "developer_community", "avg_cpc": 3.50, "avg_cpa": 50.0,
        "best_for": ["white_collar"],
        "targeting_options": ["tags", "reputation", "technology_stack"],
    },
    CH_DICE: {
        "type": "tech_job_board", "avg_cpc": 1.20, "avg_cpa": 25.0,
        "best_for": ["white_collar"],
        "targeting_options": ["skill_tags", "clearance_level", "experience"],
    },
    CH_GLASSDOOR: {
        "type": "review_job_board", "avg_cpc": 1.80, "avg_cpa": 28.0,
        "best_for": ["white_collar", "grey_collar"],
        "targeting_options": ["job_title", "location", "company_rating"],
    },
    CH_ZIPRECRUITER: {
        "type": "job_board", "avg_cpc": 0.45, "avg_cpa": 15.0,
        "best_for": ["blue_collar", "pink_collar"],
        "targeting_options": ["job_title", "location", "salary_range"],
    },
    CH_MONSTER: {
        "type": "job_board", "avg_cpc": 0.60, "avg_cpa": 20.0,
        "best_for": ["blue_collar", "white_collar"],
        "targeting_options": ["job_title", "location", "industry"],
    },
    CH_ANGELLIST: {
        "type": "startup_board", "avg_cpc": 3.00, "avg_cpa": 40.0,
        "best_for": ["white_collar"],
        "targeting_options": ["role", "stage", "remote_preference", "equity"],
    },
    CH_HIRED: {
        "type": "tech_marketplace", "avg_cpc": 8.00, "avg_cpa": 80.0,
        "best_for": ["white_collar"],
        "targeting_options": ["skill_assessment", "salary_expectation", "location"],
    },
    CH_TRIPLEBYTE: {
        "type": "tech_assessment", "avg_cpc": 7.00, "avg_cpa": 75.0,
        "best_for": ["white_collar"],
        "targeting_options": ["assessment_score", "specialization"],
    },
    CH_TOPTAL: {
        "type": "freelance_marketplace", "avg_cpc": 10.00, "avg_cpa": 120.0,
        "best_for": ["white_collar"],
        "targeting_options": ["skill_assessment", "availability", "rate"],
    },
    CH_BEHANCE: {
        "type": "creative_portfolio", "avg_cpc": 2.50, "avg_cpa": 35.0,
        "best_for": ["white_collar"],
        "targeting_options": ["creative_field", "tools", "style"],
    },
    CH_DRIBBBLE: {
        "type": "creative_portfolio", "avg_cpc": 3.00, "avg_cpa": 38.0,
        "best_for": ["white_collar"],
        "targeting_options": ["design_specialty", "tools", "availability"],
    },
    CH_FLEXJOBS: {
        "type": "remote_job_board", "avg_cpc": 1.50, "avg_cpa": 22.0,
        "best_for": ["white_collar", "pink_collar"],
        "targeting_options": ["remote_type", "schedule", "job_category"],
    },
    CH_WEWORKREMOTELY: {
        "type": "remote_job_board", "avg_cpc": 2.00, "avg_cpa": 30.0,
        "best_for": ["white_collar"],
        "targeting_options": ["category", "timezone", "experience"],
    },
    CH_BUILTIN: {
        "type": "tech_community", "avg_cpc": 2.50, "avg_cpa": 32.0,
        "best_for": ["white_collar"],
        "targeting_options": ["tech_stack", "company_size", "perks"],
    },
    CH_CAREERBUILDER: {
        "type": "job_board", "avg_cpc": 0.55, "avg_cpa": 18.0,
        "best_for": ["white_collar", "pink_collar"],
        "targeting_options": ["job_title", "location", "industry"],
    },
    CH_SNAGAJOB: {
        "type": "hourly_job_board", "avg_cpc": 0.30, "avg_cpa": 10.0,
        "best_for": ["blue_collar", "pink_collar"],
        "targeting_options": ["shift_type", "hourly_rate", "location"],
    },
    CH_NURSE_COM: {
        "type": "healthcare_board", "avg_cpc": 1.80, "avg_cpa": 25.0,
        "best_for": ["grey_collar"],
        "targeting_options": ["specialty", "license_type", "shift"],
    },
    CH_HEALTHCAREJOBSITE: {
        "type": "healthcare_board", "avg_cpc": 1.50, "avg_cpa": 22.0,
        "best_for": ["grey_collar"],
        "targeting_options": ["specialty", "facility_type", "location"],
    },
    CH_CLEARANCEJOBS: {
        "type": "cleared_job_board", "avg_cpc": 3.00, "avg_cpa": 45.0,
        "best_for": ["white_collar"],
        "targeting_options": ["clearance_level", "agency", "skill"],
    },
    CH_USAJOBS: {
        "type": "government_board", "avg_cpc": 0.00, "avg_cpa": 0.0,
        "best_for": ["white_collar", "blue_collar"],
        "targeting_options": ["gs_level", "agency", "location"],
    },
    CH_HANDSHAKE: {
        "type": "campus_recruiting", "avg_cpc": 1.00, "avg_cpa": 12.0,
        "best_for": ["white_collar"],
        "targeting_options": ["major", "graduation_year", "university", "gpa"],
    },
    CH_FACEBOOK: {
        "type": "social_media", "avg_cpc": 0.80, "avg_cpa": 12.0,
        "best_for": ["blue_collar", "pink_collar"],
        "targeting_options": ["location", "interests", "demographics", "behavior"],
    },
    CH_GOOGLE_ADS: {
        "type": "search_ppc", "avg_cpc": 2.50, "avg_cpa": 22.0,
        "best_for": ["blue_collar", "white_collar", "grey_collar", "pink_collar"],
        "targeting_options": ["keyword", "location", "device", "schedule"],
    },
    CH_PROGRAMMATIC: {
        "type": "programmatic", "avg_cpc": 0.35, "avg_cpa": 8.0,
        "best_for": ["blue_collar", "pink_collar", "grey_collar"],
        "targeting_options": ["audience_segment", "geo", "behavioral", "contextual"],
    },
}


# ── Competitor profiles by industry ──
_COMPETITOR_PROFILES: Dict[str, List[Dict[str, Any]]] = {
    "tech_engineering": [
        {"name": "Google", "hot_skills": ["llm_ai", "machine_learning", "golang", "kubernetes"]},
        {"name": "Meta", "hot_skills": ["react", "python", "machine_learning", "rust"]},
        {"name": "Amazon", "hot_skills": ["aws", "java", "python", "devops"]},
        {"name": "Microsoft", "hot_skills": ["azure", "typescript", "python", "llm_ai"]},
        {"name": "Apple", "hot_skills": ["ios_swift", "c++", "machine_learning"]},
    ],
    "healthcare_medical": [
        {"name": "UnitedHealth Group", "hot_skills": ["nursing_rn", "medical_coding", "data_engineering"]},
        {"name": "HCA Healthcare", "hot_skills": ["nursing_rn", "nursing_lpn", "pharmacy"]},
        {"name": "CVS Health", "hot_skills": ["pharmacy", "nursing_rn", "digital_marketing"]},
    ],
    "finance_banking": [
        {"name": "JPMorgan Chase", "hot_skills": ["python", "java", "cybersecurity", "financial_analysis"]},
        {"name": "Goldman Sachs", "hot_skills": ["python", "java", "machine_learning"]},
        {"name": "Blackrock", "hot_skills": ["python", "data_engineering", "financial_analysis"]},
    ],
    "retail_consumer": [
        {"name": "Walmart", "hot_skills": ["forklift_operation", "cdl_driving", "supply_chain"]},
        {"name": "Amazon", "hot_skills": ["forklift_operation", "cdl_driving", "python"]},
        {"name": "Target", "hot_skills": ["supply_chain", "leadership", "digital_marketing"]},
    ],
    "logistics_supply_chain": [
        {"name": "UPS", "hot_skills": ["cdl_driving", "forklift_operation", "supply_chain"]},
        {"name": "FedEx", "hot_skills": ["cdl_driving", "forklift_operation", "leadership"]},
        {"name": "XPO Logistics", "hot_skills": ["cdl_driving", "supply_chain", "data_engineering"]},
    ],
    "construction_real_estate": [
        {"name": "Turner Construction", "hot_skills": ["welding", "electrical_work", "pmp"]},
        {"name": "Bechtel", "hot_skills": ["welding", "hvac", "plumbing", "pmp"]},
    ],
    "automotive": [
        {"name": "Tesla", "hot_skills": ["python", "machine_learning", "electrical_work", "welding"]},
        {"name": "Ford", "hot_skills": ["cnc_machining", "welding", "electrical_work"]},
        {"name": "GM", "hot_skills": ["electrical_work", "welding", "python"]},
    ],
}

# ── Industry -> default collar mapping ──
_INDUSTRY_COLLAR_HINT: Dict[str, str] = {
    "tech_engineering": "white_collar",
    "healthcare_medical": "grey_collar",
    "finance_banking": "white_collar",
    "retail_consumer": "blue_collar",
    "logistics_supply_chain": "blue_collar",
    "hospitality_travel": "blue_collar",
    "construction_real_estate": "blue_collar",
    "automotive": "blue_collar",
    "energy_utilities": "blue_collar",
    "aerospace_defense": "white_collar",
    "pharma_biotech": "white_collar",
    "education": "white_collar",
    "food_beverage": "blue_collar",
    "media_entertainment": "white_collar",
    "legal_services": "white_collar",
    "insurance": "white_collar",
    "telecommunications": "white_collar",
    "mental_health": "grey_collar",
    "general_entry_level": "pink_collar",
}


# =============================================================================
# CORE ANALYSIS FUNCTIONS
# =============================================================================

def _normalize_skill(raw: str) -> str:
    """Normalize a raw skill string to a knowledge-base key."""
    if not raw:
        return ""
    s = raw.strip().lower()
    s = re.sub(r"[^a-z0-9#+./_ -]", "", s)
    s = s.strip()
    # Direct match
    if s in SKILL_DATABASE:
        return s
    # Underscore variant
    us = s.replace(" ", "_").replace("-", "_")
    if us in SKILL_DATABASE:
        return us
    # Alias lookup
    if s in _SKILL_ALIASES:
        return _SKILL_ALIASES[s]
    if us in _SKILL_ALIASES:
        return _SKILL_ALIASES[us]
    # Partial match -- check if any key is contained in input or vice versa
    for key in SKILL_DATABASE:
        if key in s or s in key:
            return key
    return s  # return as-is, will be flagged as unknown


def _categorize_skills(
    required: List[str],
    nice_to_have: List[str],
) -> Dict[str, Any]:
    """Categorize skills into technical, soft, certification, domain."""
    categories: Dict[str, List[Dict[str, Any]]] = {
        CAT_TECHNICAL: [],
        CAT_SOFT: [],
        CAT_CERTIFICATION: [],
        CAT_DOMAIN: [],
    }
    unknown_skills: List[str] = []

    all_skills = [(s, True) for s in required] + [(s, False) for s in nice_to_have]

    for raw_skill, is_required in all_skills:
        norm = _normalize_skill(raw_skill)
        if norm in SKILL_DATABASE:
            info = SKILL_DATABASE[norm]
            entry = {
                "skill": raw_skill,
                "normalized": norm,
                "is_required": is_required,
                "rarity": info["rarity"],
                "demand_trend": info["demand_trend"],
                "talent_pool_base": info["talent_pool_base"],
            }
            cat = info["category"]
            categories.get(cat, categories[CAT_DOMAIN]).append(entry)
        else:
            unknown_skills.append(raw_skill)

    # Compute overall rarity score (0-100, higher = rarer combo)
    rarity_weights = {RARITY_COMMON: 10, RARITY_MODERATE: 35, RARITY_RARE: 70, RARITY_UNICORN: 95}
    all_known = []
    for cat_list in categories.values():
        all_known.extend(cat_list)

    if all_known:
        req_rarities = [rarity_weights.get(s["rarity"], 30) for s in all_known if s["is_required"]]
        avg_rarity = sum(req_rarities) / len(req_rarities) if req_rarities else 30
    else:
        avg_rarity = 30

    return {
        "categories": {k: v for k, v in categories.items()},
        "unknown_skills": unknown_skills,
        "total_known": len(all_known),
        "total_unknown": len(unknown_skills),
        "overall_rarity_score": round(avg_rarity, 1),
        "rarity_label": (
            "unicorn" if avg_rarity >= 80 else
            "rare" if avg_rarity >= 55 else
            "moderate" if avg_rarity >= 25 else
            "common"
        ),
    }


def _recommend_channels(
    skill_analysis: Dict[str, Any],
    collar_type: str,
    budget: float,
) -> List[Dict[str, Any]]:
    """Recommend channels based on skill clusters, collar type, and budget."""
    channel_scores: Dict[str, Dict[str, Any]] = {}

    all_skills = []
    for cat_skills in skill_analysis["categories"].values():
        all_skills.extend(cat_skills)

    if not all_skills:
        return [
            {"channel": CH_INDEED, "match_score": 75, "reason": "General fallback - broad reach",
             "avg_cpc": 0.50, "avg_cpa": 18.0, "platform_type": "job_board",
             "targeting_options": ["job_title", "location"], "skills_covered": [], "collar_fit": True},
            {"channel": CH_LINKEDIN, "match_score": 70, "reason": "General fallback - professional network",
             "avg_cpc": 5.50, "avg_cpa": 45.0, "platform_type": "professional_network",
             "targeting_options": ["skills", "job_title"], "skills_covered": [], "collar_fit": True},
        ]

    # Score each channel by how many skills it covers
    for skill_info in all_skills:
        norm = skill_info["normalized"]
        if norm not in SKILL_DATABASE:
            continue
        db_entry = SKILL_DATABASE[norm]
        weight = 2.0 if skill_info["is_required"] else 1.0
        rarity_bonus = {RARITY_COMMON: 1.0, RARITY_MODERATE: 1.2, RARITY_RARE: 1.5, RARITY_UNICORN: 2.0}
        rb = rarity_bonus.get(db_entry["rarity"], 1.0)

        for ch in db_entry["channels"]:
            if ch not in channel_scores:
                channel_scores[ch] = {
                    "channel": ch,
                    "skills_covered": [],
                    "raw_score": 0.0,
                    "collar_fit": False,
                }
            channel_scores[ch]["skills_covered"].append(norm)
            channel_scores[ch]["raw_score"] += weight * rb

    # Apply collar fit bonus
    for ch_name, cs in channel_scores.items():
        meta = CHANNEL_METADATA.get(ch_name, {})
        best_for = meta.get("best_for", [])
        if collar_type in best_for:
            cs["collar_fit"] = True
            cs["raw_score"] *= 1.3

    # Normalize to 0-100 match score
    max_score = max((cs["raw_score"] for cs in channel_scores.values()), default=1.0)
    for cs in channel_scores.values():
        cs["match_score"] = round((cs["raw_score"] / max_score) * 100, 1)
        meta = CHANNEL_METADATA.get(cs["channel"], {})
        cs["avg_cpc"] = meta.get("avg_cpc", 1.0)
        cs["avg_cpa"] = meta.get("avg_cpa", 20.0)
        cs["platform_type"] = meta.get("type", "unknown")
        cs["targeting_options"] = meta.get("targeting_options", [])
        n_skills = len(cs["skills_covered"])
        cs["reason"] = (
            f"Covers {n_skills} skill{'s' if n_skills != 1 else ''}: "
            f"{', '.join(cs['skills_covered'][:4])}"
            f"{'...' if n_skills > 4 else ''}"
        )
        del cs["raw_score"]

    ranked = sorted(channel_scores.values(), key=lambda x: x["match_score"], reverse=True)
    return ranked[:10]


def _build_targeting_strategy(
    channels: List[Dict[str, Any]],
    skill_analysis: Dict[str, Any],
    job_title: str,
    collar_type: str,
    location: str,
) -> List[Dict[str, Any]]:
    """Build platform-specific targeting recommendations."""
    strategies = []
    for ch_rec in channels[:6]:
        ch_name = ch_rec["channel"]
        meta = CHANNEL_METADATA.get(ch_name, {})
        targeting_opts = meta.get("targeting_options", [])

        strategy = {
            "channel": ch_name,
            "match_score": ch_rec["match_score"],
            "targeting_parameters": {},
            "ad_copy_suggestions": [],
            "audience_size_estimate": "medium",
        }

        if "skills" in targeting_opts or "skill_tags" in targeting_opts or "tags" in targeting_opts:
            all_skills_list = []
            for cat_skills in skill_analysis["categories"].values():
                all_skills_list.extend([s["skill"] for s in cat_skills if s["is_required"]])
            strategy["targeting_parameters"]["skills"] = all_skills_list[:10]

        if "job_title" in targeting_opts:
            strategy["targeting_parameters"]["job_title"] = job_title

        if "location" in targeting_opts and location:
            strategy["targeting_parameters"]["location"] = location

        if "industry" in targeting_opts:
            strategy["targeting_parameters"]["industry"] = "auto-detect"

        if "seniority" in targeting_opts:
            title_lower = job_title.lower()
            if any(w in title_lower for w in ["senior", "lead", "principal", "staff"]):
                strategy["targeting_parameters"]["seniority"] = "senior"
            elif any(w in title_lower for w in ["director", "vp", "head"]):
                strategy["targeting_parameters"]["seniority"] = "director+"
            elif any(w in title_lower for w in ["junior", "entry", "associate", "intern"]):
                strategy["targeting_parameters"]["seniority"] = "entry-level"
            else:
                strategy["targeting_parameters"]["seniority"] = "mid-level"

        rarity = skill_analysis.get("overall_rarity_score", 30)
        if rarity >= 70:
            strategy["audience_size_estimate"] = "very_small"
        elif rarity >= 50:
            strategy["audience_size_estimate"] = "small"
        elif rarity >= 25:
            strategy["audience_size_estimate"] = "medium"
        else:
            strategy["audience_size_estimate"] = "large"

        if collar_type == "blue_collar":
            strategy["ad_copy_suggestions"] = [
                "Highlight hourly rate and shift schedule upfront",
                "Mention benefits: health insurance, 401k match",
                "Use simple, direct language -- avoid jargon",
                "Include 'No experience required' if applicable",
            ]
        elif collar_type == "grey_collar":
            strategy["ad_copy_suggestions"] = [
                "Lead with license/certification recognition",
                "Mention sign-on bonuses and shift differentials",
                "Highlight patient-to-staff ratios",
                "Include continuing education support",
            ]
        elif collar_type == "pink_collar":
            strategy["ad_copy_suggestions"] = [
                "Emphasize flexible scheduling and work-life balance",
                "Highlight growth opportunities and career paths",
                "Mention team culture and supportive environment",
            ]
        else:
            strategy["ad_copy_suggestions"] = [
                "Lead with technical challenges and impact",
                "Highlight tech stack alignment with candidate skills",
                "Mention remote/hybrid options if available",
                "Include compensation range for transparency",
            ]

        strategies.append(strategy)

    return strategies


def _allocate_budget(
    channels: List[Dict[str, Any]],
    total_budget: float,
    collar_type: str,
) -> Dict[str, Any]:
    """Allocate budget across recommended channels."""
    if not channels or total_budget <= 0:
        return {"total_budget": total_budget, "allocations": [], "notes": ["No channels to allocate"]}

    total_weight = sum(ch["match_score"] for ch in channels[:8])
    if total_weight == 0:
        total_weight = 1.0

    allocations = []
    remaining = total_budget

    for ch in channels[:8]:
        pct = ch["match_score"] / total_weight
        amount = round(total_budget * pct, 2)
        remaining -= amount
        cpa = ch.get("avg_cpa", 20.0)
        est_apps = int(amount / cpa) if cpa > 0 else 0

        allocations.append({
            "channel": ch["channel"],
            "budget": amount,
            "percentage": round(pct * 100, 1),
            "estimated_applications": est_apps,
            "avg_cpa": cpa,
            "match_score": ch["match_score"],
        })

    if remaining != 0 and allocations:
        allocations[0]["budget"] = round(allocations[0]["budget"] + remaining, 2)

    notes = []
    for ch in channels[:3]:
        for sk in ch.get("skills_covered", []):
            if sk in SKILL_DATABASE and SKILL_DATABASE[sk]["rarity"] == RARITY_UNICORN:
                notes.append(f"Unicorn skill '{sk}' detected -- consider sourcing + headhunting budget")
                break

    if collar_type == "blue_collar":
        notes.append("Blue collar: prioritize high-volume, low-CPA channels (Indeed, Snagajob, Programmatic)")
    elif collar_type == "grey_collar":
        notes.append("Grey collar: allocate 15-20% to niche healthcare/clinical boards")

    total_est_apps = sum(a["estimated_applications"] for a in allocations)

    return {
        "total_budget": total_budget,
        "allocations": allocations,
        "total_estimated_applications": total_est_apps,
        "estimated_cost_per_application": round(total_budget / total_est_apps, 2) if total_est_apps > 0 else 0,
        "notes": notes,
    }


def _get_competitor_insight(
    skill_analysis: Dict[str, Any],
    industry: str,
) -> Dict[str, Any]:
    """Identify competitors hiring for similar skills."""
    industry_key = industry.lower().replace(" ", "_") if industry else ""
    matched_key = ""
    for k in _COMPETITOR_PROFILES:
        if k == industry_key or industry_key in k or k in industry_key:
            matched_key = k
            break

    if not matched_key:
        for k in _COMPETITOR_PROFILES:
            if any(part in industry_key for part in k.split("_") if len(part) > 3):
                matched_key = k
                break

    competitors = _COMPETITOR_PROFILES.get(matched_key, [])

    required_norms = set()
    for cat_skills in skill_analysis["categories"].values():
        for s in cat_skills:
            if s["is_required"]:
                required_norms.add(s["normalized"])

    scored = []
    for comp in competitors:
        overlap = required_norms.intersection(set(comp["hot_skills"]))
        score = len(overlap) / max(len(required_norms), 1) * 100
        scored.append({
            "company": comp["name"],
            "overlap_score": round(score, 1),
            "shared_skills": list(overlap),
            "their_hot_skills": comp["hot_skills"],
        })

    scored.sort(key=lambda x: x["overlap_score"], reverse=True)

    return {
        "industry_matched": matched_key or "general",
        "competitors": scored[:5],
        "talent_competition_level": (
            "very_high" if any(c["overlap_score"] >= 60 for c in scored) else
            "high" if any(c["overlap_score"] >= 40 for c in scored) else
            "moderate" if scored else "low"
        ),
        "recommendation": (
            "Multiple major employers are competing for the same skills. "
            "Consider employer branding investment and faster offer timelines."
            if any(c["overlap_score"] >= 40 for c in scored) else
            "Moderate competition. Standard sourcing strategy should suffice."
        ),
    }


def _estimate_talent_pool(
    skill_analysis: Dict[str, Any],
    location: str,
) -> Dict[str, Any]:
    """Estimate available talent pool size based on skill intersection."""
    pool_sizes = []
    for cat_skills in skill_analysis["categories"].values():
        for s in cat_skills:
            if s["is_required"] and "talent_pool_base" in s:
                pool_sizes.append(s["talent_pool_base"])

    if not pool_sizes:
        return {
            "estimated_total_pool": 50_000,
            "active_seekers": 10_000,
            "passive_reachable": 17_500,
            "confidence": "low",
            "methodology": "fallback_estimate",
            "location_adjustment": location or "nationwide",
            "location_multiplier": 1.0,
            "required_skills_count": 0,
            "smallest_single_pool": 0,
            "pool_scarcity": "adequate",
        }

    smallest = min(pool_sizes)
    n_required = len(pool_sizes)
    overlap_factor = 0.5 ** max(n_required - 1, 0)
    estimated = int(smallest * overlap_factor)

    loc_lower = (location or "").lower()
    if any(city in loc_lower for city in ["new york", "san francisco", "los angeles",
                                           "chicago", "seattle", "boston", "austin"]):
        loc_multiplier = 0.08
    elif any(region in loc_lower for region in ["us", "united states", "usa", "nationwide", "remote"]):
        loc_multiplier = 1.0
    elif location:
        loc_multiplier = 0.03
    else:
        loc_multiplier = 1.0

    adjusted = max(int(estimated * loc_multiplier), 10)
    active_seekers = int(adjusted * 0.20)
    passive_reachable = int(adjusted * 0.35)

    return {
        "estimated_total_pool": adjusted,
        "active_seekers": active_seekers,
        "passive_reachable": passive_reachable,
        "location_adjustment": location or "nationwide",
        "location_multiplier": loc_multiplier,
        "confidence": "medium" if n_required <= 3 else "low",
        "methodology": "skill_intersection_overlap",
        "required_skills_count": n_required,
        "smallest_single_pool": smallest,
        "pool_scarcity": (
            "critical" if adjusted < 500 else
            "scarce" if adjusted < 5_000 else
            "tight" if adjusted < 50_000 else
            "adequate" if adjusted < 200_000 else
            "abundant"
        ),
    }


def _classify_collar_type(job_title: str, industry: str, skills: List[str]) -> Dict[str, Any]:
    """Classify collar type using collar_intelligence or fallback heuristics."""
    if _HAS_COLLAR:
        try:
            result = _classify_collar(job_title, industry=industry)
            return result
        except Exception as e:
            logger.warning("collar_intelligence failed: %s", e)

    title_lower = job_title.lower() if job_title else ""
    norm_skills = [_normalize_skill(s) for s in skills]

    blue_skills = {"welding", "hvac", "cdl_driving", "forklift_operation", "electrical_work",
                   "plumbing", "cnc_machining"}
    grey_skills = {"nursing_rn", "nursing_lpn", "medical_coding", "pharmacy", "clinical_research"}

    blue_count = len(blue_skills.intersection(norm_skills))
    grey_count = len(grey_skills.intersection(norm_skills))

    if blue_count >= 1:
        collar = "blue_collar"
    elif grey_count >= 1:
        collar = "grey_collar"
    elif any(w in title_lower for w in ["nurse", "medical", "clinical", "therapist", "technician"]):
        collar = "grey_collar"
    elif any(w in title_lower for w in ["admin", "receptionist", "secretary", "clerk", "assistant"]):
        collar = "pink_collar"
    elif any(w in title_lower for w in ["driver", "warehouse", "mechanic", "welder", "plumber",
                                         "electrician", "operator", "laborer",
                                         "janitor", "cook", "dishwasher"]):
        collar = "blue_collar"
    else:
        collar = _INDUSTRY_COLLAR_HINT.get(industry, "white_collar")

    return {
        "collar_type": collar,
        "confidence": 0.55,
        "sub_type": collar.replace("_", " ").title(),
        "method": "skill_target_fallback",
        "indicators": [f"job_title={job_title}", f"industry={industry}"],
        "channel_strategy": (
            "volume" if collar == "blue_collar" else
            "targeted" if collar in ("grey_collar", "pink_collar") else
            "premium"
        ),
    }


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def analyze_skills(data: Dict[str, Any]) -> Dict[str, Any]:
    """Main entry point for skill-based targeting analysis.

    Args:
        data: dict with keys:
            - job_title (str)
            - required_skills (list[str])
            - nice_to_have_skills (list[str], optional)
            - industry (str)
            - location (str, optional)
            - budget (float, optional)

    Returns:
        Full analysis dict with skill_analysis, channel_recommendations,
        targeting_strategy, budget_allocation, competitor_insight,
        talent_pool_estimate, collar_type.
    """
    try:
        job_title = str(data.get("job_title", "")).strip()
        required_skills = data.get("required_skills", [])
        nice_to_have = data.get("nice_to_have_skills", [])
        industry = str(data.get("industry", "")).strip()
        location = str(data.get("location", "")).strip()
        budget_raw = data.get("budget", 50_000)

        if not job_title:
            return {"error": "job_title is required", "status": "error"}
        if not required_skills:
            return {"error": "required_skills list is required (at least 1 skill)", "status": "error"}

        budget = parse_budget(budget_raw, default=50_000.0)

        # 1. Classify collar type
        collar_result = _classify_collar_type(job_title, industry, required_skills)
        collar_type = collar_result.get("collar_type", "white_collar")

        # 2. Categorize skills
        skill_analysis = _categorize_skills(required_skills, nice_to_have)

        # 3. Channel recommendations
        channels = _recommend_channels(skill_analysis, collar_type, budget)

        # 4. Targeting strategy
        targeting = _build_targeting_strategy(channels, skill_analysis, job_title, collar_type, location)

        # 5. Budget allocation
        allocation = _allocate_budget(channels, budget, collar_type)

        # 6. Competitor insight
        competitors = _get_competitor_insight(skill_analysis, industry)

        # 7. Talent pool estimate
        talent_pool = _estimate_talent_pool(skill_analysis, location)

        return {
            "status": "success",
            "job_title": job_title,
            "industry": industry,
            "location": location or "nationwide",
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "collar_type": collar_result,
            "skill_analysis": skill_analysis,
            "channel_recommendations": channels,
            "targeting_strategy": targeting,
            "budget_allocation": allocation,
            "competitor_insight": competitors,
            "talent_pool_estimate": talent_pool,
            "ui_theme": {
                "bg": _UI_BG,
                "card": _UI_CARD,
                "accent": _UI_ACCENT,
            },
        }

    except Exception as exc:
        logger.error("analyze_skills failed: %s\n%s", exc, traceback.format_exc())
        return {"error": str(exc), "status": "error", "traceback": traceback.format_exc()}


# =============================================================================
# EXCEL EXPORT
# =============================================================================

def generate_skill_report_excel(analysis: Dict[str, Any]) -> bytes:
    """Generate multi-sheet Excel report from skill analysis.

    Sheets:
        1. Executive Summary
        2. Skill Analysis
        3. Channel Recommendations
        4. Targeting Strategy
        5. Budget Allocation
        6. Talent Pool & Competitors

    Uses Sapphire Blue palette, Calibri font, data starts at column B.
    """
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        from openpyxl.utils import get_column_letter
    except ImportError:
        logger.error("openpyxl not available for Excel generation")
        return b""

    wb = Workbook()
    B = COL_START

    # Design tokens
    f_title = Font(name="Calibri", bold=True, size=18, color=_WHITE)
    f_section = Font(name="Calibri", bold=True, size=14, color=_WHITE)
    f_subsection = Font(name="Calibri", bold=True, size=12, color=_NAVY)
    f_header = Font(name="Calibri", bold=True, size=10, color=_WHITE)
    f_body = Font(name="Calibri", size=10, color=_STONE)
    f_body_bold = Font(name="Calibri", bold=True, size=10, color=_STONE)
    f_footnote = Font(name="Calibri", italic=True, size=9, color=_MUTED)
    f_green = Font(name="Calibri", bold=True, size=10, color=_GREEN)
    f_red = Font(name="Calibri", bold=True, size=10, color=_RED)
    f_amber = Font(name="Calibri", bold=True, size=10, color=_AMBER)

    fill_navy = PatternFill("solid", fgColor=_NAVY)
    fill_sapphire = PatternFill("solid", fgColor=_SAPPHIRE)
    fill_light = PatternFill("solid", fgColor=_BLUE_LIGHT)
    fill_pale = PatternFill("solid", fgColor=_BLUE_PALE)
    fill_white = PatternFill("solid", fgColor=_WHITE)
    fill_green_bg = PatternFill("solid", fgColor=_GREEN_BG)
    fill_red_bg = PatternFill("solid", fgColor=_RED_BG)
    fill_amber_bg = PatternFill("solid", fgColor=_AMBER_BG)

    align_center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    align_left = Alignment(horizontal="left", vertical="center", wrap_text=True)
    align_right = Alignment(horizontal="right", vertical="center")

    thin_border = Border(
        left=Side(style="thin", color=_WARM_GRAY),
        right=Side(style="thin", color=_WARM_GRAY),
        top=Side(style="thin", color=_WARM_GRAY),
        bottom=Side(style="thin", color=_WARM_GRAY),
    )

    def _col_widths(ws, widths):
        for i, w in enumerate(widths, start=1):
            ws.column_dimensions[get_column_letter(i)].width = w

    def _section_header(ws, row, title, span=7):
        for c in range(B, B + span):
            ws.cell(row=row, column=c).fill = fill_navy
        cell = ws.cell(row=row, column=B, value=title)
        cell.font = f_section
        cell.alignment = align_left
        ws.row_dimensions[row].height = 32
        return row + 1

    def _table_header(ws, row, headers):
        for i, h in enumerate(headers):
            cell = ws.cell(row=row, column=B + i, value=h)
            cell.font = f_header
            cell.fill = fill_sapphire
            cell.alignment = align_center
            cell.border = thin_border
        ws.row_dimensions[row].height = 28
        return row + 1

    def _data_row(ws, row, values, bold_first=False):
        for i, v in enumerate(values):
            cell = ws.cell(row=row, column=B + i, value=v)
            cell.font = f_body_bold if (bold_first and i == 0) else f_body
            cell.alignment = align_left if i == 0 else align_center
            cell.border = thin_border
            cell.fill = fill_pale if row % 2 == 0 else fill_white
        ws.row_dimensions[row].height = 22
        return row + 1

    # ── Sheet 1: Executive Summary ──
    ws1 = wb.active
    ws1.title = "Executive Summary"
    _col_widths(ws1, [4, 28, 22, 22, 22, 22, 22, 22])

    r = 2
    for c in range(B, B + 6):
        ws1.cell(row=r, column=c).fill = fill_navy
    cell = ws1.cell(row=r, column=B, value="Skill-Based Targeting Report")
    cell.font = f_title
    cell.alignment = align_left
    ws1.row_dimensions[r].height = 42
    r += 2

    summary_items = [
        ("Job Title", analysis.get("job_title", "")),
        ("Industry", analysis.get("industry", "")),
        ("Location", analysis.get("location", "")),
        ("Collar Type", analysis.get("collar_type", {}).get("collar_type", "").replace("_", " ").title()),
        ("Overall Skill Rarity", analysis.get("skill_analysis", {}).get("rarity_label", "").title()),
        ("Rarity Score", f"{analysis.get('skill_analysis', {}).get('overall_rarity_score', 0)}/100"),
        ("Budget", f"${analysis.get('budget_allocation', {}).get('total_budget', 0):,.0f}"),
        ("Est. Applications", f"{analysis.get('budget_allocation', {}).get('total_estimated_applications', 0):,}"),
        ("Talent Pool (Active)", f"{analysis.get('talent_pool_estimate', {}).get('active_seekers', 0):,}"),
        ("Competition Level", analysis.get("competitor_insight", {}).get("talent_competition_level", "").replace("_", " ").title()),
        ("Generated", analysis.get("generated_at", "")),
    ]

    r = _section_header(ws1, r, "Overview", 6)
    for label, value in summary_items:
        ws1.cell(row=r, column=B, value=label).font = f_body_bold
        ws1.cell(row=r, column=B, value=label).alignment = align_left
        ws1.cell(row=r, column=B + 1, value=str(value)).font = f_body
        ws1.cell(row=r, column=B + 1, value=str(value)).alignment = align_left
        ws1.row_dimensions[r].height = 22
        r += 1

    # ── Sheet 2: Skill Analysis ──
    ws2 = wb.create_sheet("Skill Analysis")
    _col_widths(ws2, [4, 24, 20, 16, 16, 18, 18])

    r = 2
    skill_data = analysis.get("skill_analysis", {})
    categories = skill_data.get("categories", {})

    cat_labels = {
        CAT_TECHNICAL: "Technical Skills",
        CAT_SOFT: "Soft Skills",
        CAT_CERTIFICATION: "Certifications",
        CAT_DOMAIN: "Domain Knowledge",
    }

    for cat_key, cat_label in cat_labels.items():
        skills_list = categories.get(cat_key, [])
        if not skills_list:
            continue
        r = _section_header(ws2, r, cat_label, 5)
        r = _table_header(ws2, r, ["Skill", "Rarity", "Required", "Trend", "Pool Size"])
        for sk in skills_list:
            rarity_val = sk.get("rarity", "unknown").title()
            r = _data_row(ws2, r, [
                sk.get("skill", ""),
                rarity_val,
                "Yes" if sk.get("is_required") else "Nice-to-Have",
                sk.get("demand_trend", "").title(),
                f"{sk.get('talent_pool_base', 0):,}",
            ], bold_first=True)
        r += 1

    unknown = skill_data.get("unknown_skills", [])
    if unknown:
        r = _section_header(ws2, r, "Unrecognized Skills", 5)
        for sk in unknown:
            ws2.cell(row=r, column=B, value=sk).font = f_body
            ws2.cell(row=r, column=B + 1, value="Not in database").font = f_footnote
            r += 1

    # ── Sheet 3: Channel Recommendations ──
    ws3 = wb.create_sheet("Channels")
    _col_widths(ws3, [4, 26, 16, 14, 14, 14, 36])

    r = 2
    r = _section_header(ws3, r, "Channel Recommendations", 6)
    r = _table_header(ws3, r, ["Channel", "Match Score", "Avg CPC", "Avg CPA", "Type", "Reason"])
    ch_list = analysis.get("channel_recommendations", [])
    for ch in ch_list:
        r = _data_row(ws3, r, [
            ch.get("channel", ""),
            f"{ch.get('match_score', 0)}%",
            f"${ch.get('avg_cpc', 0):.2f}",
            f"${ch.get('avg_cpa', 0):.2f}",
            ch.get("platform_type", "").replace("_", " ").title(),
            ch.get("reason", ""),
        ], bold_first=True)

    # ── Sheet 4: Targeting Strategy ──
    ws4 = wb.create_sheet("Targeting")
    _col_widths(ws4, [4, 24, 18, 36, 42])

    r = 2
    r = _section_header(ws4, r, "Platform Targeting Strategy", 4)
    strategies = analysis.get("targeting_strategy", [])
    for strat in strategies:
        r = _section_header(ws4, r, strat.get("channel", ""), 4)
        ws4.cell(row=r, column=B, value="Match Score").font = f_body_bold
        ws4.cell(row=r, column=B + 1, value=f"{strat.get('match_score', 0)}%").font = f_body
        r += 1
        ws4.cell(row=r, column=B, value="Audience Size").font = f_body_bold
        ws4.cell(row=r, column=B + 1, value=strat.get("audience_size_estimate", "").replace("_", " ").title()).font = f_body
        r += 1

        params = strat.get("targeting_parameters", {})
        if params:
            ws4.cell(row=r, column=B, value="Targeting Parameters").font = f_subsection
            r += 1
            for pk, pv in params.items():
                ws4.cell(row=r, column=B, value=pk.replace("_", " ").title()).font = f_body_bold
                val_str = ", ".join(pv) if isinstance(pv, list) else str(pv)
                ws4.cell(row=r, column=B + 1, value=val_str).font = f_body
                r += 1

        suggestions = strat.get("ad_copy_suggestions", [])
        if suggestions:
            ws4.cell(row=r, column=B, value="Ad Copy Tips").font = f_subsection
            r += 1
            for tip in suggestions:
                ws4.cell(row=r, column=B, value=f"  {tip}").font = f_footnote
                r += 1
        r += 1

    # ── Sheet 5: Budget Allocation ──
    ws5 = wb.create_sheet("Budget")
    _col_widths(ws5, [4, 26, 16, 14, 18, 16, 16])

    r = 2
    alloc = analysis.get("budget_allocation", {})
    total_b = alloc.get("total_budget", 0)
    r = _section_header(ws5, r, f"Budget Allocation (${total_b:,.0f})", 6)
    r = _table_header(ws5, r, ["Channel", "Budget", "%", "Est. Apps", "Avg CPA", "Match"])
    for a in alloc.get("allocations", []):
        r = _data_row(ws5, r, [
            a.get("channel", ""),
            f"${a.get('budget', 0):,.0f}",
            f"{a.get('percentage', 0)}%",
            f"{a.get('estimated_applications', 0):,}",
            f"${a.get('avg_cpa', 0):.2f}",
            f"{a.get('match_score', 0)}%",
        ], bold_first=True)

    r += 1
    notes = alloc.get("notes", [])
    if notes:
        ws5.cell(row=r, column=B, value="Notes").font = f_subsection
        r += 1
        for note in notes:
            ws5.cell(row=r, column=B, value=note).font = f_footnote
            r += 1

    # ── Sheet 6: Talent Pool & Competitors ──
    ws6 = wb.create_sheet("Talent & Competition")
    _col_widths(ws6, [4, 26, 18, 18, 18, 30])

    r = 2
    tp = analysis.get("talent_pool_estimate", {})
    r = _section_header(ws6, r, "Talent Pool Estimate", 5)
    pool_items = [
        ("Total Pool", f"{tp.get('estimated_total_pool', 0):,}"),
        ("Active Seekers", f"{tp.get('active_seekers', 0):,}"),
        ("Passive Reachable", f"{tp.get('passive_reachable', 0):,}"),
        ("Pool Scarcity", tp.get("pool_scarcity", "").title()),
        ("Location", tp.get("location_adjustment", "")),
        ("Confidence", tp.get("confidence", "").title()),
    ]
    for label, val in pool_items:
        ws6.cell(row=r, column=B, value=label).font = f_body_bold
        ws6.cell(row=r, column=B + 1, value=val).font = f_body
        r += 1

    r += 1
    comp = analysis.get("competitor_insight", {})
    comp_list = comp.get("competitors", [])
    if comp_list:
        r = _section_header(ws6, r, "Competitor Analysis", 5)
        r = _table_header(ws6, r, ["Company", "Overlap Score", "Shared Skills", "Their Hot Skills"])
        for c in comp_list:
            r = _data_row(ws6, r, [
                c.get("company", ""),
                f"{c.get('overlap_score', 0)}%",
                ", ".join(c.get("shared_skills", [])),
                ", ".join(c.get("their_hot_skills", [])[:4]),
            ], bold_first=True)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# =============================================================================
# PPT EXPORT
# =============================================================================

def generate_skill_report_ppt(analysis: Dict[str, Any]) -> bytes:
    """Generate branded PPT report from skill analysis.

    Slides:
        1. Title
        2. Skill Analysis Overview
        3. Channel Recommendations
        4. Budget Allocation
        5. Talent Pool & Competition
        6. Targeting Strategy Summary

    Uses Joveo branding: Port Gore #202058, Blue Violet #5A54BD, Downy #6BB3CD.
    """
    try:
        from pptx import Presentation
        from pptx.util import Inches, Pt, Emu
        from pptx.dml.color import RGBColor
        from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
        from pptx.enum.shapes import MSO_SHAPE
    except ImportError:
        logger.error("python-pptx not available for PPT generation")
        return b""

    NAVY = RGBColor(0x20, 0x20, 0x58)
    BLUE = RGBColor(0x5A, 0x54, 0xBD)
    TEAL = RGBColor(0x6B, 0xB3, 0xCD)
    WHITE = RGBColor(0xFF, 0xFF, 0xFF)
    OFF_WHITE = RGBColor(0xFF, 0xFD, 0xF9)
    DARK_TEXT = RGBColor(0x20, 0x20, 0x58)
    MUTED_TEXT = RGBColor(0x59, 0x67, 0x80)
    GREEN = RGBColor(0x33, 0x87, 0x21)
    AMBER = RGBColor(0xCE, 0x90, 0x47)
    RED_ACCENT = RGBColor(0xB5, 0x66, 0x9C)
    LIGHT_BG = RGBColor(0xF5, 0xF3, 0xFF)
    WARM_GRAY = RGBColor(0xEB, 0xE6, 0xE0)

    FONT_TITLE = "Poppins"
    FONT_BODY = "Inter"

    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)

    def _add_bg(slide, color=OFF_WHITE):
        bg = slide.background
        fill = bg.fill
        fill.solid()
        fill.fore_color.rgb = color

    def _add_shape(slide, left, top, width, height, fill_color, border_color=None):
        shape = slide.shapes.add_shape(MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height)
        shape.fill.solid()
        shape.fill.fore_color.rgb = fill_color
        if border_color:
            shape.line.color.rgb = border_color
            shape.line.width = Pt(1)
        else:
            shape.line.fill.background()
        return shape

    def _add_text_box(slide, left, top, width, height, text,
                      font_name=FONT_BODY, size=12, color=DARK_TEXT,
                      bold=False, align=PP_ALIGN.LEFT):
        txBox = slide.shapes.add_textbox(left, top, width, height)
        tf = txBox.text_frame
        tf.word_wrap = True
        p = tf.paragraphs[0]
        p.text = str(text)
        p.font.name = font_name
        p.font.size = Pt(size)
        p.font.color.rgb = color
        p.font.bold = bold
        p.alignment = align
        return txBox

    def _add_kpi_card(slide, left, top, label, value, accent=BLUE):
        _add_shape(slide, left, top, Inches(2.8), Inches(1.2), WHITE, WARM_GRAY)
        _add_text_box(slide, left + Inches(0.2), top + Inches(0.15),
                      Inches(2.4), Inches(0.4), label,
                      size=10, color=MUTED_TEXT)
        _add_text_box(slide, left + Inches(0.2), top + Inches(0.55),
                      Inches(2.4), Inches(0.5), str(value),
                      font_name=FONT_TITLE, size=22, color=accent, bold=True)

    # ── Slide 1: Title ──
    slide1 = prs.slides.add_slide(prs.slide_layouts[6])  # blank
    _add_bg(slide1, NAVY)
    _add_text_box(slide1, Inches(1), Inches(1.5), Inches(11), Inches(1.5),
                  "Skill-Based Targeting Report",
                  font_name=FONT_TITLE, size=36, color=WHITE, bold=True,
                  align=PP_ALIGN.CENTER)

    job_title = analysis.get("job_title", "")
    _add_text_box(slide1, Inches(1), Inches(3.2), Inches(11), Inches(0.8),
                  job_title,
                  font_name=FONT_TITLE, size=24, color=TEAL, bold=True,
                  align=PP_ALIGN.CENTER)

    subtitle_parts = []
    if analysis.get("industry"):
        subtitle_parts.append(analysis["industry"].replace("_", " ").title())
    if analysis.get("location"):
        subtitle_parts.append(analysis["location"])
    collar_label = analysis.get("collar_type", {}).get("collar_type", "").replace("_", " ").title()
    if collar_label:
        subtitle_parts.append(collar_label)

    _add_text_box(slide1, Inches(1), Inches(4.2), Inches(11), Inches(0.6),
                  " | ".join(subtitle_parts),
                  size=14, color=WHITE, align=PP_ALIGN.CENTER)

    _add_text_box(slide1, Inches(1), Inches(6.2), Inches(11), Inches(0.4),
                  analysis.get("generated_at", ""),
                  size=10, color=MUTED_TEXT, align=PP_ALIGN.CENTER)

    # ── Slide 2: Skill Analysis Overview ──
    slide2 = prs.slides.add_slide(prs.slide_layouts[6])
    _add_bg(slide2)
    _add_shape(slide2, Inches(0), Inches(0), Inches(13.333), Inches(1.0), NAVY)
    _add_text_box(slide2, Inches(0.5), Inches(0.2), Inches(12), Inches(0.6),
                  "Skill Analysis", font_name=FONT_TITLE, size=24, color=WHITE, bold=True)

    skill_data = analysis.get("skill_analysis", {})
    rarity_score = skill_data.get("overall_rarity_score", 0)
    rarity_label = skill_data.get("rarity_label", "moderate").title()
    total_known = skill_data.get("total_known", 0)
    total_unknown = skill_data.get("total_unknown", 0)

    _add_kpi_card(slide2, Inches(0.5), Inches(1.3), "Rarity Score", f"{rarity_score}/100", BLUE)
    _add_kpi_card(slide2, Inches(3.6), Inches(1.3), "Rarity Level", rarity_label, NAVY)
    _add_kpi_card(slide2, Inches(6.7), Inches(1.3), "Skills Mapped", f"{total_known}", TEAL)
    _add_kpi_card(slide2, Inches(9.8), Inches(1.3), "Unmapped", f"{total_unknown}",
                  GREEN if total_unknown == 0 else AMBER)

    categories = skill_data.get("categories", {})
    ppt_cat_labels = {
        CAT_TECHNICAL: "Technical",
        CAT_SOFT: "Soft Skills",
        CAT_CERTIFICATION: "Certifications",
        CAT_DOMAIN: "Domain",
    }

    y_pos = Inches(2.8)
    for cat_key, cat_label in ppt_cat_labels.items():
        skills_in_cat = categories.get(cat_key, [])
        if not skills_in_cat:
            continue
        _add_text_box(slide2, Inches(0.5), y_pos, Inches(2.5), Inches(0.35),
                      f"{cat_label} ({len(skills_in_cat)})",
                      size=11, color=NAVY, bold=True)
        skill_names = [s["skill"] for s in skills_in_cat[:6]]
        _add_text_box(slide2, Inches(3.2), y_pos, Inches(9.5), Inches(0.35),
                      ", ".join(skill_names) + ("..." if len(skills_in_cat) > 6 else ""),
                      size=10, color=MUTED_TEXT)
        y_pos += Inches(0.45)

    # ── Slide 3: Channel Recommendations ──
    slide3 = prs.slides.add_slide(prs.slide_layouts[6])
    _add_bg(slide3)
    _add_shape(slide3, Inches(0), Inches(0), Inches(13.333), Inches(1.0), NAVY)
    _add_text_box(slide3, Inches(0.5), Inches(0.2), Inches(12), Inches(0.6),
                  "Channel Recommendations", font_name=FONT_TITLE, size=24, color=WHITE, bold=True)

    channels = analysis.get("channel_recommendations", [])
    y = Inches(1.3)
    for i, ch in enumerate(channels[:8]):
        x_offset = Inches(0.5) if i % 2 == 0 else Inches(6.9)
        if i > 0 and i % 2 == 0:
            y += Inches(1.45)

        _add_shape(slide3, x_offset, y, Inches(5.8), Inches(1.3), WHITE, WARM_GRAY)
        _add_text_box(slide3, x_offset + Inches(0.2), y + Inches(0.1),
                      Inches(3.5), Inches(0.35),
                      ch.get("channel", ""),
                      size=12, color=NAVY, bold=True)
        score = ch.get("match_score", 0)
        score_color = GREEN if score >= 70 else AMBER if score >= 40 else RED_ACCENT
        _add_text_box(slide3, x_offset + Inches(4.2), y + Inches(0.1),
                      Inches(1.2), Inches(0.35),
                      f"{score}%",
                      font_name=FONT_TITLE, size=14, color=score_color, bold=True,
                      align=PP_ALIGN.RIGHT)
        _add_text_box(slide3, x_offset + Inches(0.2), y + Inches(0.5),
                      Inches(5.2), Inches(0.3),
                      ch.get("reason", ""),
                      size=9, color=MUTED_TEXT)
        _add_text_box(slide3, x_offset + Inches(0.2), y + Inches(0.85),
                      Inches(5.2), Inches(0.3),
                      f"CPC: ${ch.get('avg_cpc', 0):.2f}  |  CPA: ${ch.get('avg_cpa', 0):.2f}  |  {ch.get('platform_type', '').replace('_', ' ').title()}",
                      size=9, color=MUTED_TEXT)

    # ── Slide 4: Budget Allocation ──
    slide4 = prs.slides.add_slide(prs.slide_layouts[6])
    _add_bg(slide4)
    _add_shape(slide4, Inches(0), Inches(0), Inches(13.333), Inches(1.0), NAVY)
    alloc = analysis.get("budget_allocation", {})
    total_budget = alloc.get("total_budget", 0)
    _add_text_box(slide4, Inches(0.5), Inches(0.2), Inches(12), Inches(0.6),
                  f"Budget Allocation (${total_budget:,.0f})",
                  font_name=FONT_TITLE, size=24, color=WHITE, bold=True)

    total_apps = alloc.get("total_estimated_applications", 0)
    avg_cpa = alloc.get("estimated_cost_per_application", 0)
    _add_kpi_card(slide4, Inches(0.5), Inches(1.3), "Total Budget", f"${total_budget:,.0f}", NAVY)
    _add_kpi_card(slide4, Inches(3.6), Inches(1.3), "Est. Applications", f"{total_apps:,}", BLUE)
    _add_kpi_card(slide4, Inches(6.7), Inches(1.3), "Avg CPA", f"${avg_cpa:.2f}", TEAL)

    allocations = alloc.get("allocations", [])
    y = Inches(2.8)
    _add_shape(slide4, Inches(0.5), y, Inches(12.3), Inches(0.4), BLUE)
    headers = ["Channel", "Budget", "%", "Est. Apps", "CPA", "Match"]
    x_positions = [0.5, 3.5, 5.5, 7.0, 8.8, 10.5]
    widths_h = [3.0, 2.0, 1.5, 1.8, 1.7, 1.8]
    for hi, htext in enumerate(headers):
        _add_text_box(slide4, Inches(x_positions[hi]), y,
                      Inches(widths_h[hi]), Inches(0.4),
                      htext, size=10, color=WHITE, bold=True,
                      align=PP_ALIGN.CENTER)
    y += Inches(0.45)

    for ai, a in enumerate(allocations[:7]):
        row_bg = LIGHT_BG if ai % 2 == 0 else WHITE
        _add_shape(slide4, Inches(0.5), y, Inches(12.3), Inches(0.38), row_bg)
        row_vals = [
            a.get("channel", ""),
            f"${a.get('budget', 0):,.0f}",
            f"{a.get('percentage', 0)}%",
            f"{a.get('estimated_applications', 0):,}",
            f"${a.get('avg_cpa', 0):.2f}",
            f"{a.get('match_score', 0)}%",
        ]
        for ri, rv in enumerate(row_vals):
            _add_text_box(slide4, Inches(x_positions[ri]), y,
                          Inches(widths_h[ri]), Inches(0.38),
                          rv, size=9, color=DARK_TEXT,
                          bold=(ri == 0),
                          align=PP_ALIGN.LEFT if ri == 0 else PP_ALIGN.CENTER)
        y += Inches(0.40)

    # ── Slide 5: Talent Pool & Competition ──
    slide5 = prs.slides.add_slide(prs.slide_layouts[6])
    _add_bg(slide5)
    _add_shape(slide5, Inches(0), Inches(0), Inches(13.333), Inches(1.0), NAVY)
    _add_text_box(slide5, Inches(0.5), Inches(0.2), Inches(12), Inches(0.6),
                  "Talent Pool & Competition",
                  font_name=FONT_TITLE, size=24, color=WHITE, bold=True)

    tp = analysis.get("talent_pool_estimate", {})
    _add_kpi_card(slide5, Inches(0.5), Inches(1.3), "Total Pool",
                  f"{tp.get('estimated_total_pool', 0):,}", NAVY)
    _add_kpi_card(slide5, Inches(3.6), Inches(1.3), "Active Seekers",
                  f"{tp.get('active_seekers', 0):,}", GREEN)
    _add_kpi_card(slide5, Inches(6.7), Inches(1.3), "Passive Reachable",
                  f"{tp.get('passive_reachable', 0):,}", BLUE)

    scarcity = tp.get("pool_scarcity", "adequate").title()
    scarcity_color = GREEN if scarcity in ("Adequate", "Abundant") else AMBER if scarcity == "Tight" else RED_ACCENT
    _add_kpi_card(slide5, Inches(9.8), Inches(1.3), "Pool Scarcity", scarcity, scarcity_color)

    comp = analysis.get("competitor_insight", {})
    comp_list = comp.get("competitors", [])
    if comp_list:
        _add_text_box(slide5, Inches(0.5), Inches(2.9), Inches(12), Inches(0.4),
                      f"Competitor Landscape ({comp.get('talent_competition_level', '').replace('_', ' ').title()})",
                      size=14, color=NAVY, bold=True)

        y = Inches(3.4)
        for ci, c in enumerate(comp_list[:4]):
            x_off = Inches(0.5 + (ci % 2) * 6.4)
            if ci == 2:
                y += Inches(1.5)
            _add_shape(slide5, x_off, y, Inches(5.8), Inches(1.3), WHITE, WARM_GRAY)
            _add_text_box(slide5, x_off + Inches(0.2), y + Inches(0.1),
                          Inches(3.5), Inches(0.35),
                          c.get("company", ""),
                          size=12, color=NAVY, bold=True)
            overlap = c.get("overlap_score", 0)
            ol_color = RED_ACCENT if overlap >= 50 else AMBER if overlap >= 25 else GREEN
            _add_text_box(slide5, x_off + Inches(4.0), y + Inches(0.1),
                          Inches(1.5), Inches(0.35),
                          f"{overlap}% overlap",
                          size=11, color=ol_color, bold=True, align=PP_ALIGN.RIGHT)
            shared = c.get("shared_skills", [])
            _add_text_box(slide5, x_off + Inches(0.2), y + Inches(0.5),
                          Inches(5.2), Inches(0.3),
                          f"Shared: {', '.join(shared) if shared else 'None'}",
                          size=9, color=MUTED_TEXT)
            their_skills = c.get("their_hot_skills", [])[:4]
            _add_text_box(slide5, x_off + Inches(0.2), y + Inches(0.85),
                          Inches(5.2), Inches(0.3),
                          f"Their focus: {', '.join(their_skills)}",
                          size=9, color=MUTED_TEXT)

    # ── Slide 6: Targeting Strategy Summary ──
    slide6 = prs.slides.add_slide(prs.slide_layouts[6])
    _add_bg(slide6)
    _add_shape(slide6, Inches(0), Inches(0), Inches(13.333), Inches(1.0), NAVY)
    _add_text_box(slide6, Inches(0.5), Inches(0.2), Inches(12), Inches(0.6),
                  "Targeting Strategy Summary",
                  font_name=FONT_TITLE, size=24, color=WHITE, bold=True)

    strategies = analysis.get("targeting_strategy", [])
    y = Inches(1.3)
    for si, strat in enumerate(strategies[:4]):
        x_off = Inches(0.5 + (si % 2) * 6.4)
        if si == 2:
            y += Inches(2.8)
        card_h = Inches(2.5)
        _add_shape(slide6, x_off, y, Inches(5.8), card_h, WHITE, WARM_GRAY)
        _add_text_box(slide6, x_off + Inches(0.2), y + Inches(0.1),
                      Inches(4.0), Inches(0.35),
                      strat.get("channel", ""),
                      size=13, color=NAVY, bold=True)
        _add_text_box(slide6, x_off + Inches(4.2), y + Inches(0.1),
                      Inches(1.2), Inches(0.35),
                      f"{strat.get('match_score', 0)}%",
                      font_name=FONT_TITLE, size=14, color=BLUE, bold=True,
                      align=PP_ALIGN.RIGHT)

        params = strat.get("targeting_parameters", {})
        param_y = y + Inches(0.5)
        for pk, pv in list(params.items())[:3]:
            val_str = ", ".join(pv[:3]) if isinstance(pv, list) else str(pv)
            _add_text_box(slide6, x_off + Inches(0.2), param_y,
                          Inches(5.2), Inches(0.25),
                          f"{pk.replace('_', ' ').title()}: {val_str}",
                          size=9, color=MUTED_TEXT)
            param_y += Inches(0.28)

        tips = strat.get("ad_copy_suggestions", [])
        tip_y = param_y + Inches(0.1)
        for tip in tips[:2]:
            _add_text_box(slide6, x_off + Inches(0.2), tip_y,
                          Inches(5.2), Inches(0.25),
                          f"* {tip}",
                          size=8, color=MUTED_TEXT)
            tip_y += Inches(0.25)

    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


# =============================================================================
# UNIFIED HTTP HANDLER
# =============================================================================

def handle_skill_target_request(path: str, method: str, body: dict) -> dict:
    """Unified handler for /api/skill-target/* routes.

    Dispatches:
        POST /api/skill-target/analyze  -> analyze_skills(body)
        POST /api/skill-target/excel    -> generate_skill_report_excel(body)
        POST /api/skill-target/ppt      -> generate_skill_report_ppt(body)

    Args:
        path: The request path (e.g., "/api/skill-target/analyze")
        method: HTTP method (GET, POST, etc.)
        body: Parsed JSON body (dict)

    Returns:
        dict with keys:
            - status_code (int)
            - content_type (str)
            - body (bytes | dict)
    """
    if method.upper() != "POST":
        return {
            "status_code": 405,
            "content_type": "application/json",
            "body": {"error": "Method not allowed. Use POST.", "status": "error"},
        }

    p = path.rstrip("/").lower()

    try:
        if p.endswith("/analyze"):
            result = analyze_skills(body)
            return {
                "status_code": 200 if result.get("status") == "success" else 400,
                "content_type": "application/json",
                "body": result,
            }

        elif p.endswith("/excel"):
            if "skill_analysis" in body and "channel_recommendations" in body:
                analysis = body
            else:
                analysis = analyze_skills(body)
                if analysis.get("status") != "success":
                    return {
                        "status_code": 400,
                        "content_type": "application/json",
                        "body": analysis,
                    }

            excel_bytes = generate_skill_report_excel(analysis)
            if not excel_bytes:
                return {
                    "status_code": 500,
                    "content_type": "application/json",
                    "body": {"error": "Excel generation failed (openpyxl not available)", "status": "error"},
                }

            return {
                "status_code": 200,
                "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "body": excel_bytes,
                "filename": f"skill_target_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx",
            }

        elif p.endswith("/ppt"):
            if "skill_analysis" in body and "channel_recommendations" in body:
                analysis = body
            else:
                analysis = analyze_skills(body)
                if analysis.get("status") != "success":
                    return {
                        "status_code": 400,
                        "content_type": "application/json",
                        "body": analysis,
                    }

            ppt_bytes = generate_skill_report_ppt(analysis)
            if not ppt_bytes:
                return {
                    "status_code": 500,
                    "content_type": "application/json",
                    "body": {"error": "PPT generation failed (python-pptx not available)", "status": "error"},
                }

            return {
                "status_code": 200,
                "content_type": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                "body": ppt_bytes,
                "filename": f"skill_target_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.pptx",
            }

        else:
            return {
                "status_code": 404,
                "content_type": "application/json",
                "body": {
                    "error": f"Unknown skill-target endpoint: {path}",
                    "status": "error",
                    "available_endpoints": [
                        "POST /api/skill-target/analyze",
                        "POST /api/skill-target/excel",
                        "POST /api/skill-target/ppt",
                    ],
                },
            }

    except Exception as exc:
        logger.error("handle_skill_target_request failed: %s\n%s", exc, traceback.format_exc())
        return {
            "status_code": 500,
            "content_type": "application/json",
            "body": {"error": str(exc), "status": "error"},
        }
