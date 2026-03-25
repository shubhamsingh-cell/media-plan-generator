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


def detect_clearance_requirements(data: dict) -> dict[str, Any] | None:
    """Detect if the plan involves defense/government roles needing clearance.

    Scans industry, roles, and brief text for defense keywords.

    Returns:
        Clearance segmentation dict if defense-related, else None.
    """
    industry = str(data.get("industry") or "").lower()
    brief = str(data.get("use_case") or data.get("brief") or "").lower()
    client = str(data.get("client_name") or "").lower()
    roles_raw = data.get("target_roles") or data.get("roles") or []

    # Collect all text to scan
    all_text = f"{industry} {brief} {client}"
    for r in (roles_raw if isinstance(roles_raw, list) else []):
        if isinstance(r, str):
            all_text += f" {r.lower()}"
        elif isinstance(r, dict):
            all_text += f" {str(r.get('title') or '').lower()}"

    # Check for defense keywords
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
_INDUSTRY_TOP_EMPLOYERS: dict[str, dict[str, list[str]]] = {
    "technology": {
        "_national": ["Google", "Microsoft", "Amazon", "Meta", "Apple", "Netflix"],
        "san francisco": ["Salesforce", "Uber", "Stripe", "Airbnb", "Slack"],
        "seattle": ["Amazon", "Microsoft", "Boeing", "Expedia", "Zillow"],
        "new york": ["Google", "JPMorgan", "Bloomberg", "Goldman Sachs", "Meta"],
        "austin": ["Dell", "Tesla", "Oracle", "Samsung", "Indeed"],
        "boston": ["HubSpot", "Wayfair", "Akamai", "Toast", "DraftKings"],
    },
    "healthcare_medical": {
        "_national": [
            "UnitedHealth",
            "HCA Healthcare",
            "Kaiser Permanente",
            "CVS Health",
        ],
        "boston": ["Mass General", "Boston Children's", "Dana-Farber", "Brigham"],
        "houston": ["MD Anderson", "Memorial Hermann", "Houston Methodist"],
        "chicago": ["Northwestern Medicine", "Advocate", "Rush"],
    },
    "finance_banking": {
        "_national": ["JPMorgan", "Goldman Sachs", "Morgan Stanley", "Bank of America"],
        "new york": ["Citadel", "Two Sigma", "BlackRock", "Citi"],
        "charlotte": ["Bank of America", "Wells Fargo", "Truist"],
        "chicago": ["Citadel", "CME Group", "Northern Trust"],
    },
    "retail_consumer": {
        "_national": ["Walmart", "Amazon", "Target", "Costco", "Home Depot"],
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
        "huntsville": ["Boeing", "Northrop Grumman", "Raytheon"],
    },
}


def build_competitor_map(data: dict, city_data: dict) -> dict[str, Any]:
    """Build per-city/role competitor mapping.

    Args:
        data: Plan generation data dict.
        city_data: Output from enrich_city_level_data().

    Returns:
        Dict with per-city competitor lists and hiring intensity estimates.
    """
    industry = str(data.get("industry") or "general_entry_level").lower()
    roles_raw = data.get("target_roles") or data.get("roles") or []
    enriched = data.get("_enriched") or {}

    # Find the best industry match
    industry_employers = {}
    for ind_key, employers in _INDUSTRY_TOP_EMPLOYERS.items():
        if ind_key in industry or industry in ind_key:
            industry_employers = employers
            break

    national_competitors = industry_employers.get("_national", [])

    competitor_map: dict[str, Any] = {}
    for city_name in city_data:
        city_key = city_name.lower()
        local_competitors = industry_employers.get(city_key, [])

        # Merge national + local, dedup
        all_competitors = list(dict.fromkeys(local_competitors + national_competitors))[
            :8
        ]

        difficulty = city_data[city_name].get("hiring_difficulty", 5.5)
        intensity = (
            "high"
            if difficulty >= 7.0
            else ("moderate" if difficulty >= 5.0 else "low")
        )

        competitor_map[city_name] = {
            "top_employers": all_competitors,
            "local_competitors": local_competitors,
            "national_competitors": national_competitors[:5],
            "hiring_intensity": intensity,
            "estimated_competing_postings": _estimate_competing_postings(
                difficulty, len(roles_raw)
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
    """Classify each role by seniority/difficulty level.

    Returns:
        List of dicts with role title, detected seniority, and difficulty profile.
    """
    roles_raw = data.get("target_roles") or data.get("roles") or []
    results: list[dict[str, Any]] = []

    for r in (roles_raw if isinstance(roles_raw, list) else [str(roles_raw)]):
        title = ""
        if isinstance(r, str):
            title = r.strip()
        elif isinstance(r, dict):
            title = str(r.get("title") or "").strip()
        if not title:
            continue

        title_lower = f" {title.lower()} "
        detected_level = "mid"  # Default

        for level, keywords in _SENIORITY_KEYWORDS.items():
            for kw in keywords:
                if kw in title_lower:
                    detected_level = level
                    break
            if detected_level != "mid" or level == "mid":
                # Only break if we matched non-default or we're checking mid
                if any(kw in title_lower for kw in keywords):
                    detected_level = level
                    break

        profile = _DIFFICULTY_PROFILES[detected_level]
        results.append(
            {
                "role_title": title,
                "seniority_level": detected_level,
                "complexity_score": profile["complexity_score"],
                "avg_time_to_fill_days": profile["avg_time_to_fill_days"],
                "budget_weight": profile["budget_weight"],
                "channel_emphasis": profile["channel_emphasis"],
                "description": profile["description"],
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
    seniority_levels = [d["seniority_level"] for d in difficulty_results]

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
        ch_industry = info.get("industry", "")
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
    difficulty_str = str(
        synthesized.get("hiring_difficulty")
        or enriched.get("hiring_difficulty")
        or "moderate"
    ).lower()

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


def build_activation_calendar(data: dict) -> dict[str, Any]:
    """Build activation event calendar based on campaign start month and industry.

    Returns:
        Dict with monthly timeline, key events, and timing recommendations.
    """
    campaign_month = int(data.get("campaign_start_month") or 0)
    if campaign_month < 1 or campaign_month > 12:
        campaign_month = datetime.datetime.now().month

    industry = str(data.get("industry") or "").lower()

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

        timeline.append(
            {
                "month": month_num,
                "month_name": month_name,
                "offset_from_start": offset,
                "season": month_info["season"],
                "hiring_intensity": month_info["hiring_intensity"],
                "budget_weight": budget_weight,
                "key_events": month_info["events"],
                "recommendation": month_info["recommendation"],
            }
        )

    # Industry-specific events
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
            "AHA Annual (Nov)",
            "APHA (Oct)",
            "Nursing conferences (quarterly)",
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
