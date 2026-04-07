#!/usr/bin/env python3
"""Feature Store -- pre-computed derived features for Plan Generator and Nova AI.

Provides role-family classification, seasonal hiring factors, geo cost indices,
and channel effectiveness scores.  All lookups are O(1) after initialization.

Thread-safe singleton accessed via ``get_feature_store()``.
"""

import logging
import threading
import time
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# STATIC FEATURE DATA
# ═══════════════════════════════════════════════════════════════════════════════

ROLE_FAMILIES: Dict[str, List[str]] = {
    "engineering": [
        "software",
        "engineer",
        "developer",
        "devops",
        "sre",
        "backend",
        "frontend",
        "fullstack",
        "cloud",
        "infrastructure",
        "platform",
        "embedded",
    ],
    "data_science": [
        "data scientist",
        "machine learning",
        "ai ",
        "analytics",
        "data engineer",
        "deep learning",
        "nlp",
        "computer vision",
        "statistician",
        "quantitative",
    ],
    "healthcare": [
        "nurse",
        "physician",
        "therapist",
        "pharmacist",
        "clinical",
        "medical",
        "healthcare",
        "surgeon",
        "radiologist",
        "dental",
    ],
    "sales": [
        "sales",
        "account executive",
        "business development",
        "sdr",
        "account manager",
        "revenue",
        "quota",
        "territory",
        "inside sales",
        "field sales",
    ],
    "marketing": [
        "marketing",
        "brand",
        "content",
        "seo",
        "sem",
        "growth",
        "social media",
        "digital marketing",
        "communications",
        "campaign",
        "demand gen",
    ],
    "finance": [
        "finance",
        "accounting",
        "controller",
        "cfo",
        "auditor",
        "treasury",
        "tax",
        "financial analyst",
        "bookkeeper",
        "accounts payable",
    ],
    "operations": [
        "operations",
        "supply chain",
        "logistics",
        "procurement",
        "warehouse",
        "manufacturing",
        "quality assurance",
        "production",
        "facilities",
        "fleet",
    ],
    "executive": [
        "ceo",
        "cto",
        "cfo",
        "coo",
        "cmo",
        "vp ",
        "vice president",
        "director",
        "head of",
        "chief",
        "president",
        "partner",
    ],
}

SEASONAL_FACTORS: Dict[int, float] = {
    1: 1.15,  # January -- new year hiring surge
    2: 1.10,  # February -- budget approvals
    3: 1.05,  # March -- Q1 push
    4: 1.00,  # April -- steady
    5: 0.95,  # May -- pre-summer slowdown
    6: 0.90,  # June -- summer begins
    7: 0.85,  # July -- summer trough
    8: 0.90,  # August -- early ramp-up
    9: 1.10,  # September -- fall hiring surge
    10: 1.05,  # October -- Q4 planning
    11: 0.95,  # November -- holiday slowdown
    12: 0.75,  # December -- year-end freeze
}

# S46: Industry-specific seasonal CPA multipliers (relative to baseline 1.0)
# These adjust the general SEASONAL_FACTORS for industry-specific patterns
INDUSTRY_SEASONAL_CPA: Dict[str, Dict[int, float]] = {
    "retail_consumer": {
        1: 0.85,
        2: 0.90,
        3: 0.95,
        4: 1.00,
        5: 1.00,
        6: 1.05,
        7: 1.10,
        8: 1.25,
        9: 1.35,
        10: 1.40,
        11: 1.30,
        12: 0.80,
        # Retail: CPA spikes Aug-Nov (holiday hiring), drops Dec-Jan (positions filled)
    },
    "healthcare_medical": {
        1: 1.15,
        2: 1.10,
        3: 1.05,
        4: 1.00,
        5: 1.00,
        6: 1.10,
        7: 1.15,
        8: 1.05,
        9: 0.95,
        10: 0.90,
        11: 0.95,
        12: 1.05,
        # Healthcare: CPA spikes summer (travel nurse season), dips fall (new grads)
    },
    "tech_engineering": {
        1: 1.20,
        2: 1.15,
        3: 1.10,
        4: 1.00,
        5: 0.95,
        6: 0.85,
        7: 0.80,
        8: 0.85,
        9: 1.05,
        10: 1.10,
        11: 1.00,
        12: 0.75,
        # Tech: CPA spikes Q1 (budget flush), dips summer (intern → FTE conversions)
    },
    "logistics_transportation": {
        1: 1.00,
        2: 1.00,
        3: 0.95,
        4: 0.90,
        5: 0.95,
        6: 1.05,
        7: 1.10,
        8: 1.15,
        9: 1.20,
        10: 1.25,
        11: 1.30,
        12: 1.15,
        # Logistics: CPA rises steadily toward holiday shipping peak
    },
    "finance_banking": {
        1: 1.20,
        2: 1.15,
        3: 1.10,
        4: 1.05,
        5: 1.00,
        6: 0.90,
        7: 0.85,
        8: 0.90,
        9: 1.10,
        10: 1.05,
        11: 1.00,
        12: 0.80,
        # Finance: Q1 hiring surge (bonus season), summer lull, fall ramp
    },
    "hospitality_food": {
        1: 0.80,
        2: 0.85,
        3: 0.95,
        4: 1.05,
        5: 1.20,
        6: 1.30,
        7: 1.25,
        8: 1.15,
        9: 0.90,
        10: 0.95,
        11: 1.10,
        12: 1.20,
        # Hospitality: CPA peaks summer tourist season + holiday events
    },
}

GEO_COST_INDEX: Dict[str, float] = {
    "san francisco": 1.55,
    "new york": 1.45,
    "seattle": 1.35,
    "boston": 1.30,
    "los angeles": 1.25,
    "washington dc": 1.25,
    "chicago": 1.10,
    "denver": 1.10,
    "austin": 1.05,
    "atlanta": 1.00,
    "dallas": 1.00,
    "phoenix": 0.95,
    "minneapolis": 0.95,
    "detroit": 0.85,
    "cleveland": 0.85,
    "remote": 1.00,
    "national": 1.00,
}

# Effectiveness scores 0-100 per channel per role family
CHANNEL_EFFECTIVENESS: Dict[str, Dict[str, int]] = {
    "engineering": {
        "linkedin": 90,
        "indeed": 65,
        "glassdoor": 55,
        "github_jobs": 85,
        "stack_overflow": 80,
        "referral": 95,
        "programmatic": 70,
        "career_site": 60,
    },
    "data_science": {
        "linkedin": 88,
        "indeed": 55,
        "glassdoor": 50,
        "github_jobs": 75,
        "stack_overflow": 70,
        "referral": 92,
        "programmatic": 65,
        "career_site": 55,
    },
    "healthcare": {
        "linkedin": 60,
        "indeed": 85,
        "glassdoor": 50,
        "health_ecareers": 90,
        "referral": 88,
        "programmatic": 75,
        "career_site": 70,
        "job_boards_niche": 82,
    },
    "sales": {
        "linkedin": 92,
        "indeed": 75,
        "glassdoor": 65,
        "ziprecruiter": 70,
        "referral": 85,
        "programmatic": 72,
        "career_site": 55,
        "social_media": 60,
    },
    "marketing": {
        "linkedin": 88,
        "indeed": 65,
        "glassdoor": 60,
        "referral": 82,
        "programmatic": 68,
        "career_site": 55,
        "social_media": 75,
        "creative_boards": 70,
    },
    "finance": {
        "linkedin": 85,
        "indeed": 78,
        "glassdoor": 65,
        "referral": 88,
        "programmatic": 62,
        "career_site": 60,
        "efinancial_careers": 80,
        "ziprecruiter": 58,
    },
    "operations": {
        "linkedin": 70,
        "indeed": 88,
        "glassdoor": 60,
        "ziprecruiter": 75,
        "referral": 80,
        "programmatic": 78,
        "career_site": 65,
        "job_boards_niche": 55,
    },
    "executive": {
        "linkedin": 95,
        "referral": 92,
        "executive_search": 90,
        "glassdoor": 40,
        "career_site": 50,
        "programmatic": 35,
        "indeed": 30,
        "social_media": 45,
    },
}

# Base CPC by channel (USD)
_BASE_CPC: Dict[str, float] = {
    "linkedin": 6.50,
    "indeed": 1.20,
    "glassdoor": 2.80,
    "github_jobs": 4.00,
    "stack_overflow": 4.50,
    "referral": 0.00,
    "programmatic": 0.85,
    "career_site": 0.00,
    "ziprecruiter": 1.50,
    "health_ecareers": 2.20,
    "job_boards_niche": 2.00,
    "social_media": 1.80,
    "creative_boards": 2.50,
    "efinancial_careers": 3.00,
    "executive_search": 12.00,
}


# ═══════════════════════════════════════════════════════════════════════════════
# FEATURE STORE CLASS
# ═══════════════════════════════════════════════════════════════════════════════


class FeatureStore:
    """Pre-computed feature store for Plan Generator and Nova AI.

    Thread-safe singleton.  Call ``initialize()`` once at startup, then
    use accessor methods from any thread.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._initialized = False
        self._init_time: float = 0.0

    def initialize(self) -> None:
        """Load and validate all feature data.  Idempotent."""
        with self._lock:
            if self._initialized:
                return
            start = time.time()
            # Validate data integrity
            for month in range(1, 13):
                if month not in SEASONAL_FACTORS:
                    raise ValueError(f"Missing seasonal factor for month {month}")
            for family, channels in CHANNEL_EFFECTIVENESS.items():
                if family not in ROLE_FAMILIES:
                    raise ValueError(
                        f"Channel effectiveness references unknown family: {family}"
                    )
                for ch, score in channels.items():
                    if not 0 <= score <= 100:
                        raise ValueError(
                            f"Invalid effectiveness score {score} for {family}/{ch}"
                        )
            self._init_time = time.time() - start
            self._initialized = True
            logger.info(
                "Feature store initialized in %.1fms: %d families, %d cities, %d months",
                self._init_time * 1000,
                len(ROLE_FAMILIES),
                len(GEO_COST_INDEX),
                len(SEASONAL_FACTORS),
            )

    def get_role_family(self, job_title: str) -> str:
        """Match a job title to the best-fitting role family.

        Args:
            job_title: Free-text job title (e.g. "Senior Software Engineer").

        Returns:
            Role family key (e.g. "engineering"), or "general" if no match.
        """
        title_lower = (job_title or "").lower().strip()
        if not title_lower:
            return "general"

        best_family = "general"
        best_score = 0
        for family, keywords in ROLE_FAMILIES.items():
            score = sum(1 for kw in keywords if kw in title_lower)
            if score > best_score:
                best_score = score
                best_family = family
        return best_family

    def get_seasonal_factor(self, month: int) -> float:
        """Return the seasonal hiring multiplier for a given month.

        Args:
            month: Calendar month (1-12).

        Returns:
            Multiplier relative to baseline 1.0.
        """
        return SEASONAL_FACTORS.get(month, 1.0)

    def get_industry_seasonal_cpa(self, industry: str, month: int) -> float:
        """Return industry-specific seasonal CPA multiplier.

        Combines the general seasonal factor with industry-specific patterns.

        Args:
            industry: Industry key (e.g., "retail_consumer").
            month: Calendar month (1-12).

        Returns:
            Combined CPA multiplier (general * industry-specific).
        """
        general = SEASONAL_FACTORS.get(month, 1.0)
        industry_factor = INDUSTRY_SEASONAL_CPA.get(industry, {}).get(month, 1.0)
        return round(general * industry_factor, 2)

    def get_geo_cost_index(self, location: str) -> float:
        """Return the geographic cost index for a location.

        Args:
            location: City name, metro area, or "remote".

        Returns:
            Cost multiplier relative to national average 1.0.
        """
        loc_lower = (location or "").lower().strip()
        if not loc_lower:
            return 1.0

        # Direct match
        if loc_lower in GEO_COST_INDEX:
            return GEO_COST_INDEX[loc_lower]

        # Substring match (e.g. "San Francisco, CA" -> "san francisco")
        for city, index in GEO_COST_INDEX.items():
            if city in loc_lower or loc_lower in city:
                return index

        return 1.0  # Default to national average

    def get_channel_recommendations(
        self,
        job_title: str,
        budget: float,
        location: str,
    ) -> Dict:
        """Generate full channel recommendations with geo-adjusted CPCs.

        Args:
            job_title: Target role (e.g. "Data Scientist").
            budget: Monthly budget in USD.
            location: Target hiring location.

        Returns:
            Dict with role_family, seasonal_factor, geo_index,
            and ranked channel allocations with adjusted CPCs.
        """
        import datetime as _dt

        role_family = self.get_role_family(job_title)
        current_month = _dt.datetime.now().month
        seasonal = self.get_seasonal_factor(current_month)
        geo_index = self.get_geo_cost_index(location)

        channels = CHANNEL_EFFECTIVENESS.get(role_family) or CHANNEL_EFFECTIVENESS.get(
            "operations", {}
        )

        # Sort channels by effectiveness descending
        sorted_channels = sorted(channels.items(), key=lambda x: x[1], reverse=True)

        # Allocate budget proportionally to effectiveness
        total_effectiveness = sum(score for _, score in sorted_channels) or 1
        adjusted_budget = budget * seasonal

        allocations: List[Dict] = []
        for channel, effectiveness in sorted_channels:
            share = effectiveness / total_effectiveness
            allocation = round(adjusted_budget * share, 2)
            base_cpc = _BASE_CPC.get(channel, 1.50)
            adjusted_cpc = round(base_cpc * geo_index, 2)
            estimated_clicks = int(allocation / adjusted_cpc) if adjusted_cpc > 0 else 0

            allocations.append(
                {
                    "channel": channel,
                    "effectiveness_score": effectiveness,
                    "budget_share": round(share * 100, 1),
                    "allocated_budget": allocation,
                    "base_cpc": base_cpc,
                    "geo_adjusted_cpc": adjusted_cpc,
                    "estimated_clicks": estimated_clicks,
                }
            )

        return {
            "job_title": job_title,
            "role_family": role_family,
            "location": location,
            "geo_cost_index": geo_index,
            "month": current_month,
            "seasonal_factor": seasonal,
            "total_budget": budget,
            "adjusted_budget": round(adjusted_budget, 2),
            "channels": allocations,
        }

    def get_all_features(self) -> Dict:
        """Return a summary of feature store contents for /api/health.

        Returns:
            Dict with counts, initialization status, and sample data.
        """
        return {
            "initialized": self._initialized,
            "init_time_ms": (
                round(self._init_time * 1000, 1) if self._initialized else None
            ),
            "role_families": len(ROLE_FAMILIES),
            "role_family_names": sorted(ROLE_FAMILIES.keys()),
            "seasonal_months": len(SEASONAL_FACTORS),
            "geo_cities": len(GEO_COST_INDEX),
            "channel_families": len(CHANNEL_EFFECTIVENESS),
            "total_channels": sum(len(v) for v in CHANNEL_EFFECTIVENESS.values()),
        }


# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL SINGLETON
# ═══════════════════════════════════════════════════════════════════════════════

_instance: Optional[FeatureStore] = None
_instance_lock = threading.Lock()


def get_feature_store() -> FeatureStore:
    """Return the global FeatureStore singleton (lazy-created, thread-safe)."""
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                _instance = FeatureStore()
    return _instance
