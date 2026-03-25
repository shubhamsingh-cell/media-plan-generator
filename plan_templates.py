"""Plan Templates Marketplace -- curated and user-forked media plan templates.

Power users can browse winning plan configurations by category or role family,
fork them with custom budget/channel overrides, and track popularity via
usage counts and ratings.

Thread-safe singleton via module-level lock.
"""

import copy
import logging
import threading
import uuid
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════════
# BUILT-IN STARTER TEMPLATES
# ═══════════════════════════════════════════════════════════════════════════════

_BUILTIN_TEMPLATES: List[Dict] = [
    {
        "id": "tmpl-tech-startup-eng",
        "name": "Tech Startup Engineer Hiring",
        "description": (
            "Optimised for early-stage startups hiring software engineers. "
            "Heavy LinkedIn and Stack Overflow weighting with GitHub sponsorship "
            "for passive-candidate reach."
        ),
        "category": "technology",
        "role_family": "engineering",
        "budget_range": {"min": 20000, "max": 30000, "default": 25000},
        "channels": [
            {"name": "LinkedIn", "allocation_pct": 40},
            {"name": "Indeed", "allocation_pct": 25},
            {"name": "Stack Overflow", "allocation_pct": 20},
            {"name": "GitHub", "allocation_pct": 15},
        ],
        "tags": ["startup", "engineering", "software", "tech"],
        "author": "Nova AI Suite",
        "usage_count": 342,
        "rating": 4.7,
    },
    {
        "id": "tmpl-healthcare-nurse",
        "name": "Healthcare Nurse Recruiting",
        "description": (
            "Targeted channel mix for registered nurse and clinical staff hiring. "
            "Prioritises Indeed volume with niche healthcare boards and social reach."
        ),
        "category": "healthcare",
        "role_family": "nursing",
        "budget_range": {"min": 10000, "max": 20000, "default": 15000},
        "channels": [
            {"name": "Indeed", "allocation_pct": 35},
            {"name": "Health eCareers", "allocation_pct": 25},
            {"name": "Facebook", "allocation_pct": 20},
            {"name": "Google", "allocation_pct": 20},
        ],
        "tags": ["healthcare", "nursing", "clinical", "medical"],
        "author": "Nova AI Suite",
        "usage_count": 287,
        "rating": 4.5,
    },
    {
        "id": "tmpl-executive-search",
        "name": "Executive Search",
        "description": (
            "Premium channel allocation for C-suite and VP-level searches. "
            "LinkedIn dominates with boutique firm and invite-only board support."
        ),
        "category": "executive",
        "role_family": "leadership",
        "budget_range": {"min": 40000, "max": 75000, "default": 50000},
        "channels": [
            {"name": "LinkedIn", "allocation_pct": 60},
            {"name": "Spencer Stuart", "allocation_pct": 20},
            {"name": "ExecThread", "allocation_pct": 20},
        ],
        "tags": ["executive", "c-suite", "leadership", "senior"],
        "author": "Nova AI Suite",
        "usage_count": 156,
        "rating": 4.8,
    },
    {
        "id": "tmpl-high-volume-retail",
        "name": "High-Volume Retail",
        "description": (
            "Cost-efficient mix for large-scale hourly and retail hiring. "
            "Maximises applicant volume through Indeed and ZipRecruiter at low CPA."
        ),
        "category": "retail",
        "role_family": "hourly",
        "budget_range": {"min": 5000, "max": 15000, "default": 10000},
        "channels": [
            {"name": "Indeed", "allocation_pct": 40},
            {"name": "ZipRecruiter", "allocation_pct": 30},
            {"name": "Facebook", "allocation_pct": 20},
            {"name": "Craigslist", "allocation_pct": 10},
        ],
        "tags": ["retail", "hourly", "high-volume", "seasonal"],
        "author": "Nova AI Suite",
        "usage_count": 421,
        "rating": 4.3,
    },
    {
        "id": "tmpl-remote-tech",
        "name": "Remote Tech Hiring",
        "description": (
            "Channels optimised for fully-remote engineering and product roles. "
            "Blends mainstream LinkedIn with remote-first job boards for global reach."
        ),
        "category": "technology",
        "role_family": "engineering",
        "budget_range": {"min": 15000, "max": 25000, "default": 20000},
        "channels": [
            {"name": "LinkedIn", "allocation_pct": 35},
            {"name": "AngelList", "allocation_pct": 25},
            {"name": "HackerNews", "allocation_pct": 20},
            {"name": "We Work Remotely", "allocation_pct": 20},
        ],
        "tags": ["remote", "tech", "global", "distributed"],
        "author": "Nova AI Suite",
        "usage_count": 298,
        "rating": 4.6,
    },
    {
        "id": "tmpl-sales-scaling",
        "name": "Sales Team Scaling",
        "description": (
            "Balanced plan for rapidly growing an SDR/AE sales team. "
            "LinkedIn for targeted outreach, Indeed for volume, Glassdoor for employer brand."
        ),
        "category": "sales",
        "role_family": "sales",
        "budget_range": {"min": 20000, "max": 40000, "default": 30000},
        "channels": [
            {"name": "LinkedIn", "allocation_pct": 45},
            {"name": "Indeed", "allocation_pct": 25},
            {"name": "Glassdoor", "allocation_pct": 15},
            {"name": "ZipRecruiter", "allocation_pct": 15},
        ],
        "tags": ["sales", "sdr", "account-executive", "growth"],
        "author": "Nova AI Suite",
        "usage_count": 213,
        "rating": 4.4,
    },
    {
        "id": "tmpl-data-science",
        "name": "Data Science Recruitment",
        "description": (
            "Specialist channel mix for data scientists, ML engineers, and analytics roles. "
            "Kaggle and Stack Overflow capture passive technical talent alongside LinkedIn."
        ),
        "category": "technology",
        "role_family": "data_science",
        "budget_range": {"min": 25000, "max": 45000, "default": 35000},
        "channels": [
            {"name": "LinkedIn", "allocation_pct": 30},
            {"name": "Kaggle", "allocation_pct": 25},
            {"name": "Stack Overflow", "allocation_pct": 25},
            {"name": "GitHub", "allocation_pct": 20},
        ],
        "tags": ["data-science", "machine-learning", "analytics", "ai"],
        "author": "Nova AI Suite",
        "usage_count": 189,
        "rating": 4.6,
    },
    {
        "id": "tmpl-entry-level-mass",
        "name": "Entry-Level Mass Hiring",
        "description": (
            "Budget-friendly plan for large-scale entry-level and internship hiring. "
            "Maximises reach per dollar through high-volume aggregators and social ads."
        ),
        "category": "general",
        "role_family": "entry_level",
        "budget_range": {"min": 5000, "max": 12000, "default": 8000},
        "channels": [
            {"name": "Indeed", "allocation_pct": 45},
            {"name": "ZipRecruiter", "allocation_pct": 25},
            {"name": "Facebook", "allocation_pct": 20},
            {"name": "Craigslist", "allocation_pct": 10},
        ],
        "tags": ["entry-level", "internship", "mass-hiring", "budget"],
        "author": "Nova AI Suite",
        "usage_count": 376,
        "rating": 4.2,
    },
]

# ═══════════════════════════════════════════════════════════════════════════════
# THREAD-SAFE TEMPLATE STORE (SINGLETON)
# ═══════════════════════════════════════════════════════════════════════════════

_lock = threading.Lock()
_templates: Dict[str, Dict] = {}
_initialized = False


def _ensure_initialized() -> None:
    """Lazy-init: seed the store with built-in templates on first access."""
    global _initialized
    if _initialized:
        return
    with _lock:
        if _initialized:
            return
        for tmpl in _BUILTIN_TEMPLATES:
            _templates[tmpl["id"]] = copy.deepcopy(tmpl)
        _initialized = True
        logger.info(
            f"Plan templates marketplace initialised with {len(_templates)} built-in templates"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════


def list_templates(
    category: Optional[str] = None,
    role_family: Optional[str] = None,
) -> List[Dict]:
    """Return all templates, optionally filtered by category and/or role_family.

    Args:
        category: Filter by template category (e.g. 'technology', 'healthcare').
        role_family: Filter by role family (e.g. 'engineering', 'nursing').

    Returns:
        List of template dicts matching the filters (or all if no filters).
    """
    _ensure_initialized()
    with _lock:
        results = list(_templates.values())

    if category:
        cat_lower = category.lower()
        results = [t for t in results if t.get("category", "").lower() == cat_lower]

    if role_family:
        rf_lower = role_family.lower()
        results = [t for t in results if t.get("role_family", "").lower() == rf_lower]

    return results


def get_template(template_id: str) -> Optional[Dict]:
    """Retrieve a single template by ID.

    Args:
        template_id: The unique template identifier.

    Returns:
        Template dict if found, None otherwise.
    """
    _ensure_initialized()
    with _lock:
        tmpl = _templates.get(template_id)
        return copy.deepcopy(tmpl) if tmpl else None


def fork_template(template_id: str, customizations: Dict) -> Optional[Dict]:
    """Create a forked copy of a template with user-supplied overrides.

    Increments the source template's usage_count. The fork receives a new
    unique ID and records the original template as ``forked_from``.

    Args:
        template_id: ID of the source template to fork.
        customizations: Dict of fields to override (e.g. budget_range, channels, name).

    Returns:
        The newly created forked template dict, or None if source not found.
    """
    _ensure_initialized()
    with _lock:
        source = _templates.get(template_id)
        if source is None:
            return None

        # Bump usage count on the source
        source["usage_count"] = source.get("usage_count", 0) + 1

        # Deep-copy and apply customizations
        forked = copy.deepcopy(source)
        fork_id = f"fork-{uuid.uuid4().hex[:12]}"
        forked["id"] = fork_id
        forked["forked_from"] = template_id
        forked["usage_count"] = 0
        forked["rating"] = 0.0
        forked["author"] = customizations.get("author") or "Community"

        # Apply allowed overrides
        for key in (
            "name",
            "description",
            "category",
            "role_family",
            "budget_range",
            "channels",
            "tags",
        ):
            if key in customizations:
                forked[key] = customizations[key]

        _templates[fork_id] = forked
        logger.info(f"Template forked: {template_id} -> {fork_id}")
        return copy.deepcopy(forked)


def get_popular_templates(limit: int = 5) -> List[Dict]:
    """Return the most-used templates sorted by usage_count descending.

    Args:
        limit: Maximum number of templates to return.

    Returns:
        List of template dicts, most popular first.
    """
    _ensure_initialized()
    with _lock:
        all_tmpls = list(_templates.values())
    sorted_tmpls = sorted(
        all_tmpls, key=lambda t: t.get("usage_count", 0), reverse=True
    )
    return sorted_tmpls[:limit]


def get_templates_stats() -> Dict:
    """Return aggregate stats for the /api/health endpoint.

    Returns:
        Dict with total_templates, total_builtin, total_forked,
        total_forks_created (sum of usage_count), and categories.
    """
    _ensure_initialized()
    with _lock:
        all_tmpls = list(_templates.values())

    builtin_count = sum(1 for t in all_tmpls if "forked_from" not in t)
    forked_count = sum(1 for t in all_tmpls if "forked_from" in t)
    total_forks = sum(
        t.get("usage_count", 0) for t in all_tmpls if "forked_from" not in t
    )
    categories = sorted(set(t.get("category", "unknown") for t in all_tmpls))

    return {
        "status": "ok",
        "total_templates": len(all_tmpls),
        "total_builtin": builtin_count,
        "total_forked": forked_count,
        "total_forks_created": total_forks,
        "categories": categories,
    }
