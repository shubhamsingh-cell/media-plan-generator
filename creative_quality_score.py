"""Creative Quality Score -- scores a media plan's ad creative quality.

Factors (100 pts): Compensation transparency (25), Description length (25),
Apply friction (20), Visual elements (15), Mobile optimization (15).
"""

from __future__ import annotations
import logging
from typing import Any, Dict, List

logger = logging.getLogger(__name__)
_GRADES = [(90, "A"), (80, "B"), (70, "C"), (60, "D")]


def _grade(score: int) -> str:
    for t, g in _GRADES:
        if score >= t:
            return g
    return "F"


def score_creative_quality(data: dict) -> Dict[str, Any]:
    """Score the creative quality of a media plan.

    Returns: {score: 0-100, grade: A-F, factors: {...}, recommendations: [...]}
    """
    factors: Dict[str, Dict[str, Any]] = {}
    recs: List[str] = []

    # 1. Compensation Transparency (25 pts)
    has_sal = bool(
        data.get("salary_data")
        or data.get("salary_range")
        or data.get("compensation")
        or (
            isinstance(data.get("_synthesized"), dict)
            and data["_synthesized"].get("salary_intelligence")
        )
        or (
            isinstance(data.get("_enriched"), dict)
            and data["_enriched"].get("salary_data")
        )
    )
    c1 = 25 if has_sal else 0
    factors["compensation_transparency"] = {"score": c1, "max": 25, "present": has_sal}
    if not has_sal:
        recs.append(
            "Include salary ranges -- postings with pay transparency get 30%+ more applications."
        )

    # 2. Description Length (25 pts)
    desc = data.get("job_description") or data.get("description") or ""
    if not desc:
        roles = data.get("roles") or []
        desc = " ".join(str(r) for r in roles) if roles else ""
    wc = len(desc.split()) if desc else 0
    c2 = 25 if 201 <= wc <= 400 else (15 if 150 <= wc <= 500 else (5 if wc > 0 else 0))
    factors["description_length"] = {"score": c2, "max": 25, "word_count": wc}
    if c2 < 25:
        if wc < 150:
            recs.append(
                f"Expand job descriptions to 200-400 words (currently ~{wc}). Short ads underperform."
            )
        elif wc > 500:
            recs.append(
                f"Trim job descriptions to 200-400 words (currently ~{wc}). Longer ads lose attention."
            )

    # 3. Apply Friction (20 pts)
    method = (data.get("apply_method") or data.get("application_method") or "").lower()
    if "easy" in method:
        apply_m = "easy_apply"
    elif "ats" in method or "redirect" in method:
        apply_m = "ats_redirect"
    elif "standard" in method or "direct" in method:
        apply_m = "standard"
    else:
        # Infer from channels
        channels = data.get("channels") or data.get("selected_channels") or []
        apply_m = "standard"
        if isinstance(channels, list):
            for ch in channels:
                nm = (ch if isinstance(ch, str) else str(ch.get("name", ""))).lower()
                if "linkedin" in nm or "indeed" in nm:
                    apply_m = "easy_apply"
                    break
    c3 = 20 if apply_m == "easy_apply" else (10 if apply_m == "standard" else 5)
    factors["apply_friction"] = {"score": c3, "max": 20, "method": apply_m}
    if c3 < 20:
        recs.append(
            "Enable Easy Apply where possible -- it yields 2-3x higher apply rates vs ATS redirects."
        )

    # 4. Visual Elements (15 pts)
    has_vis = bool(
        data.get("has_visuals")
        or data.get("creative_assets")
        or data.get("brand_images")
        or data.get("employer_brand_video")
        or data.get("video_url")
    )
    c4 = 15 if has_vis else 5
    factors["visual_elements"] = {"score": c4, "max": 15, "present": has_vis}
    if not has_vis:
        recs.append(
            "Add employer brand images or video to job ads -- visual creatives improve CTR by 40%+."
        )

    # 5. Mobile Optimization (15 pts)
    mob = bool(data.get("mobile_optimized") or data.get("mobile_friendly"))
    if not mob:
        channels = data.get("channels") or data.get("selected_channels") or []
        mob = isinstance(channels, list) and len(channels) > 0
    c5 = 15 if mob else 5
    factors["mobile_optimization"] = {"score": c5, "max": 15, "optimized": mob}
    if not mob:
        recs.append(
            "Ensure all job ads are mobile-optimized -- 70%+ of job seekers apply via mobile."
        )

    total = c1 + c2 + c3 + c4 + c5
    return {
        "score": total,
        "grade": _grade(total),
        "factors": factors,
        "recommendations": recs,
    }
