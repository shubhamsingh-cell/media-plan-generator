"""Creative Quality Score -- scores a media plan's ad creative quality.

Factors (100 pts): Compensation transparency (25), Description length (25),
Apply friction (20), Visual elements (15), Mobile optimization (15).

S50: Platform-specific recommendations wired from data/platform_ad_specs.json.
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)
_GRADES = [(90, "A"), (80, "B"), (70, "C"), (60, "D")]

# ── Platform ad specs cache ──────────────────────────────────────────────────
_AD_SPECS_CACHE: Optional[Dict[str, Any]] = None


def _load_platform_ad_specs() -> Dict[str, Any]:
    """Load platform_ad_specs.json from data/ directory (cached)."""
    global _AD_SPECS_CACHE
    if _AD_SPECS_CACHE is not None:
        return _AD_SPECS_CACHE
    try:
        spec_path = Path(__file__).parent / "data" / "platform_ad_specs.json"
        if spec_path.exists():
            raw = json.loads(spec_path.read_text(encoding="utf-8"))
            _AD_SPECS_CACHE = raw.get("platforms", raw) if isinstance(raw, dict) else {}
        else:
            _AD_SPECS_CACHE = {}
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to load platform_ad_specs.json: %s", exc)
        _AD_SPECS_CACHE = {}
    return _AD_SPECS_CACHE


def _detect_platforms(data: dict) -> List[str]:
    """Detect which ad platforms are in the plan's channel mix."""
    platforms: List[str] = []
    channels = data.get("channels") or data.get("selected_channels") or []
    if isinstance(channels, str):
        channels = [channels]

    # Also check budget allocation channel names (e.g., "social_media", "global_boards")
    ba = data.get("_budget_allocation", {}).get("channel_allocations", {})
    if isinstance(ba, dict):
        channels.extend(list(ba.keys()))

    # Also check synthesized ad_platform_analysis
    synth = data.get("_synthesized", {})
    if isinstance(synth, dict):
        ap = synth.get("ad_platform_analysis", {})
        if isinstance(ap, dict):
            for pk in ap:
                channels.append(pk)

    _PLATFORM_KEYWORDS = {
        "linkedin": "linkedin",
        "facebook": "facebook",
        "meta": "facebook",
        "instagram": "facebook",
        "social": "facebook",  # social_media category -> Facebook specs
        "tiktok": "tiktok",
        "google": "google",
        "search": "google",  # search/SEM category -> Google specs
        "indeed": "indeed",
        "glassdoor": "glassdoor",
        "job_board": "indeed",  # job_board category -> Indeed specs
        "global_board": "indeed",
        "programmatic": "google",  # programmatic -> Google display specs
        "niche": "indeed",  # niche boards -> Indeed-like specs
    }
    seen = set()
    for ch in channels:
        name = (ch if isinstance(ch, str) else str(ch.get("name", ""))).lower()
        for keyword, platform_key in _PLATFORM_KEYWORDS.items():
            if keyword in name and platform_key not in seen:
                platforms.append(platform_key)
                seen.add(platform_key)
    return platforms


def _build_platform_recommendations(
    data: dict,
) -> tuple[List[str], Dict[str, Any]]:
    """Build platform-specific creative recommendations from ad specs data.

    Returns:
        Tuple of (recommendations list, platform_specs_summary dict).
    """
    recs: List[str] = []
    summary: Dict[str, Any] = {}

    specs = _load_platform_ad_specs()
    # Also check KB-injected specs
    if not specs:
        kb = data.get("_knowledge_base", {})
        if isinstance(kb, dict):
            kb_specs = kb.get("platform_ad_specs", {})
            if isinstance(kb_specs, dict):
                specs = kb_specs.get("platforms", kb_specs)

    if not specs:
        return recs, summary

    detected = _detect_platforms(data)
    if not detected:
        return recs, summary

    for platform_key in detected:
        spec = specs.get(platform_key, {})
        if not isinstance(spec, dict) or not spec:
            continue

        platform_name = spec.get("platform", platform_key.title())
        img = spec.get("image_specs", {})
        vid = spec.get("video_specs", {})
        txt = spec.get("text_limits", {})
        formats = spec.get("formats", [])

        platform_summary: Dict[str, Any] = {"platform": platform_name}

        # Image spec recommendations
        if img:
            rec_size = img.get("recommended_size", "")
            aspect = img.get("aspect_ratio", "")
            max_mb = img.get("max_file_size_mb")
            if rec_size:
                recs.append(
                    f"{platform_name}: Use {rec_size} images"
                    f" ({aspect} aspect ratio)"
                    f"{f', max {max_mb}MB' if max_mb else ''}."
                )
                platform_summary["image_size"] = rec_size
                platform_summary["aspect_ratio"] = aspect

        # Text limit recommendations
        if txt:
            txt_parts = []
            for field, limit in txt.items():
                if isinstance(limit, int):
                    label = field.replace("_", " ").title()
                    txt_parts.append(f"{label}: {limit} chars")
            if txt_parts:
                recs.append(f"{platform_name} text limits: {'; '.join(txt_parts)}.")
                platform_summary["text_limits"] = dict(txt)

        # Video spec recommendations
        if vid:
            max_dur = vid.get("max_duration_sec")
            rec_res = vid.get("recommended_resolution", "")
            if max_dur or rec_res:
                vid_parts = []
                if max_dur:
                    vid_parts.append(f"max {max_dur}s")
                if rec_res:
                    vid_parts.append(f"{rec_res} resolution")
                recs.append(f"{platform_name} video: {', '.join(vid_parts)}.")
                platform_summary["video_specs"] = {
                    "max_duration_sec": max_dur,
                    "resolution": rec_res,
                }

        # Format availability
        if formats:
            platform_summary["available_formats"] = formats

        summary[platform_key] = platform_summary

    return recs, summary


def _grade(score: int) -> str:
    """Map numeric score to letter grade."""
    for t, g in _GRADES:
        if score >= t:
            return g
    return "F"


def score_creative_quality(data: dict) -> Dict[str, Any]:
    """Score the creative quality of a media plan.

    Returns: {score: 0-100, grade: A-F, factors: {...}, recommendations: [...],
              platform_specs: {...}}
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

    # S50: Platform-specific creative recommendations from ad specs data
    platform_recs, platform_specs = _build_platform_recommendations(data)

    return {
        "score": total,
        "grade": _grade(total),
        "factors": factors,
        "recommendations": recs + platform_recs,
        "platform_specs": platform_specs,
    }
