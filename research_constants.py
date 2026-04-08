"""Shared competitive research constants for Nova AI Suite.

Top findings from competitive research report (2025-2026), hardcoded as
constants for use across products: chatbot, plan output, PPT, Excel.

These are the 5 highest-impact findings that should appear in
recommendations sections across all Nova AI Suite outputs.

Source: Nova AI Competitive Research, 28-source recruitment advertising
        database, validated Q1 2026.
"""

from __future__ import annotations

from typing import Any, Dict, List

# ═══════════════════════════════════════════════════════════════════════════════
# TOP 5 COMPETITIVE RESEARCH FINDINGS
# ═══════════════════════════════════════════════════════════════════════════════

RESEARCH_FINDINGS: List[Dict[str, str]] = [
    {
        "id": "salary_first",
        "title": "Lead with Salary",
        "stat": "3.8x more applications",
        "detail": (
            "Job postings that display compensation in the first line receive "
            "3.8x more applications. Always show salary range upfront -- even "
            "where not legally required, transparency increases apply rates 20-40%."
        ),
        "source": "Appcast 2025 Recruitment Marketing Benchmark Report",
    },
    {
        "id": "apply_time",
        "title": "Apply Time Under 5 Minutes",
        "stat": "12.5% completion rate (3.5x better)",
        "detail": (
            "Applications completed in under 5 minutes see 12.5% completion "
            "rate vs. 3.5% for longer processes. Minimize required fields, "
            "enable resume parsing, and support one-click apply."
        ),
        "source": "Appcast & SHRM Application Completion Studies 2025",
    },
    {
        "id": "short_titles",
        "title": "Concise Job Titles (1-3 Words)",
        "stat": "6.22% apply rate vs 4.5% for longer titles",
        "detail": (
            "Titles with 1-3 words achieve 6.22% apply rate compared to 4.5% "
            "for longer titles. 'CDL Driver' outperforms 'Experienced Class A "
            "CDL Truck Driver Needed'. Keep titles search-friendly and scannable."
        ),
        "source": "Joveo Internal Data Analysis, 108K LinkedIn Jobs 2025-2026",
    },
    {
        "id": "visual_ads",
        "title": "Visual Ads Outperform Text-Only",
        "stat": "+34% more applications",
        "detail": (
            "Ads with images or video receive 34% more applications than "
            "text-only postings. Use real employee photos (not stock) -- "
            "especially for blue-collar roles where authenticity drives trust."
        ),
        "source": "LinkedIn Talent Solutions & Recruitics Creative Study 2025",
    },
    {
        "id": "mobile_first",
        "title": "Mobile-First Apply Flow",
        "stat": "65-89% of traffic is mobile",
        "detail": (
            "65% of job seekers use mobile devices (89% for blue-collar). "
            "Ensure apply flow works on phones with large tap targets, "
            "minimal scrolling, and no file upload requirements on mobile."
        ),
        "source": "Indeed Mobile Apply Report & Glassdoor Mobile Usage 2025",
    },
]

# Compact version for PPT slides (one-liner per finding)
RESEARCH_FINDINGS_SHORT: List[str] = [
    "Lead with salary: 3.8x more applications when compensation is in first line",
    "Apply time <5 min: 12.5% completion rate vs 3.5% for longer processes",
    "Short titles (1-3 words): 6.22% apply rate vs 4.5% for longer titles",
    "Visual ads: +34% more applications with images/video vs text-only",
    "Mobile-first: 65-89% of job seekers apply on mobile devices",
]

# Dict keyed by finding ID for programmatic lookup
RESEARCH_FINDINGS_MAP: Dict[str, Dict[str, str]] = {
    f["id"]: f for f in RESEARCH_FINDINGS
}


def get_plan_recommendations_text() -> List[str]:
    """Return research-backed recommendation strings for plan output sections.

    Suitable for inclusion in Excel 'Key Recommendations' and PPT
    'Recommendations' sections.

    Returns:
        List of 5 actionable recommendation strings.
    """
    return [
        (
            "Salary Transparency: Lead every posting with compensation range. "
            "Research shows 3.8x more applications when salary appears in "
            "the first line (Appcast 2025)."
        ),
        (
            "Streamline Apply Process: Target <5 minute application time. "
            "Completion rates jump from 3.5% to 12.5% with shorter forms "
            "(SHRM 2025)."
        ),
        (
            "Optimize Job Titles: Use 1-3 word titles for maximum apply rate "
            "(6.22% vs 4.5%). Avoid keyword stuffing -- searchers use short "
            "queries (Joveo 108K job analysis)."
        ),
        (
            "Add Visual Creative: Include employee photos or short videos in "
            "job ads for +34% more applications. Real photos outperform stock "
            "imagery (LinkedIn Talent Solutions 2025)."
        ),
        (
            "Mobile-First Design: 65-89% of applicants use mobile. Ensure "
            "one-tap apply, no mandatory file uploads, and responsive layouts "
            "(Indeed Mobile Report 2025)."
        ),
    ]
