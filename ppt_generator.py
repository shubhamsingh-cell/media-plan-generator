#!/usr/bin/env python3
"""
Joveo-branded PowerPoint generator for AI Media Planner.

Generates a polished, data-driven .pptx presentation using python-pptx.
Uses Joveo brand identity: Port Gore navy (#202058), Blue Violet purple (#5A54BD),
Downy teal (#6BB3CD), Light Purple (#8B85E0), Light Teal (#A8D8EA), Gold (#FFD700).
Fonts: Calibri (titles & body, widely available). Incorporates hero stats, section
dividers, quality outcomes grids, channel attribution diagrams, and comparison panels.

Note: This module does not directly import data_orchestrator.py. It receives
orchestrated/enriched data transitively via app.py, which calls the orchestrator
and passes the enriched results into the PPT generation functions.
"""

import io
import logging
import os
import re
import datetime
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

from shared_utils import (
    parse_budget_display,
    INDUSTRY_LABEL_MAP as _SHARED_INDUSTRY_LABEL_MAP,
)

from pptx import Presentation
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN, MSO_ANCHOR
from pptx.enum.shapes import MSO_SHAPE

try:
    import research
except ImportError:
    research = None

try:
    import trend_engine as _trend_engine

    _HAS_TREND_ENGINE = True
except ImportError:
    _HAS_TREND_ENGINE = False

try:
    import collar_intelligence as _collar_intel

    _HAS_COLLAR_INTEL = True
except ImportError:
    _HAS_COLLAR_INTEL = False

try:
    import matplotlib

    matplotlib.use("Agg")  # Non-interactive backend for server use
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches

    _HAS_MATPLOTLIB = True
except ImportError:
    _HAS_MATPLOTLIB = False


# ---------------------------------------------------------------------------
# Chart color palette (hex strings for matplotlib, matching Joveo brand)
# ---------------------------------------------------------------------------
_CHART_COLORS = [
    "#5A54BD",  # Blue Violet (Joveo primary accent)
    "#6BB3CD",  # Downy Teal (Joveo secondary accent)
    "#8B85E0",  # Light Purple (Joveo extended)
    "#A8D8EA",  # Light Teal (Joveo extended)
    "#FFD700",  # Gold (emphasis)
    "#202058",  # Port Gore Navy
    "#CE9047",  # Bronze accent
    "#338721",  # Green (positive)
    "#B5669C",  # Pink accent
    "#14B8A6",  # Teal accent
]


def _generate_pie_chart_image(labels: List[str], sizes: List[float]) -> Optional[bytes]:
    """Generate a budget allocation pie chart as PNG bytes using matplotlib.

    Args:
        labels: Channel names.
        sizes: Percentage allocations (should sum to ~100).

    Returns:
        PNG image bytes, or None if matplotlib is unavailable or chart fails.
    """
    if not _HAS_MATPLOTLIB or not labels or not sizes:
        return None
    try:
        fig, ax = plt.subplots(figsize=(7, 5), dpi=150)
        fig.patch.set_facecolor("#FFFDF9")

        colors = _CHART_COLORS[: len(labels)]
        # Extend colors if we have more channels than palette entries
        while len(colors) < len(labels):
            colors.append(_CHART_COLORS[len(colors) % len(_CHART_COLORS)])

        wedges, texts, autotexts = ax.pie(
            sizes,
            labels=None,
            autopct=lambda pct: f"{pct:.1f}%" if pct >= 3 else "",
            startangle=90,
            colors=colors,
            pctdistance=0.75,
            wedgeprops={"linewidth": 1.5, "edgecolor": "white"},
        )

        for autotext in autotexts:
            autotext.set_fontsize(9)
            autotext.set_fontweight("bold")
            autotext.set_color("white")

        # Add legend to the right
        legend_labels = [f"{lbl} ({sz:.0f}%)" for lbl, sz in zip(labels, sizes)]
        ax.legend(
            wedges,
            legend_labels,
            title="Channels",
            loc="center left",
            bbox_to_anchor=(1.0, 0.5),
            fontsize=8,
            title_fontsize=9,
            frameon=False,
        )

        ax.set_title(
            "Budget Allocation by Channel",
            fontsize=13,
            fontweight="bold",
            color="#202058",
            pad=15,
        )

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(
            buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor()
        )
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Pie chart generation failed (non-fatal): %s", exc)
        if "fig" in dir():
            try:
                plt.close(fig)
            except Exception:
                pass
        return None


def _generate_funnel_chart_image(
    impressions: int,
    clicks: int,
    applications: int,
    hires: int,
) -> Optional[bytes]:
    """Generate a horizontal funnel chart as PNG bytes using matplotlib.

    Shows the conversion funnel: Impressions -> Clicks -> Applications -> Hires.

    Args:
        impressions: Total impressions.
        clicks: Total clicks.
        applications: Total applications.
        hires: Total hires.

    Returns:
        PNG image bytes, or None if matplotlib is unavailable or chart fails.
    """
    if not _HAS_MATPLOTLIB:
        return None
    try:
        stages = ["Impressions", "Clicks", "Applications", "Hires"]
        values = [
            max(impressions, 1),
            max(clicks, 1),
            max(applications, 1),
            max(hires, 1),
        ]

        fig, ax = plt.subplots(figsize=(8, 4.5), dpi=150)
        fig.patch.set_facecolor("#FFFDF9")

        funnel_colors = ["#202058", "#5A54BD", "#6BB3CD", "#338721"]
        max_val = values[0]

        y_positions = [3.0, 2.0, 1.0, 0.0]
        bar_height = 0.65

        for i, (stage, val, color) in enumerate(zip(stages, values, funnel_colors)):
            width = max(val / max_val, 0.04)  # Minimum visible width
            # Center-align bars
            left = (1.0 - width) / 2.0
            bar = ax.barh(
                y_positions[i],
                width,
                left=left,
                height=bar_height,
                color=color,
                edgecolor="white",
                linewidth=1.5,
                zorder=2,
            )

            # Stage label on the left
            ax.text(
                0.0,
                y_positions[i],
                stage,
                ha="right",
                va="center",
                fontsize=10,
                fontweight="bold",
                color="#202058",
                transform=ax.get_yaxis_transform(),
            )

            # Value label centered in bar
            display_val = f"{val:,}"
            ax.text(
                0.5,
                y_positions[i],
                display_val,
                ha="center",
                va="center",
                fontsize=10,
                fontweight="bold",
                color="white",
                zorder=3,
            )

            # Conversion rate arrow between stages
            if i < len(values) - 1:
                conv_rate = values[i + 1] / values[i] * 100
                ax.annotate(
                    f"{conv_rate:.1f}%",
                    xy=(0.5, y_positions[i] - bar_height / 2 - 0.05),
                    ha="center",
                    va="top",
                    fontsize=8,
                    color="#596780",
                    fontstyle="italic",
                )

        ax.set_xlim(0, 1.0)
        ax.set_ylim(-0.6, 3.8)
        ax.axis("off")

        ax.set_title(
            "Recruitment Conversion Funnel",
            fontsize=13,
            fontweight="bold",
            color="#202058",
            pad=15,
        )

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(
            buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor()
        )
        plt.close(fig)
        buf.seek(0)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Funnel chart generation failed (non-fatal): %s", exc)
        if "fig" in dir():
            try:
                plt.close(fig)
            except Exception:
                pass
        return None


# ---------------------------------------------------------------------------
# Brand name casing -- preserves known brand names when title-casing client
# ---------------------------------------------------------------------------
_BRAND_CASING: dict[str, str] = {
    "fedex": "FedEx",
    "linkedin": "LinkedIn",
    "youtube": "YouTube",
    "ibm": "IBM",
    "ups": "UPS",
    "jpmorgan": "JPMorgan",
    "walmart": "Walmart",
    "mcdonalds": "McDonald's",
    "at&t": "AT&T",
    "bmw": "BMW",
    "dhl": "DHL",
    "usps": "USPS",
    "xpo": "XPO",
    "jb hunt": "J.B. Hunt",
    "j.b. hunt": "J.B. Hunt",
    "hca": "HCA",
    "cvs": "CVS",
    "ge": "GE",
    "3m": "3M",
    "bp": "BP",
    "ihg": "IHG",
}


def _proper_client_name(name: str) -> str:
    """Title-case a client name, preserving known brand casing."""
    if not name or name == "Client":
        return name
    lower = name.strip().lower()
    if lower in _BRAND_CASING:
        return _BRAND_CASING[lower]
    return (
        name.strip().title()
        if name == name.lower() or name == name.upper()
        else name.strip()
    )


# ---------------------------------------------------------------------------
# Constants & Color Palette (Joveo brand identity)
# Primary: Port Gore #202058  |  Accent: Blue Violet #5A54BD
# Secondary: Downy Teal #6BB3CD  |  Extended: Light Purple #8B85E0
# Extended: Light Teal #A8D8EA  |  Emphasis: Gold #FFD700
# ---------------------------------------------------------------------------

# -- Joveo brand primaries --
NAVY = RGBColor(0x20, 0x20, 0x58)  # Port Gore -- primary dark / headings
BLUE = RGBColor(0x5A, 0x54, 0xBD)  # Blue Violet -- primary accent
MEDIUM_BLUE = RGBColor(0x48, 0x43, 0x9E)  # Deeper purple accent
LIGHT_BLUE = RGBColor(0xDD, 0xDB, 0xFF)  # Light purple background
PALE_BLUE = RGBColor(0xB8, 0xB4, 0xF7)  # Medium purple accent fill
SKY_BLUE = RGBColor(0xA8, 0xD8, 0xEA)  # Light teal (Joveo extended)

# -- Joveo secondary --
TEAL = RGBColor(0x6B, 0xB3, 0xCD)  # Downy Teal -- secondary accent
LIGHT_TEAL = RGBColor(0xA8, 0xD8, 0xEA)  # Light teal (Joveo extended)
PALE_TEAL = RGBColor(0xDA, 0xF5, 0xFF)  # Pale teal background

# -- Neutrals --
WHITE = RGBColor(0xFF, 0xFF, 0xFF)
OFF_WHITE = RGBColor(0xFF, 0xFD, 0xF9)  # Floral White (page bg)
WARM_WHITE = RGBColor(0xFF, 0xFD, 0xF9)  # Card backgrounds
WARM_GRAY = RGBColor(0xEB, 0xE6, 0xE0)  # Borders, dividers
MEDIUM_GRAY = RGBColor(0xD6, 0xCF, 0xC2)  # Subtle separators

# -- Text colors --
DARK_TEXT = RGBColor(0x20, 0x20, 0x58)  # Port Gore for body text
MUTED_TEXT = RGBColor(0x59, 0x67, 0x80)  # Secondary text
LIGHT_MUTED = RGBColor(0x8C, 0x96, 0xA8)  # Tertiary text

# -- Semantic colors --
GREEN = RGBColor(0x33, 0x87, 0x21)  # Positive / beating benchmark
LIGHT_GREEN = RGBColor(0xE6, 0xF2, 0xE0)  # Green background
AMBER = RGBColor(0xCE, 0x90, 0x47)  # Bronze -- trailing benchmark
LIGHT_AMBER = RGBColor(0xFD, 0xDB, 0xB2)  # Light bronze background
RED_ACCENT = RGBColor(0xB5, 0x66, 0x9C)  # Pink -- underperformance accent
GOLD = RGBColor(0xFF, 0xD7, 0x00)  # Gold -- emphasis / highlights

# -- Joveo extended palette --
JOVEO_LIGHT_PURPLE = RGBColor(0x8B, 0x85, 0xE0)  # Light purple accent
JOVEO_BRONZE = RGBColor(0xCE, 0x90, 0x47)  # Bronze accent / CTAs
JOVEO_PINK = RGBColor(0xB5, 0x66, 0x9C)  # Pink / magenta accent

# -- Fonts (Calibri: widely available, clean sans-serif) --
FONT_FAMILY = "Calibri"  # Brand title font
FONT_BODY = "Calibri"  # Brand body font

# Slide dimensions (16:9 widescreen)
SLIDE_WIDTH = Inches(13.333)
SLIDE_HEIGHT = Inches(7.5)


def _trunc_word(s: str, maxlen: int = 500) -> str:
    """Truncate text at word boundary to prevent PPT text box overflow."""
    s = str(s)
    if len(s) <= maxlen:
        return s
    return s[:maxlen].rsplit(" ", 1)[0] + "..."


# ---------------------------------------------------------------------------
# Industry Benchmark Data
# NOTE: Canonical benchmark source is trend_engine.py. These values are fallbacks only.
# See trend_engine.get_benchmark() for authoritative CPC/CPA/CPM data with
# seasonal, regional, and collar-type adjustments. The _get_benchmarks()
# function below attempts to use trend_engine first, falling back to these.
# ---------------------------------------------------------------------------

BENCHMARKS: Dict[str, Dict[str, str]] = {
    "healthcare_medical": {
        "cpa": "$35 - $85",
        "cpc": "$0.90 - $3.50",
        "cph": "$9K - $12K",
        "apply_rate": "3.2% - 4.5%",
    },
    "tech_engineering": {
        "cpa": "$25 - $75",
        "cpc": "$1.20 - $4.50",
        "cph": "$6K - $22K",
        "apply_rate": "6.41%",
    },
    "retail_consumer": {
        "cpa": "$8 - $21",
        "cpc": "$0.25 - $1.00",
        "cph": "$2.7K - $4K",
        "apply_rate": "4.5% - 5.8%",
    },
    "general_entry_level": {
        "cpa": "$10 - $25",
        "cpc": "$0.35 - $1.30",
        "cph": "$2K - $4.7K",
        "apply_rate": "5.5% - 6.1%",
    },
    "finance_banking": {
        "cpa": "$21 - $65",
        "cpc": "$0.90 - $3.50",
        "cph": "$5K - $12K",
        "apply_rate": "5.0% - 6.0%",
    },
    "logistics_supply_chain": {
        "cpa": "$15 - $52",
        "cpc": "$0.40 - $1.80",
        "cph": "$4.5K - $8K",
        "apply_rate": "4.0% - 5.2%",
    },
    "hospitality_travel": {
        "cpa": "$8 - $25",
        "cpc": "$0.22 - $1.00",
        "cph": "$2.5K - $4K",
        "apply_rate": "4.0% - 5.0%",
    },
    "blue_collar_trades": {
        "cpa": "$12 - $35",
        "cpc": "$0.40 - $1.60",
        "cph": "$3.5K - $5.6K",
        "apply_rate": "4.0% - 5.5%",
    },
    "pharma_biotech": {
        "cpa": "$40 - $110",
        "cpc": "$1.50 - $5.00",
        "cph": "$8K - $18K",
        "apply_rate": "3.8% - 5.2%",
    },
}

# ---------------------------------------------------------------------------
# Industry-Specific Hiring Challenges (Complication column)
# ---------------------------------------------------------------------------

COMPLICATIONS: Dict[str, List[str]] = {
    "healthcare_medical": [
        "Clinical talent shortages persist nationally",
        "CPA exceeds $35+ for standing-up roles",
        "Burnout driving 18% higher churn vs. 2023",
        "Credentialing requirements slow time-to-fill",
    ],
    "tech_engineering": [
        "White-collar recession creating surplus but CPCs remain high",
        "AI/ML roles still command premium sourcing costs",
        "Senior/specialized roles still average 45+ days to fill",
        "Remote-first expectations complicate geo-targeting",
    ],
    "retail_consumer": [
        "64,000 retail jobs shed in 2025 Q1",
        "Retail CPA rising sharply despite market softening",
        "5,800+ store closures accelerating talent displacement",
        "Seasonal demand creates volatile cost spikes",
    ],
    "hospitality_travel": [
        "Hospitality CPA surging as travel demand outpaces labor supply",
        "Extreme seasonal demand swings in Q2/Q4",
        "High turnover-driven churn exceeds 73%",
        "Hourly wage competition from adjacent industries",
    ],
    "logistics_supply_chain": [
        "Logistics CPA rising significantly as warehouse/CDL demand grows",
        "CDL/last-mile roles most expensive at $52+ CPA",
        "Automation creating new hybrid role types",
        "Warehouse labor competing with gig economy",
    ],
    "finance_banking": [
        "Finance CPA climbing as compliance and fintech hiring intensify",
        "Compliance-heavy hiring extends cycles by 2-3 weeks",
        "Extensive background checks inflate cost-per-hire",
        "Fintech competition drawing mid-career talent",
    ],
    "general_entry_level": [
        "CPCs trending upward across entry-level roles",
        "Seasonal Q4 spikes compress planning windows",
        "Apply rates improving but quality remains a challenge",
        "High-volume funnels require aggressive top-of-funnel spend",
    ],
    "blue_collar_trades": [
        "Skilled trades gap widening as workforce ages",
        "CPA elevated for certified/licensed positions",
        "Geographic mismatch between supply and demand",
        "Apprenticeship pipelines insufficient for near-term needs",
    ],
    "pharma_biotech": [
        "Regulatory talent scarcity drives $110+ CPAs",
        "Clinical trial staffing requires hyper-niche sourcing",
        "PhD-level roles average 60+ days to fill",
        "Compliance training costs add $3K-$5K per hire",
    ],
}

# ---------------------------------------------------------------------------
# Default Channel Allocations
# ---------------------------------------------------------------------------

# Default allocation (used as fallback)
CHANNEL_ALLOC: Dict[str, Dict[str, Any]] = {
    "programmatic_dsp": {
        "label": "Programmatic DSP",
        "pct": 35,
        "color": NAVY,
        "category": "Programmatic",
    },
    "global_boards": {
        "label": "Global Job Boards",
        "pct": 20,
        "color": BLUE,
        "category": "Job Boards",
    },
    "niche_boards": {
        "label": "Niche / Industry Boards",
        "pct": 15,
        "color": MEDIUM_BLUE,
        "category": "Job Boards",
    },
    "social_media": {
        "label": "Social Media",
        "pct": 12,
        "color": SKY_BLUE,
        "category": "Social",
    },
    "regional_boards": {
        "label": "Regional Boards",
        "pct": 8,
        "color": PALE_BLUE,
        "category": "Job Boards",
    },
    "employer_branding": {
        "label": "Employer Branding",
        "pct": 5,
        "color": TEAL,
        "category": "Employer Brand",
    },
    "apac_regional": {
        "label": "APAC Regional",
        "pct": 3,
        "color": LIGHT_TEAL,
        "category": "Job Boards",
    },
    "emea_regional": {
        "label": "EMEA Regional",
        "pct": 2,
        "color": PALE_TEAL,
        "category": "Job Boards",
    },
}

# ═══════════════════════════════════════════════════════════════════════════════
# INDUSTRY ALLOCATION PROFILES
# Methodology: Derived from analysis of 200+ recruitment media campaigns across
# 17 industries (2024-2025). Validated against Appcast 2026 Recruitment Marketing
# Benchmark Report, LinkedIn Talent Solutions data, and Joveo first-party campaign
# data (66M+ views, 11M+ clicks across 2 accounts).
# Re-calibration: Quarterly review against live campaign performance data.
# Last validated: Q1 2026
# ═══════════════════════════════════════════════════════════════════════════════

# ── Industry-specific allocation profiles ──
# Each profile shifts percentages to match industry hiring patterns.
# The channel keys match CHANNEL_ALLOC keys; only "pct" differs.
INDUSTRY_ALLOC_PROFILES: Dict[str, Dict[str, int]] = {
    # Healthcare: heavier on niche medical boards, less programmatic
    "healthcare_medical": {
        "programmatic_dsp": 22,
        "global_boards": 15,
        "niche_boards": 30,
        "social_media": 10,
        "regional_boards": 10,
        "employer_branding": 8,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Tech: heavier on programmatic/digital and social, moderate niche
    "tech_engineering": {
        "programmatic_dsp": 30,
        "global_boards": 15,
        "niche_boards": 20,
        "social_media": 18,
        "regional_boards": 5,
        "employer_branding": 7,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Finance: balanced with strong niche and employer branding
    "finance_banking": {
        "programmatic_dsp": 25,
        "global_boards": 18,
        "niche_boards": 25,
        "social_media": 10,
        "regional_boards": 7,
        "employer_branding": 10,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Retail/consumer & hospitality: high-volume programmatic + social
    "retail_consumer": {
        "programmatic_dsp": 38,
        "global_boards": 22,
        "niche_boards": 8,
        "social_media": 20,
        "regional_boards": 7,
        "employer_branding": 3,
        "apac_regional": 1,
        "emea_regional": 1,
    },
    "hospitality_travel": {
        "programmatic_dsp": 38,
        "global_boards": 22,
        "niche_boards": 8,
        "social_media": 20,
        "regional_boards": 7,
        "employer_branding": 3,
        "apac_regional": 1,
        "emea_regional": 1,
    },
    # General / entry-level: programmatic-heavy, broad reach
    "general_entry_level": {
        "programmatic_dsp": 40,
        "global_boards": 22,
        "niche_boards": 8,
        "social_media": 15,
        "regional_boards": 10,
        "employer_branding": 3,
        "apac_regional": 1,
        "emea_regional": 1,
    },
    # Blue-collar/trades: programmatic + regional, less niche digital
    "blue_collar_trades": {
        "programmatic_dsp": 35,
        "global_boards": 20,
        "niche_boards": 10,
        "social_media": 15,
        "regional_boards": 15,
        "employer_branding": 3,
        "apac_regional": 1,
        "emea_regional": 1,
    },
    # Aerospace/defense: niche-heavy, security-cleared boards matter
    "aerospace_defense": {
        "programmatic_dsp": 20,
        "global_boards": 15,
        "niche_boards": 30,
        "social_media": 8,
        "regional_boards": 10,
        "employer_branding": 12,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Pharma/biotech: niche scientific boards + employer branding
    "pharma_biotech": {
        "programmatic_dsp": 22,
        "global_boards": 15,
        "niche_boards": 28,
        "social_media": 10,
        "regional_boards": 8,
        "employer_branding": 12,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Education: niche-heavy (HigherEdJobs etc.), moderate social
    "education": {
        "programmatic_dsp": 20,
        "global_boards": 18,
        "niche_boards": 28,
        "social_media": 12,
        "regional_boards": 10,
        "employer_branding": 7,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Legal services: niche + employer brand focused
    "legal_services": {
        "programmatic_dsp": 22,
        "global_boards": 18,
        "niche_boards": 28,
        "social_media": 8,
        "regional_boards": 8,
        "employer_branding": 11,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Manufacturing/automotive: programmatic + regional + niche trade
    "automotive": {
        "programmatic_dsp": 30,
        "global_boards": 18,
        "niche_boards": 18,
        "social_media": 10,
        "regional_boards": 15,
        "employer_branding": 5,
        "apac_regional": 2,
        "emea_regional": 2,
    },
    # Energy/utilities: niche trade boards + regional
    "energy_utilities": {
        "programmatic_dsp": 25,
        "global_boards": 15,
        "niche_boards": 25,
        "social_media": 8,
        "regional_boards": 15,
        "employer_branding": 7,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Mental health: niche clinical + employer brand
    "mental_health": {
        "programmatic_dsp": 22,
        "global_boards": 18,
        "niche_boards": 28,
        "social_media": 10,
        "regional_boards": 8,
        "employer_branding": 9,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Logistics/supply chain/trucking: niche CDL boards dominate for
    # trucking/transportation (CDLjobs, TruckersReport, DriveMyWay),
    # programmatic for volume, regional for local routes.
    # S49 Issue 15: Raised niche_boards from 12% to 35% for trucking alignment.
    "logistics_supply_chain": {
        "programmatic_dsp": 25,
        "global_boards": 15,
        "niche_boards": 35,  # CDLjobs, TruckersReport, DriveMyWay
        "social_media": 10,
        "regional_boards": 10,
        "employer_branding": 5,
        "apac_regional": 0,
        "emea_regional": 0,
    },
    # Insurance: niche + professional boards
    "insurance": {
        "programmatic_dsp": 25,
        "global_boards": 18,
        "niche_boards": 25,
        "social_media": 10,
        "regional_boards": 7,
        "employer_branding": 10,
        "apac_regional": 3,
        "emea_regional": 2,
    },
    # Maritime/marine: niche-heavy, regional
    "maritime_marine": {
        "programmatic_dsp": 20,
        "global_boards": 15,
        "niche_boards": 30,
        "social_media": 8,
        "regional_boards": 15,
        "employer_branding": 7,
        "apac_regional": 3,
        "emea_regional": 2,
    },
}


def _get_industry_alloc(
    industry: str,
    budget_str: str = "",
    num_roles: int = 0,
    roles: list = None,
    locations: list = None,
) -> Dict[str, Dict[str, Any]]:
    """Return a copy of CHANNEL_ALLOC with percentages adjusted for industry, budget, roles, locations."""
    # Use manual dict copy to avoid deepcopy issues with RGBColor objects
    base = {k: dict(v) for k, v in CHANNEL_ALLOC.items()}

    # Step 1: Apply industry profile
    profile = INDUSTRY_ALLOC_PROFILES.get(industry)
    if profile:
        for key in base:
            if key in profile:
                base[key]["pct"] = profile[key]

    # Step 2: Adjust for budget size
    budget_val = _parse_budget_number(budget_str) if budget_str else None
    if budget_val is not None:
        if budget_val < 50000:
            # Small budget: concentrate on top 3-4 channels, cut low-impact ones
            base["employer_branding"]["pct"] = max(
                1, base["employer_branding"]["pct"] - 3
            )
            base["apac_regional"]["pct"] = max(0, base["apac_regional"]["pct"] - 2)
            base["emea_regional"]["pct"] = max(0, base["emea_regional"]["pct"] - 1)
            base["programmatic_dsp"]["pct"] += 4
            base["global_boards"]["pct"] += 2
        elif budget_val > 500000:
            # Large budget: spread wider, invest in branding
            base["employer_branding"]["pct"] += 4
            base["regional_boards"]["pct"] += 2
            base["social_media"]["pct"] += 2
            base["programmatic_dsp"]["pct"] -= 5
            base["global_boards"]["pct"] -= 3

    # Step 3: Adjust for number of roles (more roles = more diverse mix)
    if num_roles and num_roles > 10:
        base["niche_boards"]["pct"] += 3
        base["regional_boards"]["pct"] += 2
        base["programmatic_dsp"]["pct"] -= 3
        base["global_boards"]["pct"] -= 2

    # Step 4: Adjust for seniority mix (if roles provided)
    if roles:
        _role_strs = [
            (
                r
                if isinstance(r, str)
                else (
                    r.get("title", r.get("role", str(r)))
                    if isinstance(r, dict)
                    else str(r)
                )
            )
            for r in roles
        ]
        roles_lower = " ".join(r.lower() for r in _role_strs)
        senior_keywords = [
            "executive",
            "director",
            "vp",
            "chief",
            "president",
            "c-suite",
            "senior",
            "head of",
            "principal",
            "fellow",
        ]
        junior_keywords = [
            "intern",
            "entry",
            "junior",
            "associate",
            "trainee",
            "assistant",
            "coordinator",
            "clerk",
        ]
        senior_count = sum(1 for kw in senior_keywords if kw in roles_lower)
        junior_count = sum(1 for kw in junior_keywords if kw in roles_lower)

        if senior_count > junior_count:
            # Senior-heavy: more niche/executive boards, more employer branding
            base["niche_boards"]["pct"] += 4
            base["employer_branding"]["pct"] += 3
            base["social_media"]["pct"] -= 3
            base["programmatic_dsp"]["pct"] -= 4
        elif junior_count > senior_count:
            # Junior-heavy: more social, more global boards
            base["social_media"]["pct"] += 5
            base["global_boards"]["pct"] += 3
            base["niche_boards"]["pct"] -= 4
            base["employer_branding"]["pct"] -= 2
            base["programmatic_dsp"]["pct"] -= 2

    # Step 5: Geo-filter -- remove APAC/EMEA for US-only plans
    if locations:
        _locs_list = locations if isinstance(locations, list) else [locations]
        _us_terms = {"us", "usa", "united states", ""}
        _all_us = all(
            str(loc.get("country") if isinstance(loc, dict) else loc).lower().strip()
            in _us_terms
            for loc in _locs_list
        )
        if _all_us:
            for _intl_key in ("apac_regional", "emea_regional"):
                if _intl_key in base:
                    _freed = base[_intl_key]["pct"]
                    base[_intl_key]["pct"] = 0
                    if _freed > 0:
                        _top = max(
                            (k for k in base if k != _intl_key),
                            key=lambda k: base[k]["pct"],
                        )
                        base[_top]["pct"] += _freed

    # Ensure no negative percentages
    for key in base:
        base[key]["pct"] = max(
            0 if key in ("apac_regional", "emea_regional") else 1, base[key]["pct"]
        )

    # Filter out zero-allocation channels
    base = {k: v for k, v in base.items() if v["pct"] > 0}

    # Normalize to 100%
    total = sum(v["pct"] for v in base.values())
    if total > 0 and total != 100:
        for key in base:
            base[key]["pct"] = round(base[key]["pct"] / total * 100)
        diff = 100 - sum(v["pct"] for v in base.values())
        if diff != 0:
            # Add remainder to largest category
            largest = max(base, key=lambda k: base[k]["pct"])
            base[largest]["pct"] += diff

    return base


# Human-readable goal labels
GOAL_LABELS: Dict[str, str] = {
    "brand_awareness": "Brand Awareness",
    "high_volume": "High-Volume Hiring",
    "diversity_hiring": "Diversity & Inclusion",
    "cost_efficiency": "Cost Efficiency",
    "quality_candidates": "Quality Candidates",
    "passive_talent": "Passive Talent Reach",
    "employer_branding": "Employer Branding",
    "retention": "Retention Focus",
    "speed_to_hire": "Speed to Hire",
    "geographic_expansion": "Geographic Expansion",
}

WORK_ENV_LABELS: Dict[str, str] = {
    "hybrid": "Hybrid",
    "remote": "Remote",
    "on_site": "On-Site",
    "on-site": "On-Site",
    "flexible": "Flexible",
}

# Industry benchmark comparison data for side-by-side panel
INDUSTRY_BENCHMARKS_COMPARISON: Dict[str, Dict[str, Any]] = {
    "healthcare_medical": {
        "avg_channels": 4,
        "avg_budget_pct_programmatic": 28,
        "avg_apply_rate": 3.8,
        "avg_time_to_fill": 42,
        "avg_cpa": 60,
        "estimated_reach_multiplier": 1.0,
    },
    "tech_engineering": {
        "avg_channels": 5,
        "avg_budget_pct_programmatic": 32,
        "avg_apply_rate": 6.4,
        "avg_time_to_fill": 35,
        "avg_cpa": 50,
        "estimated_reach_multiplier": 1.1,
    },
    "retail_consumer": {
        "avg_channels": 3,
        "avg_budget_pct_programmatic": 25,
        "avg_apply_rate": 5.1,
        "avg_time_to_fill": 28,
        "avg_cpa": 14,
        "estimated_reach_multiplier": 0.9,
    },
    "general_entry_level": {
        "avg_channels": 4,
        "avg_budget_pct_programmatic": 30,
        "avg_apply_rate": 5.8,
        "avg_time_to_fill": 30,
        "avg_cpa": 18,
        "estimated_reach_multiplier": 1.0,
    },
    "finance_banking": {
        "avg_channels": 4,
        "avg_budget_pct_programmatic": 30,
        "avg_apply_rate": 5.5,
        "avg_time_to_fill": 38,
        "avg_cpa": 43,
        "estimated_reach_multiplier": 1.0,
    },
    "logistics_supply_chain": {
        "avg_channels": 4,
        "avg_budget_pct_programmatic": 30,
        "avg_apply_rate": 4.6,
        "avg_time_to_fill": 32,
        "avg_cpa": 34,
        "estimated_reach_multiplier": 1.0,
    },
    "hospitality_travel": {
        "avg_channels": 3,
        "avg_budget_pct_programmatic": 22,
        "avg_apply_rate": 4.5,
        "avg_time_to_fill": 25,
        "avg_cpa": 16,
        "estimated_reach_multiplier": 0.9,
    },
    "blue_collar_trades": {
        "avg_channels": 3,
        "avg_budget_pct_programmatic": 26,
        "avg_apply_rate": 4.8,
        "avg_time_to_fill": 30,
        "avg_cpa": 24,
        "estimated_reach_multiplier": 0.9,
    },
    "pharma_biotech": {
        "avg_channels": 5,
        "avg_budget_pct_programmatic": 35,
        "avg_apply_rate": 4.5,
        "avg_time_to_fill": 55,
        "avg_cpa": 75,
        "estimated_reach_multiplier": 1.1,
    },
}


# ===================================================================
# Helper utilities
# ===================================================================


# ---------------------------------------------------------------------------
# Number Formatting Helpers
# ---------------------------------------------------------------------------

_CURRENCY_SYMBOLS = {
    "USD": "$",
    "EUR": "\u20ac",
    "GBP": "\u00a3",
    "INR": "\u20b9",
    "JPY": "\u00a5",
    "CNY": "\u00a5",
    "AUD": "A$",
    "CAD": "C$",
    "SGD": "S$",
    "HKD": "HK$",
    "NZD": "NZ$",
    "CHF": "CHF ",
    "SEK": "kr ",
    "NOK": "kr ",
    "DKK": "kr ",
    "BRL": "R$",
    "ZAR": "R ",
    "MXN": "MX$",
    "KRW": "\u20a9",
    "THB": "\u0e3f",
    "MYR": "RM ",
    "PHP": "\u20b1",
    "IDR": "Rp ",
    "AED": "AED ",
    "SAR": "SAR ",
}


def _fmt_currency(val, currency="USD", compact=False):
    """Format a number as currency. compact=True for $1.2M style."""
    if val is None:
        return "N/A"
    try:
        val = float(val)
    except (TypeError, ValueError):
        return str(val)
    sym = _CURRENCY_SYMBOLS.get(currency, "$")
    if compact and abs(val) >= 1_000_000:
        return f"{sym}{val/1_000_000:,.1f}M"
    if compact and abs(val) >= 1_000:
        return f"{sym}{val/1_000:,.1f}K"
    if val == int(val):
        return f"{sym}{int(val):,}"
    return f"{sym}{val:,.2f}"


def _fmt_pct(val, decimals=1):
    """Format as percentage."""
    if val is None:
        return "N/A"
    try:
        val = float(val)
    except (TypeError, ValueError):
        return str(val)
    if val <= 1.0:  # assume it's a decimal like 0.05 or 1.0 (= 100%)
        val = val * 100
    return f"{val:.{decimals}f}%"


def _fmt_range(low, high, fmt_fn=None):
    """Format a range."""
    fmt = fmt_fn or _fmt_currency
    return f"{fmt(low)} - {fmt(high)}"


def _set_font(
    run,
    size: int = 10,
    bold: bool = False,
    italic: bool = False,
    color: RGBColor = DARK_TEXT,
    name: str = FONT_FAMILY,
):
    """Configure font properties on a text run."""
    run.font.name = name
    run.font.size = Pt(size)
    run.font.bold = bold
    run.font.italic = italic
    run.font.color.rgb = color


def _add_textbox(
    slide,
    left,
    top,
    width,
    height,
    text: str = "",
    font_size: int = 10,
    bold: bool = False,
    italic: bool = False,
    color: RGBColor = DARK_TEXT,
    alignment=PP_ALIGN.LEFT,
    anchor=MSO_ANCHOR.TOP,
    word_wrap: bool = True,
):
    """Add a text box to a slide and return (shape, text_frame)."""
    txBox = slide.shapes.add_textbox(left, top, width, height)
    tf = txBox.text_frame
    tf.word_wrap = word_wrap
    tf.auto_size = None
    try:
        tf.paragraphs[0].alignment = alignment
    except Exception:
        pass
    txBox.text_frame.paragraphs[0].space_before = Pt(0)
    txBox.text_frame.paragraphs[0].space_after = Pt(0)
    try:
        txBox.text_frame._txBody.bodyPr.set(
            "anchor",
            {
                MSO_ANCHOR.TOP: "t",
                MSO_ANCHOR.MIDDLE: "ctr",
                MSO_ANCHOR.BOTTOM: "b",
            }.get(anchor, "t"),
        )
    except Exception:
        pass

    if text:
        p = tf.paragraphs[0]
        p.alignment = alignment
        run = p.add_run()
        run.text = str(text) if text is not None else ""
        _set_font(run, size=font_size, bold=bold, italic=italic, color=color)

    return txBox, tf


def _add_filled_rect(slide, left, top, width, height, fill_color: RGBColor):
    """Add a rectangle shape with a solid fill and no border."""
    shape = slide.shapes.add_shape(MSO_SHAPE.RECTANGLE, left, top, width, height)
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_color
    shape.line.fill.background()
    return shape


def _add_rounded_rect(slide, left, top, width, height, fill_color: RGBColor):
    """Add a rounded rectangle shape with a solid fill and no border."""
    shape = slide.shapes.add_shape(
        MSO_SHAPE.ROUNDED_RECTANGLE, left, top, width, height
    )
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_color
    shape.line.fill.background()
    return shape


def _add_oval(slide, left, top, width, height, fill_color: RGBColor):
    """Add an oval/circle shape with solid fill and no border."""
    shape = slide.shapes.add_shape(MSO_SHAPE.OVAL, left, top, width, height)
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_color
    shape.line.fill.background()
    return shape


def _add_rule_line(slide, left_inches, top_inches, width_inches, color_hex="202058"):
    """Add a thin horizontal rule line to a slide."""
    shape = slide.shapes.add_shape(
        MSO_SHAPE.RECTANGLE,
        Inches(left_inches),
        Inches(top_inches),
        Inches(width_inches),
        Pt(1.5),  # thin line
    )
    shape.fill.solid()
    shape.fill.fore_color.rgb = RGBColor.from_string(color_hex)
    shape.line.fill.background()  # no border
    return shape


def _add_paragraph(
    tf,
    text,
    font_size=10,
    bold=False,
    italic=False,
    color=DARK_TEXT,
    alignment=PP_ALIGN.LEFT,
    space_before=0,
    space_after=2,
):
    """Append a paragraph to an existing text frame."""
    p = tf.add_paragraph()
    p.alignment = alignment
    p.space_before = Pt(space_before)
    p.space_after = Pt(space_after)
    run = p.add_run()
    run.text = str(text) if text is not None else ""
    _set_font(run, size=font_size, bold=bold, italic=italic, color=color)
    return p


def _add_multi_run_paragraph(
    tf, runs_data: List[Tuple], alignment=PP_ALIGN.LEFT, space_before=0, space_after=2
):
    """Add a paragraph with multiple styled runs.
    runs_data: list of (text, font_size, bold, color) tuples.
    """
    p = tf.add_paragraph()
    p.alignment = alignment
    p.space_before = Pt(space_before)
    p.space_after = Pt(space_after)
    for text, font_size, bold, color in runs_data:
        run = p.add_run()
        run.text = str(text) if text is not None else ""
        _set_font(run, size=font_size, bold=bold, color=color)
    return p


def _get_benchmarks(industry: str, data: Optional[Dict] = None) -> Dict[str, str]:
    """Return benchmark data for the given industry.

    v3 resolution cascade:
    1. Synthesized ad_platform_analysis (live API data)
    2. trend_engine.py dynamic benchmarks (with YoY trend arrows)
    3. Hardcoded BENCHMARKS dict (static fallback)

    Returns dict with keys: cpa, cpc, cph, apply_rate, plus optional
    cpc_trend, cpa_trend (e.g. "+8.2% YoY") and confidence.
    """
    # Start with hardcoded fallback
    result = dict(BENCHMARKS.get(industry, BENCHMARKS["general_entry_level"]))
    result["confidence"] = "curated"

    # Layer 2: trend_engine dynamic benchmarks (v3)
    if _HAS_TREND_ENGINE:
        try:
            campaign_month = 0
            if data:
                try:
                    campaign_month = int(data.get("campaign_start_month") or 0 or 0)
                except (ValueError, TypeError):
                    campaign_month = 0
            current_month = (
                campaign_month
                if (campaign_month and 1 <= campaign_month <= 12)
                else datetime.datetime.now().month
            )
            # Get CPC across platforms
            cpc_vals = []
            cpa_vals = []
            trend_dirs = []
            trend_yoys = []
            for plat in ["google", "meta_fb", "indeed", "linkedin", "programmatic"]:
                cpc_result = _trend_engine.get_benchmark(
                    platform=plat,
                    industry=industry or "general",
                    metric="cpc",
                    collar_type="both",
                    location="",
                    month=current_month,
                )
                if cpc_result and isinstance(cpc_result, dict):
                    v = cpc_result.get("value") or 0
                    if isinstance(v, (int, float)) and v > 0:
                        cpc_vals.append(v)
                        td = cpc_result.get("trend_direction", "stable")
                        ty = cpc_result.get("trend_pct_yoy") or 0
                        trend_dirs.append(td)
                        trend_yoys.append(ty)
                cpa_result = _trend_engine.get_benchmark(
                    platform=plat,
                    industry=industry or "general",
                    metric="cpa",
                    collar_type="both",
                    location="",
                    month=current_month,
                )
                if cpa_result and isinstance(cpa_result, dict):
                    v = cpa_result.get("value") or 0
                    if isinstance(v, (int, float)) and v > 0:
                        cpa_vals.append(v)

            if cpc_vals:
                min_cpc = min(cpc_vals)
                max_cpc = max(cpc_vals)
                result["cpc"] = (
                    f"${min_cpc:.2f} - ${max_cpc:.2f}"
                    if len(cpc_vals) > 1
                    else f"${min_cpc:.2f}"
                )
                result["confidence"] = "trend_engine"
                # Trend annotation
                if trend_yoys:
                    avg_yoy = sum(trend_yoys) / len(trend_yoys)
                    arrow = "+" if avg_yoy > 0 else ""
                    dom_trend = (
                        max(set(trend_dirs), key=trend_dirs.count)
                        if trend_dirs
                        else "stable"
                    )
                    result["cpc_trend"] = f"{arrow}{avg_yoy:.1f}% YoY"
                    result["cpc_trend_direction"] = dom_trend
            if cpa_vals:
                min_cpa = min(cpa_vals)
                max_cpa = max(cpa_vals)
                result["cpa"] = (
                    f"${min_cpa:.0f} - ${max_cpa:.0f}"
                    if len(cpa_vals) > 1
                    else f"${min_cpa:.0f}"
                )
        except Exception:
            pass  # Fall through to synthesized or static

    # Layer 1: Synthesized ad_platform_analysis overrides (live API data)
    if data:
        synthesized = data.get("_synthesized", {})
        if isinstance(synthesized, dict):
            ad_plat = synthesized.get("ad_platform_analysis", {})
            if isinstance(ad_plat, dict) and ad_plat:
                live_cpcs = []
                live_cpas = []
                for plat_name, plat_data in ad_plat.items():
                    if not isinstance(plat_data, dict) or plat_name.startswith("_"):
                        continue
                    cpc = plat_data.get("avg_cpc") or plat_data.get("cpc")
                    cpa = plat_data.get("avg_cpa") or plat_data.get("cpa")
                    if cpc and isinstance(cpc, (int, float)) and cpc > 0:
                        live_cpcs.append(cpc)
                    if cpa and isinstance(cpa, (int, float)) and cpa > 0:
                        live_cpas.append(cpa)
                if live_cpcs:
                    min_cpc = min(live_cpcs)
                    max_cpc = max(live_cpcs)
                    result["cpc"] = (
                        f"${min_cpc:.2f} - ${max_cpc:.2f}"
                        if min_cpc != max_cpc
                        else f"${min_cpc:.2f}"
                    )
                    result["confidence"] = "live_api"
                if live_cpas:
                    min_cpa = min(live_cpas)
                    max_cpa = max(live_cpas)
                    result["cpa"] = (
                        f"${min_cpa:.0f} - ${max_cpa:.0f}"
                        if min_cpa != max_cpa
                        else f"${min_cpa:.0f}"
                    )
                    result["confidence"] = "live_api"

    # Layer 0: Budget engine CPH and apply_rate overrides
    # These were NEVER overridden before (always hardcoded) -- fix v3.4.1
    if data:
        budget_alloc = data.get("_budget_allocation", {})
        if isinstance(budget_alloc, dict) and budget_alloc:
            total_proj = budget_alloc.get("total_projected", {})
            if isinstance(total_proj, dict):
                # CPH from budget engine
                live_cph = total_proj.get("cost_per_hire") or total_proj.get("cph")
                if isinstance(live_cph, (int, float)) and live_cph > 0:
                    # Format as range: computed +/- 20% to show realistic spread
                    low_cph = live_cph * 0.8
                    high_cph = live_cph * 1.2
                    result["cph"] = f"${low_cph:,.0f} - ${high_cph:,.0f}"
                # Apply rate from budget engine
                live_apply_rate = total_proj.get("apply_rate") or total_proj.get(
                    "conversion_rate"
                )
                if isinstance(live_apply_rate, (int, float)) and live_apply_rate > 0:
                    # Apply rate might be 0-1 or 0-100; normalize
                    if live_apply_rate < 1:
                        live_apply_rate *= 100
                    low_ar = live_apply_rate * 0.85
                    high_ar = live_apply_rate * 1.15
                    result["apply_rate"] = f"{low_ar:.1f}% - {high_ar:.1f}%"

        # Also check synthesized salary_intelligence for CPH context
        # but ONLY if the budget engine didn't already set it (budget engine
        # computes CPH from actual budget/projections -- higher quality).
        synthesized = data.get("_synthesized", {})
        _budget_set_cph = (
            isinstance(budget_alloc, dict) and budget_alloc
        )  # budget engine ran
        if not _budget_set_cph and isinstance(synthesized, dict):
            salary_intel = synthesized.get("salary_intelligence", {})
            if isinstance(salary_intel, dict):
                synth_cph = salary_intel.get(
                    "estimated_cost_per_hire"
                ) or salary_intel.get("cph")
                if isinstance(synth_cph, (int, float)) and synth_cph > 0:
                    low_cph = synth_cph * 0.8
                    high_cph = synth_cph * 1.2
                    result["cph"] = f"${low_cph:,.0f} - ${high_cph:,.0f}"

    # Layer 2.5: Appcast 2026 KB Benchmark Overlay (Priority 3)
    # Cross-reference/override CPA, CPH, apply_rate with Appcast occupation-level
    # data if the current values are still from hardcoded fallback.
    if result.get("confidence") == "curated":
        try:
            _PPT_APPCAST_MAP = {
                "healthcare": "healthcare",
                "healthcare_medical": "healthcare",
                "technology": "technology",
                "tech_engineering": "technology",
                "retail": "retail",
                "retail_consumer": "retail",
                "finance": "finance",
                "finance_banking": "finance",
                "insurance": "insurance",
                "logistics": "warehousing_logistics",
                "logistics_supply_chain": "warehousing_logistics",
                "transportation": "transportation",
                "hospitality": "hospitality",
                "hospitality_travel": "hospitality",
                "food_beverage": "food_service",
                "blue_collar_trades": "construction_skilled_trades",
                "construction": "construction_skilled_trades",
                "construction_real_estate": "construction_skilled_trades",
                "pharma_biotech": "science_engineering",
                "energy_utilities": "science_engineering",
                "general": "administration",
                "general_entry_level": "administration",
                "government_utilities": "administration",
                "manufacturing": "manufacturing",
                "education": "education",
                "legal_services": "legal",
                "marketing": "marketing_advertising",
                "media_entertainment": "marketing_advertising",
                "professional_services": "consulting",
            }
            _app_key = _PPT_APPCAST_MAP.get(industry) or ""
            if _app_key and data:
                _kb = data.get("_knowledge_base", {})
                if not _kb:
                    try:
                        from kb_loader import load_knowledge_base

                        _kb = load_knowledge_base()
                    except Exception:
                        _kb = {}
                _wp = _kb.get("white_papers", {})
                _appcast_bm = (
                    _wp.get("reports", {})
                    .get("appcast_benchmark_2026", {})
                    .get("benchmarks", {})
                )
                if _appcast_bm:
                    _occ_cpa = _appcast_bm.get("cpa_by_occupation_2025", {}).get(
                        _app_key
                    )
                    _occ_cph = _appcast_bm.get("cph_by_occupation_2025", {}).get(
                        _app_key
                    )
                    _occ_ar = _appcast_bm.get("apply_rate_by_occupation_2025", {}).get(
                        _app_key
                    )
                    if _occ_cpa:
                        result["cpa"] = _occ_cpa
                        result["cpa_source"] = "Appcast 2026 (302M clicks)"
                    if _occ_cph:
                        result["cph"] = _occ_cph
                        result["cph_source"] = "Appcast 2026 (302M clicks)"
                    if _occ_ar:
                        result["apply_rate"] = _occ_ar
                        result["apply_rate_source"] = "Appcast 2026 (302M clicks)"
                    if any([_occ_cpa, _occ_cph, _occ_ar]):
                        result["confidence"] = "appcast_kb"
        except Exception:
            pass  # Non-fatal; fall through to curated values

    return result


def _get_complications(industry: str) -> List[str]:
    """Return complication bullets for the industry, with apply rate framed correctly."""
    base = COMPLICATIONS.get(
        industry,
        [
            "Talent acquisition costs rising across sectors",
            "Competition for qualified candidates intensifying",
            "Traditional sourcing channels showing diminishing returns",
            "Time-to-fill expanding, impacting operational capacity",
        ],
    )

    # Get the apply rate for this industry and frame appropriately
    benchmarks = BENCHMARKS.get(industry, BENCHMARKS.get("general_entry_level", {}))
    apply_rate_str = benchmarks.get("apply_rate") or ""
    if apply_rate_str:
        # Parse apply rate - handle ranges like "3.2% - 4.5%" or single values like "6.41%"
        import re as _re

        rates = _re.findall(r"[\d.]+", apply_rate_str)
        if rates:
            avg_rate = sum(float(r) for r in rates) / len(rates)
            # Only add apply rate as complication if it's genuinely low (below 2%)
            if avg_rate < 2.0:
                base = list(base)  # make mutable copy
                base.append(
                    f"Low {apply_rate_str} apply rate indicates competitive market pressure"
                )

    return base


def _get_industry_comparison(
    industry: str, data: Optional[Dict] = None
) -> Dict[str, Any]:
    """Return industry benchmark comparison data.

    v3.4.1: overlay dynamic data from budget engine / synthesized data
    on top of the hardcoded base, so live API values take precedence.
    """
    result = dict(
        INDUSTRY_BENCHMARKS_COMPARISON.get(
            industry, INDUSTRY_BENCHMARKS_COMPARISON["general_entry_level"]
        )
    )

    if data:
        budget_alloc = data.get("_budget_allocation", {})
        if isinstance(budget_alloc, dict) and budget_alloc:
            total_proj = budget_alloc.get("total_projected", {})
            if isinstance(total_proj, dict):
                live_cpa = total_proj.get("cpa") or total_proj.get(
                    "cost_per_application"
                )
                if isinstance(live_cpa, (int, float)) and live_cpa > 0:
                    result["avg_cpa"] = f"${live_cpa * 0.8:.0f} - ${live_cpa * 1.2:.0f}"
                live_cph = total_proj.get("cost_per_hire") or total_proj.get("cph")
                if isinstance(live_cph, (int, float)) and live_cph > 0:
                    result["avg_cph"] = (
                        f"${live_cph * 0.8:,.0f} - ${live_cph * 1.2:,.0f}"
                    )
            ch_allocs = budget_alloc.get("channel_allocations", {})
            if isinstance(ch_allocs, dict) and ch_allocs:
                n_ch = len(
                    [
                        c
                        for c in ch_allocs.values()
                        if isinstance(c, dict) and (c.get("percentage") or 0) > 0
                    ]
                )
                if n_ch > 0:
                    # Keep as int -- used in arithmetic comparisons downstream
                    result["avg_channels"] = n_ch

    return result


def _is_us_only_campaign(data: Dict) -> bool:
    """Check if all campaign locations are within the United States.
    Also respects the target_region field from the region selector."""
    # Check explicit target_region first (set by region selector UI)
    target_region = (data.get("target_region") or "").lower().strip()
    if target_region == "us_only":
        return True
    if target_region in ("global", "emea", "apac", "custom"):
        return False
    locations = data.get("locations") or []
    if not locations:
        return True  # No locations specified -- assume domestic
    us_indicators = {
        "us",
        "usa",
        "united states",
        "america",
        # US state abbreviations
        "al",
        "ak",
        "az",
        "ar",
        "ca",
        "co",
        "ct",
        "de",
        "fl",
        "ga",
        "hi",
        "id",
        "il",
        "in",
        "ia",
        "ks",
        "ky",
        "la",
        "me",
        "md",
        "ma",
        "mi",
        "mn",
        "ms",
        "mo",
        "mt",
        "ne",
        "nv",
        "nh",
        "nj",
        "nm",
        "ny",
        "nc",
        "nd",
        "oh",
        "ok",
        "or",
        "pa",
        "ri",
        "sc",
        "sd",
        "tn",
        "tx",
        "ut",
        "vt",
        "va",
        "wa",
        "wv",
        "wi",
        "wy",
        "dc",
    }
    # US city patterns (city, state format)
    for loc in locations:
        loc_lower = str(loc).lower().strip()
        # Clearly international
        if any(
            intl in loc_lower
            for intl in (
                "uk",
                "london",
                "europe",
                "apac",
                "emea",
                "asia",
                "india",
                "germany",
                "france",
                "japan",
                "china",
                "australia",
                "canada",
                "brazil",
                "mexico",
                "singapore",
                "hong kong",
            )
        ):
            return False
        # Check if location matches US patterns
        parts = [p.strip() for p in loc_lower.replace(",", " ").split()]
        if not any(p in us_indicators for p in parts):
            # Could be a US city without state qualifier -- allow it
            pass
    return True


def _selected_channels(data: Dict) -> Dict[str, Dict[str, Any]]:
    """Return only the channels the user toggled on, with redistributed percentages.
    Uses industry-aware allocation profiles for differentiated budget splits.
    Automatically excludes APAC/EMEA channels for US-only campaigns."""
    cats = data.get("channel_categories", {})
    if isinstance(cats, list):
        cats = {k: True for k in cats}

    # Skip international channels for US-only campaigns
    us_only = _is_us_only_campaign(data)
    if us_only:
        cats["apac_regional"] = False
        cats["emea_regional"] = False

    # Get industry-aware base allocation
    industry = data.get("industry", "general_entry_level")
    budget_str = data.get("budget") or ""
    roles = data.get("roles") or []
    num_roles = len(roles) if roles else 0
    locations = data.get("locations") or []
    alloc_base = _get_industry_alloc(
        industry, budget_str, num_roles, roles, locations=locations
    )

    selected = {}
    for key, meta in alloc_base.items():
        if cats.get(key, False):
            selected[key] = dict(meta)

    if not selected:
        # Legacy plans without channel_categories: include ALL channels
        # (New UI defaults channels to OFF, so empty cats means legacy data)
        has_explicit_cats = any(v for v in cats.values()) if cats else False
        if not has_explicit_cats:
            for key, meta in alloc_base.items():
                selected[key] = dict(meta)
            # S48 FIX: Apply geo-filter AFTER legacy fallback populates all channels.
            # When channel_categories is empty (legacy/API path), the us_only check
            # at the top correctly sets cats["apac_regional"] = False, but the empty
            # cats dict triggers this fallback which re-includes ALL channels.
            # We must strip APAC/EMEA here for US-only campaigns.
            if us_only:
                selected.pop("apac_regional", None)
                selected.pop("emea_regional", None)
        else:
            # cats was provided but no True values matched alloc_base keys;
            # fall back to programmatic + global + social as minimum
            for key in ("programmatic_dsp", "global_boards", "social_media"):
                if key in alloc_base:
                    selected[key] = dict(alloc_base[key])

    raw_total = sum(v["pct"] for v in selected.values())
    if raw_total > 0:
        for v in selected.values():
            v["pct"] = round(v["pct"] / raw_total * 100)
        diff = 100 - sum(v["pct"] for v in selected.values())
        if diff != 0:
            first_key = next(iter(selected))
            selected[first_key]["pct"] += diff

    return selected


def _goal_labels(data: Dict) -> List[str]:
    """Return human-readable campaign goal labels."""
    goals = data.get("campaign_goals") or []
    return [GOAL_LABELS.get(g, g.replace("_", " ").title()) for g in goals]


def _parse_budget_number(budget_str) -> Optional[float]:
    """Try to extract a numeric budget value from a string.

    Delegates to shared_utils.parse_budget_display for consistent parsing
    across all modules.
    """
    return parse_budget_display(budget_str)


def _format_budget_display(budget_str: str) -> str:
    """Format budget for hero stat display."""
    val = _parse_budget_number(budget_str)
    if val is None:
        return budget_str
    if val >= 1000000:
        return f"${val / 1000000:.1f}M"
    if val >= 1000:
        return f"${val / 1000:.0f}K"
    return f"${val:,.0f}"


def _channel_categories_grouped(channels: Dict) -> Dict[str, List[Dict]]:
    """Group channels by their category for attribution diagram."""
    groups: Dict[str, List[Dict]] = {}
    for key, ch in channels.items():
        cat = ch.get("category", "Other")
        if cat not in groups:
            groups[cat] = []
        groups[cat].append(ch)
    return groups


def _add_footer(slide, today: str):
    """Add the standard Joveo-branded footer bar to a slide."""
    footer_top = Inches(6.95)
    _add_filled_rect(slide, Inches(0), footer_top, SLIDE_WIDTH, Inches(0.03), NAVY)
    # Left-aligned date
    _add_textbox(
        slide,
        Inches(0.55),
        footer_top + Inches(0.08),
        Inches(4),
        Inches(0.3),
        text=today,
        font_size=7,
        color=MUTED_TEXT,
        alignment=PP_ALIGN.LEFT,
    )
    # Right-aligned Joveo branding in Downy Teal
    _add_textbox(
        slide,
        Inches(8.5),
        footer_top + Inches(0.08),
        Inches(4.5),
        Inches(0.3),
        text="Created by Shubham Singh Chandel  ||  Powered by Joveo's Global Supply Team",
        font_size=7,
        color=TEAL,
        alignment=PP_ALIGN.RIGHT,
    )


def _add_top_band(slide, left_text: str, right_text: str, band_color=NAVY):
    """Add the standard top navigation band."""
    band_h = Inches(0.72)
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, band_h, band_color)
    _add_textbox(
        slide,
        Inches(0.45),
        Inches(0.15),
        Inches(7),
        Inches(0.45),
        text=left_text,
        font_size=14,
        bold=True,
        color=WHITE,
    )
    _add_textbox(
        slide,
        Inches(9),
        Inches(0.15),
        Inches(4),
        Inches(0.45),
        text=right_text,
        font_size=12,
        bold=False,
        color=RGBColor(0xA8, 0xD8, 0xEA),  # Light Teal (Joveo extended)
        alignment=PP_ALIGN.RIGHT,
    )
    return band_h


def _confidence_color(confidence: str) -> Tuple[RGBColor, str]:
    """Return (color, label) for a confidence level string.

    Green bold for live/high confidence, blue for curated, amber for fallback.
    """
    conf_lower = str(confidence).lower()
    if conf_lower in ("live_api", "high", "synthesized"):
        return (GREEN, "Live Data")
    elif conf_lower in ("trend_engine", "cached_api"):
        return (BLUE, "Trend Engine")
    elif conf_lower in ("curated", "knowledge_base", "medium"):
        return (MEDIUM_BLUE, "Curated")
    else:
        return (AMBER, "Estimated")


def _add_enrichment_badge(slide, enriched):
    """Add a small 'Powered by live data' badge if APIs were used."""
    if not enriched:
        return
    summary = enriched.get("enrichment_summary", {})
    apis = summary.get("apis_succeeded") or []
    if not apis:
        return
    # Small text at bottom-right
    txBox = slide.shapes.add_textbox(
        Inches(10.5), Inches(7.1), Inches(2.5), Inches(0.3)
    )
    tf = txBox.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = f"Live data: {', '.join(apis[:3])}"
    p.font.size = Pt(7)
    p.font.color.rgb = MUTED_TEXT
    p.alignment = PP_ALIGN.RIGHT


def _add_data_sources_footnote(slide, data: Dict, benchmarks: Dict):
    """Add a thin ruled footnote line with data source attribution."""
    sources_parts = []
    conf = benchmarks.get("confidence", "curated")
    if conf == "live_api":
        sources_parts.append("Live API data")
    elif conf == "trend_engine":
        sources_parts.append("Trend Engine (4-year history)")
    sources_parts.append(f"Appcast {datetime.date.today().year}")
    sources_parts.append("SHRM Benchmarks")

    # International disclaimer -- detect non-US campaigns
    is_us_only = _is_us_only_campaign(data)
    intl_disclaimer = ""
    if not is_us_only:
        intl_disclaimer = (
            "Note: Benchmarks shown are US-calibrated. "
            "International markets may vary significantly."
        )

    enriched = data.get("_enriched", {})
    if enriched:
        summary = enriched.get("enrichment_summary", {})
        apis = summary.get("apis_succeeded") or []
        if apis:
            sources_parts.extend(apis[:3])

    # Thin ruled line
    _add_filled_rect(
        slide, Inches(0.55), Inches(7.0), Inches(12.2), Inches(0.01), WARM_GRAY
    )
    # Source text
    source_text = "Data Sources: " + " | ".join(sources_parts)
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(7.02),
        Inches(10),
        Inches(0.2),
        text=source_text,
        font_size=6,
        italic=True,
        color=LIGHT_MUTED,
    )
    # Confidence badge
    conf_color, conf_label = _confidence_color(conf)
    _add_textbox(
        slide,
        Inches(11.0),
        Inches(7.02),
        Inches(2),
        Inches(0.2),
        text=conf_label,
        font_size=7,
        bold=True,
        color=conf_color,
        alignment=PP_ALIGN.RIGHT,
    )

    # International benchmark disclaimer (if non-US locations detected)
    if intl_disclaimer:
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(7.18),
            Inches(10),
            Inches(0.2),
            text=intl_disclaimer,
            font_size=6,
            italic=True,
            color=AMBER,
        )

    # S50: Location plausibility warning footnote
    synthesized = data.get("_synthesized", {})
    loc_warnings = (
        synthesized.get("_validation", {}).get("location_warnings") or []
        if isinstance(synthesized, dict)
        else []
    )
    if loc_warnings:
        warn_locs = [w.get("location", "") for w in loc_warnings if w.get("location")]
        if warn_locs:
            warn_text = (
                f"Location advisory: {', '.join(warn_locs[:3])} may not align "
                f"with company's known operating area. See Excel for details."
            )
            y_offset = Inches(7.18) if not intl_disclaimer else Inches(7.30)
            _add_textbox(
                slide,
                Inches(0.55),
                y_offset,
                Inches(10),
                Inches(0.2),
                text=warn_text,
                font_size=6,
                italic=True,
                color=AMBER,
            )


def _format_salary(amount):
    """Format a salary number into human-readable string like $85K or $125K."""
    if not isinstance(amount, (int, float)) or amount <= 0:
        return ""
    if amount >= 1000:
        return f"${amount / 1000:.0f}K"
    return f"${amount:,.0f}"


# ===================================================================
# SLIDE 1 - Cover / Section Divider: Title Slide
# ===================================================================


def _build_slide_cover(prs: Presentation, data: Dict):
    """Build a premium full-bleed cover slide with LinkedIn-style section divider pattern."""
    slide_layout = prs.slide_layouts[6]  # blank
    slide = prs.slides.add_slide(slide_layout)

    client = data.get("client_name", "Client")
    industry_label = data.get("industry_label") or ""
    today = datetime.date.today().strftime("%B %d, %Y")

    # Full dark navy background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, NAVY)

    # Teal accent bar at top
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, Inches(0.06), TEAL)

    # Decorative Blue Violet accent bar - left side
    _add_filled_rect(slide, Inches(0.6), Inches(1.8), Inches(1.2), Inches(0.05), BLUE)

    # Logo top-left (PNG, ~0.5" tall)
    _logo_path = os.path.join(os.path.dirname(__file__), "assets", "joveo-logo.png")
    if os.path.exists(_logo_path):
        try:
            slide.shapes.add_picture(
                _logo_path, Inches(0.6), Inches(0.4), height=Inches(0.5)
            )
        except Exception:
            pass  # Graceful fallback -- text label below still shows

    # "AI MEDIA PLANNER" small label top-left (below logo) in Downy Teal
    _add_textbox(
        slide,
        Inches(0.6),
        Inches(1.1),
        Inches(5),
        Inches(0.4),
        text="AI MEDIA PLANNER",
        font_size=14,
        bold=True,
        color=TEAL,  # Downy Teal branding
    )

    # Main title - client name large
    _add_textbox(
        slide,
        Inches(0.6),
        Inches(2.1),
        Inches(10),
        Inches(1.2),
        text=f"Media Plan",
        font_size=52,
        bold=True,
        color=WHITE,
    )

    # Client name as hero element
    _add_textbox(
        slide,
        Inches(0.6),
        Inches(3.2),
        Inches(11),
        Inches(1.0),
        text=client,
        font_size=44,
        bold=True,
        color=LIGHT_TEAL,
    )

    # Industry subtitle in light teal
    if industry_label:
        _add_textbox(
            slide,
            Inches(0.6),
            Inches(4.2),
            Inches(10),
            Inches(0.5),
            text=industry_label,
            font_size=20,
            bold=False,
            color=LIGHT_TEAL,  # Joveo Light Teal
        )

    # Company tagline from enrichment data (Wikipedia description)
    enriched = data.get("_enriched", {})
    company_info = enriched.get("company_info", {}) if enriched else {}
    if company_info and company_info.get("description"):
        # Truncate to first sentence or 120 chars for a clean tagline
        desc = company_info["description"]
        first_sentence_end = desc.find(".")
        if 0 < first_sentence_end < 120:
            tagline = desc[: first_sentence_end + 1]
        else:
            tagline = desc[:120].rsplit(" ", 1)[0] + "..." if len(desc) > 120 else desc
        _add_textbox(
            slide,
            Inches(0.6),
            Inches(4.7),
            Inches(9),
            Inches(0.4),
            text=tagline,
            font_size=11,
            italic=True,
            color=LIGHT_MUTED,
        )

    # Blue Violet accent line under title area
    _add_filled_rect(slide, Inches(0.6), Inches(5.0), Inches(3.0), Inches(0.05), BLUE)

    # Date and branding at bottom
    _add_textbox(
        slide,
        Inches(0.6),
        Inches(5.4),
        Inches(6),
        Inches(0.4),
        text=today,
        font_size=14,
        color=LIGHT_MUTED,
    )
    _add_textbox(
        slide,
        Inches(0.6),
        Inches(5.8),
        Inches(6),
        Inches(0.4),
        text="Created by Shubham Singh Chandel  ||  Powered by Joveo's Global Supply Team",
        font_size=11,
        italic=True,
        color=TEAL,  # Downy Teal for branding
    )

    # Right-side decorative element: large subtle circle
    circle_size = Inches(4.5)
    circle = _add_oval(
        slide,
        SLIDE_WIDTH - Inches(3.0),
        Inches(1.5),
        circle_size,
        circle_size,
        MEDIUM_BLUE,
    )
    # Semi-transparent Blue Violet tone for decorative circle
    circle.fill.fore_color.rgb = RGBColor(0x3A, 0x36, 0x7E)  # Muted Blue Violet

    # Smaller overlapping accent circle (muted Downy Teal tone)
    _add_oval(
        slide,
        SLIDE_WIDTH - Inches(1.5),
        Inches(3.5),
        Inches(2.5),
        Inches(2.5),
        RGBColor(0x45, 0x7A, 0x8A),  # Muted Downy Teal
    )

    # Bottom teal bar
    _add_filled_rect(
        slide, Inches(0), SLIDE_HEIGHT - Inches(0.06), SLIDE_WIDTH, Inches(0.06), TEAL
    )


# ===================================================================
# SLIDE 2 - Executive Summary with Hero Stat
# ===================================================================


def _build_slide_executive_summary(prs: Presentation, data: Dict):
    """Build the Executive Summary slide with hero stat pattern and SCR framework."""
    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)

    client = data.get("client_name", "Client")
    industry = data.get("industry", "general_entry_level")
    industry_label = data.get("industry_label", industry.replace("_", " ").title())
    locations = data.get("locations") or []
    roles = data.get("roles") or []
    budget = data.get("budget", "TBD")
    work_env = data.get("work_environment", "hybrid")
    goals = _goal_labels(data)
    channels = _selected_channels(data)
    today = datetime.date.today().strftime("%B %d, %Y")

    # Pull synthesized + budget allocation data (from pipeline)
    synthesized = data.get("_synthesized", {})
    if not isinstance(synthesized, dict):
        synthesized = {}
    budget_alloc = data.get("_budget_allocation", {})
    if not isinstance(budget_alloc, dict):
        budget_alloc = {}

    # Extract synthesized sub-sections with safe access
    salary_intel = synthesized.get("salary_intelligence", {})
    if not isinstance(salary_intel, dict):
        salary_intel = {}
    job_market = synthesized.get("job_market_demand", {})
    if not isinstance(job_market, dict):
        job_market = {}

    # Budget allocation sub-sections
    ba_total_projected = budget_alloc.get("total_projected", {})
    if not isinstance(ba_total_projected, dict):
        ba_total_projected = {}
    ba_metadata = budget_alloc.get("metadata", {})
    if not isinstance(ba_metadata, dict):
        ba_metadata = {}

    # S48 FIX: Compute hires from per-channel sum (source of truth) to ensure
    # PPT hero stats match Excel Executive Summary and ROI Projections.
    _ppt_channel_allocs = budget_alloc.get("channel_allocations", {})
    if not isinstance(_ppt_channel_allocs, dict):
        _ppt_channel_allocs = {}
    _ppt_hires_sum = sum(
        int(ch.get("projected_hires") or 0) for ch in _ppt_channel_allocs.values()
    )
    if _ppt_hires_sum == 0:
        _ppt_hires_sum = int(ba_total_projected.get("hires") or 0)
    _ppt_total_budget = ba_metadata.get("total_budget") or 0
    _ppt_cph = (
        round(_ppt_total_budget / max(_ppt_hires_sum, 1), 2)
        if _ppt_hires_sum > 0
        else 0
    )

    # Off-white background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE)

    # Top band
    _add_top_band(slide, "AI MEDIA PLANNER", client.upper())

    # Action title
    role_summary = ", ".join(roles[:3]) if roles else "key roles"
    loc_count = len(locations)
    loc_text = (
        f"{loc_count} location{'s' if loc_count != 1 else ''}"
        if loc_count > 0
        else "multiple locations"
    )

    # Enhance action text with market temperature if available
    market_temp_str = ""
    try:
        for _role_key, _role_demand in job_market.items():
            if isinstance(_role_demand, dict):
                _temp = _role_demand.get("market_temperature") or ""
                if _temp:
                    market_temp_str = _temp
                    break
    except (AttributeError, TypeError):
        pass

    temp_qualifier = ""
    if market_temp_str:
        temp_qualifier = f" in a {market_temp_str} talent market"

    action_text = (
        f"Nova AI Suite's programmatic strategy targets {role_summary} across "
        f"{loc_text} to optimize "
        f"{client}'s recruitment spend in {industry_label}{temp_qualifier}"
    )
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(0.92),
        Inches(12.2),
        Inches(0.55),
        text=action_text,
        font_size=15,
        bold=True,
        color=NAVY,
    )

    # ---- THREE-COLUMN SCR BODY ----
    col_top = Inches(1.65)
    col_height = Inches(3.5)
    col_gap = Inches(0.25)
    accent_bar_w = Inches(0.06)

    col_w = Inches(3.95)
    col1_left = Inches(0.55)
    col2_left = col1_left + col_w + col_gap
    col3_left = col2_left + col_w + col_gap

    # ---- SITUATION (left) ----
    # Light card background
    _add_rounded_rect(slide, col1_left, col_top, col_w, col_height, WHITE)
    _add_filled_rect(slide, col1_left, col_top, accent_bar_w, col_height, BLUE)

    sit_left = col1_left + Inches(0.2)
    sit_w = col_w - Inches(0.25)

    _add_textbox(
        slide,
        sit_left,
        col_top + Inches(0.08),
        sit_w,
        Inches(0.35),
        text="SITUATION",
        font_size=11,
        bold=True,
        color=BLUE,
    )

    body_top = col_top + Inches(0.45)
    work_label = WORK_ENV_LABELS.get(work_env, work_env.replace("_", " ").title())
    role_display = ", ".join(roles[:5]) if roles else "Multiple roles"
    if len(roles) > 5:
        role_display += f" (+{len(roles) - 5} more)"

    # Use total budget from budget engine metadata if available
    total_budget_val = ba_metadata.get("total_budget") or 0
    budget_display_sit = budget
    if total_budget_val and total_budget_val > 0:
        budget_display_sit = _fmt_currency(total_budget_val, compact=True)

    sit_items = [
        ("Industry", industry_label),
        (
            "Locations",
            (
                f"{loc_count} market{'s' if loc_count != 1 else ''}"
                if loc_count > 0
                else "Multiple markets"
            ),
        ),
        ("Target Roles", role_display),
        ("Work Model", work_label),
        ("Budget", budget_display_sit),
    ]

    # Add market temperature from job_market_demand
    if market_temp_str:
        temp_colors = {
            "hot": "High demand",
            "warm": "Moderate demand",
            "cool": "Balanced",
            "cold": "Low demand",
        }
        sit_items.append(
            (
                "Market Temp.",
                f"{market_temp_str.title()} ({temp_colors.get(market_temp_str, 'N/A')})",
            )
        )

    # v3: Add collar type classification from collar_intelligence
    if _HAS_COLLAR_INTEL and roles:
        try:
            collar_counts = {}
            for role_name in (roles[:8] if isinstance(roles, list) else []):
                r_str = role_name if isinstance(role_name, str) else str(role_name)
                cr = _collar_intel.classify_collar(role=r_str, industry=industry)
                ct = cr.get("collar_type", "white_collar")
                collar_counts[ct] = collar_counts.get(ct, 0) + 1
            if collar_counts:
                dominant = max(collar_counts, key=collar_counts.get)
                total_c = sum(collar_counts.values())
                dom_pct = collar_counts[dominant] / total_c * 100
                collar_label = dominant.replace("_", " ").title()
                if dom_pct > 75:
                    sit_items.append(
                        ("Talent Profile", f"{collar_label} dominant ({dom_pct:.0f}%)")
                    )
                elif len(collar_counts) > 1:
                    mix = " / ".join(k.replace("_", " ").title() for k in collar_counts)
                    sit_items.append(("Talent Profile", f"Mixed ({mix})"))
        except Exception:
            pass

    # Add apply rate insight with appropriate framing
    benchmarks = _get_benchmarks(industry, data)

    # v3: Add CPC trend direction if available from trend engine
    cpc_trend_str = benchmarks.get("cpc_trend") or ""
    cpc_trend_dir = benchmarks.get("cpc_trend_direction") or ""
    if cpc_trend_str:
        trend_label = {
            "rising": "Rising",
            "falling": "Declining",
            "stable": "Stable",
        }.get(cpc_trend_dir) or ""
        sit_items.append(("CPC Trend", f"{trend_label} {cpc_trend_str}"))

    apply_rate_str = benchmarks.get("apply_rate") or ""
    if apply_rate_str:
        import re as _re_ar

        rates = _re_ar.findall(r"[\d.]+", apply_rate_str)
        if rates:
            avg_rate = sum(float(r) for r in rates) / len(rates)
            if avg_rate > 5.0:
                sit_items.append(
                    ("Apply Rate", f"{apply_rate_str} (above average - strength)")
                )
            elif avg_rate >= 2.0:
                sit_items.append(
                    ("Apply Rate", f"{apply_rate_str} (at industry average)")
                )
            else:
                sit_items.append(
                    ("Apply Rate", f"{apply_rate_str} (below average - challenge)")
                )

    # Add salary benchmark from salary_intelligence (synthesized) first,
    # fall back to enriched salary_data
    enriched = data.get("_enriched", {})
    salary_data = enriched.get("salary_data", {}) if enriched else {}
    _salary_added = False
    if salary_intel:
        try:
            for _si_role, _si_data in salary_intel.items():
                if isinstance(_si_data, dict):
                    _si_median = _si_data.get("median") or 0
                    _si_min = _si_data.get("min") or 0
                    _si_max = _si_data.get("max") or 0
                    if _si_median and _si_median > 0:
                        salary_str = _format_salary(_si_median)
                        range_str = ""
                        if _si_min > 0 and _si_max > 0:
                            range_str = f" ({_format_salary(_si_min)}-{_format_salary(_si_max)})"
                        sit_items.append(
                            (
                                "Salary Range",
                                f"{salary_str} median{range_str} - {_si_role}",
                            )
                        )
                        _salary_added = True
                        break
        except (AttributeError, TypeError):
            pass
    if not _salary_added and salary_data:
        try:
            first_role = list(salary_data.keys())[0]
            median = salary_data[first_role].get("median") or 0
            if median > 0:
                salary_str = _format_salary(median)
                sit_items.append(
                    ("Salary Benchmark", f"{salary_str} median ({first_role})")
                )
        except (IndexError, KeyError, TypeError):
            pass

    box2, tf2 = _add_textbox(slide, sit_left, body_top, sit_w, col_height - Inches(0.5))
    tf2.paragraphs[0].space_before = Pt(0)
    tf2.paragraphs[0].space_after = Pt(0)

    first = True
    for label, value in sit_items:
        if first:
            p = tf2.paragraphs[0]
            first = False
        else:
            p = tf2.add_paragraph()
        p.space_before = Pt(2)
        p.space_after = Pt(6)
        p.alignment = PP_ALIGN.LEFT

        run_label = p.add_run()
        run_label.text = f"{label}:  "
        _set_font(run_label, size=10, bold=True, color=DARK_TEXT)

        run_val = p.add_run()
        run_val.text = str(value)
        _set_font(run_val, size=10, bold=False, color=MUTED_TEXT)

    # ---- COMPLICATION (middle) ----
    _add_rounded_rect(slide, col2_left, col_top, col_w, col_height, WHITE)
    _add_filled_rect(slide, col2_left, col_top, accent_bar_w, col_height, TEAL)

    comp_left = col2_left + Inches(0.2)
    comp_w = col_w - Inches(0.25)

    _add_textbox(
        slide,
        comp_left,
        col_top + Inches(0.08),
        comp_w,
        Inches(0.35),
        text="COMPLICATION",
        font_size=11,
        bold=True,
        color=TEAL,
    )

    complications = _get_complications(industry)
    box3, tf3 = _add_textbox(
        slide, comp_left, body_top, comp_w, col_height - Inches(0.5)
    )
    tf3.paragraphs[0].space_before = Pt(0)
    tf3.paragraphs[0].space_after = Pt(0)

    for i, item in enumerate(complications):
        if i == 0:
            p = tf3.paragraphs[0]
        else:
            p = tf3.add_paragraph()
        p.space_before = Pt(2)
        p.space_after = Pt(8)
        p.alignment = PP_ALIGN.LEFT

        run_bullet = p.add_run()
        run_bullet.text = "\u25b8  "
        _set_font(run_bullet, size=10, bold=False, color=TEAL)

        run_text = p.add_run()
        run_text.text = str(item) if item is not None else ""
        _set_font(run_text, size=10, bold=False, color=DARK_TEXT)

    # ---- RESOLUTION (right) ----
    _add_rounded_rect(slide, col3_left, col_top, col_w, col_height, WHITE)
    _add_filled_rect(slide, col3_left, col_top, accent_bar_w, col_height, GREEN)

    res_left = col3_left + Inches(0.2)
    res_w = col_w - Inches(0.25)

    _add_textbox(
        slide,
        res_left,
        col_top + Inches(0.08),
        res_w,
        Inches(0.35),
        text="RESOLUTION",
        font_size=11,
        bold=True,
        color=GREEN,
    )

    box4, tf4 = _add_textbox(slide, res_left, body_top, res_w, col_height - Inches(0.5))
    tf4.paragraphs[0].space_before = Pt(0)
    tf4.paragraphs[0].space_after = Pt(0)

    # Market Thesis -- the WHY this plan will work
    p0 = tf4.paragraphs[0]
    r0 = p0.add_run()
    r0.text = "MARKET THESIS"
    _set_font(r0, size=10, bold=True, color=GREEN)
    p0.space_after = Pt(4)

    # Build thesis from data -- S48: use per-channel-sum hires
    _thesis_parts: list[str] = []
    if ba_total_projected:
        _proj_h = _ppt_hires_sum
        _proj_cph = _ppt_cph
        if _proj_h > 0:
            _thesis_parts.append(f"This plan projects {int(_proj_h)} hires")
        if _proj_cph > 0:
            _thesis_parts.append(f"at ${_proj_cph:,.0f}/hire")
    if market_temp_str:
        temp_map = {
            "hot": "high-demand",
            "warm": "active",
            "cool": "balanced",
            "cold": "buyer's",
        }
        _thesis_parts.append(
            f"in a {temp_map.get(market_temp_str, market_temp_str)} market"
        )
    if len(channels) > 0:
        _thesis_parts.append(f"via {len(channels)}-channel programmatic strategy")

    thesis_text = (
        " ".join(_thesis_parts) + "."
        if _thesis_parts
        else (f"Programmatic multi-channel strategy optimized for {industry_label}.")
    )
    _add_paragraph(
        tf4,
        thesis_text,
        font_size=9,
        color=DARK_TEXT,
        space_before=1,
        space_after=6,
    )

    # Strategy resolution
    _add_paragraph(
        tf4,
        "Nova AI Strategy:",
        font_size=9,
        bold=True,
        color=NAVY,
        space_before=2,
        space_after=2,
    )

    for ch in list(channels.values())[:5]:
        p = tf4.add_paragraph()
        p.space_before = Pt(1)
        p.space_after = Pt(3)
        rb = p.add_run()
        rb.text = "\u2713  "
        _set_font(rb, size=9, bold=False, color=GREEN)
        rt = p.add_run()
        rt.text = ch["label"]
        _set_font(rt, size=9, bold=False, color=DARK_TEXT)

    _total_pubs = data.get("_joveo_publishers", {}).get(
        "total_active_publishers", 10238
    )
    _add_paragraph(
        tf4,
        f"\u2713  ML-optimized bidding across {_total_pubs:,}+ publishers",
        font_size=8,
        color=DARK_TEXT,
        space_before=1,
        space_after=3,
    )

    if goals:
        for g in goals[:2]:
            p = tf4.add_paragraph()
            p.space_before = Pt(1)
            p.space_after = Pt(2)
            rb = p.add_run()
            rb.text = "\u25cf  "
            _set_font(rb, size=8, color=BLUE)
            rt = p.add_run()
            rt.text = g
            _set_font(rt, size=8, color=DARK_TEXT)

    # ---- HERO STAT METRICS BAR ----
    bar_top = Inches(5.35)
    bar_h = Inches(1.15)

    # Main bar background
    _add_filled_rect(slide, Inches(0.55), bar_top, Inches(12.2), bar_h, NAVY)

    # Teal accent line at top of bar
    _add_filled_rect(slide, Inches(0.55), bar_top, Inches(12.2), Inches(0.04), TEAL)

    # Hero stat: budget (if parseable) or channel count
    budget_display = _format_budget_display(budget)
    hero_value = budget_display if budget_display != budget else str(len(channels))
    hero_label = "Campaign Budget" if budget_display != budget else "Channels Selected"

    # Hero stat on the left
    _add_textbox(
        slide,
        Inches(0.85),
        bar_top + Inches(0.12),
        Inches(3.2),
        Inches(0.65),
        text=hero_value,
        font_size=36,
        bold=True,
        color=TEAL,
        alignment=PP_ALIGN.CENTER,
        anchor=MSO_ANCHOR.MIDDLE,
    )
    _add_textbox(
        slide,
        Inches(0.85),
        bar_top + Inches(0.72),
        Inches(3.2),
        Inches(0.3),
        text=hero_label,
        font_size=9,
        bold=False,
        color=LIGHT_MUTED,
        alignment=PP_ALIGN.CENTER,
    )

    # Divider
    _add_filled_rect(
        slide, Inches(4.2), bar_top + Inches(0.2), Inches(0.02), Inches(0.75), TEAL
    )

    # Secondary metrics - include salary data from enrichment if available
    secondary_metrics = [
        m
        for m in [
            (str(len(channels)), "Channels"),
            (str(loc_count), "Locations") if loc_count > 0 else None,
            (str(len(roles)), "Target Roles") if roles else None,
            (str(len(goals)), "Campaign Goals") if goals else None,
        ]
        if m is not None
    ]

    # Replace last metric with salary benchmark if available
    if salary_data:
        try:
            first_role = list(salary_data.keys())[0]
            median = salary_data[first_role].get("median") or 0
            if median > 0:
                salary_str = _format_salary(median)
                secondary_metrics.append((salary_str, "Median Salary"))
        except (IndexError, KeyError, TypeError):
            pass

    # Add budget allocation metrics if available (projected hires, avg CPA)
    # S48: use per-channel-sum hires for consistency
    if ba_total_projected:
        projected_hires = _ppt_hires_sum
        avg_cpa_val = ba_total_projected.get("cost_per_application") or 0
        avg_cph_val = _ppt_cph
        if projected_hires and projected_hires > 0:
            secondary_metrics.append((str(int(projected_hires)), "Projected Hires"))
        if avg_cpa_val and avg_cpa_val > 0:
            secondary_metrics.append((_fmt_currency(avg_cpa_val), "Avg CPA"))
        elif avg_cph_val and avg_cph_val > 0:
            secondary_metrics.append(
                (_fmt_currency(avg_cph_val, compact=True), "Cost/Hire")
            )

    # Add market temperature badge if available
    if market_temp_str and len(secondary_metrics) < 5:
        secondary_metrics.append((market_temp_str.upper(), "Market Temp."))

    metric_w = Inches(1.9)
    metric_start = Inches(4.55)

    for i, (value, label) in enumerate(secondary_metrics):
        mx = metric_start + i * metric_w

        _add_textbox(
            slide,
            mx,
            bar_top + Inches(0.12),
            metric_w,
            Inches(0.55),
            text=value,
            font_size=24,
            bold=True,
            color=WHITE,
            alignment=PP_ALIGN.CENTER,
            anchor=MSO_ANCHOR.MIDDLE,
        )
        _add_textbox(
            slide,
            mx,
            bar_top + Inches(0.72),
            metric_w,
            Inches(0.3),
            text=label,
            font_size=8,
            bold=False,
            color=LIGHT_MUTED,
            alignment=PP_ALIGN.CENTER,
        )

    # Thin dividers between secondary metrics
    for i in range(1, 4):
        div_x = metric_start + i * metric_w
        _add_filled_rect(
            slide,
            div_x,
            bar_top + Inches(0.25),
            Inches(0.015),
            Inches(0.65),
            RGBColor(0x1A, 0x45, 0x70),
        )

    # ── Creative Quality Score badge (P1-16) ──
    cqs = data.get("_creative_quality_score")
    if cqs and isinstance(cqs, dict) and cqs.get("score") is not None:
        _cqs_score = cqs.get("score", 0)
        _cqs_grade = cqs.get("grade", "N/A")
        _cqs_color = (
            GREEN
            if _cqs_score >= 70
            else BLUE if _cqs_score >= 50 else RGBColor(0xCC, 0x33, 0x33)
        )
        _add_rounded_rect(
            slide,
            Inches(10.8),
            Inches(0.18),
            Inches(2.3),
            Inches(0.55),
            _cqs_color,
        )
        _add_textbox(
            slide,
            Inches(10.8),
            Inches(0.2),
            Inches(2.3),
            Inches(0.32),
            text=f"Creative Score: {_cqs_score}/100",
            font_size=11,
            bold=True,
            color=WHITE,
            alignment=PP_ALIGN.CENTER,
        )
        _add_textbox(
            slide,
            Inches(10.8),
            Inches(0.46),
            Inches(2.3),
            Inches(0.2),
            text=f"Grade {_cqs_grade}",
            font_size=8,
            bold=False,
            color=WHITE,
            alignment=PP_ALIGN.CENTER,
        )

    # Enrichment badge
    _add_enrichment_badge(slide, enriched)

    # Footer
    _add_footer(slide, today)


# ===================================================================
# SLIDE 3 - Section Divider: "Channel Strategy"
# ===================================================================


def _build_slide_divider_channel_strategy(prs: Presentation, data: Dict):
    """Build a full-bleed section divider slide for Channel Strategy."""
    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)

    # Full LinkedIn Blue background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, BLUE)

    # Teal accent bar at top
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, Inches(0.06), TEAL)

    # Teal accent stripe left
    _add_filled_rect(slide, Inches(0.6), Inches(2.8), Inches(2.0), Inches(0.06), TEAL)

    # Section number
    _add_textbox(
        slide,
        Inches(0.6),
        Inches(2.2),
        Inches(3),
        Inches(0.5),
        text="02",
        font_size=18,
        bold=True,
        color=LIGHT_TEAL,
    )

    # Large section title
    _add_textbox(
        slide,
        Inches(0.6),
        Inches(3.1),
        Inches(10),
        Inches(1.5),
        text="Channel Strategy\n& Investment",
        font_size=48,
        bold=True,
        color=WHITE,
    )

    # Subtitle
    _add_textbox(
        slide,
        Inches(0.6),
        Inches(5.0),
        Inches(8),
        Inches(0.5),
        text="Optimized channel mix with programmatic intelligence",
        font_size=16,
        italic=True,
        color=PALE_BLUE,
    )

    # Bottom teal bar
    _add_filled_rect(
        slide, Inches(0), SLIDE_HEIGHT - Inches(0.06), SLIDE_WIDTH, Inches(0.06), TEAL
    )

    # Decorative shapes right side
    _add_oval(
        slide,
        Inches(10.5),
        Inches(1.0),
        Inches(3.5),
        Inches(3.5),
        RGBColor(0x09, 0x58, 0xB0),
    )
    _add_oval(
        slide,
        Inches(11.5),
        Inches(3.5),
        Inches(2.5),
        Inches(2.5),
        RGBColor(0x08, 0x50, 0xA0),
    )


# ===================================================================
# SLIDE 4 - Channel Strategy & Investment with Attribution Diagram
# ===================================================================


def _build_slide_channel_strategy(prs: Presentation, data: Dict):
    """Build the Channel Strategy slide with channel mix bars, benchmarks, and attribution."""
    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)

    client = data.get("client_name", "Client")
    industry = data.get("industry", "general_entry_level")
    industry_label = data.get("industry_label", industry.replace("_", " ").title())
    channels = _selected_channels(data)
    benchmarks = _get_benchmarks(industry, data)
    today = datetime.date.today().strftime("%B %d, %Y")

    # Pull synthesized + budget allocation data (from pipeline)
    synthesized = data.get("_synthesized", {})
    budget_alloc = data.get("_budget_allocation", {})

    # Off-white background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE)

    # Top band
    _add_top_band(slide, "CHANNEL STRATEGY & INVESTMENT", today)

    # Action title -- insight-rich with WHY reasoning
    n_cats = len(channels)
    budget_alloc_meta = (
        budget_alloc.get("metadata", {}) if isinstance(budget_alloc, dict) else {}
    )
    total_budget_val = budget_alloc_meta.get("total_budget") or 0
    # S48 FIX: per-channel sum hires for consistency
    _cs_ch_allocs = (
        budget_alloc.get("channel_allocations", {})
        if isinstance(budget_alloc, dict)
        else {}
    )
    if not isinstance(_cs_ch_allocs, dict):
        _cs_ch_allocs = {}
    proj_hires_ch = sum(
        int(ch.get("projected_hires") or 0) for ch in _cs_ch_allocs.values()
    )
    if proj_hires_ch == 0:
        proj_hires_ch = (budget_alloc.get("total_projected", {}) or {}).get(
            "hires"
        ) or 0
    if total_budget_val > 0 and proj_hires_ch > 0:
        action_text = (
            f"{n_cats}-channel strategy allocates {_fmt_currency(total_budget_val, compact=True)} "
            f"to project {int(proj_hires_ch)} hires for {client} in {industry_label} -- "
            f"each channel selected for cost-efficiency and audience reach"
        )
    else:
        action_text = (
            f"Optimized {n_cats}-channel mix delivers targeted reach for "
            f"{client}'s {industry_label} hiring priorities with data-driven allocation"
        )
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(0.92),
        Inches(12.2),
        Inches(0.5),
        text=action_text,
        font_size=15,
        bold=True,
        color=NAVY,
    )

    # ==== LEFT: Channel Mix with horizontal bars ====
    left_col_left = Inches(0.55)
    section_top = Inches(1.6)

    # Section header with teal underline
    _add_textbox(
        slide,
        left_col_left,
        section_top,
        Inches(4),
        Inches(0.35),
        text="CHANNEL MIX",
        font_size=11,
        bold=True,
        color=NAVY,
    )
    _add_filled_rect(
        slide,
        left_col_left,
        section_top + Inches(0.33),
        Inches(1.3),
        Inches(0.03),
        TEAL,
    )

    bar_area_top = section_top + Inches(0.5)
    bar_max_w = Inches(3.5)
    bar_h = Inches(0.30)
    bar_spacing = Inches(0.42)
    label_w = Inches(2.3)

    # Override channel percentages with real budget allocation if available
    ba_channel_alloc = (
        budget_alloc.get("channel_allocations", {}) if budget_alloc else {}
    )
    if ba_channel_alloc:
        # Map budget engine channel names to display channels
        ba_total_budget = budget_alloc.get("metadata", {}).get("total_budget") or 0
        for ch_key, ch_data in channels.items():
            # Try exact key match, then fuzzy label match
            ba_match = ba_channel_alloc.get(ch_key)
            if not ba_match:
                # Try matching by label (case-insensitive)
                ch_label_lower = (ch_data.get("label") or "").lower()
                for ba_key, ba_val in ba_channel_alloc.items():
                    if isinstance(ba_val, dict):
                        ba_label = ba_val.get("label", ba_key).lower()
                        if (
                            ba_label == ch_label_lower
                            or ba_key.lower() == ch_key.lower()
                        ):
                            ba_match = ba_val
                            break
            if ba_match and isinstance(ba_match, dict):
                real_pct = ba_match.get("percentage") or 0
                real_dollar = ba_match.get("dollar_amount") or 0
                if real_pct > 0:
                    ch_data["pct"] = round(real_pct)
                if real_dollar > 0:
                    ch_data["_dollar_amount"] = real_dollar

    sorted_channels = sorted(channels.values(), key=lambda c: c["pct"], reverse=True)

    for idx, ch in enumerate(sorted_channels):
        row_y = bar_area_top + idx * bar_spacing

        # Category label (include dollar amount if available from budget engine)
        label_text = ch["label"]
        if ch.get("_dollar_amount"):
            label_text = (
                f"{ch['label']} ({_fmt_currency(ch['_dollar_amount'], compact=True)})"
            )

        _add_textbox(
            slide,
            left_col_left,
            row_y,
            label_w,
            bar_h,
            text=label_text,
            font_size=9,
            bold=True,
            color=DARK_TEXT,
            alignment=PP_ALIGN.RIGHT,
            anchor=MSO_ANCHOR.MIDDLE,
        )

        # Bar
        pct = ch["pct"]
        bar_w_val = bar_max_w * pct / 100
        if bar_w_val < Inches(0.15):
            bar_w_val = Inches(0.15)

        bar_left = left_col_left + label_w + Inches(0.15)
        bar_color = ch.get("color", BLUE)
        _add_rounded_rect(
            slide,
            bar_left,
            row_y + Inches(0.04),
            bar_w_val,
            bar_h - Inches(0.08),
            bar_color,
        )

        # Percentage
        _add_textbox(
            slide,
            bar_left + bar_w_val + Inches(0.08),
            row_y,
            Inches(0.6),
            bar_h,
            text=f"{pct}%",
            font_size=10,
            bold=True,
            color=NAVY,
            anchor=MSO_ANCHOR.MIDDLE,
        )

    # ==== RIGHT TOP: Benchmark Data Table ====
    right_col_left = Inches(7.5)
    right_col_w = Inches(5.3)

    _add_textbox(
        slide,
        right_col_left,
        section_top,
        right_col_w,
        Inches(0.35),
        text="INDUSTRY BENCHMARKS",
        font_size=11,
        bold=True,
        color=NAVY,
    )
    _add_filled_rect(
        slide,
        right_col_left,
        section_top + Inches(0.33),
        Inches(2.0),
        Inches(0.03),
        TEAL,
    )

    table_top = section_top + Inches(0.5)
    table_left = right_col_left
    table_w = Inches(5.1)
    row_h = Inches(0.38)

    # Build benchmark rows with trend annotations (v3)
    cpc_val = benchmarks["cpc"]
    cpa_val = benchmarks["cpa"]
    cpc_trend = benchmarks.get("cpc_trend") or ""
    if cpc_trend:
        cpc_val = f"{cpc_val}  ({cpc_trend})"
    bench_rows = [
        ("Industry CPA", cpa_val),
        ("Industry CPC", cpc_val),
        ("Est. Cost-per-Hire", benchmarks["cph"]),
        ("Apply Rate", benchmarks["apply_rate"]),
    ]

    # Add real job market data -- prefer synthesized over raw enrichment
    job_market = (
        synthesized.get("job_market_demand", {})
        if isinstance(synthesized, dict) and synthesized
        else {}
    )
    if not job_market:
        enriched = data.get("_enriched", {})
        job_market = enriched.get("job_market", {}) if enriched else {}
    if job_market:
        try:
            for role_name, jm_data in list(job_market.items())[:2]:
                if not isinstance(jm_data, dict) or role_name.startswith("_"):
                    continue
                # Handle both synthesized (total_postings) and raw enriched (posting_count) keys
                posting_count = jm_data.get(
                    "total_postings", jm_data.get("posting_count") or 0
                )
                avg_sal = jm_data.get("avg_salary") or 0
                if posting_count and posting_count > 0:
                    bench_rows.append(
                        (
                            f"Live Postings: {role_name}",
                            f"{posting_count:,} active jobs",
                        )
                    )
                if avg_sal and avg_sal > 0:
                    bench_rows.append(
                        (f"Avg Salary: {role_name}", _format_salary(avg_sal))
                    )
                # Synthesized data may have market_temperature
                market_temp = jm_data.get("market_temperature") or ""
                if market_temp and isinstance(market_temp, str):
                    bench_rows.append(
                        (f"Market Temp: {role_name}", market_temp.title())
                    )
        except (TypeError, AttributeError):
            pass

    # Add real ad platform analysis data from synthesized pipeline
    ad_plat = synthesized.get("ad_platform_analysis", {}) if synthesized else {}
    if ad_plat:
        try:
            for plat_name, plat_data in list(ad_plat.items())[:5]:
                if not isinstance(plat_data, dict) or plat_name.startswith("_"):
                    continue
                plat_label = plat_data.get(
                    "platform_name", plat_name.replace("_", " ").title()
                )
                plat_cpc = plat_data.get("CPC", plat_data.get("cpc") or 0)
                plat_cpa = plat_data.get("CPA", plat_data.get("cpa") or 0)
                plat_reach = plat_data.get("estimated_reach") or 0
                fit_score = plat_data.get("fit_score") or 0
                if plat_cpc and plat_cpc > 0:
                    bench_rows.append((f"{plat_label} CPC", _fmt_currency(plat_cpc)))
                if plat_cpa and plat_cpa > 0:
                    bench_rows.append((f"{plat_label} CPA", _fmt_currency(plat_cpa)))
                if plat_reach and plat_reach > 0:
                    bench_rows.append((f"{plat_label} Est. Reach", f"{plat_reach:,}"))
                if fit_score and fit_score > 0:
                    bench_rows.append(
                        (f"{plat_label} Fit Score", _fmt_pct(fit_score, decimals=0))
                    )
                # Deep intelligence data (91-platform KB enrichment)
                deep = plat_data.get("deep_intelligence", {})
                if isinstance(deep, dict) and deep:
                    visitors = deep.get("monthly_visitors")
                    if visitors:
                        bench_rows.append(
                            (f"{plat_label} Monthly Visitors", str(visitors))
                        )
                    best_for = deep.get("best_for") or []
                    if isinstance(best_for, list) and best_for:
                        bench_rows.append(
                            (
                                f"{plat_label} Best For",
                                ", ".join(str(b) for b in best_for[:3]),
                            )
                        )
        except (TypeError, AttributeError):
            pass

        # Programmatic insights from supply ecosystem KB
        prog_insights = ad_plat.get("_programmatic_insights", {})
        if isinstance(prog_insights, dict) and prog_insights:
            try:
                bidding = prog_insights.get("bidding_models", {})
                if isinstance(bidding, dict) and bidding:
                    for bk, bv in list(bidding.items())[:2]:
                        label = str(bk).replace("_", " ").title()
                        if isinstance(bv, dict):
                            desc = bv.get(
                                "description", str(next(iter(bv.values()), ""))
                            )
                        else:
                            desc = str(bv)
                        bench_rows.append((f"Bidding: {label}", str(desc)[:40]))
            except (TypeError, AttributeError):
                pass

    # Has ad platform data - use for 3-column table header
    has_ad_plat_data = bool(ad_plat)

    # Table header
    _add_filled_rect(slide, table_left, table_top, table_w, row_h, NAVY)
    _add_textbox(
        slide,
        table_left + Inches(0.15),
        table_top,
        Inches(2.2),
        row_h,
        text="Metric",
        font_size=9,
        bold=True,
        color=WHITE,
        anchor=MSO_ANCHOR.MIDDLE,
    )
    _add_textbox(
        slide,
        table_left + Inches(2.4),
        table_top,
        Inches(2.5),
        row_h,
        text=f"{industry_label} Range",
        font_size=9,
        bold=True,
        color=WHITE,
        anchor=MSO_ANCHOR.MIDDLE,
    )

    # v3: Determine value color based on confidence level
    bench_conf = benchmarks.get("confidence", "curated")
    conf_value_color, _conf_label = _confidence_color(bench_conf)

    for i, (metric, value) in enumerate(bench_rows):
        ry = table_top + row_h * (i + 1)
        bg = WHITE if i % 2 == 0 else RGBColor(0xF8, 0xF6, 0xF3)
        _add_filled_rect(slide, table_left, ry, table_w, row_h, bg)
        # Thin ruled line between rows (McKinsey/Bain style)
        _add_filled_rect(
            slide,
            table_left,
            ry + row_h - Inches(0.01),
            table_w,
            Inches(0.01),
            WARM_GRAY,
        )

        _add_textbox(
            slide,
            table_left + Inches(0.15),
            ry,
            Inches(2.2),
            row_h,
            text=metric,
            font_size=9,
            bold=True,
            color=DARK_TEXT,
            anchor=MSO_ANCHOR.MIDDLE,
        )
        # Use confidence-colored text for industry benchmark rows (first 4)
        # Live API rows use green, trend engine uses blue, curated uses navy
        val_color = conf_value_color if i < 4 else NAVY
        _add_textbox(
            slide,
            table_left + Inches(2.4),
            ry,
            Inches(2.5),
            row_h,
            text=str(value),
            font_size=10,
            bold=True,
            color=val_color,
            anchor=MSO_ANCHOR.MIDDLE,
        )

    # Source - adjust position based on actual number of rows
    source_top = table_top + row_h * (len(bench_rows) + 1) + Inches(0.05)
    source_text = f"Sources: Appcast {datetime.date.today().year}, Recruitics TMI, SHRM {datetime.date.today().year}"
    if job_market:
        source_text += ", Adzuna Job Market API"
    if ad_plat:
        source_text += ", Nova AI Suite Ad Platform Intelligence"
    _add_textbox(
        slide,
        table_left,
        source_top,
        table_w,
        Inches(0.2),
        text=source_text,
        font_size=7,
        italic=True,
        color=MUTED_TEXT,
    )

    # ==== CHANNEL ATTRIBUTION DIAGRAM (bottom area) ====
    attrib_top = Inches(4.85)
    _add_textbox(
        slide,
        Inches(0.55),
        attrib_top,
        Inches(12.2),
        Inches(0.35),
        text="CHANNEL CATEGORY ATTRIBUTION",
        font_size=11,
        bold=True,
        color=NAVY,
    )
    _add_filled_rect(
        slide, Inches(0.55), attrib_top + Inches(0.33), Inches(2.8), Inches(0.03), TEAL
    )

    # Build category groups
    cat_groups = _channel_categories_grouped(channels)

    # Attribution category boxes
    cat_colors = {
        "Programmatic": (NAVY, WHITE),
        "Job Boards": (BLUE, WHITE),
        "Social": (SKY_BLUE, NAVY),
        "Employer Brand": (TEAL, NAVY),
        "Other": (MEDIUM_BLUE, WHITE),
    }

    box_top = attrib_top + Inches(0.5)
    box_h = Inches(1.2)
    total_available_w = Inches(12.2)
    n_groups = len(cat_groups)

    if n_groups > 0:
        box_gap = Inches(0.15)
        box_w = (
            (total_available_w - box_gap * (n_groups - 1)) / n_groups
            if n_groups > 1
            else total_available_w
        )
        overlap_w = Inches(0.3)  # visual overlap zone

        for gi, (cat_name, cat_channels) in enumerate(cat_groups.items()):
            bx = Inches(0.55) + gi * (box_w + box_gap)
            bg_color, text_color = cat_colors.get(cat_name, (MEDIUM_BLUE, WHITE))

            # Category card
            _add_rounded_rect(slide, bx, box_top, box_w, box_h, bg_color)

            # Category name
            _add_textbox(
                slide,
                bx + Inches(0.15),
                box_top + Inches(0.08),
                box_w - Inches(0.3),
                Inches(0.3),
                text=cat_name.upper(),
                font_size=10,
                bold=True,
                color=text_color,
            )

            # Total percentage for this category
            cat_pct = sum(c["pct"] for c in cat_channels)
            _add_textbox(
                slide,
                bx + Inches(0.15),
                box_top + Inches(0.35),
                box_w - Inches(0.3),
                Inches(0.35),
                text=f"{cat_pct}%",
                font_size=22,
                bold=True,
                color=text_color,
            )

            # Channel list
            ch_list = ", ".join(c["label"] for c in cat_channels)
            _add_textbox(
                slide,
                bx + Inches(0.15),
                box_top + Inches(0.72),
                box_w - Inches(0.3),
                Inches(0.42),
                text=ch_list,
                font_size=7,
                color=text_color,
            )

        # Overlap connectors between categories (teal diamonds)
        for gi in range(n_groups - 1):
            connector_x = (
                Inches(0.55) + (gi + 1) * (box_w + box_gap) - box_gap / 2 - Inches(0.12)
            )
            connector_y = box_top + box_h / 2 - Inches(0.12)
            diamond = slide.shapes.add_shape(
                MSO_SHAPE.DIAMOND, connector_x, connector_y, Inches(0.24), Inches(0.24)
            )
            diamond.fill.solid()
            diamond.fill.fore_color.rgb = TEAL
            diamond.line.fill.background()

    # v3: Data sources footnote with confidence indicator
    _add_data_sources_footnote(slide, data, benchmarks)

    # Footer
    _add_footer(slide, today)


# ===================================================================
# SLIDE 5 - Quality & ROI Outcomes Grid
# ===================================================================


def _build_slide_quality_outcomes(prs: Presentation, data: Dict):
    """Build the Quality Outcomes grid slide with 4-quadrant metrics."""
    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)

    client = data.get("client_name", "Client")
    industry = data.get("industry", "general_entry_level")
    channels = _selected_channels(data)
    budget = data.get("budget", "TBD")
    roles = data.get("roles") or []
    locations = data.get("locations") or []
    today = datetime.date.today().strftime("%B %d, %Y")

    # Pull synthesized + budget allocation data (from pipeline)
    synthesized = data.get("_synthesized", {})
    budget_alloc = data.get("_budget_allocation", {})

    # Off-white background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE)

    # Top band
    _add_top_band(slide, "QUALITY & ROI PROJECTIONS", today)

    # Action title
    n_channels = len(channels)
    action_text = (
        f"Projected quality outcomes across {n_channels} optimized channels "
        f"for {client}'s programmatic media plan"
    )
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(0.92),
        Inches(12.2),
        Inches(0.5),
        text=action_text,
        font_size=15,
        bold=True,
        color=NAVY,
    )

    # ---- HERO STAT at top center ----
    hero_top = Inches(1.55)
    hero_h = Inches(1.3)

    # Hero stat card with teal accent
    _add_rounded_rect(slide, Inches(3.5), hero_top, Inches(6.33), hero_h, WHITE)
    _add_filled_rect(slide, Inches(3.5), hero_top, Inches(6.33), Inches(0.05), TEAL)

    # Hero number
    budget_display = _format_budget_display(budget)
    _add_textbox(
        slide,
        Inches(3.5),
        hero_top + Inches(0.12),
        Inches(6.33),
        Inches(0.75),
        text=budget_display if budget_display != budget else f"{n_channels} Channels",
        font_size=44,
        bold=True,
        color=BLUE,
        alignment=PP_ALIGN.CENTER,
        anchor=MSO_ANCHOR.MIDDLE,
    )
    hero_subtitle = (
        "Total Campaign Investment"
        if budget_display != budget
        else "Selected for Maximum Impact"
    )
    _add_textbox(
        slide,
        Inches(3.5),
        hero_top + Inches(0.85),
        Inches(6.33),
        Inches(0.35),
        text=hero_subtitle,
        font_size=12,
        bold=False,
        color=MUTED_TEXT,
        alignment=PP_ALIGN.CENTER,
    )

    # ---- CAMPAIGN PROJECTIONS SUMMARY (5-card row) ----
    ba_total_proj = budget_alloc.get("total_projected", {}) if budget_alloc else {}
    if not isinstance(ba_total_proj, dict):
        ba_total_proj = {}
    ba_metadata_qo = budget_alloc.get("metadata", {}) if budget_alloc else {}
    if not isinstance(ba_metadata_qo, dict):
        ba_metadata_qo = {}

    proj_clicks = ba_total_proj.get("clicks") or 0
    proj_apps = ba_total_proj.get("applications") or 0
    # S48 FIX: Compute hires from per-channel sum for consistency
    _qo_ch_allocs = budget_alloc.get("channel_allocations", {}) if budget_alloc else {}
    if not isinstance(_qo_ch_allocs, dict):
        _qo_ch_allocs = {}
    projected_hires = sum(
        int(ch.get("projected_hires") or 0) for ch in _qo_ch_allocs.values()
    )
    if projected_hires == 0:
        projected_hires = ba_total_proj.get("hires") or 0
    real_avg_cpa = ba_total_proj.get("cost_per_application") or 0
    _qo_budget = ba_metadata_qo.get("total_budget") or 0
    ba_avg_cph = (
        round(_qo_budget / max(projected_hires, 1), 2)
        if projected_hires > 0
        else (ba_total_proj.get("cost_per_hire") or 0)
    )

    benchmarks = _get_benchmarks(industry, data)
    cpa_str = benchmarks.get("cpa", "$25")
    try:
        cpa_nums = re.findall(r"[\d.]+", cpa_str.replace(",", ""))
        benchmark_avg_cpa = (
            sum(float(x) for x in cpa_nums) / len(cpa_nums) if cpa_nums else 25
        )
    except Exception:
        benchmark_avg_cpa = 25
    avg_cpa = real_avg_cpa if real_avg_cpa and real_avg_cpa > 0 else benchmark_avg_cpa
    efficiency_improvement = min(35, max(15, round(100 / avg_cpa * 5)))

    enriched = data.get("_enriched", {})
    salary_data = enriched.get("salary_data", {}) if enriched else {}

    # Section label
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(1.5),
        Inches(5),
        Inches(0.28),
        text="CAMPAIGN PROJECTIONS SUMMARY",
        font_size=10,
        bold=True,
        color=BLUE,
    )
    _add_filled_rect(slide, Inches(0.55), Inches(1.76), Inches(2.5), Inches(0.03), TEAL)

    # 5-metric summary cards
    summary_top = Inches(1.9)
    summary_h = Inches(1.0)
    card_w = Inches(2.3)
    card_gap = Inches(0.12)
    card_start_x = Inches(0.55)

    summary_metrics = [
        {
            "value": f"{proj_clicks:,}" if proj_clicks > 0 else "--",
            "label": "Projected Clicks",
            "accent": BLUE,
        },
        {
            "value": f"{int(proj_apps):,}" if proj_apps > 0 else "--",
            "label": "Projected Applications",
            "accent": TEAL,
        },
        {
            "value": f"{int(projected_hires):,}" if projected_hires > 0 else "--",
            "label": "Projected Hires",
            "accent": GREEN,
        },
        {
            "value": f"${avg_cpa:,.0f}" if avg_cpa > 0 else "--",
            "label": "Avg CPA",
            "accent": RGBColor(0xED, 0x7D, 0x31),
        },
        {
            "value": f"${ba_avg_cph:,.0f}" if ba_avg_cph > 0 else "--",
            "label": "Avg Cost/Hire",
            "accent": NAVY,
        },
    ]

    for si, sm in enumerate(summary_metrics):
        sx = card_start_x + si * (card_w + card_gap)
        _add_rounded_rect(slide, sx, summary_top, card_w, summary_h, WHITE)
        _add_filled_rect(slide, sx, summary_top, card_w, Inches(0.04), sm["accent"])
        _add_textbox(
            slide,
            sx,
            summary_top + Inches(0.1),
            card_w,
            Inches(0.5),
            text=sm["value"],
            font_size=26,
            bold=True,
            color=sm["accent"],
            alignment=PP_ALIGN.CENTER,
            anchor=MSO_ANCHOR.MIDDLE,
        )
        _add_textbox(
            slide,
            sx,
            summary_top + Inches(0.65),
            card_w,
            Inches(0.25),
            text=sm["label"],
            font_size=9,
            bold=False,
            color=MUTED_TEXT,
            alignment=PP_ALIGN.CENTER,
        )

    # ---- CHANNEL BREAKDOWN TABLE ----
    ch_table_top = Inches(3.15)
    ch_table_left = Inches(0.55)
    ch_table_w = Inches(12.2)

    # Section label
    _add_textbox(
        slide,
        ch_table_left,
        ch_table_top,
        Inches(5),
        Inches(0.28),
        text="CHANNEL-BY-CHANNEL PROJECTIONS",
        font_size=10,
        bold=True,
        color=BLUE,
    )

    # Get channel allocations
    ba_channel_alloc_qo = (
        budget_alloc.get("channel_allocations", {}) if budget_alloc else {}
    )
    if not isinstance(ba_channel_alloc_qo, dict):
        ba_channel_alloc_qo = {}

    # Build display data for channels
    ch_display_list = []
    for ch_key, ch_data in ba_channel_alloc_qo.items():
        if not isinstance(ch_data, dict):
            continue
        ch_display_list.append(
            {
                "label": CHANNEL_ALLOC.get(ch_key, {}).get("label")
                or ch_key.replace("_", " ").title(),
                "budget": ch_data.get("dollar_amount", ch_data.get("dollars") or 0),
                "clicks": ch_data.get("projected_clicks") or 0,
                "apps": ch_data.get("projected_applications") or 0,
                "hires": ch_data.get("projected_hires") or 0,
                "cpa": ch_data.get("cpa", ch_data.get("cost_per_application") or 0),
            }
        )

    # If no budget engine data, try to build from channels dict
    if not ch_display_list:
        ba_total_budget_qo = ba_metadata_qo.get("total_budget") or 0
        for ch_key, ch_data in channels.items():
            ch_pct = ch_data.get("pct") or 0
            ch_dollars = (
                ba_total_budget_qo * ch_pct / 100.0 if ba_total_budget_qo > 0 else 0
            )
            ch_display_list.append(
                {
                    "label": ch_data.get("label", ch_key.replace("_", " ").title()),
                    "budget": ch_dollars,
                    "clicks": 0,
                    "apps": 0,
                    "hires": 0,
                    "cpa": 0,
                }
            )

    # Sort by budget descending, take top 5
    ch_display_list.sort(key=lambda c: c.get("budget") or 0, reverse=True)
    ch_display_top5 = ch_display_list[:5]

    # Table header row
    header_y = ch_table_top + Inches(0.32)
    row_h = Inches(0.34)
    col_widths_qo = [
        Inches(3.0),
        Inches(2.0),
        Inches(1.8),
        Inches(1.8),
        Inches(1.8),
        Inches(1.8),
    ]
    col_headers_qo = ["Channel", "Budget", "Clicks", "Applications", "Hires", "CPA"]
    col_aligns_qo = [
        PP_ALIGN.LEFT,
        PP_ALIGN.CENTER,
        PP_ALIGN.CENTER,
        PP_ALIGN.CENTER,
        PP_ALIGN.CENTER,
        PP_ALIGN.CENTER,
    ]

    # Header background
    _add_filled_rect(slide, ch_table_left, header_y, ch_table_w, row_h, NAVY)
    cx = ch_table_left
    for ci, (header, cw) in enumerate(zip(col_headers_qo, col_widths_qo)):
        _add_textbox(
            slide,
            cx + Inches(0.1),
            header_y,
            cw - Inches(0.1),
            row_h,
            text=header,
            font_size=9,
            bold=True,
            color=WHITE,
            alignment=col_aligns_qo[ci],
            anchor=MSO_ANCHOR.MIDDLE,
        )
        cx += cw

    # Data rows (top 5 channels)
    for ri, ch in enumerate(ch_display_top5):
        row_y = header_y + row_h + ri * row_h
        row_bg = WHITE if ri % 2 == 0 else RGBColor(0xF5, 0xF5, 0xF3)
        _add_filled_rect(slide, ch_table_left, row_y, ch_table_w, row_h, row_bg)

        row_values = [
            ch["label"],
            f"${ch['budget']:,.0f}" if ch["budget"] > 0 else "--",
            f"{int(ch['clicks']):,}" if ch["clicks"] > 0 else "--",
            f"{int(ch['apps']):,}" if ch["apps"] > 0 else "--",
            f"{int(ch['hires']):,}" if ch["hires"] > 0 else "--",
            f"${ch['cpa']:,.0f}" if ch["cpa"] > 0 else "--",
        ]

        cx = ch_table_left
        for ci, (val, cw) in enumerate(zip(row_values, col_widths_qo)):
            left_pad = Inches(0.15) if ci == 0 else Inches(0.1)
            _add_textbox(
                slide,
                cx + left_pad,
                row_y,
                cw - left_pad,
                row_h,
                text=val,
                font_size=9,
                bold=(ci == 0),
                color=DARK_TEXT,
                alignment=col_aligns_qo[ci],
                anchor=MSO_ANCHOR.MIDDLE,
            )
            cx += cw

    # ---- BUDGET REALITY CHECK (if budget is insufficient) ----
    _suff_data = budget_alloc.get("sufficiency", {}) if budget_alloc else {}
    if not isinstance(_suff_data, dict):
        _suff_data = {}
    _budget_reality = (
        budget_alloc.get("budget_reality_check", {}) if budget_alloc else {}
    )
    if not isinstance(_budget_reality, dict):
        _budget_reality = {}

    _is_critical = False
    _reality_message = ""

    # Check budget_reality_check first (if another agent added it)
    if _budget_reality:
        _feas_tier = _budget_reality.get("feasibility_tier") or ""
        if _feas_tier in ("impossible", "severely_underfunded"):
            _is_critical = True
            _reality_message = _budget_reality.get("feasibility_message") or ""
            if not _reality_message:
                _reality_message = (
                    f"Budget is {_budget_reality.get('feasibility_label', 'severely underfunded')}. "
                    f"Budget per hire: ${_budget_reality.get('budget_per_hire') or 0:,.0f} vs. "
                    f"industry avg: ${_budget_reality.get('industry_avg_cph') or 0:,.0f}."
                )
    # Fall back to sufficiency data
    elif _suff_data and not _suff_data.get("sufficient", True):
        _is_critical = True
        _gap = _suff_data.get("gap_amount") or 0
        _avg_cph_suff = _suff_data.get("industry_avg_cost_per_hire") or 0
        _bpo = _suff_data.get("budget_per_opening") or 0
        _reality_message = (
            f"Budget per opening (${_bpo:,.0f}) is below industry average "
            f"cost-per-hire (${_avg_cph_suff:,.0f}). "
        )
        if _gap > 0:
            _reality_message += (
                f"An additional ${_gap:,.0f} is recommended to meet all hiring targets."
            )

    # Position for reality check or insight callout
    bottom_section_top = Inches(5.15)

    if _is_critical and _reality_message:
        # Red callout box for budget reality check
        reality_top = bottom_section_top
        reality_h = Inches(0.7)
        RED_BG = RGBColor(0xFD, 0xE8, 0xE8)
        RED_ACCENT = RGBColor(0xC6, 0x28, 0x28)
        _add_rounded_rect(
            slide, Inches(0.55), reality_top, Inches(12.2), reality_h, RED_BG
        )
        _add_filled_rect(
            slide, Inches(0.55), reality_top, Inches(0.06), reality_h, RED_ACCENT
        )

        # Badge
        _add_rounded_rect(
            slide,
            Inches(0.85),
            reality_top + Inches(0.17),
            Inches(2.0),
            Inches(0.35),
            RED_ACCENT,
        )
        _add_textbox(
            slide,
            Inches(0.85),
            reality_top + Inches(0.17),
            Inches(2.0),
            Inches(0.35),
            text="BUDGET REALITY CHECK",
            font_size=8,
            bold=True,
            color=WHITE,
            alignment=PP_ALIGN.CENTER,
            anchor=MSO_ANCHOR.MIDDLE,
        )

        _add_textbox(
            slide,
            Inches(3.1),
            reality_top + Inches(0.1),
            Inches(9.4),
            reality_h - Inches(0.15),
            text=_reality_message,
            font_size=10,
            bold=False,
            color=RGBColor(0xC6, 0x28, 0x28),
        )

        # Shift insight callout below
        insight_top = reality_top + reality_h + Inches(0.1)
    else:
        insight_top = bottom_section_top

    # ---- KEY INSIGHT CALLOUT BOX ----
    insight_h = Inches(0.85)
    _add_rounded_rect(
        slide, Inches(0.55), insight_top, Inches(12.2), insight_h, PALE_TEAL
    )
    _add_filled_rect(slide, Inches(0.55), insight_top, Inches(0.06), insight_h, TEAL)

    # Insight icon/badge
    _add_rounded_rect(
        slide, Inches(0.85), insight_top + Inches(0.15), Inches(1.0), Inches(0.35), TEAL
    )
    _add_textbox(
        slide,
        Inches(0.85),
        insight_top + Inches(0.15),
        Inches(1.0),
        Inches(0.35),
        text="KEY INSIGHT",
        font_size=8,
        bold=True,
        color=WHITE,
        alignment=PP_ALIGN.CENTER,
        anchor=MSO_ANCHOR.MIDDLE,
    )

    # Build insight text using real data when available
    if real_avg_cpa and real_avg_cpa > 0:
        insight_text = (
            f"Nova AI Suite's programmatic approach distributes {client}'s budget across "
            f"{n_channels} optimized channels with ML-driven bid management, "
            f"projecting ${real_avg_cpa:,.0f} avg CPA (vs. industry benchmark ${benchmark_avg_cpa:.0f}). "
            f"Quality-focused optimization (CPQA) ensures spend is directed toward "
            f"candidates most likely to apply and convert."
        )
    else:
        insight_text = (
            f"Nova AI Suite's programmatic approach distributes {client}'s budget across "
            f"{n_channels} optimized channels with ML-driven bid management, "
            f"projecting {efficiency_improvement}% CPA improvement over manual posting. "
            f"Quality-focused optimization (CPQA) ensures spend is directed toward "
            f"candidates most likely to apply and convert."
        )

    # Append projected hires if available from budget allocation
    if projected_hires and projected_hires > 0:
        total_apps_insight = ba_total_proj.get("applications") or 0
        if total_apps_insight and total_apps_insight > 0:
            insight_text += (
                f" Budget engine projects {int(total_apps_insight):,} applications and "
                f"{int(projected_hires):,} hires from the allocated investment."
            )
        else:
            insight_text += (
                f" Budget engine projects {int(projected_hires):,} hires "
                f"from the allocated investment."
            )

    # Append salary insight if enrichment data is available
    if salary_data:
        try:
            first_role = list(salary_data.keys())[0]
            median = salary_data[first_role].get("median") or 0
            source = salary_data[first_role].get("source", "BLS")
            if median > 0:
                insight_text += (
                    f" Market salary data ({source}) shows {_format_salary(median)} "
                    f"median for {first_role}, enabling precise budget calibration."
                )
        except (IndexError, KeyError, TypeError):
            pass

    _add_textbox(
        slide,
        Inches(2.1),
        insight_top + Inches(0.08),
        Inches(10.4),
        insight_h - Inches(0.15),
        text=_trunc_word(insight_text, 500),
        font_size=9,
        color=DARK_TEXT,
    )

    # Enrichment badge
    _add_enrichment_badge(slide, enriched)

    # Footer
    _add_footer(slide, today)


# ===================================================================
# SLIDE 6 - Budget Allocation & Projections
# ===================================================================


def _build_slide_budget_allocation(prs: Presentation, data: Dict):
    """Build a dedicated Budget Allocation slide showing dollar breakdown per channel,
    projected applications, projected hires, and ROI projections.

    This slide is only added when real budget allocation data is available from the
    budget engine. It provides the financial transparency Fortune 500 clients expect.
    """
    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)

    client = data.get("client_name", "Client")
    industry = data.get("industry", "general_entry_level")
    channels = _selected_channels(data)
    budget = data.get("budget", "TBD")
    today = datetime.date.today().strftime("%B %d, %Y")

    budget_alloc = data.get("_budget_allocation", {})
    if not isinstance(budget_alloc, dict):
        budget_alloc = {}
    ba_total_proj = budget_alloc.get("total_projected", {})
    if not isinstance(ba_total_proj, dict):
        ba_total_proj = {}
    ba_channel_alloc = budget_alloc.get("channel_allocations", {})
    if not isinstance(ba_channel_alloc, dict):
        ba_channel_alloc = {}
    ba_metadata = budget_alloc.get("metadata", {})
    if not isinstance(ba_metadata, dict):
        ba_metadata = {}
    ba_total_budget = ba_metadata.get("total_budget") or 0

    enriched = data.get("_enriched", {})

    # Off-white background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE)

    # Top band
    _add_top_band(slide, "BUDGET ALLOCATION & PROJECTIONS", today)

    # Action title
    n_channels = len(channels)
    action_text = (
        f"Investment breakdown across {n_channels} channels "
        f"with projected outcomes for {client}"
    )
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(0.92),
        Inches(12.2),
        Inches(0.45),
        text=action_text,
        font_size=15,
        bold=True,
        color=NAVY,
    )

    # ---- HERO STATS ROW (3 cards) ----
    hero_top = Inches(1.5)
    hero_h = Inches(1.1)
    hero_w = Inches(3.8)
    hero_gap = Inches(0.35)
    hero_start_x = Inches(0.55)

    # Total Investment
    total_display = (
        f"${ba_total_budget:,.0f}"
        if ba_total_budget > 0
        else _format_budget_display(budget)
    )

    # Projected Applications
    proj_apps = ba_total_proj.get("applications") or 0
    apps_display = f"{int(proj_apps):,}" if proj_apps and proj_apps > 0 else "--"

    # Projected Hires -- S48: per-channel sum for consistency
    proj_hires = sum(
        int(ch.get("projected_hires") or 0) for ch in ba_channel_alloc.values()
    )
    if proj_hires == 0:
        proj_hires = ba_total_proj.get("hires") or 0
    hires_display = f"{int(proj_hires):,}" if proj_hires and proj_hires > 0 else "--"

    hero_cards = [
        {"value": total_display, "label": "Total Investment", "accent": BLUE},
        {"value": apps_display, "label": "Projected Applications", "accent": TEAL},
        {"value": hires_display, "label": "Projected Hires", "accent": GREEN},
    ]

    for hi, hc in enumerate(hero_cards):
        hx = hero_start_x + hi * (hero_w + hero_gap)
        _add_rounded_rect(slide, hx, hero_top, hero_w, hero_h, WHITE)
        _add_filled_rect(slide, hx, hero_top, hero_w, Inches(0.05), hc["accent"])
        _add_textbox(
            slide,
            hx,
            hero_top + Inches(0.12),
            hero_w,
            Inches(0.6),
            text=hc["value"],
            font_size=34,
            bold=True,
            color=hc["accent"],
            alignment=PP_ALIGN.CENTER,
            anchor=MSO_ANCHOR.MIDDLE,
        )
        _add_textbox(
            slide,
            hx,
            hero_top + Inches(0.72),
            hero_w,
            Inches(0.3),
            text=hc["label"],
            font_size=11,
            bold=False,
            color=MUTED_TEXT,
            alignment=PP_ALIGN.CENTER,
        )

    # ---- CHANNEL BREAKDOWN TABLE ----
    table_top = Inches(2.85)
    table_left = Inches(0.55)
    table_w = Inches(12.2)

    # Section label
    _add_textbox(
        slide,
        table_left,
        table_top,
        Inches(5),
        Inches(0.3),
        text="CHANNEL-BY-CHANNEL BREAKDOWN",
        font_size=10,
        bold=True,
        color=BLUE,
    )

    # Map budget engine channel data onto our display channels
    display_channels = []
    for ch_key, ch_data in channels.items():
        entry = {
            "label": ch_data.get("label", ch_key.replace("_", " ").title()),
            "pct": ch_data.get("pct") or 0,
            "color": ch_data.get("color", BLUE),
            "dollar": 0,
            "projected_apps": 0,
            "projected_hires": 0,
            "cpa": 0,
        }
        # Match with budget engine data
        ba_match = ba_channel_alloc.get(ch_key)
        if not ba_match:
            ch_label_lower = (ch_data.get("label") or "").lower()
            for ba_key, ba_val in ba_channel_alloc.items():
                if isinstance(ba_val, dict):
                    ba_label = ba_val.get("label", ba_key).lower()
                    if ba_label == ch_label_lower or ba_key.lower() == ch_key.lower():
                        ba_match = ba_val
                        break
        if ba_match and isinstance(ba_match, dict):
            entry["dollar"] = ba_match.get("dollar_amount") or 0
            entry["projected_apps"] = ba_match.get("projected_applications") or 0
            entry["projected_hires"] = ba_match.get("projected_hires") or 0
            entry["cpa"] = ba_match.get("cpa") or 0
            real_pct = ba_match.get("percentage") or 0
            if real_pct > 0:
                entry["pct"] = round(real_pct)
        # Fallback: compute dollar from percentage if budget engine didn't provide it
        if entry["dollar"] == 0 and ba_total_budget > 0 and entry["pct"] > 0:
            entry["dollar"] = ba_total_budget * entry["pct"] / 100

        display_channels.append(entry)

    # Sort by dollar amount (descending), then by percentage
    display_channels.sort(key=lambda c: (c["dollar"], c["pct"]), reverse=True)

    # Table header row
    header_y = table_top + Inches(0.35)
    row_h = Inches(0.38)
    col_widths = [
        Inches(3.0),
        Inches(1.8),
        Inches(2.2),
        Inches(1.8),
        Inches(1.8),
        Inches(1.5),
    ]
    col_headers = [
        "Channel",
        "Allocation %",
        "Investment",
        "Proj. Apps",
        "Proj. Hires",
        "CPA",
    ]
    col_aligns = [
        PP_ALIGN.LEFT,
        PP_ALIGN.CENTER,
        PP_ALIGN.CENTER,
        PP_ALIGN.CENTER,
        PP_ALIGN.CENTER,
        PP_ALIGN.CENTER,
    ]

    # Header background
    _add_filled_rect(slide, table_left, header_y, table_w, row_h, NAVY)

    cx = table_left
    for ci, (header, cw) in enumerate(zip(col_headers, col_widths)):
        _add_textbox(
            slide,
            cx + Inches(0.1),
            header_y,
            cw - Inches(0.1),
            row_h,
            text=header,
            font_size=9,
            bold=True,
            color=WHITE,
            alignment=col_aligns[ci],
            anchor=MSO_ANCHOR.MIDDLE,
        )
        cx += cw

    # Data rows (limit to 8 channels to fit on slide)
    max_rows = min(len(display_channels), 8)
    for ri in range(max_rows):
        ch = display_channels[ri]
        row_y = header_y + row_h + ri * row_h
        row_bg = WHITE if ri % 2 == 0 else RGBColor(0xF5, 0xF5, 0xF3)
        _add_filled_rect(slide, table_left, row_y, table_w, row_h, row_bg)

        # Color indicator dot + Channel name
        dot_size = Inches(0.12)
        _add_oval(
            slide,
            table_left + Inches(0.12),
            row_y + (row_h - dot_size) / 2,
            dot_size,
            dot_size,
            ch["color"],
        )

        row_values = [
            ch["label"],
            f"{ch['pct']}%",
            f"${ch['dollar']:,.0f}" if ch["dollar"] > 0 else "--",
            f"{int(ch['projected_apps']):,}" if ch["projected_apps"] > 0 else "--",
            f"{int(ch['projected_hires']):,}" if ch["projected_hires"] > 0 else "--",
            f"${ch['cpa']:,.0f}" if ch["cpa"] > 0 else "--",
        ]

        cx = table_left
        for ci, (val, cw) in enumerate(zip(row_values, col_widths)):
            left_pad = Inches(0.3) if ci == 0 else Inches(0.1)
            _add_textbox(
                slide,
                cx + left_pad,
                row_y,
                cw - left_pad,
                row_h,
                text=val,
                font_size=9,
                bold=(ci == 0),
                color=DARK_TEXT,
                alignment=col_aligns[ci],
                anchor=MSO_ANCHOR.MIDDLE,
            )
            cx += cw

    # ---- ROI INSIGHT CALLOUT ----
    insight_top = Inches(6.05)
    insight_h = Inches(0.65)
    _add_rounded_rect(
        slide, Inches(0.55), insight_top, Inches(12.2), insight_h, PALE_TEAL
    )
    _add_filled_rect(slide, Inches(0.55), insight_top, Inches(0.06), insight_h, TEAL)

    # Build insight text
    avg_cpa = ba_total_proj.get("cost_per_application") or 0
    avg_cph = ba_total_proj.get("cost_per_hire") or 0

    if avg_cpa and avg_cpa > 0 and proj_hires and proj_hires > 0:
        insight_text = (
            f"Budget engine projects ${avg_cpa:,.0f} average CPA across all channels"
        )
        if avg_cph and avg_cph > 0:
            insight_text += f", with ${avg_cph:,.0f} average cost-per-hire"
        insight_text += (
            f". At {int(proj_hires):,} projected hires, "
            f"{client}'s investment yields strong programmatic ROI "
            f"through ML-driven bid optimization."
        )
    elif ba_total_budget > 0:
        insight_text = (
            f"{client}'s ${ba_total_budget:,.0f} investment is distributed across "
            f"{n_channels} channels using Nova AI Suite's programmatic optimization engine, "
            f"maximizing reach and conversion through real-time bid management."
        )
    else:
        insight_text = (
            f"Nova AI Suite's programmatic engine distributes {client}'s budget across "
            f"{n_channels} optimized channels with ML-driven bid management, "
            f"ensuring maximum ROI through continuous performance optimization."
        )

    _add_textbox(
        slide,
        Inches(0.8),
        insight_top + Inches(0.08),
        Inches(11.7),
        insight_h - Inches(0.15),
        text=_trunc_word(insight_text, 500),
        font_size=10,
        color=DARK_TEXT,
    )

    # Enrichment badge
    _add_enrichment_badge(slide, enriched)

    # Footer
    _add_footer(slide, today)


def _embed_pie_chart_on_budget_slide(prs: Presentation, data: Dict) -> None:
    """Embed a small budget pie chart onto the last slide (budget allocation).

    S48: Instead of a separate pie chart slide, this places a compact
    pie chart image in the bottom-right area of the budget allocation slide.
    Called immediately after _build_slide_budget_allocation in generate_pptx.
    """
    if not prs.slides or len(prs.slides) == 0:
        return

    channels = _selected_channels(data)
    if not channels:
        return

    budget_alloc = data.get("_budget_allocation", {})
    ba_channel_alloc = (
        budget_alloc.get("channel_allocations", {})
        if isinstance(budget_alloc, dict)
        else {}
    )

    labels: List[str] = []
    sizes: List[float] = []

    for ch_key, ch_data in channels.items():
        label = ch_data.get("label", ch_key.replace("_", " ").title())
        pct = ch_data.get("pct") or 0

        ba_match = ba_channel_alloc.get(ch_key)
        if not ba_match:
            ch_label_lower = (ch_data.get("label") or "").lower()
            for ba_key, ba_val in ba_channel_alloc.items():
                if isinstance(ba_val, dict):
                    ba_label = ba_val.get("label", ba_key).lower()
                    if ba_label == ch_label_lower or ba_key.lower() == ch_key.lower():
                        ba_match = ba_val
                        break
        if ba_match and isinstance(ba_match, dict):
            real_pct = ba_match.get("percentage") or 0
            if real_pct > 0:
                pct = round(real_pct)

        if pct > 0:
            labels.append(label)
            sizes.append(pct)

    if not labels:
        return

    # Generate a compact pie chart (smaller than the standalone version)
    try:
        fig, ax = plt.subplots(figsize=(3.5, 2.8), dpi=150)
        fig.patch.set_facecolor("#FFFDF9")

        colors = _CHART_COLORS[: len(labels)]
        while len(colors) < len(labels):
            colors.append(_CHART_COLORS[len(colors) % len(_CHART_COLORS)])

        wedges, texts, autotexts = ax.pie(
            sizes,
            labels=None,
            autopct=lambda pct: f"{pct:.0f}%" if pct >= 5 else "",
            startangle=90,
            colors=colors,
            pctdistance=0.75,
            wedgeprops={"linewidth": 1, "edgecolor": "white"},
        )

        for autotext in autotexts:
            autotext.set_fontsize(7)
            autotext.set_fontweight("bold")
            autotext.set_color("white")

        legend_labels = [f"{lbl} ({sz:.0f}%)" for lbl, sz in zip(labels, sizes)]
        ax.legend(
            wedges,
            legend_labels,
            loc="center left",
            bbox_to_anchor=(1.0, 0.5),
            fontsize=6,
            frameon=False,
        )

        plt.tight_layout()
        buf = io.BytesIO()
        fig.savefig(
            buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor()
        )
        plt.close(fig)
        buf.seek(0)
        chart_bytes = buf.getvalue()
    except Exception as exc:
        logger.warning("Embedded pie chart generation failed: %s", exc)
        return

    # Place the chart on the last slide (budget allocation) -- bottom-right
    budget_slide = prs.slides[len(prs.slides) - 1]
    chart_stream = io.BytesIO(chart_bytes)
    budget_slide.shapes.add_picture(
        chart_stream,
        Inches(8.8),  # right side
        Inches(5.5),  # below table rows, above footer
        Inches(4.2),  # width
        Inches(1.9),  # height
    )


# ===================================================================
# SLIDE 7 - Side-by-Side Comparison Panel + Implementation Timeline
# ===================================================================


def _build_slide_comparison_timeline(prs: Presentation, data: Dict):
    """Build comparison panel (Client Plan vs Industry Average) and implementation timeline."""
    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)

    client = data.get("client_name", "Client")
    industry = data.get("industry", "general_entry_level")
    industry_label = data.get("industry_label", industry.replace("_", " ").title())
    channels = _selected_channels(data)
    budget = data.get("budget", "TBD")
    locations = data.get("locations") or []
    roles = data.get("roles") or []
    today = datetime.date.today().strftime("%B %d, %Y")

    # Pull synthesized + budget allocation data (from pipeline)
    synthesized = data.get("_synthesized", {})
    budget_alloc = data.get("_budget_allocation", {})

    # Off-white background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE)

    # Top band
    _add_top_band(slide, "PLAN COMPARISON & IMPLEMENTATION", today)

    # Action title
    action_text = (
        f"{client}'s optimized media plan vs. {industry_label} industry averages "
        f"with phased implementation roadmap"
    )
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(0.92),
        Inches(12.2),
        Inches(0.45),
        text=action_text,
        font_size=15,
        bold=True,
        color=NAVY,
    )

    # ---- SIDE-BY-SIDE COMPARISON ----
    comp_top = Inches(1.55)
    panel_h = Inches(2.95)
    panel_w = Inches(5.9)
    panel_gap = Inches(0.4)
    left_panel_x = Inches(0.55)
    right_panel_x = left_panel_x + panel_w + panel_gap

    ind_benchmarks = _get_industry_comparison(industry, data)
    n_channels = len(channels)
    n_locations = len(locations)

    # Calculate client metrics
    sorted_ch = sorted(channels.values(), key=lambda c: c["pct"], reverse=True)
    programmatic_pct = 0
    for ch in channels.values():
        if ch.get("category") == "Programmatic":
            programmatic_pct += ch["pct"]
    if programmatic_pct == 0:
        programmatic_pct = sorted_ch[0]["pct"] if sorted_ch else 30

    client_reach_mult = 1.0 + (n_channels - 4) * 0.15
    ind_reach_mult = ind_benchmarks.get("estimated_reach_multiplier", 1.0)

    # Comparison metrics - build all candidates
    all_comparison_rows = [
        {
            "metric": "Channels Selected",
            "client_val": str(n_channels),
            "industry_val": str(ind_benchmarks.get("avg_channels", 4)),
            "is_better": n_channels >= ind_benchmarks.get("avg_channels", 4),
        },
        {
            "metric": "Programmatic Allocation",
            "client_val": f"{programmatic_pct}%",
            "industry_val": f"{ind_benchmarks.get('avg_budget_pct_programmatic', 30)}%",
            "is_better": programmatic_pct
            >= ind_benchmarks.get("avg_budget_pct_programmatic", 30),
        },
        {
            "metric": "Channel Diversity Score",
            "client_val": f"{min(10.0, n_channels * 1.5):.1f}/10",
            "industry_val": f"{min(10.0, ind_benchmarks.get('avg_channels', 4) * 1.5):.1f}/10",
            "is_better": n_channels >= ind_benchmarks.get("avg_channels", 4),
        },
        {
            "metric": "Geographic Coverage",
            "client_val": f"{n_locations} market{'s' if n_locations != 1 else ''}",
            "industry_val": "3-5 markets",
            "is_better": n_locations >= 3,
        },
        {
            "metric": "Reach Multiplier",
            "client_val": f"{client_reach_mult:.1f}x",
            "industry_val": f"{ind_reach_mult:.1f}x",
            "is_better": client_reach_mult >= ind_reach_mult,
        },
    ]

    # Add budget-allocation-powered comparison rows if real data is available
    ba_total_proj_comp = budget_alloc.get("total_projected", {}) if budget_alloc else {}
    if not isinstance(ba_total_proj_comp, dict):
        ba_total_proj_comp = {}
    ba_channel_alloc = (
        budget_alloc.get("channel_allocations", {}) if budget_alloc else {}
    )
    ba_metadata_comp = budget_alloc.get("metadata", {}) if budget_alloc else {}
    if not isinstance(ba_metadata_comp, dict):
        ba_metadata_comp = {}
    ba_total_budget = ba_metadata_comp.get("total_budget") or 0

    if ba_total_proj_comp:
        proj_cpa = ba_total_proj_comp.get("cost_per_application") or 0
        # S48 FIX: per-channel sum for consistency
        if isinstance(ba_channel_alloc, dict) and ba_channel_alloc:
            proj_hires = sum(
                int(ch.get("projected_hires") or 0) for ch in ba_channel_alloc.values()
            )
        else:
            proj_hires = 0
        if proj_hires == 0:
            proj_hires = ba_total_proj_comp.get("hires") or 0
        proj_apps = ba_total_proj_comp.get("applications") or 0

        # Get industry benchmark CPA for comparison
        bench = _get_benchmarks(industry, data)
        cpa_str = bench.get("cpa", "$25")
        try:
            cpa_nums = re.findall(r"[\d.]+", cpa_str.replace(",", ""))
            ind_avg_cpa = (
                sum(float(x) for x in cpa_nums) / len(cpa_nums) if cpa_nums else 25
            )
        except Exception:
            ind_avg_cpa = 25

        if proj_cpa and proj_cpa > 0:
            all_comparison_rows.append(
                {
                    "metric": "Projected CPA",
                    "client_val": f"${proj_cpa:,.0f}",
                    "industry_val": cpa_str,
                    "is_better": proj_cpa <= ind_avg_cpa,
                }
            )
        if proj_hires and proj_hires > 0:
            all_comparison_rows.append(
                {
                    "metric": "Projected Hires",
                    "client_val": f"{int(proj_hires):,}",
                    "industry_val": "N/A",
                    "is_better": True,
                }
            )
        if ba_total_budget and ba_total_budget > 0:
            all_comparison_rows.append(
                {
                    "metric": "Total Investment",
                    "client_val": f"${ba_total_budget:,.0f}",
                    "industry_val": "Varies",
                    "is_better": True,
                }
            )

    # Reframe trailing metrics with improvement targets to build confidence
    # Count how many are beating vs trailing
    beating_count = sum(1 for r in all_comparison_rows if r["is_better"])

    # If majority trailing, reframe trailing metrics as improvement opportunities
    if beating_count < len(all_comparison_rows) / 2:
        for row in all_comparison_rows:
            if not row["is_better"]:
                # Reframe with target - show current and where the plan aims to get
                row["client_val"] = f"{row['client_val']} \u2192 {row['industry_val']}"
                row["is_better"] = True  # Mark as positive (targeting improvement)
                row["metric"] = f"{row['metric']} (Target)"

    # Prioritize: show beating-benchmark rows first, then reframed ones
    comparison_rows = sorted(all_comparison_rows, key=lambda r: (not r["is_better"], 0))
    comparison_rows = comparison_rows[:5]  # limit to 5 rows

    # ==== LEFT PANEL: Client Plan ====
    _add_rounded_rect(slide, left_panel_x, comp_top, panel_w, panel_h, WHITE)
    # Header bar
    _add_filled_rect(slide, left_panel_x, comp_top, panel_w, Inches(0.45), NAVY)
    _add_textbox(
        slide,
        left_panel_x + Inches(0.2),
        comp_top + Inches(0.05),
        panel_w - Inches(0.4),
        Inches(0.35),
        text=f"\u2b22  {client}'s Plan",
        font_size=12,
        bold=True,
        color=WHITE,
        anchor=MSO_ANCHOR.MIDDLE,
    )

    row_h_comp = Inches(0.45)
    for ri, row in enumerate(comparison_rows):
        ry = comp_top + Inches(0.5) + ri * row_h_comp
        bg = WHITE if ri % 2 == 0 else RGBColor(0xF8, 0xF6, 0xF3)
        _add_filled_rect(
            slide,
            left_panel_x + Inches(0.05),
            ry,
            panel_w - Inches(0.1),
            row_h_comp,
            bg,
        )

        # Metric label
        _add_textbox(
            slide,
            left_panel_x + Inches(0.2),
            ry,
            Inches(2.8),
            row_h_comp,
            text=row["metric"],
            font_size=9,
            bold=True,
            color=DARK_TEXT,
            anchor=MSO_ANCHOR.MIDDLE,
        )

        # Value with status indicator
        status_color = GREEN if row["is_better"] else AMBER
        indicator = "\u25b2" if row["is_better"] else "\u25bc"

        val_box, val_tf = _add_textbox(
            slide,
            left_panel_x + Inches(3.2),
            ry,
            Inches(2.5),
            row_h_comp,
            anchor=MSO_ANCHOR.MIDDLE,
        )
        p = val_tf.paragraphs[0]
        p.alignment = PP_ALIGN.RIGHT
        r1 = p.add_run()
        r1.text = row["client_val"]
        _set_font(r1, size=12, bold=True, color=NAVY)
        r2 = p.add_run()
        r2.text = f"  {indicator}"
        _set_font(r2, size=10, bold=True, color=status_color)

    # ==== RIGHT PANEL: Industry Average ====
    _add_rounded_rect(slide, right_panel_x, comp_top, panel_w, panel_h, WHITE)
    _add_filled_rect(slide, right_panel_x, comp_top, panel_w, Inches(0.45), MUTED_TEXT)
    _add_textbox(
        slide,
        right_panel_x + Inches(0.2),
        comp_top + Inches(0.05),
        panel_w - Inches(0.4),
        Inches(0.35),
        text=f"\u25cb  {industry_label} Average",
        font_size=12,
        bold=True,
        color=WHITE,
        anchor=MSO_ANCHOR.MIDDLE,
    )

    for ri, row in enumerate(comparison_rows):
        ry = comp_top + Inches(0.5) + ri * row_h_comp
        bg = WHITE if ri % 2 == 0 else RGBColor(0xF8, 0xF6, 0xF3)
        _add_filled_rect(
            slide,
            right_panel_x + Inches(0.05),
            ry,
            panel_w - Inches(0.1),
            row_h_comp,
            bg,
        )

        _add_textbox(
            slide,
            right_panel_x + Inches(0.2),
            ry,
            Inches(2.8),
            row_h_comp,
            text=row["metric"],
            font_size=9,
            bold=True,
            color=DARK_TEXT,
            anchor=MSO_ANCHOR.MIDDLE,
        )

        _add_textbox(
            slide,
            right_panel_x + Inches(3.2),
            ry,
            Inches(2.5),
            row_h_comp,
            text=row["industry_val"],
            font_size=12,
            bold=True,
            color=MUTED_TEXT,
            alignment=PP_ALIGN.RIGHT,
            anchor=MSO_ANCHOR.MIDDLE,
        )

    # ---- Legend ----
    legend_y = comp_top + panel_h + Inches(0.1)
    leg_box, leg_tf = _add_textbox(
        slide,
        Inches(0.55),
        legend_y,
        Inches(6),
        Inches(0.25),
    )
    p = leg_tf.paragraphs[0]
    r1 = p.add_run()
    r1.text = "\u25b2 "
    _set_font(r1, size=8, bold=True, color=GREEN)
    r2 = p.add_run()
    r2.text = "Beating benchmark    "
    _set_font(r2, size=8, color=MUTED_TEXT)
    r3 = p.add_run()
    r3.text = "\u25bc "
    _set_font(r3, size=8, bold=True, color=AMBER)
    r4 = p.add_run()
    r4.text = "Trailing benchmark"
    _set_font(r4, size=8, color=MUTED_TEXT)

    # ==== IMPLEMENTATION TIMELINE (bottom) ====
    timeline_top = Inches(4.8)

    _add_textbox(
        slide,
        Inches(0.55),
        timeline_top,
        Inches(12.2),
        Inches(0.32),
        text="IMPLEMENTATION TIMELINE",
        font_size=11,
        bold=True,
        color=NAVY,
    )
    _add_filled_rect(
        slide, Inches(0.55), timeline_top + Inches(0.3), Inches(2.2), Inches(0.03), TEAL
    )

    # Build timeline phases based on actual campaign_weeks from input
    cw = data.get("campaign_weeks", 12)
    if cw <= 12:
        p2_end = min(6, cw)
        p3_start = min(7, cw)
        p3_end = cw
        phases = [
            {
                "phase": "PHASE 1",
                "weeks": "Weeks 1-2",
                "title": "Launch & Calibrate",
                "bullets": [
                    "Campaign setup & publisher activation",
                    "Baseline measurement & tracking",
                    "Attribution configuration",
                ],
                "color": BLUE,
                "accent_bg": LIGHT_BLUE,
            },
            {
                "phase": "PHASE 2",
                "weeks": f"Weeks 3-{p2_end}",
                "title": "Optimize & Scale",
                "bullets": [
                    "ML bid optimization active",
                    "A/B test creative & targeting",
                    "Scale top performers",
                ],
                "color": GREEN,
                "accent_bg": LIGHT_GREEN,
            },
            {
                "phase": "PHASE 3",
                "weeks": f"Weeks {p3_start}-{p3_end}",
                "title": "Maximize & Report",
                "bullets": [
                    "Full CPQA optimization",
                    "ROI analysis & reallocation",
                    "Performance review",
                ],
                "color": NAVY,
                "accent_bg": RGBColor(0xE8, 0xED, 0xF4),
            },
        ]
    elif cw <= 26:
        phases = [
            {
                "phase": "PHASE 1",
                "weeks": "Weeks 1-3",
                "title": "Launch & Calibrate",
                "bullets": [
                    "Campaign setup & publisher activation",
                    "Baseline measurement & tracking",
                    "Attribution configuration",
                ],
                "color": BLUE,
                "accent_bg": LIGHT_BLUE,
            },
            {
                "phase": "PHASE 2",
                "weeks": f"Weeks 4-{cw // 2}",
                "title": "Optimize & Scale",
                "bullets": [
                    "ML bid optimization active",
                    "A/B test creative & targeting",
                    "Scale top performers",
                ],
                "color": GREEN,
                "accent_bg": LIGHT_GREEN,
            },
            {
                "phase": "PHASE 3",
                "weeks": f"Weeks {cw // 2 + 1}-{cw}",
                "title": "Maximize & Report",
                "bullets": [
                    "Full CPQA optimization",
                    "ROI analysis & reallocation",
                    "Quarterly performance review",
                ],
                "color": NAVY,
                "accent_bg": RGBColor(0xE8, 0xED, 0xF4),
            },
        ]
    else:
        phases = [
            {
                "phase": "PHASE 1",
                "weeks": "Weeks 1-4",
                "title": "Launch & Calibrate",
                "bullets": [
                    "Campaign setup & publisher activation",
                    "Baseline measurement & tracking",
                    "Attribution configuration",
                ],
                "color": BLUE,
                "accent_bg": LIGHT_BLUE,
            },
            {
                "phase": "PHASE 2",
                "weeks": f"Weeks 5-{cw // 3}",
                "title": "Optimize & Scale",
                "bullets": [
                    "ML bid optimization active",
                    "A/B test creative & targeting",
                    "Scale top performers",
                ],
                "color": GREEN,
                "accent_bg": LIGHT_GREEN,
            },
            {
                "phase": "PHASE 3",
                "weeks": f"Weeks {cw // 3 + 1}-{cw}",
                "title": "Maximize & Report",
                "bullets": [
                    "Full CPQA optimization",
                    "ROI analysis & reallocation",
                    "Quarterly performance review",
                ],
                "color": NAVY,
                "accent_bg": RGBColor(0xE8, 0xED, 0xF4),
            },
        ]

    phase_w = Inches(3.85)
    phase_gap = Inches(0.25)
    phase_top = timeline_top + Inches(0.45)
    phase_h = Inches(1.65)

    for i, ph in enumerate(phases):
        px = Inches(0.55) + i * (phase_w + phase_gap)

        # Phase card
        _add_rounded_rect(slide, px, phase_top, phase_w, phase_h, WHITE)

        # Top accent bar
        _add_filled_rect(slide, px, phase_top, phase_w, Inches(0.05), ph["color"])

        # Phase number badge
        badge_w = Inches(0.9)
        badge_h = Inches(0.25)
        _add_rounded_rect(
            slide,
            px + Inches(0.12),
            phase_top + Inches(0.15),
            badge_w,
            badge_h,
            ph["accent_bg"],
        )
        _add_textbox(
            slide,
            px + Inches(0.12),
            phase_top + Inches(0.15),
            badge_w,
            badge_h,
            text=ph["phase"],
            font_size=7,
            bold=True,
            color=ph["color"],
            alignment=PP_ALIGN.CENTER,
            anchor=MSO_ANCHOR.MIDDLE,
        )

        # Weeks
        _add_textbox(
            slide,
            px + Inches(1.1),
            phase_top + Inches(0.15),
            Inches(1.5),
            badge_h,
            text=ph["weeks"],
            font_size=8,
            color=MUTED_TEXT,
            anchor=MSO_ANCHOR.MIDDLE,
        )

        # Title
        _add_textbox(
            slide,
            px + Inches(0.12),
            phase_top + Inches(0.48),
            phase_w - Inches(0.24),
            Inches(0.25),
            text=ph["title"],
            font_size=11,
            bold=True,
            color=DARK_TEXT,
        )

        # Bullets
        bx, btf = _add_textbox(
            slide,
            px + Inches(0.12),
            phase_top + Inches(0.78),
            phase_w - Inches(0.24),
            Inches(0.85),
        )
        btf.paragraphs[0].space_before = Pt(0)
        btf.paragraphs[0].space_after = Pt(0)

        for j, bullet in enumerate(ph["bullets"]):
            if j == 0:
                bp = btf.paragraphs[0]
            else:
                bp = btf.add_paragraph()
            bp.space_before = Pt(1)
            bp.space_after = Pt(3)
            bp.alignment = PP_ALIGN.LEFT

            br = bp.add_run()
            br.text = "\u2713  "
            _set_font(br, size=8, bold=False, color=ph["color"])
            bt = bp.add_run()
            bt.text = bullet
            _set_font(bt, size=8, color=MUTED_TEXT)

    # Arrow connectors between phases
    for i in range(2):
        ax = (
            Inches(0.55) + (i + 1) * (phase_w + phase_gap) - phase_gap / 2 - Inches(0.1)
        )
        ay = phase_top + phase_h / 2 - Inches(0.1)
        _add_textbox(
            slide,
            ax,
            ay,
            Inches(0.2),
            Inches(0.2),
            text="\u25b6",
            font_size=12,
            bold=True,
            color=TEAL,
            alignment=PP_ALIGN.CENTER,
        )

    # Footer
    _add_footer(slide, today)


# ===================================================================
# SLIDE - Market & Workforce Analysis (NEW)
# ===================================================================


def _build_slide_market_analysis(prs: Presentation, data: Dict):
    """Build the Market & Workforce Analysis slide.

    Uses:
    - job_market_demand: market temperature, trends, macro-economic data
    - workforce_insights: Gen-Z trends, employer branding, research
    - salary_intelligence: salary ranges per role
    """
    try:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)

        client = data.get("client_name", "Client")
        industry = data.get("industry", "general_entry_level")
        industry_label = data.get("industry_label", industry.replace("_", " ").title())
        roles = data.get("roles") or []
        today = datetime.date.today().strftime("%B %d, %Y")

        synthesized = data.get("_synthesized", {})
        if not isinstance(synthesized, dict):
            synthesized = {}
        job_market = synthesized.get("job_market_demand", {})
        if not isinstance(job_market, dict):
            job_market = {}
        workforce = synthesized.get("workforce_insights", {})
        if not isinstance(workforce, dict):
            workforce = {}
        salary_intel = synthesized.get("salary_intelligence", {})
        if not isinstance(salary_intel, dict):
            salary_intel = {}

        # Off-white background
        _add_filled_rect(
            slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
        )

        # Top band
        _add_top_band(slide, "MARKET & WORKFORCE ANALYSIS", today)

        # Action title
        action_text = (
            f"Labor market intelligence and workforce trend analysis for "
            f"{client}'s {industry_label} hiring strategy"
        )
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(0.92),
            Inches(12.2),
            Inches(0.5),
            text=action_text,
            font_size=15,
            bold=True,
            color=NAVY,
        )

        # ---- LEFT COLUMN: Market Demand by Role ----
        section_top = Inches(1.6)
        left_col_left = Inches(0.55)
        left_col_w = Inches(6.0)

        _add_textbox(
            slide,
            left_col_left,
            section_top,
            left_col_w,
            Inches(0.35),
            text="JOB MARKET DEMAND BY ROLE",
            font_size=11,
            bold=True,
            color=NAVY,
        )
        _add_filled_rect(
            slide,
            left_col_left,
            section_top + Inches(0.33),
            Inches(2.5),
            Inches(0.03),
            TEAL,
        )

        # Market demand table
        table_top = section_top + Inches(0.5)
        row_h = Inches(0.36)

        # Header
        _add_filled_rect(slide, left_col_left, table_top, left_col_w, row_h, NAVY)
        col_widths = [Inches(2.0), Inches(1.0), Inches(1.0), Inches(1.0), Inches(1.0)]
        col_headers = ["Role", "Postings", "Temp.", "Trend", "Competition"]
        cx = left_col_left
        for ci, (header, cw) in enumerate(zip(col_headers, col_widths)):
            _add_textbox(
                slide,
                cx + Inches(0.08),
                table_top,
                cw,
                row_h,
                text=header,
                font_size=8,
                bold=True,
                color=WHITE,
                anchor=MSO_ANCHOR.MIDDLE,
            )
            cx += cw

        # Data rows
        market_rows = []
        for role_name, role_data in list(job_market.items())[:6]:
            if not isinstance(role_data, dict):
                continue
            postings = role_data.get(
                "total_postings", role_data.get("posting_count") or 0
            )
            temp = role_data.get("market_temperature", "N/A")
            trend = role_data.get("trend_direction", "stable")
            comp_idx = role_data.get("competition_index") or 0
            market_rows.append(
                (
                    str(role_name)[:25],
                    (
                        f"{postings:,}"
                        if isinstance(postings, (int, float)) and postings > 0
                        else "N/A"
                    ),
                    temp.title() if temp else "N/A",
                    trend.title() if trend else "Stable",
                    (
                        f"{comp_idx:.2f}"
                        if isinstance(comp_idx, (int, float)) and comp_idx > 0
                        else "N/A"
                    ),
                )
            )

        if not market_rows:
            market_rows = [("Market data not available", "-", "-", "-", "-")]

        for ri, row_vals in enumerate(market_rows):
            ry = table_top + row_h * (ri + 1)
            bg = WHITE if ri % 2 == 0 else RGBColor(0xF8, 0xF6, 0xF3)
            _add_filled_rect(slide, left_col_left, ry, left_col_w, row_h, bg)
            cx = left_col_left
            for ci, (val, cw) in enumerate(zip(row_vals, col_widths)):
                # Color code temperature
                val_color = DARK_TEXT
                if ci == 2:  # Temperature column
                    if val.lower() == "hot":
                        val_color = RED_ACCENT
                    elif val.lower() == "warm":
                        val_color = AMBER
                    elif val.lower() == "cool":
                        val_color = BLUE
                    elif val.lower() == "cold":
                        val_color = MEDIUM_BLUE
                _add_textbox(
                    slide,
                    cx + Inches(0.08),
                    ry,
                    cw,
                    row_h,
                    text=val,
                    font_size=8,
                    bold=(ci == 0),
                    color=val_color,
                    anchor=MSO_ANCHOR.MIDDLE,
                )
                cx += cw

        # ---- Macro-Economic Context (below market table) ----
        macro_top = table_top + row_h * (len(market_rows) + 1) + Inches(0.25)
        _add_textbox(
            slide,
            left_col_left,
            macro_top,
            left_col_w,
            Inches(0.3),
            text="MACRO-ECONOMIC CONTEXT",
            font_size=10,
            bold=True,
            color=NAVY,
        )
        _add_filled_rect(
            slide,
            left_col_left,
            macro_top + Inches(0.28),
            Inches(2.0),
            Inches(0.03),
            TEAL,
        )

        # Extract macro data from first role's data
        macro_data = {}
        for _rk, _rv in job_market.items():
            if isinstance(_rv, dict) and _rv.get("macro_economic"):
                macro_data = _rv["macro_economic"]
                break

        macro_items = []
        if macro_data:
            unemp = macro_data.get("unemployment_rate")
            if unemp is not None:
                macro_items.append(
                    (
                        "Unemployment Rate",
                        f"{unemp}%" if isinstance(unemp, (int, float)) else str(unemp),
                    )
                )
            lfpr = macro_data.get("labor_force_participation")
            if lfpr is not None:
                macro_items.append(
                    (
                        "Labor Force Participation",
                        f"{lfpr}%" if isinstance(lfpr, (int, float)) else str(lfpr),
                    )
                )
            jolts = macro_data.get("job_openings_rate")
            if jolts is not None:
                macro_items.append(
                    (
                        "Job Openings Rate",
                        f"{jolts}%" if isinstance(jolts, (int, float)) else str(jolts),
                    )
                )

        if not macro_items:
            macro_items = [
                ("Unemployment Rate", "Data not available"),
                ("Labor Force Participation", "Data not available"),
            ]

        macro_card_top = macro_top + Inches(0.4)
        card_w = Inches(1.8)
        card_h = Inches(0.7)
        card_gap = Inches(0.15)

        for mi, (m_label, m_val) in enumerate(macro_items[:3]):
            mx = left_col_left + mi * (card_w + card_gap)
            _add_rounded_rect(slide, mx, macro_card_top, card_w, card_h, WHITE)
            _add_filled_rect(slide, mx, macro_card_top, card_w, Inches(0.04), TEAL)
            _add_textbox(
                slide,
                mx + Inches(0.1),
                macro_card_top + Inches(0.08),
                card_w - Inches(0.2),
                Inches(0.35),
                text=m_val,
                font_size=16,
                bold=True,
                color=NAVY,
                alignment=PP_ALIGN.CENTER,
                anchor=MSO_ANCHOR.MIDDLE,
            )
            _add_textbox(
                slide,
                mx + Inches(0.1),
                macro_card_top + Inches(0.42),
                card_w - Inches(0.2),
                Inches(0.22),
                text=m_label,
                font_size=7,
                color=MUTED_TEXT,
                alignment=PP_ALIGN.CENTER,
            )

        # ---- RIGHT COLUMN: Salary Intelligence ----
        right_col_left = Inches(7.0)
        right_col_w = Inches(5.8)

        _add_textbox(
            slide,
            right_col_left,
            section_top,
            right_col_w,
            Inches(0.35),
            text="SALARY INTELLIGENCE",
            font_size=11,
            bold=True,
            color=NAVY,
        )
        _add_filled_rect(
            slide,
            right_col_left,
            section_top + Inches(0.33),
            Inches(2.0),
            Inches(0.03),
            TEAL,
        )

        salary_card_top = section_top + Inches(0.5)
        sal_card_h = Inches(1.1)
        sal_card_gap = Inches(0.15)
        sal_card_w = right_col_w

        sal_count = 0
        for role_name, role_sal in list(salary_intel.items())[:4]:
            if not isinstance(role_sal, dict):
                continue
            median = role_sal.get("median") or 0
            if not median or median <= 0:
                continue

            sy = salary_card_top + sal_count * (sal_card_h + sal_card_gap)
            _add_rounded_rect(slide, right_col_left, sy, sal_card_w, sal_card_h, WHITE)
            _add_filled_rect(slide, right_col_left, sy, Inches(0.06), sal_card_h, BLUE)

            # Role name
            _add_textbox(
                slide,
                right_col_left + Inches(0.2),
                sy + Inches(0.06),
                sal_card_w - Inches(0.3),
                Inches(0.25),
                text=str(role_name)[:35],
                font_size=10,
                bold=True,
                color=DARK_TEXT,
            )

            # Salary bar visualization
            sal_min = role_sal.get("min", role_sal.get("p25", median * 0.7))
            sal_max = role_sal.get("max", role_sal.get("p75", median * 1.3))
            sources = role_sal.get("source_count") or 0
            confidence = role_sal.get("confidence") or ""

            bar_left = right_col_left + Inches(0.2)
            bar_top_y = sy + Inches(0.38)
            bar_w = sal_card_w - Inches(0.4)
            bar_h_sal = Inches(0.22)

            # Background bar
            _add_rounded_rect(slide, bar_left, bar_top_y, bar_w, bar_h_sal, LIGHT_BLUE)

            # Median marker (proportional position)
            if sal_max > sal_min and sal_max > 0:
                median_pct = min(
                    1.0, max(0.0, (median - sal_min) / (sal_max - sal_min))
                )
                marker_x = bar_left + bar_w * median_pct - Inches(0.05)
                _add_filled_rect(
                    slide,
                    marker_x,
                    bar_top_y - Inches(0.02),
                    Inches(0.1),
                    bar_h_sal + Inches(0.04),
                    BLUE,
                )

            # Labels
            label_y = sy + Inches(0.65)
            _add_textbox(
                slide,
                bar_left,
                label_y,
                Inches(1.5),
                Inches(0.2),
                text=f"Min: {_format_salary(sal_min)}" if sal_min > 0 else "",
                font_size=7,
                color=MUTED_TEXT,
            )
            _add_textbox(
                slide,
                bar_left + Inches(1.8),
                label_y,
                Inches(1.8),
                Inches(0.2),
                text=f"Median: {_format_salary(median)}",
                font_size=8,
                bold=True,
                color=NAVY,
                alignment=PP_ALIGN.CENTER,
            )
            _add_textbox(
                slide,
                bar_left + Inches(3.5),
                label_y,
                Inches(1.5),
                Inches(0.2),
                text=f"Max: {_format_salary(sal_max)}" if sal_max > 0 else "",
                font_size=7,
                color=MUTED_TEXT,
                alignment=PP_ALIGN.RIGHT,
            )

            # Source/confidence badge
            badge_text = ""
            if sources and sources > 0:
                badge_text = f"{sources} sources"
            if confidence:
                badge_text += f" | {confidence}" if badge_text else str(confidence)
            if badge_text:
                _add_textbox(
                    slide,
                    right_col_left + sal_card_w - Inches(1.8),
                    sy + Inches(0.06),
                    Inches(1.6),
                    Inches(0.2),
                    text=badge_text,
                    font_size=7,
                    italic=True,
                    color=MUTED_TEXT,
                    alignment=PP_ALIGN.RIGHT,
                )

            sal_count += 1

        if sal_count == 0:
            _add_textbox(
                slide,
                right_col_left,
                salary_card_top,
                sal_card_w,
                Inches(0.4),
                text="Salary data not available for selected roles",
                font_size=10,
                italic=True,
                color=MUTED_TEXT,
            )

        # ---- Workforce Trend Highlights (bottom right) ----
        wf_top = (
            section_top
            + Inches(0.5)
            + max(sal_count, 1) * (sal_card_h + sal_card_gap)
            + Inches(0.15)
        )
        _add_textbox(
            slide,
            right_col_left,
            wf_top,
            right_col_w,
            Inches(0.3),
            text="WORKFORCE TREND HIGHLIGHTS",
            font_size=10,
            bold=True,
            color=NAVY,
        )
        _add_filled_rect(
            slide,
            right_col_left,
            wf_top + Inches(0.28),
            Inches(2.2),
            Inches(0.03),
            TEAL,
        )

        wf_bullet_top = wf_top + Inches(0.4)
        wf_bullets = []

        # Gen-Z insights
        gen_z = workforce.get("gen_z_insights", {})
        if isinstance(gen_z, dict):
            wf_share = gen_z.get("workforce_share")
            if wf_share:
                wf_bullets.append(f"Gen-Z now represents {wf_share} of the workforce")
            platforms = gen_z.get("job_search_platforms", {})
            if isinstance(platforms, dict) and platforms:
                top_platform = next(iter(platforms.items()), (None, None))
                if top_platform[0]:
                    wf_bullets.append(
                        f"Top Gen-Z job search: {top_platform[0]} ({top_platform[1]})"
                    )

        # Employer branding
        eb = workforce.get("employer_branding", {})
        if isinstance(eb, dict):
            roi = eb.get("roi_data", {})
            if isinstance(roi, dict) and roi:
                cost_reduction = roi.get("cost_per_hire_reduction")
                if cost_reduction:
                    wf_bullets.append(
                        f"Strong employer brand reduces cost-per-hire by {cost_reduction}"
                    )

        # Research highlights
        research = workforce.get("relevant_research") or []
        if isinstance(research, list):
            for rr in research[:2]:
                if isinstance(rr, dict):
                    title = rr.get("title") or ""
                    publisher = rr.get("publisher") or ""
                    if title:
                        wf_bullets.append(
                            f"Research: {title[:50]}{'...' if len(title) > 50 else ''} ({publisher})"
                        )

        if not wf_bullets:
            wf_bullets = ["Workforce trend data not available for this industry"]

        box_wf, tf_wf = _add_textbox(
            slide, right_col_left, wf_bullet_top, right_col_w, Inches(1.2)
        )
        tf_wf.paragraphs[0].space_before = Pt(0)
        tf_wf.paragraphs[0].space_after = Pt(0)

        for bi, bullet in enumerate(wf_bullets[:4]):
            if bi == 0:
                p = tf_wf.paragraphs[0]
            else:
                p = tf_wf.add_paragraph()
            p.space_before = Pt(2)
            p.space_after = Pt(4)
            rb = p.add_run()
            rb.text = "\u25b8  "
            _set_font(rb, size=9, color=TEAL)
            rt = p.add_run()
            rt.text = str(bullet)
            _set_font(rt, size=8, color=DARK_TEXT)

        # Source line
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(6.7),
            Inches(12.2),
            Inches(0.2),
            text="Sources: BLS OES, O*NET, FRED, Google Trends, Adzuna, Industry Knowledge Base",
            font_size=7,
            italic=True,
            color=MUTED_TEXT,
        )

        # Footer
        _add_footer(slide, today)

    except Exception as exc:
        # If slide generation fails, log but don't crash the whole deck
        import logging

        logging.getLogger(__name__).warning("Market analysis slide failed: %s", exc)


# ===================================================================
# SLIDE - Location Analysis (NEW)
# ===================================================================


def _build_slide_location_analysis(prs: Presentation, data: Dict):
    """Build Location Analysis slide using location_profiles data.

    Uses:
    - location_profiles: population, cost of living, regional intelligence,
      top job boards, hiring regulations, cultural norms
    """
    try:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)

        client = data.get("client_name", "Client")
        locations = data.get("locations") or []
        today = datetime.date.today().strftime("%B %d, %Y")

        synthesized = data.get("_synthesized", {})
        if not isinstance(synthesized, dict):
            synthesized = {}
        loc_profiles = synthesized.get("location_profiles", {})
        if not isinstance(loc_profiles, dict):
            loc_profiles = {}

        # Fallback: if no synthesized location profiles, build from research.COUNTRY_DATA
        if not loc_profiles and research is not None and locations:
            for loc_str in locations[:4]:
                if not isinstance(loc_str, str):
                    continue
                # Try to detect a country name from the location string
                country_name = research._detect_country(loc_str)
                if country_name and country_name in research.COUNTRY_DATA:
                    cd = research.COUNTRY_DATA[country_name]
                    # Build a profile matching the expected location_profiles schema
                    pop_str = cd.get("population") or ""
                    try:
                        pop_val = int(
                            re.sub(
                                r"[^\d]",
                                "",
                                str(pop_str)
                                .replace("M", "000000")
                                .replace("B", "000000000"),
                            )
                        )
                    except (ValueError, TypeError):
                        pop_val = 0
                    loc_profiles[country_name] = {
                        "population": pop_val,
                        "median_household_income": cd.get("median_salary") or 0,
                        "cost_of_living_index": cd.get("coli") or 0,
                        "currency": cd.get("currency") or "",
                        "timezone": "",
                        "top_job_boards": cd.get("top_boards") or "",
                        "unemployment_rate": cd.get("unemployment") or "",
                        "top_industries": cd.get("top_industries") or "",
                    }
                elif not country_name:
                    # US location -- add a basic "United States" card if not already added
                    if "United States" not in loc_profiles:
                        us_data = research.COUNTRY_DATA.get("United States", {})
                        loc_profiles[loc_str] = {
                            "population": 333000000,
                            "median_household_income": us_data.get(
                                "median_salary", 65000
                            ),
                            "cost_of_living_index": us_data.get("coli", 100),
                            "currency": "USD",
                            "timezone": "",
                            "top_job_boards": us_data.get("top_boards") or "",
                            "unemployment_rate": us_data.get("unemployment") or "",
                            "top_industries": us_data.get("top_industries") or "",
                        }

        # Off-white background
        _add_filled_rect(
            slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
        )

        # Top band
        _add_top_band(slide, "LOCATION ANALYSIS", today)

        n_locs = len(locations)
        action_text = (
            f"Regional market intelligence across {n_locs} target location{'s' if n_locs != 1 else ''} "
            f"for {client}'s recruitment strategy"
        )
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(0.92),
            Inches(12.2),
            Inches(0.5),
            text=action_text,
            font_size=15,
            bold=True,
            color=NAVY,
        )

        # ---- Location Cards ----
        card_top = Inches(1.6)
        total_w = Inches(12.2)
        max_cards = min(len(loc_profiles), 4)  # Show up to 4 locations

        if max_cards == 0:
            # No location data -- show placeholder
            _add_textbox(
                slide,
                Inches(0.55),
                card_top,
                total_w,
                Inches(1.0),
                text="Location profile data not yet available. API enrichment in progress.",
                font_size=14,
                italic=True,
                color=MUTED_TEXT,
                alignment=PP_ALIGN.CENTER,
            )
            _add_footer(slide, today)
            return

        card_gap = Inches(0.2)
        card_w = (
            (total_w - card_gap * (max_cards - 1)) / max_cards
            if max_cards > 1
            else total_w
        )
        card_h = Inches(4.8)

        for li, (loc_name, loc_data) in enumerate(
            list(loc_profiles.items())[:max_cards]
        ):
            if not isinstance(loc_data, dict):
                continue

            cx = Inches(0.55) + li * (card_w + card_gap)

            # Card background
            _add_rounded_rect(slide, cx, card_top, card_w, card_h, WHITE)

            # Location name header
            _add_filled_rect(slide, cx, card_top, card_w, Inches(0.45), NAVY)
            _add_textbox(
                slide,
                cx + Inches(0.15),
                card_top + Inches(0.05),
                card_w - Inches(0.3),
                Inches(0.35),
                text=str(loc_name)[:30],
                font_size=11,
                bold=True,
                color=WHITE,
                anchor=MSO_ANCHOR.MIDDLE,
            )

            content_top = card_top + Inches(0.55)
            content_left = cx + Inches(0.15)
            content_w = card_w - Inches(0.3)

            # Demographics section
            items = []
            pop = loc_data.get("population") or 0
            if pop and pop > 0:
                items.append(("Population", f"{pop:,}"))
            income = loc_data.get("median_household_income") or 0
            if income and income > 0:
                items.append(("Median Income", f"${income:,}"))
            col_index = loc_data.get("cost_of_living_index") or 0
            if col_index and col_index > 0:
                items.append(("Cost of Living", f"{col_index:.0f}/100"))
            talent_density = loc_data.get("talent_density") or 0
            if talent_density and talent_density > 0:
                items.append(("Talent Density", f"{talent_density:.1%}"))
            unemployment = loc_data.get("unemployment_rate") or ""
            if unemployment:
                items.append(("Unemployment", str(unemployment)))
            currency = loc_data.get("currency") or ""
            if currency:
                items.append(("Currency", str(currency)))
            timezone = loc_data.get("timezone") or ""
            if timezone:
                items.append(("Timezone", str(timezone)[:18]))
            top_boards = loc_data.get("top_job_boards") or ""
            if top_boards:
                items.append(("Top Boards", str(top_boards)[:60]))

            box_loc, tf_loc = _add_textbox(
                slide, content_left, content_top, content_w, Inches(1.8)
            )
            tf_loc.paragraphs[0].space_before = Pt(0)
            tf_loc.paragraphs[0].space_after = Pt(0)

            first = True
            for label, value in items[:6]:
                if first:
                    p = tf_loc.paragraphs[0]
                    first = False
                else:
                    p = tf_loc.add_paragraph()
                p.space_before = Pt(1)
                p.space_after = Pt(3)
                rl = p.add_run()
                rl.text = f"{label}: "
                _set_font(rl, size=8, bold=True, color=DARK_TEXT)
                rv = p.add_run()
                rv.text = str(value)
                _set_font(rv, size=8, color=MUTED_TEXT)

            # Regional Intelligence section
            reg_intel = loc_data.get("regional_intelligence", {})
            if isinstance(reg_intel, dict) and reg_intel:
                ri_top = content_top + Inches(1.9)
                _add_filled_rect(
                    slide, content_left, ri_top, content_w, Inches(0.03), TEAL
                )

                _add_textbox(
                    slide,
                    content_left,
                    ri_top + Inches(0.08),
                    content_w,
                    Inches(0.2),
                    text="REGIONAL INTEL",
                    font_size=7,
                    bold=True,
                    color=TEAL,
                )

                ri_items = []

                # Top job boards
                boards = reg_intel.get("top_job_boards") or []
                if isinstance(boards, list) and boards:
                    board_names = [
                        b.get("name", str(b)) if isinstance(b, dict) else str(b)
                        for b in boards[:3]
                    ]
                    ri_items.append(("Top Boards", ", ".join(board_names)))

                # Hiring regulations
                regs = reg_intel.get("hiring_regulations", {})
                if isinstance(regs, dict) and regs:
                    notice_period = regs.get("notice_period") or ""
                    if notice_period:
                        ri_items.append(("Notice Period", str(notice_period)))
                    probation = regs.get("probation_period") or ""
                    if probation:
                        ri_items.append(("Probation", str(probation)))

                # Cultural norms
                norms = reg_intel.get("cultural_norms", {})
                if isinstance(norms, dict) and norms:
                    lang = norms.get("primary_language", norms.get("language") or "")
                    if lang:
                        ri_items.append(("Language", str(lang)))
                    comm = norms.get("communication_style") or ""
                    if comm:
                        ri_items.append(("Comm. Style", str(comm)[:20]))

                # CPA benchmark
                cpa_bench = reg_intel.get("cpa_benchmark", {})
                if isinstance(cpa_bench, dict):
                    cpa_range = cpa_bench.get("range", cpa_bench.get("typical") or "")
                    if cpa_range:
                        ri_items.append(("CPA Range", str(cpa_range)))

                box_ri, tf_ri = _add_textbox(
                    slide, content_left, ri_top + Inches(0.32), content_w, Inches(1.8)
                )
                tf_ri.paragraphs[0].space_before = Pt(0)
                tf_ri.paragraphs[0].space_after = Pt(0)

                ri_first = True
                for rl_label, rl_val in ri_items[:5]:
                    if ri_first:
                        p = tf_ri.paragraphs[0]
                        ri_first = False
                    else:
                        p = tf_ri.add_paragraph()
                    p.space_before = Pt(1)
                    p.space_after = Pt(3)
                    rl_run = p.add_run()
                    rl_run.text = f"\u25b8 {rl_label}: "
                    _set_font(rl_run, size=7, bold=True, color=TEAL)
                    rv_run = p.add_run()
                    rv_run.text = str(rl_val)
                    _set_font(rv_run, size=7, color=DARK_TEXT)

        # Source line
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(6.7),
            Inches(12.2),
            Inches(0.2),
            text="Sources: US Census Bureau, GeoNames, Teleport, DataUSA, World Bank, IMF",
            font_size=7,
            italic=True,
            color=MUTED_TEXT,
        )

        # Footer
        _add_footer(slide, today)

    except Exception as exc:
        import logging

        logging.getLogger(__name__).warning("Location analysis slide failed: %s", exc)


# ===================================================================
# SLIDE - Competitive Landscape (NEW)
# ===================================================================


def _build_slide_competitive_landscape(prs: Presentation, data: Dict):
    """Build the Competitive Landscape slide.

    Uses:
    - competitive_intelligence: company profile, competitor data,
      industry hiring trends, market positioning
    """
    try:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)

        client = data.get("client_name", "Client")
        industry_label = data.get("industry_label") or ""
        today = datetime.date.today().strftime("%B %d, %Y")

        synthesized = data.get("_synthesized", {})
        if not isinstance(synthesized, dict):
            synthesized = {}
        comp_intel = synthesized.get("competitive_intelligence", {})
        if not isinstance(comp_intel, dict):
            comp_intel = {}

        # Fallback: if no competitive intelligence from synthesis, try knowledge base
        if not comp_intel:
            kb = data.get("_knowledge_base", {})
            if isinstance(kb, dict) and kb:
                industry_key = data.get("industry", "general_entry_level")
                # Try recruitment_benchmarks section for industry-level data
                rb = kb.get("recruitment_benchmarks", {})
                if isinstance(rb, dict):
                    ind_bench = rb.get("industry_benchmarks", {}).get(industry_key, {})
                    if not isinstance(ind_bench, dict):
                        # Try alternative key formats (e.g., "technology_engineering" vs "tech_engineering")
                        for kb_key in rb.get("industry_benchmarks", {}):
                            if industry_key.split("_")[0] in kb_key:
                                ind_bench = rb["industry_benchmarks"][kb_key]
                                break
                    if ind_bench:
                        # Build a minimal comp_intel from KB benchmarks
                        hiring_trends_fb = {}
                        if ind_bench.get("time_to_fill"):
                            hiring_trends_fb["avg_time_to_fill"] = ind_bench[
                                "time_to_fill"
                            ]
                        if ind_bench.get("offer_acceptance_rate"):
                            hiring_trends_fb["offer_acceptance_rate"] = ind_bench[
                                "offer_acceptance_rate"
                            ]
                        if ind_bench.get("quality_of_hire"):
                            hiring_trends_fb["quality_metrics"] = ind_bench[
                                "quality_of_hire"
                            ]
                        if ind_bench.get("source_of_hire"):
                            hiring_trends_fb["top_sources"] = ind_bench[
                                "source_of_hire"
                            ]
                        if hiring_trends_fb:
                            comp_intel["hiring_trends"] = hiring_trends_fb
                            comp_intel["company_profile"] = {"name": client}

        # Off-white background
        _add_filled_rect(
            slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
        )

        # Top band
        _add_top_band(slide, "COMPETITIVE LANDSCAPE", today)

        action_text = (
            f"Market positioning and competitor intelligence for "
            f"{client}'s talent acquisition strategy in {industry_label}"
        )
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(0.92),
            Inches(12.2),
            Inches(0.5),
            text=action_text,
            font_size=15,
            bold=True,
            color=NAVY,
        )

        # ---- LEFT: Company Profile ----
        section_top = Inches(1.6)
        left_w = Inches(5.5)

        _add_textbox(
            slide,
            Inches(0.55),
            section_top,
            left_w,
            Inches(0.35),
            text="COMPANY PROFILE",
            font_size=11,
            bold=True,
            color=NAVY,
        )
        _add_filled_rect(
            slide,
            Inches(0.55),
            section_top + Inches(0.33),
            Inches(1.8),
            Inches(0.03),
            TEAL,
        )

        company = comp_intel.get("company_profile", {})
        if not isinstance(company, dict):
            company = {}

        profile_top = section_top + Inches(0.5)
        _add_rounded_rect(slide, Inches(0.55), profile_top, left_w, Inches(2.2), WHITE)
        _add_filled_rect(
            slide, Inches(0.55), profile_top, Inches(0.06), Inches(2.2), BLUE
        )

        profile_items = [
            ("Company", company.get("name", client)),
        ]
        desc = company.get("description") or ""
        if desc:
            profile_items.append(
                (
                    "Description",
                    str(desc)[:100] + ("..." if len(str(desc)) > 100 else ""),
                )
            )
        domain = company.get("domain") or ""
        if domain:
            profile_items.append(("Domain", str(domain)))
        is_public = company.get("is_public", False)
        if is_public:
            ticker = company.get("sec_ticker") or ""
            profile_items.append(
                ("Public Company", f"Ticker: {ticker}" if ticker else "Yes")
            )
            filings = company.get("recent_filings_count") or 0
            if filings and filings > 0:
                profile_items.append(("SEC Filings", f"{filings} recent filings"))
        sic_desc = company.get("sec_sic_description") or ""
        if sic_desc:
            profile_items.append(("SIC Industry", str(sic_desc)[:50]))
        tags = company.get("clearbit_tags") or []
        if isinstance(tags, list) and tags:
            profile_items.append(("Tags", ", ".join(str(t) for t in tags[:4])))

        box_p, tf_p = _add_textbox(
            slide,
            Inches(0.8),
            profile_top + Inches(0.15),
            left_w - Inches(0.4),
            Inches(2.0),
        )
        tf_p.paragraphs[0].space_before = Pt(0)
        tf_p.paragraphs[0].space_after = Pt(0)

        first = True
        for label, value in profile_items[:7]:
            if first:
                p = tf_p.paragraphs[0]
                first = False
            else:
                p = tf_p.add_paragraph()
            p.space_before = Pt(2)
            p.space_after = Pt(4)
            rl = p.add_run()
            rl.text = f"{label}:  "
            _set_font(rl, size=9, bold=True, color=DARK_TEXT)
            rv = p.add_run()
            rv.text = str(value)
            _set_font(rv, size=9, color=MUTED_TEXT)

        # ---- Industry Hiring Trends (below company profile) ----
        trends_top = profile_top + Inches(2.4)
        _add_textbox(
            slide,
            Inches(0.55),
            trends_top,
            left_w,
            Inches(0.3),
            text="INDUSTRY HIRING TRENDS",
            font_size=10,
            bold=True,
            color=NAVY,
        )
        _add_filled_rect(
            slide,
            Inches(0.55),
            trends_top + Inches(0.28),
            Inches(2.0),
            Inches(0.03),
            TEAL,
        )

        hiring_trends = comp_intel.get("hiring_trends", {})
        if not isinstance(hiring_trends, dict):
            hiring_trends = {}

        trend_items = []
        emp_count = hiring_trends.get("employment_count")
        if emp_count and isinstance(emp_count, (int, float)) and emp_count > 0:
            trend_items.append(f"Industry employment: {int(emp_count):,}")
        emp_growth = hiring_trends.get("employment_growth_rate")
        if emp_growth is not None:
            trend_items.append(f"Growth rate: {emp_growth}")
        avg_wage = hiring_trends.get("average_weekly_wage")
        if avg_wage and isinstance(avg_wage, (int, float)) and avg_wage > 0:
            trend_items.append(f"Avg weekly wage: ${avg_wage:,.0f}")
        establishments = hiring_trends.get("establishments")
        if (
            establishments
            and isinstance(establishments, (int, float))
            and establishments > 0
        ):
            trend_items.append(f"Establishments: {int(establishments):,}")

        # KB-derived trends
        kb_insights = hiring_trends.get("kb_insights", {})
        if isinstance(kb_insights, dict):
            outlook = kb_insights.get("outlook") or ""
            if outlook:
                trend_items.append(f"Outlook: {outlook}")
            demand_drivers = kb_insights.get("demand_drivers") or []
            if isinstance(demand_drivers, list) and demand_drivers:
                trend_items.append(
                    f"Drivers: {', '.join(str(d) for d in demand_drivers[:3])}"
                )

        # KB benchmark fallback data (from _knowledge_base)
        ttf = hiring_trends.get("avg_time_to_fill")
        if ttf:
            trend_items.append(f"Avg Time-to-Fill: {ttf}")
        oar = hiring_trends.get("offer_acceptance_rate")
        if oar:
            trend_items.append(f"Offer Acceptance: {oar}")
        top_src = hiring_trends.get("top_sources")
        if isinstance(top_src, dict) and top_src:
            src_items = [f"{k}: {v}" for k, v in list(top_src.items())[:3]]
            trend_items.append(f"Top Sources: {', '.join(src_items)}")

        if not trend_items:
            trend_items = ["Industry trend data not available"]

        box_t, tf_t = _add_textbox(
            slide, Inches(0.55), trends_top + Inches(0.4), left_w, Inches(1.5)
        )
        tf_t.paragraphs[0].space_before = Pt(0)
        tf_t.paragraphs[0].space_after = Pt(0)

        for ti, item in enumerate(trend_items[:5]):
            if ti == 0:
                p = tf_t.paragraphs[0]
            else:
                p = tf_t.add_paragraph()
            p.space_before = Pt(1)
            p.space_after = Pt(4)
            rb = p.add_run()
            rb.text = "\u25b8  "
            _set_font(rb, size=9, color=TEAL)
            rt = p.add_run()
            rt.text = str(item)
            _set_font(rt, size=9, color=DARK_TEXT)

        # ---- RIGHT: Competitor Cards ----
        right_left = Inches(6.5)
        right_w = Inches(6.3)

        _add_textbox(
            slide,
            right_left,
            section_top,
            right_w,
            Inches(0.35),
            text="COMPETITOR LANDSCAPE",
            font_size=11,
            bold=True,
            color=NAVY,
        )
        _add_filled_rect(
            slide,
            right_left,
            section_top + Inches(0.33),
            Inches(2.2),
            Inches(0.03),
            TEAL,
        )

        competitors = comp_intel.get("competitors", {})
        if not isinstance(competitors, dict):
            competitors = {}

        # Fallback: pull from gold standard competitor_mapping if synthesis is empty
        if not competitors:
            gold = data.get("_gold_standard") or {}
            comp_map = gold.get("competitor_mapping") or {}
            if isinstance(comp_map, dict) and comp_map:
                seen: set = set()
                for city_name, city_info in comp_map.items():
                    if isinstance(city_name, str) and city_name.startswith("_"):
                        continue
                    if not isinstance(city_info, dict):
                        continue
                    for employer in (city_info.get("top_employers") or [])[:3]:
                        emp_name = str(employer).strip()
                        if emp_name and emp_name not in seen:
                            seen.add(emp_name)
                            competitors[emp_name] = {
                                "domain": "",
                                "description": f"Competing employer in {city_name}",
                            }
                        if len(competitors) >= 4:
                            break
                    if len(competitors) >= 4:
                        break

        comp_card_top = section_top + Inches(0.5)
        comp_card_h = Inches(0.8)
        comp_card_gap = Inches(0.12)

        if not competitors:
            _add_textbox(
                slide,
                right_left,
                comp_card_top,
                right_w,
                Inches(0.4),
                text="No competitor data available. Add competitors to your request.",
                font_size=10,
                italic=True,
                color=MUTED_TEXT,
            )
        else:
            comp_card_h = Inches(1.0)  # Taller cards for counter-strategy
            comp_card_gap = Inches(0.1)
            for ci, (comp_name, comp_data) in enumerate(list(competitors.items())[:4]):
                if not isinstance(comp_data, dict):
                    continue
                cy = comp_card_top + ci * (comp_card_h + comp_card_gap)

                _add_rounded_rect(slide, right_left, cy, right_w, comp_card_h, WHITE)

                # Competitor name with color accent
                accent_colors = [BLUE, TEAL, NAVY, GREEN, AMBER]
                accent = accent_colors[ci % len(accent_colors)]
                _add_filled_rect(
                    slide, right_left, cy, Inches(0.06), comp_card_h, accent
                )

                _add_textbox(
                    slide,
                    right_left + Inches(0.2),
                    cy + Inches(0.05),
                    Inches(3.0),
                    Inches(0.25),
                    text=str(comp_name),
                    font_size=10,
                    bold=True,
                    color=DARK_TEXT,
                )

                # Why they matter
                comp_domain = comp_data.get("domain") or ""
                comp_desc = comp_data.get("description") or ""
                why_text = ""
                if comp_desc:
                    why_text = str(comp_desc)[:80]
                elif comp_domain:
                    why_text = f"Competes for same talent pool ({comp_domain})"
                else:
                    why_text = "Active in same talent market"

                _add_textbox(
                    slide,
                    right_left + Inches(0.2),
                    cy + Inches(0.3),
                    right_w - Inches(0.4),
                    Inches(0.25),
                    text=f"Why: {why_text}",
                    font_size=8,
                    color=MUTED_TEXT,
                )

                # Counter-strategy
                counter = (
                    f"Counter: Differentiate with career growth narrative, "
                    f"faster hiring process, and culture-first employer brand"
                )
                _add_textbox(
                    slide,
                    right_left + Inches(0.2),
                    cy + Inches(0.58),
                    right_w - Inches(0.4),
                    Inches(0.35),
                    text=counter,
                    font_size=8,
                    bold=False,
                    color=GREEN,
                )

        # Market positioning insight
        positioning = comp_intel.get("market_positioning", {})
        if isinstance(positioning, dict) and positioning:
            pos_top = Inches(5.8)
            _add_rounded_rect(
                slide, Inches(0.55), pos_top, Inches(12.2), Inches(0.7), PALE_TEAL
            )
            _add_filled_rect(
                slide, Inches(0.55), pos_top, Inches(0.06), Inches(0.7), TEAL
            )

            pos_text = positioning.get("summary", positioning.get("insight") or "")
            if pos_text:
                _add_textbox(
                    slide,
                    Inches(0.8),
                    pos_top + Inches(0.1),
                    Inches(11.7),
                    Inches(0.5),
                    text=str(pos_text)[:200],
                    font_size=9,
                    color=DARK_TEXT,
                )

        # Source line
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(6.7),
            Inches(12.2),
            Inches(0.2),
            text="Sources: Wikipedia, Clearbit, SEC EDGAR, BLS QCEW, Industry Knowledge Base",
            font_size=7,
            italic=True,
            color=MUTED_TEXT,
        )

        # Footer
        _add_footer(slide, today)

    except Exception as exc:
        import logging

        logging.getLogger(__name__).warning(
            "Competitive landscape slide failed: %s", exc
        )


# ===================================================================
# SLIDE - Workforce Trends (NEW)
# ===================================================================


def _build_slide_geopolitical_risk(prs: Presentation, data: Dict):
    """Build the Geopolitical Risk Assessment slide.

    Uses _synthesized.geopolitical_context from api_enrichment.
    Shows risk score badge, per-location event cards, recommendations.
    Only shown when risk_level is not 'low'.
    """
    try:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)

        client = data.get("client_name", "Client")
        today = datetime.date.today().strftime("%B %d, %Y")
        synthesized = data.get("_synthesized", {})
        if not isinstance(synthesized, dict):
            synthesized = {}
        geo = synthesized.get("geopolitical_context", {})
        if not isinstance(geo, dict):
            geo = {}

        risk_level = geo.get("risk_level", "low")
        overall_score = geo.get("overall_risk_score", 1.0)
        summary_text = geo.get(
            "summary", "No significant geopolitical risks identified."
        )
        recommendations = geo.get("recommendations") or []
        loc_data = geo.get("locations", {})

        # Background
        _add_filled_rect(
            slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
        )

        # Top band - use red for high/critical, amber for moderate
        band_color = NAVY
        if risk_level in ("high", "critical"):
            band_color = RED_ACCENT
        elif risk_level == "moderate":
            band_color = AMBER
        _add_top_band(
            slide, "GEOPOLITICAL RISK ASSESSMENT", today, band_color=band_color
        )

        # Action text
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(0.92),
            Inches(12.2),
            Inches(0.5),
            text=f"Market risk factors impacting {client}'s recruitment campaigns",
            font_size=15,
            bold=True,
            color=NAVY,
        )

        # Risk score badge (top right area)
        badge_color = (
            GREEN
            if overall_score <= 3
            else (AMBER if overall_score <= 6 else RED_ACCENT)
        )
        badge_bg = (
            LIGHT_GREEN
            if overall_score <= 3
            else (LIGHT_AMBER if overall_score <= 6 else RGBColor(0xFD, 0xE2, 0xE2))
        )
        _add_rounded_rect(
            slide, Inches(10.5), Inches(0.85), Inches(2.2), Inches(0.6), badge_bg
        )
        _add_textbox(
            slide,
            Inches(10.5),
            Inches(0.88),
            Inches(2.2),
            Inches(0.55),
            text=f"Risk: {risk_level.upper()} ({overall_score:.1f}/10)",
            font_size=12,
            bold=True,
            color=badge_color,
            alignment=PP_ALIGN.CENTER,
        )

        # Summary section -- size text to fit longer narratives
        summary_display = _trunc_word(summary_text, 500)
        summary_font = 11 if len(summary_display) <= 200 else 9
        summary_box_h = Inches(0.9) if len(summary_display) <= 200 else Inches(1.1)
        _add_rounded_rect(
            slide, Inches(0.55), Inches(1.6), Inches(12.2), summary_box_h, WHITE
        )
        _add_textbox(
            slide,
            Inches(0.75),
            Inches(1.65),
            Inches(11.8),
            summary_box_h - Inches(0.1),
            text=summary_display,
            font_size=summary_font,
            color=DARK_TEXT,
        )

        # Per-location cards -- adjust top for variable summary height
        card_top = Inches(1.6) + summary_box_h + Inches(0.2)
        max_locations = min(len(loc_data), 4)
        if max_locations > 0:
            card_w = Inches((12.0 / max_locations) - 0.15)
            card_gap = Inches(0.15)

            for i, (loc_name, loc_info) in enumerate(list(loc_data.items())[:4]):
                if not isinstance(loc_info, dict):
                    continue
                card_left = Inches(0.55) + i * (card_w + card_gap)
                loc_score = loc_info.get("risk_score", 1.0)
                loc_events = loc_info.get("events") or []
                loc_adj = loc_info.get("budget_adjustment_factor", 1.0)

                # Card background
                card_bg = (
                    LIGHT_GREEN
                    if loc_score <= 3
                    else (LIGHT_AMBER if loc_score <= 6 else RGBColor(0xFD, 0xE2, 0xE2))
                )
                _add_rounded_rect(
                    slide, card_left, card_top, card_w, Inches(2.8), WHITE
                )
                # Top accent bar
                accent = (
                    GREEN
                    if loc_score <= 3
                    else (AMBER if loc_score <= 6 else RED_ACCENT)
                )
                _add_filled_rect(
                    slide, card_left, card_top, card_w, Inches(0.05), accent
                )

                # Location name + score
                _add_textbox(
                    slide,
                    card_left + Inches(0.1),
                    card_top + Inches(0.1),
                    card_w - Inches(0.2),
                    Inches(0.3),
                    text=str(loc_name)[:30],
                    font_size=11,
                    bold=True,
                    color=NAVY,
                )
                _add_textbox(
                    slide,
                    card_left + Inches(0.1),
                    card_top + Inches(0.38),
                    card_w - Inches(0.2),
                    Inches(0.25),
                    text=f"Risk: {loc_score:.1f}/10  |  Budget adj: {loc_adj:.2f}x",
                    font_size=9,
                    color=accent,
                    bold=True,
                )

                # Events list
                event_y = card_top + Inches(0.7)
                for ev in loc_events[:3]:
                    if not isinstance(ev, dict):
                        continue
                    ev_text = ev.get("event") or ""[:80]
                    severity = ev.get("severity", "low")
                    sev_icon = {
                        "low": " ",
                        "moderate": " ",
                        "high": " ",
                        "critical": " ",
                    }.get(severity, " ")
                    _add_textbox(
                        slide,
                        card_left + Inches(0.1),
                        event_y,
                        card_w - Inches(0.2),
                        Inches(0.45),
                        text=f"{sev_icon}{ev_text}",
                        font_size=8,
                        color=DARK_TEXT,
                    )
                    event_y += Inches(0.42)

        # Recommendations section
        rec_top = Inches(5.7)
        if recommendations:
            _add_textbox(
                slide,
                Inches(0.55),
                rec_top,
                Inches(12.2),
                Inches(0.3),
                text="RECOMMENDATIONS",
                font_size=10,
                bold=True,
                color=BLUE,
            )

            rec_text = " | ".join(_trunc_word(r, 200) for r in recommendations[:4])
            _add_textbox(
                slide,
                Inches(0.55),
                rec_top + Inches(0.3),
                Inches(12.2),
                Inches(0.9),
                text=rec_text,
                font_size=9,
                color=DARK_TEXT,
            )

        # Source attribution -- sanitize internal provider names
        _llm_keywords = {
            "claude",
            "haiku",
            "sonnet",
            "opus",
            "gpt",
            "gemini",
            "groq",
            "llama",
            "mistral",
            "anthropic",
            "openai",
        }
        raw_source = str(geo.get("source") or "")
        # Strip provider names and confidence suffixes from source string
        if any(kw in raw_source.lower() for kw in _llm_keywords) or not raw_source:
            source_display = "AI Analysis"
        else:
            source_display = raw_source
        # Format confidence as proper percentage
        raw_conf = geo.get("confidence") or 0
        try:
            conf_val = float(raw_conf)
        except (TypeError, ValueError):
            conf_val = 0.0
        if 0 < conf_val <= 1.0:
            conf_val = conf_val * 100
        conf_str = f"{conf_val:.0f}%" if conf_val > 0 else "N/A"
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(6.8),
            Inches(12.2),
            Inches(0.3),
            text=f"Source: {source_display} | Confidence: {conf_str}",
            font_size=7,
            italic=True,
            color=MUTED_TEXT,
        )

    except Exception as exc:
        logger.warning("Failed to build geopolitical risk slide: %s", exc)


def _build_slide_workforce_trends(prs: Presentation, data: Dict):
    """Build the Workforce Trends slide.

    Uses:
    - workforce_insights: Gen-Z preferences, employer branding,
      white paper citations, remote work trends, supply partner trends
    """
    try:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)

        client = data.get("client_name", "Client")
        industry_label = data.get("industry_label") or ""
        today = datetime.date.today().strftime("%B %d, %Y")

        synthesized = data.get("_synthesized", {})
        if not isinstance(synthesized, dict):
            synthesized = {}
        workforce = synthesized.get("workforce_insights", {})
        if not isinstance(workforce, dict):
            workforce = {}

        # Off-white background
        _add_filled_rect(
            slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
        )

        # Top band
        _add_top_band(slide, "WORKFORCE TRENDS & INSIGHTS", today)

        action_text = (
            f"Emerging workforce trends shaping {client}'s talent acquisition "
            f"strategy in {industry_label}"
        )
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(0.92),
            Inches(12.2),
            Inches(0.5),
            text=action_text,
            font_size=15,
            bold=True,
            color=NAVY,
        )

        # ---- THREE-COLUMN LAYOUT ----
        section_top = Inches(1.6)
        col_w = Inches(3.95)
        col_gap = Inches(0.2)
        col1_left = Inches(0.55)
        col2_left = col1_left + col_w + col_gap
        col3_left = col2_left + col_w + col_gap
        col_h = Inches(4.5)

        # ---- COLUMN 1: Gen-Z Insights ----
        _add_rounded_rect(slide, col1_left, section_top, col_w, col_h, WHITE)
        _add_filled_rect(slide, col1_left, section_top, col_w, Inches(0.05), BLUE)

        _add_textbox(
            slide,
            col1_left + Inches(0.15),
            section_top + Inches(0.12),
            col_w - Inches(0.3),
            Inches(0.3),
            text="GEN-Z WORKFORCE TRENDS",
            font_size=10,
            bold=True,
            color=BLUE,
        )

        gen_z = workforce.get("gen_z_insights", {})
        if not isinstance(gen_z, dict):
            gen_z = {}

        gz_items = []

        wf_share = gen_z.get("workforce_share")
        if wf_share:
            gz_items.append(("Workforce Share", str(wf_share)))

        # Platform preferences
        platforms = gen_z.get("job_search_platforms", {})
        if isinstance(platforms, dict) and platforms:
            for pname, pval in list(platforms.items())[:3]:
                gz_items.append((str(pname), str(pval)))

        # Mobile vs desktop
        mobile = gen_z.get("mobile_vs_desktop", {})
        if isinstance(mobile, dict):
            mobile_pct = mobile.get("mobile", mobile.get("mobile_first") or "")
            if mobile_pct:
                gz_items.append(("Mobile Usage", str(mobile_pct)))

        # Social media habits
        social = gen_z.get("social_media_habits", {})
        if isinstance(social, dict) and social:
            for sname, sval in list(social.items())[:2]:
                gz_items.append((str(sname).title(), str(sval)))

        # Workplace expectations
        expectations = gen_z.get("workplace_expectations", {})
        if isinstance(expectations, dict):
            flex = expectations.get("flexibility", {})
            if isinstance(flex, dict):
                remote_pref = flex.get(
                    "remote_preference", flex.get("flexible_work") or ""
                )
                if remote_pref:
                    gz_items.append(("Flexibility", str(remote_pref)))
            dei = expectations.get("dei", {})
            if isinstance(dei, dict):
                dei_imp = dei.get("importance", dei.get("priority") or "")
                if dei_imp:
                    gz_items.append(("DEI Expectations", str(dei_imp)))
            mh = expectations.get("mental_health", {})
            if isinstance(mh, dict):
                mh_priority = mh.get("priority", mh.get("importance") or "")
                if mh_priority:
                    gz_items.append(("Mental Health", str(mh_priority)))

        # Tenure
        tenure = gen_z.get("tenure", {})
        if isinstance(tenure, dict):
            avg_tenure = tenure.get("average", tenure.get("median") or "")
            if avg_tenure:
                gz_items.append(("Avg Tenure", str(avg_tenure)))

        if not gz_items:
            gz_items = [("Status", "Gen-Z data not available")]

        box_gz, tf_gz = _add_textbox(
            slide,
            col1_left + Inches(0.15),
            section_top + Inches(0.5),
            col_w - Inches(0.3),
            col_h - Inches(0.6),
        )
        tf_gz.paragraphs[0].space_before = Pt(0)
        tf_gz.paragraphs[0].space_after = Pt(0)

        for gi, (g_label, g_val) in enumerate(gz_items[:10]):
            if gi == 0:
                p = tf_gz.paragraphs[0]
            else:
                p = tf_gz.add_paragraph()
            p.space_before = Pt(2)
            p.space_after = Pt(4)
            rl = p.add_run()
            rl.text = f"{g_label}:  "
            _set_font(rl, size=8, bold=True, color=DARK_TEXT)
            rv = p.add_run()
            rv.text = str(g_val)[:50]
            _set_font(rv, size=8, color=MUTED_TEXT)

        # ---- COLUMN 2: Employer Branding ----
        _add_rounded_rect(slide, col2_left, section_top, col_w, col_h, WHITE)
        _add_filled_rect(slide, col2_left, section_top, col_w, Inches(0.05), TEAL)

        _add_textbox(
            slide,
            col2_left + Inches(0.15),
            section_top + Inches(0.12),
            col_w - Inches(0.3),
            Inches(0.3),
            text="EMPLOYER BRANDING",
            font_size=10,
            bold=True,
            color=TEAL,
        )

        eb = workforce.get("employer_branding", {})
        if not isinstance(eb, dict):
            eb = {}

        eb_items = []

        # ROI data
        roi = eb.get("roi_data", {})
        if isinstance(roi, dict):
            for rk, rv_val in list(roi.items())[:5]:
                label = str(rk).replace("_", " ").title()
                eb_items.append((label, str(rv_val)))

        # Best practices
        bp = eb.get("best_practices", {})
        if isinstance(bp, dict):
            for bk, bv in list(bp.items())[:3]:
                label = str(bk).replace("_", " ").title()
                if isinstance(bv, list):
                    eb_items.append((label, ", ".join(str(v) for v in bv[:3])))
                elif isinstance(bv, dict):
                    eb_items.append((label, str(next(iter(bv.values()), ""))))
                else:
                    eb_items.append((label, str(bv)[:50]))

        # Channel effectiveness
        ch_eff = eb.get("channel_effectiveness", {})
        if isinstance(ch_eff, dict) and ch_eff:
            for ck, cv in list(ch_eff.items())[:3]:
                label = str(ck).replace("_", " ").title()
                if isinstance(cv, dict):
                    eb_items.append(
                        (f"Channel: {label}", str(next(iter(cv.values()), "")))
                    )
                else:
                    eb_items.append((f"Channel: {label}", str(cv)[:40]))

        if not eb_items:
            eb_items = [("Status", "Employer branding data not available")]

        box_eb, tf_eb = _add_textbox(
            slide,
            col2_left + Inches(0.15),
            section_top + Inches(0.5),
            col_w - Inches(0.3),
            col_h - Inches(0.6),
        )
        tf_eb.paragraphs[0].space_before = Pt(0)
        tf_eb.paragraphs[0].space_after = Pt(0)

        for ei, (e_label, e_val) in enumerate(eb_items[:10]):
            if ei == 0:
                p = tf_eb.paragraphs[0]
            else:
                p = tf_eb.add_paragraph()
            p.space_before = Pt(2)
            p.space_after = Pt(4)
            rl = p.add_run()
            rl.text = f"{e_label}:  "
            _set_font(rl, size=8, bold=True, color=DARK_TEXT)
            rv = p.add_run()
            rv.text = str(e_val)[:50]
            _set_font(rv, size=8, color=MUTED_TEXT)

        # ---- COLUMN 3: Research & Supply Trends ----
        _add_rounded_rect(slide, col3_left, section_top, col_w, col_h, WHITE)
        _add_filled_rect(slide, col3_left, section_top, col_w, Inches(0.05), NAVY)

        _add_textbox(
            slide,
            col3_left + Inches(0.15),
            section_top + Inches(0.12),
            col_w - Inches(0.3),
            Inches(0.3),
            text="RESEARCH & INDUSTRY DATA",
            font_size=10,
            bold=True,
            color=NAVY,
        )

        # White paper citations
        research = workforce.get("relevant_research") or []
        r_items = []
        if isinstance(research, list):
            for rr in research[:4]:
                if isinstance(rr, dict):
                    title = rr.get("title") or ""
                    publisher = rr.get("publisher") or ""
                    year = rr.get("year") or ""
                    findings = rr.get("top_findings") or []
                    if title:
                        r_items.append(
                            {
                                "title": str(title)[:60],
                                "publisher": str(publisher),
                                "year": str(year) if year else "",
                                "finding": (
                                    str(findings[0])[:60]
                                    if isinstance(findings, list) and findings
                                    else ""
                                ),
                            }
                        )

        # Supply partner trends
        sp = workforce.get("supply_partner_trends", {})
        # Job type trends
        jt = workforce.get("job_type_trends", {})

        box_r, tf_r = _add_textbox(
            slide,
            col3_left + Inches(0.15),
            section_top + Inches(0.5),
            col_w - Inches(0.3),
            col_h - Inches(0.6),
        )
        tf_r.paragraphs[0].space_before = Pt(0)
        tf_r.paragraphs[0].space_after = Pt(0)

        first_r = True
        for ri_item in r_items:
            if first_r:
                p = tf_r.paragraphs[0]
                first_r = False
            else:
                p = tf_r.add_paragraph()
            p.space_before = Pt(3)
            p.space_after = Pt(2)
            rt = p.add_run()
            rt.text = ri_item["title"]
            _set_font(rt, size=8, bold=True, color=DARK_TEXT)

            p2 = tf_r.add_paragraph()
            p2.space_before = Pt(0)
            p2.space_after = Pt(2)
            rs = p2.add_run()
            source_str = ri_item["publisher"]
            if ri_item["year"]:
                source_str += f" ({ri_item['year']})"
            rs.text = source_str
            _set_font(rs, size=7, italic=True, color=MUTED_TEXT)

            if ri_item["finding"]:
                p3 = tf_r.add_paragraph()
                p3.space_before = Pt(0)
                p3.space_after = Pt(4)
                rf = p3.add_run()
                rf.text = f"\u25b8 {ri_item['finding']}"
                _set_font(rf, size=7, color=TEAL)

        # Supply partner trends section
        if isinstance(sp, dict) and sp:
            p_sp = tf_r.add_paragraph()
            p_sp.space_before = Pt(6)
            p_sp.space_after = Pt(2)
            r_sp = p_sp.add_run()
            r_sp.text = "Supply Partner Trends"
            _set_font(r_sp, size=8, bold=True, color=NAVY)

            for sk, sv in list(sp.items())[:3]:
                p_s = tf_r.add_paragraph()
                p_s.space_before = Pt(1)
                p_s.space_after = Pt(3)
                rs1 = p_s.add_run()
                rs1.text = f"\u25b8 {str(sk).replace('_', ' ').title()}: "
                _set_font(rs1, size=7, bold=True, color=TEAL)
                rs2 = p_s.add_run()
                if isinstance(sv, dict):
                    rs2.text = str(next(iter(sv.values()), ""))[:40]
                else:
                    rs2.text = str(sv)[:40]
                _set_font(rs2, size=7, color=DARK_TEXT)

        # Job type trends section
        if isinstance(jt, dict) and jt:
            p_jt = tf_r.add_paragraph()
            p_jt.space_before = Pt(6)
            p_jt.space_after = Pt(2)
            r_jt = p_jt.add_run()
            r_jt.text = "Job Type Trends"
            _set_font(r_jt, size=8, bold=True, color=NAVY)

            for jk, jv in list(jt.items())[:3]:
                p_j = tf_r.add_paragraph()
                p_j.space_before = Pt(1)
                p_j.space_after = Pt(3)
                rj1 = p_j.add_run()
                rj1.text = f"\u25b8 {str(jk).replace('_', ' ').title()}: "
                _set_font(rj1, size=7, bold=True, color=TEAL)
                rj2 = p_j.add_run()
                if isinstance(jv, dict):
                    rj2.text = str(next(iter(jv.values()), ""))[:40]
                else:
                    rj2.text = str(jv)[:40]
                _set_font(rj2, size=7, color=DARK_TEXT)

        if not r_items and not sp and not jt:
            p = tf_r.paragraphs[0]
            r = p.add_run()
            r.text = "Research and trend data not available for this industry"
            _set_font(r, size=9, italic=True, color=MUTED_TEXT)

        # Source line
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(6.7),
            Inches(12.2),
            Inches(0.2),
            text="Sources: Recruitment Industry White Papers, Workforce Trends Intelligence, Employer Branding Research",
            font_size=7,
            italic=True,
            color=MUTED_TEXT,
        )

        # Footer
        _add_footer(slide, today)

    except Exception as exc:
        import logging

        logging.getLogger(__name__).warning("Workforce trends slide failed: %s", exc)


# ===================================================================
# SLIDE: Creative Testing Plan (A/B Test Recommendations)
# ===================================================================


# -- Channel-specific creative testing data --
_CHANNEL_CREATIVE_DATA: Dict[str, Dict[str, Any]] = {
    "indeed": {
        "variants": [
            "Benefit-focused: Lead with salary range and benefits package",
            "Growth-focused: Emphasize career development and training",
            "Culture-focused: Highlight team dynamics and work environment",
        ],
        "bidding": "Start with CPC bidding; shift to CPA once 50+ conversions",
        "ctr_range": "2.5% - 4.2%",
    },
    "linkedin": {
        "variants": [
            "Professional tone: Industry-specific language and requirements",
            "Employer brand: Company mission and values-led messaging",
            "Opportunity-focused: Unique projects and impact statements",
        ],
        "bidding": "Maximum delivery bidding for awareness; target cost for applications",
        "ctr_range": "0.4% - 0.9%",
    },
    "google": {
        "variants": [
            "Direct CTA: 'Apply Now' with role title and location",
            "Question-based: 'Looking for [Role]? Join [Company]'",
            "Benefit-led: '[Salary] + Benefits - [Role] at [Company]'",
        ],
        "bidding": "Enhanced CPC with conversion tracking; shift to target CPA at scale",
        "ctr_range": "3.0% - 6.5%",
    },
    "facebook": {
        "variants": [
            "Video testimonial: 30s employee day-in-the-life",
            "Carousel: Multiple roles with distinct creative per card",
            "Single image: Bold text overlay with clear CTA",
        ],
        "bidding": "Lowest cost bidding; use cost cap once baseline CPA established",
        "ctr_range": "0.8% - 2.1%",
    },
    "programmatic": {
        "variants": [
            "Retargeting: Personalized ads for previous job page visitors",
            "Contextual: Industry-relevant placement with role-specific copy",
            "Dynamic: Auto-populate role title and location from feed",
        ],
        "bidding": "Programmatic CPC with daily budget caps; optimize toward CPA",
        "ctr_range": "0.3% - 1.2%",
    },
}

_INDUSTRY_TEST_FRAMEWORKS: Dict[str, List[str]] = {
    "healthcare_medical": [
        "Test credential requirements prominence (above vs. below fold)",
        "Compare sign-on bonus vs. career growth messaging",
        "A/B shift flexibility language (flexible vs. set schedules)",
    ],
    "tech_engineering": [
        "Test remote-first vs. hybrid messaging in job titles",
        "Compare tech stack listing vs. project impact descriptions",
        "A/B salary transparency (range shown vs. competitive compensation)",
    ],
    "retail_consumer": [
        "Test immediate start language vs. standard posting",
        "Compare hourly rate prominence vs. total compensation",
        "A/B employee discount/perks messaging effectiveness",
    ],
    "default": [
        "Test salary transparency vs. competitive compensation language",
        "Compare short-form (3 bullet) vs. detailed job descriptions",
        "A/B apply button placement and CTA wording",
    ],
}


def _build_slide_creative_testing(prs: Presentation, data: Dict) -> None:
    """Build the Creative Testing Plan slide with A/B test recommendations.

    Provides per-channel ad copy variants, testing framework, bidding
    strategies, and expected CTR ranges based on the plan's channel list
    and industry vertical.
    """
    try:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)

        client = data.get("client_name") or "Client"
        industry = data.get("industry") or "general_entry_level"
        industry_label = (
            data.get("industry_label") or industry.replace("_", " ").title()
        )
        today = datetime.date.today().strftime("%B %d, %Y")

        # Off-white background
        _add_filled_rect(
            slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
        )

        # Top band
        _add_top_band(slide, "CREATIVE TESTING PLAN", today)

        # Subtitle
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(0.92),
            Inches(12.2),
            Inches(0.5),
            text=f"A/B testing recommendations for {client}'s {industry_label} campaigns",
            font_size=15,
            bold=True,
            color=NAVY,
        )

        # Get channels from plan data
        channels_raw = data.get("channel_categories") or {}
        channel_names: List[str] = []
        if isinstance(channels_raw, dict):
            channel_names = [k for k, v in channels_raw.items() if v]
        elif isinstance(channels_raw, list):
            channel_names = [
                (c.get("name") or str(c)) if isinstance(c, dict) else str(c)
                for c in channels_raw
            ]

        # Map channel names to creative data keys
        matched_channels: List[Tuple[str, Dict]] = []
        for ch_name in channel_names[:5]:
            ch_lower = ch_name.lower().replace("_", " ")
            for key, cdata in _CHANNEL_CREATIVE_DATA.items():
                if key in ch_lower or ch_lower in key:
                    matched_channels.append((ch_name.replace("_", " ").title(), cdata))
                    break
        # Ensure at least 2 channels
        if len(matched_channels) < 2:
            for key in ["indeed", "linkedin", "google"]:
                if len(matched_channels) >= 3:
                    break
                if not any(key in mc[0].lower() for mc in matched_channels):
                    matched_channels.append((key.title(), _CHANNEL_CREATIVE_DATA[key]))

        # ── LEFT COLUMN: Channel-specific variants (60% width) ──
        col1_left = Inches(0.55)
        col1_w = Inches(7.3)
        col2_left = Inches(8.1)
        col2_w = Inches(4.7)
        section_top = Inches(1.55)

        # Channel variant cards
        _add_rounded_rect(slide, col1_left, section_top, col1_w, Inches(5.0), WHITE)
        _add_filled_rect(slide, col1_left, section_top, col1_w, Inches(0.05), BLUE)

        _add_textbox(
            slide,
            col1_left + Inches(0.15),
            section_top + Inches(0.1),
            col1_w - Inches(0.3),
            Inches(0.3),
            text="AD COPY VARIANTS BY CHANNEL",
            font_size=10,
            bold=True,
            color=BLUE,
        )

        box_ch, tf_ch = _add_textbox(
            slide,
            col1_left + Inches(0.15),
            section_top + Inches(0.45),
            col1_w - Inches(0.3),
            Inches(4.4),
        )
        tf_ch.paragraphs[0].space_before = Pt(0)
        tf_ch.paragraphs[0].space_after = Pt(0)

        first_item = True
        for ch_label, ch_data in matched_channels[:4]:
            if first_item:
                p = tf_ch.paragraphs[0]
                first_item = False
            else:
                p = tf_ch.add_paragraph()
            p.space_before = Pt(6)
            p.space_after = Pt(2)
            rh = p.add_run()
            rh.text = f"{ch_label}  (CTR: {ch_data.get('ctr_range', 'N/A')})"
            _set_font(rh, size=9, bold=True, color=NAVY)

            for vi, variant in enumerate(ch_data.get("variants", [])[:3]):
                pv = tf_ch.add_paragraph()
                pv.space_before = Pt(1)
                pv.space_after = Pt(2)
                rv = pv.add_run()
                rv.text = f"  {chr(65 + vi)}. {variant}"
                _set_font(rv, size=7, color=MUTED_TEXT)

            # Bidding strategy
            pb = tf_ch.add_paragraph()
            pb.space_before = Pt(1)
            pb.space_after = Pt(4)
            rb = pb.add_run()
            rb.text = f"  Bidding: {ch_data.get('bidding', 'CPC recommended')}"
            _set_font(rb, size=7, italic=True, color=TEAL)

        # ── RIGHT COLUMN: Testing Framework ──
        _add_rounded_rect(slide, col2_left, section_top, col2_w, Inches(5.0), WHITE)
        _add_filled_rect(slide, col2_left, section_top, col2_w, Inches(0.05), TEAL)

        _add_textbox(
            slide,
            col2_left + Inches(0.15),
            section_top + Inches(0.1),
            col2_w - Inches(0.3),
            Inches(0.3),
            text="TESTING FRAMEWORK",
            font_size=10,
            bold=True,
            color=TEAL,
        )

        box_fw, tf_fw = _add_textbox(
            slide,
            col2_left + Inches(0.15),
            section_top + Inches(0.45),
            col2_w - Inches(0.3),
            Inches(4.4),
        )
        tf_fw.paragraphs[0].space_before = Pt(0)
        tf_fw.paragraphs[0].space_after = Pt(0)

        # A/B test structure
        framework_items = [
            ("Test Duration", "7-14 days per variant (min. 100 conversions)"),
            ("Sample Split", "50/50 traffic split; 95% confidence threshold"),
            ("Primary Metric", "Cost Per Application (CPA)"),
            ("Secondary Metrics", "CTR, Apply Rate, Quality Score"),
            ("Winner Criteria", "Lower CPA at statistical significance"),
        ]

        first_fw = True
        for fw_label, fw_val in framework_items:
            if first_fw:
                p = tf_fw.paragraphs[0]
                first_fw = False
            else:
                p = tf_fw.add_paragraph()
            p.space_before = Pt(3)
            p.space_after = Pt(3)
            rl = p.add_run()
            rl.text = f"{fw_label}:  "
            _set_font(rl, size=8, bold=True, color=DARK_TEXT)
            rv = p.add_run()
            rv.text = fw_val
            _set_font(rv, size=8, color=MUTED_TEXT)

        # Industry-specific tests
        p_ind_h = tf_fw.add_paragraph()
        p_ind_h.space_before = Pt(10)
        p_ind_h.space_after = Pt(4)
        r_ind_h = p_ind_h.add_run()
        r_ind_h.text = f"INDUSTRY-SPECIFIC TESTS ({industry_label.upper()})"
        _set_font(r_ind_h, size=8, bold=True, color=NAVY)

        industry_tests = _INDUSTRY_TEST_FRAMEWORKS.get(
            industry, _INDUSTRY_TEST_FRAMEWORKS["default"]
        )
        for test_desc in industry_tests[:3]:
            pt = tf_fw.add_paragraph()
            pt.space_before = Pt(2)
            pt.space_after = Pt(3)
            rt = pt.add_run()
            rt.text = f"\u25b8 {test_desc}"
            _set_font(rt, size=7, color=TEAL)

        # Source line
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(6.7),
            Inches(12.2),
            Inches(0.2),
            text="Source: Nova AI Suite Creative Testing Engine | Industry Benchmarks from Google Ads & Meta Ads data",
            font_size=7,
            italic=True,
            color=MUTED_TEXT,
        )

        # Footer
        _add_footer(slide, today)

    except Exception as exc:
        logger.warning("Failed to build creative testing slide: %s", exc)


# ===================================================================
# GOLD STANDARD SLIDES -- Market Intelligence, Strategy, Timeline
# ===================================================================


def _build_slides_gold_standard(prs: Presentation, data: Dict) -> None:
    """Build 2-3 slides from Gold Standard quality gate outputs.

    Adds the following slides (if data is available):
      - Market Intelligence: city-level data + competitor mapping
      - Strategy & Difficulty: channel strategy + role difficulty framework
      - Activation Timeline: hiring calendar + budget phasing

    Each slide is wrapped in its own try/except so a single failure
    does not block the remaining slides.

    Args:
        prs: The python-pptx Presentation object.
        data: The full enriched data dict containing ``_gold_standard``.
    """
    gold: dict = data.get("_gold_standard") or {}
    if not gold:
        return

    today = datetime.date.today().strftime("%B %d, %Y")

    # ── Slide A: Market Intelligence (city-level + competitors) ──
    city_data: dict = gold.get("city_level_data") or {}
    competitor_map: dict = gold.get("competitor_mapping") or {}
    clearance: Optional[dict] = gold.get("clearance_segmentation")

    if city_data or competitor_map:
        try:
            slide = prs.slides.add_slide(prs.slide_layouts[6])
            _add_filled_rect(
                slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
            )
            _add_top_band(slide, "MARKET INTELLIGENCE", today)

            _add_textbox(
                slide,
                Inches(0.55),
                Inches(0.92),
                Inches(12.2),
                Inches(0.4),
                text="City-level supply-demand analysis and competitive landscape",
                font_size=14,
                bold=True,
                color=NAVY,
            )

            content_top = Inches(1.45)

            # ---- City-Level Data Table ----
            if city_data:
                _add_textbox(
                    slide,
                    Inches(0.55),
                    content_top,
                    Inches(5.5),
                    Inches(0.3),
                    text="CITY-LEVEL SUPPLY DATA",
                    font_size=10,
                    bold=True,
                    color=NAVY,
                )
                _add_filled_rect(
                    slide,
                    Inches(0.55),
                    content_top + Inches(0.3),
                    Inches(1.5),
                    Inches(0.025),
                    TEAL,
                )

                # Table header
                city_table_top = content_top + Inches(0.45)
                col_labels = ["City", "Salary", "Difficulty", "Supply Tier"]
                col_widths = [Inches(1.5), Inches(1.3), Inches(1.1), Inches(1.3)]
                total_w = sum(w for w in col_widths)

                _add_filled_rect(
                    slide,
                    Inches(0.55),
                    city_table_top,
                    total_w,
                    Inches(0.3),
                    NAVY,
                )
                x_pos = Inches(0.55)
                for label, cw in zip(col_labels, col_widths):
                    _add_textbox(
                        slide,
                        x_pos + Inches(0.05),
                        city_table_top,
                        cw - Inches(0.05),
                        Inches(0.3),
                        text=label,
                        font_size=8,
                        bold=True,
                        color=WHITE,
                        anchor=MSO_ANCHOR.MIDDLE,
                    )
                    x_pos += cw

                # Table rows (limit to 8 cities to fit)
                row_y = city_table_top + Inches(0.3)
                row_h = Inches(0.28)
                cities_shown = list(city_data.items())[:8]
                for idx, (city_name, info) in enumerate(cities_shown):
                    bg = LIGHT_BLUE if idx % 2 == 0 else WARM_WHITE
                    _add_filled_rect(slide, Inches(0.55), row_y, total_w, row_h, bg)
                    vals = [
                        city_name,
                        f"${info.get('estimated_salary', 0):,.0f}",
                        f"{info.get('hiring_difficulty', 0):.1f}/10",
                        str(info.get("supply_tier") or "balanced")
                        .replace("_", " ")
                        .title(),
                    ]
                    x_pos = Inches(0.55)
                    for val, cw in zip(vals, col_widths):
                        _add_textbox(
                            slide,
                            x_pos + Inches(0.05),
                            row_y,
                            cw - Inches(0.05),
                            row_h,
                            text=val,
                            font_size=7,
                            color=DARK_TEXT,
                            anchor=MSO_ANCHOR.MIDDLE,
                        )
                        x_pos += cw
                    row_y += row_h

            # ---- Competitor Mapping (right side) ----
            if competitor_map:
                comp_left = Inches(6.2)
                _add_textbox(
                    slide,
                    comp_left,
                    content_top,
                    Inches(6.5),
                    Inches(0.3),
                    text="COMPETITOR LANDSCAPE",
                    font_size=10,
                    bold=True,
                    color=NAVY,
                )
                _add_filled_rect(
                    slide,
                    comp_left,
                    content_top + Inches(0.3),
                    Inches(1.5),
                    Inches(0.025),
                    TEAL,
                )

                comp_y = content_top + Inches(0.5)
                comp_entries = [
                    (city, info)
                    for city, info in competitor_map.items()
                    if not city.startswith("_")
                ][:6]

                for city_name, info in comp_entries:
                    employers = info.get("top_employers") or []
                    intensity = str(info.get("hiring_intensity") or "moderate").title()

                    _add_textbox(
                        slide,
                        comp_left,
                        comp_y,
                        Inches(6.5),
                        Inches(0.22),
                        text=f"{city_name} ({intensity} intensity)",
                        font_size=8,
                        bold=True,
                        color=NAVY,
                    )
                    _add_textbox(
                        slide,
                        comp_left + Inches(0.1),
                        comp_y + Inches(0.22),
                        Inches(6.4),
                        Inches(0.22),
                        text=", ".join(employers[:5]),
                        font_size=7,
                        color=MUTED_TEXT,
                    )
                    comp_y += Inches(0.5)

            # Clearance badge (if applicable)
            if clearance:
                primary = clearance.get("primary_clearance") or {}
                badge_text = f"Clearance: {primary.get('level', 'N/A')} | +{primary.get('salary_premium_pct', 0)}% premium"
                _add_filled_rect(
                    slide, Inches(0.55), Inches(6.4), Inches(4), Inches(0.35), NAVY
                )
                _add_textbox(
                    slide,
                    Inches(0.65),
                    Inches(6.4),
                    Inches(3.8),
                    Inches(0.35),
                    text=badge_text,
                    font_size=8,
                    bold=True,
                    color=TEAL,
                    anchor=MSO_ANCHOR.MIDDLE,
                )

            _add_footer(slide, today)

        except Exception as exc:
            logger.error(
                "Gold Standard slide (Market Intelligence) failed: %s",
                exc,
                exc_info=True,
            )

    # ── Slide B: Strategy & Difficulty ──
    channel_strategy: dict = gold.get("channel_strategy") or {}
    difficulty_framework: list = gold.get("difficulty_framework") or []

    if channel_strategy or difficulty_framework:
        try:
            slide = prs.slides.add_slide(prs.slide_layouts[6])
            _add_filled_rect(
                slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
            )
            _add_top_band(slide, "STRATEGY & DIFFICULTY FRAMEWORK", today)

            _add_textbox(
                slide,
                Inches(0.55),
                Inches(0.92),
                Inches(12.2),
                Inches(0.4),
                text="Channel strategy split and role complexity classification",
                font_size=14,
                bold=True,
                color=NAVY,
            )

            content_top = Inches(1.5)

            # ---- Channel Strategy (left half) ----
            if channel_strategy:
                split = channel_strategy.get("recommended_split") or {}
                trad_pct = split.get("traditional_pct", 65)
                nontrad_pct = split.get("non_traditional_pct", 35)

                _add_textbox(
                    slide,
                    Inches(0.55),
                    content_top,
                    Inches(5.5),
                    Inches(0.3),
                    text="CHANNEL MIX STRATEGY",
                    font_size=10,
                    bold=True,
                    color=NAVY,
                )
                _add_filled_rect(
                    slide,
                    Inches(0.55),
                    content_top + Inches(0.3),
                    Inches(1.5),
                    Inches(0.025),
                    TEAL,
                )

                # Split visualization bar
                bar_top = content_top + Inches(0.5)
                bar_h = Inches(0.5)
                bar_w = Inches(5.2)
                trad_w = Inches(5.2 * trad_pct / 100)
                nontrad_w = Inches(5.2 * nontrad_pct / 100)

                _add_filled_rect(slide, Inches(0.55), bar_top, trad_w, bar_h, BLUE)
                _add_textbox(
                    slide,
                    Inches(0.6),
                    bar_top,
                    trad_w - Inches(0.1),
                    bar_h,
                    text=f"Traditional {trad_pct}%",
                    font_size=10,
                    bold=True,
                    color=WHITE,
                    anchor=MSO_ANCHOR.MIDDLE,
                )

                _add_filled_rect(
                    slide, Inches(0.55) + trad_w, bar_top, nontrad_w, bar_h, TEAL
                )
                _add_textbox(
                    slide,
                    Inches(0.6) + trad_w,
                    bar_top,
                    nontrad_w - Inches(0.1),
                    bar_h,
                    text=f"Non-Trad {nontrad_pct}%",
                    font_size=10,
                    bold=True,
                    color=WHITE,
                    anchor=MSO_ANCHOR.MIDDLE,
                )

                # Channel lists below bar
                list_top = bar_top + Inches(0.65)
                trad_channels = channel_strategy.get("traditional_channels") or []
                nontrad_channels = (
                    channel_strategy.get("non_traditional_channels") or []
                )

                for idx, ch in enumerate(trad_channels[:4]):
                    _add_textbox(
                        slide,
                        Inches(0.7),
                        list_top + Inches(idx * 0.22),
                        Inches(2.5),
                        Inches(0.22),
                        text=f"  {ch.get('name', '')}",
                        font_size=7,
                        color=DARK_TEXT,
                    )

                for idx, ch in enumerate(nontrad_channels[:4]):
                    _add_textbox(
                        slide,
                        Inches(3.2),
                        list_top + Inches(idx * 0.22),
                        Inches(2.8),
                        Inches(0.22),
                        text=f"  {ch.get('name', '')}",
                        font_size=7,
                        color=DARK_TEXT,
                    )

            # ---- Difficulty Framework (right half) ----
            if difficulty_framework:
                diff_left = Inches(6.5)
                _add_textbox(
                    slide,
                    diff_left,
                    content_top,
                    Inches(6),
                    Inches(0.3),
                    text="ROLE DIFFICULTY CLASSIFICATION",
                    font_size=10,
                    bold=True,
                    color=NAVY,
                )
                _add_filled_rect(
                    slide,
                    diff_left,
                    content_top + Inches(0.3),
                    Inches(1.5),
                    Inches(0.025),
                    TEAL,
                )

                # Difficulty cards
                card_top = content_top + Inches(0.5)
                card_h = Inches(0.7)
                card_w = Inches(6.2)

                for idx, role_info in enumerate(difficulty_framework[:6]):
                    card_y = card_top + Inches(idx * (0.7 + 0.08))
                    complexity = role_info.get("complexity_score", 0)
                    seniority = str(role_info.get("seniority_level") or "mid").title()

                    # Complexity-based color
                    if complexity >= 7:
                        card_color = LIGHT_AMBER
                    elif complexity >= 4:
                        card_color = LIGHT_BLUE
                    else:
                        card_color = LIGHT_GREEN

                    _add_filled_rect(
                        slide, diff_left, card_y, card_w, card_h, card_color
                    )

                    _add_textbox(
                        slide,
                        diff_left + Inches(0.1),
                        card_y + Inches(0.05),
                        Inches(3),
                        Inches(0.25),
                        text=str(role_info.get("role_title") or ""),
                        font_size=9,
                        bold=True,
                        color=NAVY,
                    )
                    _add_textbox(
                        slide,
                        diff_left + Inches(0.1),
                        card_y + Inches(0.3),
                        Inches(5.8),
                        Inches(0.35),
                        text=(
                            f"{seniority} | Complexity: {complexity}/10 | "
                            f"TTF: {role_info.get('avg_time_to_fill_days', 0)} days | "
                            f"Budget: {role_info.get('budget_weight', 1.0):.1f}x"
                        ),
                        font_size=7,
                        color=MUTED_TEXT,
                    )

            _add_footer(slide, today)

        except Exception as exc:
            logger.error(
                "Gold Standard slide (Strategy & Difficulty) failed: %s",
                exc,
                exc_info=True,
            )

    # ── Slide C: Activation Timeline ──
    activation: dict = gold.get("activation_calendar") or {}
    budget_tiers: dict = gold.get("budget_tiers") or {}

    if activation:
        try:
            slide = prs.slides.add_slide(prs.slide_layouts[6])
            _add_filled_rect(
                slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
            )
            _add_top_band(slide, "ACTIVATION TIMELINE", today)

            _add_textbox(
                slide,
                Inches(0.55),
                Inches(0.92),
                Inches(12.2),
                Inches(0.4),
                text="Hiring intensity calendar and budget phasing",
                font_size=14,
                bold=True,
                color=NAVY,
            )

            content_top = Inches(1.5)
            timeline: list = activation.get("timeline") or []

            if timeline:
                _add_textbox(
                    slide,
                    Inches(0.55),
                    content_top,
                    Inches(12),
                    Inches(0.3),
                    text="6-MONTH HIRING CALENDAR",
                    font_size=10,
                    bold=True,
                    color=NAVY,
                )
                _add_filled_rect(
                    slide,
                    Inches(0.55),
                    content_top + Inches(0.3),
                    Inches(1.5),
                    Inches(0.025),
                    TEAL,
                )

                # Calendar cards (horizontal layout)
                card_w = Inches(1.95)
                card_h = Inches(3.5)
                cards_left = Inches(0.55)
                cards_top = content_top + Inches(0.5)
                gap = Inches(0.1)

                intensity_colors: dict[str, RGBColor] = {
                    "very_high": RGBColor(0xB5, 0x66, 0x9C),  # Pink
                    "high": RGBColor(0x5A, 0x54, 0xBD),  # Blue Violet
                    "moderate": RGBColor(0x6B, 0xB3, 0xCD),  # Teal
                    "low": RGBColor(0xEB, 0xE6, 0xE0),  # Warm gray
                }

                for idx, month_info in enumerate(timeline[:6]):
                    card_x = cards_left + idx * (card_w + gap)
                    intensity = str(month_info.get("hiring_intensity") or "moderate")
                    bar_color = intensity_colors.get(intensity, TEAL)

                    # Card background
                    _add_filled_rect(
                        slide, card_x, cards_top, card_w, card_h, WARM_WHITE
                    )

                    # Intensity color bar at top of card
                    _add_filled_rect(
                        slide, card_x, cards_top, card_w, Inches(0.08), bar_color
                    )

                    # Month name
                    _add_textbox(
                        slide,
                        card_x + Inches(0.08),
                        cards_top + Inches(0.15),
                        card_w - Inches(0.16),
                        Inches(0.25),
                        text=str(month_info.get("month_name") or ""),
                        font_size=11,
                        bold=True,
                        color=NAVY,
                    )

                    # Season + intensity
                    _add_textbox(
                        slide,
                        card_x + Inches(0.08),
                        cards_top + Inches(0.42),
                        card_w - Inches(0.16),
                        Inches(0.2),
                        text=f"{month_info.get('season', '')} | {intensity.replace('_', ' ').upper()}",
                        font_size=7,
                        bold=True,
                        color=bar_color if intensity != "low" else MUTED_TEXT,
                    )

                    # Budget weight
                    weight = month_info.get("budget_weight", 1.0)
                    _add_textbox(
                        slide,
                        card_x + Inches(0.08),
                        cards_top + Inches(0.65),
                        card_w - Inches(0.16),
                        Inches(0.2),
                        text=f"Budget weight: {weight:.1f}x",
                        font_size=7,
                        color=DARK_TEXT,
                    )

                    # Events list
                    events = month_info.get("key_events") or []
                    event_y = cards_top + Inches(0.9)
                    for ev in events[:3]:
                        _add_textbox(
                            slide,
                            card_x + Inches(0.08),
                            event_y,
                            card_w - Inches(0.16),
                            Inches(0.18),
                            text=f"- {ev}",
                            font_size=6,
                            color=MUTED_TEXT,
                        )
                        event_y += Inches(0.18)

                    # Recommendation
                    rec = str(month_info.get("recommendation") or "")
                    if rec:
                        _add_textbox(
                            slide,
                            card_x + Inches(0.08),
                            cards_top + Inches(1.7),
                            card_w - Inches(0.16),
                            Inches(0.6),
                            text=rec,
                            font_size=6,
                            italic=True,
                            color=DARK_TEXT,
                        )

            # ---- Budget Tiers Summary (bottom section) ----
            if budget_tiers and "error" not in budget_tiers:
                tier_top = Inches(5.3)
                _add_textbox(
                    slide,
                    Inches(0.55),
                    tier_top,
                    Inches(5),
                    Inches(0.25),
                    text="BUDGET TIER ALLOCATION",
                    font_size=10,
                    bold=True,
                    color=NAVY,
                )
                _add_filled_rect(
                    slide,
                    Inches(0.55),
                    tier_top + Inches(0.25),
                    Inches(1.5),
                    Inches(0.025),
                    TEAL,
                )

                tier_breakdown: dict = budget_tiers.get("tier_breakdown") or {}
                tier_x = Inches(0.55)
                tier_card_w = Inches(4.0)
                tier_card_h = Inches(1.0)

                for idx, (tier_key, tier_info) in enumerate(tier_breakdown.items()):
                    tx = tier_x + idx * (tier_card_w + Inches(0.1))
                    _add_filled_rect(
                        slide,
                        tx,
                        tier_top + Inches(0.4),
                        tier_card_w,
                        tier_card_h,
                        NAVY,
                    )

                    tier_label = tier_key.replace("_", " ").title()
                    amount = tier_info.get("amount", 0)
                    pct = tier_info.get("pct", 0)

                    _add_textbox(
                        slide,
                        tx + Inches(0.1),
                        tier_top + Inches(0.45),
                        tier_card_w - Inches(0.2),
                        Inches(0.35),
                        text=f"${amount:,.0f}",
                        font_size=16,
                        bold=True,
                        color=TEAL,
                        anchor=MSO_ANCHOR.MIDDLE,
                    )
                    _add_textbox(
                        slide,
                        tx + Inches(0.1),
                        tier_top + Inches(0.82),
                        tier_card_w - Inches(0.2),
                        Inches(0.25),
                        text=f"{tier_label} ({pct:.0f}%)",
                        font_size=8,
                        color=LIGHT_MUTED,
                        alignment=PP_ALIGN.CENTER,
                    )

            _add_footer(slide, today)

        except Exception as exc:
            logger.error(
                "Gold Standard slide (Activation Timeline) failed: %s",
                exc,
                exc_info=True,
            )


# ===================================================================
# SLIDE: Risk Analysis
# ===================================================================


def _build_slide_risk_analysis(prs: Presentation, data: Dict) -> None:
    """Build a Risk Analysis slide with budget, timing, channel, and competitive risks.

    Presents 4 risk categories with impact assessment and mitigation strategies,
    styled as a professional risk matrix for C-suite audiences.
    """
    try:
        slide_layout = prs.slide_layouts[6]
        slide = prs.slides.add_slide(slide_layout)
        today = datetime.date.today().strftime("%B %d, %Y")

        client = data.get("client_name", "Client")
        budget_alloc = data.get("_budget_allocation", {})
        if not isinstance(budget_alloc, dict):
            budget_alloc = {}
        total_proj = budget_alloc.get("total_projected", {})
        if not isinstance(total_proj, dict):
            total_proj = {}
        channel_allocs = budget_alloc.get("channel_allocations", {})
        if not isinstance(channel_allocs, dict):
            channel_allocs = {}
        gold = data.get("_gold_standard") or {}
        competitor_map = gold.get("competitor_mapping") or {}
        budget_meta = budget_alloc.get("metadata", {})
        if not isinstance(budget_meta, dict):
            budget_meta = {}
        total_budget = budget_meta.get("total_budget") or 0

        # Background
        _add_filled_rect(
            slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE
        )
        _add_top_band(slide, "RISK ANALYSIS", today)

        _add_textbox(
            slide,
            Inches(0.55),
            Inches(0.92),
            Inches(12.2),
            Inches(0.45),
            text=f"Strategic risk assessment for {client}'s recruitment campaign",
            font_size=15,
            bold=True,
            color=NAVY,
        )

        # Build risk items
        risks: list[tuple[str, str, str, str]] = (
            []
        )  # (category, risk, impact, mitigation)

        # S48 FIX: per-channel sum for consistency
        proj_hires = sum(
            int(ch.get("projected_hires") or 0) for ch in channel_allocs.values()
        )
        if proj_hires == 0:
            proj_hires = total_proj.get("hires") or 0
        cph = (
            round(total_budget / max(proj_hires, 1), 2)
            if proj_hires > 0
            else (total_proj.get("cost_per_hire") or 0)
        )

        # 1. Budget risk
        if proj_hires > 0 and cph > 0 and total_budget > 0:
            hires_if_cpa_up = int(total_budget / (cph * 1.2))
            risks.append(
                (
                    "BUDGET",
                    "CPA Inflation Risk",
                    f"If CPA rises 20%, hires drop from {proj_hires:,.0f} to {hires_if_cpa_up:,.0f}",
                    "Build 10-15% budget contingency; diversify channels",
                )
            )
        else:
            risks.append(
                (
                    "BUDGET",
                    "Budget Uncertainty",
                    "Insufficient data for precise projection -- actuals may vary 20-30%",
                    "Start with 2-week pilot; adjust allocation based on early CPA data",
                )
            )

        # 2. Market timing
        campaign_start = data.get("campaign_start_month") or 0
        if isinstance(campaign_start, int) and campaign_start in (4, 5, 6):
            risks.append(
                (
                    "TIMING",
                    "Q2 Competitive Peak",
                    "Q2 hiring is 15-20% more competitive due to fiscal year budget cycles",
                    "Front-load spend in weeks 1-4; lock niche channel inventory early",
                )
            )
        elif isinstance(campaign_start, int) and campaign_start in (1, 2, 3):
            risks.append(
                (
                    "TIMING",
                    "Q1 New Year Surge",
                    "25% more job seekers but 20% more employer competition in Q1",
                    "Capitalize on high candidate supply with aggressive apply-rate optimization",
                )
            )
        else:
            risks.append(
                (
                    "TIMING",
                    "Seasonal Variations",
                    "Hiring demand and candidate supply fluctuate throughout the year",
                    "Monitor weekly CPA trends; shift budget to high-performing periods",
                )
            )

        # 3. Channel dependency
        if channel_allocs:
            sorted_ch = sorted(
                channel_allocs.items(),
                key=lambda x: (
                    x[1].get("percentage", 0) if isinstance(x[1], dict) else 0
                ),
                reverse=True,
            )
            top_2_names = [ch[0] for ch in sorted_ch[:2]]
            top_2_pct = sum(
                ch[1].get("percentage", 0) if isinstance(ch[1], dict) else 0
                for ch in sorted_ch[:2]
            )
            if top_2_pct > 55:
                risks.append(
                    (
                        "CHANNELS",
                        "Channel Concentration",
                        f"{top_2_pct:.0f}% of budget on {', '.join(top_2_names)} -- "
                        f"disruption could impact {top_2_pct * proj_hires / 100:.0f} hires",
                        "Diversify to 4+ channels; maintain backup channels on standby",
                    )
                )
            else:
                risks.append(
                    (
                        "CHANNELS",
                        "Channel Fragmentation",
                        "Budget spread across many channels may dilute impact",
                        "Consolidate on top 3-4 performers after 2-week pilot period",
                    )
                )
        else:
            risks.append(
                (
                    "CHANNELS",
                    "Channel Selection",
                    "Channel performance varies by role and location",
                    "A/B test top 3 channels in first 2 weeks before full commitment",
                )
            )

        # 4. Competitive pressure
        n_high_comp = sum(
            1
            for k, v in competitor_map.items()
            if not str(k).startswith("_")
            and isinstance(v, dict)
            and str(v.get("hiring_intensity") or "").lower() in ("high", "very_high")
        )
        if n_high_comp > 0:
            risks.append(
                (
                    "COMPETITION",
                    f"High Competition ({n_high_comp} markets)",
                    f"Fortune 500+ companies hiring same roles in {n_high_comp} market(s)",
                    "Differentiate with employer brand; emphasize career growth and culture",
                )
            )
        else:
            risks.append(
                (
                    "COMPETITION",
                    "Competitive Landscape",
                    "Competitors may increase hiring activity during campaign period",
                    "Monitor competitor job posting volumes weekly; adjust messaging",
                )
            )

        # Render risk cards in 2x2 grid
        card_w = Inches(6.0)
        card_h = Inches(2.2)
        gap = Inches(0.25)
        grid_top = Inches(1.5)
        grid_left = Inches(0.55)

        risk_colors = {
            "BUDGET": RED_ACCENT,
            "TIMING": AMBER,
            "CHANNELS": BLUE,
            "COMPETITION": TEAL,
        }

        for idx, (category, risk_title, impact, mitigation) in enumerate(risks[:4]):
            col = idx % 2
            row_idx = idx // 2
            x = grid_left + col * (card_w + gap)
            y = grid_top + row_idx * (card_h + gap)

            # Card background
            _add_rounded_rect(slide, x, y, card_w, card_h, WHITE)
            accent_color = risk_colors.get(category, BLUE)
            _add_filled_rect(slide, x, y, Inches(0.06), card_h, accent_color)

            # Category badge
            _add_textbox(
                slide,
                x + Inches(0.2),
                y + Inches(0.08),
                Inches(1.5),
                Inches(0.25),
                text=category,
                font_size=8,
                bold=True,
                color=accent_color,
            )

            # Risk title
            _add_textbox(
                slide,
                x + Inches(0.2),
                y + Inches(0.32),
                card_w - Inches(0.4),
                Inches(0.28),
                text=risk_title,
                font_size=11,
                bold=True,
                color=NAVY,
            )

            # Impact
            _add_textbox(
                slide,
                x + Inches(0.2),
                y + Inches(0.65),
                card_w - Inches(0.4),
                Inches(0.6),
                text=f"Impact: {impact}",
                font_size=9,
                color=DARK_TEXT,
            )

            # Mitigation
            _add_textbox(
                slide,
                x + Inches(0.2),
                y + Inches(1.4),
                card_w - Inches(0.4),
                Inches(0.6),
                text=f"Mitigation: {mitigation}",
                font_size=9,
                bold=False,
                color=MUTED_TEXT,
            )

        _add_footer(slide, today)

    except Exception as exc:
        import logging as _logging

        _logging.getLogger(__name__).error(
            "Risk Analysis slide failed: %s", exc, exc_info=True
        )


# ===================================================================
# SLIDE N (Last) - Data Sources & Methodology
# ===================================================================


def _build_slide_data_sources(prs: Presentation, data: Dict):
    """Build a Data Sources slide listing all API sources, freshness, and confidence.

    This slide is appended as the LAST slide in the deck and provides
    transparency into the data pipeline that powered the media plan.
    """
    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)
    today = datetime.date.today().strftime("%B %d, %Y")

    # Off-white background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE)

    # Top band
    _add_top_band(slide, "DATA SOURCES & METHODOLOGY", today)

    # Action title
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(0.92),
        Inches(12.2),
        Inches(0.5),
        text="Transparency into the data pipeline powering this media plan",
        font_size=15,
        bold=True,
        color=NAVY,
    )

    # Extract enrichment data
    enriched = data.get("_enriched", {})
    if not isinstance(enriched, dict):
        enriched = {}
    summary = enriched.get("enrichment_summary", {})
    if not isinstance(summary, dict):
        summary = {}

    apis_called = summary.get("apis_called") or []
    apis_succeeded = summary.get("apis_succeeded") or []
    apis_failed = summary.get("apis_failed") or []
    apis_skipped = summary.get("apis_skipped") or []
    api_details = summary.get("api_details", {})
    if not isinstance(api_details, dict):
        api_details = {}
    # Prefer the computed confidence from Excel's Sources sheet for consistency
    confidence_score = (
        data.get("_computed_confidence_pct") or summary.get("confidence_score") or 0
    )
    # Normalize: if stored as 0-100 integer, convert to 0-1 for _fmt_pct
    if isinstance(confidence_score, (int, float)) and confidence_score > 1:
        confidence_score = confidence_score / 100.0
    total_time = summary.get("total_time_seconds") or 0

    # ---- HERO STATS BAR ----
    bar_top = Inches(1.55)
    bar_h = Inches(0.85)
    _add_filled_rect(slide, Inches(0.55), bar_top, Inches(12.2), bar_h, NAVY)
    _add_filled_rect(slide, Inches(0.55), bar_top, Inches(12.2), Inches(0.03), TEAL)

    # Exclude skipped APIs from succeeded count
    truly_succeeded = set(apis_succeeded) - set(apis_skipped)
    hero_metrics = [
        (str(len(apis_called)), "APIs Called"),
        (str(len(truly_succeeded)), "APIs Succeeded"),
        (str(len(apis_failed)), "APIs Failed"),
    ]
    if apis_skipped:
        hero_metrics.append((str(len(apis_skipped)), "APIs Skipped"))
    hero_metrics.extend(
        [
            (
                _fmt_pct(confidence_score, decimals=0) if confidence_score else "N/A",
                "Confidence Score",
            ),
            (f"{total_time:.1f}s" if total_time else "N/A", "Fetch Time"),
        ]
    )

    hero_w = Inches(2.44)
    for i, (val, label) in enumerate(hero_metrics):
        hx = Inches(0.55) + i * hero_w
        _add_textbox(
            slide,
            hx,
            bar_top + Inches(0.05),
            hero_w,
            Inches(0.48),
            text=val,
            font_size=22,
            bold=True,
            color=TEAL,
            alignment=PP_ALIGN.CENTER,
            anchor=MSO_ANCHOR.MIDDLE,
        )
        _add_textbox(
            slide,
            hx,
            bar_top + Inches(0.52),
            hero_w,
            Inches(0.25),
            text=label,
            font_size=8,
            bold=False,
            color=LIGHT_MUTED,
            alignment=PP_ALIGN.CENTER,
        )
        # Thin divider between metrics (skip after last)
        if i < len(hero_metrics) - 1:
            div_x = Inches(0.55) + (i + 1) * hero_w
            _add_filled_rect(
                slide,
                div_x,
                bar_top + Inches(0.15),
                Inches(0.015),
                Inches(0.55),
                RGBColor(0x1A, 0x45, 0x70),
            )

    # ---- DATA SOURCES TABLE ----
    table_top = Inches(2.65)
    table_left = Inches(0.55)
    table_w = Inches(12.2)
    row_h = Inches(0.35)

    # Section header
    _add_textbox(
        slide,
        table_left,
        table_top - Inches(0.4),
        Inches(4),
        Inches(0.35),
        text="API DATA SOURCES",
        font_size=11,
        bold=True,
        color=NAVY,
    )
    _add_filled_rect(
        slide, table_left, table_top - Inches(0.08), Inches(1.8), Inches(0.03), TEAL
    )

    # Table header row
    _add_filled_rect(slide, table_left, table_top, table_w, row_h, NAVY)
    col_widths = [Inches(2.8), Inches(2.5), Inches(2.0), Inches(2.2), Inches(2.7)]
    col_headers = ["Data Source", "Status", "Freshness", "Response Time", "Data Points"]
    col_offsets = [Inches(0)]
    for cw in col_widths[:-1]:
        col_offsets.append(col_offsets[-1] + cw)

    for ci, (header, cw, co) in enumerate(zip(col_headers, col_widths, col_offsets)):
        _add_textbox(
            slide,
            table_left + co + Inches(0.1),
            table_top,
            cw - Inches(0.1),
            row_h,
            text=header,
            font_size=9,
            bold=True,
            color=WHITE,
            anchor=MSO_ANCHOR.MIDDLE,
        )

    # Build table rows from api_details
    # If api_details is empty, build from apis_called/succeeded/failed lists
    table_rows = []
    if api_details:
        for api_name, detail in api_details.items():
            if not isinstance(detail, dict):
                continue
            source = detail.get("source", "unknown")
            success = detail.get("success", False)
            elapsed = detail.get("elapsed_time") or 0
            status_label = detail.get("status", "unknown")

            # Determine freshness label
            if source == "live":
                freshness = "Live (real-time)"
            elif source == "cached":
                freshness = "Cached"
            elif source == "error":
                freshness = "--"
            else:
                freshness = "Curated"

            # Estimate data points from enriched data keys
            data_points = "--"
            result_key_map = {
                "BLS": "salary_data",
                "Adzuna": "job_market",
                "Census-ACS": "location_demographics",
                "BLS-QCEW": "industry_employment",
                "WorldBank": "global_indicators",
                "Clearbit": "clearbit_data",
                "Google-Ads": "google_ads_data",
                "Meta-Ads": "meta_ads_data",
                "Teleport": "teleport_data",
            }
            mapped_key = None
            for rk_prefix, rk_val in result_key_map.items():
                if rk_prefix.lower() in api_name.lower():
                    mapped_key = rk_val
                    break
            if mapped_key and enriched.get(mapped_key):
                raw = enriched[mapped_key]
                if isinstance(raw, dict):
                    data_points = str(len(raw))
                elif isinstance(raw, list):
                    data_points = str(len(raw))

            # Check if this API is in the skipped list
            is_skipped = api_name in apis_skipped

            # Determine status display
            # success=True but no actual data returned means it was effectively skipped
            has_data = (
                detail.get("data_points", 0) not in (0, None, "--")
                or data_points != "--"
            )
            if is_skipped or (success and not has_data):
                status_display = "Skipped"
                status_color = AMBER
            elif success:
                status_display = "Succeeded"
                status_color = GREEN
            elif status_label == "empty":
                status_display = "Skipped (no data)"
                status_color = AMBER
            elif status_label == "circuit_open":
                status_display = "Circuit Broken"
                status_color = RED_ACCENT
            else:
                status_display = "Failed"
                status_color = RED_ACCENT

            table_rows.append(
                (
                    api_name,
                    (status_display, status_color),
                    freshness,
                    f"{elapsed:.2f}s" if elapsed else "--",
                    data_points,
                )
            )
    else:
        # Fallback: build rows from simple lists
        # Check skipped BEFORE succeeded -- an API can appear in both
        all_apis = set(apis_called) if apis_called else set()
        for api_name in sorted(all_apis):
            if api_name in apis_skipped:
                status_display = "Skipped"
                status_color = AMBER
                freshness = "--"
            elif api_name in apis_succeeded:
                status_display = "Succeeded"
                status_color = GREEN
                freshness = "Live (real-time)"
            elif api_name in apis_failed:
                status_display = "Failed"
                status_color = RED_ACCENT
                freshness = "--"
            else:
                status_display = "Unknown"
                status_color = MUTED_TEXT
                freshness = "--"
            table_rows.append(
                (
                    api_name,
                    (status_display, status_color),
                    freshness,
                    "--",
                    "--",
                )
            )

    # If no API data at all, show a placeholder row
    if not table_rows:
        table_rows.append(
            (
                "No API enrichment data",
                ("N/A", MUTED_TEXT),
                "Curated benchmarks used",
                "--",
                "--",
            )
        )

    # Render table rows (cap at 12 to fit the slide)
    max_display_rows = 12
    for ri, row_data in enumerate(table_rows[:max_display_rows]):
        ry = table_top + row_h * (ri + 1)
        bg = WHITE if ri % 2 == 0 else RGBColor(0xF8, 0xF6, 0xF3)
        _add_filled_rect(slide, table_left, ry, table_w, row_h, bg)
        # Thin ruled line
        _add_filled_rect(
            slide,
            table_left,
            ry + row_h - Inches(0.008),
            table_w,
            Inches(0.008),
            WARM_GRAY,
        )

        api_name_str, (status_str, status_clr), freshness_str, elapsed_str, dp_str = (
            row_data
        )

        vals = [api_name_str, status_str, freshness_str, elapsed_str, dp_str]
        colors = [DARK_TEXT, status_clr, MUTED_TEXT, MUTED_TEXT, DARK_TEXT]
        bolds = [True, False, False, False, False]

        for ci, (val, clr, bld, cw, co) in enumerate(
            zip(vals, colors, bolds, col_widths, col_offsets)
        ):
            _add_textbox(
                slide,
                table_left + co + Inches(0.1),
                ry,
                cw - Inches(0.1),
                row_h,
                text=str(val),
                font_size=8,
                bold=bld,
                color=clr,
                anchor=MSO_ANCHOR.MIDDLE,
            )

    # Overflow indicator
    if len(table_rows) > max_display_rows:
        overflow_y = table_top + row_h * (max_display_rows + 1) + Inches(0.05)
        _add_textbox(
            slide,
            table_left,
            overflow_y,
            table_w,
            Inches(0.2),
            text=f"+ {len(table_rows) - max_display_rows} additional sources (see appendix)",
            font_size=7,
            italic=True,
            color=MUTED_TEXT,
        )

    # ---- METHODOLOGY FOOTER ----
    # Thin rule line
    _add_rule_line(slide, 0.55, 7.0, 12.2, "D6CFC2")

    # S49 P2-20: Research-backed key recommendations (compact, bottom section)
    _research_recs = data.get("_research_recommendations") or []
    if _research_recs:
        _recs_text = "Key Insights: " + " | ".join(_research_recs[:5])
        _add_textbox(
            slide,
            Inches(0.55),
            Inches(6.65),
            Inches(12.2),
            Inches(0.35),
            text=_recs_text,
            font_size=7,
            bold=False,
            color=TEAL,
        )

    meth_text = (
        "Methodology: Data sourced from real-time API integrations, Nova AI Suite's proprietary "
        "job board knowledge base (91+ platforms), trend engine (4-year history), and "
        "curated industry benchmarks. Confidence score reflects the ratio of successful "
        "live API calls to total attempted."
    )
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(7.05),
        Inches(10),
        Inches(0.35),
        text=meth_text,
        font_size=6,
        italic=True,
        color=LIGHT_MUTED,
    )

    # Generation timestamp
    _add_textbox(
        slide,
        Inches(10.5),
        Inches(7.05),
        Inches(2.2),
        Inches(0.2),
        text=f"Generated: {today}",
        font_size=7,
        bold=False,
        color=MUTED_TEXT,
        alignment=PP_ALIGN.RIGHT,
    )

    # Footer
    _add_footer(slide, today)


# ===================================================================
# CHART SLIDES - Matplotlib-generated visual data slides
# ===================================================================


def _build_slide_budget_pie_chart(prs: Presentation, data: Dict):
    """Build a slide with a matplotlib pie chart showing budget allocation by channel.

    This slide provides a visual breakdown of how the campaign budget is
    distributed across channels. Only added when matplotlib is available
    and channel data exists.
    """
    channels = _selected_channels(data)
    if not channels:
        return

    budget_alloc = data.get("_budget_allocation", {})
    ba_channel_alloc = (
        budget_alloc.get("channel_allocations", {})
        if isinstance(budget_alloc, dict)
        else {}
    )

    labels = []
    sizes = []

    for ch_key, ch_data in channels.items():
        label = ch_data.get("label", ch_key.replace("_", " ").title())
        pct = ch_data.get("pct") or 0

        # Try to get real allocation from budget engine
        ba_match = ba_channel_alloc.get(ch_key)
        if not ba_match:
            ch_label_lower = (ch_data.get("label") or "").lower()
            for ba_key, ba_val in ba_channel_alloc.items():
                if isinstance(ba_val, dict):
                    ba_label = ba_val.get("label", ba_key).lower()
                    if ba_label == ch_label_lower or ba_key.lower() == ch_key.lower():
                        ba_match = ba_val
                        break
        if ba_match and isinstance(ba_match, dict):
            real_pct = ba_match.get("percentage") or 0
            if real_pct > 0:
                pct = round(real_pct)

        if pct > 0:
            labels.append(label)
            sizes.append(pct)

    if not labels:
        return

    chart_bytes = _generate_pie_chart_image(labels, sizes)
    if not chart_bytes:
        return

    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)
    today = datetime.date.today().strftime("%B %d, %Y")

    # Background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE)
    _add_top_band(slide, "BUDGET ALLOCATION OVERVIEW", today)

    client = data.get("client_name", "Client")
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(0.92),
        Inches(12.2),
        Inches(0.45),
        text=f"Visual breakdown of channel investment strategy for {client}",
        font_size=15,
        bold=True,
        color=NAVY,
    )

    # Insert the pie chart image
    chart_stream = io.BytesIO(chart_bytes)
    slide.shapes.add_picture(
        chart_stream,
        Inches(2.5),
        Inches(1.6),
        Inches(8.3),
        Inches(5.0),
    )

    _add_footer(slide, today)


def _build_slide_conversion_funnel(prs: Presentation, data: Dict):
    """Build a slide with a matplotlib funnel chart showing conversion metrics.

    Shows the recruitment funnel: Impressions -> Clicks -> Applications -> Hires.
    Data sourced from budget allocation projections and enriched analytics.
    """
    budget_alloc = data.get("_budget_allocation", {})
    if not isinstance(budget_alloc, dict):
        budget_alloc = {}
    ba_total_proj = budget_alloc.get("total_projected", {})
    if not isinstance(ba_total_proj, dict):
        ba_total_proj = {}
    ba_metadata = budget_alloc.get("metadata", {})
    if not isinstance(ba_metadata, dict):
        ba_metadata = {}
    ba_channel_alloc = budget_alloc.get("channel_allocations", {})
    if not isinstance(ba_channel_alloc, dict):
        ba_channel_alloc = {}

    # Gather funnel metrics -- S48: per-channel sum for consistency
    applications = int(ba_total_proj.get("applications") or 0)
    hires = sum(int(ch.get("projected_hires") or 0) for ch in ba_channel_alloc.values())
    if hires == 0:
        hires = int(ba_total_proj.get("hires") or 0)

    # Estimate impressions and clicks from budget if not provided
    total_budget = ba_metadata.get("total_budget") or 0
    if total_budget <= 0:
        budget_val = _parse_budget_number(data.get("budget") or "")
        total_budget = budget_val if budget_val else 0

    # Calculate clicks and impressions from channel data or defaults
    total_clicks = 0
    total_impressions = 0
    for _ch_key, ch_data in ba_channel_alloc.items():
        if isinstance(ch_data, dict):
            total_clicks += int(ch_data.get("projected_clicks") or 0)
            total_impressions += int(ch_data.get("projected_impressions") or 0)

    # Fallback: estimate from budget using industry averages
    if total_clicks <= 0 and total_budget > 0:
        avg_cpc = 1.50  # Industry average CPC
        total_clicks = int(total_budget / avg_cpc)
    if total_impressions <= 0 and total_clicks > 0:
        avg_ctr = 0.035  # Industry average CTR 3.5%
        total_impressions = int(total_clicks / avg_ctr)
    if applications <= 0 and total_clicks > 0:
        avg_apply_rate = 0.05  # Industry average apply rate 5%
        applications = int(total_clicks * avg_apply_rate)
    if hires <= 0 and applications > 0:
        avg_hire_rate = 0.08  # Industry average hire rate 8%
        hires = max(1, int(applications * avg_hire_rate))

    # Need at least impressions to show a funnel
    if total_impressions <= 0:
        return

    chart_bytes = _generate_funnel_chart_image(
        total_impressions, total_clicks, applications, hires
    )
    if not chart_bytes:
        return

    slide_layout = prs.slide_layouts[6]
    slide = prs.slides.add_slide(slide_layout)
    today = datetime.date.today().strftime("%B %d, %Y")

    # Background
    _add_filled_rect(slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, OFF_WHITE)
    _add_top_band(slide, "RECRUITMENT CONVERSION FUNNEL", today)

    client = data.get("client_name", "Client")
    _add_textbox(
        slide,
        Inches(0.55),
        Inches(0.92),
        Inches(12.2),
        Inches(0.45),
        text=f"Projected candidate pipeline from awareness to hire for {client}",
        font_size=15,
        bold=True,
        color=NAVY,
    )

    # Insert the funnel chart image
    chart_stream = io.BytesIO(chart_bytes)
    slide.shapes.add_picture(
        chart_stream,
        Inches(2.2),
        Inches(1.6),
        Inches(8.8),
        Inches(5.0),
    )

    _add_footer(slide, today)


# ===================================================================
# Public API
# ===================================================================


def generate_pptx(data: Dict[str, Any]) -> bytes:
    """
    Generate a premium LinkedIn-inspired PowerPoint presentation.

    Args:
        data: Dictionary containing client information, industry details,
              channel selections, and campaign parameters. Expected keys:
              client_name, industry, budget, campaign_goals, target_roles/roles,
              work_environment, channel_categories, locations, experience_level.

    Returns:
        bytes: The .pptx file content as bytes, suitable for streaming
               to a client or writing to disk.

    Raises:
        ValueError: If required data fields are missing.
        RuntimeError: If presentation generation fails.
    """
    if data is None or not isinstance(data, dict):
        raise ValueError("Data must be a non-null dictionary.")

    # Ensure minimum required fields have sensible defaults
    data.setdefault("client_name", "Client")
    # Normalize client name casing (preserves known brands)
    data["client_name"] = _proper_client_name(data["client_name"] or "Client")
    data.setdefault("industry", "general_entry_level")
    data.setdefault("locations", [])
    # Frontend sends "target_roles" but PPT uses "roles" -- normalize
    if "target_roles" in data and "roles" not in data:
        data["roles"] = data["target_roles"]
    data.setdefault("roles", [])
    # Normalize roles: dicts -> strings (frontend may send [{"title": "..."}])
    if data["roles"] and isinstance(data["roles"][0], dict):
        data["roles"] = [
            r.get("title", r.get("role", str(r))) if isinstance(r, dict) else str(r)
            for r in data["roles"]
        ]
    data.setdefault("campaign_goals", [])
    data.setdefault("channel_categories", {})
    # Frontend sends "budget_range" but PPT reads "budget" -- normalize
    if data.get("budget_range") and not data.get("budget"):
        data["budget"] = data["budget_range"]
    data.setdefault("budget", "TBD")
    # Frontend sends work_environment as array -- normalize to string
    we = data.get("work_environment", "hybrid")
    if isinstance(we, list):
        data["work_environment"] = we[0] if we else "hybrid"
    data.setdefault("work_environment", "hybrid")

    # Null safety - replace None values with defaults
    for key, default in [
        ("client_name", "Client"),
        ("company_name", "Client"),
        ("industry", "general_entry_level"),
        ("budget", "TBD"),
        ("work_environment", "hybrid"),
    ]:
        if data.get(key) is None:
            data[key] = default
    # Ensure list fields are actual lists
    for key in ["locations", "roles", "target_roles", "campaign_goals", "competitors"]:
        val = data.get(key)
        if val is None:
            data[key] = []
        elif isinstance(val, str):
            data[key] = [val]
    # Ensure channel_categories is a dict
    cc = data.get("channel_categories")
    if cc is None:
        data["channel_categories"] = {}
    elif isinstance(cc, list):
        data["channel_categories"] = {
            (item.get("name") or "" if isinstance(item, dict) else str(item)): True
            for item in cc
        }

    # S49 P2-20: Inject research-backed recommendations into data for slides
    try:
        from research_constants import RESEARCH_FINDINGS_SHORT

        _existing_recs = data.get("_research_recommendations") or []
        if not _existing_recs:
            data["_research_recommendations"] = RESEARCH_FINDINGS_SHORT
    except ImportError:
        pass

    # Industry label mapping (single source of truth in shared_utils.py)
    if not data.get("industry_label"):
        data["industry_label"] = _SHARED_INDUSTRY_LABEL_MAP.get(
            data["industry"], data["industry"].replace("_", " ").title()
        )

    try:
        prs = Presentation()

        # Set document metadata for GEO/SEO discoverability
        core_props = prs.core_properties
        client = data.get("client_name", "Client")
        industry_label = data.get(
            "industry_label", (data.get("industry") or "").replace("_", " ").title()
        )
        core_props.title = f"Recruitment Media Plan - {client}"
        core_props.author = "Nova AI Suite"
        core_props.subject = f"AI-generated recruitment advertising media plan for {client} in the {industry_label} industry"
        core_props.keywords = f"recruitment media plan, {industry_label}, job advertising, programmatic recruitment, {client}, talent acquisition, hiring strategy"
        core_props.comments = f"Generated by Nova AI Media Plan Generator (media-plan-generator.onrender.com). Data sourced from 25 real-time APIs, 91+ job board platforms, and Nova AI Suite industry knowledge base."
        core_props.category = "Recruitment Advertising"
        core_props.last_modified_by = "Nova AI Suite"

        # Set 16:9 widescreen dimensions
        prs.slide_width = SLIDE_WIDTH
        prs.slide_height = SLIDE_HEIGHT

        # S48: Capped at 8 slides for executive pitch deck. Extended analysis lives in Excel.
        # Slide order: Cover, Exec Summary, Channel Strategy, Budget+Pie,
        #              Competitive Landscape, Timeline, Risk, Data Sources.

        # Slide 1: Premium cover / section divider
        _build_slide_cover(prs, data)

        # Quality warning disclaimer on cover slide (when enrichment degraded)
        # S49 Issue 17: Severity-scaled prefix -- "Caution" for minimal data,
        # "Warning" for limited data, "Note" for moderate degradation.
        _quality_warn = data.get("quality_warning") or ""
        if _quality_warn and prs.slides:
            try:
                _cover_slide = prs.slides[0]
                # Determine severity prefix and color from warning text
                if "Minimal data" in _quality_warn:
                    _warn_prefix = "Caution"
                    _warn_color = "FF4444"  # Red for severe
                elif "Limited data" in _quality_warn:
                    _warn_prefix = "Warning"
                    _warn_color = "FF8C00"  # Dark orange for moderate
                else:
                    _warn_prefix = "Note"
                    _warn_color = "FFD700"  # Gold for mild
                _add_textbox(
                    _cover_slide,
                    Inches(0.6),
                    Inches(7.0),
                    Inches(10),
                    Inches(0.3),
                    text=f"{_warn_prefix}: {_quality_warn}",
                    font_size=7,
                    italic=True,
                    color=_warn_color,
                )
            except Exception as _qw_err:
                logger.debug(
                    "Quality warning disclaimer failed (non-fatal): %s", _qw_err
                )

        # Slide 2: Executive Summary (hero stats + SCR + market context)
        _build_slide_executive_summary(prs, data)

        # Slide 3: Channel Strategy with benchmarks + attribution
        _build_slide_channel_strategy(prs, data)

        # Slide 4: Budget Allocation & ROI with embedded pie chart
        #          (or Quality Outcomes as fallback when budget_allocation is empty)
        budget_alloc_data = data.get("_budget_allocation", {})
        if isinstance(budget_alloc_data, dict) and budget_alloc_data:
            _ba_has_data = (
                budget_alloc_data.get("metadata", {}).get("total_budget")
                or 0 > 0
                or budget_alloc_data.get("total_projected", {})
                or budget_alloc_data.get("channel_allocations", {})
            )
            if _ba_has_data:
                _build_slide_budget_allocation(prs, data)
                # Embed pie chart onto the budget allocation slide (right side)
                if _HAS_MATPLOTLIB:
                    try:
                        _embed_pie_chart_on_budget_slide(prs, data)
                    except Exception as _pie_exc:
                        logger.debug(
                            "Embedded pie chart failed (non-fatal): %s", _pie_exc
                        )
            else:
                _build_slide_quality_outcomes(prs, data)
        else:
            _build_slide_quality_outcomes(prs, data)

        # Slide 5: Competitive Landscape (always shown)
        _build_slide_competitive_landscape(prs, data)

        # Slide 6: Implementation Timeline + Competitive Context
        _build_slide_comparison_timeline(prs, data)

        # Slide 7: Risk Analysis -- always included for C-suite readiness
        _build_slide_risk_analysis(prs, data)

        # Slide 8: Data Sources & Methodology
        _build_slide_data_sources(prs, data)

        buffer = io.BytesIO()
        prs.save(buffer)
        buffer.seek(0)
        return buffer.getvalue()

    except Exception as exc:
        logger.error("generate_pptx top-level crash: %s", exc, exc_info=True)
        # Return a minimal error presentation so the caller always gets valid bytes
        try:
            err_prs = Presentation()
            err_prs.slide_width = SLIDE_WIDTH
            err_prs.slide_height = SLIDE_HEIGHT
            err_slide = err_prs.slides.add_slide(err_prs.slide_layouts[6])
            _add_filled_rect(
                err_slide, Inches(0), Inches(0), SLIDE_WIDTH, SLIDE_HEIGHT, NAVY
            )
            _add_textbox(
                err_slide,
                Inches(1),
                Inches(2),
                Inches(11),
                Inches(1),
                text="Media Plan Generation Error",
                font_size=28,
                bold=True,
                color="FFFFFF",
            )
            _add_textbox(
                err_slide,
                Inches(1),
                Inches(3.5),
                Inches(11),
                Inches(1.5),
                text=f"An error occurred while generating the presentation: {exc}\n\n"
                "Please try again or contact support if the issue persists.",
                font_size=14,
                color="CCCCCC",
            )
            err_buf = io.BytesIO()
            err_prs.save(err_buf)
            err_buf.seek(0)
            return err_buf.getvalue()
        except Exception as inner_exc:
            logger.error(
                "generate_pptx: even error presentation creation failed: %s",
                inner_exc,
                exc_info=True,
            )
            raise RuntimeError(
                f"Failed to generate PowerPoint presentation: {exc}"
            ) from exc


# ===================================================================
# CLI entry point for testing
# ===================================================================

if __name__ == "__main__":
    sample_data = {
        "client_name": "Acme Healthcare",
        "client_website": "https://www.acmehealthcare.com",
        "industry": "healthcare_medical",
        "industry_label": "Healthcare & Medical",
        "locations": [
            "New York, NY",
            "Chicago, IL",
            "Houston, TX",
            "Phoenix, AZ",
            "San Diego, CA",
        ],
        "roles": [
            "Registered Nurse",
            "Medical Assistant",
            "Physical Therapist",
            "Lab Technician",
            "Pharmacist",
        ],
        "job_categories": ["Clinical", "Allied Health", "Administrative"],
        "use_case": "Scaling clinical hiring across 5 metro areas to meet Q3 demand surge",
        "campaign_goals": ["high_volume", "cost_efficiency", "speed_to_hire"],
        "work_environment": "on_site",
        "budget": "$75,000 / month",
        "competitors": ["HCA Healthcare", "UnitedHealth Group"],
        "channel_categories": {
            "regional_boards": True,
            "global_boards": True,
            "niche_boards": True,
            "social_media": True,
            "programmatic_dsp": True,
            "employer_branding": False,
            "apac_regional": False,
            "emea_regional": False,
        },
        "include_dei": True,
        "include_innovative": False,
        "include_budget_guide": True,
        "include_global_supply": False,
    }

    pptx_bytes = generate_pptx(sample_data)
    output_path = "media_plan_sample.pptx"
    with open(output_path, "wb") as f:
        f.write(pptx_bytes)
    print(f"Generated {output_path} ({len(pptx_bytes):,} bytes)")
